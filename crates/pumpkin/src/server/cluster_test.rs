use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::controller::ControllerDecision;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::inventory::{
    INV_ARMOR, INV_MAIN, INV_OFFHAND, INV_SELECTED, InvLoc, InventoryCell, InventoryTarget,
};
use pumpkin_cluster::protocol::{BlockPos as ClusterBlockPos, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_data::{Block, dimension::Dimension, item::Item, item_stack::ItemStack};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::Server;

#[path = "cluster_test_avatar.rs"]
mod avatar;
#[path = "cluster_test_interactions.rs"]
mod interactions;
#[path = "cluster_test_loadouts.rs"]
mod loadouts;
pub(crate) use loadouts::FixtureLoadoutReceipt;

const MAGIC: [u8; 8] = *b"PCTE\0\0\0\x02";
const ARENA_X: [f64; 2] = [-0.5, 1.5];
const ARENA_Y: f64 = 80.0;
const ARENA_Z: f64 = 0.5;
const BEACON_POS: ClusterBlockPos = ClusterBlockPos { x: -2, y: 80, z: 0 };

#[derive(Debug)]
enum TestCommand {
    Run(TestKind),
}

pub(crate) struct TestRuntime {
    commands: crossbeam::queue::ArrayQueue<TestCommand>,
    loadout_requests: crossbeam::queue::ArrayQueue<loadouts::RoutedLoadoutRequest>,
    pub(crate) observations: super::cluster_test_observation::ObservationQueue,
    pub(crate) scenarios: super::cluster_test_scenarios::ScenarioRuntime,
    pub(crate) preflight: super::cluster_test_preflight::PreflightRuntime,
    pub(crate) handoff: super::cluster_test_handoff::HandoffRuntime,
    pub(crate) health_policy: super::cluster_test_health_policy::HealthPolicyRuntime,
}

impl TestRuntime {
    pub(crate) fn new() -> Self {
        Self {
            commands: crossbeam::queue::ArrayQueue::new(64),
            loadout_requests: crossbeam::queue::ArrayQueue::new(64),
            observations: super::cluster_test_observation::ObservationQueue::new(),
            scenarios: super::cluster_test_scenarios::ScenarioRuntime::new(),
            preflight: super::cluster_test_preflight::PreflightRuntime::new(),
            handoff: super::cluster_test_handoff::HandoffRuntime::new(),
            health_policy: super::cluster_test_health_policy::HealthPolicyRuntime::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TestKind {
    Arena,
    ItemPlace,
    ItemStale,
    ControllerDivergence,
    Beacon,
    DepartureAdminLobby,
    DepartureAdminDisconnect,
    HandoffRevisionMismatch,
    PacketPreflightLobbyDepart,
    HandoffCallbackRelease,
    DoorTrapdoor,
    ButtonLever,
    EndFrameAnchor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TestWire {
    LoadoutRequest {
        origin: ServerId,
        target: pumpkin_cluster::canonical_action::LivePlayerLease,
        template: super::cluster_test_scenarios::FixtureLoadoutTemplate,
        token: [u8; 16],
    },
    LoadoutPhase {
        token: [u8; 16],
        target: pumpkin_cluster::canonical_action::LivePlayerLease,
        template: super::cluster_test_scenarios::FixtureLoadoutTemplate,
        phase: super::cluster_test_scenarios::InventoryScenarioPhase,
        digest: [u8; 32],
        count: u16,
    },
    Request {
        kind: TestKind,
        request: u64,
        origin: ServerId,
        target: ServerId,
    },
    Observed {
        kind: TestKind,
        request: u64,
        origin: ServerId,
        target: ServerId,
        success: bool,
    },
}

struct PendingParcel {
    target: ServerId,
    generation: u64,
    bytes: Vec<u8>,
}

struct ArenaRun {
    request: TestWire,
    participants: [GlobalPlayerId; 2],
    participant_leases: [pumpkin_cluster::canonical_action::LivePlayerLease; 2],
    world: Arc<crate::world::World>,
    world_origin: u64,
    world_submitted: bool,
    desired: BTreeMap<ClusterBlockPos, u16>,
    observed: BTreeSet<ClusterBlockPos>,
    inventory_origins: [u64; 2],
    loadout_tokens: [Option<[u8; 16]>; 2],
    inventory_submitted: [bool; 2],
    inventory_accepted: [bool; 2],
    reset_correlations: [Option<[u8; 16]>; 2],
    reset_submitted: [bool; 2],
    reset_accepted: [bool; 2],
    teleport_correlations: [Option<[u8; 16]>; 2],
    teleport_submitted: [bool; 2],
    teleport_accepted: [bool; 2],
}

struct DivergenceRun {
    request: TestWire,
    target: GlobalPlayerId,
    armed: bool,
}

struct BeaconRun {
    request: TestWire,
    world: Arc<crate::world::World>,
    world_origin: u64,
    world_submitted: bool,
    desired: BTreeMap<ClusterBlockPos, u16>,
    observed: BTreeSet<ClusterBlockPos>,
}

struct ItemRun {
    request: TestWire,
    target: GlobalPlayerId,
    target_lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    world: Arc<crate::world::World>,
    world_origin: u64,
    world_submitted: bool,
    desired: BTreeMap<ClusterBlockPos, u16>,
    observed: BTreeSet<ClusterBlockPos>,
    inventory_origin: u64,
    loadout_token: Option<[u8; 16]>,
    inventory_submitted: bool,
    inventory_accepted: bool,
}

struct BeaconClientRun {
    target: pumpkin_cluster::inventory::InventoryTarget,
    world: Arc<crate::world::World>,
    seeded: bool,
    prepared: bool,
    base_revision: Option<u64>,
    configured: bool,
    rejected: bool,
    conserved: bool,
}

struct DepartureRun {
    kind: TestKind,
    target: GlobalPlayerId,
    uuid: uuid::Uuid,
    world: pumpkin_cluster::protocol::WorldId,
    position: [f64; 3],
    origin: u64,
    prior_revision: u64,
    correlation: Option<[u8; 16]>,
    submitted: bool,
    accepted: bool,
}

fn beacon_target(world: &Arc<crate::world::World>) -> pumpkin_cluster::inventory::InventoryTarget {
    pumpkin_cluster::inventory::InventoryTarget::Block {
        chunk: pumpkin_cluster::world_delta::chunk_of_block(
            super::cluster_world_delta::world_id(world),
            BEACON_POS.x,
            BEACON_POS.z,
        ),
        position: BEACON_POS,
    }
}

fn seed_beacon(_run: &BeaconClientRun) -> bool {
    false
}

fn beacon_payment_count(
    server: &Server,
    target: pumpkin_cluster::inventory::InventoryTarget,
) -> Option<u8> {
    let snapshot = super::cluster_invsee::inventory_snapshot(server, target)?;
    let count = snapshot
        .cells
        .iter()
        .filter(|cell| {
            pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS
                .1
                .contains(&cell.item)
        })
        .map(|cell| usize::from(cell.count))
        .sum::<usize>();
    u8::try_from(count).ok()
}

fn encode(message: TestWire) -> Option<Vec<u8>> {
    postcard::to_extend(&message, Vec::from(MAGIC)).ok()
}

fn decode(bytes: &[u8]) -> Option<TestWire> {
    bytes
        .starts_with(&MAGIC)
        .then(|| postcard::from_bytes(&bytes[MAGIC.len()..]).ok())
        .flatten()
}

fn generation(server: &Server, peer: ServerId) -> Option<u64> {
    server.cluster_peer_generations.load().get(&peer).copied()
}

fn connected_primary(server: &Server) -> Option<(ServerId, u64)> {
    let connected = server.cluster_connected_peers.load();
    let primaries = server.cluster_primary_peers.load();
    select_connected_primary(
        &connected,
        &primaries,
        &server.cluster_peer_generations.load(),
    )
}

fn select_connected_primary(
    connected: &HashSet<ServerId>,
    primaries: &HashSet<ServerId>,
    generations: &HashMap<ServerId, u64>,
) -> Option<(ServerId, u64)> {
    connected
        .intersection(primaries)
        .copied()
        .filter_map(|peer| {
            generations
                .get(&peer)
                .copied()
                .map(|generation| (peer, generation))
        })
        .min_by_key(|(peer, _)| *peer)
}

fn registry_names() -> [String; 2] {
    [
        std::env::var("PUMPKIN_CLUSTER_E2E_PLAYER_A").unwrap_or_else(|_| "PumpkinE2eA".to_string()),
        std::env::var("PUMPKIN_CLUSTER_E2E_PLAYER_B").unwrap_or_else(|_| "PumpkinE2eB".to_string()),
    ]
}

pub(crate) fn registered_players(server: &Server) -> Option<[GlobalPlayerId; 2]> {
    let names = registry_names();
    let entries = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .map(|(gid, entry)| (gid, entry.name, entry.in_lobby))
        .collect::<Vec<_>>();
    select_registered_players(&names, &entries)
}

fn select_registered_players(
    names: &[String; 2],
    entries: &[(GlobalPlayerId, String, bool)],
) -> Option<[GlobalPlayerId; 2]> {
    let lookup = |name: &str| {
        entries
            .iter()
            .find(|(_, entry_name, in_lobby)| !in_lobby && entry_name == name)
            .map(|(gid, _, _)| *gid)
    };
    let players = [lookup(&names[0])?, lookup(&names[1])?];
    (players[0] != players[1]).then_some(players)
}

pub fn submit(server: &Server, test_id: &str) -> bool {
    let kind = match test_id {
        "arena" => TestKind::Arena,
        "item-place" => TestKind::ItemPlace,
        "item-stale" => TestKind::ItemStale,
        "controller-divergence" => TestKind::ControllerDivergence,
        "beacon" => TestKind::Beacon,
        "departure-admin-lobby" => TestKind::DepartureAdminLobby,
        "departure-admin-disconnect" => TestKind::DepartureAdminDisconnect,
        "handoff-revision-mismatch" => TestKind::HandoffRevisionMismatch,
        "packet-preflight-lobby-depart" => TestKind::PacketPreflightLobbyDepart,
        "handoff-callback-release" => TestKind::HandoffCallbackRelease,
        "door-trapdoor" => TestKind::DoorTrapdoor,
        "button-lever" => TestKind::ButtonLever,
        "end-frame-anchor" => TestKind::EndFrameAnchor,
        _ => return false,
    };
    server
        .cluster_test
        .commands
        .push(TestCommand::Run(kind))
        .is_ok()
}

fn announce(server: &Server, kind: TestKind, success: bool) {
    let name = match kind {
        TestKind::Arena => "arena",
        TestKind::ItemPlace => "item-place",
        TestKind::ItemStale => "item-stale",
        TestKind::ControllerDivergence => "controller-divergence",
        TestKind::Beacon => "beacon",
        TestKind::DepartureAdminLobby => "departure-admin-lobby",
        TestKind::DepartureAdminDisconnect => "departure-admin-disconnect",
        TestKind::HandoffRevisionMismatch => "handoff-revision-mismatch",
        TestKind::PacketPreflightLobbyDepart => "packet-preflight-lobby-depart",
        TestKind::HandoffCallbackRelease => "handoff-callback-release",
        TestKind::DoorTrapdoor => "door-trapdoor",
        TestKind::ButtonLever => "button-lever",
        TestKind::EndFrameAnchor => "end-frame-anchor",
    };
    let state = if success { "accepted" } else { "failed" };
    for player in server.get_all_players() {
        player.send_system_message(&pumpkin_util::text::TextComponent::text(format!(
            "cluster test {name} {state}"
        )));
    }
}

fn arena_world(server: &Server) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| world.dimension == Dimension::OVERWORLD)
        .cloned()
}

fn arena_desired() -> BTreeMap<ClusterBlockPos, u16> {
    let mut desired = BTreeMap::new();
    for x in -4..=4 {
        for y in 79..=85 {
            for z in -4..=4 {
                let stone = (y == 80 && !(z == 0 && (x == -1 || x == 1)))
                    || (y == 79 && z == 0 && (x == -1 || x == 1));
                desired.insert(
                    ClusterBlockPos { x, y, z },
                    if stone {
                        Block::STONE.default_state.id.as_u16()
                    } else {
                        Block::AIR.default_state.id.as_u16()
                    },
                );
            }
        }
    }
    desired
}

fn beacon_desired() -> BTreeMap<ClusterBlockPos, u16> {
    let mut desired = BTreeMap::new();
    for x in -3..=-1 {
        for y in 79..=85 {
            for z in -1..=1 {
                let position = ClusterBlockPos { x, y, z };
                let state = if y == 79 {
                    Block::IRON_BLOCK.default_state.id.as_u16()
                } else if position == BEACON_POS {
                    Block::BEACON.default_state.id.as_u16()
                } else {
                    Block::AIR.default_state.id.as_u16()
                };
                desired.insert(position, state);
            }
        }
    }
    desired
}

fn origin(request: u64, source: ServerId, generation: u64, phase: u8, index: usize) -> u64 {
    0xC17E_0000_0000_0000
        ^ request.rotate_left(17)
        ^ u64::from(source.0).wrapping_shl(48)
        ^ generation.rotate_left(32)
        ^ u64::from(phase).wrapping_shl(2)
        ^ index as u64
}

fn start_arena(server: &Server, request: TestWire, source_generation: u64) -> Option<ArenaRun> {
    let participants = registered_players(server)?;
    let participant_leases = [
        loadouts::lease_for(server, participants[0])?,
        loadouts::lease_for(server, participants[1])?,
    ];
    let world = arena_world(server)?;
    let (request_id, source) = match request {
        TestWire::Request {
            request, origin, ..
        } => (request, origin),
        _ => return None,
    };
    Some(ArenaRun {
        request,
        participants,
        participant_leases,
        world,
        world_origin: origin(request_id, source, source_generation, 1, 0),
        world_submitted: false,
        desired: arena_desired(),
        observed: BTreeSet::new(),
        inventory_origins: [
            origin(request_id, source, source_generation, 2, 0),
            origin(request_id, source, source_generation, 2, 1),
        ],
        loadout_tokens: [None; 2],
        inventory_submitted: [false; 2],
        inventory_accepted: [false; 2],
        reset_correlations: [None; 2],
        reset_submitted: [false; 2],
        reset_accepted: [false; 2],
        teleport_correlations: [None; 2],
        teleport_submitted: [false; 2],
        teleport_accepted: [false; 2],
    })
}

fn start_beacon(server: &Server, request: TestWire, source_generation: u64) -> Option<BeaconRun> {
    registered_players(server)?;
    let world = arena_world(server)?;
    let (request_id, source) = match request {
        TestWire::Request {
            request, origin, ..
        } => (request, origin),
        _ => return None,
    };
    Some(BeaconRun {
        request,
        world,
        world_origin: origin(request_id, source, source_generation, 5, 0),
        world_submitted: false,
        desired: beacon_desired(),
        observed: BTreeSet::new(),
    })
}

fn start_item(server: &Server, request: TestWire, source_generation: u64) -> Option<ItemRun> {
    let target = registered_players(server)?[0];
    let target_lease = loadouts::lease_for(server, target)?;
    let world = arena_world(server)?;
    let TestWire::Request {
        kind,
        request: request_id,
        origin: source,
        ..
    } = request
    else {
        return None;
    };
    let stone = Block::STONE.default_state.id.as_u16();
    let target_state = if kind == TestKind::ItemPlace {
        Block::AIR.default_state.id.as_u16()
    } else {
        Block::OBSIDIAN.default_state.id.as_u16()
    };
    let desired = BTreeMap::from([
        (ClusterBlockPos { x: 0, y: 80, z: 2 }, stone),
        (ClusterBlockPos { x: 0, y: 81, z: 2 }, target_state),
    ]);
    Some(ItemRun {
        request,
        target,
        target_lease,
        world,
        world_origin: origin(request_id, source, source_generation, 6, 0),
        world_submitted: false,
        desired,
        observed: BTreeSet::new(),
        inventory_origin: origin(request_id, source, source_generation, 7, 0),
        loadout_token: None,
        inventory_submitted: false,
        inventory_accepted: false,
    })
}

pub(crate) fn item_inventory_cells() -> Option<Vec<InventoryCell>> {
    Some(vec![
        super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 4),
            &ItemStack::new(2, &Item::STONE),
        )
        .ok()?,
        InventoryCell {
            loc: InvLoc::new(INV_SELECTED, 0),
            item: 5,
            count: 1,
            nbt: Vec::new(),
        },
    ])
}

fn submit_item_inventory(
    server: &Arc<Server>,
    local: ServerId,
    run: &mut ItemRun,
    loadouts: &mut loadouts::Loadouts,
    pending: &mut VecDeque<PendingParcel>,
) {
    if !run.inventory_submitted {
        run.loadout_token = loadouts.request(
            server,
            local,
            run.target_lease,
            super::cluster_test_scenarios::FixtureLoadoutTemplate::ItemPlace,
            run.inventory_origin,
            pending,
        );
        run.inventory_submitted = run.loadout_token.is_some();
    }
    run.inventory_accepted = loadouts.ready(run.loadout_token);
}

fn start_departure(
    server: &Server,
    local: ServerId,
    kind: TestKind,
    request: u64,
) -> Option<DepartureRun> {
    let target = registered_players(server)?[0];
    if target.server != local {
        return None;
    }
    let player = server
        .get_all_players()
        .into_iter()
        .find(|player| player.cluster_gid() == Some(target))?;
    let uuid = player.gameprofile.id;
    let prior_revision = server
        .cluster_handoff
        .load()
        .hints
        .get(&uuid)
        .map_or(0, |hint| hint.playerdata_revision);
    let position = match kind {
        TestKind::DepartureAdminLobby => [0.5, 80.0, 2.5],
        TestKind::DepartureAdminDisconnect => [0.5, 80.0, 4.5],
        _ => return None,
    };
    Some(DepartureRun {
        kind,
        target,
        uuid,
        world: super::cluster_world_delta::world_id(&player.world()),
        position,
        origin: origin(
            request,
            local,
            0,
            6,
            usize::from(matches!(kind, TestKind::DepartureAdminDisconnect)),
        ),
        prior_revision,
        correlation: None,
        submitted: false,
        accepted: false,
    })
}

fn submit_departure(server: &Arc<Server>, run: &DepartureRun) -> Option<[u8; 16]> {
    let player = server
        .get_all_players()
        .into_iter()
        .find(|player| player.cluster_gid() == Some(run.target))?;
    let source = crate::command::CommandSource::new(
        crate::command::CommandSender::Console,
        player.world(),
        None,
        pumpkin_util::math::vector3::Vector3::default(),
        pumpkin_util::math::vector2::Vector2::default(),
        "cluster-e2e".to_owned(),
        pumpkin_util::text::TextComponent::text("cluster-e2e"),
        Arc::clone(server),
    );
    super::cluster_player_commands::submit_test_departure(
        server,
        &source,
        run.target,
        *run.uuid.as_bytes(),
        pumpkin_cluster::player_command_control::PlayerCommandMutation::Teleport(
            pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
                world: run.world,
                position: run.position,
                rotation: Some([0.0, 0.0]),
            },
        ),
    )
    .ok()
}

fn departure_committed(server: &Server, run: &DepartureRun) -> Option<u64> {
    let snapshot = server.cluster_handoff.load();
    let hint = snapshot.hints.get(&run.uuid)?;
    (hint.playerdata_revision > run.prior_revision && hint.position == run.position)
        .then_some(hint.playerdata_revision)
}

fn submit_world(
    world: &Arc<crate::world::World>,
    desired: &BTreeMap<ClusterBlockPos, u16>,
    world_origin: u64,
) -> bool {
    let edits = desired
        .iter()
        .map(
            |(pos, new_state)| super::cluster_world_delta::AdministrativeEditIntent {
                pos: *pos,
                new_state: *new_state,
            },
        )
        .collect();
    super::cluster_world_delta::submit_primary_world_edits(world, edits, Some(world_origin)).is_ok()
}

fn observe_installed_ground(
    server: &Server,
    world: &Arc<crate::world::World>,
    desired: &BTreeMap<ClusterBlockPos, u16>,
    observed: &mut BTreeSet<ClusterBlockPos>,
) {
    let world = super::cluster_world_delta::world_id(world);
    for (position, expected) in desired {
        if observed.contains(position) {
            continue;
        }
        let chunk = pumpkin_cluster::world_delta::chunk_of_block(world, position.x, position.z);
        if super::cluster_ground::accepted_ground_block(server, chunk, *position) == Some(*expected)
        {
            observed.insert(*position);
        }
    }
}

pub(crate) fn desired_loadout() -> Option<Vec<InventoryCell>> {
    let stacks = [
        (
            InvLoc::new(INV_MAIN, 0),
            ItemStack::new(1, &Item::IRON_SWORD),
        ),
        (InvLoc::new(INV_MAIN, 1), ItemStack::new(1, &Item::BOW)),
        (InvLoc::new(INV_MAIN, 2), ItemStack::new(64, &Item::ARROW)),
        (
            InvLoc::new(INV_ARMOR, 0),
            ItemStack::new(1, &Item::IRON_BOOTS),
        ),
        (
            InvLoc::new(INV_ARMOR, 1),
            ItemStack::new(1, &Item::IRON_LEGGINGS),
        ),
        (
            InvLoc::new(INV_ARMOR, 2),
            ItemStack::new(1, &Item::IRON_CHESTPLATE),
        ),
        (
            InvLoc::new(INV_ARMOR, 3),
            ItemStack::new(1, &Item::IRON_HELMET),
        ),
        (
            InvLoc::new(INV_OFFHAND, 0),
            ItemStack::new(1, &Item::SHIELD),
        ),
    ];
    let mut desired = stacks
        .iter()
        .map(|(loc, stack)| super::cluster_invsee::inventory_cell_from_item_stack(*loc, stack))
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    desired.push(InventoryCell {
        loc: InvLoc::new(INV_SELECTED, 0),
        item: 1,
        count: 1,
        nbt: Vec::new(),
    });
    Some(desired)
}

fn submit_inventory(
    server: &Arc<Server>,
    local: ServerId,
    run: &mut ArenaRun,
    index: usize,
    loadouts: &mut loadouts::Loadouts,
    pending: &mut VecDeque<PendingParcel>,
) {
    if !run.inventory_submitted[index] {
        run.loadout_tokens[index] = loadouts.request(
            server,
            local,
            run.participant_leases[index],
            super::cluster_test_scenarios::FixtureLoadoutTemplate::Arena,
            run.inventory_origins[index],
            pending,
        );
        run.inventory_submitted[index] = run.loadout_tokens[index].is_some();
    }
    run.inventory_accepted[index] = loadouts.ready(run.loadout_tokens[index]);
}

fn submit_arena_player_command(
    server: &Arc<Server>,
    run: &ArenaRun,
    index: usize,
    mutation: pumpkin_cluster::player_command_control::PlayerCommandMutation,
) -> Option<[u8; 16]> {
    let target = run.participants[index];
    let uuid = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .find(|(gid, entry)| *gid == target && !entry.in_lobby)?
        .1
        .uuid;
    let source = crate::command::CommandSource::new(
        crate::command::CommandSender::Console,
        Arc::clone(&run.world),
        None,
        pumpkin_util::math::vector3::Vector3::default(),
        pumpkin_util::math::vector2::Vector2::default(),
        "cluster-e2e".to_owned(),
        pumpkin_util::text::TextComponent::text("cluster-e2e"),
        Arc::clone(server),
    );
    super::cluster_player_commands::submit(server, &source, target, uuid, mutation).ok()
}

fn submit_reset(server: &Arc<Server>, run: &mut ArenaRun, index: usize) {
    if run.reset_submitted[index] {
        return;
    }
    let mutation = pumpkin_cluster::player_command_control::PlayerCommandMutation::Reset(
        pumpkin_cluster::player_command_control::PlayerResetState {
            health: 20.0,
            absorption: 0.0,
            food: 20,
            saturation: 5.0,
            gamemode: 0,
            clear_effects: true,
            velocity: [0.0; 3],
            fall_distance: 0.0,
            on_fire: false,
        },
    );
    if let Some(correlation) = submit_arena_player_command(server, run, index, mutation) {
        run.reset_correlations[index] = Some(correlation);
        run.reset_submitted[index] = true;
    }
}

fn submit_teleport(server: &Arc<Server>, run: &mut ArenaRun, index: usize) {
    if run.teleport_submitted[index] {
        return;
    }
    let mutation = pumpkin_cluster::player_command_control::PlayerCommandMutation::Teleport(
        pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
            world: super::cluster_world_delta::world_id(&run.world),
            position: [ARENA_X[index], ARENA_Y, ARENA_Z],
            rotation: Some([if index == 0 { -90.0 } else { 90.0 }, 0.0]),
        },
    );
    if let Some(correlation) = submit_arena_player_command(server, run, index, mutation) {
        run.teleport_correlations[index] = Some(correlation);
        run.teleport_submitted[index] = true;
    }
}

fn arena_commands_failed(server: &Server, run: &ArenaRun) -> bool {
    run.reset_correlations
        .into_iter()
        .chain(run.teleport_correlations)
        .flatten()
        .any(|correlation| {
            server
                .cluster_player_commands
                .terminal(correlation)
                .is_some_and(|terminal| !terminal.applied || terminal.action.is_none())
        })
}

fn observe_arena_commands(server: &Server, run: &mut ArenaRun) {
    for index in 0..2 {
        run.reset_accepted[index] = run.reset_correlations[index]
            .and_then(|correlation| server.cluster_player_commands.terminal(correlation))
            .is_some_and(|terminal| terminal.applied && terminal.action.is_some());
        run.teleport_accepted[index] = run.teleport_correlations[index]
            .and_then(|correlation| server.cluster_player_commands.terminal(correlation))
            .is_some_and(|terminal| terminal.applied && terminal.action.is_some());
    }
}

fn observed_reply(server: &Server, request: TestWire, success: bool) -> Option<PendingParcel> {
    let TestWire::Request {
        kind,
        request,
        origin,
        target,
    } = request
    else {
        return None;
    };
    encode(TestWire::Observed {
        kind,
        request,
        origin,
        target,
        success,
    })
    .and_then(|bytes| {
        generation(server, origin).map(|generation| PendingParcel {
            target: origin,
            generation,
            bytes,
        })
    })
}

fn valid_request(
    server: &Server,
    parcel: &InboundParcel,
    message: TestWire,
    local: ServerId,
) -> bool {
    let TestWire::Request { origin, target, .. } = message else {
        return false;
    };
    origin == parcel.peer
        && target == local
        && generation(server, origin) == Some(parcel.generation)
}

fn flush(
    server: &Server,
    outbound: &mpsc::Sender<OutboundParcel>,
    pending: &mut VecDeque<PendingParcel>,
) {
    let Some(front) = pending.front() else { return };
    if !server
        .cluster_connected_peers
        .load()
        .contains(&front.target)
        || generation(server, front.target) != Some(front.generation)
    {
        return;
    }
    let parcel = OutboundParcel {
        peer: front.target,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes: front.bytes.clone(),
    };
    if outbound.try_send(parcel).is_ok() {
        pending.pop_front();
    }
}

pub fn spawn_test_apply(
    server: &Arc<Server>,
    local: ServerId,
    _peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut inbound: mpsc::Receiver<InboundParcel>,
) {
    let server = Arc::clone(server);
    server.clone().spawn_task(async move {
        let mut interaction_setup: Option<interactions::Setup> = None;
        let mut interaction_client: Option<interactions::ClientSetup> = None;
        let mut interaction_observers = interactions::Observer::selected();
        let mut preflight = super::cluster_test_preflight::PreflightOwner::default();
        let mut loadouts = loadouts::Loadouts::default();
        let mut avatar_observer=avatar::AvatarObserver::selected();
        let mut prepared_tokens=BTreeSet::new();
        let mut next_request = 1_u64;
        let mut pending: VecDeque<PendingParcel> = VecDeque::new();
        let mut outstanding = BTreeMap::new();
        let mut failures = VecDeque::new();
        let mut retained_command = None;
        let mut arena: Option<ArenaRun> = None;
        let mut item: Option<ItemRun> = None;
        let mut beacon_setup: Option<BeaconRun> = None;
        let mut beacon_client: Option<BeaconClientRun> = None;
        let mut departure: Option<DepartureRun> = None;
        let mut divergence: Option<DivergenceRun> = None;
        let mut revision_fault: Option<(uuid::Uuid, std::time::Instant)> = None;
        let mut world_publications = super::cluster_world_delta::subscribe_administrative_publications(&server);
        let mut inventory_publications = super::cluster_invsee::subscribe_inventory_publications(&server);
        let mut advance = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = advance.tick() => {
                    let departure_context = departure.as_ref().filter(|run| run.submitted).map(|run| super::cluster_test_preflight::DepartureContext {
                        scenario: if run.kind == TestKind::DepartureAdminLobby { super::cluster_test_preflight::PreflightScenario::Lobby } else { super::cluster_test_preflight::PreflightScenario::Disconnect },
                        target: run.target, uuid: run.uuid, world: run.world, position: run.position, prior_revision: run.prior_revision,
                    });
                    preflight.advance(&server, departure_context);
                    loadouts.advance(&server,local,&mut pending);
                    for _ in 0..64 {
                        let Some(event)=server.cluster_test.scenarios.pop_prepared_action() else {break};
                        if prepared_tokens.len()==64 || !prepared_tokens.insert(event.token) {tracing::error!(token=?event.token,"cluster test prepared action duplicate or capacity failure");continue;}
                        announce_action_prepared(&server,event);
                    }
                    if let Some(observer)=avatar_observer.as_mut() {observer.advance(&server,local);}
                    for observer in &mut interaction_observers { observer.advance(&server); }
                    if let Some(setup) = interaction_setup.as_mut() && let Some(reply) = setup.advance(&server) {
                        pending.push_back(reply); interaction_setup = None;
                    }
                    if let Some(setup) = interaction_client.as_mut() && let Some(success) = setup.advance(&server,local,&mut loadouts,&mut pending) {
                        announce(&server, setup.kind(), success); interaction_client = None;
                    }
                    if pending.front().is_some_and(|parcel| {
                        generation(&server, parcel.target)
                            .is_some_and(|generation| generation != parcel.generation)
                    }) && let Some(stale) = pending.pop_front() {
                        match decode(&stale.bytes) {
                            Some(TestWire::Request { kind, request, .. }) => {
                                outstanding.remove(&request);
                                retained_command.get_or_insert(TestCommand::Run(kind));
                            }
                            Some(TestWire::Observed { kind, request, .. }) => {
                                if failures.len() == 64 {
                                    failures.pop_front();
                                }
                                failures.push_back((request, kind));
                                tracing::error!(
                                    request,
                                    test = ?kind,
                                    peer = stale.target.0,
                                    "cluster test completion route generation changed"
                                );
                            }
                            _ => {}
                        }
                    }
                    if let Some((uuid, started)) = revision_fault {
                        let state = server.cluster_handoff.load();
                        if state.revision_mismatch_armed == Some(uuid) {
                            for player in server.get_all_players() {
                                player.send_system_message(&pumpkin_util::text::TextComponent::text("cluster test handoff-revision-mismatch armed"));
                            }
                            revision_fault = None;
                        } else if started.elapsed() > std::time::Duration::from_secs(5) {
                            announce(&server, TestKind::HandoffRevisionMismatch, false);
                            revision_fault = None;
                        }
                    }
                    if let Some(TestCommand::Run(kind)) = retained_command.take().or_else(|| server.cluster_test.commands.pop()) {
                    if kind == TestKind::HandoffRevisionMismatch {
                        let Ok(uuid) = crate::net::offline_uuid("PumpkinE2eE") else {
                            announce(&server, kind, false);
                            continue;
                        };
                        if revision_fault.is_some() { continue; }
                        if server.cluster_handoff_tx.get().is_some_and(|sender| sender.try_send(super::cluster_handoff::HandoffCommand::ArmRevisionMismatch { uuid }).is_ok()) {
                            revision_fault = Some((uuid, std::time::Instant::now()));
                        } else {
                            announce(&server, kind, false);
                        }
                        continue;
                    }
                    if matches!(kind, TestKind::DepartureAdminLobby | TestKind::DepartureAdminDisconnect) {
                        let request = next_request;
                        if let Some(run) = start_departure(&server, local, kind, request) {
                            next_request = next_request.wrapping_add(1);
                            departure = Some(run);
                        } else {
                            retained_command = Some(TestCommand::Run(kind));
                        }
                        continue;
                    }
                    if kind == TestKind::ControllerDivergence
                        && let Some(target) = registered_players(&server).map(|players| players[0])
                        && target.server == local
                    {
                        let request = TestWire::Request {
                            kind,
                            request: next_request,
                            origin: local,
                            target: local,
                        };
                        next_request = next_request.wrapping_add(1);
                        divergence = Some(DivergenceRun { request, target, armed: false });
                        continue;
                    }
                    let destination = match kind {
                        TestKind::Arena | TestKind::Beacon | TestKind::ItemPlace | TestKind::ItemStale | TestKind::DoorTrapdoor | TestKind::ButtonLever | TestKind::EndFrameAnchor => connected_primary(&server),
                        TestKind::ControllerDivergence => registered_players(&server)
                            .map(|players| players[0].server)
                            .and_then(|target| generation(&server, target).map(|epoch| (target, epoch))),
                        TestKind::PacketPreflightLobbyDepart | TestKind::HandoffCallbackRelease => registered_players(&server)
                            .map(|players| players[0].server)
                            .and_then(|target| generation(&server, target).map(|epoch| (target, epoch))),
                        TestKind::DepartureAdminLobby | TestKind::DepartureAdminDisconnect | TestKind::HandoffRevisionMismatch => None,
                    };
                    if let Some((target, target_epoch)) = destination {
                        let request = next_request;
                        let message = TestWire::Request { kind, request, origin: local, target };
                        next_request = next_request.wrapping_add(1);
                        if let Some(bytes) = encode(message) {
                            outstanding.insert(request, (target, target_epoch));
                            pending.push_back(PendingParcel { target, generation: target_epoch, bytes });
                        }
                    } else {
                        retained_command = Some(TestCommand::Run(kind));
                    }
                    }
                    flush(&server, &outbound, &mut pending);
                    if arena.as_ref().is_some_and(|run| arena_commands_failed(&server, run)) {
                        if let Some(run) = arena.take()
                            && let Some(reply) = observed_reply(&server, run.request, false) {
                            pending.push_back(reply);
                        }
                    }
                    if let Some(run) = arena.as_mut() {
                        observe_arena_commands(&server, run);
                        if !run.world_submitted && submit_world(&run.world, &run.desired, run.world_origin) {
                            run.world_submitted = true;
                        }
                        if run.observed.len() == run.desired.len() {
                            for index in 0..2 { submit_inventory(&server,local,run,index,&mut loadouts,&mut pending); }
                        }
                        if run.inventory_accepted.into_iter().all(|ready| ready) {
                            for index in 0..2 { submit_reset(&server, run, index); }
                        }
                        if run.reset_accepted.into_iter().all(|ready| ready) {
                            for index in 0..2 { submit_teleport(&server, run, index); }
                        }
                    }
                    if arena.as_ref().is_some_and(|run| run.teleport_accepted.into_iter().all(|ready| ready)) {
                        if let Some(run) = arena.take() { for token in run.loadout_tokens {loadouts.retire(token);} if let Some(reply) = observed_reply(&server, run.request, true) { pending.push_back(reply); } }
                    }
                    if let Some(run) = item.as_mut() {
                        if !run.world_submitted && submit_world(&run.world, &run.desired, run.world_origin) {
                            run.world_submitted = true;
                        }
                        if run.observed.len() == run.desired.len() {
                            submit_item_inventory(&server,local,run,&mut loadouts,&mut pending);
                        }
                    }
                    if item.as_ref().is_some_and(|run| run.inventory_accepted) {
                        if let Some(run)=item.take() {loadouts.retire(run.loadout_token);if let Some(reply)=observed_reply(&server,run.request,true) {pending.push_back(reply);}}
                    }
                    if let Some(run) = beacon_setup.as_mut() {
                        if !run.world_submitted && submit_world(&run.world, &run.desired, run.world_origin) {
                            run.world_submitted = true;
                        }
                    }
                    if beacon_setup.as_ref().is_some_and(|run| run.observed.len() == run.desired.len()) {
                        if let Some(run) = beacon_setup.take()
                            && let Some(reply) = observed_reply(&server, run.request, true)
                        {
                            pending.push_back(reply);
                        }
                    }
                    if let Some(run) = beacon_client.as_mut() {
                        if !run.seeded {
                            run.seeded = seed_beacon(run);
                        }
                        if run.seeded && !run.prepared {
                            run.prepared = true;
                            for player in server.get_all_players() {
                                player.send_system_message(&pumpkin_util::text::TextComponent::text(
                                    "cluster test beacon prepared",
                                ));
                            }
                        }
                        if run.seeded {
                            let state = server.cluster_beacon_states.load().get(&run.target).copied();
                            let payment = beacon_payment_count(&server, run.target);
                            if run.base_revision.is_none()
                                && let (Some(state), Some(1)) = (state, payment)
                                && state.levels >= 1
                            {
                                run.base_revision = Some(state.revision);
                                announce(&server, TestKind::Beacon, true);
                            }
                            if !run.configured
                                && let (Some(base_revision), Some(state), Some(0)) = (run.base_revision, state, payment)
                                && state.revision > base_revision
                                && matches!(state.primary_effect, Some(1 | 3))
                            {
                                run.configured = true;
                                for player in server.get_all_players() {
                                    player.send_system_message(&pumpkin_util::text::TextComponent::text(
                                        "cluster test beacon configured",
                                    ));
                                }
                            }
                            if run.configured && run.rejected && !run.conserved && payment == Some(0) {
                                run.conserved = true;
                                for player in server.get_all_players() {
                                    player.send_system_message(&pumpkin_util::text::TextComponent::text(
                                        "cluster test beacon conserved",
                                    ));
                                }
                            }
                        }
                    }
                    if let Some(run) = departure.as_mut() {
                        if !run.submitted {
                            run.correlation = submit_departure(&server, run);
                            run.submitted = run.correlation.is_some();
                            if run.submitted {
                                let stage = if run.kind == TestKind::DepartureAdminLobby {
                                    "cluster test departure-admin-lobby staged"
                                } else {
                                    "cluster test departure-admin-disconnect staged"
                                };
                                if let Some(player) = server.get_all_players().into_iter().find(|player| player.cluster_gid() == Some(run.target)) {
                                    player.send_system_message(&pumpkin_util::text::TextComponent::text(stage));
                                }
                            }
                        }
                    }
                    if let Some(run) = departure.as_mut()
                        && let Some(terminal) = run.correlation
                            .and_then(|correlation| server.cluster_player_commands.terminal(correlation)) {
                        run.accepted = terminal.applied && terminal.action.is_some();
                        if !run.accepted {
                            tracing::error!(test = ?run.kind, correlation = ?run.correlation,
                                "cluster test canonical departure action rejected");
                            departure = None;
                        }
                    }
                    if let Some(run) = departure.as_ref()
                        && run.accepted
                        && let Some(revision) = departure_committed(&server, run)
                    {
                        tracing::info!(
                            test = ?run.kind,
                            origin = run.origin,
                            correlation = ?run.correlation,
                            ground_action = ?run.correlation.and_then(|correlation| server.cluster_player_commands.terminal(correlation)).and_then(|terminal| terminal.action),
                            canonical_ground = run.accepted,
                            gid = ?run.target,
                            %revision,
                            position = ?run.position,
                            "cluster test departure admin committed"
                        );
                        departure = None;
                    }
                    if let Some(run) = divergence.as_mut() {
                        if !run.armed { run.armed = server.cluster_controller.set_test_offset(run.target, [3.0, 0.0, 0.0]); }
                        if run.armed && server.cluster_controller.frame(run.target).is_some_and(|frame| {
                            matches!(frame.decision, ControllerDecision::Modified(1))
                                && (frame.effective.position[0] - frame.claimed.position[0] - 3.0).abs() < f64::EPSILON
                        }) {
                            if let Some(run) = divergence.take() {
                                if matches!(run.request, TestWire::Request { origin, .. } if origin == local) {
                                    announce(&server, TestKind::ControllerDivergence, true);
                                } else if let Some(reply) = observed_reply(&server, run.request, true) {
                                    pending.push_back(reply);
                                }
                            }
                        }
                    }
                }
                Some(parcel) = inbound.recv() => {
                    let Some(message) = decode(&parcel.bytes) else { continue };
                    match message {
                        TestWire::LoadoutRequest {origin,target,template,token} => {
                            loadouts.receive_request(&server,local,parcel.peer,parcel.generation,origin,target,template,token);
                        }
                        TestWire::LoadoutPhase {token,target,template,phase,digest,count} => {
                            loadouts.receive_phase(&server,parcel.peer,parcel.generation,token,target,template,phase,digest,count);
                        }
                        TestWire::Request { kind: TestKind::Arena, .. }
                            if server.advanced_config.cluster.role == ClusterRole::Primary
                                && valid_request(&server, &parcel, message, local) => {
                            arena = start_arena(&server, message, parcel.generation);
                            if arena.is_none()
                                && let Some(reply) = observed_reply(&server, message, false)
                            {
                                pending.push_back(reply);
                            }
                        }
                        TestWire::Request { kind: TestKind::ItemPlace | TestKind::ItemStale, .. }
                            if server.advanced_config.cluster.role == ClusterRole::Primary
                                && valid_request(&server, &parcel, message, local) => {
                            item = start_item(&server, message, parcel.generation);
                            if item.is_none()
                                && let Some(reply) = observed_reply(&server, message, false)
                            {
                                pending.push_back(reply);
                            }
                        }
                        TestWire::Request { kind: TestKind::ControllerDivergence, .. }
                            if valid_request(&server, &parcel, message, local) => {
                            if let Some(target) = registered_players(&server).map(|players| players[0]).filter(|gid| gid.server == local) {
                                divergence = Some(DivergenceRun { request: message, target, armed: false });
                            }
                        }
                        TestWire::Request { kind: TestKind::DoorTrapdoor | TestKind::ButtonLever | TestKind::EndFrameAnchor, .. }
                            if server.advanced_config.cluster.role == ClusterRole::Primary && valid_request(&server, &parcel, message, local) => {
                            interaction_setup = interactions::Setup::new(&server, message, parcel.generation);
                            if interaction_setup.is_none() && let Some(reply) = observed_reply(&server,message,false) {pending.push_back(reply);}
                        }
                        TestWire::Request { kind: TestKind::HandoffCallbackRelease, origin, .. }
                            if valid_request(&server, &parcel, message, local) => {
                            let success = super::cluster_test_handoff::release(&server, origin);
                            if let Some(reply) = observed_reply(&server, message, success) { pending.push_back(reply); }
                        }
                        TestWire::Request { kind: TestKind::PacketPreflightLobbyDepart, origin, .. }
                            if valid_request(&server, &parcel, message, local) => {
                            let success = preflight.request_external_departure(&server, origin);
                            if let Some(reply) = observed_reply(&server, message, success) { pending.push_back(reply); }
                        }
                        TestWire::Request { kind: TestKind::Beacon, .. }
                            if server.advanced_config.cluster.role == ClusterRole::Primary
                                && valid_request(&server, &parcel, message, local) => {
                            beacon_setup = start_beacon(&server, message, parcel.generation);
                            if beacon_setup.is_none()
                                && let Some(reply) = observed_reply(&server, message, false)
                            {
                                pending.push_back(reply);
                            }
                        }
                        TestWire::Observed { kind, request, origin, target, success }
                            if origin == local && target == parcel.peer
                                && outstanding.get(&request) == Some(&(target, parcel.generation))
                                && generation(&server, target) == Some(parcel.generation) => {
                            outstanding.remove(&request);
                            if !success {
                                if failures.len() == 64 {
                                    failures.pop_front();
                                }
                                failures.push_back((request, kind));
                                tracing::error!(
                                    request,
                                    test = ?kind,
                                    peer = target.0,
                                    generation = parcel.generation,
                                    "cluster test preparation failed"
                                );
                            }
                            if success && let Some(scenario) = interactions::Scenario::from_kind(kind) {
                                interaction_client = interactions::ClientSetup::new(&server,scenario,request);
                            } else                             if kind == TestKind::Beacon && success {
                                let world = arena_world(&server);
                                let participant = registered_players(&server).map(|players| players[0]);
                                if let (Some(world), Some(participant)) = (world, participant)
                                    && participant.server == local
                                {
                                    beacon_client = Some(BeaconClientRun {
                                        target: beacon_target(&world),
                                        world,
                                        seeded: false,
                                        prepared: false,
                                        base_revision: None,
                                        configured: false,
                                        rejected: false,
                                        conserved: false,
                                    });
                                } else {
                                    announce(&server, kind, false);
                                }
                            } else {
                                announce(&server, kind, success);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(publication) = world_publications.recv(), if arena.is_some() || beacon_setup.is_some() || item.is_some() => {
                    if let Some(run) = arena.as_mut().filter(|run| publication.origin == Some(run.world_origin) && publication.accepted) {
                        observe_installed_ground(&server, &run.world, &run.desired, &mut run.observed);
                    }
                    if let Some(run) = beacon_setup.as_mut().filter(|run| publication.origin == Some(run.world_origin) && publication.accepted) {
                        observe_installed_ground(&server, &run.world, &run.desired, &mut run.observed);
                    }
                    if let Some(run) = item.as_mut().filter(|run| publication.origin == Some(run.world_origin) && publication.accepted) {
                        observe_installed_ground(&server, &run.world, &run.desired, &mut run.observed);
                    }
                }
                Ok(publication) = inventory_publications.recv(), if arena.is_some() || beacon_client.is_some() || item.is_some() => {
                    if let Some(run) = beacon_client.as_mut()
                        && run.base_revision.is_some()
                        && publication.resolution.result.is_err()
                        && publication.resolution.authoritative.iter().any(|snapshot| snapshot.target == run.target)
                    {
                        run.rejected = true;
                    }
                }
                else => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_selection_uses_live_authenticated_generation() {
        let connected = HashSet::from([ServerId(2), ServerId(3)]);
        let primaries = HashSet::from([ServerId(1), ServerId(3)]);
        let generations = HashMap::from([(ServerId(3), 7)]);
        assert_eq!(
            select_connected_primary(&connected, &primaries, &generations),
            Some((ServerId(3), 7))
        );
    }

    #[test]
    fn saturated_command_queue_preserves_the_queued_request() {
        let runtime = TestRuntime {
            commands: crossbeam::queue::ArrayQueue::new(1),
            ..TestRuntime::new()
        };
        runtime
            .commands
            .push(TestCommand::Run(TestKind::Arena))
            .unwrap();
        assert!(
            runtime
                .commands
                .push(TestCommand::Run(TestKind::ControllerDivergence))
                .is_err()
        );
        assert!(matches!(
            runtime.commands.pop(),
            Some(TestCommand::Run(TestKind::Arena))
        ));
    }

    #[test]
    fn configured_registry_excludes_unrelated_and_lobby_players() {
        use pumpkin_cluster::identity::PlayerSlot;

        let names = ["A".to_string(), "B".to_string()];
        let a = GlobalPlayerId::new(ServerId(2), PlayerSlot(1));
        let b = GlobalPlayerId::new(ServerId(3), PlayerSlot(1));
        let entries = vec![
            (
                GlobalPlayerId::new(ServerId(4), PlayerSlot(1)),
                "unrelated".to_string(),
                false,
            ),
            (a, "A".to_string(), false),
            (b, "B".to_string(), false),
            (
                GlobalPlayerId::new(ServerId(5), PlayerSlot(1)),
                "A".to_string(),
                true,
            ),
        ];
        assert_eq!(select_registered_players(&names, &entries), Some([a, b]));
    }
}

pub(crate) fn submit_console_loadout(
    server: &Server,
    target: pumpkin_cluster::canonical_action::LivePlayerLease,
    template: super::cluster_test_scenarios::FixtureLoadoutTemplate,
    token: [u8; 16],
) -> Result<tokio::sync::oneshot::Receiver<Result<FixtureLoadoutReceipt, ()>>, ()> {
    let (completion, receipt) = tokio::sync::oneshot::channel();
    server
        .cluster_test
        .loadout_requests
        .push(loadouts::RoutedLoadoutRequest {
            target,
            template,
            token,
            completion,
        })
        .map_err(|_| ())?;
    Ok(receipt)
}

fn announce_action_prepared(
    server: &Server,
    event: super::cluster_test_scenarios::PreparedActionScenario,
) {
    if event
        .actors
        .iter()
        .any(|lease| loadouts::lease_for(server, lease.gid) != Some(*lease))
    {
        tracing::error!(token=?event.token,"cluster test prepared action target lease changed");
        return;
    }
    let target = event.actors[0];
    let Some(player) = server.get_all_players().into_iter().find(|player| {
        super::cluster_world_owner::player_action_lease(server, player) == Some(target)
    }) else {
        tracing::error!(token=?event.token,?target,"cluster test prepared action feedback host unavailable");
        return;
    };
    let token = event
        .token
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    tracing::info!(scenario=event.command.id(),%token,actors=?event.actors,world=?event.world,world_actions=?event.world_actions,teleports=?event.teleports,grant_actions=event.loadouts.iter().map(|receipt|u64::from(receipt.grant_action_count)).sum::<u64>(),"cluster e2e action scenario prepared");
    player.send_system_message(&pumpkin_util::text::TextComponent::text(format!(
        "cluster test {} prepared",
        event.command.id()
    )));
}
