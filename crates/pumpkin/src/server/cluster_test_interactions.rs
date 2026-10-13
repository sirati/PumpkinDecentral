use super::*;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{ActionRef, ChunkPart, Target, Value};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
pub(super) enum Scenario {
    DoorTrapdoor,
    ButtonLever,
    EndFrameAnchor,
}

impl Scenario {
    pub(super) fn kind(self) -> TestKind {
        match self {
            Self::DoorTrapdoor => TestKind::DoorTrapdoor,
            Self::ButtonLever => TestKind::ButtonLever,
            Self::EndFrameAnchor => TestKind::EndFrameAnchor,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::DoorTrapdoor => "door-trapdoor",
            Self::ButtonLever => "button-lever",
            Self::EndFrameAnchor => "end-frame-anchor",
        }
    }
    pub(super) fn from_kind(kind: TestKind) -> Option<Self> {
        match kind {
            TestKind::DoorTrapdoor => Some(Self::DoorTrapdoor),
            TestKind::ButtonLever => Some(Self::ButtonLever),
            TestKind::EndFrameAnchor => Some(Self::EndFrameAnchor),
            _ => None,
        }
    }
    fn blocks(self) -> [u16; 2] {
        match self {
            Self::DoorTrapdoor => [
                Block::OAK_DOOR.default_state.id.as_u16(),
                Block::OAK_TRAPDOOR.default_state.id.as_u16(),
            ],
            Self::ButtonLever => [
                Block::OAK_BUTTON.default_state.id.as_u16(),
                Block::LEVER.default_state.id.as_u16(),
            ],
            Self::EndFrameAnchor => [
                Block::END_PORTAL_FRAME.default_state.id.as_u16(),
                Block::RESPAWN_ANCHOR.default_state.id.as_u16(),
            ],
        }
    }
}

const POSITIONS: [ClusterBlockPos; 2] = [
    ClusterBlockPos { x: 0, y: 80, z: 4 },
    ClusterBlockPos { x: 1, y: 80, z: 4 },
];

pub(super) struct Setup {
    request: TestWire,
    world: Arc<crate::world::World>,
    desired: BTreeMap<ClusterBlockPos, u16>,
    submitted: bool,
    observed: BTreeSet<ClusterBlockPos>,
    origin: u64,
    accepted: bool,
    failed: bool,
    publications: tokio::sync::broadcast::Receiver<
        super::super::cluster_world_delta::AdministrativePublication,
    >,
}

impl Setup {
    pub(super) fn new(server: &Server, request: TestWire, generation: u64) -> Option<Self> {
        let TestWire::Request {
            kind,
            request: id,
            origin: peer,
            ..
        } = request
        else {
            return None;
        };
        let scenario = Scenario::from_kind(kind)?;
        let mut desired = BTreeMap::new();
        for x in -4..=4 {
            for z in 1..=7 {
                for y in 79..=85 {
                    desired.insert(
                        ClusterBlockPos { x, y, z },
                        if y == 79 {
                            Block::STONE.default_state.id.as_u16()
                        } else {
                            Block::AIR.default_state.id.as_u16()
                        },
                    );
                }
            }
        }
        for (pos, state) in POSITIONS.into_iter().zip(scenario.blocks()) {
            desired.insert(pos, state);
        }
        match scenario {
            Scenario::DoorTrapdoor => {
                let mut properties = pumpkin_data::block_properties::OakDoorLikeProperties::default(
                    &Block::OAK_DOOR,
                );
                properties.half = pumpkin_data::block_properties::DoubleBlockHalf::Upper;
                desired.insert(
                    ClusterBlockPos { x: 0, y: 81, z: 4 },
                    properties.to_state_id(&Block::OAK_DOOR).as_u16(),
                );
            }
            Scenario::EndFrameAnchor => {}
            Scenario::ButtonLever => {
                for x in 0..=1 {
                    desired.insert(
                        ClusterBlockPos { x, y: 80, z: 5 },
                        Block::STONE.default_state.id.as_u16(),
                    );
                }
            }
        }
        Some(Self {
            request,
            world: arena_world(server)?,
            desired,
            submitted: false,
            observed: BTreeSet::new(),
            origin: origin(id, peer, generation, 12, 0),
            accepted: false,
            failed: false,
            publications: super::super::cluster_world_delta::subscribe_administrative_publications(
                server,
            ),
        })
    }
    pub(super) fn advance(&mut self, server: &Server) -> Option<PendingParcel> {
        if !self.submitted {
            self.submitted = submit_world(&self.world, &self.desired, self.origin);
        }
        loop {
            match self.publications.try_recv() {
                Ok(publication) if publication.origin == Some(self.origin) => {
                    self.accepted = publication.accepted;
                    self.failed |= !publication.accepted;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(_) => {
                    if !self.failed {
                        tracing::error!(
                            origin = self.origin,
                            "cluster test interaction Ground publication unavailable"
                        );
                    }
                    self.failed = true;
                    break;
                }
            }
        }
        if self.failed {
            return observed_reply(server, self.request, false);
        }
        if self.accepted {
            observe_installed_ground(server, &self.world, &self.desired, &mut self.observed);
        }
        (self.submitted && self.accepted && self.observed.len() == self.desired.len())
            .then(|| observed_reply(server, self.request, true))
            .flatten()
    }
}

pub(super) struct ClientSetup {
    scenario: Scenario,
    target: pumpkin_cluster::canonical_action::LivePlayerLease,
    loadout_token: Option<[u8; 16]>,
    nonce: u64,
    correlation: Option<[u8; 16]>,
}

impl ClientSetup {
    pub(super) fn new(server: &Server, scenario: Scenario, nonce: u64) -> Option<Self> {
        Some(Self {
            scenario,
            target: loadouts::lease_for(server, registered_players(server)?[0])?,
            loadout_token: None,
            nonce,
            correlation: None,
        })
    }
    pub(super) fn advance(
        &mut self,
        server: &Arc<Server>,
        local: ServerId,
        loadouts: &mut loadouts::Loadouts,
        pending: &mut VecDeque<PendingParcel>,
    ) -> Option<bool> {
        if loadouts::lease_for(server, self.target.gid) != Some(self.target) {
            return Some(false);
        }
        if matches!(self.scenario, Scenario::EndFrameAnchor) {
            if self.loadout_token.is_none() {
                self.loadout_token = loadouts.request(
                    server,
                    local,
                    self.target,
                    super::super::cluster_test_scenarios::FixtureLoadoutTemplate::EndFrameAnchor,
                    self.nonce,
                    pending,
                );
            }
            if !loadouts.ready(self.loadout_token) {
                return None;
            }
        }
        if self.correlation.is_none() {
            let target = registered_players(server)?[0];
            let uuid = super::super::cluster_presence::all_presence_entries(server)
                .into_iter()
                .find(|(gid, entry)| *gid == target && !entry.in_lobby)?
                .1
                .uuid;
            let world = arena_world(server)?;
            let source = crate::command::CommandSource::new(
                crate::command::CommandSender::Console,
                Arc::clone(&world),
                None,
                Default::default(),
                Default::default(),
                "cluster-e2e-interaction".to_owned(),
                pumpkin_util::text::TextComponent::text("cluster-e2e-interaction"),
                Arc::clone(server),
            );
            self.correlation = super::super::cluster_player_commands::submit(
                server,
                &source,
                target,
                uuid,
                pumpkin_cluster::player_command_control::PlayerCommandMutation::Teleport(
                    pumpkin_cluster::player_command_control::TeleportDestination::Absolute {
                        world: super::super::cluster_world_delta::world_id(&world),
                        position: [0.5, 80.0, 3.5],
                        rotation: Some([0.0, 0.0]),
                    },
                ),
            )
            .ok();
        }
        let terminal = server.cluster_player_commands.terminal(self.correlation?)?;
        loadouts.retire(self.loadout_token);
        Some(terminal.applied && terminal.action.is_some())
    }
    pub(super) fn kind(&self) -> TestKind {
        self.scenario.kind()
    }
}

pub(super) struct Observer {
    scenario: Scenario,
    input: Option<mpsc::Receiver<super::super::cluster_test_observation::GraphObservation>>,
    states: BTreeMap<ClusterBlockPos, u16>,
    actions: BTreeSet<ActionRef>,
    settled: bool,
}

impl Observer {
    pub(super) fn selected() -> Vec<Self> {
        std::env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|name| match name {
                "door-trapdoor" => Some(Scenario::DoorTrapdoor),
                "button-lever" => Some(Scenario::ButtonLever),
                "end-frame-anchor" => Some(Scenario::EndFrameAnchor),
                _ => None,
            })
            .map(|scenario| Self {
                scenario,
                input: None,
                states: BTreeMap::new(),
                actions: BTreeSet::new(),
                settled: false,
            })
            .collect()
    }
    pub(super) fn advance(&mut self, server: &Server) {
        if self.settled {
            return;
        }
        if self.input.is_none() {
            let Some(players) = registered_players(server) else {
                return;
            };
            let Some(world) = arena_world(server) else {
                return;
            };
            let world = super::super::cluster_world_delta::world_id(&world);
            let targets = POSITIONS
                .into_iter()
                .map(|pos| Target::Chunk {
                    key: pumpkin_cluster::world_delta::chunk_of_block(world, pos.x, pos.z),
                    part: ChunkPart::Block(pos),
                    inventory: None,
                })
                .collect();
            match server
                .cluster_test
                .observations
                .watches
                .watch(BTreeSet::from([ActorId::Player(players[0])]), targets)
            {
                Ok(input) => self.input = Some(input),
                Err(()) => {
                    tracing::error!(
                        scenario = self.scenario.name(),
                        "cluster test interaction observer resource failure"
                    );
                    self.settled = true;
                    return;
                }
            }
        }
        let Some(input) = self.input.as_mut() else {
            return;
        };
        while let Ok(observation) = input.try_recv() {
            if !observation.ground_applied {
                continue;
            }
            let Some(result) = observation.factual_result else {
                continue;
            };
            let mut changed = false;
            for write in result.writes {
                let Target::Chunk {
                    part: ChunkPart::Block(pos),
                    ..
                } = write.target
                else {
                    continue;
                };
                if !POSITIONS.contains(&pos) {
                    continue;
                }
                let Some(pumpkin_cluster::canonical_action::Cell {
                    value: Value::Block(state),
                    ..
                }) = write.after
                else {
                    continue;
                };
                let initial = self.scenario.blocks()
                    [POSITIONS.iter().position(|target| *target == pos).unwrap()];
                if !matches!(write.before, Some(pumpkin_cluster::canonical_action::Cell { value: Value::Block(before), .. }) if before == initial)
                {
                    continue;
                }
                if state != initial {
                    self.states.insert(pos, state);
                    changed = true;
                }
            }
            if changed {
                self.actions.insert(observation.reference);
            }
            if self.states.len() == 2 && self.actions.len() >= 2 {
                let bytes = postcard::to_allocvec(&self.states)
                    .expect("bounded interaction state serializes");
                let ground_hash = Sha256::digest(bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                let tick = self.actions.last().unwrap().tick.0;
                tracing::info!(scenario=self.scenario.name(),%ground_hash,tick,actions=self.actions.len(),peer_action_reply_protocol_variants=0u64,peer_action_reply_evidence="static_schema","cluster e2e action scenario settled");
                self.settled = true;
                self.input = None;
                return;
            }
        }
    }
}
