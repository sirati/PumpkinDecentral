use pumpkin_cluster::canonical_action::{LivePlayerLease, PlayerKey};
use sha2::{Digest, Sha256};

use crate::command::{CommandSender, CommandSource};

use super::Server;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryScenarioCommand {
    InitialAvatarInventory,
    CreativeCloneSourceArm,
    CreativeCloneSourceRelease,
    CreativeCloneCursorArm,
    CreativeCloneCursorRelease,
    Loadout(FixtureLoadoutTemplate),
}

impl InventoryScenarioCommand {
    pub(crate) fn parse(id: &str) -> Option<Self> {
        match id {
            "initial-avatar-inventory" => Some(Self::InitialAvatarInventory),
            "creative-clone-source-arm" => Some(Self::CreativeCloneSourceArm),
            "creative-clone-source-release" => Some(Self::CreativeCloneSourceRelease),
            "creative-clone-cursor-arm" => Some(Self::CreativeCloneCursorArm),
            "creative-clone-cursor-release" => Some(Self::CreativeCloneCursorRelease),
            "loadout-arena" => Some(Self::Loadout(FixtureLoadoutTemplate::Arena)),
            "loadout-placement-conflict" => {
                Some(Self::Loadout(FixtureLoadoutTemplate::PlacementConflict))
            }
            "loadout-item-place" => Some(Self::Loadout(FixtureLoadoutTemplate::ItemPlace)),
            "loadout-end-frame-anchor" => {
                Some(Self::Loadout(FixtureLoadoutTemplate::EndFrameAnchor))
            }
            "loadout-hazard-death" => Some(Self::Loadout(FixtureLoadoutTemplate::HazardDeath)),
            "loadout-vehicle-ignition" => {
                Some(Self::Loadout(FixtureLoadoutTemplate::VehicleIgnition))
            }
            _ => None,
        }
    }

    fn scenario(self) -> &'static str {
        match self {
            Self::InitialAvatarInventory => "initial-avatar-inventory",
            Self::Loadout(template) => template.scenario(),
            Self::CreativeCloneSourceArm
            | Self::CreativeCloneSourceRelease
            | Self::CreativeCloneCursorArm
            | Self::CreativeCloneCursorRelease => "creative-clone-source-cursor",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScenarioIssuer {
    Console,
    Player(LivePlayerLease),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct InventoryScenarioRequest {
    pub command: InventoryScenarioCommand,
    pub issuer: ScenarioIssuer,
    pub target: LivePlayerLease,
    pub token: [u8; 16],
}

pub(crate) struct ScenarioRuntime {
    inventory_outcomes: crossbeam::queue::ArrayQueue<InventoryScenarioOutcome>,
    unclaimed_inventory_outcomes: crossbeam::queue::ArrayQueue<InventoryScenarioOutcome>,
    actions: crossbeam::queue::ArrayQueue<ActionScenarioRequest>,
    prepared_actions: crossbeam::queue::ArrayQueue<PreparedActionScenario>,
    inventory: crossbeam::queue::ArrayQueue<InventoryScenarioRequest>,
}

impl ScenarioRuntime {
    pub(crate) fn new() -> Self {
        Self {
            inventory: crossbeam::queue::ArrayQueue::new(64),
            actions: crossbeam::queue::ArrayQueue::new(64),
            prepared_actions: crossbeam::queue::ArrayQueue::new(64),
            inventory_outcomes: crossbeam::queue::ArrayQueue::new(128),
            unclaimed_inventory_outcomes: crossbeam::queue::ArrayQueue::new(128),
        }
    }

    pub(crate) fn pop_inventory_outcome(&self) -> Option<InventoryScenarioOutcome> {
        self.inventory_outcomes.pop()
    }

    pub(crate) fn defer_inventory_outcome(
        &self,
        outcome: InventoryScenarioOutcome,
    ) -> Result<(), InventoryScenarioOutcome> {
        self.unclaimed_inventory_outcomes.push(outcome)
    }

    pub(crate) fn pop_unclaimed_inventory_outcome(&self) -> Option<InventoryScenarioOutcome> {
        self.unclaimed_inventory_outcomes.pop()
    }

    pub(crate) fn publish_inventory_outcome(
        &self,
        outcome: InventoryScenarioOutcome,
    ) -> Result<(), InventoryScenarioOutcome> {
        if outcome.actions.is_empty()
            || outcome.actions.len() > 64
            || outcome
                .actions
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != outcome.actions.len()
        {
            return Err(outcome);
        }
        self.inventory_outcomes.push(outcome)
    }

    pub(crate) fn pop_action(&self) -> Option<ActionScenarioRequest> {
        self.actions.pop()
    }

    pub(crate) fn pop_inventory(&self) -> Option<InventoryScenarioRequest> {
        self.inventory.pop()
    }
}

#[derive(Debug)]
pub(crate) enum ScenarioSubmitError {
    IssuerUnavailable,
    TargetUnavailable,
    QueueFull,
}

pub(crate) fn submit_inventory(
    server: &Server,
    source: &CommandSource,
    command: InventoryScenarioCommand,
) -> Result<(), ScenarioSubmitError> {
    let issuer = match &source.output {
        CommandSender::Console => ScenarioIssuer::Console,
        CommandSender::Player(player) => ScenarioIssuer::Player(
            super::cluster_world_owner::player_action_lease(server, player)
                .ok_or(ScenarioSubmitError::IssuerUnavailable)?,
        ),
        _ => return Err(ScenarioSubmitError::IssuerUnavailable),
    };
    let gid = super::cluster_test::registered_players(server)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?[0];
    let presence = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .find(|(candidate, entry)| *candidate == gid && !entry.in_lobby)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?
        .1;
    let incarnation = server
        .cluster_actor_statistics
        .load()
        .get(&gid)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?
        .incarnation;
    let target = LivePlayerLease {
        key: PlayerKey(uuid::Uuid::from_bytes(presence.uuid)),
        gid,
        incarnation,
    };
    let nonce =
        std::env::var("PUMPKIN_E2E_NONCE").unwrap_or_else(|_| std::process::id().to_string());
    let mut digest = Sha256::new();
    digest.update(nonce.as_bytes());
    digest.update([0]);
    digest.update(command.scenario().as_bytes());
    digest.update(target.key.0.as_bytes());
    let mut token = [0; 16];
    token.copy_from_slice(&digest.finalize()[..16]);
    server
        .cluster_test
        .scenarios
        .inventory
        .push(InventoryScenarioRequest {
            command,
            issuer,
            target,
            token,
        })
        .map_err(|_| ScenarioSubmitError::QueueFull)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum FixtureLoadoutTemplate {
    InitialAvatar,
    Arena,
    ItemPlace,
    EndFrameAnchor,
    HazardDeath,
    VehicleIgnition,
    PlacementConflict,
    Replay,
    HealthObservation,
}

impl FixtureLoadoutTemplate {
    fn scenario(self) -> &'static str {
        match self {
            Self::InitialAvatar => "initial-avatar-inventory",
            Self::Arena => "arena",
            Self::ItemPlace => "item-place",
            Self::EndFrameAnchor => "end-frame-anchor",
            Self::HazardDeath => "hazard-death",
            Self::VehicleIgnition => "vehicle-ignition",
            Self::PlacementConflict => "placement-conflict",
            Self::Replay => "replay",
            Self::HealthObservation => "health-observation",
        }
    }

    pub(crate) fn final_gamemode(self) -> u8 {
        0
    }

    pub(crate) fn desired_cells(self) -> Option<Vec<pumpkin_cluster::inventory::InventoryCell>> {
        use pumpkin_cluster::inventory::{
            INV_ARMOR, INV_CURSOR, INV_MAIN, INV_OFFHAND, INV_SELECTED, InvLoc, InventoryCell,
        };
        use pumpkin_data::item::Item;
        use pumpkin_data::item_stack::ItemStack;
        let mut cells = (0..36)
            .map(|slot| InventoryCell::empty(InvLoc::new(INV_MAIN, slot)))
            .chain((0..4).map(|slot| InventoryCell::empty(InvLoc::new(INV_ARMOR, slot))))
            .chain([
                InventoryCell::empty(InvLoc::new(INV_OFFHAND, 0)),
                InventoryCell::empty(InvLoc::new(INV_CURSOR, 0)),
            ])
            .collect::<Vec<_>>();
        let wanted = match self {
            Self::Arena => super::cluster_test::desired_loadout()?,
            Self::ItemPlace => super::cluster_test::item_inventory_cells()?,
            _ => {
                let stacks: Vec<(u16, u8, &'static Item)> = match self {
                    Self::InitialAvatar => vec![(0, 2, &Item::STONE), (1, 3, &Item::OAK_PLANKS)],
                    Self::EndFrameAnchor => {
                        vec![(0, 1, &Item::ENDER_EYE), (1, 1, &Item::GLOWSTONE)]
                    }
                    Self::HealthObservation => Vec::new(),
                    Self::HazardDeath => vec![(0, 7, &Item::NETHERITE_INGOT)],
                    Self::Replay => vec![(0, 2, &Item::STONE), (1, 1, &Item::OAK_PLANKS)],
                    Self::PlacementConflict => vec![
                        (0, 2, &Item::STONE),
                        (1, 2, &Item::OAK_PLANKS),
                        (2, 2, &Item::DIRT),
                        (3, 1, &Item::IRON_SHOVEL),
                    ],
                    Self::VehicleIgnition => vec![
                        (0, 1, &Item::FLINT_AND_STEEL),
                        (1, 1, &Item::OAK_BOAT),
                        (2, 1, &Item::MINECART),
                        (3, 1, &Item::TNT_MINECART),
                        (4, 32, &Item::RAIL),
                        (5, 1, &Item::CHEST_MINECART),
                        (6, 1, &Item::FURNACE_MINECART),
                        (7, 1, &Item::HOPPER_MINECART),
                        (8, 8, &Item::COAL),
                    ],
                    Self::Arena | Self::ItemPlace => unreachable!(),
                };
                stacks
                    .into_iter()
                    .map(|(slot, count, item)| {
                        super::cluster_invsee::inventory_cell_from_item_stack(
                            InvLoc::new(INV_MAIN, slot),
                            &ItemStack::new(count, item),
                        )
                        .ok()
                    })
                    .collect::<Option<Vec<_>>>()?
            }
        };
        let mut wanted = wanted;
        if self == Self::InitialAvatar {
            wanted.push(
                super::cluster_invsee::inventory_cell_from_item_stack(
                    InvLoc::new(INV_ARMOR, 0),
                    &ItemStack::new(1, &Item::IRON_BOOTS),
                )
                .ok()?,
            );
            wanted.push(
                super::cluster_invsee::inventory_cell_from_item_stack(
                    InvLoc::new(INV_OFFHAND, 0),
                    &ItemStack::new(1, &Item::SHIELD),
                )
                .ok()?,
            );
        }
        let selected = wanted
            .iter()
            .find(|cell| cell.loc == InvLoc::new(INV_SELECTED, 0))
            .cloned()
            .unwrap_or(InventoryCell {
                loc: InvLoc::new(INV_SELECTED, 0),
                item: 1,
                count: 1,
                nbt: Vec::new(),
            });
        for desired in wanted
            .into_iter()
            .filter(|cell| cell.loc.inv != INV_SELECTED)
        {
            let cell = cells.iter_mut().find(|cell| cell.loc == desired.loc)?;
            *cell = desired;
        }
        cells.push(selected);
        Some(cells)
    }
}

pub(crate) fn submit_loadout(
    server: &Server,
    source: &CommandSource,
    target: LivePlayerLease,
    template: FixtureLoadoutTemplate,
    token: [u8; 16],
) -> Result<(), ScenarioSubmitError> {
    let issuer = match &source.output {
        CommandSender::Console => ScenarioIssuer::Console,
        CommandSender::Player(player) => ScenarioIssuer::Player(
            super::cluster_world_owner::player_action_lease(server, player)
                .ok_or(ScenarioSubmitError::IssuerUnavailable)?,
        ),
        _ => return Err(ScenarioSubmitError::IssuerUnavailable),
    };
    let exact = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .any(|(gid, entry)| {
            gid == target.gid && entry.uuid == *target.key.0.as_bytes() && !entry.in_lobby
        })
        && server
            .cluster_actor_statistics
            .load()
            .get(&target.gid)
            .is_some_and(|state| state.incarnation == target.incarnation);
    if !exact {
        return Err(ScenarioSubmitError::TargetUnavailable);
    }
    server
        .cluster_test
        .scenarios
        .inventory
        .push(InventoryScenarioRequest {
            command: InventoryScenarioCommand::Loadout(template),
            issuer,
            target,
            token,
        })
        .map_err(|_| ScenarioSubmitError::QueueFull)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActionScenarioCommand {
    ConflictTwoPlace,
    ConflictThreePlace,
    StaleDirtBreak,
    TargetedUndo,
    LateDependentReplay,
    CounterfactualGroundBlock,
}

impl ActionScenarioCommand {
    pub(crate) fn parse(id: &str) -> Option<Self> {
        match id {
            "conflict-two-place" => Some(Self::ConflictTwoPlace),
            "conflict-three-place" => Some(Self::ConflictThreePlace),
            "stale-dirt-break" => Some(Self::StaleDirtBreak),
            "targeted-undo" => Some(Self::TargetedUndo),
            "late-dependent-replay" => Some(Self::LateDependentReplay),
            "counterfactual-ground-block" => Some(Self::CounterfactualGroundBlock),
            _ => None,
        }
    }

    pub(crate) fn id(self) -> &'static str {
        match self {
            Self::ConflictTwoPlace => "conflict-two-place",
            Self::ConflictThreePlace => "conflict-three-place",
            Self::StaleDirtBreak => "stale-dirt-break",
            Self::TargetedUndo => "targeted-undo",
            Self::LateDependentReplay => "late-dependent-replay",
            Self::CounterfactualGroundBlock => "counterfactual-ground-block",
        }
    }

    pub(crate) fn positions(self) -> [pumpkin_cluster::protocol::BlockPos; 2] {
        [
            pumpkin_cluster::protocol::BlockPos { x: 0, y: 80, z: 4 },
            pumpkin_cluster::protocol::BlockPos { x: 17, y: 80, z: 4 },
        ]
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ActionScenarioRequest {
    pub command: ActionScenarioCommand,
    pub issuer: ScenarioIssuer,
    pub actors: Vec<LivePlayerLease>,
    pub token: [u8; 16],
}

fn exact_scenario_actor(
    server: &Server,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
) -> Result<LivePlayerLease, ScenarioSubmitError> {
    let presence = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .find(|(candidate, entry)| *candidate == gid && !entry.in_lobby)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?
        .1;
    let incarnation = server
        .cluster_actor_statistics
        .load()
        .get(&gid)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?
        .incarnation;
    Ok(LivePlayerLease {
        key: PlayerKey(uuid::Uuid::from_bytes(presence.uuid)),
        gid,
        incarnation,
    })
}

pub(crate) fn submit_action_scenario(
    server: &Server,
    source: &CommandSource,
    command: ActionScenarioCommand,
) -> Result<(), ScenarioSubmitError> {
    let issuer = match &source.output {
        CommandSender::Console => ScenarioIssuer::Console,
        CommandSender::Player(player) => ScenarioIssuer::Player(
            super::cluster_world_owner::player_action_lease(server, player)
                .ok_or(ScenarioSubmitError::IssuerUnavailable)?,
        ),
        _ => return Err(ScenarioSubmitError::IssuerUnavailable),
    };
    let players = super::cluster_test::registered_players(server)
        .ok_or(ScenarioSubmitError::TargetUnavailable)?;
    let mut actors = players
        .into_iter()
        .map(|gid| exact_scenario_actor(server, gid))
        .collect::<Result<Vec<_>, _>>()?;
    if command == ActionScenarioCommand::ConflictThreePlace {
        let name = std::env::var("PUMPKIN_CLUSTER_E2E_PLAYER_C")
            .unwrap_or_else(|_| "PumpkinE2eC".to_owned());
        let third = super::cluster_presence::all_presence_entries(server)
            .into_iter()
            .find(|(_, entry)| entry.name == name && !entry.in_lobby)
            .ok_or(ScenarioSubmitError::TargetUnavailable)?
            .0;
        let third = exact_scenario_actor(server, third)?;
        if actors.contains(&third) {
            return Err(ScenarioSubmitError::TargetUnavailable);
        }
        actors.push(third);
    }
    let nonce =
        std::env::var("PUMPKIN_E2E_NONCE").unwrap_or_else(|_| std::process::id().to_string());
    let mut digest = Sha256::new();
    digest.update(nonce.as_bytes());
    digest.update([0]);
    digest.update(command.id().as_bytes());
    for actor in &actors {
        digest.update(actor.key.0.as_bytes());
        digest.update(actor.incarnation.to_be_bytes());
    }
    let mut token = [0; 16];
    token.copy_from_slice(&digest.finalize()[..16]);
    server
        .cluster_test
        .scenarios
        .actions
        .push(ActionScenarioRequest {
            command,
            issuer,
            actors,
            token,
        })
        .map_err(|_| ScenarioSubmitError::QueueFull)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum InventoryScenarioPhase {
    Captured,
    Accepted,
    Restored,
}

#[derive(Clone, Debug)]
pub(crate) struct InventoryScenarioOutcome {
    pub token: [u8; 16],
    pub target: LivePlayerLease,
    pub template: FixtureLoadoutTemplate,
    pub phase: InventoryScenarioPhase,
    pub actions: Vec<pumpkin_cluster::canonical_action::ActionRef>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedActionScenario {
    pub token: [u8; 16],
    pub command: ActionScenarioCommand,
    pub actors: Vec<LivePlayerLease>,
    pub world: pumpkin_cluster::protocol::WorldId,
    pub positions: [pumpkin_cluster::protocol::BlockPos; 2],
    pub loadouts: Vec<super::cluster_test::FixtureLoadoutReceipt>,
    pub teleports: Vec<pumpkin_cluster::canonical_action::ActionRef>,
    pub world_actions: Vec<pumpkin_cluster::canonical_action::ActionRef>,
}

impl ScenarioRuntime {
    pub(crate) fn publish_prepared_action(
        &self,
        event: PreparedActionScenario,
    ) -> Result<(), PreparedActionScenario> {
        let count = if event.command == ActionScenarioCommand::ConflictThreePlace {
            3
        } else {
            2
        };
        let template = match event.command {
            ActionScenarioCommand::LateDependentReplay
            | ActionScenarioCommand::CounterfactualGroundBlock => FixtureLoadoutTemplate::Replay,
            _ => FixtureLoadoutTemplate::PlacementConflict,
        };
        let distinct = event
            .actors
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if event.token == [0; 16]
            || event.actors.len() != count
            || distinct.len() != count
            || event
                .actors
                .iter()
                .map(|actor| actor.gid)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != count
            || event
                .actors
                .iter()
                .map(|actor| actor.key)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != count
            || event.positions != event.command.positions()
            || event.loadouts.len() != count
            || event.teleports.len() != count
            || event.world_actions.is_empty()
            || event.world_actions.len() > 64
            || event
                .world_actions
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != event.world_actions.len()
            || event
                .teleports
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != count
            || event.actors.iter().any(|actor| {
                event
                    .loadouts
                    .iter()
                    .filter(|receipt| {
                        receipt.target == *actor
                            && receipt.template == template
                            && receipt.grant_action_count > 0
                            && receipt.grant_action_count <= 64
                    })
                    .count()
                    != 1
                    || event
                        .teleports
                        .iter()
                        .filter(|reference| {
                            reference.id.actor == pumpkin_cluster::actor::ActorId::Player(actor.gid)
                                && reference.id.incarnation == actor.incarnation
                        })
                        .count()
                        != 1
            })
        {
            return Err(event);
        }
        self.prepared_actions.push(event)
    }

    pub(crate) fn pop_prepared_action(&self) -> Option<PreparedActionScenario> {
        self.prepared_actions.pop()
    }
}
