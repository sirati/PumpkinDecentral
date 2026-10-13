use serde::{Deserialize, Serialize};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::WorldId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandPrincipal {
    Player {
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        permission_revision: u64,
    },
    Console,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TeleportDestination {
    Absolute {
        world: WorldId,
        position: [f64; 3],
        rotation: Option<[f32; 2]>,
    },
    Player {
        gid: GlobalPlayerId,
        uuid: [u8; 16],
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerCommandTarget {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeMutation {
    SetBase { attribute: u16, value: u64 },
    AddModifier(crate::combat_action::CombatAttributeModifier),
    RemoveModifier { attribute: u16, id: String },
}

impl AttributeMutation {
    pub fn attribute(&self) -> u16 {
        match self {
            Self::SetBase { attribute, .. } | Self::RemoveModifier { attribute, .. } => *attribute,
            Self::AddModifier(modifier) => modifier.attribute,
        }
    }
    pub fn valid(&self) -> bool {
        match self {
            Self::SetBase { value, .. } => f64::from_bits(*value).is_finite(),
            Self::AddModifier(modifier) => {
                !modifier.id.is_empty()
                    && modifier.id.len() <= 256
                    && modifier.slot == 0
                    && f64::from_bits(modifier.amount).is_finite()
            }
            Self::RemoveModifier { id, .. } => !id.is_empty() && id.len() <= 256,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlayerCommandMutation {
    GameMode {
        mode: u8,
        previous: u8,
    },
    Attribute(AttributeMutation),
    Recipes {
        ids: Vec<String>,
        granted: bool,
    },
    Advancements {
        ids: Vec<String>,
        criterion: Option<String>,
        granted: bool,
        achieved_millis: u64,
    },
    Tags {
        tag: String,
        present: bool,
    },
    Reset(PlayerResetState),
    Teleport(TeleportDestination),
    Spectate {
        target: Option<PlayerCommandTarget>,
    },
    Kill,
    Summon(crate::mob_initialization::EntityAdmissionRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandTarget {
    World {
        world: crate::protocol::WorldId,
        owner: crate::realm_completion::PeerGeneration,
    },
    Player {
        gid: GlobalPlayerId,
        uuid: [u8; 16],
    },
    Entity {
        identity: crate::item_contact::ContactIdentity,
        owner: crate::realm_completion::PeerGeneration,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerResetState {
    pub health: f32,
    pub absorption: f32,
    pub food: u8,
    pub saturation: f32,
    pub gamemode: u8,
    pub clear_effects: bool,
    pub velocity: [f64; 3],
    pub fall_distance: f32,
    pub on_fire: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerCommandRequest {
    pub source: ServerId,
    pub source_incarnation: u64,
    pub principal: CommandPrincipal,
    pub permission_snapshot: Option<crate::issuer_entity_admission::CommandPermissionSnapshot>,
    pub continuation: Option<Box<crate::command_continuation::CommandContinuationProof>>,
    pub target: CommandTarget,
    pub correlation: [u8; 16],
    pub mutation: PlayerCommandMutation,
}

impl PlayerCommandRequest {
    pub fn target_owner(&self) -> ServerId {
        match &self.target {
            CommandTarget::Player { gid, .. } => gid.server,
            CommandTarget::Entity { owner, .. } | CommandTarget::World { owner, .. } => {
                owner.server
            }
        }
    }
    pub fn player_target(&self) -> Option<(GlobalPlayerId, [u8; 16])> {
        match &self.target {
            CommandTarget::Player { gid, uuid } => Some((*gid, *uuid)),
            _ => None,
        }
    }
    pub fn is_valid(&self) -> bool {
        let principal = match &self.principal {
            CommandPrincipal::Player { gid, .. } => gid.server == self.source,
            CommandPrincipal::Console => true,
        };
        principal
            && self
                .continuation
                .as_ref()
                .is_none_or(|proof| proof.matches(self))
            && match (&self.mutation, self.permission_snapshot) {
                (PlayerCommandMutation::Summon(_), Some(snapshot)) => {
                    snapshot.valid_for(&self.principal)
                }
                (PlayerCommandMutation::Summon(_), None) => false,
                (_, Some(snapshot)) => snapshot.valid_for(&self.principal),
                (_, None) => true,
            }
            && match &self.target {
                CommandTarget::Player { .. } => true,
                CommandTarget::World { world, owner } => {
                    owner.generation != 0
                        && matches!(&self.mutation, PlayerCommandMutation::Summon(request) if request.spatial.world == *world)
                }
                CommandTarget::Entity { identity, owner } => {
                    owner.generation != 0
                        && matches!(identity, crate::item_contact::ContactIdentity::Entity { birth, .. } if birth.source.generation != 0)
                }
            }
            && match &self.mutation {
                PlayerCommandMutation::GameMode { mode, previous } => {
                    self.player_target().is_some() && *mode <= 3 && *previous <= 3
                }
                PlayerCommandMutation::Kill => true,
                PlayerCommandMutation::Attribute(mutation) => mutation.valid(),
                PlayerCommandMutation::Summon(request) => request.valid(),
                PlayerCommandMutation::Recipes { ids, .. } => {
                    self.player_target().is_some()
                        && !ids.is_empty()
                        && ids.len() <= 16
                        && ids.iter().all(|id| !id.is_empty() && id.len() <= 256)
                }
                PlayerCommandMutation::Advancements { ids, criterion, .. } => {
                    self.player_target().is_some()
                        && !ids.is_empty()
                        && ids.len() <= 16
                        && ids.iter().all(|id| !id.is_empty() && id.len() <= 256)
                        && criterion
                            .as_ref()
                            .is_none_or(|name| !name.is_empty() && name.len() <= 256)
                }
                PlayerCommandMutation::Tags { tag, .. } => !tag.is_empty() && tag.len() <= 1024,
                PlayerCommandMutation::Reset(state) => {
                    state.health.is_finite()
                        && state.health >= 0.0
                        && state.absorption.is_finite()
                        && state.absorption >= 0.0
                        && state.food <= 20
                        && state.saturation.is_finite()
                        && state.saturation >= 0.0
                        && state.gamemode <= 3
                        && state.velocity.iter().all(|value| value.is_finite())
                        && state.fall_distance.is_finite()
                        && state.fall_distance >= 0.0
                }
                PlayerCommandMutation::Teleport(TeleportDestination::Absolute {
                    position,
                    rotation,
                    ..
                }) => {
                    position.iter().all(|value| value.is_finite())
                        && rotation
                            .as_ref()
                            .is_none_or(|rotation| rotation.iter().all(|value| value.is_finite()))
                }
                PlayerCommandMutation::Teleport(TeleportDestination::Player { gid, .. }) => self
                    .player_target()
                    .is_none_or(|(target, _)| *gid != target),
                PlayerCommandMutation::Spectate { target } => {
                    target.as_ref().is_none_or(|target| {
                        self.player_target()
                            .is_none_or(|(gid, _)| target.gid != gid)
                    })
                }
            }
    }
}

pub const PLAYER_COMMAND_PREFIX: &[u8; 8] = b"PCommand";

pub fn encode(request: &PlayerCommandRequest) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(request)?;
    let mut bytes = Vec::with_capacity(PLAYER_COMMAND_PREFIX.len() + payload.len());
    bytes.extend_from_slice(PLAYER_COMMAND_PREFIX);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode(bytes: &[u8]) -> Option<PlayerCommandRequest> {
    let payload = bytes.strip_prefix(PLAYER_COMMAND_PREFIX)?;
    if payload.len() > 1024 {
        return None;
    }
    let request = postcard::from_bytes::<PlayerCommandRequest>(payload).ok()?;
    request.is_valid().then_some(request)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerCommandOutcome {
    pub correlation: [u8; 16],
    pub action: Option<crate::canonical_action::ActionRef>,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerCommandObserverBatch {
    pub recipient: crate::realm_completion::PeerGeneration,
    pub tick: crate::time::TickStamp,
    pub outcomes: Vec<PlayerCommandOutcome>,
}

pub fn encode_observer(batch: &PlayerCommandObserverBatch) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(batch)?;
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.extend_from_slice(b"PCObs001");
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_observer(bytes: &[u8]) -> Option<PlayerCommandObserverBatch> {
    let payload = bytes.strip_prefix(b"PCObs001")?;
    if payload.len() > 65_536 {
        return None;
    }
    let batch = postcard::from_bytes::<PlayerCommandObserverBatch>(payload).ok()?;
    let valid = batch.outcomes.len() <= 256
        && batch.outcomes.iter().all(|outcome| {
            (!outcome.applied || outcome.action.is_some())
                && (!outcome.applied
                    || outcome
                        .action
                        .is_none_or(|action| batch.tick.distance_since(action.tick) < 0x8000))
        });
    valid.then_some(batch)
}
