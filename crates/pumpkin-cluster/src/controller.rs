use serde::{Deserialize, Serialize};
use std::num::NonZero;

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ClientClaimedState {
    pub actor: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub position: [f64; 3],
    pub velocity: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub on_ground: bool,
    pub input_flags: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EffectiveControllerState {
    pub actor: GlobalPlayerId,
    pub source_seq: PlayerSeq,
    pub tick: TickStamp,
    pub position: [f64; 3],
    pub velocity: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub on_ground: bool,
    pub input_flags: u64,
}

impl From<ClientClaimedState> for EffectiveControllerState {
    fn from(claimed: ClientClaimedState) -> Self {
        Self {
            actor: claimed.actor,
            source_seq: claimed.seq,
            tick: claimed.tick,
            position: claimed.position,
            velocity: claimed.velocity,
            yaw: claimed.yaw,
            pitch: claimed.pitch,
            on_ground: claimed.on_ground,
            input_flags: claimed.input_flags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControllerDecision {
    Accepted,
    Modified(u16),
    Rejected(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ControllerFrame {
    pub claimed: ClientClaimedState,
    pub effective: EffectiveControllerState,
    pub decision: ControllerDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerMovementStatistic {
    Unchecked,
    Cancelled,
    Amount(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControllerActorSource {
    Initial,
    Client,
    AcceptedAdministrative(crate::completion::ActionKey, TickStamp),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ControllerActorIntent {
    pub actor: GlobalPlayerId,
    pub spatial_epoch: Option<crate::canonical_action::SpatialEpoch>,
    pub player_key: uuid::Uuid,
    pub owner_install_generation: u64,
    pub head_yaw: f32,
    pub source: ControllerActorSource,
    pub world: crate::protocol::WorldId,
    pub tick: TickStamp,
    pub frame: ControllerFrame,
    pub position_changed: bool,
    pub velocity_changed: bool,
    pub rotation_changed: bool,
    pub ground_changed: bool,
    pub input_changed: bool,
    pub sneaking: Option<bool>,
    pub sprinting: Option<bool>,
    pub crawling: Option<bool>,
    pub swimming: Option<bool>,
    pub gliding: Option<bool>,
    pub flying: Option<bool>,
    pub effective_flying: bool,
    pub gamemode: Option<u8>,
    pub movement_statistic: ControllerMovementStatistic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewIntent {
    pub actor: GlobalPlayerId,
    pub owner_install_generation: u64,
    pub world: crate::protocol::WorldId,
    pub center: crate::protocol::ChunkAddr,
    pub radius: NonZero<u8>,
    pub spectator_generates: bool,
    pub simulation_distance: u8,
    pub chunk_send_epoch: u32,
    pub tick: TickStamp,
}

pub trait ControllerFilter {
    fn filter(
        &mut self,
        claimed: &ClientClaimedState,
        effective: &mut EffectiveControllerState,
    ) -> ControllerDecision;
}

#[must_use]
pub fn apply_filters(
    claimed: ClientClaimedState,
    filters: &mut [&mut dyn ControllerFilter],
) -> ControllerFrame {
    let mut effective = EffectiveControllerState::from(claimed);
    let mut decision = ControllerDecision::Accepted;
    for filter in filters {
        match filter.filter(&claimed, &mut effective) {
            ControllerDecision::Accepted => {}
            modified @ ControllerDecision::Modified(_) => decision = modified,
            rejected @ ControllerDecision::Rejected(_) => {
                decision = rejected;
                break;
            }
        }
    }
    ControllerFrame {
        claimed,
        effective,
        decision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    struct ClampX;

    impl ControllerFilter for ClampX {
        fn filter(
            &mut self,
            _claimed: &ClientClaimedState,
            effective: &mut EffectiveControllerState,
        ) -> ControllerDecision {
            if effective.position[0] <= 4.0 {
                return ControllerDecision::Accepted;
            }
            effective.position[0] = 4.0;
            ControllerDecision::Modified(1)
        }
    }

    #[test]
    fn filtered_state_diverges_without_destroying_claimed_evidence() {
        let claimed = ClientClaimedState {
            actor: GlobalPlayerId::new(ServerId(2), PlayerSlot(9)),
            seq: PlayerSeq(7),
            tick: TickStamp(11),
            position: [40.0, 64.0, 2.0],
            velocity: [36.0, 0.0, 0.0],
            yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
            input_flags: 0,
        };
        let mut clamp = ClampX;
        let mut filters: [&mut dyn ControllerFilter; 1] = [&mut clamp];
        let frame = apply_filters(claimed, &mut filters);

        assert_eq!(frame.claimed.position[0], 40.0);
        assert_eq!(frame.effective.position[0], 4.0);
        assert_eq!(frame.decision, ControllerDecision::Modified(1));
        assert_eq!(frame.effective.source_seq, frame.claimed.seq);
    }
}
