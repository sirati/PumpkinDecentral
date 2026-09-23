use serde::{Deserialize, Serialize};

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{ChunkKey, WorldId};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectState {
    pub kind: u16,
    pub duration: i32,
    pub amplifier: u8,
    pub ambient: bool,
    pub show_particles: bool,
    pub show_icon: bool,
    pub blend: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeState {
    pub world: WorldId,
    pub position: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub velocity: [f64; 3],
    pub health: f32,
    pub absorption: f32,
    pub food: u8,
    pub saturation: f32,
    pub gamemode: u8,
    pub effects: Vec<EffectState>,
    pub fall_distance: f32,
    pub on_fire: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerLivingAdministrativeState {
    pub health: f32,
    pub absorption: f32,
    pub food: u8,
    pub saturation: f32,
    pub gamemode: u8,
    pub effects: Vec<EffectState>,
    pub fall_distance: f32,
    pub on_fire: bool,
}

impl PlayerAdministrativeState {
    #[must_use]
    pub fn living(&self) -> PlayerLivingAdministrativeState {
        PlayerLivingAdministrativeState {
            health: self.health,
            absorption: self.absorption,
            food: self.food,
            saturation: self.saturation,
            gamemode: self.gamemode,
            effects: self.effects.clone(),
            fall_distance: self.fall_distance,
            on_fire: self.on_fire,
        }
    }

    pub fn set_living(&mut self, living: PlayerLivingAdministrativeState) {
        self.health = living.health;
        self.absorption = living.absorption;
        self.food = living.food;
        self.saturation = living.saturation;
        self.gamemode = living.gamemode;
        self.effects = living.effects;
        self.fall_distance = living.fall_distance;
        self.on_fire = living.on_fire;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeAction {
    pub target: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub affected: Vec<ChunkKey>,
    pub expected: PlayerAdministrativeState,
    pub forward: PlayerAdministrativeState,
    pub undo: PlayerAdministrativeState,
    pub origin: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlayerAdministrativeMutation {
    Reset {
        health: f32,
        absorption: f32,
        food: u8,
        saturation: f32,
        gamemode: u8,
        clear_effects: bool,
        velocity: [f64; 3],
        fall_distance: f32,
        on_fire: bool,
    },
    Teleport {
        world: WorldId,
        position: [f64; 3],
        yaw: f32,
        pitch: f32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeIntent {
    pub target: GlobalPlayerId,
    pub mutation: PlayerAdministrativeMutation,
    pub origin: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativePublication {
    pub action: crate::completion::ActionKey,
    pub tick: TickStamp,
    pub target: GlobalPlayerId,
    pub origin: Option<u64>,
    pub accepted: bool,
    pub state: PlayerAdministrativeState,
}

#[must_use]
pub fn capture_action(
    target: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    affected: Vec<ChunkKey>,
    expected: PlayerAdministrativeState,
    mutation: &PlayerAdministrativeMutation,
    origin: Option<u64>,
) -> Option<PlayerAdministrativeAction> {
    if affected.is_empty() {
        return None;
    }
    let mut forward = expected.clone();
    match mutation {
        PlayerAdministrativeMutation::Reset {
            health,
            absorption,
            food,
            saturation,
            gamemode,
            clear_effects,
            velocity,
            fall_distance,
            on_fire,
        } => {
            forward.health = *health;
            forward.absorption = *absorption;
            forward.food = *food;
            forward.saturation = *saturation;
            forward.gamemode = *gamemode;
            if *clear_effects {
                forward.effects.clear();
            }
            forward.velocity = *velocity;
            forward.fall_distance = *fall_distance;
            forward.on_fire = *on_fire;
        }
        PlayerAdministrativeMutation::Teleport {
            world,
            position,
            yaw,
            pitch,
        } => {
            forward.world = *world;
            forward.position = *position;
            forward.yaw = *yaw;
            forward.pitch = *pitch;
        }
    }
    Some(PlayerAdministrativeAction {
        target,
        seq,
        tick,
        affected,
        expected: expected.clone(),
        forward,
        undo: expected,
        origin,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn state() -> PlayerAdministrativeState {
        PlayerAdministrativeState {
            world: WorldId::new(2, 9),
            position: [4.0, 70.0, -3.0],
            yaw: 12.0,
            pitch: -5.0,
            velocity: [1.0, 2.0, 3.0],
            health: 7.0,
            absorption: 2.0,
            food: 6,
            saturation: 1.0,
            gamemode: 3,
            effects: vec![EffectState {
                kind: 4,
                duration: 80,
                amplifier: 2,
                ambient: true,
                show_particles: false,
                show_icon: true,
                blend: false,
            }],
            fall_distance: 8.0,
            on_fire: true,
        }
    }

    #[test]
    fn reset_preserves_location_and_carries_exact_undo() {
        let before = state();
        let action = capture_action(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            PlayerSeq(3),
            TickStamp(11),
            vec![ChunkKey::new(before.world, 0, 0)],
            before.clone(),
            &PlayerAdministrativeMutation::Reset {
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
            Some(8),
        )
        .unwrap();
        assert_eq!(action.forward.world, before.world);
        assert_eq!(action.forward.position, before.position);
        assert!(action.forward.effects.is_empty());
        assert_eq!(action.undo, before);
    }

    #[test]
    fn teleport_preserves_living_state() {
        let before = state();
        let target_world = WorldId::new(4, 12);
        let action = capture_action(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            PlayerSeq(4),
            TickStamp(12),
            vec![
                ChunkKey::new(before.world, 0, 0),
                ChunkKey::new(target_world, 2, 3),
            ],
            before.clone(),
            &PlayerAdministrativeMutation::Teleport {
                world: target_world,
                position: [32.5, 80.0, 48.5],
                yaw: 90.0,
                pitch: 0.0,
            },
            Some(9),
        )
        .unwrap();
        assert_eq!(action.forward.health, before.health);
        assert_eq!(action.forward.effects, before.effects);
        assert_eq!(action.undo, before);
    }
}
