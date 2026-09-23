use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_action::{ActorCombatState, ActorFoodState};
use pumpkin_nbt::compound::NbtCompound;

use super::cluster_world_actor::{ActorLivingImport, ActorLivingState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorLivingImportError {
    InvalidHealth,
    InvalidAbsorption,
    InvalidSaturation,
    InvalidVelocity,
}

pub(crate) fn from_player_nbt(
    gid: GlobalPlayerId,
    nbt: Option<&NbtCompound>,
) -> Result<ActorLivingImport, ActorLivingImportError> {
    let actor = ActorId::Player(gid);
    let food = nbt
        .and_then(|nbt| nbt.get_int("foodLevel"))
        .unwrap_or(20)
        .clamp(0, 20) as u8;
    let saturation = nbt
        .and_then(|nbt| nbt.get_float("foodSaturationLevel"))
        .unwrap_or(5.0);
    let health = nbt.and_then(|nbt| nbt.get_float("Health")).unwrap_or(20.0);
    let absorption = nbt
        .and_then(|nbt| nbt.get_float("AbsorptionAmount"))
        .unwrap_or(0.0);
    if !health.is_finite() || health < 0.0 || health > u32::MAX as f32 / 1000.0 {
        return Err(ActorLivingImportError::InvalidHealth);
    }
    if !absorption.is_finite() || absorption < 0.0 || absorption > u32::MAX as f32 / 1000.0 {
        return Err(ActorLivingImportError::InvalidAbsorption);
    }
    if !saturation.is_finite() || saturation < 0.0 {
        return Err(ActorLivingImportError::InvalidSaturation);
    }
    let motion = nbt.and_then(|nbt| nbt.get_list("Motion"));
    let velocity = std::array::from_fn(|axis| {
        motion
            .and_then(|motion| motion.get(axis))
            .and_then(pumpkin_nbt::tag::NbtTag::extract_double)
            .unwrap_or(0.0)
    });
    if velocity
        .iter()
        .any(|value| !value.is_finite() || value.abs() > 1_000_000.0)
    {
        return Err(ActorLivingImportError::InvalidVelocity);
    }
    Ok(ActorLivingImport {
        actor,
        state: ActorLivingState {
            food: ActorFoodState {
                actor,
                food,
                saturation_milli: (saturation * 1000.0)
                    .round()
                    .clamp(0.0, f32::from(food) * 1000.0) as u32,
                revision: 0,
            },
            combat: ActorCombatState {
                actor,
                revision: 0,
                health_milli: (health * 1000.0).round() as u32,
                absorption_milli: (absorption * 1000.0).round() as u32,
                velocity_micro: velocity.map(|value| (value * 1_000_000.0).round() as i64),
            },
            use_state: None,
            cooldowns: std::collections::HashMap::new(),
        },
    })
}
