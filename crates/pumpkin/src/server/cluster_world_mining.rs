use pumpkin_cluster::block_action::MiningEvidence;
use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_data::BlockStateId;
use pumpkin_data::item_stack::ItemStack;

pub(super) fn mining_evidence(
    block_state: BlockStateId,
    tool: &ItemStack,
    tool_revision: u64,
    living: &ActorLivingState,
    mining_efficiency: f64,
    submerged_multiplier: f64,
    on_ground: bool,
    can_modify_world: bool,
) -> Result<MiningEvidence, ApplyFailure> {
    let avatar = living.avatar.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let state = block_state.to_state();
    let block = block_state.to_block();
    if !mining_efficiency.is_finite()
        || mining_efficiency < 0.0
        || !submerged_multiplier.is_finite()
        || submerged_multiplier < 0.0
        || avatar.game_mode > 3
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let creative = avatar.game_mode == 1;
    let can_break = can_modify_world
        && living.combat.health_milli != 0
        && avatar.game_mode != 3
        && !state.is_air()
        && !state.is_liquid()
        && (creative || state.hardness >= 0.0);
    let mut speed = f64::from(tool.get_speed(block));
    if speed > 1.0 {
        speed += mining_efficiency;
    }
    let amplifier = |kind| {
        living
            .effects
            .iter()
            .find(|effect| effect.kind == kind)
            .map(|effect| effect.amplifier)
    };
    let haste = amplifier(u16::from(pumpkin_data::effect::StatusEffect::HASTE.id))
        .into_iter()
        .chain(amplifier(u16::from(
            pumpkin_data::effect::StatusEffect::CONDUIT_POWER.id,
        )))
        .max();
    if let Some(haste) = haste {
        speed *= 1.0 + (f64::from(haste) + 1.0) * 0.2;
    }
    if let Some(fatigue) = amplifier(u16::from(
        pumpkin_data::effect::StatusEffect::MINING_FATIGUE.id,
    )) {
        speed *= match fatigue {
            0 => 0.3,
            1 => 0.09,
            2 => 0.0027,
            _ => 0.00081,
        };
    }
    speed *= submerged_multiplier;
    if !on_ground {
        speed /= 5.0;
    }
    if !speed.is_finite() || speed < 0.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let divisor = if !state.tool_required() || tool.is_correct_for_drops(block) {
        30.0
    } else {
        100.0
    };
    let progress = if !can_break {
        0.0
    } else if creative || state.hardness == 0.0 {
        1.0
    } else {
        speed / f64::from(state.hardness) / divisor
    };
    Ok(MiningEvidence {
        block_state: block_state.as_u16(),
        tool_revision,
        speed_milli_per_tick: (progress * 1000.0).clamp(0.0, u32::MAX as f64) as u32,
        speed_fraction_millionths: ((progress * 1000.0).fract() * 1_000_000.0).clamp(0.0, 999_999.0)
            as u32,
        can_break,
        creative,
        predicted_threshold_milli: 1000,
    })
}
