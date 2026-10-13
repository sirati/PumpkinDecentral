pub(super) fn modifiers(
    effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
) -> Result<
    Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_cluster::combat_action::{CombatAttributeModifier, CombatModifierOperation};
    use pumpkin_data::data_component_impl::{IDSetContent, Operation};
    if !effects.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut result = Vec::new();
    for effect in effects.visible.iter().filter(|effect| effect.duration != 0) {
        let registry = pumpkin_data::effect::StatusEffect::from_id(effect.kind)
            .ok_or(ApplyFailure::InvalidAction)?;
        for modifier in registry.attribute_modifiers {
            let amount = modifier.base_value * (f64::from(effect.amplifier) + 1.0);
            if !amount.is_finite() {
                return Err(ApplyFailure::InvalidAction);
            }
            result.push(CombatAttributeModifier {
                attribute: u16::from(modifier.attribute.id),
                slot: 7,
                id: modifier.id.to_string(),
                amount: amount.to_bits(),
                operation: match modifier.operation {
                    Operation::AddValue => CombatModifierOperation::Add,
                    Operation::AddMultipliedBase => CombatModifierOperation::MultiplyBase,
                    Operation::AddMultipliedTotal => CombatModifierOperation::MultiplyTotal,
                },
            });
        }
    }
    result.sort_by(|a, b| (a.attribute, &a.id, a.slot).cmp(&(b.attribute, &b.id, b.slot)));
    if result.len() > 256 {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(result)
}
