use std::collections::BTreeMap;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{ActionId, ActionState, ApplyFailure, Cell, ChunkPart, Effect, Target, Value};
use pumpkin_cluster::living_property::{LivingProperty as P, LivingPropertyValue as V};
use pumpkin_cluster::player_admin::EffectState;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::data_component_impl::PotionContentsImpl;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_data::tag::Taggable;

pub(super) fn potion_effects(stack: &ItemStack) -> Result<Vec<EffectState>, ApplyFailure> {
    let Some(contents) = stack.get_data_component::<PotionContentsImpl>() else { return Ok(Vec::new()); };
    let mut effects = Vec::new();
    if let Some(id) = contents.potion_id {
        let potion = u8::try_from(id).ok().and_then(pumpkin_data::potion::Potion::from_id).ok_or(ApplyFailure::InvalidAction)?;
        effects.extend(potion.effects.iter().map(|effect| EffectState { kind: u16::from(effect.effect_type.id),
            duration: effect.duration, amplifier: effect.amplifier, ambient: effect.ambient, show_particles: effect.show_particles,
            show_icon: effect.show_icon, blend: effect.blend }));
    }
    for effect in &contents.custom_effects {
        let kind = pumpkin_data::effect::StatusEffect::from_minecraft_name(&effect.effect_id).ok_or(ApplyFailure::InvalidAction)?;
        effects.push(EffectState { kind: u16::from(kind.id), duration: effect.duration,
            amplifier: u8::try_from(effect.amplifier).map_err(|_| ApplyFailure::InvalidAction)?, ambient: effect.ambient,
            show_particles: effect.show_particles, show_icon: effect.show_icon, blend: false });
    }
    if effects.len() > 256 { return Err(ApplyFailure::InvalidAction); }
    Ok(effects)
}

pub(super) fn apply<S: ActionState>(context: &S, id: ActionId, tick: TickStamp, source: ActorId,
    target: Target, effects: &[EffectState], intensity: f64, duration_scale: f64, minimum_duration: i32,
    max_health_attribute: u16, attribute_ids: &[u16], values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    if !intensity.is_finite() || !(0.0..=1.0).contains(&intensity) || !duration_scale.is_finite() || !(0.0..=1.0).contains(&duration_scale) {
        return Err(ApplyFailure::InvalidAction);
    }
    let Target::Chunk { key, part, inventory: None } = target else { return Err(ApplyFailure::Type(target)); };
    if values.get(&target).is_some_and(|cell| matches!(&cell.value, Value::Entity(body)
        if !matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(living) if living.stand.is_none()))) {
        return Ok(Vec::new());
    }
    let actor = match part { ChunkPart::Player(player) => ActorId::Player(player), ChunkPart::Entity(entity) => ActorId::Entity(entity), _ => return Err(ApplyFailure::Type(target)) };
    let property = |property| Target::Chunk { key, part: ChunkPart::LivingProperty { actor, property }, inventory: None };
    let Some(Value::LivingProperty(V::StatusEffects(active))) = values.get(&property(P::StatusEffects)).map(|cell| &cell.value) else { return Err(ApplyFailure::Type(property(P::StatusEffects))); };
    let mut active = active.clone();
    let kind = values.get(&target).and_then(|cell| match &cell.value {
        Value::Entity(body) => pumpkin_data::entity::EntityType::from_raw(body.kind), _ => None,
    });
    let inverted = kind.is_some_and(|kind| kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_INVERTED_HEALING_AND_HARM));
    let ignores_regeneration = kind.is_some_and(|kind| kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_IGNORES_POISON_AND_REGEN));
    let mut projected = values.clone();
    let mut deltas = Vec::new();
    for effect in effects {
        let healing = effect.kind == u16::from(pumpkin_data::effect::StatusEffect::INSTANT_HEALTH.id);
        let harming = effect.kind == u16::from(pumpkin_data::effect::StatusEffect::INSTANT_DAMAGE.id);
        if healing || harming {
            let healing = healing != inverted;
            let base = if healing { 4000_u32 } else { 6000_u32 };
            let multiplier = 1_u32.checked_shl(u32::from(effect.amplifier)).ok_or(ApplyFailure::InvalidAction)?;
            let amount = (f64::from(base.checked_mul(multiplier).ok_or(ApplyFailure::InvalidAction)?) * intensity).round() as u32;
            if amount == 0 { continue; }
            for mut delta in pumpkin_cluster::combat_action::derive_instant_health(id, target, actor, tick, healing, amount, max_health_attribute, &projected)? {
                if let Effect::Put { target, value } = &mut delta {
                    if matches!(target, Target::Chunk { part: ChunkPart::LivingProperty { property: P::LastAttacker, .. }, .. }) {
                        *value = Value::LivingProperty(V::Actor(Some(source)));
                    }
                    let revision = projected.get(target).ok_or(ApplyFailure::Condition(*target))?.revision;
                    projected.insert(*target, Cell { revision, value: value.clone() });
                }
                deltas.push(delta);
            }
        } else {
            if ignores_regeneration && [pumpkin_data::effect::StatusEffect::REGENERATION.id, pumpkin_data::effect::StatusEffect::POISON.id]
                .into_iter().any(|kind| effect.kind == u16::from(kind)) { continue; }
            let mut effect = effect.clone();
            if effect.duration != -1 { effect.duration = (f64::from(effect.duration) * duration_scale * intensity).round() as i32; }
            if effect.duration != -1 && effect.duration < minimum_duration { continue; }
            if !active.insert(effect) { return Err(ApplyFailure::InvalidAction); }
        }
    }
    pumpkin_cluster::canonical_inventory::append_status_effects(&projected, &mut deltas, target, &active, attribute_ids, context)?;
    Ok(deltas)
}
