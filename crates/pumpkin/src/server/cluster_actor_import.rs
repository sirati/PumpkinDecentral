use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_action::{ActorCombatState, ActorFoodState};
use pumpkin_data::data_component_impl::IDSetContent;
use pumpkin_nbt::compound::NbtCompound;

use super::cluster_world_actor::{ActorLivingImport, ActorLivingState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorLivingImportError {
    InvalidTags,
    InvalidAttributes,
    InvalidAttributeModifiers,
    MissingAdvancements,
    InvalidEffects,
    InvalidFoodTiming,
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
    let permanent_attribute_modifiers = read_attribute_modifiers(nbt)?;
    let tags = match nbt.and_then(|data| data.get_list("Tags")) {
        Some(values) => values.iter().map(|value| value.extract_string().map(str::to_owned))
            .collect::<Option<std::collections::BTreeSet<_>>>().ok_or(ActorLivingImportError::InvalidTags)?,
        None => Default::default(),
    };
    if !pumpkin_cluster::actor_tags::valid(&tags) { return Err(ActorLivingImportError::InvalidTags); }
    let advancements = match nbt {
        Some(data) => super::cluster_advancements::read_nbt(data).ok_or(ActorLivingImportError::MissingAdvancements)?,
        None => Default::default(),
    };
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
    let exhaustion = nbt
        .and_then(|nbt| nbt.get_float("foodExhaustionLevel"))
        .unwrap_or(0.0);
    let food_timer = nbt
        .and_then(|nbt| nbt.get_int("foodTickTimer"))
        .unwrap_or(0);
    if !exhaustion.is_finite()
        || !(0.0..=40.0).contains(&exhaustion)
        || !(0..80).contains(&food_timer)
    {
        return Err(ActorLivingImportError::InvalidFoodTiming);
    }
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
            permanent_attribute_modifiers: permanent_attribute_modifiers.clone(),
            menu: Default::default(),
            environment: pumpkin_cluster::item_action::LivingEnvironmentState {
                air_ticks: nbt.and_then(|nbt| nbt.get_short("Air")).unwrap_or(300).clamp(-20, 300),
                frozen_ticks: nbt.and_then(|nbt| nbt.get_int("TicksFrozen")).unwrap_or(0).clamp(0, 140) as u16,
            },
            tags,
            tags_revision: 0,
            advancements,
            stand: None,
            mob: None,
            merchant: None,
            simulation_tick: None,
            effects: read_effects(nbt)?,
            hidden_effects: read_hidden_effects(nbt)?,
            property_versions: std::collections::BTreeMap::new(),
            food: ActorFoodState {
                exhaustion_milli: (exhaustion * 1000.0).round() as u32,
                tick_timer: food_timer as u16,
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
                timing: pumpkin_cluster::item_action::CombatTiming {
                    death_inventory_processed: nbt.and_then(|nbt|nbt.get_bool("ClusterDeathInventoryProcessed")).unwrap_or(health == 0.0),
                    hurt_ticks: nbt
                        .and_then(|nbt| nbt.get_short("HurtTime"))
                        .unwrap_or(0)
                        .max(0) as u16,
                    fire_ticks: nbt
                        .and_then(|nbt| nbt.get_short("Fire"))
                        .unwrap_or(0)
                        .max(0) as u32,
                    death_ticks: nbt
                        .and_then(|nbt| nbt.get_short("DeathTime"))
                        .unwrap_or(0)
                        .max(0) as u16,
                    ..Default::default()
                },
                effective_attributes: read_attributes(nbt)?,
                equipment: Default::default(),
                status: Default::default(),
            },
            use_state: None,
            cooldowns: std::collections::BTreeMap::new(),
            avatar: None,
        },
    })
}

pub(crate) fn read_effects(
    nbt: Option<&NbtCompound>,
) -> Result<Vec<pumpkin_cluster::player_admin::EffectState>, ActorLivingImportError> {
    let Some(effects) = nbt.and_then(|nbt| {
        nbt.get_list("active_effects")
            .or_else(|| nbt.get_list("ActiveEffects"))
    }) else {
        return Ok(Vec::new());
    };
    if effects.len() > 256 {
        return Err(ActorLivingImportError::InvalidEffects);
    }
    let mut result = Vec::with_capacity(effects.len());
    for effect in effects {
        let effect = effect
            .extract_compound()
            .ok_or(ActorLivingImportError::InvalidEffects)?;
        let kind = if let Some(id) = effect.get_string("id") {
            pumpkin_data::effect::StatusEffect::from_minecraft_name(id)
                .map(|kind| u16::from(kind.id))
        } else {
            effect.get_byte("Id").map(|id| u16::from(id as u8))
        }
        .ok_or(ActorLivingImportError::InvalidEffects)?;
        if pumpkin_data::effect::StatusEffect::from_id(kind).is_none() {
            return Err(ActorLivingImportError::InvalidEffects);
        }
        let duration = effect
            .get_int("duration")
            .or_else(|| effect.get_int("Duration"))
            .unwrap_or(0);
        if duration < -1 {
            return Err(ActorLivingImportError::InvalidEffects);
        }
        result.push(pumpkin_cluster::player_admin::EffectState {
            kind,
            duration,
            amplifier: effect
                .get_int("amplifier")
                .and_then(|level| u8::try_from(level).ok())
                .or_else(|| effect.get_byte("Amplifier").map(|level| level as u8))
                .unwrap_or(0),
            ambient: effect
                .get_bool("ambient")
                .or_else(|| effect.get_bool("Ambient"))
                .unwrap_or(false),
            show_particles: effect
                .get_bool("show_particles")
                .or_else(|| effect.get_bool("ShowParticles"))
                .unwrap_or(true),
            show_icon: effect
                .get_bool("show_icon")
                .or_else(|| effect.get_bool("ShowIcon"))
                .unwrap_or(true),
            blend: false,
        });
    }
    result.sort_by_key(|effect| effect.kind);
    if result.windows(2).any(|pair| pair[0].kind == pair[1].kind) {
        return Err(ActorLivingImportError::InvalidEffects);
    }
    Ok(result)
}

pub(crate) fn read_hidden_effects(
    nbt: Option<&NbtCompound>,
) -> Result<
    std::collections::BTreeMap<u16, Vec<pumpkin_cluster::player_admin::EffectState>>,
    ActorLivingImportError,
> {
    let visible = read_effects(nbt)?;
    let mut hidden = std::collections::BTreeMap::new();
    let Some(tags) = nbt.and_then(|nbt| {
        nbt.get_list("active_effects")
            .or_else(|| nbt.get_list("ActiveEffects"))
    }) else {
        return Ok(hidden);
    };
    let mut count = 0;
    for tag in tags {
        let parent = tag
            .extract_compound()
            .ok_or(ActorLivingImportError::InvalidEffects)?;
        let kind = parent
            .get_string("id")
            .and_then(pumpkin_data::effect::StatusEffect::from_minecraft_name)
            .map(|kind| u16::from(kind.id))
            .or_else(|| parent.get_byte("Id").map(|id| u16::from(id as u8)))
            .ok_or(ActorLivingImportError::InvalidEffects)?;
        let mut next = parent
            .get_compound("hidden_effect")
            .or_else(|| parent.get_compound("HiddenEffect"));
        let mut chain = Vec::new();
        while let Some(compound) = next {
            count += 1;
            if count > 256 {
                return Err(ActorLivingImportError::InvalidEffects);
            }
            let child_kind = compound
                .get_string("id")
                .and_then(pumpkin_data::effect::StatusEffect::from_minecraft_name)
                .map(|kind| u16::from(kind.id))
                .or_else(|| compound.get_byte("Id").map(|id| u16::from(id as u8)))
                .unwrap_or(kind);
            if child_kind != kind
                || pumpkin_data::effect::StatusEffect::from_id(child_kind).is_none()
            {
                return Err(ActorLivingImportError::InvalidEffects);
            }
            chain.push(pumpkin_cluster::player_admin::EffectState {
                kind,
                duration: compound
                    .get_int("duration")
                    .or_else(|| compound.get_int("Duration"))
                    .unwrap_or(0),
                amplifier: compound
                    .get_int("amplifier")
                    .and_then(|value| u8::try_from(value).ok())
                    .or_else(|| compound.get_byte("Amplifier").map(|value| value as u8))
                    .unwrap_or(0),
                ambient: compound
                    .get_bool("ambient")
                    .or_else(|| compound.get_bool("Ambient"))
                    .unwrap_or(false),
                show_particles: compound
                    .get_bool("show_particles")
                    .or_else(|| compound.get_bool("ShowParticles"))
                    .unwrap_or(true),
                show_icon: compound
                    .get_bool("show_icon")
                    .or_else(|| compound.get_bool("ShowIcon"))
                    .unwrap_or(true),
                blend: false,
            });
            next = compound
                .get_compound("hidden_effect")
                .or_else(|| compound.get_compound("HiddenEffect"));
        }
        if !chain.is_empty() {
            hidden.insert(kind, chain);
        }
    }
    if !(pumpkin_cluster::active_effects::ActiveEffectSet {
        visible,
        hidden: hidden.clone(),
    })
    .valid()
    {
        return Err(ActorLivingImportError::InvalidEffects);
    }
    Ok(hidden)
}

pub(crate) fn read_attributes(nbt: Option<&NbtCompound>) -> Result<std::collections::BTreeMap<u16, u64>, ActorLivingImportError> {
    let mut values = std::collections::BTreeMap::new();
    let Some(entries) = nbt.and_then(|nbt| nbt.get_list("attributes").or_else(|| nbt.get_list("Attributes"))) else { return Ok(values); };
    if entries.len() > pumpkin_data::attributes::Attributes::ALL.len() { return Err(ActorLivingImportError::InvalidAttributes); }
    for entry in entries {
        let entry = entry.extract_compound().ok_or(ActorLivingImportError::InvalidAttributes)?;
        let name = entry.get_string("id").or_else(|| entry.get_string("Name")).ok_or(ActorLivingImportError::InvalidAttributes)?;
        let Some(attribute) = attribute_for_name(name) else { continue; };
        let base = entry.get_double("base").or_else(|| entry.get_double("Base")).ok_or(ActorLivingImportError::InvalidAttributes)?;
        if !base.is_finite() || values.insert(u16::from(attribute.id), base.to_bits()).is_some() { return Err(ActorLivingImportError::InvalidAttributes); }
    }
    Ok(values)
}

pub(crate) fn read_attribute_modifiers(nbt: Option<&NbtCompound>) -> Result<std::collections::BTreeMap<u16, Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>>, ActorLivingImportError> {
    use pumpkin_cluster::combat_action::{CombatAttributeModifier, CombatModifierOperation};
    let mut values = std::collections::BTreeMap::new();
    let Some(attributes) = nbt.and_then(|nbt| nbt.get_list("attributes").or_else(|| nbt.get_list("Attributes"))) else { return Ok(values); };
    let mut total = 0;
    for entry in attributes {
        let entry = entry.extract_compound().ok_or(ActorLivingImportError::InvalidAttributeModifiers)?;
        let name = entry.get_string("id").or_else(|| entry.get_string("Name")).ok_or(ActorLivingImportError::InvalidAttributeModifiers)?;
        let Some(attribute) = attribute_for_name(name) else { continue; };
        let Some(modifiers) = entry.get_list("modifiers").or_else(|| entry.get_list("Modifiers")) else { continue; };
        let mut parsed = Vec::new();
        for modifier in modifiers {
            total += 1;
            if total > 256 { return Err(ActorLivingImportError::InvalidAttributeModifiers); }
            let modifier = modifier.extract_compound().ok_or(ActorLivingImportError::InvalidAttributeModifiers)?;
            let id = modifier.get_string("id").map(str::to_owned).or_else(|| {
                let parts = modifier.get_int_array("UUID")?;
                let [a, b, c, d] = parts else { return None; };
                Some(uuid::Uuid::from_u128((u128::from(*a as u32) << 96) | (u128::from(*b as u32) << 64)
                    | (u128::from(*c as u32) << 32) | u128::from(*d as u32)).to_string())
            }).or_else(|| {
                let most = modifier.get_long("UUIDMost")? as u64;
                let least = modifier.get_long("UUIDLeast")? as u64;
                Some(uuid::Uuid::from_u128((u128::from(most) << 64) | u128::from(least)).to_string())
            }).ok_or(ActorLivingImportError::InvalidAttributeModifiers)?;
            let amount = modifier.get_double("amount").or_else(|| modifier.get_double("Amount"))
                .ok_or(ActorLivingImportError::InvalidAttributeModifiers)?;
            let operation = match modifier.get_string("operation") {
                Some("add_value") => CombatModifierOperation::Add,
                Some("add_multiplied_base") => CombatModifierOperation::MultiplyBase,
                Some("add_multiplied_total") => CombatModifierOperation::MultiplyTotal,
                None => match modifier.get_int("Operation") {
                    Some(0) => CombatModifierOperation::Add, Some(1) => CombatModifierOperation::MultiplyBase,
                    Some(2) => CombatModifierOperation::MultiplyTotal, _ => return Err(ActorLivingImportError::InvalidAttributeModifiers),
                },
                _ => return Err(ActorLivingImportError::InvalidAttributeModifiers),
            };
            if id.is_empty() || id.len() > 256 || !amount.is_finite()
                || parsed.iter().any(|existing: &CombatAttributeModifier| existing.id == id) {
                return Err(ActorLivingImportError::InvalidAttributeModifiers);
            }
            parsed.push(CombatAttributeModifier { attribute: u16::from(attribute.id), slot: 0, id,
                amount: amount.to_bits(), operation });
        }
        parsed.sort_by(|left, right| left.id.cmp(&right.id));
        if !parsed.is_empty() { values.insert(u16::from(attribute.id), parsed); }
    }
    Ok(values)
}

#[cfg(test)]
mod attribute_tests {
    use super::*;
    use pumpkin_nbt::tag::NbtTag;
    use pumpkin_cluster::combat_action::CombatModifierOperation;

    #[test]
    fn permanent_attribute_bases_and_modifier_operations_survive_saved_player_import() {
        let mut modifier = NbtCompound::new();
        modifier.put_string("id", "test:durable".to_owned());
        modifier.put_double("amount", 0.5);
        modifier.put_string("operation", "add_multiplied_base".to_owned());
        let mut attribute = NbtCompound::new();
        attribute.put_string("id", pumpkin_data::attributes::Attributes::MAX_HEALTH.name.to_owned());
        attribute.put_double("base", 40.0);
        attribute.put("modifiers", NbtTag::List(vec![NbtTag::Compound(modifier)]));
        let mut nbt = NbtCompound::new();
        nbt.put("attributes", NbtTag::List(vec![NbtTag::Compound(attribute)]));
        let id = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
        assert_eq!(f64::from_bits(read_attributes(Some(&nbt)).unwrap()[&id]), 40.0);
        let modifiers = read_attribute_modifiers(Some(&nbt)).unwrap();
        assert_eq!(modifiers[&id][0].id, "test:durable");
        assert_eq!(modifiers[&id][0].operation, CombatModifierOperation::MultiplyBase);
        assert_eq!(f64::from_bits(modifiers[&id][0].amount), 0.5);
    }

    #[test]
    fn malformed_permanent_attributes_fail_before_actor_admission() {
        let mut attribute = NbtCompound::new();
        attribute.put_string("id", pumpkin_data::attributes::Attributes::MAX_HEALTH.name.to_owned());
        attribute.put_double("base", f64::NAN);
        let mut nbt = NbtCompound::new();
        nbt.put("attributes", NbtTag::List(vec![NbtTag::Compound(attribute)]));
        assert!(matches!(read_attributes(Some(&nbt)), Err(ActorLivingImportError::InvalidAttributes)));
    }
}

fn attribute_for_name(name: &str) -> Option<&'static pumpkin_data::attributes::Attributes> {
    let name = name.strip_prefix("minecraft:").unwrap_or(name);
    let name = name.strip_prefix("generic.").or_else(|| name.strip_prefix("player.")).unwrap_or(name);
    let mut normalized = String::from("minecraft:");
    for character in name.chars() {
        if character.is_ascii_uppercase() { normalized.push('_'); normalized.push(character.to_ascii_lowercase()); }
        else { normalized.push(character); }
    }
    pumpkin_data::attributes::Attributes::ALL.iter().find(|attribute| attribute.name == normalized)
}
