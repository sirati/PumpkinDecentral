use pumpkin_data::data_component_impl::IDSetContent;
use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::inventory::InventoryCell;
use pumpkin_cluster::mob_continuation::MobSpecies;
use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};

pub(crate) fn write_living_nbt(
    body: &EntityBody,
    root: &mut NbtCompound,
    equipment: Option<&[InventoryCell]>,
) -> Result<(), ApplyFailure> {
    let EntityPayload::Living(living) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
    let mut output = root.clone();
    output.put_float("Health", living.combat.health_milli as f32 / 1000.0);
    output.put_float("AbsorptionAmount", living.combat.absorption_milli as f32 / 1000.0);
    output.put_short("Fire", living.combat.timing.fire_ticks.min(i16::MAX as u32) as i16);
    output.put_short("HurtTime", living.combat.timing.hurt_ticks.min(i16::MAX as u16) as i16);
    output.put_short("DeathTime", living.combat.timing.death_ticks.min(i16::MAX as u16) as i16);
    output.put_short("Air", living.environment.air_ticks);
    output.put_int("TicksFrozen", i32::from(living.environment.frozen_ticks));
    let mut attributes = Vec::new();
    let ids = living.combat.effective_attributes.keys().chain(living.permanent_attribute_modifiers.keys())
        .copied().collect::<std::collections::BTreeSet<_>>();
    for id in ids {
        let kind = pumpkin_data::attributes::Attributes::ALL.iter().find(|kind| u16::from(kind.id) == id)
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut attribute = NbtCompound::new();
        attribute.put_string("id", kind.name.to_owned());
        attribute.put_double("base", living.combat.effective_attributes.get(&id).copied().map(f64::from_bits).unwrap_or(kind.default_value));
        if let Some(modifiers) = living.permanent_attribute_modifiers.get(&id) {
            let values = modifiers.iter().map(|modifier| {
                let mut value = NbtCompound::new();
                value.put_string("id", modifier.id.clone());
                value.put_double("amount", f64::from_bits(modifier.amount));
                value.put_string("operation", match modifier.operation {
                    pumpkin_cluster::combat_action::CombatModifierOperation::Add => "add_value",
                    pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyBase => "add_multiplied_base",
                    pumpkin_cluster::combat_action::CombatModifierOperation::MultiplyTotal => "add_multiplied_total",
                }.to_owned());
                NbtTag::Compound(value)
            }).collect();
            attribute.put("modifiers", NbtTag::List(values));
        }
        attributes.push(NbtTag::Compound(attribute));
    }
    output.put("attributes", NbtTag::List(attributes));
    let mut effects = Vec::new();
    for effect in &living.effects {
        let mut value = effect_nbt(effect)?;
        if let Some(hidden) = living.hidden_effects.get(&effect.kind) {
            let mut chain = None;
            for effect in hidden.iter().rev() {
                let mut next = effect_nbt(effect)?;
                if let Some(child) = chain { next.put_compound("hidden_effect", child); }
                chain = Some(next);
            }
            if let Some(child) = chain { value.put_compound("hidden_effect", child); }
        }
        effects.push(NbtTag::Compound(value));
    }
    output.put("active_effects", NbtTag::List(effects));
    if let Some(cells) = equipment {
        let mut stacks = NbtCompound::new();
        let mut seen = std::collections::BTreeSet::new();
        for cell in cells {
            if cell.loc.inv != pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT { continue; }
            let name = ["mainhand", "offhand", "feet", "legs", "chest", "head", "body", "saddle"]
                .get(usize::from(cell.loc.slot)).ok_or(ApplyFailure::InvalidAction)?;
            if !seen.insert(cell.loc.slot) { return Err(ApplyFailure::InvalidAction); }
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            if stack.is_empty() { continue; }
            let mut value = NbtCompound::new();
            stack.write_item_stack(&mut value);
            stacks.put_compound(name, value);
        }
        output.put_compound("equipment", stacks);
    }
    if let Some(merchant) = &living.merchant {
        super::cluster_inventory_merchant::merchant_offers_to_nbt(&mut output, merchant)
            .map_err(|_| ApplyFailure::InvalidAction)?;
    }
    if let Some(mob) = &living.mob {
        let brain = &mob.brain;
        output.put_float("FallDistance", f32::from_bits(brain.fall_distance));
        output.put_bool("NoAI", brain.ai_disabled);
        output.put_bool("PersistenceRequired", brain.persistence_required);
        output.put_bool("CanPickUpLoot", brain.can_pick_up_loot);
        output.put_bool("LeftHanded", brain.left_handed);
        output.put_int("Age", mob.reproduction.growth_age);
        output.put_int("ForcedAge", mob.reproduction.forced_age);
        output.put_bool("AgeLocked", mob.reproduction.age_locked);
        output.put_int("InLove", mob.reproduction.love_ticks.min(i32::MAX as u32) as i32);
        if let Some(breeder) = mob.reproduction.breeder { output.put_uuid("LoveCause", breeder); }
        match &mob.species {
            MobSpecies::Generic | MobSpecies::Blaze { .. } => {}
            MobSpecies::Skeleton { converting, .. } => output.put_int("StrayConversionTime", converting.map_or(-1, |ticks| ticks.min(i32::MAX as u32) as i32)),
            MobSpecies::Sheep { color, sheared } => {
                output.put_byte("Color", *color as i8); output.put_bool("Sheared", *sheared);
            }
            MobSpecies::Horse { color, markings, flags, chested, temper, owner, .. } => {
                output.put_int("Variant", i32::from(*color) | (i32::from(*markings) << 8));
                horse_nbt(&mut output, *flags, *chested, *temper, *owner);
            }
            MobSpecies::Llama { variant, strength, carpet_color, flags, chested, temper, owner, .. } => {
                output.put_int("Variant", i32::from(*variant)); output.put_int("Strength", i32::from(*strength));
                if let Some(color) = carpet_color { output.put_int("DecorColor", i32::from(*color)); }
                horse_nbt(&mut output, *flags, *chested, *temper, *owner);
            }
            MobSpecies::Creeper { maximum_fuse, radius, ignited, powered, .. } => {
                output.put_short("Fuse", (*maximum_fuse).min(i16::MAX as u32) as i16);
                output.put_byte("ExplosionRadius", (*radius).min(i8::MAX as u32) as i8);
                output.put_bool("ignited", *ignited); output.put_bool("powered", *powered);
            }
            MobSpecies::Slime { size, .. } => output.put_int("Size", size.saturating_sub(1).min(i32::MAX as u32) as i32),
            MobSpecies::Enderman { carried_block, .. } => {
                if let Some(state) = carried_block {
                    output.put_int("carriedBlockState", i32::from(*state));
                }
            }
            MobSpecies::Zombie { baby, conversion, breaking_doors } => {
                output.put_bool("IsBaby", *baby); output.put_bool("CanBreakDoors", *breaking_doors);
                output.put_int("DrownedConversionTime", conversion.map_or(-1, |ticks| ticks.min(i32::MAX as u32) as i32));
            }
            MobSpecies::Bat { resting, .. } => output.put_byte("BatFlags", i8::from(*resting)),
            MobSpecies::Phantom { size, anchor, .. } => {
                output.put_int("Size", (*size).min(i32::MAX as u32) as i32);
                output.put_int("AX", anchor.x); output.put_int("AY", anchor.y); output.put_int("AZ", anchor.z);
            }
        }
    }
    *root = output;
    Ok(())
}

fn horse_nbt(root: &mut NbtCompound, flags: u8, chested: bool, temper: u32, owner: Option<uuid::Uuid>) {
    root.put_bool("Tame", flags & 2 != 0);
    root.put_bool("EatingHaystack", flags & 16 != 0);
    root.put_bool("Bred", flags & 8 != 0);
    root.put_bool("ChestedHorse", chested);
    root.put_int("Temper", temper.min(i32::MAX as u32) as i32);
    if let Some(owner) = owner { root.put_uuid("Owner", owner); }
}

fn effect_nbt(effect: &pumpkin_cluster::player_admin::EffectState) -> Result<NbtCompound, ApplyFailure> {
    let kind = pumpkin_data::effect::StatusEffect::from_id(effect.kind).ok_or(ApplyFailure::InvalidAction)?;
    let mut value = NbtCompound::new();
    value.put_string("id", kind.minecraft_name.to_owned());
    value.put_int("duration", effect.duration);
    value.put_int("amplifier", i32::from(effect.amplifier));
    value.put_bool("ambient", effect.ambient);
    value.put_bool("show_particles", effect.show_particles);
    value.put_bool("show_icon", effect.show_icon);
    Ok(value)
}
