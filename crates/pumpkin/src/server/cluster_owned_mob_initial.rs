use std::collections::BTreeSet;

use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload, EntitySpatial};
use pumpkin_cluster::entity_origin::EntityOriginIdentity;
use pumpkin_cluster::inventory::{InvLoc, InventoryCell};
use pumpkin_cluster::mob_continuation::{HorseSpeciesKind, MobBrain, MobContinuation, MobReproduction, MobSpecies, NavigationKind};
use pumpkin_cluster::mob_initialization::MobInitialization;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};

pub(crate) fn parse_initialization(kind: &EntityType, spatial: &EntitySpatial, root: &NbtCompound)
    -> Result<MobInitialization, ApplyFailure> {
    if !kind.mob { return Err(ApplyFailure::InvalidAction); }
    super::cluster_owned_mob_disk::validate_initialization_fields(kind, root)?;
    parse_living_initialization(kind, spatial, root)
}

pub(crate) fn parse_living_initialization(kind: &EntityType, spatial: &EntitySpatial, root: &NbtCompound)
    -> Result<MobInitialization, ApplyFailure> {
    if !kind.mob && kind.resource_name != "armor_stand" { return Err(ApplyFailure::InvalidAction); }
    let tags = root.get_list("Tags").map(|tags| tags.iter().map(|tag| match tag {
        NbtTag::String(tag) if tag.len() <= 256 => Ok(tag.to_string()),
        _ => Err(ApplyFailure::InvalidAction),
    }).collect::<Result<BTreeSet<_>, _>>()).transpose()?.unwrap_or_default();
    let health_milli = root.get_float("Health").map(|health| {
        if !health.is_finite() || health < 0.0 || f64::from(health) * 1000.0 > f64::from(u32::MAX) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok((f64::from(health) * 1000.0).round() as u32)
    }).transpose()?;
    let merchant = match kind.resource_name {
        "villager" => super::cluster_inventory_merchant::merchant_offers_from_nbt(root,
            pumpkin_cluster::canonical_merchant::MerchantScheme::Villager),
        "wandering_trader" => super::cluster_inventory_merchant::merchant_offers_from_nbt(root,
            pumpkin_cluster::canonical_merchant::MerchantScheme::WanderingTrader),
        _ => Ok(None),
    }.map_err(|_| ApplyFailure::InvalidAction)?;
    let mut equipment = Vec::new();
    if let Some(compound) = root.get_compound("equipment") {
        use pumpkin_data::data_component_impl::EquipmentSlot;
        for (slot, role, name) in [(0, EquipmentSlot::MAIN_HAND, "mainhand"),
            (1, EquipmentSlot::OFF_HAND, "offhand"), (2, EquipmentSlot::FEET, "feet"),
            (3, EquipmentSlot::LEGS, "legs"), (4, EquipmentSlot::CHEST, "chest"),
            (5, EquipmentSlot::HEAD, "head"), (6, EquipmentSlot::BODY, "body"),
            (7, EquipmentSlot::SADDLE, "saddle")] {
            let Some(item) = compound.get_compound(name) else { continue; };
            if item.get_int("count").is_none_or(|count| !(0..=99).contains(&count)) {
                return Err(ApplyFailure::InvalidAction);
            }
            let stack = pumpkin_data::item_stack::ItemStack::read_item_stack(item)
                .ok_or(ApplyFailure::InvalidAction)?;
            if stack.is_empty() { continue; }
            if slot >= 2 && (!crate::entity::mob::equipment::is_equippable_in_slot(kind, &stack, &role)
                || stack.item_count > 1) { return Err(ApplyFailure::InvalidAction); }
            let loc = InvLoc::new(pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT, slot);
            equipment.push(super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack)
                .map_err(|_| ApplyFailure::InvalidAction)?);
        }
    }
    let initialization = MobInitialization {
        attributes: super::cluster_actor_import::read_attributes(Some(root)).map_err(|_| ApplyFailure::InvalidAction)?,
        attribute_modifiers: super::cluster_actor_import::read_attribute_modifiers(Some(root)).map_err(|_| ApplyFailure::InvalidAction)?,
        effects: pumpkin_cluster::active_effects::ActiveEffectSet {
            visible: super::cluster_actor_import::read_effects(Some(root)).map_err(|_| ApplyFailure::InvalidAction)?,
            hidden: super::cluster_actor_import::read_hidden_effects(Some(root)).map_err(|_| ApplyFailure::InvalidAction)?,
        }, species: species(kind, root, spatial)?,
        reproduction: MobReproduction { growth_age: root.get_int("Age").unwrap_or(0),
            forced_age: root.get_int("ForcedAge").unwrap_or(0),
            age_locked: root.get_bool("AgeLocked").unwrap_or(false),
            love_ticks: unsigned(root.get_int("InLove").unwrap_or(0))?,
            breeder: root.get_uuid("LoveCause"), ..Default::default() },
        tags, health_milli, merchant, equipment,
        fall_distance: root.get_float("FallDistance").unwrap_or(0.0).to_bits(),
        ai_disabled: root.get_bool("NoAI").unwrap_or(false),
        persistence_required: root.get_bool("PersistenceRequired").unwrap_or(false),
        can_pick_up_loot: root.get_bool("CanPickUpLoot").unwrap_or(false),
        left_handed: root.get_bool("LeftHanded").unwrap_or(false) };
    if !initialization.valid() { return Err(ApplyFailure::InvalidAction); }
    Ok(initialization)
}

pub(crate) fn prepare_typed_body(identity: &EntityOriginIdentity, reference: ActionRef,
    owner: PeerGeneration, kind: &EntityType, spatial: EntitySpatial, initial: &MobInitialization)
    -> Result<(EntityBody, Vec<InventoryCell>), ApplyFailure> {
    if identity.source != owner || identity.id.origin != owner.server
        || identity.id.generation != owner.generation || !kind.mob || !initial.valid()
        || !super::cluster_owned_mob_metadata::species_matches_kind(&initial.species, kind.id) {
        return Err(ApplyFailure::InvalidAction);
    }
    let position = spatial.position.map(f64::from_bits);
    if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0) {
        return Err(ApplyFailure::InvalidAction);
    }
    let chunk = pumpkin_cluster::protocol::ChunkKey::new(spatial.world,
        (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    if !spatial.belongs_to(chunk) { return Err(ApplyFailure::InvalidAction); }
    let raw = identity.uuid.as_u128();
    let mut brain = MobBrain::at_birth([raw as u64, (raw >> 64) as u64], position,
        f32::from_bits(spatial.yaw), navigation(kind)).ok_or(ApplyFailure::InvalidAction)?;
    brain.fall_distance = initial.fall_distance;
    brain.ai_disabled = initial.ai_disabled;
    brain.persistence_required = initial.persistence_required;
    brain.can_pick_up_loot = initial.can_pick_up_loot;
    brain.left_handed = initial.left_handed;
    brain.selector_phase = (identity.id.local_id & 1) as u8;
    if let Some(goals) = super::cluster_owned_mob_goals::initial(kind) { brain.goals = goals; }
    let continuation = MobContinuation { brain, species: initial.species.clone(),
        reproduction: initial.reproduction.clone() };
    let mut body = super::cluster_owned_mob_birth::prepare_body(super::cluster_owned_mob_birth::MobBirthInput {
        tags: initial.tags.clone(), entity: identity.id, action: reference.id, owner, tick: reference.tick,
        uuid: identity.uuid, kind: kind.clone(), spatial, continuation,
    }, chunk)?;
    let supplied = apply_living_initialization(&mut body, initial)?;
    if initial.merchant.is_none() && kind.resource_name == "wandering_trader" {
        let offers = super::cluster_inventory_merchant::initial_merchant_offers(
            pumpkin_cluster::canonical_merchant::MerchantScheme::WanderingTrader, None, identity.uuid)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let EntityPayload::Living(living) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
        living.merchant = offers;
    }
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, identity.id) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok((body, supplied))
}

pub(crate) fn prepare_initial_body(identity: &EntityOriginIdentity, reference: ActionRef,
    owner: PeerGeneration, kind: &EntityType, spatial: EntitySpatial, root: &NbtCompound)
    -> Result<(EntityBody, Vec<InventoryCell>), ApplyFailure> {
    let initial = parse_initialization(kind, &spatial, root)?;
    prepare_typed_body(identity, reference, owner, kind, spatial, &initial)
}
fn unsigned(value: i32) -> Result<u32, ApplyFailure> {
    u32::try_from(value).map_err(|_| ApplyFailure::InvalidAction)
}

fn navigation(kind: &EntityType) -> NavigationKind {
    match kind.resource_name {
        "bat" | "bee" | "blaze" | "ghast" | "phantom" | "allay" | "vex" | "parrot" => NavigationKind::Fly,
        "cod" | "salmon" | "pufferfish" | "tropical_fish" | "squid" | "glow_squid" | "dolphin" | "guardian" | "elder_guardian" => NavigationKind::Swim,
        "axolotl" | "frog" | "turtle" | "drowned" => NavigationKind::Amphibious,
        _ => NavigationKind::Walk,
    }
}

fn species(kind: &EntityType, root: &NbtCompound, spatial: &EntitySpatial) -> Result<MobSpecies, ApplyFailure> {
    let byte = |key, default| u8::try_from(root.get_byte(key).unwrap_or(default)).map_err(|_| ApplyFailure::InvalidAction);
    let position = spatial.position.map(f64::from_bits);
    let at = pumpkin_cluster::protocol::BlockPos { x: position[0].floor() as i32,
        y: position[1].floor() as i32, z: position[2].floor() as i32 };
    let zero = 0.0_f32.to_bits();
    Ok(match kind.resource_name {
        "horse" | "donkey" | "mule" | "skeleton_horse" | "zombie_horse" => {
            let variant = unsigned(root.get_int("Variant").unwrap_or(0))?;
            if variant & !0xffff != 0 { return Err(ApplyFailure::InvalidAction); }
            MobSpecies::Horse { kind: match kind.resource_name { "horse" => HorseSpeciesKind::Horse,
                "donkey" => HorseSpeciesKind::Donkey, "mule" => HorseSpeciesKind::Mule,
                "skeleton_horse" => HorseSpeciesKind::Skeleton, _ => HorseSpeciesKind::Zombie },
                color: (variant & 255) as u8, markings: ((variant >> 8) & 255) as u8,
                flags: (if root.get_bool("Tame").unwrap_or(false) { 2 } else { 0 })
                    | (if root.get_bool("EatingHaystack").unwrap_or(false) { 16 } else { 0 })
                    | (if root.get_bool("Bred").unwrap_or(false) { 8 } else { 0 }),
                chested: root.get_bool("ChestedHorse").unwrap_or(false),
                temper: unsigned(root.get_int("Temper").unwrap_or(0))?, owner: root.get_uuid("Owner") }
        }
        "llama" | "trader_llama" => MobSpecies::Llama { trader: kind.resource_name == "trader_llama",
            variant: u8::try_from(root.get_int("Variant").unwrap_or(0)).map_err(|_| ApplyFailure::InvalidAction)?,
            strength: u8::try_from(root.get_int("Strength").unwrap_or(1)).map_err(|_| ApplyFailure::InvalidAction)?,
            carpet_color: root.get_int("DecorColor").map(u8::try_from).transpose().map_err(|_| ApplyFailure::InvalidAction)?, flags: (if root.get_bool("Tame").unwrap_or(false) { 2 } else { 0 })
                    | (if root.get_bool("EatingHaystack").unwrap_or(false) { 16 } else { 0 })
                    | (if root.get_bool("Bred").unwrap_or(false) { 8 } else { 0 }),
            chested: root.get_bool("ChestedHorse").unwrap_or(false), temper: unsigned(root.get_int("Temper").unwrap_or(0))?,
            owner: root.get_uuid("Owner") },
        "sheep" => MobSpecies::Sheep { color: byte("Color", 0)?, sheared: root.get_bool("Sheared").unwrap_or(false) },
        "creeper" => MobSpecies::Creeper { fuse: 0, previous_fuse: 0,
            maximum_fuse: u32::try_from(root.get_short("Fuse").unwrap_or(30)).map_err(|_| ApplyFailure::InvalidAction)?,
            radius: u32::from(byte("ExplosionRadius", 3)?), powered: root.get_bool("powered").unwrap_or(false),
            ignited: root.get_bool("ignited").unwrap_or(false) },
        "slime" | "magma_cube" => MobSpecies::Slime { size: unsigned(root.get_int("Size").unwrap_or(0))?.checked_add(1)
            .ok_or(ApplyFailure::InvalidAction)?, jump_delay: 0, squish: zero, previous_squish: zero, target_squish: zero },
        "enderman" => MobSpecies::Enderman { carried_block: root.get_int("carriedBlockState").map(|value| u16::try_from(value).ok().and_then(pumpkin_data::BlockStateId::new).map(|state| state.as_u16()).ok_or(ApplyFailure::InvalidAction)).transpose()?, teleport_cooldown: 0, aggressive: false, stared_at: false },
        "zombie" | "husk" | "drowned" | "zombie_villager" | "zombified_piglin" => MobSpecies::Zombie {
            baby: root.get_bool("IsBaby").unwrap_or(false), conversion: optional_timer(root.get_int("DrownedConversionTime"))?, breaking_doors: root.get_bool("CanBreakDoors").unwrap_or(false) },
        "skeleton" | "stray" | "bogged" | "parched" | "wither_skeleton" => MobSpecies::Skeleton { ranged_cooldown: 0, converting: optional_timer(root.get_int("StrayConversionTime"))? },
        "bat" => MobSpecies::Bat { resting: byte("BatFlags", 0)? & 1 != 0, target: None },
        "phantom" => MobSpecies::Phantom { size: unsigned(root.get_int("Size").unwrap_or(0))?,
            anchor: pumpkin_cluster::protocol::BlockPos { x: root.get_int("AX").unwrap_or(at.x),
                y: root.get_int("AY").unwrap_or(at.y), z: root.get_int("AZ").unwrap_or(at.z) }, phase: 0, target: spatial.position, sweep_ticks: 0 },
        "blaze" => MobSpecies::Blaze { attack_step: 0, attack_time: 0, unseen_ticks: 0,
            charged: false, height_offset: zero, height_ticks: 0 },
        _ => MobSpecies::Generic,
    })
}

pub(crate) fn paired_admission(
    identity: &EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    kind: &EntityType,
    spatial: EntitySpatial,
    root: &NbtCompound,
) -> Result<pumpkin_cluster::mob_breeding::MobBirthAdmission, ApplyFailure> {
    let (body, supplied) = prepare_initial_body(identity, reference, owner, kind, spatial, root)?;
    let position = body.spatial.position.map(f64::from_bits);
    let chunk = pumpkin_cluster::protocol::ChunkKey::new(body.spatial.world,
        (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 0, supplied)
}

pub(crate) fn merchant_open_target(
    view: &impl pumpkin_cluster::canonical_action::ActionState,
    contact: &pumpkin_cluster::item_contact::ContactObservation,
    kind: u16,
) -> Result<(pumpkin_cluster::canonical_merchant::MerchantScheme,
    pumpkin_cluster::canonical_action::Target), ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target, TruthView, Value};
    use pumpkin_cluster::canonical_merchant::MerchantScheme;
    let definition = EntityType::from_raw(kind).ok_or(ApplyFailure::InvalidAction)?;
    let scheme = match definition.resource_name {
        "villager" => MerchantScheme::Villager,
        "wandering_trader" => MerchantScheme::WanderingTrader,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &contact.identity
        else { return Err(ApplyFailure::InvalidAction); };
    let Some(cell) = view.read(TruthView::Local, contact.target()) else {
        return Err(ApplyFailure::Condition(contact.target()));
    };
    let Value::Entity(body) = cell.value else { return Err(ApplyFailure::Type(contact.target())); };
    let EntityPayload::Living(living) = &body.payload else { return Err(ApplyFailure::Type(contact.target())); };
    if body.id != *id || body.birth != *birth || body.kind != kind || body.owner != contact.owner
        || body.spatial_epoch != contact.epoch || living.combat.health_milli == 0
        || living.merchant.as_ref().is_none_or(|offers| offers.scheme != scheme) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok((scheme, Target::Chunk { key: contact.chunk,
        part: ChunkPart::LivingProperty { actor: contact.actor(),
            property: pumpkin_cluster::living_property::LivingProperty::MerchantOffers }, inventory: None }))
}

pub(crate) fn apply_living_initialization(body: &mut EntityBody, initial: &MobInitialization)
    -> Result<Vec<InventoryCell>, ApplyFailure> {
    if !initial.valid() { return Err(ApplyFailure::InvalidAction); }
    for cell in &initial.equipment {
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if cell.count > super::cluster_owned_mob_birth::inventory_slot_limit(body, &stack, cell.loc)? {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let EntityPayload::Living(living) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
    living.merchant = initial.merchant.clone();
    if let Some(health) = initial.health_milli {
        living.combat.health_milli = health;
        if living.combat.health_milli == 0 {
            living.combat.timing.death_cause = Some(body.birth.action);
            living.combat.timing.death_ticks = 0;
        }
    }
    living.combat.effective_attributes.extend(initial.attributes.clone());
    living.permanent_attribute_modifiers = initial.attribute_modifiers.clone();
    living.effects = initial.effects.visible.clone();
    living.hidden_effects = initial.effects.hidden.clone();
    living.combat.status = crate::entity::combat::normalized_status(living.effects.iter(), false);
    Ok(initial.equipment.clone())
}

fn optional_timer(value: Option<i32>) -> Result<Option<u32>, ApplyFailure> {
    match value { None | Some(-1) => Ok(None), Some(value) => unsigned(value).map(Some) }
}

pub(crate) fn apply_living_disk_timers(body: &mut EntityBody, root: &NbtCompound) -> Result<(), ApplyFailure> {
    let short = |key: &str, default| match root.get(key) {
        None => Ok(default), Some(NbtTag::Short(value)) => Ok(*value), _ => Err(ApplyFailure::InvalidAction),
    };
    let air = short("Air", 300)?;
    let hurt = short("HurtTime", 0)?;
    let fire = short("Fire", 0)?;
    let death = short("DeathTime", 0)?;
    let frozen = match root.get("TicksFrozen") { None => 0, Some(NbtTag::Int(value)) => *value,
        _ => return Err(ApplyFailure::InvalidAction) };
    let absorption = match root.get("AbsorptionAmount") { None => 0.0,
        Some(NbtTag::Float(value)) => *value, _ => return Err(ApplyFailure::InvalidAction) };
    if !(-20..=300).contains(&air) || hurt < 0 || fire < 0 || death < 0
        || !(0..=140).contains(&frozen) || !absorption.is_finite() || absorption < 0.0
        || f64::from(absorption) * 1000.0 > f64::from(u32::MAX) {
        return Err(ApplyFailure::InvalidAction);
    }
    let EntityPayload::Living(living) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
    if living.combat.health_milli > 0 && death != 0 { return Err(ApplyFailure::InvalidAction); }
    living.environment.air_ticks = air;
    living.environment.frozen_ticks = frozen as u16;
    living.combat.absorption_milli = (f64::from(absorption) * 1000.0).round() as u32;
    living.combat.timing.hurt_ticks = hurt as u16;
    living.combat.timing.fire_ticks = fire as u32;
    living.combat.timing.death_ticks = death as u16;
    living.combat.timing.death_inventory_processed = false;
    if living.combat.health_milli == 0 { living.combat.timing.death_cause = Some(body.birth.action); }
    Ok(())
}

pub(crate) fn horse_mount_eligible(
    body: &EntityBody,
    held: &InventoryCell,
    saddle: &InventoryCell,
) -> Result<bool, ApplyFailure> {
    use pumpkin_data::tag::Taggable;
    if body.kind != EntityType::HORSE.id { return Ok(false); }
    if saddle.loc != InvLoc::new(pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT, 7)
        || saddle.count > 1 { return Err(ApplyFailure::InvalidAction); }
    let EntityPayload::Living(living) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
    let mob = living.mob.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let MobSpecies::Horse { kind: HorseSpeciesKind::Horse, flags, .. } = &mob.species
        else { return Err(ApplyFailure::InvalidAction); };
    if living.combat.health_milli == 0 || mob.reproduction.growth_age < 0 { return Ok(false); }
    let saddled = if saddle.count == 0 { false } else {
        if saddle.item != pumpkin_data::item::Item::SADDLE.id { return Err(ApplyFailure::InvalidAction); }
        true
    };
    if held.count == 0 { return Ok(true); }
    let item = pumpkin_data::item::Item::from_id(held.item).ok_or(ApplyFailure::InvalidAction)?;
    if flags & 2 != 0 && item == &pumpkin_data::item::Item::SADDLE && !saddled { return Ok(false); }
    Ok(!item.has_tag(&pumpkin_data::tag::Item::MINECRAFT_HORSE_FOOD)
        && ![pumpkin_data::item::Item::WHEAT, pumpkin_data::item::Item::SUGAR,
            pumpkin_data::item::Item::HAY_BLOCK, pumpkin_data::item::Item::APPLE,
            pumpkin_data::item::Item::GOLDEN_CARROT, pumpkin_data::item::Item::GOLDEN_APPLE,
            pumpkin_data::item::Item::ENCHANTED_GOLDEN_APPLE].contains(item))
}
