use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure};
use pumpkin_cluster::entity_body::EntitySpatial;
use pumpkin_cluster::entity_origin::EntityOriginIdentity;
use pumpkin_cluster::mob_breeding::MobBirthAdmission;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};

pub(crate) fn decode_bootstrap(
    identity: &EntityOriginIdentity,
    genesis: ActionRef,
    owner: PeerGeneration,
    kind: &EntityType,
    chunk: ChunkKey,
    spatial: EntitySpatial,
    root: &NbtCompound,
) -> Result<MobBirthAdmission, ApplyFailure> {
    if !kind.mob || root.get_uuid("UUID") != Some(identity.uuid) || !spatial.belongs_to(chunk)
        || root.get_string("id").is_none_or(|name| name.strip_prefix("minecraft:").unwrap_or(name) != kind.resource_name) {
        return Err(ApplyFailure::InvalidAction);
    }
    validate_fields(kind, root)?;
    let initial = super::cluster_owned_mob_initial::parse_living_initialization(kind, &spatial, root)?;
    let (mut body, mut supplied) = super::cluster_owned_mob_initial::prepare_typed_body(identity,
        genesis, owner, kind, spatial, &initial)?;
    super::cluster_owned_mob_initial::apply_living_disk_timers(&mut body, root)?;
    if let Some(entries) = root.get_list("Inventory") {
        if !matches!(kind.resource_name, "villager" | "piglin") || entries.len() > 8 { return Err(ApplyFailure::InvalidAction); }
        for (slot, entry) in entries.iter().enumerate() {
            let NbtTag::Compound(entry) = entry else { return Err(ApplyFailure::InvalidAction); };
            validate_stack(entry)?;
            if entry.get_int("count").is_none_or(|count| !(0..=99).contains(&count)) { return Err(ApplyFailure::InvalidAction); }
            let stack = pumpkin_data::item_stack::ItemStack::read_item_stack(entry).ok_or(ApplyFailure::InvalidAction)?;
            let loc = pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, slot as u16);
            supplied.push(super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack).map_err(|_| ApplyFailure::InvalidAction)?);
        }
    }
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 0, supplied)
}

fn validate_fields(kind: &EntityType, root: &NbtCompound) -> Result<(), ApplyFailure> {
    if root.has("Xp") && !root.has("Offers") { return Err(ApplyFailure::InvalidAction); }
    if root.has("VillagerData") {
        let data = root.get_compound("VillagerData").ok_or(ApplyFailure::InvalidAction)?;
        unique_keys(data)?;
        if kind.resource_name != "villager" || !root.has("Offers")
            || data.child_tags.iter().any(|(key, value)| key.as_ref() != "level" || !matches!(value, NbtTag::Int(1..=5))) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    unique_keys(root)?;
    validate_attributes(root)?;
    validate_effects(root)?;
    if root.has("attributes") && root.has("Attributes")
        || root.has("active_effects") && root.has("ActiveEffects") { return Err(ApplyFailure::InvalidAction); }
    if let Some(equipment) = root.get_compound("equipment") {
        unique_keys(equipment)?;
        if equipment.child_tags.iter().any(|(key, value)| !matches!(key.as_ref(),
            "mainhand" | "offhand" | "feet" | "legs" | "chest" | "head" | "body" | "saddle")
            || !matches!(value, NbtTag::Compound(_))) { return Err(ApplyFailure::InvalidAction); }
    }
    if let Some(equipment) = root.get_compound("equipment") {
        for (_, value) in &equipment.child_tags {
            let NbtTag::Compound(stack) = value else { return Err(ApplyFailure::InvalidAction); };
            validate_stack(stack)?;
        }
    }
    for (key, value) in &root.child_tags {
        let represented = match key.as_ref() {
            "id" | "UUID" | "Pos" | "Motion" | "Rotation" | "OnGround" | "DataVersion" => true,
            "Tags" | "Health" | "AbsorptionAmount" | "Air" | "TicksFrozen" | "HurtTime" | "Fire" | "DeathTime"
                | "attributes" | "Attributes" | "active_effects" | "ActiveEffects" | "equipment" | "FallDistance"
                | "NoAI" | "PersistenceRequired" | "CanPickUpLoot" | "LeftHanded" | "Age" | "ForcedAge"
                | "AgeLocked" | "InLove" | "LoveCause" => true,
            "Offers" | "Xp" => matches!(kind.resource_name, "villager" | "wandering_trader"),
            "VillagerData" => kind.resource_name == "villager",
            "Inventory" => matches!(kind.resource_name, "villager" | "piglin"),
            "Color" | "Sheared" => kind.resource_name == "sheep",
            "Bred" | "Variant" | "Tame" | "ChestedHorse" | "Temper" | "Owner" | "EatingHaystack" => matches!(kind.resource_name,
                "horse" | "donkey" | "mule" | "skeleton_horse" | "zombie_horse" | "llama" | "trader_llama"),
            "DecorColor" | "Strength" => matches!(kind.resource_name, "llama" | "trader_llama"),
            "Fuse" | "ExplosionRadius" | "powered" | "ignited" => kind.resource_name == "creeper",
            "Size" => matches!(kind.resource_name, "slime" | "magma_cube" | "phantom"),
            "IsBaby" | "CanBreakDoors" | "DrownedConversionTime" => matches!(kind.resource_name,
                "zombie" | "husk" | "drowned" | "zombie_villager" | "zombified_piglin"),
            "StrayConversionTime" => matches!(kind.resource_name, "skeleton" | "stray" | "bogged" | "parched" | "wither_skeleton"),
            "BatFlags" => kind.resource_name == "bat" && matches!(value, NbtTag::Byte(0 | 1)),
            "AX" | "AY" | "AZ" => kind.resource_name == "phantom",
            "carriedBlockState" => kind.resource_name == "enderman",
            "Invulnerable" | "Silent" | "NoGravity" | "Glowing" | "HasVisualFire" | "FallFlying" | "CustomNameVisible" => matches!(value, NbtTag::Byte(0)),
            "PortalCooldown" | "HurtByTimestamp" => matches!(value, NbtTag::Int(0)),
            _ => false,
        };
        if !represented { return Err(ApplyFailure::InvalidAction); }
        let correct_type = match key.as_ref() {
            "Health" | "AbsorptionAmount" | "FallDistance" => matches!(value, NbtTag::Float(_)),
            "Air" | "HurtTime" | "Fire" | "DeathTime" | "Fuse" => matches!(value, NbtTag::Short(_)),
            "carriedBlockState" | "DecorColor" | "Xp" | "Age" | "ForcedAge" | "InLove" | "TicksFrozen" | "Variant" | "Temper" | "Strength" | "Size"
                | "DrownedConversionTime" | "StrayConversionTime" | "AX" | "AY" | "AZ" => matches!(value, NbtTag::Int(_)),
            "NoAI" | "PersistenceRequired" | "CanPickUpLoot" | "LeftHanded" | "AgeLocked" | "Sheared"
                | "Bred" | "Tame" | "ChestedHorse" | "EatingHaystack" | "powered" | "ignited" | "IsBaby" | "CanBreakDoors" => matches!(value, NbtTag::Byte(0 | 1)),
            "Color" | "ExplosionRadius" => matches!(value, NbtTag::Byte(_)),
            "attributes" | "Attributes" | "active_effects" | "ActiveEffects" | "Inventory" | "Tags" => matches!(value, NbtTag::List(_)),
            "equipment" | "Offers" | "VillagerData" => matches!(value, NbtTag::Compound(_)),
            "LoveCause" | "Owner" => matches!(value, NbtTag::IntArray(values) if values.len() == 4),
            _ => true,
        };
        if !correct_type { return Err(ApplyFailure::InvalidAction); }
    }
    Ok(())
}

fn validate_attributes(root: &NbtCompound) -> Result<(), ApplyFailure> {
    let Some(entries) = root.get_list("attributes").or_else(|| root.get_list("Attributes")) else { return Ok(()); };
    if super::cluster_actor_import::read_attributes(Some(root)).map_err(|_| ApplyFailure::InvalidAction)?.len() != entries.len() {
        return Err(ApplyFailure::InvalidAction);
    }
    for entry in entries {
        let NbtTag::Compound(entry) = entry else { return Err(ApplyFailure::InvalidAction); };
        unique_keys(entry)?;
        if [("id", "Name"), ("base", "Base"), ("modifiers", "Modifiers")].iter()
            .any(|(first, second)| entry.has(first) && entry.has(second))
            || entry.child_tags.iter().any(|(key, _)| !matches!(key.as_ref(),
                "id" | "Name" | "base" | "Base" | "modifiers" | "Modifiers")) {
            return Err(ApplyFailure::InvalidAction);
        }
        for name in ["modifiers", "Modifiers"] {
            let Some(tag) = entry.get(name) else { continue; };
            let NbtTag::List(modifiers) = tag else { return Err(ApplyFailure::InvalidAction); };
            for modifier in modifiers {
                let NbtTag::Compound(modifier) = modifier else { return Err(ApplyFailure::InvalidAction); };
                unique_keys(modifier)?;
                if modifier.child_tags.iter().any(|(key, _)| !matches!(key.as_ref(),
                    "id" | "UUID" | "UUIDMost" | "UUIDLeast" | "amount" | "Amount" | "operation" | "Operation"))
                    || [("amount", "Amount"), ("operation", "Operation")].iter()
                        .any(|(first, second)| modifier.has(first) && modifier.has(second))
                    || usize::from(modifier.has("id")) + usize::from(modifier.has("UUID"))
                        + usize::from(modifier.has("UUIDMost") || modifier.has("UUIDLeast")) != 1 {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
    }
    Ok(())
}

fn validate_effects(root: &NbtCompound) -> Result<(), ApplyFailure> {
    let Some(entries) = root.get_list("active_effects").or_else(|| root.get_list("ActiveEffects")) else { return Ok(()); };
    let mut pending = Vec::new();
    for entry in entries {
        let NbtTag::Compound(entry) = entry else { return Err(ApplyFailure::InvalidAction); };
        pending.push(entry);
    }
    let mut count = 0;
    while let Some(effect) = pending.pop() {
        unique_keys(effect)?;
        count += 1;
        if count > 256 || [("id", "Id"), ("duration", "Duration"), ("amplifier", "Amplifier"),
            ("ambient", "Ambient"), ("show_particles", "ShowParticles"), ("show_icon", "ShowIcon"),
            ("hidden_effect", "HiddenEffect")].iter().any(|(first, second)| effect.has(first) && effect.has(second)) {
            return Err(ApplyFailure::InvalidAction);
        }
        for (key, tag) in &effect.child_tags {
            let valid = match key.as_ref() {
                "id" => matches!(tag, NbtTag::String(_)),
                "Id" | "Amplifier" => matches!(tag, NbtTag::Byte(_)),
                "duration" | "Duration" => matches!(tag, NbtTag::Int(value) if *value >= -1),
                "amplifier" => matches!(tag, NbtTag::Int(value) if (0..=255).contains(value)),
                "ambient" | "Ambient" | "show_particles" | "ShowParticles" | "show_icon" | "ShowIcon" =>
                    matches!(tag, NbtTag::Byte(0 | 1)),
                "hidden_effect" | "HiddenEffect" => match tag {
                    NbtTag::Compound(hidden) => { pending.push(hidden); true },
                    _ => false,
                },
                _ => false,
            };
            if !valid { return Err(ApplyFailure::InvalidAction); }
        }
    }
    Ok(())
}

fn validate_stack(root: &NbtCompound) -> Result<(), ApplyFailure> {
    unique_keys(root)?;
    if let Some(components) = root.get_compound("components") {
        unique_keys(components)?;
        if components.child_tags.iter().any(|(key, tag)| key.starts_with("!")
            && !matches!(tag, NbtTag::Compound(value) if value.child_tags.is_empty())) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if root.child_tags.iter().any(|(key, tag)| !match key.as_ref() {
        "id" => matches!(tag, NbtTag::String(_)),
        "count" => matches!(tag, NbtTag::Int(value) if (0..=99).contains(value)),
        "components" => matches!(tag, NbtTag::Compound(_)),
        _ => false,
    }) { return Err(ApplyFailure::InvalidAction); }
    Ok(())
}

fn unique_keys(root: &NbtCompound) -> Result<(), ApplyFailure> {
    let mut keys = std::collections::BTreeSet::<&str>::new();
    if root.child_tags.iter().any(|(key, _)| !keys.insert(key.as_ref())) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(())
}
pub(crate) fn validate_initialization_fields(kind: &EntityType, root: &NbtCompound) -> Result<(), ApplyFailure> {
    validate_fields(kind, root)?;
    for (key, value) in &root.child_tags {
        if matches!(key.as_ref(), "UUID" | "AbsorptionAmount" | "Air" | "TicksFrozen" | "HurtTime" | "Fire" | "DeathTime" | "Inventory") {
            return Err(ApplyFailure::InvalidAction);
        }
        let valid = match key.as_ref() {
            "Motion" => matches!(value, NbtTag::List(values) if values.len() == 3 && values.iter().all(|value| matches!(value, NbtTag::Double(value) if value.is_finite() && value.abs() <= 1_000_000.0))),
            "Rotation" => matches!(value, NbtTag::List(values) if values.len() == 2 && values.iter().all(|value| matches!(value, NbtTag::Float(value) if value.is_finite()))),
            "OnGround" => matches!(value, NbtTag::Byte(0 | 1)),
            "id" => matches!(value, NbtTag::String(value) if value.strip_prefix("minecraft:").unwrap_or(value) == kind.resource_name),
            "Pos" => matches!(value, NbtTag::List(values) if values.len() == 3 && values.iter().all(|value| matches!(value, NbtTag::Double(value) if value.is_finite() && value.abs() <= 30_000_000.0))),
            "DataVersion" => matches!(value, NbtTag::Int(_)),
            _ => true,
        };
        if !valid { return Err(ApplyFailure::InvalidAction); }
    }
    Ok(())
}
