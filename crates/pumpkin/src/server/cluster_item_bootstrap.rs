use std::collections::BTreeSet;
use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure};
use pumpkin_cluster::entity_body::{EntityPayload, EntitySpatial};
use pumpkin_cluster::entity_origin::EntityOriginIdentity;
use pumpkin_cluster::mob_breeding::MobBirthAdmission;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_data::{entity::EntityType, item_stack::ItemStack};
use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};

fn short(root: &NbtCompound, key: &str, default: i16) -> Result<i16, ApplyFailure> {
    match root.child_tags.get(key) {
        None => Ok(default),
        Some(NbtTag::Short(value)) => Ok(*value),
        _ => Err(ApplyFailure::InvalidAction),
    }
}

pub(crate) fn decode_bootstrap(identity: &EntityOriginIdentity, genesis: ActionRef,
    owner: PeerGeneration, kind: &EntityType, chunk: ChunkKey, spatial: EntitySpatial,
    root: &NbtCompound) -> Result<MobBirthAdmission, ApplyFailure> {
    if !spatial.belongs_to(chunk) || root.get_uuid("UUID") != Some(identity.uuid)
        || !matches!(kind.resource_name, "item" | "experience_orb") {
        return Err(ApplyFailure::InvalidAction);
    }
    let identifier = root.get_string("id").ok_or(ApplyFailure::InvalidAction)?;
    if identifier.strip_prefix("minecraft:").unwrap_or(identifier) != kind.resource_name { return Err(ApplyFailure::InvalidAction); }
    let common = ["id", "UUID", "Pos", "Motion", "Rotation", "OnGround", "Air", "Fire",
        "FallDistance", "PortalCooldown", "Invulnerable", "NoGravity", "Silent", "Glowing",
        "CustomName", "CustomNameVisible", "Tags", "Passengers", "HasVisualFire"];
    let specific: &[&str] = if kind.resource_name == "item" {
        &["Item", "Age", "PickupDelay", "Health"]
    } else { &["Value", "Count", "Age", "Health"] };
    if root.child_tags.keys().any(|key| !common.contains(&key.as_ref()) && !specific.contains(&key.as_ref())) {
        return Err(ApplyFailure::InvalidAction);
    }
    for flag in ["Invulnerable", "NoGravity", "Silent", "Glowing", "CustomNameVisible", "HasVisualFire"] {
        if root.child_tags.contains_key(flag) && root.get_byte(flag) != Some(0) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if root.child_tags.contains_key("CustomName") || short(root, "Fire", 0)? > 0
        || root.child_tags.contains_key("PortalCooldown") && root.get_int("PortalCooldown") != Some(0) {
        return Err(ApplyFailure::InvalidAction);
    }
    let tags = match root.child_tags.get("Tags") {
        None => BTreeSet::new(),
        Some(NbtTag::List(tags)) => tags.iter().map(|tag| match tag {
            NbtTag::String(tag) => Ok(tag.to_string()),
            _ => Err(ApplyFailure::InvalidAction),
        }).collect::<Result<BTreeSet<_>, _>>()?,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if !pumpkin_cluster::actor_tags::valid(&tags) { return Err(ApplyFailure::InvalidAction); }
    let age = short(root, "Age", 0)?;
    let health = u32::try_from(short(root, "Health", 5)?).map_err(|_| ApplyFailure::InvalidAction)?;
    if health == 0 { return Err(ApplyFailure::InvalidAction); }
    let mut body = if kind.resource_name == "item" {
        let item_root = root.get_compound("Item").ok_or(ApplyFailure::InvalidAction)?;
        let count = item_root.get_int("count").ok_or(ApplyFailure::InvalidAction)?;
        if !(1..=99).contains(&count) || item_root.child_tags.keys().any(|key| !["id", "count", "components"].contains(&key.as_ref())) {
            return Err(ApplyFailure::InvalidAction);
        }
        let item = ItemStack::read_item_stack(item_root).ok_or(ApplyFailure::InvalidAction)?;
        if i32::from(item.item_count) != count { return Err(ApplyFailure::InvalidAction); }
        let cell = super::cluster_invsee::inventory_cell_from_item_stack(
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0), &item)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let delay = short(root, "PickupDelay", 0)?;
        let mut body = super::cluster_item_birth::reserved_item_body(genesis, identity, owner, spatial,
            &cell, u16::try_from(delay).map_err(|_| ApplyFailure::InvalidAction)?)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let EntityPayload::Item(item) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
        item.never_despawn = age == i16::MIN;
        item.age = if item.never_despawn { 0 } else { u32::try_from(age).map_err(|_| ApplyFailure::InvalidAction)? };
        item.never_pickup = delay == i16::MAX;
        item.health_milli = health.checked_mul(1000).ok_or(ApplyFailure::InvalidAction)?;
        item.last_lifetime_tick = Some(genesis.tick);
        body
    } else {
        let value = match root.child_tags.get("Value") {
            Some(NbtTag::Short(value)) => i32::from(*value),
            Some(NbtTag::Int(value)) => *value,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let count = match root.child_tags.get("Count") {
            None => 1,
            Some(NbtTag::Int(value)) => u32::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let mut body = super::cluster_item_birth::reserved_experience_body(genesis, identity, owner,
            spatial, u32::try_from(value).map_err(|_| ApplyFailure::InvalidAction)?)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let EntityPayload::Experience(orb) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
        orb.count = count;
        orb.age = u32::try_from(age).map_err(|_| ApplyFailure::InvalidAction)?;
        orb.health_milli = health.checked_mul(1000).ok_or(ApplyFailure::InvalidAction)?;
        orb.last_lifetime_tick = Some(genesis.tick);
        body
    };
    body.tags = tags;
    if !body.belongs_to(identity.id, chunk) { return Err(ApplyFailure::InvalidAction); }
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 0, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (EntityOriginIdentity, ActionRef, ChunkKey, EntitySpatial, NbtCompound) {
        let owner = PeerGeneration { server: pumpkin_cluster::identity::ServerId(1), generation: 7 };
        let identity = EntityOriginIdentity { id: pumpkin_cluster::protocol::GlobalEntityId {
            origin: owner.server, generation: owner.generation, local_id: 3 },
            uuid: uuid::Uuid::from_u128(31), source: owner };
        let genesis = ActionRef { tick: pumpkin_cluster::time::TickStamp(10), id: pumpkin_cluster::canonical_action::ActionId {
            actor: pumpkin_cluster::actor::ActorId::Entity(identity.id), incarnation: owner.generation, sequence: 0, family: 2 } };
        let chunk = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 1), 0, 0);
        let spatial = EntitySpatial { world: chunk.world, position: [1.0_f64.to_bits(), 64.0_f64.to_bits(), 1.0_f64.to_bits()],
            velocity_micro: [10, 20, 30], yaw: 0.0_f32.to_bits(), pitch: 0.0_f32.to_bits(), on_ground: false };
        let mut root = NbtCompound::new();
        root.put_uuid("UUID", identity.uuid);
        (identity, genesis, chunk, spatial, root)
    }
    #[test]
    fn imports_item_lifetime_components_and_rejects_unrepresented_owner() {
        let (identity, genesis, chunk, spatial, mut root) = fixture();
        let stack = ItemStack::new(12, &pumpkin_data::item::Item::DIAMOND);
        let mut item = NbtCompound::new();
        stack.write_item_stack(&mut item);
        root.put_string("id", "minecraft:item".to_owned());
        root.put_compound("Item", item);
        root.put_short("Age", i16::MIN);
        root.put_short("PickupDelay", i16::MAX);
        root.put_short("Health", 3);
        let admission = decode_bootstrap(&identity, genesis, identity.source, &EntityType::ITEM,
            chunk, spatial.clone(), &root).unwrap();
        let EntityPayload::Item(item) = admission.body.payload else { panic!("wrong item family"); };
        assert_eq!(item.count, 12);
        assert_eq!(item.health_milli, 3000);
        assert!(item.never_despawn && item.never_pickup);
        assert_eq!(item.last_lifetime_tick, Some(genesis.tick));
        let expected = super::super::cluster_invsee::inventory_cell_from_item_stack(
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0), &stack).unwrap();
        assert_eq!(item.components, expected.nbt);
        root.put_uuid("Owner", uuid::Uuid::from_u128(45));
        assert!(decode_bootstrap(&identity, genesis, identity.source, &EntityType::ITEM, chunk, spatial.clone(), &root).is_err());
        root.child_tags.remove("Owner");
        let mut oversized = NbtCompound::new();
        stack.write_item_stack(&mut oversized);
        oversized.put_int("count", 257);
        root.put_compound("Item", oversized);
        assert!(decode_bootstrap(&identity, genesis, identity.source, &EntityType::ITEM, chunk, spatial, &root).is_err());
    }
    #[test]
    fn imports_experience_multiplicity_and_rejects_identity_change() {
        let (identity, genesis, chunk, spatial, mut root) = fixture();
        root.put_string("id", "minecraft:experience_orb".to_owned());
        root.put_short("Value", 7);
        root.put_int("Count", 4);
        root.put_short("Age", 123);
        let admission = decode_bootstrap(&identity, genesis, identity.source, &EntityType::EXPERIENCE_ORB,
            chunk, spatial.clone(), &root).unwrap();
        let EntityPayload::Experience(orb) = admission.body.payload else { panic!("wrong XP family"); };
        assert_eq!((orb.amount, orb.count, orb.age), (7, 4, 123));
        root.put_uuid("UUID", uuid::Uuid::from_u128(46));
        assert!(decode_bootstrap(&identity, genesis, identity.source, &EntityType::EXPERIENCE_ORB, chunk, spatial, &root).is_err());
    }
}
