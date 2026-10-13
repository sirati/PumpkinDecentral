use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{ActionId, ActionRef, ApplyFailure, SpatialEpoch};
use pumpkin_cluster::entity_body::{
    EntityBirth, EntityBody, EntityPayload, EntitySpatial, VehicleKind, VehicleState,
};
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::EntityType;

pub(super) struct VehicleBirthInput {
    pub entity: GlobalEntityId,
    pub action: ActionId,
    pub owner: PeerGeneration,
    pub tick: TickStamp,
    pub uuid: uuid::Uuid,
    pub kind: EntityType,
    pub spatial: EntitySpatial,
}

pub(crate) fn vehicle_kind(kind: &EntityType) -> Option<VehicleKind> {
    Some(match kind.resource_name {
        "minecart" => VehicleKind::Minecart,
        "chest_minecart" => VehicleKind::ChestMinecart,
        "furnace_minecart" => VehicleKind::FurnaceMinecart,
        "hopper_minecart" => VehicleKind::HopperMinecart,
        "tnt_minecart" => VehicleKind::TntMinecart,
        "spawner_minecart" => VehicleKind::SpawnerMinecart,
        "command_block_minecart" => VehicleKind::CommandBlockMinecart,
        name if name.ends_with("_chest_boat") || name.ends_with("_chest_raft") => {
            VehicleKind::ChestBoat
        }
        name if name.ends_with("_boat") || name.ends_with("_raft") => VehicleKind::Boat,
        _ => return None,
    })
}

pub(super) fn prepare_body(
    input: VehicleBirthInput,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    let kind = vehicle_kind(&input.kind).ok_or(ApplyFailure::InvalidAction)?;
    let authorized_actor = match input.action.actor {
        pumpkin_cluster::actor::ActorId::Entity(entity) => {
            entity == input.entity && input.action.incarnation == input.entity.generation
        }
        pumpkin_cluster::actor::ActorId::Player(player) => {
            player.server == input.owner.server && input.action.incarnation != 0
        }
        pumpkin_cluster::actor::ActorId::Peer(peer) => {
            peer == input.owner.server && input.action.incarnation == input.owner.generation
        }
    };
    if !authorized_actor
        || input.owner.generation == 0
        || input.uuid.is_nil()
        || input.entity.origin != input.owner.server
        || input.entity.generation == 0
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let reference = ActionRef {
        id: input.action,
        tick: input.tick,
    };
    let body = EntityBody {
        mount: None,
        tags: Default::default(),
        tags_revision: 1,
        relation_versions: BTreeMap::new(),
        passengers: BTreeMap::new(),
        id: input.entity,
        birth: EntityBirth {
            action: reference,
            source: input.owner,
            uuid: input.uuid,
        },
        owner: input.owner,
        kind: input.kind.id,
        spatial: input.spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Vehicle(VehicleState::new(kind)),
    };
    if !body.belongs_to(body.id, chunk) || !body.valid_at(input.tick) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn vehicle_drop_item(
    body: &EntityBody,
) -> Result<pumpkin_cluster::inventory::InventoryCell, ApplyFailure> {
    use pumpkin_data::item::Item;
    use pumpkin_data::item_stack::ItemStack;
    let EntityPayload::Vehicle(vehicle) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let kind = EntityType::from_raw(body.kind).ok_or(ApplyFailure::InvalidAction)?;
    if vehicle_kind(&kind) != Some(vehicle.kind) {
        return Err(ApplyFailure::InvalidAction);
    }
    let item = if vehicle.kind == VehicleKind::SpawnerMinecart {
        &Item::MINECART
    } else {
        Item::from_registry_key(&format!("minecraft:{}", kind.resource_name))
            .ok_or(ApplyFailure::InvalidAction)?
    };
    super::cluster_invsee::inventory_cell_from_item_stack(
        pumpkin_cluster::inventory::InvLoc::new(0, 0),
        &ItemStack::static_new_java(1, item),
    )
    .map_err(|_| ApplyFailure::InvalidAction)
}

fn initialization(
    tags: std::collections::BTreeSet<String>,
) -> pumpkin_cluster::mob_initialization::MobInitialization {
    pumpkin_cluster::mob_initialization::MobInitialization {
        attributes: Default::default(),
        attribute_modifiers: Default::default(),
        effects: Default::default(),
        species: pumpkin_cluster::mob_continuation::MobSpecies::Generic,
        reproduction: Default::default(),
        tags,
        health_milli: None,
        merchant: None,
        equipment: Vec::new(),
        fall_distance: 0.0_f32.to_bits(),
        ai_disabled: false,
        persistence_required: false,
        can_pick_up_loot: false,
        left_handed: false,
    }
}

pub(super) fn valid_initialization(
    initial: &pumpkin_cluster::mob_initialization::MobInitialization,
) -> bool {
    initial.valid() && *initial == initialization(initial.tags.clone())
}

pub(crate) fn parse_initialization(
    kind: &EntityType,
    root: &pumpkin_nbt::compound::NbtCompound,
) -> Result<pumpkin_cluster::mob_initialization::MobInitialization, ApplyFailure> {
    if vehicle_kind(kind).is_none()
        || root
            .child_tags
            .keys()
            .any(|key| !matches!(key.as_ref(), "Tags" | "Motion" | "Rotation" | "OnGround"))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    if root.child_tags.contains_key("Tags") && root.get_list("Tags").is_none()
        || root.child_tags.contains_key("OnGround") && root.get_bool("OnGround").is_none()
        || root.child_tags.contains_key("Motion")
            && root.get_list("Motion").is_none_or(|values| {
                values.len() != 3
                    || values.iter().any(|value| {
                        value
                            .extract_double()
                            .is_none_or(|value| !value.is_finite())
                    })
            })
        || root.child_tags.contains_key("Rotation")
            && root.get_list("Rotation").is_none_or(|values| {
                values.len() != 2
                    || values
                        .iter()
                        .any(|value| value.extract_float().is_none_or(|value| !value.is_finite()))
            })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let tags = root
        .get_list("Tags")
        .map(|tags| {
            tags.iter()
                .map(|tag| match tag {
                    pumpkin_nbt::tag::NbtTag::String(tag) => Ok(tag.to_string()),
                    _ => Err(ApplyFailure::InvalidAction),
                })
                .collect::<Result<std::collections::BTreeSet<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let initial = initialization(tags);
    valid_initialization(&initial)
        .then_some(initial)
        .ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn prepare_reserved_body(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    reference: ActionRef,
    owner: PeerGeneration,
    kind: &EntityType,
    spatial: EntitySpatial,
    initial: &pumpkin_cluster::mob_initialization::MobInitialization,
) -> Result<(EntityBody, Vec<pumpkin_cluster::inventory::InventoryCell>), ApplyFailure> {
    if identity.source != owner
        || identity.id.origin != owner.server
        || identity.id.generation != owner.generation
        || !valid_initialization(initial)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let position = spatial.position.map(f64::from_bits);
    let chunk = ChunkKey::new(
        spatial.world,
        (position[0].floor() as i32).div_euclid(16),
        (position[2].floor() as i32).div_euclid(16),
    );
    let mut body = prepare_body(
        VehicleBirthInput {
            entity: identity.id,
            action: reference.id,
            owner,
            tick: reference.tick,
            uuid: identity.uuid,
            kind: kind.clone(),
            spatial,
        },
        chunk,
    )?;
    body.tags = initial.tags.clone();
    Ok((body, Vec::new()))
}

pub(super) fn item_kind(item: u16) -> Option<&'static EntityType> {
    let item = pumpkin_data::item::Item::from_id(item)?;
    let name = item.registry_key.strip_prefix("minecraft:")?;
    let kind = EntityType::from_name(name)?;
    vehicle_kind(kind).map(|_| kind)
}

pub(super) fn cart_placement(
    state: pumpkin_data::BlockStateId,
    position: pumpkin_cluster::protocol::BlockPos,
    world: pumpkin_cluster::protocol::WorldId,
) -> Option<EntitySpatial> {
    use pumpkin_data::block_properties::{
        BlockProperties, PoweredRailLikeProperties, RailLikeProperties,
    };
    let block = state.to_block();
    if !matches!(
        block.name,
        "rail" | "powered_rail" | "detector_rail" | "activator_rail"
    ) {
        return None;
    }
    let ascending = if PoweredRailLikeProperties::handles_block_id(block.id) {
        PoweredRailLikeProperties::from_state_id(state)
            .shape
            .is_ascending()
    } else {
        RailLikeProperties::from_state_id(state)
            .shape
            .is_ascending()
    };
    Some(EntitySpatial {
        world,
        position: [
            f64::from(position.x) + 0.5,
            f64::from(position.y) + 0.0625 + if ascending { 0.5 } else { 0.0 },
            f64::from(position.z) + 0.5,
        ]
        .map(f64::to_bits),
        velocity_micro: [0; 3],
        yaw: 0.0_f32.to_bits(),
        pitch: 0.0_f32.to_bits(),
        on_ground: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::entity_origin::EntityOriginAllocator;
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;

    #[test]
    fn vehicle_factory_uses_reserved_original_birth_and_complete_storage_layout() {
        let source = PeerGeneration {
            server: ServerId(1),
            generation: 1,
        };
        let mut allocator = EntityOriginAllocator::new(source).unwrap();
        for (index, (kind, slots)) in [
            (&EntityType::OAK_BOAT, 0),
            (&EntityType::BAMBOO_RAFT, 0),
            (&EntityType::OAK_CHEST_BOAT, 27),
            (&EntityType::BAMBOO_CHEST_RAFT, 27),
            (&EntityType::MINECART, 0),
            (&EntityType::CHEST_MINECART, 27),
            (&EntityType::HOPPER_MINECART, 5),
            (&EntityType::FURNACE_MINECART, 0),
            (&EntityType::TNT_MINECART, 0),
            (&EntityType::SPAWNER_MINECART, 0),
            (&EntityType::COMMAND_BLOCK_MINECART, 0),
        ]
        .into_iter()
        .enumerate()
        {
            let reference = ActionRef {
                tick: TickStamp(10),
                id: ActionId {
                    actor: ActorId::Peer(source.server),
                    incarnation: source.generation,
                    sequence: index as u16 + 1,
                    family: 1,
                },
            };
            let identities = allocator.reserve(source, reference, 1).unwrap();
            let spatial = EntitySpatial {
                world: WorldId::new(0, 1),
                position: [1.0_f64, 64.0, 1.0].map(f64::to_bits),
                velocity_micro: [0; 3],
                yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
            };
            let initial =
                parse_initialization(kind, &pumpkin_nbt::compound::NbtCompound::new()).unwrap();
            let (body, supplied) =
                prepare_reserved_body(&identities[0], reference, source, kind, spatial, &initial)
                    .unwrap();
            assert_eq!(body.birth.action, reference);
            assert_eq!(body.id, identities[0].id);
            assert_eq!(body.birth.uuid, identities[0].uuid);
            assert!(supplied.is_empty());
            assert_eq!(
                super::super::cluster_owned_mob_birth::inventory_layout(&body)
                    .unwrap()
                    .len(),
                slots
            );
            assert_eq!(
                allocator.reserve(source, reference, 1).unwrap()[0],
                identities[0]
            );
        }
    }

    #[test]
    fn vehicle_initialization_rejects_unsupported_or_malformed_fields() {
        let mut nbt = pumpkin_nbt::compound::NbtCompound::new();
        nbt.put_short("Fuse", 1);
        assert!(parse_initialization(&EntityType::TNT_MINECART, &nbt).is_err());
        let mut nbt = pumpkin_nbt::compound::NbtCompound::new();
        nbt.put_string("Tags", "bad".into());
        assert!(parse_initialization(&EntityType::OAK_BOAT, &nbt).is_err());
        let mut initial = initialization(Default::default());
        initial.health_milli = Some(20_000);
        assert!(!valid_initialization(&initial));
        assert!(item_kind(pumpkin_data::item::Item::APPLE.id).is_none());
        assert_eq!(
            item_kind(pumpkin_data::item::Item::BAMBOO_CHEST_RAFT.id)
                .unwrap()
                .id,
            EntityType::BAMBOO_CHEST_RAFT.id
        );
    }
}

pub(crate) fn decode_bootstrap(
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    genesis: ActionRef,
    owner: PeerGeneration,
    kind: &EntityType,
    chunk: ChunkKey,
    spatial: EntitySpatial,
    root: &pumpkin_nbt::compound::NbtCompound,
) -> Result<pumpkin_cluster::mob_breeding::MobBirthAdmission, ApplyFailure> {
    use pumpkin_cluster::inventory::InvLoc;
    use pumpkin_data::item_stack::ItemStack;
    use pumpkin_nbt::tag::NbtTag;
    if root.child_tags.keys().any(|key| {
        !matches!(
            key.as_ref(),
            "id" | "UUID"
                | "Pos"
                | "Motion"
                | "Rotation"
                | "OnGround"
                | "Tags"
                | "Passengers"
                | "Fuel"
                | "PushX"
                | "PushZ"
                | "Enabled"
                | "TNTFuse"
                | "fuse"
                | "Items"
                | "HurtTime"
                | "HurtDir"
                | "Damage"
                | "Invulnerable"
                | "Silent"
                | "NoGravity"
                | "FallDistance"
                | "Fire"
                | "Air"
                | "PortalCooldown"
        )
    }) {
        return Err(ApplyFailure::InvalidAction);
    }
    for name in ["Invulnerable", "Silent", "NoGravity"] {
        if root.child_tags.contains_key(name) && root.get_bool(name) != Some(false) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    for (name, expected) in [("Fire", -1), ("Air", 300)] {
        if root.child_tags.contains_key(name) && root.get_short(name) != Some(expected) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    if root.child_tags.contains_key("FallDistance") && root.get_float("FallDistance") != Some(0.0)
        || root.child_tags.contains_key("PortalCooldown")
            && root.get_int("PortalCooldown") != Some(0)
        || root.child_tags.contains_key("Passengers")
            && root.get_list("Passengers").is_none_or(|values| {
                values
                    .iter()
                    .any(|value| !matches!(value, NbtTag::Compound(_)))
            })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut source = pumpkin_nbt::compound::NbtCompound::new();
    for name in ["Tags", "Motion", "Rotation", "OnGround"] {
        if let Some(value) = root.child_tags.get(name) {
            source.put(name, value.clone());
        }
    }
    let initial = parse_initialization(kind, &source)?;
    let (mut body, _) = prepare_reserved_body(identity, genesis, owner, kind, spatial, &initial)?;
    let EntityPayload::Vehicle(vehicle) = &mut body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    if root.child_tags.contains_key("Fuel") {
        vehicle.fuel_ticks =
            u32::try_from(root.get_short("Fuel").ok_or(ApplyFailure::InvalidAction)?)
                .map_err(|_| ApplyFailure::InvalidAction)?;
        if vehicle.kind != VehicleKind::FurnaceMinecart {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    for (axis, name) in ["PushX", "PushZ"].into_iter().enumerate() {
        if root.child_tags.contains_key(name) {
            let value = root.get_double(name).ok_or(ApplyFailure::InvalidAction)?;
            let scaled = value * 1_000_000.0;
            if vehicle.kind != VehicleKind::FurnaceMinecart
                || !scaled.is_finite()
                || scaled.abs() > 1_000_000.0
                || scaled.round() != scaled
            {
                return Err(ApplyFailure::InvalidAction);
            }
            vehicle.furnace_push_micro[axis] = scaled as i64;
        }
    }
    if root.child_tags.contains_key("Enabled") {
        if vehicle.kind != VehicleKind::HopperMinecart {
            return Err(ApplyFailure::InvalidAction);
        }
        vehicle.hopper_enabled = root
            .get_bool("Enabled")
            .ok_or(ApplyFailure::InvalidAction)?;
    }
    if root.child_tags.contains_key("TNTFuse") && root.child_tags.contains_key("fuse") {
        return Err(ApplyFailure::InvalidAction);
    }
    for name in ["TNTFuse", "fuse"] {
        if root.child_tags.contains_key(name) {
            if vehicle.kind != VehicleKind::TntMinecart {
                return Err(ApplyFailure::InvalidAction);
            }
            let value = root.get_int(name).ok_or(ApplyFailure::InvalidAction)?;
            vehicle.fuse_ticks = if value == -1 {
                None
            } else {
                Some(u16::try_from(value).map_err(|_| ApplyFailure::InvalidAction)?)
            };
        }
    }
    if root.child_tags.contains_key("HurtTime") {
        vehicle.wobble_ticks = u16::try_from(
            root.get_int("HurtTime")
                .ok_or(ApplyFailure::InvalidAction)?,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    }
    if root.child_tags.contains_key("HurtDir") {
        vehicle.wobble_side =
            i8::try_from(root.get_int("HurtDir").ok_or(ApplyFailure::InvalidAction)?)
                .map_err(|_| ApplyFailure::InvalidAction)?;
    }
    if root.child_tags.contains_key("Damage") {
        let value = root
            .get_float("Damage")
            .ok_or(ApplyFailure::InvalidAction)?;
        let scaled = f64::from(value) * 1000.0;
        if !scaled.is_finite()
            || scaled < 0.0
            || scaled > f64::from(u32::MAX)
            || scaled.round() != scaled
        {
            return Err(ApplyFailure::InvalidAction);
        }
        vehicle.damage_milli = scaled as u32;
        if vehicle.damage_milli > 40_000 {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let capacity = match vehicle.kind {
        VehicleKind::ChestBoat | VehicleKind::ChestMinecart => 27,
        VehicleKind::HopperMinecart => 5,
        _ => 0,
    };
    let mut supplied = Vec::new();
    if root.child_tags.contains_key("Items") {
        if capacity == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let items = root.get_list("Items").ok_or(ApplyFailure::InvalidAction)?;
        for item in items {
            let NbtTag::Compound(item) = item else {
                return Err(ApplyFailure::InvalidAction);
            };
            if item
                .child_tags
                .keys()
                .any(|key| !matches!(key.as_ref(), "Slot" | "id" | "count" | "components"))
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let slot = u16::from(item.get_byte("Slot").ok_or(ApplyFailure::InvalidAction)? as u8);
            let count = item.get_int("count").ok_or(ApplyFailure::InvalidAction)?;
            if slot >= capacity || !(1..=99).contains(&count) {
                return Err(ApplyFailure::InvalidAction);
            }
            let stack = ItemStack::read_item_stack(item).ok_or(ApplyFailure::InvalidAction)?;
            supplied.push(
                super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(0, slot), &stack)
                    .map_err(|_| ApplyFailure::InvalidAction)?,
            );
        }
    }
    if !body.valid_at(genesis.tick) {
        return Err(ApplyFailure::InvalidAction);
    }
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 1, supplied)
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_nbt::{compound::NbtCompound, tag::NbtTag};

    #[test]
    fn disk_vehicle_preserves_identity_fuel_and_complete_cargo_or_rejects_loss() {
        let owner = PeerGeneration {
            server: ServerId(2),
            generation: 7,
        };
        let identity = pumpkin_cluster::entity_origin::EntityOriginIdentity {
            id: GlobalEntityId {
                origin: owner.server,
                local_id: 4,
                generation: owner.generation,
            },
            source: owner,
            uuid: uuid::Uuid::from_u128(15),
        };
        let reference = ActionRef {
            tick: TickStamp(3),
            id: ActionId {
                actor: ActorId::Peer(owner.server),
                incarnation: owner.generation,
                sequence: 9,
                family: 1,
            },
        };
        let spatial = EntitySpatial {
            world: WorldId::new(0, 1),
            position: [1.0_f64, 64.0, 1.0].map(f64::to_bits),
            velocity_micro: [0; 3],
            yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
        };
        let chunk = ChunkKey::new(spatial.world, 0, 0);
        let mut furnace = NbtCompound::new();
        furnace.put_short("Fuel", 400);
        furnace.put_double("PushX", 0.25);
        let imported = decode_bootstrap(
            &identity,
            reference,
            owner,
            &EntityType::FURNACE_MINECART,
            chunk,
            spatial.clone(),
            &furnace,
        )
        .unwrap();
        assert_eq!(imported.body.id, identity.id);
        assert_eq!(imported.body.birth.uuid, identity.uuid);
        assert_eq!(imported.body.birth.action, reference);
        let EntityPayload::Vehicle(vehicle) = imported.body.payload else {
            unreachable!()
        };
        assert_eq!(vehicle.fuel_ticks, 400);
        assert_eq!(vehicle.furnace_push_micro, [250_000, 0]);
        let mut item = NbtCompound::new();
        item.put_byte("Slot", 2);
        item.put_string("id", "minecraft:stone".into());
        item.put_int("count", 3);
        let mut chest = NbtCompound::new();
        chest.put("Items", NbtTag::List(vec![NbtTag::Compound(item.clone())]));
        let imported = decode_bootstrap(
            &identity,
            reference,
            owner,
            &EntityType::CHEST_MINECART,
            chunk,
            spatial.clone(),
            &chest,
        )
        .unwrap();
        assert_eq!(imported.inventory.cells.len(), 27);
        assert!(
            matches!(&imported.inventory.cells[2].value, pumpkin_cluster::canonical_action::Value::Inventory(cell) if cell.count == 3 && cell.loc.slot == 2)
        );
        chest.put(
            "Items",
            NbtTag::List(vec![NbtTag::Compound(item.clone()), NbtTag::Compound(item)]),
        );
        assert!(
            decode_bootstrap(
                &identity,
                reference,
                owner,
                &EntityType::CHEST_MINECART,
                chunk,
                spatial.clone(),
                &chest
            )
            .is_err()
        );
        let mut high_damage = NbtCompound::new();
        high_damage.put_float("Damage", 41.0);
        assert!(
            decode_bootstrap(
                &identity,
                reference,
                owner,
                &EntityType::MINECART,
                chunk,
                spatial.clone(),
                &high_damage
            )
            .is_err()
        );
        let mut unsupported = NbtCompound::new();
        unsupported.put_string("LootTable", "minecraft:chests/simple_dungeon".into());
        assert!(
            decode_bootstrap(
                &identity,
                reference,
                owner,
                &EntityType::CHEST_MINECART,
                chunk,
                spatial,
                &unsupported
            )
            .is_err()
        );
    }
}
