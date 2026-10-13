use pumpkin_cluster::{
    armor_stand::ArmorStandState,
    canonical_action::{ActionRef, ApplyFailure},
    entity_body::EntitySpatial,
    entity_origin::EntityOriginIdentity,
    mob_breeding::MobBirthAdmission,
    protocol::ChunkKey,
    realm_completion::PeerGeneration,
};
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::{NbtCompound, tag::NbtTag};

pub(crate) fn decode_bootstrap(
    identity: &EntityOriginIdentity,
    genesis: ActionRef,
    owner: PeerGeneration,
    kind: &EntityType,
    chunk: ChunkKey,
    spatial: EntitySpatial,
    root: &NbtCompound,
) -> Result<MobBirthAdmission, ApplyFailure> {
    if kind.id == EntityType::MARKER.id {
        for name in [
            "CustomName",
            "CustomNameVisible",
            "Invisible",
            "Silent",
            "NoGravity",
            "Invulnerable",
        ] {
            if root
                .get(name)
                .is_some_and(|value| !matches!(value, NbtTag::Byte(0)))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let data = match root.get("data") {
            None => NbtCompound::new(),
            Some(NbtTag::Compound(data)) => data.clone(),
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let data = pumpkin_nbt::Nbt::new(String::new(), data)
            .write_unnamed_canonical()
            .map_err(|_| ApplyFailure::InvalidAction)?
            .to_vec();
        let state = pumpkin_cluster::object_state::ObjectState {
            simulation_tick: Some(genesis.tick),
            property_versions: Default::default(),
            kind: pumpkin_cluster::object_state::ObjectKind::Marker { data },
        };
        let mut body = super::cluster_object_birth::prepare_reserved(
            identity,
            genesis,
            owner,
            kind.clone(),
            spatial,
            state,
            chunk,
        )?;
        if let Some(tags) = root.get("Tags") {
            let NbtTag::List(tags) = tags else {
                return Err(ApplyFailure::InvalidAction);
            };
            for tag in tags {
                let NbtTag::String(tag) = tag else {
                    return Err(ApplyFailure::InvalidAction);
                };
                body.tags.insert(tag.to_string());
            }
        }
        return super::cluster_owned_mob_birth::paired_admission(body, chunk, 1, Vec::new());
    }
    if [
        EntityType::ITEM_FRAME.id,
        EntityType::GLOW_ITEM_FRAME.id,
        EntityType::PAINTING.id,
    ]
    .contains(&kind.id)
    {
        for name in [
            "CustomName",
            "CustomNameVisible",
            "Silent",
            "NoGravity",
            "Invulnerable",
        ] {
            if root
                .get(name)
                .is_some_and(|value| !matches!(value, NbtTag::Byte(0)))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let (state, supplied) = hanging_state(kind, genesis.tick, root)?;
        let mut body = super::cluster_object_birth::prepare_reserved(
            identity,
            genesis,
            owner,
            kind.clone(),
            spatial,
            state,
            chunk,
        )?;
        if let Some(tags) = root.get("Tags") {
            let NbtTag::List(tags) = tags else {
                return Err(ApplyFailure::InvalidAction);
            };
            for tag in tags {
                let NbtTag::String(tag) = tag else {
                    return Err(ApplyFailure::InvalidAction);
                };
                body.tags.insert(tag.to_string());
            }
        }
        return super::cluster_owned_mob_birth::paired_admission(body, chunk, 1, supplied);
    }
    if [
        EntityType::TNT.id,
        EntityType::FALLING_BLOCK.id,
        EntityType::END_CRYSTAL.id,
        EntityType::BLOCK_DISPLAY.id,
        EntityType::ITEM_DISPLAY.id,
        EntityType::TEXT_DISPLAY.id,
        EntityType::INTERACTION.id,
        EntityType::LEASH_KNOT.id,
    ]
    .contains(&kind.id)
    {
        for name in [
            "CustomName",
            "CustomNameVisible",
            "Invisible",
            "Silent",
            "NoGravity",
        ] {
            if root
                .get(name)
                .is_some_and(|value| !matches!(value, NbtTag::Byte(0)))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if kind.id != EntityType::END_CRYSTAL.id && disk_boolean(root, "Invulnerable", false)? {
            return Err(ApplyFailure::InvalidAction);
        }
        let state = pumpkin_cluster::object_state::ObjectState {
            simulation_tick: Some(genesis.tick),
            property_versions: Default::default(),
            kind: dynamic_state(kind, &spatial, root)?,
        };
        let mut body = super::cluster_object_birth::prepare_reserved(
            identity,
            genesis,
            owner,
            kind.clone(),
            spatial,
            state,
            chunk,
        )?;
        if let Some(tags) = root.get("Tags") {
            let NbtTag::List(tags) = tags else {
                return Err(ApplyFailure::InvalidAction);
            };
            for tag in tags {
                let NbtTag::String(tag) = tag else {
                    return Err(ApplyFailure::InvalidAction);
                };
                body.tags.insert(tag.to_string());
            }
        }
        return super::cluster_owned_mob_birth::paired_admission(body, chunk, 1, Vec::new());
    }
    if kind.id != EntityType::ARMOR_STAND.id {
        return Err(ApplyFailure::InvalidAction);
    }
    for name in [
        "CustomName",
        "CustomNameVisible",
        "Invisible",
        "Silent",
        "NoGravity",
        "Invulnerable",
        "ArmorItems",
        "HandItems",
        "ArmorDropChances",
        "HandDropChances",
    ] {
        if root
            .get(name)
            .is_some_and(|value| !matches!(value, NbtTag::Byte(0)))
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let mut stand = ArmorStandState::vanilla();
    for (name, mask) in [
        ("Small", 1),
        ("ShowArms", 4),
        ("NoBasePlate", 8),
        ("Marker", 16),
    ] {
        if let Some(value) = root.get(name) {
            match value {
                NbtTag::Byte(0) => {}
                NbtTag::Byte(1) => stand.flags |= mask,
                _ => return Err(ApplyFailure::InvalidAction),
            }
        }
    }
    if let Some(value) = root.get("DisabledSlots") {
        let NbtTag::Int(value) = value else {
            return Err(ApplyFailure::InvalidAction);
        };
        stand.disabled_slots = u32::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?;
    }
    if let Some(value) = root.get("Pose") {
        let NbtTag::Compound(pose) = value else {
            return Err(ApplyFailure::InvalidAction);
        };
        for (index, name) in ["Head", "Body", "LeftArm", "RightArm", "LeftLeg", "RightLeg"]
            .into_iter()
            .enumerate()
        {
            if let Some(value) = pose.get(name) {
                let NbtTag::List(values) = value else {
                    return Err(ApplyFailure::InvalidAction);
                };
                if values.len() != 3 {
                    return Err(ApplyFailure::InvalidAction);
                }
                for (axis, value) in values.iter().enumerate() {
                    let NbtTag::Float(value) = value else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if !value.is_finite() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    stand.poses[index][axis] = value.to_bits();
                }
            }
        }
    }
    if !stand.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let initial =
        super::cluster_owned_mob_initial::parse_living_initialization(kind, &spatial, root)?;
    let mut body = super::cluster_object_birth::prepare_reserved_stand(
        identity, genesis, owner, spatial, stand, chunk,
    )?;
    body.tags = initial.tags.clone();
    let supplied =
        super::cluster_owned_mob_initial::apply_living_initialization(&mut body, &initial)?;
    super::cluster_owned_mob_initial::apply_living_disk_timers(&mut body, root)?;
    super::cluster_owned_mob_birth::paired_admission(body, chunk, 1, supplied)
}

fn hanging_state(
    kind: &EntityType,
    tick: pumpkin_cluster::time::TickStamp,
    root: &NbtCompound,
) -> Result<
    (
        pumpkin_cluster::object_state::ObjectState,
        Vec<pumpkin_cluster::inventory::InventoryCell>,
    ),
    ApplyFailure,
> {
    use pumpkin_cluster::{
        inventory::{INV_MAIN, InvLoc, InventoryCell},
        object_state::{FrameState, ObjectKind, ObjectState, PaintingState},
        protocol::BlockPos,
    };
    let attachment = match root.get("block_pos") {
        Some(NbtTag::IntArray(at)) if at.len() == 3 => BlockPos {
            x: at[0],
            y: at[1],
            z: at[2],
        },
        None => {
            let coordinate = |name| match root.get(name) {
                Some(NbtTag::Int(value)) => Ok(*value),
                _ => Err(ApplyFailure::InvalidAction),
            };
            BlockPos {
                x: coordinate("TileX")?,
                y: coordinate("TileY")?,
                z: coordinate("TileZ")?,
            }
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let boolean = |name, default| match root.get(name) {
        None => Ok(default),
        Some(NbtTag::Byte(0)) => Ok(false),
        Some(NbtTag::Byte(1)) => Ok(true),
        _ => Err(ApplyFailure::InvalidAction),
    };
    let facing = match root.get("Facing").or_else(|| root.get("facing")) {
        Some(NbtTag::Byte(value)) => {
            u8::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let (object, supplied) =
        if kind.id == EntityType::ITEM_FRAME.id || kind.id == EntityType::GLOW_ITEM_FRAME.id {
            let loc = InvLoc::new(INV_MAIN, 0);
            let item = match root.get("Item") {
                None => InventoryCell::empty(loc),
                Some(NbtTag::Compound(item)) => {
                    let stack = pumpkin_data::item_stack::ItemStack::read_item_stack(item)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack)
                        .map_err(|_| ApplyFailure::InvalidAction)?
                }
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let rotation = match root.get("ItemRotation") {
                None => 0,
                Some(NbtTag::Byte(value)) => {
                    u8::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?
                }
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let drop_chance = match root.get("ItemDropChance") {
                None => 1.0_f32,
                Some(NbtTag::Float(value)) => *value,
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let supplied = if item.count == 0 {
                Vec::new()
            } else {
                vec![item.clone()]
            };
            (
                ObjectKind::ItemFrame(FrameState {
                    attachment,
                    facing,
                    glow: kind.id == EntityType::GLOW_ITEM_FRAME.id,
                    item,
                    rotation,
                    drop_chance: drop_chance.to_bits(),
                    invisible: boolean("Invisible", false)?,
                    fixed: boolean("Fixed", false)?,
                }),
                supplied,
            )
        } else {
            let name = match root.get("variant") {
                Some(NbtTag::String(name)) => name.as_ref(),
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let registry = pumpkin_data::registry::REGISTRY_V_26_2
                .iter()
                .find(|registry| registry.registry_id == "painting_variant")
                .ok_or(ApplyFailure::InvalidAction)?;
            let index = registry
                .entries
                .iter()
                .position(|entry| entry.name == name)
                .ok_or(ApplyFailure::InvalidAction)?;
            let variant = u16::try_from(index).map_err(|_| ApplyFailure::InvalidAction)?;
            let (_, width, height) = super::cluster_object_metadata::painting_variant(variant)
                .ok_or(ApplyFailure::InvalidAction)?;
            (
                ObjectKind::Painting(PaintingState {
                    attachment,
                    facing,
                    variant,
                    width,
                    height,
                }),
                Vec::new(),
            )
        };
    let state = ObjectState {
        simulation_tick: Some(tick),
        property_versions: Default::default(),
        kind: object,
    };
    if !state.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok((state, supplied))
}

pub(super) fn block_state(root: &NbtCompound) -> Result<u16, ApplyFailure> {
    let name = match root.get("Name") {
        Some(NbtTag::String(name)) => name.as_ref(),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let block = pumpkin_data::Block::from_name(name).ok_or(ApplyFailure::InvalidAction)?;
    let mut wanted: std::collections::BTreeMap<&str, &str> = block
        .properties(block.default_state.id)
        .map(|properties| properties.to_props().into_iter().collect())
        .unwrap_or_default();
    if let Some(properties) = root.get("Properties") {
        let NbtTag::Compound(properties) = properties else {
            return Err(ApplyFailure::InvalidAction);
        };
        for (key, value) in &properties.child_tags {
            let NbtTag::String(value) = value else {
                return Err(ApplyFailure::InvalidAction);
            };
            if !wanted.contains_key(key.as_ref()) {
                return Err(ApplyFailure::InvalidAction);
            }
            wanted.insert(key.as_ref(), value.as_ref());
        }
    }
    for state in block.states {
        let actual: std::collections::BTreeMap<&str, &str> = block
            .properties(state.id)
            .map(|properties| properties.to_props().into_iter().collect())
            .unwrap_or_default();
        if actual == wanted {
            return Ok(state.id.as_u16());
        }
    }
    Err(ApplyFailure::InvalidAction)
}

fn disk_boolean(root: &NbtCompound, name: &str, default: bool) -> Result<bool, ApplyFailure> {
    match root.get(name) {
        None => Ok(default),
        Some(NbtTag::Byte(0)) => Ok(false),
        Some(NbtTag::Byte(1)) => Ok(true),
        _ => Err(ApplyFailure::InvalidAction),
    }
}

fn dynamic_state(
    kind: &EntityType,
    spatial: &EntitySpatial,
    root: &NbtCompound,
) -> Result<pumpkin_cluster::object_state::ObjectKind, ApplyFailure> {
    use pumpkin_cluster::{
        object_state::{CrystalState, FallingBlockState, ObjectKind, TntState},
        protocol::BlockPos,
    };
    let float = |name, default| match root.get(name) {
        None => Ok(default),
        Some(NbtTag::Float(value)) if value.is_finite() => Ok(*value),
        _ => Err(ApplyFailure::InvalidAction),
    };
    let unsigned = |name, default| match root.get(name) {
        None => Ok(default),
        Some(NbtTag::Int(value)) => u32::try_from(*value).map_err(|_| ApplyFailure::InvalidAction),
        _ => Err(ApplyFailure::InvalidAction),
    };
    if kind.id == EntityType::TNT.id {
        if root.get("owner").is_some() || root.get("Owner").is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        let fuse = match root.get("fuse").or_else(|| root.get("Fuse")) {
            None => 80,
            Some(NbtTag::Int(value)) => {
                u16::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?
            }
            Some(NbtTag::Short(value)) => {
                u16::try_from(*value).map_err(|_| ApplyFailure::InvalidAction)?
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let block_state = match root.get("block_state") {
            None => pumpkin_data::Block::TNT.default_state.id.as_u16(),
            Some(NbtTag::Compound(value)) => block_state(value)?,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        return Ok(ObjectKind::PrimedTnt(TntState {
            fuse,
            power: float("explosion_power", 4.0)?.to_bits(),
            block_state,
            igniter: None,
        }));
    }
    if kind.id == EntityType::FALLING_BLOCK.id {
        if disk_boolean(root, "CancelDrop", false)? {
            return Err(ApplyFailure::InvalidAction);
        }
        let block_state = match root.get("BlockState") {
            Some(NbtTag::Compound(value)) => block_state(value)?,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let at = spatial.position.map(f64::from_bits);
        let start = BlockPos {
            x: at[0].floor() as i32,
            y: at[1].floor() as i32,
            z: at[2].floor() as i32,
        };
        let tile_data = match root.get("TileEntityData") {
            None => Vec::new(),
            Some(NbtTag::Compound(value)) => pumpkin_nbt::Nbt::new(String::new(), value.clone())
                .write_unnamed_canonical()
                .map_err(|_| ApplyFailure::InvalidAction)?
                .to_vec(),
            _ => return Err(ApplyFailure::InvalidAction),
        };
        return Ok(ObjectKind::FallingBlock(FallingBlockState {
            block_state,
            start,
            age: unsigned("Time", 0)?,
            drop_item: disk_boolean(root, "DropItem", true)?,
            hurt_entities: disk_boolean(root, "HurtEntities", false)?,
            fall_distance: float("FallDistance", 0.0)?.to_bits(),
            damage_per_block: float("FallHurtAmount", 2.0)?.to_bits(),
            max_damage: (unsigned("FallHurtMax", 40)? as f32).to_bits(),
            tile_data,
        }));
    }
    if kind.id == EntityType::END_CRYSTAL.id {
        let beam = match root.get("BeamTarget").or_else(|| root.get("beam_target")) {
            None => None,
            Some(NbtTag::IntArray(at)) if at.len() == 3 => Some(BlockPos {
                x: at[0],
                y: at[1],
                z: at[2],
            }),
            Some(NbtTag::Compound(at)) => {
                let coordinate = |name| match at.get(name) {
                    Some(NbtTag::Int(value)) => Ok(*value),
                    _ => Err(ApplyFailure::InvalidAction),
                };
                Some(BlockPos {
                    x: coordinate("X")?,
                    y: coordinate("Y")?,
                    z: coordinate("Z")?,
                })
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        return Ok(ObjectKind::EndCrystal(CrystalState {
            beam,
            show_bottom: disk_boolean(root, "ShowBottom", true)?,
            invulnerable: disk_boolean(root, "Invulnerable", false)?,
        }));
    }
    if [
        EntityType::BLOCK_DISPLAY.id,
        EntityType::ITEM_DISPLAY.id,
        EntityType::TEXT_DISPLAY.id,
    ]
    .contains(&kind.id)
    {
        return super::cluster_object_bootstrap_display::decode(kind, root);
    }
    if kind.id == EntityType::LEASH_KNOT.id {
        let attachment = match root.get("block_pos") {
            Some(NbtTag::IntArray(at)) if at.len() == 3 => BlockPos {
                x: at[0],
                y: at[1],
                z: at[2],
            },
            _ => return Err(ApplyFailure::InvalidAction),
        };
        return Ok(ObjectKind::LeashKnot { attachment });
    }
    if kind.id == EntityType::INTERACTION.id {
        if root.get("attack").is_some() || root.get("interaction").is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        return Ok(ObjectKind::Interaction(
            pumpkin_cluster::object_state::InteractionState {
                width: float("width", 1.0)?.to_bits(),
                height: float("height", 1.0)?.to_bits(),
                response: disk_boolean(root, "response", false)?,
                attack: None,
                interaction: None,
            },
        ));
    }
    Err(ApplyFailure::InvalidAction)
}
