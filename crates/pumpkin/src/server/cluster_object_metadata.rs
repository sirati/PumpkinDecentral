use pumpkin_cluster::object_state::{DisplayContent, ObjectKind, ObjectState};
use pumpkin_protocol::{codec::var_int::VarInt, java::client::play::Metadata, ser::WritingError};
use pumpkin_util::{
    math::{position::BlockPos, vector3::Vector3},
    version::JavaMinecraftVersion,
};

pub(super) fn kind_matches(object: &ObjectState, kind: u16) -> bool {
    use pumpkin_data::entity::EntityType as E;
    match &object.kind {
        ObjectKind::ItemFrame(frame) => {
            kind == if frame.glow {
                E::GLOW_ITEM_FRAME.id
            } else {
                E::ITEM_FRAME.id
            }
        }
        ObjectKind::Painting(_) => kind == E::PAINTING.id,
        ObjectKind::LeashKnot { .. } => kind == E::LEASH_KNOT.id,
        ObjectKind::PrimedTnt(_) => kind == E::TNT.id,
        ObjectKind::FallingBlock(_) => kind == E::FALLING_BLOCK.id,
        ObjectKind::AreaEffectCloud(_) => kind == E::AREA_EFFECT_CLOUD.id,
        ObjectKind::EndCrystal(_) => kind == E::END_CRYSTAL.id,
        ObjectKind::Display(display) => {
            kind == match &display.content {
                DisplayContent::Block(_) => E::BLOCK_DISPLAY.id,
                DisplayContent::Item { .. } => E::ITEM_DISPLAY.id,
                DisplayContent::Text { .. } => E::TEXT_DISPLAY.id,
            }
        }
        ObjectKind::Interaction(_) => kind == E::INTERACTION.id,
        ObjectKind::Marker { .. } => kind == E::MARKER.id,
    }
}

pub(super) fn spawn_data(object: &ObjectState) -> i32 {
    match &object.kind {
        ObjectKind::ItemFrame(frame) => i32::from(frame.facing),
        ObjectKind::Painting(painting) => i32::from(painting.facing),
        ObjectKind::FallingBlock(falling) => i32::from(falling.block_state),
        _ => 0,
    }
}

pub(super) fn valid_content(object: &ObjectState) -> bool {
    let block = |id| pumpkin_data::BlockStateId::new(id).is_some();
    let item = |cell: &pumpkin_cluster::inventory::InventoryCell| {
        super::cluster_invsee::item_stack_from_inventory_cell(cell)
            .is_ok_and(|stack| cell.count <= stack.get_max_stack_size())
    };
    match &object.kind {
        ObjectKind::PrimedTnt(tnt) => block(tnt.block_state),
        ObjectKind::FallingBlock(falling) => block(falling.block_state),
        ObjectKind::ItemFrame(frame) => item(&frame.item),
        ObjectKind::Painting(painting) => painting_variant(painting.variant)
            .is_some_and(|(_, width, height)| width == painting.width && height == painting.height),
        ObjectKind::Display(display) => match &display.content {
            DisplayContent::Block(id) => block(*id),
            DisplayContent::Item { item: cell, .. } => item(cell),
            DisplayContent::Text { component, .. } => {
                serde_json::from_slice::<pumpkin_util::text::TextComponent>(component).is_ok()
            }
        },
        _ => true,
    }
}

pub(super) fn painting_variant(id: u16) -> Option<(&'static str, u16, u16)> {
    let registry = pumpkin_data::registry::REGISTRY_V_26_2
        .iter()
        .find(|registry| registry.registry_id == "painting_variant")?;
    let entry = registry.entries.get(usize::from(id))?;
    let mut cursor = std::io::Cursor::new(entry.data);
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let document = pumpkin_nbt::Nbt::read_unnamed(&mut reader).ok()?;
    let width = u16::try_from(document.root_tag.get_int("width")?).ok()?;
    let height = u16::try_from(document.root_tag.get_int("height")?).ok()?;
    Some((entry.name, width, height))
}

pub(super) fn bedrock_painting_motif(id: u16) -> Option<String> {
    let (name, _, _) = painting_variant(id)?;
    let motif = match name {
        "skull_and_roses" => "SkullAndRoses",
        "burning_skull" => "BurningSkull",
        "donkey_kong" => "DonkeyKong",
        "kebab" | "aztec" | "alban" | "aztec2" | "bomb" | "plant" | "wasteland" | "pool"
        | "courbet" | "sea" | "sunset" | "creebet" | "wanderer" | "graham" | "match" | "bust"
        | "stage" | "void" | "fighters" | "pointer" | "pigscene" | "skeleton" | "earth"
        | "wind" | "water" | "fire" => {
            let mut motif = name.to_owned();
            motif.get_mut(..1)?.make_ascii_uppercase();
            return Some(motif);
        }
        _ => name,
    };
    Some(motif.to_owned())
}

fn position(value: pumpkin_cluster::protocol::BlockPos) -> BlockPos {
    BlockPos(Vector3::new(value.x, value.y, value.z))
}

pub(super) fn write_java(
    object: &ObjectState,
    version: JavaMinecraftVersion,
    out: &mut Vec<u8>,
) -> Result<(), WritingError> {
    use pumpkin_data::tracked_data as t;
    macro_rules! field {
        ($key:expr, $value:expr) => {{
            let key = $key;
            if key.id.get(&version) != 255 {
                Metadata::new(key, $value).write(&mut *out, &version)?;
            }
        }};
    }
    match &object.kind {
        ObjectKind::ItemFrame(frame) => {
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(&frame.item)
                .map_err(|_| WritingError::Message("invalid frame item".into()))?;
            field!(
                t::item_frame::ITEM,
                pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer::from(stack)
            );
            field!(t::item_frame::ROTATION, i32::from(frame.rotation));
            field!(
                t::entity::DATA_SHARED_FLAGS_ID,
                if frame.invisible { 0x20u8 } else { 0 }
            );
        }
        ObjectKind::Painting(painting) => {
            field!(
                t::painting::VARIANT,
                VarInt(
                    pumpkin_data::painting_variant_id_remap::remap_painting_variant_id_for_version(
                        u32::from(painting.variant),
                        version
                    ) as i32
                )
            );
        }
        ObjectKind::PrimedTnt(tnt) => {
            field!(t::tnt::FUSE_ID, VarInt(i32::from(tnt.fuse)));
            field!(t::tnt::BLOCK_STATE_ID, VarInt(i32::from(tnt.block_state)));
        }
        ObjectKind::FallingBlock(falling) => {
            field!(t::falling_block::START_POS, position(falling.start));
        }
        ObjectKind::EndCrystal(crystal) => {
            field!(t::end_crystal::BEAM_TARGET, crystal.beam.map(position));
            field!(t::end_crystal::SHOW_BOTTOM, crystal.show_bottom);
        }
        ObjectKind::Interaction(interaction) => {
            field!(t::interaction::WIDTH, f32::from_bits(interaction.width));
            field!(t::interaction::HEIGHT, f32::from_bits(interaction.height));
            field!(t::interaction::RESPONSE, interaction.response);
        }
        ObjectKind::Display(display) => {
            use crate::entity::decoration::display::{QuaternionfSerializer, Vector3fSerializer};
            let transform = &display.transform;
            let translation = transform.translation.map(f32::from_bits);
            let scale = transform.scale.map(f32::from_bits);
            let left = transform.left_rotation.map(f32::from_bits);
            let right = transform.right_rotation.map(f32::from_bits);
            field!(
                t::display::START_INTERPOLATION,
                VarInt(display.interpolation_start)
            );
            field!(
                t::display::INTERPOLATION_DURATION,
                VarInt(i32::try_from(display.interpolation_duration).map_err(|_| {
                    WritingError::Message("display interpolation exceeds protocol".into())
                })?)
            );
            field!(
                t::display::TELEPORT_DURATION,
                VarInt(i32::from(display.teleport_duration))
            );
            field!(
                t::display::TRANSLATION,
                Vector3fSerializer(translation[0], translation[1], translation[2])
            );
            field!(
                t::display::SCALE,
                Vector3fSerializer(scale[0], scale[1], scale[2])
            );
            field!(
                t::display::LEFT_ROTATION,
                QuaternionfSerializer(left[0], left[1], left[2], left[3])
            );
            field!(
                t::display::RIGHT_ROTATION,
                QuaternionfSerializer(right[0], right[1], right[2], right[3])
            );
            field!(t::display::BILLBOARD, display.billboard);
            field!(t::display::BRIGHTNESS, VarInt(display.brightness));
            field!(t::display::VIEW_RANGE, f32::from_bits(display.view_range));
            field!(
                t::display::SHADOW_RADIUS,
                f32::from_bits(display.shadow_radius)
            );
            field!(
                t::display::SHADOW_STRENGTH,
                f32::from_bits(display.shadow_strength)
            );
            field!(t::display::WIDTH, f32::from_bits(display.width));
            field!(t::display::HEIGHT, f32::from_bits(display.height));
            field!(t::display::GLOW_COLOR_OVERRIDE, VarInt(display.glow_color));
            match &display.content {
                DisplayContent::Block(state) => {
                    field!(t::block_display::BLOCK_STATE, VarInt(i32::from(*state)));
                }
                DisplayContent::Item { item, context } => {
                    let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
                        .map_err(|_| WritingError::Message("invalid display item".into()))?;
                    field!(
                        t::item_display::ITEM,
                        pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer::from(
                            stack
                        )
                    );
                    field!(t::item_display::ITEM_DISPLAY, *context);
                }
                DisplayContent::Text {
                    component,
                    line_width,
                    background,
                    opacity,
                    flags,
                } => {
                    let text: pumpkin_util::text::TextComponent = serde_json::from_slice(component)
                        .map_err(|_| WritingError::Message("invalid display text".into()))?;
                    field!(t::text_display::TEXT, text);
                    field!(t::text_display::LINE_WIDTH, VarInt(*line_width));
                    field!(t::text_display::BACKGROUND, VarInt(*background));
                    field!(t::text_display::TEXT_OPACITY, *opacity);
                    field!(t::text_display::TEXT_DISPLAY_FLAGS, *flags);
                }
            }
        }
        ObjectKind::AreaEffectCloud(cloud) => {
            field!(
                t::area_effect_cloud::RADIUS,
                f32::from_bits(cloud.continuation.radius)
            );
            field!(
                t::area_effect_cloud::WAITING,
                cloud.continuation.age < cloud.wait
            );
            field!(t::entity::NO_GRAVITY, true);
            field!(
                t::area_effect_cloud::PARTICLE,
                CloudParticle(cloud_color(cloud)?)
            );
        }
        ObjectKind::LeashKnot { .. } | ObjectKind::Marker { .. } => {}
    }
    Ok(())
}

pub(super) fn bedrock(
    object: &ObjectState,
) -> Result<pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList, std::io::Error>
{
    use pumpkin_protocol::bedrock::client::set_actor_data::entity_data_key as key;
    use pumpkin_protocol::bedrock::client::set_actor_data::{MetadataValue, SyncedActorDataList};
    let mut data = SyncedActorDataList::new();
    match &object.kind {
        ObjectKind::AreaEffectCloud(cloud) => {
            data.set(
                key::DATA_RADIUS,
                MetadataValue::Float(f32::from_bits(cloud.continuation.radius)),
            );
            data.set(
                key::DATA_WAITING,
                MetadataValue::Byte(i8::from(cloud.continuation.age < cloud.wait)),
            );
            let color =
                cloud_color(cloud).map_err(|error| std::io::Error::other(error.to_string()))?;
            data.set(key::EFFECT_COLOR, MetadataValue::Int(color));
        }
        ObjectKind::PrimedTnt(tnt) => {
            data.set(key::FUSE_TIME, MetadataValue::Int(i32::from(tnt.fuse)))
        }
        ObjectKind::FallingBlock(falling) => {
            let state = pumpkin_data::BlockStateId::new(falling.block_state)
                .ok_or_else(|| std::io::Error::other("invalid falling block state"))?;
            data.set(
                key::VARIANT,
                MetadataValue::Int(i32::from(pumpkin_data::BlockState::to_be_network_id(state))),
            );
        }
        ObjectKind::Interaction(interaction) => {
            data.set(
                key::WIDTH,
                MetadataValue::Float(f32::from_bits(interaction.width)),
            );
            data.set(
                key::HEIGHT,
                MetadataValue::Float(f32::from_bits(interaction.height)),
            );
        }
        _ => {}
    }
    Ok(data)
}

pub(super) fn write_java_stand(
    living: &pumpkin_cluster::item_action::ActorLivingState,
    version: JavaMinecraftVersion,
    out: &mut Vec<u8>,
) -> Result<(), WritingError> {
    let Some(stand) = &living.stand else {
        return Ok(());
    };
    use pumpkin_data::tracked_data::armor_stand as t;
    let flags = t::ARMOR_STAND_FLAGS;
    if flags.id.get(&version) != 255 {
        Metadata::new(flags, stand.flags).write(&mut *out, &version)?;
    }
    for (key, pose) in [
        t::HEAD_POSE,
        t::BODY_POSE,
        t::LEFT_ARM_POSE,
        t::RIGHT_ARM_POSE,
        t::LEFT_LEG_POSE,
        t::RIGHT_LEG_POSE,
    ]
    .into_iter()
    .zip(&stand.poses)
    {
        if key.id.get(&version) == 255 {
            continue;
        }
        let pose = pose.map(f32::from_bits);
        Metadata::new(
            key,
            crate::entity::decoration::display::Vector3fSerializer(pose[0], pose[1], pose[2]),
        )
        .write(&mut *out, &version)?;
    }
    Ok(())
}

pub(super) fn bounds(
    body: &pumpkin_cluster::entity_body::EntityBody,
) -> Option<pumpkin_util::math::boundingbox::BoundingBox> {
    bounds_at(body, body.spatial.position.map(f64::from_bits))
}

pub(super) fn bounds_at(
    body: &pumpkin_cluster::entity_body::EntityBody,
    at: [f64; 3],
) -> Option<pumpkin_util::math::boundingbox::BoundingBox> {
    use pumpkin_util::math::boundingbox::BoundingBox;
    if at.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let (minimum, maximum) = match &body.payload {
        pumpkin_cluster::entity_body::EntityPayload::Object(object) => match &object.kind {
            ObjectKind::ItemFrame(frame) => attachment_bounds(at, frame.facing, 0.75, 0.75, 0.0625),
            ObjectKind::Painting(painting) => attachment_bounds(
                at,
                painting.facing,
                f64::from(painting.width),
                f64::from(painting.height),
                0.0625,
            ),
            ObjectKind::Interaction(interaction) => {
                let width = f64::from(f32::from_bits(interaction.width));
                let height = f64::from(f32::from_bits(interaction.height));
                (
                    [at[0] - width / 2.0, at[1], at[2] - width / 2.0],
                    [at[0] + width / 2.0, at[1] + height, at[2] + width / 2.0],
                )
            }
            ObjectKind::AreaEffectCloud(cloud) => {
                let radius = f64::from(f32::from_bits(cloud.continuation.radius));
                (
                    [at[0] - radius, at[1], at[2] - radius],
                    [at[0] + radius, at[1] + 0.5, at[2] + radius],
                )
            }
            ObjectKind::Marker { .. } | ObjectKind::Display(_) => (at, at),
            _ => {
                let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)?;
                let width = f64::from(kind.width());
                let height = f64::from(kind.height());
                (
                    [at[0] - width / 2.0, at[1], at[2] - width / 2.0],
                    [at[0] + width / 2.0, at[1] + height, at[2] + width / 2.0],
                )
            }
        },
        pumpkin_cluster::entity_body::EntityPayload::Living(living) => {
            living.stand.as_ref()?;
            let dimensions = super::cluster_owned_mob_metadata::dimensions(living, body.kind)?;
            return Some(BoundingBox::new_from_pos(at[0], at[1], at[2], &dimensions));
        }
        _ => return None,
    };
    Some(BoundingBox::new(
        Vector3::new(minimum[0], minimum[1], minimum[2]),
        Vector3::new(maximum[0], maximum[1], maximum[2]),
    ))
}

fn attachment_bounds(
    at: [f64; 3],
    facing: u8,
    width: f64,
    height: f64,
    depth: f64,
) -> ([f64; 3], [f64; 3]) {
    let half = match facing {
        0 | 1 => [width / 2.0, depth / 2.0, height / 2.0],
        2 | 3 => [width / 2.0, height / 2.0, depth / 2.0],
        _ => [depth / 2.0, height / 2.0, width / 2.0],
    };
    (
        core::array::from_fn(|axis| at[axis] - half[axis]),
        core::array::from_fn(|axis| at[axis] + half[axis]),
    )
}

struct CloudParticle(i32);
impl pumpkin_protocol::java::client::play::MetadataSerializer for CloudParticle {
    fn write_metadata(
        &self,
        writer: &mut impl std::io::Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        use pumpkin_protocol::ser::NetworkWriteExt;
        let id = pumpkin_data::particle_id_remap::remap_particle_id_for_version(
            pumpkin_data::particle::Particle::EntityEffect as u16,
            *version,
        );
        writer.write_var_int(&pumpkin_protocol::codec::var_int::VarInt(i32::from(id)))?;
        writer.write_i32(self.0)
    }
}

fn cloud_color(
    cloud: &pumpkin_cluster::object_state::AreaEffectCloudState,
) -> Result<i32, WritingError> {
    use pumpkin_data::data_component_impl::IDSetContent;
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(&cloud.potion)
        .map_err(|_| WritingError::Message("invalid cloud potion".into()))?;
    if let Some(contents) =
        stack.get_data_component::<pumpkin_data::data_component_impl::PotionContentsImpl>()
        && let Some(color) = contents.custom_color
    {
        return Ok(color | (0xff_u32 << 24) as i32);
    }
    let effects = super::cluster_projectile_potion::potion_effects(&stack)
        .map_err(|_| WritingError::Message("invalid cloud potion effects".into()))?;
    let mut channels = [0_u64; 3];
    let mut weight = 0_u64;
    for effect in effects {
        let kind = pumpkin_data::effect::StatusEffect::from_id(effect.kind)
            .ok_or_else(|| WritingError::Message("invalid cloud effect".into()))?;
        let factor = u64::from(effect.amplifier) + 1;
        for (index, shift) in [16, 8, 0].into_iter().enumerate() {
            channels[index] += ((kind.color >> shift) & 0xff) as u64 * factor;
        }
        weight += factor;
    }
    if weight == 0 {
        return Ok(0xff38_5dc6_u32 as i32);
    }
    Ok((0xff00_0000_u32
        | ((channels[0] / weight) as u32) << 16
        | ((channels[1] / weight) as u32) << 8
        | (channels[2] / weight) as u32) as i32)
}
