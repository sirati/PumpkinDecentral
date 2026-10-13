use pumpkin_cluster::{
    canonical_action::ApplyFailure,
    inventory::{INV_MAIN, InvLoc, InventoryCell},
    object_state::{DisplayContent, DisplayState, DisplayTransform, ObjectKind},
};
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::{NbtCompound, tag::NbtTag};

fn integer(root: &NbtCompound, key: &str, default: i32) -> Result<i32, ApplyFailure> {
    match root.get(key) {
        None => Ok(default),
        Some(NbtTag::Int(value)) => Ok(*value),
        _ => Err(ApplyFailure::InvalidAction),
    }
}
fn scalar(root: &NbtCompound, key: &str, default: f32) -> Result<u32, ApplyFailure> {
    match root.get(key) {
        None => Ok(default.to_bits()),
        Some(NbtTag::Float(value)) if value.is_finite() => Ok(value.to_bits()),
        _ => Err(ApplyFailure::InvalidAction),
    }
}
fn boolean(root: &NbtCompound, key: &str) -> Result<bool, ApplyFailure> {
    match root.get(key) {
        None | Some(NbtTag::Byte(0)) => Ok(false),
        Some(NbtTag::Byte(1)) => Ok(true),
        _ => Err(ApplyFailure::InvalidAction),
    }
}
fn name<'a>(root: &'a NbtCompound, key: &str, default: &'a str) -> Result<&'a str, ApplyFailure> {
    match root.get(key) {
        None => Ok(default),
        Some(NbtTag::String(value)) => Ok(value.as_ref()),
        _ => Err(ApplyFailure::InvalidAction),
    }
}
fn vector<const N: usize>(
    root: &NbtCompound,
    key: &str,
    default: [f32; N],
) -> Result<[u32; N], ApplyFailure> {
    let Some(value) = root.get(key) else {
        return Ok(default.map(f32::to_bits));
    };
    let NbtTag::List(values) = value else {
        return Err(ApplyFailure::InvalidAction);
    };
    if values.len() != N {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut result = [0; N];
    for (index, value) in values.iter().enumerate() {
        let NbtTag::Float(value) = value else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !value.is_finite() {
            return Err(ApplyFailure::InvalidAction);
        }
        result[index] = value.to_bits();
    }
    Ok(result)
}

pub(super) fn decode(kind: &EntityType, root: &NbtCompound) -> Result<ObjectKind, ApplyFailure> {
    let empty = NbtCompound::new();
    let transformation = match root.get("transformation") {
        None => &empty,
        Some(NbtTag::Compound(value)) => value,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let transform = DisplayTransform {
        translation: vector(transformation, "translation", [0.0; 3])?,
        scale: vector(transformation, "scale", [1.0; 3])?,
        left_rotation: vector(transformation, "left_rotation", [0.0, 0.0, 0.0, 1.0])?,
        right_rotation: vector(transformation, "right_rotation", [0.0, 0.0, 0.0, 1.0])?,
    };
    let billboard = match name(root, "billboard", "fixed")? {
        "fixed" => 0,
        "vertical" => 1,
        "horizontal" => 2,
        "center" => 3,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let brightness = match root.get("brightness") {
        None => -1,
        Some(NbtTag::Compound(value)) => {
            let block = integer(value, "block", 0)?;
            let sky = integer(value, "sky", 0)?;
            if !(0..=15).contains(&block) || !(0..=15).contains(&sky) {
                return Err(ApplyFailure::InvalidAction);
            }
            (block << 4) | (sky << 20)
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let content = if kind.id == EntityType::BLOCK_DISPLAY.id {
        let state = match root.get("block_state") {
            None => pumpkin_data::BlockStateId::AIR.as_u16(),
            Some(NbtTag::Compound(value)) => super::cluster_object_bootstrap::block_state(value)?,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        DisplayContent::Block(state)
    } else if kind.id == EntityType::ITEM_DISPLAY.id {
        let loc = InvLoc::new(INV_MAIN, 0);
        let item = match root.get("item") {
            None => InventoryCell::empty(loc),
            Some(NbtTag::Compound(value)) => {
                let stack = pumpkin_data::item_stack::ItemStack::read_item_stack(value)
                    .ok_or(ApplyFailure::InvalidAction)?;
                super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack)
                    .map_err(|_| ApplyFailure::InvalidAction)?
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let context = match name(root, "item_display", "none")? {
            "none" => 0,
            "thirdperson_lefthand" => 1,
            "thirdperson_righthand" => 2,
            "firstperson_lefthand" => 3,
            "firstperson_righthand" => 4,
            "head" => 5,
            "gui" => 6,
            "ground" => 7,
            "fixed" => 8,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        DisplayContent::Item { item, context }
    } else if kind.id == EntityType::TEXT_DISPLAY.id {
        let text = name(root, "text", "\"\"")?;
        let _: pumpkin_util::text::TextComponent =
            serde_json::from_str(text).map_err(|_| ApplyFailure::InvalidAction)?;
        let component = text.as_bytes().to_vec();
        let mut flags = u8::from(boolean(root, "shadow")?)
            | (u8::from(boolean(root, "see_through")?) << 1)
            | (u8::from(boolean(root, "default_background")?) << 2);
        flags |= match name(root, "alignment", "center")? {
            "center" => 0,
            "left" => 8,
            "right" => 16,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let opacity = match root.get("text_opacity") {
            None => 255,
            Some(NbtTag::Byte(value)) => *value as u8,
            _ => return Err(ApplyFailure::InvalidAction),
        };
        DisplayContent::Text {
            component,
            line_width: integer(root, "line_width", 200)?,
            background: integer(root, "background", 0x40000000)?,
            opacity,
            flags,
        }
    } else {
        return Err(ApplyFailure::InvalidAction);
    };
    let display = DisplayState {
        transform,
        content,
        billboard,
        brightness,
        interpolation_start: integer(root, "start_interpolation", 0)?,
        interpolation_duration: u32::try_from(integer(root, "interpolation_duration", 0)?)
            .map_err(|_| ApplyFailure::InvalidAction)?,
        teleport_duration: u8::try_from(integer(root, "teleport_duration", 0)?)
            .map_err(|_| ApplyFailure::InvalidAction)?,
        view_range: scalar(root, "view_range", 1.0)?,
        shadow_radius: scalar(root, "shadow_radius", 0.0)?,
        shadow_strength: scalar(root, "shadow_strength", 1.0)?,
        width: scalar(root, "width", 0.0)?,
        height: scalar(root, "height", 0.0)?,
        glow_color: integer(root, "glow_color_override", -1)?,
    };
    if !display.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(ObjectKind::Display(display))
}
