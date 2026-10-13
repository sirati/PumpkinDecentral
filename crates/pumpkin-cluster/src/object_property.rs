use crate::{
    canonical_action::{Cell, ChunkPart, Target, Value},
    entity_body::EntityPayload,
    inventory::InventoryCell,
    object_state::*,
    protocol::BlockPos,
    time::TickStamp,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectPropertyValue {
    Tick(Option<TickStamp>),
    FrameItem(InventoryCell),
    FramePolicy {
        fixed: bool,
        invisible: bool,
        drop_chance: u32,
    },
    Rotation(u8),
    Attachment {
        position: BlockPos,
        facing: u8,
    },
    Fuse(u16),
    Falling(FallingBlockState),
    Cloud(CloudContinuation),
    Crystal(CrystalState),
    Transform(DisplayTransform),
    Content(DisplayContent),
    Presentation(DisplayPresentation),
    InteractionBounds {
        width: u32,
        height: u32,
        response: bool,
    },
    InteractionRecord(Option<InteractionRecord>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayPresentation {
    pub interpolation_start: i32,
    pub interpolation_duration: u32,
    pub teleport_duration: u8,
    pub billboard: u8,
    pub brightness: i32,
    pub view_range: u32,
    pub shadow_radius: u32,
    pub shadow_strength: u32,
    pub width: u32,
    pub height: u32,
    pub glow_color: i32,
}

impl ObjectProperty {
    pub const ALL: [Self; 14] = [
        Self::SimulationTick,
        Self::Cloud,
        Self::FramePolicy,
        Self::FrameRotation,
        Self::Attachment,
        Self::Fuse,
        Self::Falling,
        Self::Crystal,
        Self::DisplayTransform,
        Self::DisplayContent,
        Self::DisplayPresentation,
        Self::InteractionBounds,
        Self::InteractionAttack,
        Self::InteractionUse,
    ];

    pub fn accepts(self, value: &ObjectPropertyValue) -> bool {
        match (self, value) {
            (Self::SimulationTick, ObjectPropertyValue::Tick(_))
            | (Self::FrameItem, ObjectPropertyValue::FrameItem(_))
            | (Self::FramePolicy, ObjectPropertyValue::FramePolicy { .. })
            | (Self::Fuse, ObjectPropertyValue::Fuse(_))
            | (Self::Cloud, ObjectPropertyValue::Cloud(_))
            | (Self::Falling, ObjectPropertyValue::Falling(_))
            | (Self::Crystal, ObjectPropertyValue::Crystal(_))
            | (Self::DisplayTransform, ObjectPropertyValue::Transform(_))
            | (Self::DisplayContent, ObjectPropertyValue::Content(_))
            | (Self::DisplayPresentation, ObjectPropertyValue::Presentation(_))
            | (Self::InteractionBounds, ObjectPropertyValue::InteractionBounds { .. })
            | (
                Self::InteractionAttack | Self::InteractionUse,
                ObjectPropertyValue::InteractionRecord(_),
            ) => true,
            (Self::FrameRotation, ObjectPropertyValue::Rotation(value)) => *value < 8,
            (Self::Attachment, ObjectPropertyValue::Attachment { facing, .. }) => *facing < 6,
            _ => false,
        }
    }
}

fn state(body: &Value) -> Option<&ObjectState> {
    let Value::Entity(body) = body else {
        return None;
    };
    let EntityPayload::Object(state) = &body.payload else {
        return None;
    };
    Some(state)
}

fn state_mut(body: &mut Value) -> Option<&mut ObjectState> {
    let Value::Entity(body) = body else {
        return None;
    };
    let EntityPayload::Object(state) = &mut body.payload else {
        return None;
    };
    Some(state)
}

pub fn body_target(target: Target) -> Option<(Target, ObjectProperty)> {
    let Target::Chunk {
        key,
        part: ChunkPart::ObjectProperty { actor, property },
        inventory: None,
    } = target
    else {
        return None;
    };
    Some((
        Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            inventory: None,
        },
        property,
    ))
}

pub fn read(body: &Value, property: ObjectProperty) -> Option<ObjectPropertyValue> {
    let state = state(body)?;
    if property == ObjectProperty::SimulationTick {
        return Some(ObjectPropertyValue::Tick(state.simulation_tick));
    }
    Some(match (&state.kind, property) {
        (ObjectKind::ItemFrame(frame), ObjectProperty::FrameItem) => {
            ObjectPropertyValue::FrameItem(frame.item.clone())
        }
        (ObjectKind::ItemFrame(frame), ObjectProperty::FramePolicy) => {
            ObjectPropertyValue::FramePolicy {
                fixed: frame.fixed,
                invisible: frame.invisible,
                drop_chance: frame.drop_chance,
            }
        }
        (ObjectKind::ItemFrame(frame), ObjectProperty::FrameRotation) => {
            ObjectPropertyValue::Rotation(frame.rotation)
        }
        (ObjectKind::ItemFrame(frame), ObjectProperty::Attachment) => {
            ObjectPropertyValue::Attachment {
                position: frame.attachment,
                facing: frame.facing,
            }
        }
        (ObjectKind::Painting(painting), ObjectProperty::Attachment) => {
            ObjectPropertyValue::Attachment {
                position: painting.attachment,
                facing: painting.facing,
            }
        }
        (ObjectKind::LeashKnot { attachment }, ObjectProperty::Attachment) => {
            ObjectPropertyValue::Attachment {
                position: *attachment,
                facing: 0,
            }
        }
        (ObjectKind::PrimedTnt(tnt), ObjectProperty::Fuse) => ObjectPropertyValue::Fuse(tnt.fuse),
        (ObjectKind::FallingBlock(falling), ObjectProperty::Falling) => {
            ObjectPropertyValue::Falling(falling.clone())
        }
        (ObjectKind::AreaEffectCloud(cloud), ObjectProperty::Cloud) => {
            ObjectPropertyValue::Cloud(cloud.continuation.clone())
        }
        (ObjectKind::EndCrystal(crystal), ObjectProperty::Crystal) => {
            ObjectPropertyValue::Crystal(crystal.clone())
        }
        (ObjectKind::Display(display), ObjectProperty::DisplayTransform) => {
            ObjectPropertyValue::Transform(display.transform.clone())
        }
        (ObjectKind::Display(display), ObjectProperty::DisplayContent) => {
            ObjectPropertyValue::Content(display.content.clone())
        }
        (ObjectKind::Display(display), ObjectProperty::DisplayPresentation) => {
            ObjectPropertyValue::Presentation(DisplayPresentation {
                interpolation_start: display.interpolation_start,
                interpolation_duration: display.interpolation_duration,
                teleport_duration: display.teleport_duration,
                billboard: display.billboard,
                brightness: display.brightness,
                view_range: display.view_range,
                shadow_radius: display.shadow_radius,
                shadow_strength: display.shadow_strength,
                width: display.width,
                height: display.height,
                glow_color: display.glow_color,
            })
        }
        (ObjectKind::Interaction(interaction), ObjectProperty::InteractionBounds) => {
            ObjectPropertyValue::InteractionBounds {
                width: interaction.width,
                height: interaction.height,
                response: interaction.response,
            }
        }
        (ObjectKind::Interaction(interaction), ObjectProperty::InteractionAttack) => {
            ObjectPropertyValue::InteractionRecord(interaction.attack.clone())
        }
        (ObjectKind::Interaction(interaction), ObjectProperty::InteractionUse) => {
            ObjectPropertyValue::InteractionRecord(interaction.interaction.clone())
        }
        _ => return None,
    })
}

pub fn patch(body: &mut Value, property: ObjectProperty, value: &ObjectPropertyValue) -> bool {
    if !property.accepts(value) {
        return false;
    }
    let Some(original) = state(body) else {
        return false;
    };
    if !original.supports(property) {
        return false;
    }
    let mut next = original.clone();
    match (&mut next.kind, property, value) {
        (_, ObjectProperty::SimulationTick, ObjectPropertyValue::Tick(tick)) => {
            next.simulation_tick = *tick
        }
        (
            ObjectKind::ItemFrame(frame),
            ObjectProperty::FrameItem,
            ObjectPropertyValue::FrameItem(item),
        ) => frame.item = item.clone(),
        (
            ObjectKind::ItemFrame(frame),
            ObjectProperty::FramePolicy,
            ObjectPropertyValue::FramePolicy {
                fixed,
                invisible,
                drop_chance,
            },
        ) => {
            frame.fixed = *fixed;
            frame.invisible = *invisible;
            frame.drop_chance = *drop_chance;
        }
        (
            ObjectKind::ItemFrame(frame),
            ObjectProperty::FrameRotation,
            ObjectPropertyValue::Rotation(rotation),
        ) => frame.rotation = *rotation,
        (
            ObjectKind::ItemFrame(frame),
            ObjectProperty::Attachment,
            ObjectPropertyValue::Attachment { position, facing },
        ) => {
            frame.attachment = *position;
            frame.facing = *facing;
        }
        (
            ObjectKind::Painting(painting),
            ObjectProperty::Attachment,
            ObjectPropertyValue::Attachment { position, facing },
        ) => {
            painting.attachment = *position;
            painting.facing = *facing;
        }
        (
            ObjectKind::LeashKnot { attachment },
            ObjectProperty::Attachment,
            ObjectPropertyValue::Attachment {
                position,
                facing: 0,
            },
        ) => *attachment = *position,
        (ObjectKind::PrimedTnt(tnt), ObjectProperty::Fuse, ObjectPropertyValue::Fuse(fuse)) => {
            tnt.fuse = *fuse
        }
        (
            ObjectKind::FallingBlock(falling),
            ObjectProperty::Falling,
            ObjectPropertyValue::Falling(value),
        ) => {
            if falling.block_state != value.block_state
                || falling.start != value.start
                || falling.tile_data != value.tile_data
            {
                return false;
            }
            *falling = value.clone();
        }
        (
            ObjectKind::AreaEffectCloud(cloud),
            ObjectProperty::Cloud,
            ObjectPropertyValue::Cloud(value),
        ) => {
            if !value.valid() || f32::from_bits(value.radius) > f32::from_bits(cloud.initial_radius)
            {
                return false;
            }
            cloud.continuation = value.clone();
        }
        (
            ObjectKind::EndCrystal(crystal),
            ObjectProperty::Crystal,
            ObjectPropertyValue::Crystal(value),
        ) => *crystal = value.clone(),
        (
            ObjectKind::Display(display),
            ObjectProperty::DisplayTransform,
            ObjectPropertyValue::Transform(transform),
        ) => display.transform = transform.clone(),
        (
            ObjectKind::Display(display),
            ObjectProperty::DisplayContent,
            ObjectPropertyValue::Content(content),
        ) => {
            if std::mem::discriminant(&display.content) != std::mem::discriminant(content) {
                return false;
            }
            display.content = content.clone();
        }
        (
            ObjectKind::Display(display),
            ObjectProperty::DisplayPresentation,
            ObjectPropertyValue::Presentation(value),
        ) => {
            display.interpolation_start = value.interpolation_start;
            display.interpolation_duration = value.interpolation_duration;
            display.teleport_duration = value.teleport_duration;
            display.billboard = value.billboard;
            display.brightness = value.brightness;
            display.view_range = value.view_range;
            display.shadow_radius = value.shadow_radius;
            display.shadow_strength = value.shadow_strength;
            display.width = value.width;
            display.height = value.height;
            display.glow_color = value.glow_color;
        }
        (
            ObjectKind::Interaction(interaction),
            ObjectProperty::InteractionBounds,
            ObjectPropertyValue::InteractionBounds {
                width,
                height,
                response,
            },
        ) => {
            interaction.width = *width;
            interaction.height = *height;
            interaction.response = *response;
        }
        (
            ObjectKind::Interaction(interaction),
            ObjectProperty::InteractionAttack,
            ObjectPropertyValue::InteractionRecord(record),
        ) => interaction.attack = record.clone(),
        (
            ObjectKind::Interaction(interaction),
            ObjectProperty::InteractionUse,
            ObjectPropertyValue::InteractionRecord(record),
        ) => interaction.interaction = record.clone(),
        _ => return false,
    }
    if !next.valid() {
        return false;
    }
    *state_mut(body).unwrap() = next;
    true
}

pub fn cell(body: &Cell, property: ObjectProperty) -> Option<Cell> {
    Some(Cell {
        revision: state(&body.value)?
            .property_versions
            .get(&property)
            .copied()
            .unwrap_or(0),
        value: Value::ObjectProperty(read(&body.value, property)?),
    })
}

pub fn patch_cell(body: &mut Cell, property: ObjectProperty, next: &Cell) -> bool {
    let Value::ObjectProperty(value) = &next.value else {
        return false;
    };
    if !patch(&mut body.value, property, value) {
        return false;
    }
    let versions = &mut state_mut(&mut body.value).unwrap().property_versions;
    if next.revision == 0 {
        versions.remove(&property);
    } else {
        versions.insert(property, next.revision);
    }
    true
}

pub fn normalize_body_versions(before: &Cell, after: &mut Cell) -> bool {
    let Some(original) = state(&before.value) else {
        return false;
    };
    let Some(next) = state(&after.value) else {
        return false;
    };
    if std::mem::discriminant(&original.kind) != std::mem::discriminant(&next.kind) {
        return false;
    }
    let mut versions = original.property_versions.clone();
    for property in ObjectProperty::ALL {
        if read(&before.value, property) != read(&after.value, property) {
            let Some(revision) = versions.get(&property).copied().unwrap_or(0).checked_add(1)
            else {
                return false;
            };
            versions.insert(property, revision);
        }
    }
    state_mut(&mut after.value).unwrap().property_versions = versions;
    true
}

pub fn project_body_writes(
    target: Target,
    before: Option<&Cell>,
    writes: &[crate::canonical_action::AppliedWrite],
) -> Result<Option<Cell>, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::ApplyFailure;
    let facets = writes
        .iter()
        .filter_map(|write| {
            body_target(write.target)
                .filter(|(body, _)| *body == target)
                .map(|(_, property)| (write, property))
        })
        .collect::<Vec<_>>();
    if facets.is_empty() {
        return Ok(writes
            .iter()
            .find(|write| write.target == target)
            .map_or_else(|| before.cloned(), |write| write.after.clone()));
    }
    let mut result = before.cloned().ok_or(ApplyFailure::Condition(target))?;
    for (write, property) in facets {
        if cell(&result, property) != write.before {
            return Err(ApplyFailure::Condition(write.target));
        }
        let next = write
            .after
            .as_ref()
            .ok_or(ApplyFailure::Type(write.target))?;
        if !patch_cell(&mut result, property, next) {
            return Err(ApplyFailure::Type(write.target));
        }
    }
    result.revision = result
        .revision
        .checked_add(1)
        .ok_or(ApplyFailure::Overflow(target))?;
    if let Some(existing) = writes.iter().find(|write| write.target == target) {
        if existing.before.as_ref() != before || existing.after.as_ref() != Some(&result) {
            return Err(ApplyFailure::Condition(target));
        }
    }
    Ok(Some(result))
}
