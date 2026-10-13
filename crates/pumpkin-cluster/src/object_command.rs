use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::{
    action_command::{
        ActionFootprint, ActorReference, CommandContext, CommandResult, ItemCommandSource,
        ResolutionRecord,
    },
    action_identity::ActionIdentity,
    actor::ActorId,
    actor_occupancy::ActorOccupancyCut,
    canonical_action::{ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Target, Value},
    entity_body::EntityPayload,
    item_action::ActorLivingState,
    living_property::LivingProperty,
    object_property::ObjectPropertyValue,
    object_state::{InteractionRecord, ObjectKind, ObjectProperty},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UseObject {
    pub target: ActorReference,
    pub cut: ActorOccupancyCut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectInteractionAttributes {
    pub scale: u16,
    pub reach: u16,
}

pub trait ObjectInteractionRegistry {
    fn object_interaction_attributes(&self) -> Result<ObjectInteractionAttributes, ApplyFailure>;
    fn object_interaction_eye_height(&self, living: &ActorLivingState)
    -> Result<f64, ApplyFailure>;
}

impl UseObject {
    fn facet(&self, property: ObjectProperty) -> Result<Target, ApplyFailure> {
        let ActorReference::Entity { id, chunk, .. } = self.target else {
            return Err(ApplyFailure::InvalidAction);
        };
        Ok(Target::Chunk {
            key: chunk,
            part: ChunkPart::ObjectProperty {
                actor: id,
                property,
            },
            inventory: None,
        })
    }

    fn frame_inventory(&self) -> Result<Target, ApplyFailure> {
        let ActorReference::Entity { id, chunk, .. } = self.target else {
            return Err(ApplyFailure::InvalidAction);
        };
        Ok(Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: Some(crate::canonical_action::InventoryId::Entity {
                inv: crate::inventory::INV_MAIN,
                slot: 0,
            }),
        })
    }

    fn source_property(
        source: &ItemCommandSource,
        property: LivingProperty,
    ) -> Result<Target, ApplyFailure> {
        let ActorReference::Player { lease, chunk, .. } = source.actor else {
            return Err(ApplyFailure::InvalidAction);
        };
        Ok(Target::Chunk {
            key: chunk,
            part: ChunkPart::LivingProperty {
                actor: ActorId::Player(lease.gid),
                property,
            },
            inventory: None,
        })
    }

    pub fn footprint<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
        source: &ItemCommandSource,
        registry: &impl ObjectInteractionRegistry,
    ) -> Result<ActionFootprint, ApplyFailure> {
        if !self.cut.valid()
            || !source.cut.valid()
            || source.actor.chunk().world != self.target.chunk().world
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let ActorReference::Entity { .. } = self.target else {
            return Err(ApplyFailure::InvalidAction);
        };
        let attributes = registry.object_interaction_attributes()?;
        let source_properties = [
            LivingProperty::Health,
            LivingProperty::GameMode,
            LivingProperty::PlayerStance,
            LivingProperty::Attribute(attributes.scale),
            LivingProperty::PermanentAttributeModifiers(attributes.scale),
            LivingProperty::Attribute(attributes.reach),
            LivingProperty::PermanentAttributeModifiers(attributes.reach),
        ];
        let mut reads = BTreeSet::from([source.actor.body_target(), self.target.body_target()]);
        for property in source_properties {
            reads.insert(Self::source_property(source, property)?);
        }
        let target = self.target.body_target();
        let body = context
            .lookup(target)?
            .ok_or(ApplyFailure::Condition(target))?;
        let Value::Entity(body) = &body.value else {
            return Err(ApplyFailure::Type(target));
        };
        let mut writes = BTreeSet::new();
        match &body.payload {
            EntityPayload::Object(object) => match object.kind {
                ObjectKind::ItemFrame(_) => {
                    for property in [
                        ObjectProperty::FramePolicy,
                        ObjectProperty::FrameRotation,
                        ObjectProperty::Attachment,
                    ] {
                        reads.insert(self.facet(property)?);
                    }
                    reads.insert(self.frame_inventory()?);
                    writes.insert(self.facet(ObjectProperty::FrameRotation)?);
                }
                ObjectKind::Interaction(_) => {
                    reads.extend([
                        self.facet(ObjectProperty::InteractionBounds)?,
                        self.facet(ObjectProperty::InteractionUse)?,
                    ]);
                    writes.insert(self.facet(ObjectProperty::InteractionUse)?);
                }
                _ => {}
            },
            _ => {}
        }
        Ok(ActionFootprint {
            counterfactual_reads: reads.clone(),
            counterfactual_writes: writes.clone(),
            reads,
            writes,
        })
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
        source: &ItemCommandSource,
        registry: &impl ObjectInteractionRegistry,
    ) -> Result<ResolutionRecord, ApplyFailure> {
        let footprint = self.footprint(context, source, registry)?;
        let rejected = || ResolutionRecord {
            identity: context.identity,
            tick: context.tick,
            footprint: footprint.clone(),
            result: CommandResult::NotApplied,
        };
        let ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        let ActorReference::Player {
            lease,
            chunk,
            epoch,
            ..
        } = source.actor
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if id.actor != ActorId::Player(lease.gid)
            || id.incarnation != lease.incarnation
            || !context.state.validate_live_player_lease(lease)
            || source.cut.sample_tick.is_after(context.tick)
            || self.cut.sample_tick.is_after(context.tick)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let get = |target: Target| {
            context
                .lookup(target)?
                .ok_or(ApplyFailure::Condition(target))
        };
        for target in &footprint.reads {
            get(*target)?;
        }
        let Value::Living(living) = &get(source.actor.body_target())?.value else {
            return Err(ApplyFailure::Type(source.actor.body_target()));
        };
        let mut living = living.clone();
        for target in &footprint.reads {
            if let Target::Chunk {
                part:
                    ChunkPart::LivingProperty {
                        actor: ActorId::Player(gid),
                        property,
                    },
                ..
            } = *target
            {
                if gid == lease.gid {
                    let Value::LivingProperty(value) = &get(*target)?.value else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if !property.hydrate(&mut living, value) {
                        return Err(ApplyFailure::Type(*target));
                    }
                }
            }
        }
        let avatar = living
            .avatar
            .as_ref()
            .ok_or(ApplyFailure::Type(source.actor.body_target()))?;
        if avatar.spatial_epoch != epoch
            || avatar.world != chunk.world
            || living.combat.health_milli == 0
            || avatar.game_mode == 3
            || avatar.presentation.sleeping.is_some()
        {
            return Ok(rejected());
        }
        let target = self.target.body_target();
        let Value::Entity(body) = &get(target)?.value else {
            return Err(ApplyFailure::Type(target));
        };
        let ActorReference::Entity {
            id: target_id,
            birth,
            owner,
            epoch,
            ..
        } = &self.target
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if body.id != *target_id
            || body.birth != *birth
            || body.owner != *owner
            || body.spatial_epoch != *epoch
        {
            return Ok(rejected());
        }
        let EntityPayload::Object(_) = &body.payload else {
            return Ok(rejected());
        };
        let mut projected = Value::Entity((body.clone()).into());
        for target in &footprint.reads {
            if let Target::Chunk {
                part: ChunkPart::ObjectProperty { actor, property },
                ..
            } = *target
            {
                if actor == body.id {
                    let Value::ObjectProperty(value) = &get(*target)?.value else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    if !crate::object_property::patch(&mut projected, property, value) {
                        return Err(ApplyFailure::Type(*target));
                    }
                }
            }
        }
        let shape = context.state.actor_collision_shape(target, &projected)?;
        let Value::Entity(projected_body) = projected else {
            return Err(ApplyFailure::InvalidAction);
        };
        let EntityPayload::Object(object) = projected_body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !shape.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let attributes = registry.object_interaction_attributes()?;
        let base = *living
            .combat
            .effective_attributes
            .get(&attributes.reach)
            .ok_or(ApplyFailure::InvalidAction)?;
        let reach = living.attribute(attributes.reach, f64::from_bits(base));
        let eye_height = registry.object_interaction_eye_height(&living)?;
        let mut eye = source.cut.position.map(f64::from_bits);
        eye[1] += eye_height;
        let origin = self.cut.position.map(f64::from_bits);
        if !reach.is_finite()
            || !(0.0..=1024.0).contains(&reach)
            || !eye_height.is_finite()
            || !(0.0..=32.0).contains(&eye_height)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let distance = (0..3)
            .map(|axis| {
                let minimum = origin[axis] + f64::from_bits(shape.offsets[0][axis]);
                let maximum = origin[axis] + f64::from_bits(shape.offsets[1][axis]);
                (eye[axis] - eye[axis].clamp(minimum, maximum)).powi(2)
            })
            .sum::<f64>();
        if distance > reach * reach {
            return Ok(rejected());
        }
        let (property, value) = match object.kind {
            ObjectKind::ItemFrame(frame) if !frame.fixed => {
                let inventory = self.frame_inventory()?;
                let Value::Inventory(item) = &get(inventory)?.value else {
                    return Err(ApplyFailure::Type(inventory));
                };
                if item.is_empty() {
                    return Ok(rejected());
                }
                (
                    ObjectProperty::FrameRotation,
                    ObjectPropertyValue::Rotation((frame.rotation + 1) % 8),
                )
            }
            ObjectKind::Interaction(_) => (
                ObjectProperty::InteractionUse,
                ObjectPropertyValue::InteractionRecord(Some(InteractionRecord {
                    player: lease.key.0,
                    tick: context.tick,
                })),
            ),
            _ => return Ok(rejected()),
        };
        let target = self.facet(property)?;
        if !footprint.writes.contains(&target) {
            return Err(ApplyFailure::InvalidAction);
        }
        let before = get(target)?;
        let after = Cell {
            revision: before
                .revision
                .checked_add(1)
                .ok_or(ApplyFailure::Overflow(target))?,
            value: Value::ObjectProperty(value),
        };
        let writes = if before.value == after.value {
            Vec::new()
        } else {
            vec![AppliedWrite {
                target,
                before: Some(before.clone()),
                after: Some(after),
            }]
        };
        Ok(ResolutionRecord {
            identity: context.identity,
            tick: context.tick,
            footprint,
            result: CommandResult::Applied(writes),
        })
    }
}

#[cfg(test)]
#[path = "object_command_tests.rs"]
mod tests;
