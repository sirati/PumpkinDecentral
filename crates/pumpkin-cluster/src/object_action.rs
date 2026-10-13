use crate::{
    actor::ActorId,
    canonical_action::*,
    canonical_inventory::InventoryActionCapture,
    item_contact::{ContactIdentity, ContactObservation},
    object_property::ObjectPropertyValue,
    object_state::{InteractionRecord, ObjectProperty},
    time::TickStamp,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectOperation {
    RotateFrame,
    RecordAttack,
    RecordInteraction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIntent {
    pub source: ContactObservation,
    pub target: ContactObservation,
    pub eye_height: u64,
    pub reach: u64,
    pub reach_attribute: u16,
    pub bounds_minimum: [u64; 3],
    pub bounds_maximum: [u64; 3],
    pub operation: ObjectOperation,
}

impl ObjectIntent {
    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        let ContactIdentity::Player(lease) = self.source.identity else {
            return false;
        };
        let eye_height = f64::from_bits(self.eye_height);
        let reach = f64::from_bits(self.reach);
        let mut eye = self.source.position.map(f64::from_bits);
        eye[1] += eye_height;
        let minimum = self.bounds_minimum.map(f64::from_bits);
        let maximum = self.bounds_maximum.map(f64::from_bits);
        self.source.valid()
            && self.target.valid()
            && matches!(self.target.identity, ContactIdentity::Entity { .. })
            && id.actor == ActorId::Player(lease.gid)
            && id.incarnation == lease.incarnation
            && self.source.world == self.target.world
            && self.source.simulation_tick == tick
            && self.target.simulation_tick == tick
            && (0..3).all(|axis| {
                minimum[axis].is_finite()
                    && maximum[axis].is_finite()
                    && minimum[axis] <= maximum[axis]
            })
            && eye_height.is_finite()
            && (0.0..=f64::from_bits(self.source.height)).contains(&eye_height)
            && reach.is_finite()
            && (0.0..=1024.0).contains(&reach)
            && (0..3)
                .map(|axis| {
                    let closest = eye[axis].clamp(minimum[axis], maximum[axis]);
                    (eye[axis] - closest).powi(2)
                })
                .sum::<f64>()
                <= reach * reach
    }

    pub fn property(&self) -> ObjectProperty {
        match self.operation {
            ObjectOperation::RotateFrame => ObjectProperty::FrameRotation,
            ObjectOperation::RecordAttack => ObjectProperty::InteractionAttack,
            ObjectOperation::RecordInteraction => ObjectProperty::InteractionUse,
        }
    }

    pub fn facet(&self, property: ObjectProperty) -> Target {
        let ContactIdentity::Entity { id, .. } = self.target.identity else {
            return self.target.target();
        };
        Target::Chunk {
            key: self.target.chunk,
            part: ChunkPart::ObjectProperty {
                actor: id,
                property,
            },
            inventory: None,
        }
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> {
        let mut targets = vec![self.facet(self.property()), self.reach_target()];
        if self.operation == ObjectOperation::RotateFrame {
            targets.push(self.facet(ObjectProperty::FramePolicy));
            targets.push(self.facet(ObjectProperty::Attachment));
            targets.push(self.frame_inventory());
        } else {
            targets.push(self.facet(ObjectProperty::InteractionBounds));
        }
        targets.into_iter()
    }

    pub fn reach_target(&self) -> Target {
        Target::Chunk { key: self.source.chunk,
            part: ChunkPart::LivingProperty { actor: self.source.actor(),
                property: crate::living_property::LivingProperty::Attribute(self.reach_attribute) },
            inventory: None }
    }

    pub fn frame_inventory(&self) -> Target {
        let ContactIdentity::Entity { id, .. } = self.target.identity else {
            return self.target.target();
        };
        Target::Chunk {
            key: self.target.chunk,
            part: ChunkPart::Entity(id),
            inventory: Some(InventoryId::Entity {
                inv: crate::inventory::INV_MAIN,
                slot: 0,
            }),
        }
    }

    pub fn guards(&self) -> Vec<Condition> {
        let ContactIdentity::Entity { birth, .. } = &self.target.identity else {
            return Vec::new();
        };
        vec![
            Condition {
                target: self.source.target(),
                expected: Expected::SpatialEpochEquals(self.source.epoch),
            },
            Condition {
                target: self.target.target(),
                expected: Expected::EntityBirthOwned {
                    birth: birth.clone(),
                    owner: self.target.owner,
                },
            },
            Condition {
                target: self.target.target(),
                expected: Expected::SpatialEpochEquals(self.target.epoch),
            },
        ]
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        self.valid(action.id, action.tick)
            && action.actor_chunk == Some(self.source.chunk)
            && self
                .guards()
                .iter()
                .all(|guard| action.conditions.contains(guard))
            && matches!(self.source.identity, ContactIdentity::Player(lease) if action.player_leases.contains(&lease))
    }

    pub fn derive(
        &self,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let reach_target = self.reach_target();
        let Some(Cell { value: Value::LivingProperty(
            crate::living_property::LivingPropertyValue::Attribute { base: Some(base), modifiers, .. }), .. })
            = values.get(&reach_target) else { return Err(ApplyFailure::Type(reach_target)); };
        let equipment = crate::combat_action::CombatEquipmentFacts {
            modifiers: modifiers.clone(), ..Default::default()
        };
        let mut resolved = self.clone();
        resolved.reach = equipment.attribute(self.reach_attribute, f64::from_bits(*base)).to_bits();
        if !resolved.valid(id, tick) { return Err(ApplyFailure::Condition(reach_target)); }
        let target = self.facet(self.property());
        let Some(Cell {
            value: Value::ObjectProperty(current),
            ..
        }) = values.get(&target)
        else {
            return Err(ApplyFailure::Condition(target));
        };
        let next = match (&self.operation, current) {
            (ObjectOperation::RotateFrame, ObjectPropertyValue::Rotation(rotation)) => {
                if !matches!(values.get(&self.frame_inventory()), Some(Cell { value: Value::Inventory(item), .. }) if !item.is_empty())
                {
                    return Err(ApplyFailure::Condition(self.frame_inventory()));
                }
                let policy = self.facet(ObjectProperty::FramePolicy);
                if !matches!(
                    values.get(&policy),
                    Some(Cell {
                        value: Value::ObjectProperty(ObjectPropertyValue::FramePolicy {
                            fixed: false,
                            ..
                        }),
                        ..
                    })
                ) {
                    return Err(ApplyFailure::Condition(policy));
                }
                ObjectPropertyValue::Rotation((rotation + 1) % 8)
            }
            (
                ObjectOperation::RecordAttack | ObjectOperation::RecordInteraction,
                ObjectPropertyValue::InteractionRecord(_),
            ) => {
                let ContactIdentity::Player(lease) = self.source.identity else {
                    return Err(ApplyFailure::InvalidAction);
                };
                ObjectPropertyValue::InteractionRecord(Some(InteractionRecord {
                    player: lease.key.0,
                    tick,
                }))
            }
            _ => return Err(ApplyFailure::Type(target)),
        };
        Ok(vec![Effect::Put {
            target,
            value: Value::ObjectProperty(next),
        }])
    }

    pub fn capture<S: ActionState>(
        self,
        owners: &S,
        capture: InventoryActionCapture,
    ) -> Result<Action, ApplyFailure> {
        if !self.valid(capture.id, capture.tick) || capture.actor_chunk != Some(self.source.chunk) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut values = BTreeMap::new();
        let mut conditions = capture.guards;
        conditions.extend(self.guards());
        let targets = self.targets().collect::<Vec<_>>();
        for target in &targets {
            let cell = owners
                .read(TruthView::Local, *target)
                .ok_or(ApplyFailure::Condition(*target))?;
            conditions.push(Condition {
                target: *target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(*target, cell);
        }
        let effects = self.derive(capture.id, capture.tick, &values)?;
        let counterfactual_effects = vec![self.facet(self.property())];
        let mut stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(targets.iter().map(|target| target.stakeholder()))
            .collect::<BTreeSet<_>>();
        stakeholders.extend(
            capture
                .player_chunks
                .iter()
                .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
        );
        let mut dependencies = BTreeSet::new();
        for epoch in [self.source.epoch, self.target.epoch] {
            if let SpatialEpoch::Action(reference) = epoch {
                dependencies.insert(reference);
            }
        }
        let action = Action {
            edit_authority: None,
            id: capture.id,
            tick: capture.tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: capture.actor_chunk,
            player_leases: capture.player_leases,
            player_chunks: capture.player_chunks,
            stakeholders: stakeholders.into_iter().collect(),
            dependencies: dependencies.into_iter().collect(),
            conditions,
            effects,
            counterfactual_conditions: targets,
            counterfactual_effects,
            semantic_intent: Some(SemanticIntent::Object(self)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            client_origin: capture.client_origin,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}
