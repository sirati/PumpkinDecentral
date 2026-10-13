use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorId,
    actor_occupancy::{ActorOccupancyWitness, CollisionBox},
    canonical_action::*,
    canonical_inventory::{InventorySlotBinding, ItemTransformIntent, ItemTransformOperation},
    entity_body::{EntityBody, EntityPayload},
    entity_inventory_admission::EntityInventoryAdmission,
    item_contact::{ContactIdentity, ContactObservation},
    object_state::ObjectKind,
    protocol::{BlockPos, ChunkKey},
    time::TickStamp,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityPlacementGeometry {
    Clicked {
        block: BlockPos,
        face: u8,
    },
    BoatRay {
        start: [u64; 3],
        end: [u64; 3],
        block: BlockPos,
        face: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityPlacementIntent {
    pub rules: crate::world_rules::WorldRulesWitness,
    pub source: ContactObservation,
    pub source_slot: InventorySlotBinding,
    pub equipment: crate::canonical_inventory::InventoryEquipmentBinding,
    pub source_cell: Cell,
    pub selected: (Target, Cell),
    pub eye_height: u64,
    pub yaw: u32,
    pub pitch: u32,
    pub reach: u64,
    pub reach_attribute: u16,
    pub geometry: EntityPlacementGeometry,
    pub body: EntityBody,
    pub inventory: EntityInventoryAdmission,
    pub collision: CollisionBox,
    pub blocks: Vec<(Target, Cell)>,
    pub occupancy: Vec<ActorOccupancyWitness>,
}

impl EntityPlacementIntent {
    pub fn target(&self) -> Target {
        let at = self.body.spatial.position.map(f64::from_bits);
        Target::Chunk {
            key: ChunkKey::new(
                self.body.spatial.world,
                (at[0].floor() as i32).div_euclid(16),
                (at[2].floor() as i32).div_euclid(16),
            ),
            part: ChunkPart::Entity(self.body.id),
            inventory: None,
        }
    }

    pub fn source_property(&self, property: crate::living_property::LivingProperty) -> Target {
        Target::Chunk {
            key: self.source.chunk,
            part: ChunkPart::LivingProperty {
                actor: self.source.actor(),
                property,
            },
            inventory: None,
        }
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        let ContactIdentity::Player(lease) = self.source.identity else {
            return false;
        };
        let Value::Inventory(item) = &self.source_cell.value else {
            return false;
        };
        let Target::Chunk { key, .. } = self.target() else {
            return false;
        };
        self.source.valid() && f32::from_bits(self.yaw).is_finite() && f32::from_bits(self.pitch).is_finite()
            && f64::from_bits(self.reach).is_finite() && (0.0..=64.0).contains(&f64::from_bits(self.reach)) && self.source.simulation_tick == tick
            && id.actor == ActorId::Player(lease.gid) && id.incarnation == lease.incarnation
            && self.body.birth.action == (ActionRef { id, tick }) && self.body.birth.source == self.source.owner
            && self.body.owner == self.source.owner && self.body.id.origin == self.source.owner.server
            && self.body.id.generation == self.source.owner.generation && !self.body.birth.uuid.is_nil()
            && self.body.spatial_epoch == SpatialEpoch::Action(self.body.birth.action)
            && self.body.belongs_to(self.body.id, key) && self.body.valid_at(tick)
            && self.rules.rules.world == self.source.world && self.rules.slot as u16 == tick.0
            && self.body.spatial.world == self.source.world
            && self.body.mount.is_none() && self.body.passengers.is_empty()
            && self.inventory.valid() && self.source_slot.is_valid() && self.equipment.is_valid()
            && self.equipment.lease == lease && self.equipment.living == self.source.target() && item.count > 0
            && item.loc == self.source_slot.slot.loc
            && self.source_slot.slot.target == crate::inventory::InventoryTarget::Player(lease.gid)
            && matches!(self.source_slot.target, Target::Player { key, lease: Some(actual), inventory: Some(_) }
                if key == lease.key && actual == lease)
            && matches!(item.loc.inv, crate::inventory::INV_MAIN | crate::inventory::INV_OFFHAND)
            && matches!(&self.selected, (Target::Player { key, lease: Some(actual), inventory: Some(InventoryId::Player { inv, slot: 0 }) }, Cell { value: Value::Inventory(selected), .. })
                if *key == lease.key && *actual == lease && *inv == crate::inventory::INV_SELECTED
                    && selected.count == 1 && selected.nbt.is_empty() && (1..=9).contains(&selected.item)
                    && (item.loc.inv == crate::inventory::INV_OFFHAND || item.loc.slot == selected.item - 1))
            && self.collision.valid() && !self.blocks.is_empty() && self.blocks.len() <= 4096
            && self.blocks.windows(2).all(|pair| pair[0].0 < pair[1].0)
            && self.blocks.iter().all(|(target, cell)| matches!((target, &cell.value),
                (Target::Chunk { key, part: ChunkPart::Block(_), inventory: None }, Value::Block(_)) if key.world == self.source.world))
            && !self.occupancy.is_empty() && self.occupancy.len() <= 64 && self.occupancy.iter().all(ActorOccupancyWitness::valid)
            && self.occupancy.windows(2).all(|pair| pair[0].key < pair[1].key)
            && self.occupancy.iter().all(|witness| witness.blocks == [self.collision.clone()])
            && self.occupancy.iter().map(|witness| witness.key).collect::<BTreeSet<_>>() == self.occupancy_keys()
            && f64::from_bits(self.eye_height).is_finite()
            && (0.0..=f64::from_bits(self.source.height)).contains(&f64::from_bits(self.eye_height))
    }

    pub fn occupancy_keys(&self) -> BTreeSet<ChunkKey> {
        let minimum = self.collision.bounds[0].map(f64::from_bits);
        let maximum = self.collision.bounds[1].map(f64::from_bits);
        let min_x = (minimum[0].floor() as i32).div_euclid(16) - 64;
        let max_x = (maximum[0].floor() as i32).div_euclid(16) + 64;
        let min_z = (minimum[2].floor() as i32).div_euclid(16) - 64;
        let max_z = (maximum[2].floor() as i32).div_euclid(16) + 64;
        if max_x - min_x > 7 || max_z - min_z > 7 {
            return BTreeSet::new();
        }
        (min_x..=max_x)
            .flat_map(|x| (min_z..=max_z).map(move |z| ChunkKey::new(self.source.world, x, z)))
            .collect()
    }

    pub fn matches_source(&self, actual: Option<&Cell>) -> bool {
        matches!(actual, Some(Cell { value: Value::Living(living), .. })
            if living.avatar.as_ref().is_some_and(|avatar|
                avatar.world == self.source.world && avatar.spatial_epoch == self.source.epoch))
    }

    pub fn guards(&self) -> Vec<Condition> {
        let hanging = matches!(&self.body.payload, EntityPayload::Object(object)
            if matches!(object.kind, ObjectKind::ItemFrame(_) | ObjectKind::Painting(_)));
        let stand =
            matches!(&self.body.payload, EntityPayload::Living(living) if living.stand.is_some());
        let mut guards = vec![
            Condition {
                target: self.selected.0,
                expected: Expected::Exact(self.selected.1.clone()),
            },
            Condition {
                target: self.source_slot.target,
                expected: Expected::Exact(self.source_cell.clone()),
            },
            Condition {
                target: self.source.target(),
                expected: Expected::SpatialEpochEquals(self.source.epoch),
            },
            Condition {
                target: self.target(),
                expected: Expected::Missing,
            },
        ];
        guards.extend(
            self.inventory
                .entries(self.spawn_chunk(), self.body.id)
                .map(|(target, _)| Condition {
                    target,
                    expected: Expected::Missing,
                }),
        );
        guards.extend(self.blocks.iter().map(|(target, cell)| Condition {
            target: *target,
            expected: Expected::Exact(cell.clone()),
        }));
        guards.extend(self.occupancy.iter().map(|witness| Condition {
            target: Target::Chunk {
                key: witness.key,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            },
            expected: if hanging {
                Expected::HangingOccupancyClear(witness.clone())
            } else if stand {
                Expected::EntitiesOccupancyClear(witness.clone())
            } else {
                Expected::ActorOccupancyClear(witness.clone())
            },
        }));
        guards
    }

    pub fn effects<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        state.world_rules(self.rules, tick)?;
        if !self.valid(id, tick) || !state.entity_placement_matches(self) {
            return Err(ApplyFailure::InvalidAction);
        }
        let read = |property| {
            let target = self.source_property(property);
            state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))
        };
        if !matches!(read(P::Health)?.value, Value::LivingProperty(V::Unsigned(health)) if health > 0)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let Value::LivingProperty(V::Unsigned(mode @ 0..=2)) = read(P::GameMode)?.value else {
            return Err(ApplyFailure::InvalidAction);
        };
        let Value::LivingProperty(V::Attribute {
            base: Some(base),
            modifiers,
            ..
        }) = read(P::Attribute(self.reach_attribute))?.value
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let reach = crate::combat_action::CombatEquipmentFacts {
            modifiers,
            ..Default::default()
        }
        .attribute(self.reach_attribute, f64::from_bits(base));
        let eye = self.source.position.map(f64::from_bits);
        let eye = [eye[0], eye[1] + f64::from_bits(self.eye_height), eye[2]];
        let block = match self.geometry {
            EntityPlacementGeometry::Clicked { block, .. }
            | EntityPlacementGeometry::BoatRay { block, .. } => block,
        };
        if reach.to_bits() != self.reach
            || !reach.is_finite()
            || !(0.0..=64.0).contains(&reach)
            || (0..3)
                .map(|axis| {
                    let min = f64::from([block.x, block.y, block.z][axis]);
                    (eye[axis] - eye[axis].clamp(min, min + 1.0)).powi(2)
                })
                .sum::<f64>()
                > reach * reach
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut effects = Vec::new();
        if mode != 1 {
            let Value::Inventory(item) = &self.source_cell.value else {
                return Err(ApplyFailure::InvalidAction);
            };
            effects.push(Effect::TransformItems {
                intent: ItemTransformIntent {
                    source: self.source_slot.clone(),
                    item: item.item,
                    components: item.nbt.clone(),
                    operation: ItemTransformOperation::Consume { count: 1 },
                    remainder_destinations: Vec::new(),
                },
            });
        }
        if mode != 1 {
            let mut values = std::collections::BTreeMap::new();
            for target in self.equipment.targets() {
                values.insert(
                    target,
                    state
                        .read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?,
                );
            }
            let mut projected = values.clone();
            for effect in &effects {
                if let Effect::TransformItems { intent } = effect {
                    for (target, item) in intent.derive(state, id, tick, &values)? {
                        projected
                            .get_mut(&target)
                            .ok_or(ApplyFailure::Condition(target))?
                            .value = Value::Inventory(item);
                    }
                }
            }
            crate::canonical_inventory::append_equipment_effects_from_projection(
                &values,
                &projected,
                &mut effects,
                std::slice::from_ref(&self.equipment),
                state,
            )
            .map_err(|_| ApplyFailure::InvalidAction)?;
        }
        effects.push(Effect::EntityAdmission {
            target: self.target(),
            body: self.body.clone(),
            inventory: self.inventory.clone(),
        });
        Ok(effects)
    }

    pub fn capture<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
        client_origin: ClientOrigin,
    ) -> Result<Action, ApplyFailure> {
        use crate::living_property::LivingProperty as P;
        let ContactIdentity::Player(lease) = self.source.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        let mut conditions = self.guards();
        for property in [P::Health, P::GameMode, P::Attribute(self.reach_attribute)] {
            let target = self.source_property(property);
            conditions.push(Condition {
                target,
                expected: Expected::Exact(
                    state
                        .read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?,
                ),
            });
        }
        for target in self.equipment.targets() {
            let guard = Condition {
                target,
                expected: Expected::Exact(
                    state
                        .read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?,
                ),
            };
            if !conditions.contains(&guard) {
                conditions.push(guard);
            }
        }
        conditions.push(Condition {
            target: self.source.target(),
            expected: Expected::EntityPlacement(Box::new(self.clone())),
        });
        let effects = self.effects(state, id, tick)?;
        let stakeholders = conditions
            .iter()
            .map(|guard| guard.target.stakeholder())
            .chain(
                effects
                    .iter()
                    .flat_map(Effect::targets)
                    .map(Target::stakeholder),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let action = Action {
            id,
            tick,
            edit_authority: None,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(self.source.chunk),
            player_leases: vec![lease],
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects,
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Item,
            client_origin,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}

pub fn witness(action: &Action) -> Option<&EntityPlacementIntent> {
    action
        .conditions
        .iter()
        .find_map(|condition| match &condition.expected {
            Expected::EntityPlacement(witness) => Some(witness.as_ref()),
            _ => None,
        })
}

pub fn permits_admission(
    action: &Action,
    target: Target,
    body: &EntityBody,
    inventory: &EntityInventoryAdmission,
) -> bool {
    witness(action).is_some_and(|witness| {
        witness.valid(action.id, action.tick)
            && witness.target() == target
            && witness.body == *body
            && witness.inventory == *inventory
            && action.kind == ActionKind::Item
            && action.class == ActionClass::Undoable
            && witness
                .guards()
                .iter()
                .all(|guard| action.conditions.contains(guard))
    })
}

impl EntityPlacementIntent {
    pub fn spawn_chunk(&self) -> ChunkKey {
        let Target::Chunk { key, .. } = self.target() else {
            unreachable!()
        };
        key
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        let ContactIdentity::Player(lease) = self.source.identity else {
            return false;
        };
        self.valid(action.id, action.tick) && action.class == ActionClass::Undoable && action.kind == ActionKind::Item
            && action.actor_chunk == Some(self.source.chunk) && !action.chunk_transition
            && action.player_leases.contains(&lease) && self.guards().iter().all(|guard| action.conditions.contains(guard))
            && action.conditions.iter().filter(|guard| matches!(guard.expected, Expected::EntityPlacement(_))).count() == 1
            && action.conditions.iter().any(|guard| guard.target == self.source.target() && matches!(&guard.expected, Expected::EntityPlacement(witness) if witness.as_ref() == self))
            && action.effects.iter().filter(|effect| matches!(effect, Effect::EntityAdmission { .. })).count() == 1
            && action.effects.iter().all(|effect| match effect {
                Effect::EntityAdmission { target, body, inventory } => *target == self.target() && *body == self.body && *inventory == self.inventory,
                Effect::Put { target, value: Value::LivingProperty(_) } => self.equipment.property_targets().contains(target),
                Effect::TransformItems { intent } => matches!(intent.operation, ItemTransformOperation::Consume { count: 1 })
                    && intent.source == self.source_slot && intent.remainder_destinations.is_empty()
                    && matches!(&self.source_cell.value, Value::Inventory(item) if intent.item == item.item && intent.components == item.nbt),
                _ => false,
            })
    }

    pub fn validate_context<S: ActionState>(
        &self,
        state: &S,
        action: &Action,
    ) -> Result<(), ApplyFailure> {
        use crate::living_property::LivingProperty as P;
        if !self.guards_valid(action)
            || self.effects(state, action.id, action.tick)? != action.effects
        {
            return Err(ApplyFailure::InvalidAction);
        }
        for target in self.equipment.targets() {
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            if !action.conditions.contains(&Condition {
                target,
                expected: Expected::Exact(cell),
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for property in [P::Health, P::GameMode, P::Attribute(self.reach_attribute)] {
            let target = self.source_property(property);
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            if !action.conditions.contains(&Condition {
                target,
                expected: Expected::Exact(cell),
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        Ok(())
    }
}
