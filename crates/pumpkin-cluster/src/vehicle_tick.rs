use crate::canonical_action::*;
use crate::entity_body::EntityBody;
use crate::time::TickStamp;
use crate::vehicle_property::{VehicleProperty as P, VehiclePropertyValue as V};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleTickIntent {
    pub body: Target,
    pub birth: crate::entity_body::EntityBirth,
    pub owner: crate::realm_completion::PeerGeneration,
    pub epoch: SpatialEpoch,
    pub activation: Option<VehicleActivation>,
}

impl VehicleTickIntent {
    fn lifecycle_targets(&self) -> Vec<Target> {
        let Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            inventory: None,
        } = self.body
        else {
            return Vec::new();
        };
        [
            P::LifecycleTick,
            P::Fuel,
            P::FurnacePush,
            P::Fuse,
            P::Damage,
            P::Wobble,
        ]
        .into_iter()
        .map(|property| Target::Chunk {
            key,
            part: ChunkPart::VehicleProperty { actor, property },
            inventory: None,
        })
        .collect()
    }

    pub fn derive(
        &self,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        let mut effects = Vec::new();
        for target in self.lifecycle_targets() {
            let Target::Chunk {
                part: ChunkPart::VehicleProperty { property, .. },
                ..
            } = target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let Some(Value::VehicleProperty(value)) = values.get(&target).map(|cell| &cell.value)
            else {
                return Err(ApplyFailure::Type(target));
            };
            let next = match (property, value) {
                (P::LifecycleTick, V::Tick(previous)) => {
                    if previous.is_some_and(|previous| !tick.is_after(previous)) {
                        return Ok(Vec::new());
                    }
                    V::Tick(Some(tick))
                }
                (P::Fuel, V::Unsigned(value)) => V::Unsigned(value.saturating_sub(1)),
                (P::FurnacePush, V::Push(push)) => {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::VehicleProperty { actor, .. },
                        ..
                    } = target
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    let fuel_target = Target::Chunk {
                        key,
                        part: ChunkPart::VehicleProperty {
                            actor,
                            property: P::Fuel,
                        },
                        inventory: None,
                    };
                    let Some(Cell {
                        value: Value::VehicleProperty(V::Unsigned(fuel)),
                        ..
                    }) = values.get(&fuel_target)
                    else {
                        return Err(ApplyFailure::Type(fuel_target));
                    };
                    V::Push(if *fuel <= 1 { [0; 2] } else { *push })
                }
                (P::Damage, V::Unsigned(value)) => V::Unsigned(if *value > 40_000 {
                    *value
                } else {
                    value.saturating_sub(1000)
                }),
                (P::Fuse, V::Fuse(value)) => V::Fuse(value.map(|value| value.saturating_sub(1))),
                (P::Wobble, V::Wobble { ticks, side }) => V::Wobble {
                    ticks: ticks.saturating_sub(1),
                    side: *side,
                },
                _ => return Err(ApplyFailure::InvalidAction),
            };
            if &next != value {
                effects.push(Effect::Put {
                    target,
                    value: Value::VehicleProperty(next),
                });
            }
        }
        Ok(effects)
    }

    pub fn capture_activated<S: ActionState>(
        state: &S,
        body: &EntityBody,
        chunk: crate::protocol::ChunkKey,
        id: ActionId,
        tick: TickStamp,
        activation: Option<VehicleActivation>,
    ) -> Result<Action, ApplyFailure> {
        if id.actor != crate::actor::ActorId::Entity(body.id)
            || id.incarnation != body.id.generation
            || body.owner.generation == 0
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let intent = Self {
            body: Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            },
            birth: body.birth.clone(),
            owner: body.owner,
            epoch: body.spatial_epoch,
            activation,
        };
        let mut values = BTreeMap::new();
        let mut conditions = vec![
            Condition {
                target: intent.body,
                expected: Expected::ActorControlled {
                    birth: body.birth.clone(),
                    issuer: body.owner,
                },
            },
            Condition {
                target: intent.body,
                expected: Expected::SpatialEpochEquals(body.spatial_epoch),
            },
        ];
        for target in intent.targets() {
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            conditions.push(Condition {
                target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(target, cell);
        }
        if let Some(plan) = intent
            .activation
            .as_ref()
            .and_then(|activation| activation.detach.as_ref())
        {
            for condition in &plan.conditions {
                if !conditions.contains(condition) {
                    conditions.push(condition.clone());
                }
            }
        }
        let effects = intent.derive_with(state, tick, &values)?;
        let footprint = intent.targets();
        let player_leases = intent
            .activation
            .as_ref()
            .and_then(|activation| activation.detach.as_ref())
            .map_or_else(Vec::new, |plan| plan.leases.clone());
        let mut stakeholders = footprint
            .iter()
            .map(|target| target.stakeholder())
            .collect::<Vec<_>>();
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let mut dependencies = conditions
            .iter()
            .filter_map(|condition| match condition.expected {
                Expected::SpatialEpochEquals(SpatialEpoch::Action(reference)) => Some(reference),
                _ => None,
            })
            .collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies.dedup();
        let action = Action {
            edit_authority: None,
            id,
            tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            player_leases,
            player_chunks: Vec::new(),
            stakeholders,
            dependencies,
            conditions,
            effects,
            counterfactual_conditions: footprint.clone(),
            counterfactual_effects: footprint,
            semantic_intent: Some(SemanticIntent::VehicleTick(intent)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleActivation {
    pub detach: Option<crate::actor_relation::OwnerDetachPlan>,
    pub block: Target,
    pub kind: crate::entity_body::VehicleKind,
    pub position: [u64; 3],
}

impl VehicleTickIntent {
    pub fn targets(&self) -> Vec<Target> {
        let mut targets = self.lifecycle_targets();
        if let Some(activation) = &self.activation {
            targets.extend([self.body, activation.block]);
            if let Some(plan) = &activation.detach {
                targets.extend(plan.conditions.iter().map(|condition| condition.target));
                targets.extend(plan.effects.iter().flat_map(Effect::targets));
            }
            if activation.kind == crate::entity_body::VehicleKind::HopperMinecart {
                if let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(actor),
                    ..
                } = self.body
                {
                    targets.push(Target::Chunk {
                        key,
                        part: ChunkPart::VehicleProperty {
                            actor,
                            property: P::HopperEnabled,
                        },
                        inventory: None,
                    });
                }
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    pub fn activation_valid(&self) -> bool {
        let Some(activation) = &self.activation else {
            return true;
        };
        let point = activation.position.map(f64::from_bits);
        if !point
            .iter()
            .all(|axis| axis.is_finite() && axis.abs() <= 30_000_000.0)
        {
            return false;
        }
        matches!((self.body, activation.block), (Target::Chunk { key, part: ChunkPart::Entity(_), inventory: None },
            Target::Chunk { key: block_chunk, part: ChunkPart::Block(position), inventory: None })
            if key.world == block_chunk.world && position.x.div_euclid(16) == block_chunk.x() && position.z.div_euclid(16) == block_chunk.z()
                && position.x == point[0].floor() as i32 && position.z == point[2].floor() as i32
                && (position.y == point[1].floor() as i32 || position.y == (point[1]-0.001).floor() as i32))
            && match activation.kind {
                crate::entity_body::VehicleKind::HopperMinecart
                | crate::entity_body::VehicleKind::TntMinecart => activation.detach.is_none(),
                crate::entity_body::VehicleKind::Minecart => activation.detach.is_some(),
                _ => false,
            }
    }

    pub fn derive_with<S: ActionState>(
        &self,
        state: &S,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.activation_valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut effects = self.derive(tick, values)?;
        if effects.is_empty() {
            return Ok(effects);
        }
        let Some(activation) = &self.activation else {
            return Ok(effects);
        };
        let Some(Value::Entity(body)) = values.get(&self.body).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Type(self.body));
        };
        let crate::entity_body::EntityPayload::Vehicle(vehicle) = &body.payload else {
            return Err(ApplyFailure::Type(self.body));
        };
        if vehicle.kind != activation.kind || body.birth != self.birth || body.owner != self.owner {
            return Err(ApplyFailure::InvalidAction);
        }
        let Some(Value::Block(block)) = values.get(&activation.block).map(|cell| &cell.value)
        else {
            return Err(ApplyFailure::Type(activation.block));
        };
        let Some(powered) = state.vehicle_activator(*block)? else {
            return Ok(effects);
        };
        if vehicle.kind == crate::entity_body::VehicleKind::Minecart {
            let plan = activation
                .detach
                .as_ref()
                .ok_or(ApplyFailure::InvalidAction)?;
            if !self.detach_valid(plan, body, tick) {
                return Err(ApplyFailure::InvalidAction);
            }
            for condition in &plan.conditions {
                if !condition.expected.matches(values.get(&condition.target)) {
                    return Err(ApplyFailure::Condition(condition.target));
                }
            }
            if powered {
                effects.extend(plan.effects.clone());
            }
            return Ok(effects);
        }
        let Target::Chunk {
            key,
            part: ChunkPart::Entity(actor),
            ..
        } = self.body
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if vehicle.kind == crate::entity_body::VehicleKind::HopperMinecart {
            let target = Target::Chunk {
                key,
                part: ChunkPart::VehicleProperty {
                    actor,
                    property: P::HopperEnabled,
                },
                inventory: None,
            };
            let Some(Value::VehicleProperty(V::Boolean(enabled))) =
                values.get(&target).map(|cell| &cell.value)
            else {
                return Err(ApplyFailure::Type(target));
            };
            if *enabled == powered {
                effects.push(Effect::Put {
                    target,
                    value: Value::VehicleProperty(V::Boolean(!powered)),
                });
            }
        } else if powered {
            let target = Target::Chunk {
                key,
                part: ChunkPart::VehicleProperty {
                    actor,
                    property: P::Fuse,
                },
                inventory: None,
            };
            let Some(Value::VehicleProperty(V::Fuse(fuse))) =
                values.get(&target).map(|cell| &cell.value)
            else {
                return Err(ApplyFailure::Type(target));
            };
            if fuse.is_none() {
                effects.push(Effect::Put {
                    target,
                    value: Value::VehicleProperty(V::Fuse(Some(80))),
                });
            }
        }
        Ok(effects)
    }

    pub fn capture<S: ActionState>(
        state: &S,
        body: &EntityBody,
        chunk: crate::protocol::ChunkKey,
        id: ActionId,
        tick: TickStamp,
    ) -> Result<Action, ApplyFailure> {
        Self::capture_activated(state, body, chunk, id, tick, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> VehicleTickIntent {
        let server = crate::identity::ServerId(1);
        let actor = crate::protocol::GlobalEntityId {
            origin: server,
            local_id: 7,
            generation: 1,
        };
        let source = crate::realm_completion::PeerGeneration {
            server,
            generation: 1,
        };
        let reference = ActionRef {
            tick: TickStamp(1),
            id: ActionId {
                actor: crate::actor::ActorId::Entity(actor),
                incarnation: 1,
                sequence: 1,
                family: 1,
            },
        };
        VehicleTickIntent {
            body: Target::Chunk {
                key: crate::protocol::ChunkKey::new(crate::protocol::WorldId::new(0, 1), 0, 0),
                part: ChunkPart::Entity(actor),
                inventory: None,
            },
            birth: crate::entity_body::EntityBirth {
                action: reference,
                source,
                uuid: uuid::Uuid::nil(),
            },
            owner: source,
            epoch: SpatialEpoch::Action(reference),
            activation: None,
        }
    }

    fn values(intent: &VehicleTickIntent, fuel: u32, damage: u32) -> BTreeMap<Target, Cell> {
        intent
            .lifecycle_targets()
            .into_iter()
            .zip([
                V::Tick(Some(TickStamp(1))),
                V::Unsigned(fuel),
                V::Push([20_000, -30_000]),
                V::Fuse(Some(2)),
                V::Unsigned(damage),
                V::Wobble { ticks: 2, side: -1 },
            ])
            .map(|(target, value)| {
                (
                    target,
                    Cell {
                        revision: 1,
                        value: Value::VehicleProperty(value),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn furnace_expiry_uses_own_fuel_even_when_other_vehicle_has_fuel() {
        let intent = intent();
        let mut values = values(&intent, 1, 0);
        let Target::Chunk { key, .. } = intent.body else {
            unreachable!()
        };
        values.insert(
            Target::Chunk {
                key,
                part: ChunkPart::VehicleProperty {
                    actor: crate::protocol::GlobalEntityId {
                        origin: crate::identity::ServerId(1),
                        local_id: 1,
                        generation: 1,
                    },
                    property: P::Fuel,
                },
                inventory: None,
            },
            Cell {
                revision: 1,
                value: Value::VehicleProperty(V::Unsigned(300)),
            },
        );
        let effects = intent.derive(TickStamp(2), &values).unwrap();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Put {
                target: Target::Chunk {
                    part: ChunkPart::VehicleProperty {
                        property: P::FurnacePush,
                        ..
                    },
                    ..
                },
                value: Value::VehicleProperty(V::Push([0, 0]))
            }
        )));
    }

    #[test]
    fn lifecycle_is_once_per_tick_and_does_not_heal_destroyed_vehicle() {
        let intent = intent();
        let original = values(&intent, 3, 40_001);
        let effects = intent.derive(TickStamp(2), &original).unwrap();
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Put {
                target: Target::Chunk {
                    part: ChunkPart::VehicleProperty {
                        property: P::Damage,
                        ..
                    },
                    ..
                },
                ..
            }
        )));
        let mut after = original.clone();
        for effect in &effects {
            let Effect::Put { target, value } = effect else {
                unreachable!()
            };
            after.insert(
                *target,
                Cell {
                    revision: 2,
                    value: value.clone(),
                },
            );
        }
        assert!(intent.derive(TickStamp(2), &after).unwrap().is_empty());
        assert_eq!(intent.derive(TickStamp(2), &original).unwrap(), effects);
    }
}

impl VehicleTickIntent {
    fn detach_valid(
        &self,
        plan: &crate::actor_relation::OwnerDetachPlan,
        body: &EntityBody,
        tick: TickStamp,
    ) -> bool {
        use crate::actor_relation::ActorRelation;
        use crate::entity_body::{EntityPayload, VehicleKind};
        let Target::Chunk {
            key,
            part: ChunkPart::Entity(entity),
            inventory: None,
        } = self.body
        else {
            return false;
        };
        if body.id != entity
            || body.birth != self.birth
            || body.owner != self.owner
            || body.spatial_epoch != self.epoch
            || body.mount.is_some()
            || body.passengers.is_empty()
            || !body.belongs_to(entity, key)
            || !body.valid_at(tick)
            || !matches!(&body.payload, EntityPayload::Vehicle(vehicle) if vehicle.kind == VehicleKind::Minecart && vehicle.damage_milli <= 40_000)
        {
            return false;
        }
        let exact = |relation, identity| {
            let target = Target::Chunk {
                key,
                part: ChunkPart::ActorRelation {
                    actor: crate::actor::ActorId::Entity(entity),
                    relation,
                },
                inventory: None,
            };
            plan.conditions.iter().any(|condition| condition.target == target && matches!(&condition.expected,
                Expected::Exact(Cell { value: Value::ActorRelation(actual), .. }) if *actual == identity))
        };
        if !exact(ActorRelation::Mount, None)
            || !(0..32).all(|seat| {
                exact(
                    ActorRelation::Passenger(seat),
                    body.passengers.get(&seat).cloned(),
                )
            })
            || !plan.conditions.contains(&Condition {
                target: self.body,
                expected: Expected::EntityBirthOwned {
                    birth: self.birth.clone(),
                    owner: self.owner,
                },
            })
            || !plan.conditions.contains(&Condition {
                target: self.body,
                expected: Expected::SpatialEpochEquals(self.epoch),
            })
        {
            return false;
        }
        let action = Action {
            edit_authority: None,
            id: ActionId {
                actor: crate::actor::ActorId::Entity(entity),
                incarnation: entity.generation,
                sequence: 0,
                family: 15,
            },
            tick,
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(key),
            player_leases: plan.leases.clone(),
            player_chunks: Vec::new(),
            stakeholders: plan
                .chunks
                .iter()
                .copied()
                .map(Stakeholder::Chunk)
                .collect(),
            dependencies: Vec::new(),
            conditions: plan.conditions.clone(),
            effects: plan.effects.clone(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        crate::actor_relation::owner_detach_all(&action)
    }
}

pub fn valid_activator_detach(action: &Action) -> bool {
    let Some(SemanticIntent::VehicleTick(intent)) = &action.semantic_intent else {
        return false;
    };
    let Some(activation) = &intent.activation else {
        return false;
    };
    let Some(plan) = &activation.detach else {
        return false;
    };
    let Some(body) = action
        .conditions
        .iter()
        .find_map(|condition| match &condition.expected {
            Expected::Exact(Cell {
                value: Value::Entity(body),
                ..
            }) if condition.target == intent.body => Some(body),
            _ => None,
        })
    else {
        return false;
    };
    let footprint = intent.targets();
    let relation_effects = action
        .effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::Put {
                    target: Target::Chunk {
                        part: ChunkPart::ActorRelation { .. },
                        ..
                    },
                    ..
                }
            )
        })
        .collect::<Vec<_>>();
    action.class == ActionClass::Replayable
        && action.kind == ActionKind::Actor
        && intent.activation_valid()
        && action.id.actor == crate::actor::ActorId::Entity(body.id)
        && action.id.incarnation == body.id.generation
        && intent.detach_valid(plan, body, action.tick)
        && action.player_leases == plan.leases
        && plan
            .conditions
            .iter()
            .all(|condition| action.conditions.contains(condition))
        && footprint.iter().all(|target| {
            action.counterfactual_conditions.contains(target)
                && action.counterfactual_effects.contains(target)
        })
        && (relation_effects.is_empty()
            || relation_effects == plan.effects.iter().collect::<Vec<_>>())
}

pub fn permits_activator_detach(action: &Action) -> bool {
    valid_activator_detach(action)
        && action.effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::Put {
                    target: Target::Chunk {
                        part: ChunkPart::ActorRelation { .. },
                        ..
                    },
                    ..
                }
            )
        })
}
