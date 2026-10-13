use crate::entity_body::EntityPayload;
use crate::{
    object_property::ObjectPropertyValue,
    object_state::{ObjectKind, ObjectProperty, ObjectState},
    time::TickStamp,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTickPlan {
    pub elapsed: u16,
    pub updates: Vec<(ObjectProperty, ObjectPropertyValue)>,
    pub terminal: Option<ObjectTerminal>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectTerminal {
    Explosion {
        power: u32,
        igniter: Option<crate::item_contact::ContactIdentity>,
    },
    FallingExpired {
        block_state: u16,
        drop_item: bool,
    },
    FallingLanded {
        block_state: u16,
        drop_item: bool,
    },
}

pub fn capture(
    state: &ObjectState,
    birth_tick: TickStamp,
    tick: TickStamp,
    y: f64,
    minimum_y: i32,
    maximum_y: i32,
) -> Option<ObjectTickPlan> {
    if !state.valid() || !y.is_finite() || minimum_y >= maximum_y {
        return None;
    }
    let previous = state.simulation_tick.unwrap_or(birth_tick);
    if !tick.is_after(previous) {
        return None;
    }
    let elapsed = tick.distance_since(previous);
    let mut updates = Vec::new();
    let terminal = match &state.kind {
        ObjectKind::PrimedTnt(tnt) => {
            let fuse = tnt.fuse.saturating_sub(elapsed);
            if fuse != tnt.fuse {
                updates.push((ObjectProperty::Fuse, ObjectPropertyValue::Fuse(fuse)));
            }
            (fuse == 0).then(|| ObjectTerminal::Explosion {
                power: tnt.power,
                igniter: tnt.igniter.clone(),
            })
        }
        ObjectKind::FallingBlock(falling) => {
            let mut next = falling.clone();
            next.age = next.age.saturating_add(u32::from(elapsed));
            let expired = next.age > 600
                || next.age > 100 && (y < f64::from(minimum_y) || y > f64::from(maximum_y));
            updates.push((ObjectProperty::Falling, ObjectPropertyValue::Falling(next)));
            expired.then_some(ObjectTerminal::FallingExpired {
                block_state: falling.block_state,
                drop_item: falling.drop_item,
            })
        }
        _ => return None,
    };
    updates.push((
        ObjectProperty::SimulationTick,
        ObjectPropertyValue::Tick(Some(tick)),
    ));
    Some(ObjectTickPlan {
        elapsed,
        updates,
        terminal,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTerminalLeg {
    pub conditions: Vec<crate::canonical_action::Condition>,
    pub effects: Vec<crate::canonical_action::Effect>,
}

pub fn terminal_leg(
    body: &crate::entity_body::EntityBody,
    chunk: crate::protocol::ChunkKey,
    snapshot: &impl crate::canonical_action::ActionState,
    terminal: &ObjectTerminal,
) -> Result<ObjectTerminalLeg, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::*;
    let EntityPayload::Object(object) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let property = match (&object.kind, terminal) {
        (ObjectKind::PrimedTnt(tnt), ObjectTerminal::Explosion { power, igniter })
            if tnt.fuse == 0 && tnt.power == *power && tnt.igniter == *igniter =>
        {
            ObjectProperty::Fuse
        }
        (
            ObjectKind::FallingBlock(falling),
            ObjectTerminal::FallingExpired {
                block_state,
                drop_item,
            }
            | ObjectTerminal::FallingLanded {
                block_state,
                drop_item,
            },
        ) if falling.block_state == *block_state && falling.drop_item == *drop_item => {
            ObjectProperty::Falling
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let facet = Target::Chunk {
        key: chunk,
        part: ChunkPart::ObjectProperty {
            actor: body.id,
            property,
        },
        inventory: None,
    };
    let value = snapshot
        .read(TruthView::Local, facet)
        .ok_or(ApplyFailure::Condition(facet))?;
    Ok(ObjectTerminalLeg {
        conditions: vec![
            Condition {
                target,
                expected: Expected::ActorControlled {
                    birth: body.birth.clone(),
                    issuer: body.owner,
                },
            },
            Condition {
                target,
                expected: Expected::SpatialEpochEquals(body.spatial_epoch),
            },
            Condition {
                target: facet,
                expected: Expected::Exact(value),
            },
        ],
        effects: vec![Effect::Delete { target }],
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectTickKind {
    Tnt,
    Falling,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectTickIntent {
    pub entity: crate::protocol::GlobalEntityId,
    pub birth: crate::entity_body::EntityBirth,
    pub owner: crate::realm_completion::PeerGeneration,
    pub chunk: crate::protocol::ChunkKey,
    pub kind: ObjectTickKind,
    pub travel_down: u32,
}

impl ObjectTickIntent {
    pub fn target(&self, property: ObjectProperty) -> crate::canonical_action::Target {
        crate::canonical_action::Target::Chunk {
            key: self.chunk,
            part: crate::canonical_action::ChunkPart::ObjectProperty {
                actor: self.entity,
                property,
            },
            inventory: None,
        }
    }
    pub fn body_target(&self) -> crate::canonical_action::Target {
        crate::canonical_action::Target::Chunk {
            key: self.chunk,
            part: crate::canonical_action::ChunkPart::Entity(self.entity),
            inventory: None,
        }
    }
    pub fn targets(&self) -> [crate::canonical_action::Target; 2] {
        [
            self.target(ObjectProperty::SimulationTick),
            self.target(match self.kind {
                ObjectTickKind::Tnt => ObjectProperty::Fuse,
                ObjectTickKind::Falling => ObjectProperty::Falling,
            }),
        ]
    }
    pub fn valid(&self, id: crate::canonical_action::ActionId) -> bool {
        self.entity.generation != 0
            && !self.birth.uuid.is_nil()
            && self.birth.source.server == self.entity.origin
            && self.birth.source.generation != 0
            && self.owner.generation != 0
            && id.actor == crate::actor::ActorId::Entity(self.entity)
            && id.incarnation == u64::from(self.entity.generation)
            && f32::from_bits(self.travel_down).is_finite()
            && (0.0..=30_000_000.0).contains(&f32::from_bits(self.travel_down))
            && (self.kind != ObjectTickKind::Tnt || self.travel_down == 0.0f32.to_bits())
    }
    pub fn derive(
        &self,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
        values: &std::collections::BTreeMap<
            crate::canonical_action::Target,
            crate::canonical_action::Cell,
        >,
    ) -> Result<Vec<crate::canonical_action::Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::*;
        if !self.valid(id) {
            return Err(ApplyFailure::InvalidAction);
        }
        let targets = self.targets();
        let Some(Cell {
            value: Value::ObjectProperty(ObjectPropertyValue::Tick(previous)),
            ..
        }) = values.get(&targets[0])
        else {
            return Err(ApplyFailure::Type(targets[0]));
        };
        let previous = previous.unwrap_or(self.birth.action.tick);
        if !tick.is_after(previous) {
            return Err(ApplyFailure::Condition(targets[0]));
        }
        let elapsed = tick.distance_since(previous);
        let next = match (&self.kind, values.get(&targets[1]).map(|cell| &cell.value)) {
            (ObjectTickKind::Tnt, Some(Value::ObjectProperty(ObjectPropertyValue::Fuse(fuse)))) => {
                ObjectPropertyValue::Fuse(fuse.saturating_sub(elapsed))
            }
            (
                ObjectTickKind::Falling,
                Some(Value::ObjectProperty(ObjectPropertyValue::Falling(falling))),
            ) => {
                let mut next = falling.clone();
                next.age = next.age.saturating_add(u32::from(elapsed));
                next.fall_distance = (f32::from_bits(next.fall_distance)
                    + f32::from_bits(self.travel_down))
                .to_bits();
                if !f32::from_bits(next.fall_distance).is_finite()
                    || f32::from_bits(next.fall_distance) > 30_000_000.0
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                ObjectPropertyValue::Falling(next)
            }
            _ => return Err(ApplyFailure::Type(targets[1])),
        };
        Ok(vec![
            Effect::Put {
                target: targets[1],
                value: Value::ObjectProperty(next),
            },
            Effect::Put {
                target: targets[0],
                value: Value::ObjectProperty(ObjectPropertyValue::Tick(Some(tick))),
            },
        ])
    }
    pub fn capture(
        &self,
        state: &impl crate::canonical_action::ActionState,
        id: crate::canonical_action::ActionId,
        tick: TickStamp,
        epoch: crate::canonical_action::SpatialEpoch,
    ) -> Result<crate::canonical_action::Action, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::*;
        let mut values = std::collections::BTreeMap::new();
        let mut conditions = vec![
            Condition {
                target: self.body_target(),
                expected: Expected::ActorControlled {
                    birth: self.birth.clone(),
                    issuer: self.owner,
                },
            },
            Condition {
                target: self.body_target(),
                expected: Expected::SpatialEpochEquals(epoch),
            },
        ];
        for target in self.targets() {
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            conditions.push(Condition {
                target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(target, cell);
        }
        let effects = self.derive(id, tick, &values)?;
        let action = Action {
            id,
            tick,
            edit_authority: None,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: Some(self.chunk),
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(self.chunk)],
            dependencies: vec![self.birth.action],
            conditions,
            effects,
            counterfactual_conditions: self.targets().to_vec(),
            counterfactual_effects: self.targets().to_vec(),
            semantic_intent: Some(SemanticIntent::ObjectTick(self.clone())),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Actor,
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}

pub fn terminal_leg_at(
    body: &crate::entity_body::EntityBody,
    chunk: crate::protocol::ChunkKey,
    snapshot: &impl crate::canonical_action::ActionState,
    terminal: &ObjectTerminal,
    tick: TickStamp,
    y: f64,
    minimum_y: i32,
    maximum_y: i32,
) -> Result<ObjectTerminalLeg, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::*;
    let EntityPayload::Object(object) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let plan = capture(
        object,
        body.birth.action.tick,
        tick,
        y,
        minimum_y,
        maximum_y,
    )
    .ok_or(ApplyFailure::InvalidAction)?;
    let landed = matches!((&object.kind, terminal),
        (ObjectKind::FallingBlock(falling), ObjectTerminal::FallingLanded { block_state, drop_item })
            if falling.block_state == *block_state && falling.drop_item == *drop_item);
    if plan.terminal.as_ref() != Some(terminal) && !(plan.terminal.is_none() && landed) {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let mut conditions = vec![
        Condition {
            target,
            expected: Expected::Exact(
                snapshot
                    .read(TruthView::Local, target)
                    .filter(|cell| matches!(&cell.value, Value::Entity(actual) if actual.as_ref() == body))
                    .ok_or(ApplyFailure::Condition(target))?,
            ),
        },
        Condition {
            target,
            expected: Expected::ActorControlled {
                birth: body.birth.clone(),
                issuer: body.owner,
            },
        },
        Condition {
            target,
            expected: Expected::SpatialEpochEquals(body.spatial_epoch),
        },
    ];
    for property in [
        ObjectProperty::SimulationTick,
        match object.kind {
            ObjectKind::PrimedTnt(_) => ObjectProperty::Fuse,
            ObjectKind::FallingBlock(_) => ObjectProperty::Falling,
            _ => return Err(ApplyFailure::InvalidAction),
        },
    ] {
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::ObjectProperty {
                actor: body.id,
                property,
            },
            inventory: None,
        };
        let cell = snapshot
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        let expected = match (&object.kind, property) {
            (_, ObjectProperty::SimulationTick) => {
                ObjectPropertyValue::Tick(object.simulation_tick)
            }
            (ObjectKind::PrimedTnt(tnt), ObjectProperty::Fuse) => {
                ObjectPropertyValue::Fuse(tnt.fuse)
            }
            (ObjectKind::FallingBlock(falling), ObjectProperty::Falling) => {
                ObjectPropertyValue::Falling(falling.clone())
            }
            _ => return Err(ApplyFailure::Type(target)),
        };
        if cell.value != Value::ObjectProperty(expected) {
            return Err(ApplyFailure::Condition(target));
        }
        conditions.push(Condition {
            target,
            expected: Expected::Exact(cell),
        });
    }
    Ok(ObjectTerminalLeg {
        conditions,
        effects: vec![Effect::Delete { target }],
    })
}

pub fn permits_terminal_admission(
    action: &crate::canonical_action::Action,
    target: crate::canonical_action::Target,
    body: &crate::entity_body::EntityBody,
    inventory: &crate::entity_inventory_admission::EntityInventoryAdmission,
) -> bool {
    use crate::canonical_action::*;
    let Some(witness) = terminal_witness(action) else {
        return false;
    };
    if !valid_terminal_action(action) || !inventory.cells.is_empty() {
        return false;
    }
    let ObjectTerminal::Explosion { igniter, .. } = &witness.terminal else {
        return false;
    };
    if !action.conditions.iter().any(|condition| condition.target == witness.target()
        && matches!(&condition.expected, Expected::Exact(cell) if matches!(&cell.value, Value::Entity(actual) if actual.as_ref() == &witness.body)))
    { return false; }
    let EntityPayload::Object(newborn) = &body.payload else {
        return false;
    };
    let ObjectKind::PrimedTnt(chain) = &newborn.kind else {
        return false;
    };
    if body.owner != witness.body.owner
        || body.birth.source != witness.body.owner
        || body.birth.action
            != (ActionRef {
                id: action.id,
                tick: action.tick,
            })
        || chain.power != 4.0f32.to_bits()
        || chain.igniter != *igniter
        || !(10..=30).contains(&chain.fuse)
        || newborn.simulation_tick != Some(action.tick)
    {
        return false;
    }
    let Target::Chunk {
        key,
        part: ChunkPart::Entity(entity),
        inventory: None,
    } = target
    else {
        return false;
    };
    if body.id != entity || !body.belongs_to(entity, key) {
        return false;
    }
    let position = body.spatial.position.map(f64::from_bits);
    let block = crate::protocol::BlockPos {
        x: position[0].floor() as i32,
        y: position[1].floor() as i32,
        z: position[2].floor() as i32,
    };
    let block_target = Target::Chunk {
        key,
        part: ChunkPart::Block(block),
        inventory: None,
    };
    action.conditions.iter().any(|condition| {
        condition.target == block_target && matches!(&condition.expected, Expected::BlockState(_))
    }) && action.effects.iter().any(|effect| {
        matches!(effect,
            Effect::Put { target, value: Value::Block(0) } if *target == block_target)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectTerminalWitness {
    pub fall_distance: u32,
    pub combat: crate::combat_action::CombatIntent,
    pub chunk: crate::protocol::ChunkKey,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub do_entity_drops: bool,
    pub body: crate::entity_body::EntityBody,
    pub origin_spatial: crate::entity_body::EntitySpatial,
    pub effective_spatial: crate::entity_body::EntitySpatial,
    pub source_epoch: crate::canonical_action::SpatialEpoch,
    pub source_tick: TickStamp,
    pub terminal: ObjectTerminal,
}

impl ObjectTerminalWitness {
    pub fn target(&self) -> crate::canonical_action::Target {
        crate::canonical_action::Target::Chunk {
            key: self.chunk,
            part: crate::canonical_action::ChunkPart::Entity(self.body.id),
            inventory: None,
        }
    }

    pub fn matches(&self, actual: Option<&crate::canonical_action::Cell>) -> bool {
        matches!(actual, Some(crate::canonical_action::Cell {
            value: crate::canonical_action::Value::Entity(body), ..
        }) if body.as_ref() == &self.body)
    }

    pub fn valid(&self, id: crate::canonical_action::ActionId, tick: TickStamp) -> bool {
        f32::from_bits(self.fall_distance).is_finite()
            && (0.0..=30_000_000.0).contains(&f32::from_bits(self.fall_distance))
            && id.actor == crate::actor::ActorId::Entity(self.body.id)
            && id.incarnation == self.body.id.generation
            && self.body.belongs_to(self.body.id, self.chunk)
            && self.body.valid_at(tick)
            && matches!(
                &self.body.payload,
                EntityPayload::Object(_)
                    | EntityPayload::Vehicle(crate::entity_body::VehicleState {
                        kind: crate::entity_body::VehicleKind::TntMinecart,
                        ..
                    })
            )
            && self.source_tick == tick
            && self.source_epoch == self.body.spatial_epoch
            && self.origin_spatial.world == self.chunk.world
            && self.effective_spatial.world == self.chunk.world
            && self
                .origin_spatial
                .position
                .iter()
                .chain(self.effective_spatial.position.iter())
                .all(|bits| {
                    f64::from_bits(*bits).is_finite() && f64::from_bits(*bits).abs() <= 30_000_000.0
                })
            && self
                .origin_spatial
                .position
                .iter()
                .zip(self.effective_spatial.position)
                .all(|(before, after)| {
                    (f64::from_bits(*before) - f64::from_bits(after)).abs() <= 256.0
                })
            && self.rules.rules.world == self.chunk.world
            && self.rules.slot as u16 == tick.0
    }

    pub fn validate_context<S: crate::canonical_action::ActionState>(
        &self,
        state: &S,
        action: &crate::canonical_action::Action,
    ) -> Result<(), crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Effect};
        let id = action.id;
        let tick = action.tick;
        for effect in &action.effects {
            if let Effect::EntityAdmission { body, .. } = effect {
                if !matches!(body.payload, EntityPayload::Object(_)) {
                    continue;
                }
                let (kind, block) = state
                    .primed_tnt_identity()
                    .ok_or(ApplyFailure::InvalidAction)?;
                if body.kind != kind
                    || !matches!(&body.payload,
                    EntityPayload::Object(crate::object_state::ObjectState { kind: ObjectKind::PrimedTnt(tnt), .. })
                        if tnt.block_state == block)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let at = body.spatial.position.map(f64::from_bits);
                let block = crate::protocol::BlockPos {
                    x: at[0].floor() as i32,
                    y: at[1].floor() as i32,
                    z: at[2].floor() as i32,
                };
                let target = crate::canonical_action::Target::Chunk {
                    key: crate::protocol::ChunkKey::new(
                        body.spatial.world,
                        block.x.div_euclid(16),
                        block.z.div_euclid(16),
                    ),
                    part: crate::canonical_action::ChunkPart::Block(block),
                    inventory: None,
                };
                if !action.conditions.iter().any(|condition| condition.target == target
                    && matches!(condition.expected, crate::canonical_action::Expected::BlockState(before) if state.primed_tnt_block(before)))
                { return Err(ApplyFailure::InvalidAction); }
            }
        }
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let rules = state.world_rules(self.rules, tick)?;
        if rules.do_entity_drops != self.do_entity_drops {
            return Err(ApplyFailure::InvalidAction);
        }
        self.validate_combat(state, action)?;
        let plan = capture_body(
            &self.body,
            tick,
            &self.effective_spatial,
            rules.min_y,
            rules.min_y + rules.height as i32,
        )
        .ok_or(ApplyFailure::InvalidAction)?;
        if plan.terminal.as_ref() == Some(&self.terminal) {
            return Ok(());
        }
        if plan.terminal.is_none()
            && matches!((&self.body.payload, &self.terminal),
            (EntityPayload::Object(crate::object_state::ObjectState { kind: ObjectKind::FallingBlock(falling), .. }), ObjectTerminal::FallingLanded { block_state, drop_item })
                if falling.block_state == *block_state && falling.drop_item == *drop_item)
        {
            return Ok(());
        }
        Err(ApplyFailure::InvalidAction)
    }

    pub fn guards_valid(&self, action: &crate::canonical_action::Action) -> bool {
        use crate::canonical_action::*;
        self.valid(action.id, action.tick)
            && action.class == ActionClass::Undoable
            && action.kind == ActionKind::World
            && action.actor_chunk == Some(self.chunk)
            && !action.chunk_transition
            && action.conditions.iter().any(|condition| {
                condition.target == self.target()
                    && condition.expected
                        == (Expected::ActorControlled {
                            birth: self.body.birth.clone(),
                            issuer: self.body.owner,
                        })
            })
            && action.conditions.iter().any(|condition| {
                condition.target == self.target()
                    && condition.expected == Expected::SpatialEpochEquals(self.source_epoch)
            })
            && action.effects.iter().any(
                |effect| matches!(effect, Effect::RetireEntity { intent } if *intent == self.retirement()),
            )
    }
}

pub fn terminal_witness(
    action: &crate::canonical_action::Action,
) -> Option<&ObjectTerminalWitness> {
    action
        .conditions
        .iter()
        .find_map(|condition| match &condition.expected {
            crate::canonical_action::Expected::ObjectTerminal(witness) => Some(witness.as_ref()),
            _ => None,
        })
}

pub fn valid_terminal_action(action: &crate::canonical_action::Action) -> bool {
    let witnesses = action
        .conditions
        .iter()
        .filter_map(|condition| match &condition.expected {
            crate::canonical_action::Expected::ObjectTerminal(witness) => {
                Some((condition.target, witness.as_ref()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    matches!(witnesses.as_slice(), [(target, witness)] if *target == witness.target() && witness.guards_valid(action))
}

pub fn tnt_cart_power(
    body: &crate::entity_body::EntityBody,
    spatial: &crate::entity_body::EntitySpatial,
    tick: TickStamp,
) -> u32 {
    let speed =
        (spatial.velocity_micro[0] as f64).hypot(spatial.velocity_micro[2] as f64) / 1_000_000.0;
    let rank = crate::order::rank_tick_actor(
        body.birth.action.id.incarnation,
        tick,
        crate::actor::ActorId::Entity(body.id),
    );
    let random = (rank >> 11) as f64 / ((1u64 << 53) as f64);
    ((4.0 + random * 1.5 * speed.min(5.0)) as f32).to_bits()
}

pub fn capture_body(
    body: &crate::entity_body::EntityBody,
    tick: TickStamp,
    spatial: &crate::entity_body::EntitySpatial,
    minimum_y: i32,
    maximum_y: i32,
) -> Option<ObjectTickPlan> {
    match &body.payload {
        EntityPayload::Object(object) => capture(
            object,
            body.birth.action.tick,
            tick,
            f64::from_bits(spatial.position[1]),
            minimum_y,
            maximum_y,
        ),
        EntityPayload::Vehicle(vehicle)
            if vehicle.kind == crate::entity_body::VehicleKind::TntMinecart
                && vehicle.fuse_ticks.is_some_and(|fuse| fuse <= 1)
                && body.passengers.is_empty()
                && tick.is_after(vehicle.lifecycle_tick.unwrap_or(body.birth.action.tick)) =>
        {
            Some(ObjectTickPlan {
                elapsed: tick
                    .distance_since(vehicle.lifecycle_tick.unwrap_or(body.birth.action.tick)),
                updates: Vec::new(),
                terminal: Some(ObjectTerminal::Explosion {
                    power: tnt_cart_power(body, spatial, tick),
                    igniter: None,
                }),
            })
        }
        _ => None,
    }
}

pub fn tnt_cart_terminal_leg(
    witness: &ObjectTerminalWitness,
    state: &impl crate::canonical_action::ActionState,
) -> Result<ObjectTerminalLeg, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::*;
    use crate::vehicle_property::{VehicleProperty, VehiclePropertyValue};
    let EntityPayload::Vehicle(vehicle) = &witness.body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    if vehicle.kind != crate::entity_body::VehicleKind::TntMinecart
        || !witness.body.passengers.is_empty()
        || !vehicle.fuse_ticks.is_some_and(|fuse| fuse <= 1)
        || !witness.source_tick.is_after(
            vehicle
                .lifecycle_tick
                .unwrap_or(witness.body.birth.action.tick),
        )
        || witness.terminal
            != (ObjectTerminal::Explosion {
                power: tnt_cart_power(
                    &witness.body,
                    &witness.effective_spatial,
                    witness.source_tick,
                ),
                igniter: None,
            })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = witness.target();
    let body = state
        .read(TruthView::Local, target)
        .filter(|cell| matches!(&cell.value, Value::Entity(actual) if actual.as_ref() == &witness.body))
        .ok_or(ApplyFailure::Condition(target))?;
    let mut conditions = vec![
        Condition {
            target,
            expected: Expected::Exact(body),
        },
        Condition {
            target,
            expected: Expected::ActorControlled {
                birth: witness.body.birth.clone(),
                issuer: witness.body.owner,
            },
        },
        Condition {
            target,
            expected: Expected::SpatialEpochEquals(witness.body.spatial_epoch),
        },
    ];
    for (property, expected) in [
        (
            VehicleProperty::Fuse,
            VehiclePropertyValue::Fuse(vehicle.fuse_ticks),
        ),
        (
            VehicleProperty::LifecycleTick,
            VehiclePropertyValue::Tick(vehicle.lifecycle_tick),
        ),
    ] {
        let target = Target::Chunk {
            key: witness.chunk,
            part: ChunkPart::VehicleProperty {
                actor: witness.body.id,
                property,
            },
            inventory: None,
        };
        let cell = state
            .read(TruthView::Local, target)
            .filter(|cell| cell.value == Value::VehicleProperty(expected.clone()))
            .ok_or(ApplyFailure::Condition(target))?;
        conditions.push(Condition {
            target,
            expected: Expected::Exact(cell),
        });
    }
    Ok(ObjectTerminalLeg {
        conditions,
        effects: vec![Effect::Delete { target }],
    })
}

impl ObjectTerminalWitness {
    pub fn retirement(&self) -> crate::canonical_entity_retirement::EntityRetirementIntent {
        crate::canonical_entity_retirement::EntityRetirementIntent {
            target: self.target(),
            birth: self.body.birth.clone(),
            owner: self.body.owner,
            kind: self.body.kind,
            inventory: Vec::new(),
            living_death: None,
            disposition: crate::canonical_entity_retirement::EntityInventoryDisposition::Destroy,
        }
    }
}

impl ObjectTerminalWitness {
    fn validate_combat<S: crate::canonical_action::ActionState>(
        &self,
        state: &S,
        action: &crate::canonical_action::Action,
    ) -> Result<(), crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Effect, Expected, TruthView, Value};
        let allowed = crate::combat_action::CombatIntent {
            damage_contacts: self.combat.damage_contacts.clone(),
            equipment: self.combat.equipment.clone(),
            ..Default::default()
        };
        if allowed != self.combat
            || (!self.combat.damage_contacts.is_empty() && !self.combat.is_valid(action.tick))
            || (self.combat.damage_contacts.is_empty() && !self.combat.equipment.is_empty())
            || self.combat.damage_contacts.iter().any(|incoming| {
                !match self.terminal {
                    ObjectTerminal::Explosion { .. } => {
                        incoming.damage.kind == crate::combat_action::DamageKind::Explosion
                    }
                    ObjectTerminal::FallingLanded { .. } => {
                        self.falling_damage(state).is_some_and(|(kind, amount)| {
                            incoming.damage.kind == kind
                                && incoming.damage.damage_milli == amount
                                && incoming.damage.knockback_micro == [0; 3]
                                && incoming.damage.fire_ticks == 0
                                && !incoming.damage.bypass_hurt_cooldown
                                && !incoming.damage.bypass_invulnerability
                                && incoming.damage.reduction.is_some_and(|facts| {
                                    !facts.bypass_armor
                                        && !facts.bypass_effects
                                        && !facts.bypass_enchantments
                                        && facts.protection
                                            == crate::combat_action::DamageProtection::General
                                        && facts.breach == 0
                                })
                                && self.falling_intersects(&incoming.contact)
                        })
                    }
                    _ => false,
                } || incoming.damage.source != action.id.actor
                    || incoming.contact.world != self.chunk.world
                    || incoming.attack_origin != self.effective_spatial.position
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        for incoming in &self.combat.damage_contacts {
            let contact = &incoming.contact;
            if !action.conditions.iter().any(|condition| {
                condition.target == contact.target()
                    && condition.expected == Expected::SpatialEpochEquals(contact.epoch)
            }) || match &contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => {
                    !action.player_leases.contains(lease)
                }
                crate::item_contact::ContactIdentity::Entity { birth, .. } => {
                    !action.conditions.iter().any(|condition| {
                        condition.target == contact.target()
                            && condition.expected
                                == (Expected::EntityBirthOwned {
                                    birth: birth.clone(),
                                    owner: contact.owner,
                                })
                    })
                }
            } {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut values = std::collections::BTreeMap::new();
        for target in self.combat.targets() {
            if let Some(cell) = state.read(TruthView::Local, target) {
                values.insert(target, cell);
            }
        }
        for target in self.combat.resolution_targets() {
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            if !action.conditions.iter().any(|condition| {
                condition.target == target && condition.expected == Expected::Exact(cell.clone())
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
            values.insert(target, cell);
        }
        let expected = if self.combat.damage_contacts.is_empty() {
            Vec::new()
        } else {
            self.combat
                .derive_with(state, action.id, action.tick, &values)?
        };
        if expected.iter().any(|effect| {
            action
                .effects
                .iter()
                .filter(|actual| *actual == effect)
                .count()
                != 1
        }) || action.effects.iter().any(|effect| {
            matches!(
                effect,
                Effect::TransformItems { .. }
                    | Effect::Put {
                        value: Value::LivingProperty(_),
                        ..
                    }
            ) && !expected.contains(effect)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }
}

impl ObjectTerminalWitness {
    pub fn falling_damage<S: crate::canonical_action::ActionState>(
        &self,
        state: &S,
    ) -> Option<(crate::combat_action::DamageKind, u32)> {
        let EntityPayload::Object(object) = &self.body.payload else {
            return None;
        };
        let ObjectKind::FallingBlock(falling) = &object.kind else {
            return None;
        };
        if !falling.hurt_entities || !matches!(self.terminal, ObjectTerminal::FallingLanded { .. })
        {
            return None;
        }
        let distance = f32::from_bits(self.fall_distance);
        let prior = f32::from_bits(falling.fall_distance);
        let movement = (f64::from_bits(self.origin_spatial.position[1])
            - f64::from_bits(self.effective_spatial.position[1]))
        .max(0.0) as f32;
        if !distance.is_finite()
            || !(0.0..=30_000_000.0).contains(&distance)
            || (distance - (prior + movement)).abs() > 0.001
        {
            return None;
        }
        let blocks = (distance - 1.0).ceil().max(0.0);
        let amount = (blocks * f32::from_bits(falling.damage_per_block))
            .floor()
            .min(f32::from_bits(falling.max_damage))
            .max(0.0);
        if amount <= 0.0 {
            return None;
        }
        Some((
            state.falling_damage_kind(falling.block_state)?,
            (f64::from(amount) * 1000.0).min(f64::from(u32::MAX)) as u32,
        ))
    }

    pub fn falling_intersects(&self, contact: &crate::item_contact::ContactObservation) -> bool {
        let center = self.effective_spatial.position.map(f64::from_bits);
        let at = contact.position.map(f64::from_bits);
        let radius = f64::from_bits(contact.half_width);
        let height = f64::from_bits(contact.height);
        contact.world == self.chunk.world
            && at[0] + radius > center[0] - 0.49
            && at[0] - radius < center[0] + 0.49
            && at[2] + radius > center[2] - 0.49
            && at[2] - radius < center[2] + 0.49
            && at[1] + height > center[1]
            && at[1] < center[1] + 0.98
    }
}
