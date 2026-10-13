use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorId,
    actor_occupancy::{ActorOccupancyWitness, CollisionBox},
    actor_relation::ActorRelation,
    canonical_action::*,
    canonical_entity_retirement::{EntityInventoryDisposition, EntityRetirementIntent},
    entity_body::{EntityBody, EntityPayload, EntitySpatial},
    item_contact::{ContactIdentity, ContactObservation},
    living_property::LivingProperty as P,
    object_property::ObjectPropertyValue,
    object_state::{
        AreaEffectCloudState, CloudContinuation, CloudVictimCooldown, ObjectKind, ObjectProperty,
    },
    protocol::ChunkKey,
    time::TickStamp,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AreaCloudIntent {
    pub cloud: ContactObservation,
    pub original: EntitySpatial,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub domain: Vec<ActorOccupancyWitness>,
    pub victims: Vec<ContactObservation>,
    pub status_attributes: Vec<u16>,
    pub max_health_attribute: u16,
}

pub struct CloudPotionOutcome {
    pub affected: bool,
    pub effects: Vec<Effect>,
}

pub fn property(contact: &ContactObservation, property: P) -> Target {
    Target::Chunk {
        key: contact.chunk,
        part: ChunkPart::LivingProperty {
            actor: contact.actor(),
            property,
        },
        inventory: None,
    }
}

impl AreaCloudIntent {
    pub fn object_property(&self, property: ObjectProperty) -> Target {
        let ActorId::Entity(actor) = self.cloud.actor() else {
            unreachable!()
        };
        Target::Chunk {
            key: self.cloud.chunk,
            part: ChunkPart::ObjectProperty { actor, property },
            inventory: None,
        }
    }

    pub fn relation_targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(ActorRelation::Mount)
            .chain((0..32).map(ActorRelation::Passenger))
            .map(|relation| Target::Chunk {
                key: self.cloud.chunk,
                part: ChunkPart::ActorRelation {
                    actor: self.cloud.actor(),
                    relation,
                },
                inventory: None,
            })
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        self.cloud.valid()
            && self.cloud.actor() == id.actor
            && self.cloud.simulation_tick == tick
            && matches!(self.cloud.identity, ContactIdentity::Entity { id: entity, .. } if entity.generation == id.incarnation)
            && self.original.world == self.cloud.world
            && self.original.position == self.cloud.position
            && self.original.velocity_micro == [0; 3]
            && self.rules.rules.world == self.cloud.world
            && self.rules.slot as u16 == tick.0
            && !self.domain.is_empty()
            && self.domain.len() <= 64
            && self.domain.iter().all(ActorOccupancyWitness::valid)
            && self.domain.windows(2).all(|pair| pair[0].key < pair[1].key)
            && self
                .domain
                .iter()
                .all(|witness| witness.key.world == self.cloud.world)
            && self.victims.len() <= 4096
            && self.victims.iter().all(|victim| {
                victim.valid()
                    && victim.simulation_tick == tick
                    && victim.world == self.cloud.world
                    && victim.actor() != id.actor
            })
            && self
                .victims
                .windows(2)
                .all(|pair| pair[0].actor() < pair[1].actor())
            && self.status_attributes.len() <= 256
            && self
                .status_attributes
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && self.status_attributes.contains(&self.max_health_attribute)
            && self
                .domain
                .iter()
                .flat_map(|witness| witness.poses.iter().map(|pose| pose.entry.actor))
                .collect::<BTreeSet<_>>()
                .len()
                == self
                    .domain
                    .iter()
                    .map(|witness| witness.poses.len())
                    .sum::<usize>()
    }

    pub fn victim_targets(&self) -> BTreeSet<Target> {
        let combat = crate::combat_action::CombatIntent {
            damages: self
                .victims
                .iter()
                .map(|victim| crate::combat_action::DamageIntent {
                    kind: crate::combat_action::DamageKind::Magic,
                    source: self.cloud.actor(),
                    target: victim.target(),
                    tick: self.cloud.simulation_tick,
                    damage_milli: 1,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: 0,
                    reduction: None,
                    bypass_invulnerability: false,
                })
                .collect(),
            ..Default::default()
        };
        combat
            .targets()
            .chain(self.victims.iter().flat_map(|victim| {
                [
                    P::StatusEffects,
                    P::Strength,
                    P::Weakness,
                    P::Resistance,
                    P::FireResistance,
                    P::Blindness,
                ]
                .into_iter()
                .chain(
                    self.status_attributes
                        .iter()
                        .copied()
                        .flat_map(|attribute| {
                            [
                                P::Attribute(attribute),
                                P::PermanentAttributeModifiers(attribute),
                            ]
                        }),
                )
                .map(move |facet| property(victim, facet))
            }))
            .chain(self.victims.iter().flat_map(|victim| {
                crate::combat_action::instant_health_targets(
                    victim.target(),
                    self.max_health_attribute,
                )
            }))
            .collect()
    }

    pub fn targets(&self) -> BTreeSet<Target> {
        std::iter::once(self.cloud.target())
            .chain([
                self.object_property(ObjectProperty::Cloud),
                self.object_property(ObjectProperty::SimulationTick),
            ])
            .chain(self.relation_targets())
            .chain(self.victim_targets())
            .chain(self.domain.iter().flat_map(|witness| {
                std::iter::once(Target::Chunk {
                    key: witness.key,
                    part: ChunkPart::ActorOccupancy,
                    inventory: None,
                })
                .chain(witness.poses.iter().map(move |pose| Target::Chunk {
                    key: witness.key,
                    part: match pose.entry.actor {
                        ActorId::Player(gid) => ChunkPart::Player(gid),
                        ActorId::Entity(id) => ChunkPart::Entity(id),
                        ActorId::Peer(_) => unreachable!(),
                    },
                    inventory: None,
                }))
            }))
            .collect()
    }

    pub fn retirement(&self, body: &EntityBody) -> EntityRetirementIntent {
        EntityRetirementIntent {
            target: self.cloud.target(),
            birth: body.birth.clone(),
            owner: body.owner,
            kind: body.kind,
            inventory: Vec::new(),
            living_death: None,
            disposition: EntityInventoryDisposition::Destroy,
        }
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        let ContactIdentity::Entity { birth, .. } = &self.cloud.identity else {
            return false;
        };
        self.valid(action.id, action.tick)
            && action.kind == ActionKind::Actor
            && action.class == ActionClass::Replayable
            && action.actor_chunk == Some(self.cloud.chunk)
            && !action.chunk_transition
            && action.conditions.contains(&Condition {
                target: self.cloud.target(),
                expected: Expected::ActorControlled {
                    birth: birth.clone(),
                    issuer: self.cloud.owner,
                },
            })
            && action.conditions.contains(&Condition {
                target: self.cloud.target(),
                expected: Expected::SpatialEpochEquals(self.cloud.epoch),
            })
            && self.domain.iter().all(|witness| {
                action.conditions.contains(&Condition {
                    target: Target::Chunk {
                        key: witness.key,
                        part: ChunkPart::ActorOccupancy,
                        inventory: None,
                    },
                    expected: Expected::ActorOccupancyDomain(witness.clone()),
                })
            })
            && self.targets().iter().all(|target| {
                action.counterfactual_conditions.contains(target)
                    && action.counterfactual_effects.contains(target)
            })
            && action.effects.iter().all(|effect| match effect {
                Effect::Put {
                    target,
                    value: Value::LivingProperty(_),
                } => self.victim_targets().contains(target),
                Effect::Put {
                    target,
                    value: Value::ObjectProperty(_),
                } => [
                    self.object_property(ObjectProperty::Cloud),
                    self.object_property(ObjectProperty::SimulationTick),
                ]
                .contains(target),
                Effect::Put {
                    target,
                    value: Value::ActorRelation(None),
                } => self.relation_targets().any(|relation| relation == *target),
                Effect::RetireEntity { intent } => {
                    intent.target == self.cloud.target()
                        && intent.inventory.is_empty()
                        && intent.living_death.is_none()
                }
                _ => false,
            })
    }

    pub fn derive<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        state.world_rules(self.rules, tick)?;
        let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = values.get(&self.cloud.target())
        else {
            return Err(ApplyFailure::Type(self.cloud.target()));
        };
        let ContactIdentity::Entity { birth, .. } = &self.cloud.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        if body.birth != *birth
            || body.owner != self.cloud.owner
            || body.spatial_epoch != self.cloud.epoch
            || body.mount.is_some()
            || !body.passengers.is_empty()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let EntityPayload::Object(object) = &body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        let ObjectKind::AreaEffectCloud(cloud) = &object.kind else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !cloud.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let Some(Cell {
            value: Value::ObjectProperty(ObjectPropertyValue::Cloud(continuation)),
            ..
        }) = values.get(&self.object_property(ObjectProperty::Cloud))
        else {
            return Err(ApplyFailure::Type(
                self.object_property(ObjectProperty::Cloud),
            ));
        };
        let Some(Cell {
            value: Value::ObjectProperty(ObjectPropertyValue::Tick(previous)),
            ..
        }) = values.get(&self.object_property(ObjectProperty::SimulationTick))
        else {
            return Err(ApplyFailure::Type(
                self.object_property(ObjectProperty::SimulationTick),
            ));
        };
        let previous = previous.unwrap_or(body.birth.action.tick);
        if !tick.is_after(previous) {
            return Err(ApplyFailure::Condition(
                self.object_property(ObjectProperty::SimulationTick),
            ));
        }
        self.validate_domain(values, cloud)?;
        let mut next = advance(cloud, continuation, tick.distance_since(previous), tick);
        let mut projected = values.clone();
        let mut changed = BTreeMap::new();
        if !expired(cloud, &next) && next.age >= cloud.wait && next.age % 5 == 0 {
            for victim in &self.victims {
                let radius = f64::from(f32::from_bits(next.radius));
                if radius < 0.5 {
                    break;
                }
                let at = victim.position.map(f64::from_bits);
                let origin = self.original.position.map(f64::from_bits);
                if (at[0] - origin[0]).powi(2) + (at[2] - origin[2]).powi(2) > radius * radius
                    || at[1] + f64::from_bits(victim.height) <= origin[1]
                    || at[1] >= origin[1] + 0.5
                {
                    continue;
                }
                if next.cooldowns.get(&victim.actor()).is_some_and(|cooldown| {
                    cooldown.identity == victim.identity
                        && (tick == cooldown.tick
                            || tick.is_after(cooldown.tick)
                                && tick.distance_since(cooldown.tick) < cloud.reapply)
                }) {
                    continue;
                }
                let outcome = state.derive_cloud_potion(self, victim, id, tick, &projected)?;
                if !outcome.affected {
                    continue;
                }
                for effect in outcome.effects {
                    let Effect::Put {
                        target,
                        value: Value::LivingProperty(value),
                    } = effect
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if !self.victim_targets().contains(&target)
                        || crate::living_property::body_target(target)
                            .is_none_or(|(body, _)| body != victim.target())
                    {
                        return Err(ApplyFailure::Type(target));
                    }
                    let cell = projected
                        .get_mut(&target)
                        .ok_or(ApplyFailure::Condition(target))?;
                    cell.value = Value::LivingProperty(value.clone());
                    changed.insert(target, Value::LivingProperty(value));
                }
                next.cooldowns.insert(
                    victim.actor(),
                    CloudVictimCooldown {
                        identity: victim.identity.clone(),
                        tick,
                    },
                );
                next.radius = (f32::from_bits(next.radius) + f32::from_bits(cloud.radius_on_use))
                    .max(0.0)
                    .to_bits();
            }
        }
        let mut effects = changed
            .into_iter()
            .map(|(target, value)| Effect::Put { target, value })
            .collect::<Vec<_>>();
        if expired(cloud, &next) {
            for target in self.relation_targets() {
                if !matches!(
                    values.get(&target),
                    Some(Cell {
                        value: Value::ActorRelation(None),
                        ..
                    })
                ) {
                    return Err(ApplyFailure::Condition(target));
                }
                effects.push(Effect::Put {
                    target,
                    value: Value::ActorRelation(None),
                });
            }
            effects.push(Effect::RetireEntity {
                intent: self.retirement(body),
            });
        } else {
            effects.push(Effect::Put {
                target: self.object_property(ObjectProperty::Cloud),
                value: Value::ObjectProperty(ObjectPropertyValue::Cloud(next)),
            });
            effects.push(Effect::Put {
                target: self.object_property(ObjectProperty::SimulationTick),
                value: Value::ObjectProperty(ObjectPropertyValue::Tick(Some(tick))),
            });
        }
        Ok(effects)
    }
}

pub fn advance(
    cloud: &AreaEffectCloudState,
    previous: &CloudContinuation,
    elapsed: u16,
    tick: TickStamp,
) -> CloudContinuation {
    let mut next = previous.clone();
    next.age = next.age.saturating_add(u32::from(elapsed));
    let active = next
        .age
        .saturating_sub(cloud.wait.saturating_sub(1))
        .saturating_sub(previous.age.saturating_sub(cloud.wait.saturating_sub(1)));
    next.radius = (f32::from_bits(previous.radius)
        + f32::from_bits(cloud.radius_per_tick) * active as f32)
        .max(0.0)
        .to_bits();
    next.cooldowns.retain(|_, cooldown| {
        tick == cooldown.tick
            || tick.is_after(cooldown.tick) && tick.distance_since(cooldown.tick) < cloud.reapply
    });
    next
}

pub fn expired(cloud: &AreaEffectCloudState, continuation: &CloudContinuation) -> bool {
    continuation.age >= cloud.wait.saturating_add(cloud.duration)
        || f32::from_bits(continuation.radius) < 0.5
}

impl AreaCloudIntent {
    fn validate_domain(
        &self,
        values: &BTreeMap<Target, Cell>,
        cloud: &AreaEffectCloudState,
    ) -> Result<(), ApplyFailure> {
        let at = self.original.position.map(f64::from_bits);
        let radius = f64::from(f32::from_bits(cloud.initial_radius));
        let bounds = CollisionBox {
            bounds: [
                [at[0] - radius, at[1], at[2] - radius].map(f64::to_bits),
                [at[0] + radius, at[1] + 0.5, at[2] + radius].map(f64::to_bits),
            ],
        };
        let min = bounds.bounds[0].map(f64::from_bits);
        let max = bounds.bounds[1].map(f64::from_bits);
        let a = [
            (min[0].floor() as i32).div_euclid(16) - 64,
            (min[2].floor() as i32).div_euclid(16) - 64,
        ];
        let b = [
            (max[0].floor() as i32).div_euclid(16) + 64,
            (max[2].floor() as i32).div_euclid(16) + 64,
        ];
        if b[0] - a[0] > 7 || b[1] - a[1] > 7 {
            return Err(ApplyFailure::ResourceLimit);
        }
        let required = (a[0]..=b[0])
            .flat_map(|x| (a[1]..=b[1]).map(move |z| ChunkKey::new(self.cloud.world, x, z)))
            .collect::<BTreeSet<_>>();
        if self
            .domain
            .iter()
            .map(|witness| witness.key)
            .collect::<BTreeSet<_>>()
            != required
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut contacts = BTreeMap::new();
        for witness in &self.domain {
            let root = Target::Chunk {
                key: witness.key,
                part: ChunkPart::ActorOccupancy,
                inventory: None,
            };
            if witness.blocks != [bounds.clone()]
                || values
                    .get(&root)
                    .is_none_or(|cell| !witness.matches_domain(cell))
            {
                return Err(ApplyFailure::Condition(root));
            }
            for pose in &witness.poses {
                let target = Target::Chunk {
                    key: witness.key,
                    part: match pose.entry.actor {
                        ActorId::Player(gid) => ChunkPart::Player(gid),
                        ActorId::Entity(id) => ChunkPart::Entity(id),
                        ActorId::Peer(_) => return Err(ApplyFailure::InvalidAction),
                    },
                    inventory: None,
                };
                let cell = values.get(&target).ok_or(ApplyFailure::Condition(target))?;
                let (identity, owner, affected) = match (&pose.entry.identity, &cell.value) {
                    (
                        crate::actor_occupancy::OccupancyIdentity::Entity(birth),
                        Value::Entity(body),
                    ) if pose.entry.actor == ActorId::Entity(body.id)
                        && body.birth == *birth
                        && body.spatial_epoch == pose.entry.spatial_epoch =>
                    {
                        (
                            ContactIdentity::Entity {
                                id: body.id,
                                birth: birth.clone(),
                            },
                            Some(body.owner),
                            matches!(&body.payload,EntityPayload::Living(living)if living.stand.is_none()),
                        )
                    }
                    (
                        crate::actor_occupancy::OccupancyIdentity::Player(lease),
                        Value::Living(living),
                    ) if pose.entry.actor == ActorId::Player(lease.gid)
                        && living.avatar.as_ref().is_some_and(|avatar| {
                            avatar.spatial_epoch == pose.entry.spatial_epoch
                                && avatar.world == self.cloud.world
                        }) =>
                    {
                        let owner = self
                            .victims
                            .iter()
                            .find(|victim| victim.actor() == pose.entry.actor)
                            .map(|victim| victim.owner);
                        (ContactIdentity::Player(*lease), owner, true)
                    }
                    _ => return Err(ApplyFailure::Type(target)),
                };
                if !affected {
                    continue;
                }
                let position = pose.position.map(f64::from_bits);
                let width = f64::from_bits(pose.entry.shape.offsets[1][0])
                    - f64::from_bits(pose.entry.shape.offsets[0][0]);
                let height = f64::from_bits(pose.entry.shape.offsets[1][1])
                    - f64::from_bits(pose.entry.shape.offsets[0][1]);
                if width <= 0.0
                    || height <= 0.0
                    || (position[0] - at[0]).powi(2) + (position[2] - at[2]).powi(2)
                        > radius * radius
                    || position[1] + height <= at[1]
                    || position[1] >= at[1] + 0.5
                {
                    continue;
                }
                let owner = owner.ok_or(ApplyFailure::InvalidAction)?;
                contacts.insert(
                    pose.entry.actor,
                    ContactObservation {
                        identity,
                        owner,
                        epoch: pose.entry.spatial_epoch,
                        world: self.cloud.world,
                        chunk: witness.key,
                        position: pose.position,
                        half_width: (width / 2.0).to_bits(),
                        height: height.to_bits(),
                        simulation_tick: self.cloud.simulation_tick,
                        observed_sequence: crate::identity::PlayerSeq(pose.simulation_sequence),
                    },
                );
            }
        }
        if contacts.into_values().collect::<Vec<_>>() != self.victims {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    pub fn capture<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
    ) -> Result<Action, ApplyFailure> {
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut values = BTreeMap::new();
        let mut conditions = Vec::new();
        let mut leases = Vec::new();
        for target in self.targets() {
            let cell = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            let expected = match (&target, &cell.value) {
                (
                    Target::Chunk {
                        part: ChunkPart::ActorOccupancy,
                        key,
                        ..
                    },
                    _,
                ) => Expected::ActorOccupancyDomain(
                    self.domain
                        .iter()
                        .find(|witness| witness.key == *key)
                        .ok_or(ApplyFailure::InvalidAction)?
                        .clone(),
                ),
                (
                    Target::Chunk {
                        part: ChunkPart::Entity(_),
                        inventory: None,
                        ..
                    },
                    Value::Entity(body),
                ) => Expected::EntityBirthKind {
                    birth: body.birth.clone(),
                    owner: body.owner,
                    kind: body.kind,
                },
                (
                    Target::Chunk {
                        part: ChunkPart::Player(gid),
                        inventory: None,
                        key,
                    },
                    Value::Living(living),
                ) => {
                    let lease = self
                        .domain
                        .iter()
                        .flat_map(|witness| &witness.poses)
                        .find_map(|pose| match pose.entry.identity {
                            crate::actor_occupancy::OccupancyIdentity::Player(lease)
                                if lease.gid == *gid =>
                            {
                                Some(lease)
                            }
                            _ => None,
                        })
                        .ok_or(ApplyFailure::InvalidAction)?;
                    leases.push(lease);
                    let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
                    let _ = key;
                    Expected::SpatialEpochEquals(avatar.spatial_epoch)
                }
                _ => Expected::Exact(cell.clone()),
            };
            conditions.push(Condition { target, expected });
            values.insert(target, cell);
        }
        let ContactIdentity::Entity { birth, .. } = &self.cloud.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        conditions.push(Condition {
            target: self.cloud.target(),
            expected: Expected::ActorControlled {
                birth: birth.clone(),
                issuer: self.cloud.owner,
            },
        });
        conditions.push(Condition {
            target: self.cloud.target(),
            expected: Expected::SpatialEpochEquals(self.cloud.epoch),
        });
        for victim in &self.victims {
            conditions.push(Condition {
                target: victim.target(),
                expected: Expected::SpatialEpochEquals(victim.epoch),
            });
            if let ContactIdentity::Player(lease) = victim.identity {
                leases.push(lease);
            }
        }
        conditions.sort_by_key(|condition| condition.target);
        let mut seen = BTreeMap::<Target, Vec<Expected>>::new();
        conditions.retain(|condition| {
            let prior = seen.entry(condition.target).or_default();
            if prior.contains(&condition.expected) {
                false
            } else {
                prior.push(condition.expected.clone());
                true
            }
        });
        leases.sort_unstable();
        leases.dedup();
        let footprint = self.targets().into_iter().collect::<Vec<_>>();
        let action = Action {
            id,
            tick,
            edit_authority: None,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: Some(self.cloud.chunk),
            player_leases: leases,
            player_chunks: Vec::new(),
            stakeholders: footprint
                .iter()
                .map(|target| target.stakeholder())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            dependencies: vec![birth.action],
            conditions,
            effects: self.derive(state, id, tick, &values)?,
            counterfactual_conditions: footprint.clone(),
            counterfactual_effects: footprint,
            semantic_intent: Some(SemanticIntent::AreaCloud(Box::new(self.clone()))),
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
