use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::LivePlayerLease;
use crate::canonical_action::{
    ActionId, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Effect, SpatialEpoch, Target,
};
use crate::item_action::PlayerAvatarState;
use crate::protocol::ChunkKey;
use crate::time::TickStamp;
use crate::world_respawn::WorldRespawnPlan;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticRespawn {
    pub lease: LivePlayerLease,
    pub source: ChunkKey,
    pub preferred: Option<WorldRespawnPlan>,
    pub fallback: WorldRespawnPlan,
    pub avatar: PlayerAvatarState,
    pub max_health_attribute: u16,
    pub detach: Option<crate::actor_relation::PlayerDetachPlan>,
    pub death_inventory: Option<crate::death_inventory::DeathInventoryIntent>,
    pub light_plans: Vec<crate::world_action::WorldLightPlan>,
}

impl SemanticRespawn {
    pub fn is_valid(&self) -> bool {
        self.lease.incarnation != 0
            && self.avatar.is_valid()
            && self.fallback.valid()
            && self.preferred.is_none_or(WorldRespawnPlan::valid)
            && match (&self.avatar.mount, &self.detach) {
                (None, None) => true,
                (Some(crate::item_contact::ContactIdentity::Entity { id, .. }), Some(detach)) => {
                    detach.effects.len() == 2
                        && detach.effects.iter().any(|effect| matches!(effect,
                            Effect::Put { target: Target::Chunk { key, part: ChunkPart::ActorRelation { actor: crate::actor::ActorId::Player(player), relation: crate::actor_relation::ActorRelation::Mount }, inventory: None }, value: crate::canonical_action::Value::ActorRelation(None) }
                            if *key == self.source && *player == self.lease.gid))
                        && detach.effects.iter().any(|effect| matches!(effect,
                            Effect::Put { target: Target::Chunk { key, part: ChunkPart::ActorRelation { actor: crate::actor::ActorId::Entity(entity), relation: crate::actor_relation::ActorRelation::Passenger(0..=31) }, inventory: None }, value: crate::canonical_action::Value::ActorRelation(None) }
                            if key.world == self.source.world && entity == id))
                }
                _ => false,
            }
    }

    pub fn targets(&self) -> Result<Vec<Target>, ApplyFailure> {
        if !self.is_valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut targets = BTreeSet::new();
        targets.insert(Target::Chunk {
            key: self.source,
            part: ChunkPart::Player(self.lease.gid),
            inventory: None,
        });
        for plan in self.preferred.iter().chain(std::iter::once(&self.fallback)) {
            for target in plan.targets()? {
                if let Target::Chunk { key, .. } = target {
                    targets.insert(Target::Chunk {
                        key,
                        part: ChunkPart::Player(self.lease.gid),
                        inventory: None,
                    });
                }
                targets.insert(target);
            }
        }
        if let Some(death) = &self.death_inventory {
            targets.extend(death.targets());
        }
        if let Some(detach) = &self.detach {
            targets.extend(detach.conditions.iter().map(|guard| guard.target));
            targets.extend(detach.effects.iter().flat_map(Effect::targets));
        }
        for plan in &self.light_plans {
            targets.extend(plan.section_targets()?);
            targets.extend(plan.light_targets()?);
        }
        Ok(targets.into_iter().collect())
    }

    pub fn effect_targets(&self) -> Result<Vec<Target>, ApplyFailure> {
        let mut targets = BTreeSet::new();
        targets.insert(Target::Chunk {
            key: self.source,
            part: ChunkPart::Player(self.lease.gid),
            inventory: None,
        });
        for plan in self.preferred.iter().chain(std::iter::once(&self.fallback)) {
            for target in plan.targets()? {
                if let Target::Chunk { key, .. } = target {
                    targets.insert(Target::Chunk {
                        key,
                        part: ChunkPart::Player(self.lease.gid),
                        inventory: None,
                    });
                }
            }
        }
        if let Some(death) = &self.death_inventory {
            targets.extend(death.targets());
        }
        if let Some(detach) = &self.detach {
            targets.extend(detach.effects.iter().flat_map(Effect::targets));
        }
        if let Some(plan) = self.preferred {
            targets.insert(plan.target(plan.point.position));
        }
        for plan in &self.light_plans {
            targets.extend(plan.light_targets()?);
        }
        Ok(targets.into_iter().collect())
    }

    pub fn guards_valid(&self, action: &crate::canonical_action::Action) -> bool {
        use crate::canonical_action::{Expected, Value};
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        let actor = crate::actor::ActorId::Player(self.lease.gid);
        let processed = self.death_inventory.is_none();
        let fate_guard = action.conditions.iter().any(|condition| condition.target == (Target::Chunk { key: self.source, part: ChunkPart::LivingProperty { actor, property: P::DeathInventoryProcessed }, inventory: None })
            && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(V::Boolean(value)), .. }) if *value || !processed));
        let fate_valid = self.death_inventory.as_ref().is_none_or(|death|
            death.valid(action.id, action.tick) && death.identity.identity == crate::item_contact::ContactIdentity::Player(self.lease)
                && death.identity.chunk == self.source && action.dependencies.contains(&death.death_cause)
                && action.conditions.iter().any(|condition| condition.target == death.property(P::DeathCause)
                    && matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(V::ActionReference(Some(reference))), .. }) if *reference == death.death_cause)));

        fate_guard
            && fate_valid
            && self.detach.as_ref().is_none_or(|detach| {
                detach
                    .conditions
                    .iter()
                    .all(|guard| action.conditions.contains(guard))
            })
            && self.is_valid()
            && action.id.actor == actor
            && action.id.incarnation == self.lease.incarnation
            && action.actor_chunk == Some(self.source)
            && action.player_leases.as_slice() == [self.lease]
            && [P::Health, P::DeathCause].into_iter().all(|property| {
                action.conditions.iter().any(|condition| {
                    condition.target
                        == (Target::Chunk {
                            key: self.source,
                            part: ChunkPart::LivingProperty { actor, property },
                            inventory: None,
                        })
                        && matches!(
                            (&condition.expected, property),
                            (
                                Expected::Exact(Cell {
                                    value: Value::LivingProperty(V::Unsigned(0)),
                                    ..
                                }),
                                P::Health
                            ) | (
                                Expected::Exact(Cell {
                                    value: Value::LivingProperty(V::ActionReference(Some(_))),
                                    ..
                                }),
                                P::DeathCause
                            )
                        )
                })
            })
            && action.conditions.iter().any(|condition| {
                condition.target
                    == (Target::Chunk {
                        key: self.source,
                        part: ChunkPart::Player(self.lease.gid),
                        inventory: None,
                    })
                    && matches!(condition.expected, Expected::SpatialEpochEquals(_))
            })
    }

    pub fn capture_action<S: ActionState>(
        self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
    ) -> Result<crate::canonical_action::Action, ApplyFailure> {
        use crate::canonical_action::{
            Action, ActionClass, ActionKind, ClientOrigin, Condition, Expected, SemanticIntent,
            TruthView, UndoProcedure, Value,
        };
        use crate::living_property::LivingProperty as P;
        let source = Target::Chunk {
            key: self.source,
            part: ChunkPart::Player(self.lease.gid),
            inventory: None,
        };
        let Some(Cell {
            value: Value::Living(dead),
            ..
        }) = state.read(TruthView::Local, source)
        else {
            return Err(ApplyFailure::Condition(source));
        };
        if dead.combat.health_milli != 0 || dead.combat.timing.death_cause.is_none() {
            return Err(ApplyFailure::InvalidAction);
        }
        let epoch = dead
            .avatar
            .as_ref()
            .ok_or(ApplyFailure::Type(source))?
            .spatial_epoch;
        let mut conditions = vec![Condition {
            target: source,
            expected: Expected::SpatialEpochEquals(epoch),
        }];
        for property in [P::Health, P::DeathCause, P::DeathInventoryProcessed] {
            let target = Target::Chunk {
                key: self.source,
                part: ChunkPart::LivingProperty {
                    actor: id.actor,
                    property,
                },
                inventory: None,
            };
            let cell = crate::derived_living::facet_cell(
                &Cell {
                    revision: 0,
                    value: Value::Living((dead.clone()).into()),
                },
                property,
            )
            .ok_or(ApplyFailure::Type(target))?;
            conditions.push(Condition {
                target,
                expected: Expected::Exact(cell),
            });
        }
        if dead
            .avatar
            .as_ref()
            .is_some_and(|avatar| avatar.mount.is_some())
            != self.detach.is_some()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(detach) = &self.detach {
            conditions.extend(detach.conditions.clone());
        }
        if !dead.combat.timing.death_inventory_processed && self.death_inventory.is_none() {
            return Err(ApplyFailure::InvalidAction);
        }
        let targets = self.targets()?;
        let mut values = BTreeMap::new();
        let virtual_close_targets = self
            .death_inventory
            .as_ref()
            .and_then(|death| death.close.as_ref())
            .into_iter()
            .flat_map(|close| {
                close
                    .sources
                    .iter()
                    .chain(&close.destinations)
                    .chain(&close.results)
            })
            .filter(|binding| {
                matches!(
                    binding.slot.target,
                    crate::inventory::InventoryTarget::Virtual { .. }
                        | crate::inventory::InventoryTarget::LeasedVirtual { .. }
                )
            })
            .map(|binding| binding.target)
            .collect::<BTreeSet<_>>();
        for target in &targets {
            let cell = state.read(TruthView::Local, *target);
            if self.death_inventory.as_ref().is_some_and(|death| {
                death.drops.iter().any(|drop| drop.destination == *target)
                    || death.close.as_ref().is_some_and(|close| {
                        close.overflow.iter().any(|drop| drop.target == *target)
                    })
            }) {
                if cell.is_some() {
                    return Err(ApplyFailure::Condition(*target));
                }
                conditions.push(Condition {
                    target: *target,
                    expected: Expected::Missing,
                });
                continue;
            }
            if !matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::Player(_) | ChunkPart::Entity(_),
                    ..
                }
            ) {
                if cell.is_none() && virtual_close_targets.contains(target) {
                    conditions.push(Condition {
                        target: *target,
                        expected: Expected::Missing,
                    });
                    continue;
                }
                let cell = cell.ok_or(ApplyFailure::Condition(*target))?;

                if !matches!(target, Target::Chunk { part: ChunkPart::LivingProperty {
                    actor, property: P::Health | P::DeathCause | P::DeathInventoryProcessed,
                }, inventory: None, .. } if *actor == id.actor) {
                    conditions.push(Condition {
                        target: *target,
                        expected: Expected::Exact(cell.clone()),
                    });
                }
                values.insert(*target, cell);
            } else if let Some(cell) = cell {
                values.insert(*target, cell);
            } else {
                conditions.push(Condition {
                    target: *target,
                    expected: Expected::Missing,
                });
            }
        }
        let effects = self.derive(state, id, tick, &values)?;
        let chunk_transition = effects.iter().any(|effect| matches!(effect, Effect::RespawnPlayerAvatar { source: Target::Chunk { key: old, .. }, destination: Target::Chunk { key: next, .. }, .. } if old != next));
        let effect_targets = self.effect_targets()?;
        let player_chunks = targets.iter().any(|target| matches!(target,
            Target::Player { key, lease: Some(lease), inventory: Some(_) }
                if *key == self.lease.key && *lease == self.lease))
            .then_some((self.lease.key, self.source)).into_iter().collect();
        let action = Action {
            id,
            tick,
            class: ActionClass::Replayable,
            kind: ActionKind::PlayerAdministration,
            edit_authority: None,
            actor_chunk: Some(self.source),
            player_leases: vec![self.lease],
            player_chunks,
            stakeholders: targets
                .iter()
                .map(|target| target.stakeholder())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            dependencies: dead
                .combat
                .timing
                .death_cause
                .into_iter()
                .chain(match epoch {
                    SpatialEpoch::Action(reference) => Some(reference),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            conditions,
            effects,
            chunk_transition,
            counterfactual_conditions: targets,
            counterfactual_effects: effect_targets,
            semantic_intent: Some(SemanticIntent::Respawn(self)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }

    pub fn derive<S: ActionState>(
        &self,
        state: &S,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.is_valid()
            || id.actor != crate::actor::ActorId::Player(self.lease.gid)
            || id.incarnation != self.lease.incarnation
        {
            return Err(ApplyFailure::InvalidAction);
        }
        for plan in self.preferred.iter().chain(std::iter::once(&self.fallback)) {
            state.validate_world_border(plan.border, ActionRef { id, tick })?;
        }
        let preferred = self
            .preferred
            .map(|plan| {
                state
                    .resolve_world_respawn(plan, values)
                    .map(|result| result.map(|result| (plan.point.world, result)))
            })
            .transpose()?
            .flatten();
        let (world, result) = match preferred {
            Some(result) => result,
            None => (
                self.fallback.point.world,
                state
                    .resolve_world_respawn(self.fallback, values)?
                    .ok_or(ApplyFailure::InvalidAction)?,
            ),
        };
        let x = f64::from_bits(result.position[0]);
        let z = f64::from_bits(result.position[2]);
        if !x.is_finite() || !z.is_finite() {
            return Err(ApplyFailure::InvalidAction);
        }
        let destination = ChunkKey::new(
            world,
            (x.floor() as i32).div_euclid(16),
            (z.floor() as i32).div_euclid(16),
        );
        let mut avatar = self.avatar.clone();
        avatar.world = world;
        avatar.position = result.position;
        avatar.yaw = result.yaw;
        avatar.head_yaw = result.yaw;
        avatar.pitch = 0.0_f32.to_bits();
        avatar.spatial_epoch = SpatialEpoch::Action(ActionRef { tick, id });
        avatar.movement_tick = tick;
        let mut effects = if let Some(death) = &self.death_inventory {
            match values
                .get(
                    &death
                        .property(crate::living_property::LivingProperty::DeathInventoryProcessed),
                )
                .map(|cell| &cell.value)
            {
                Some(crate::canonical_action::Value::LivingProperty(
                    crate::living_property::LivingPropertyValue::Boolean(true),
                )) => Vec::new(),
                Some(crate::canonical_action::Value::LivingProperty(
                    crate::living_property::LivingPropertyValue::Boolean(false),
                )) => death.derive(state, id, tick, values)?,
                _ => return Err(ApplyFailure::InvalidAction),
            }
        } else {
            Vec::new()
        };
        effects.extend(
            self.detach
                .as_ref()
                .map_or_else(Vec::new, |detach| detach.effects.clone()),
        );
        effects.push(Effect::RespawnPlayerAvatar {
            source: Target::Chunk {
                key: self.source,
                part: ChunkPart::Player(self.lease.gid),
                inventory: None,
            },
            destination: Target::Chunk {
                key: destination,
                part: ChunkPart::Player(self.lease.gid),
                inventory: None,
            },
            avatar,
            max_health_attribute: self.max_health_attribute,
        });
        if let Some((target, _, after)) = result.anchor {
            effects.push(Effect::Put {
                target,
                value: crate::canonical_action::Value::Block(after),
            });
        }
        if let Some((anchor, _, _)) = result.anchor {
            let Target::Chunk { key, .. } = anchor else {
                return Err(ApplyFailure::InvalidAction);
            };
            let mut required = BTreeSet::new();
            for z in -1..=1 {
                for x in -1..=1 {
                    required.insert(ChunkKey::new(
                        key.world,
                        key.x().checked_add(x).ok_or(ApplyFailure::InvalidAction)?,
                        key.z().checked_add(z).ok_or(ApplyFailure::InvalidAction)?,
                    ));
                }
            }
            let plans: Vec<_> = self
                .light_plans
                .iter()
                .filter(|plan| required.contains(&plan.center))
                .copied()
                .collect();
            if plans
                .iter()
                .map(|plan| plan.center)
                .collect::<BTreeSet<_>>()
                != required
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let before = values.get(&anchor).ok_or(ApplyFailure::Condition(anchor))?;
            let after = effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::Put {
                        target,
                        value: crate::canonical_action::Value::Block(after),
                    } if *target == anchor => Some(*after),
                    _ => None,
                })
                .ok_or(ApplyFailure::InvalidAction)?;
            let writes = [crate::canonical_action::AppliedWrite {
                target: anchor,
                before: Some(before.clone()),
                after: Some(Cell {
                    revision: before
                        .revision
                        .checked_add(1)
                        .ok_or(ApplyFailure::InvalidAction)?,
                    value: crate::canonical_action::Value::Block(after),
                }),
            }];
            for plan in plans {
                let mut projected = values.clone();
                for target in plan.section_targets()? {
                    let before = values.get(&target).ok_or(ApplyFailure::Condition(target))?;
                    projected.insert(
                        target,
                        crate::block_section::project_writes(target, before, &writes)?,
                    );
                }
                effects.extend(state.derive_world_light(plan, &projected)?);
            }
        }
        Ok(effects)
    }
}

#[cfg(test)]
#[path = "semantic_respawn_tests.rs"]
mod tests;
