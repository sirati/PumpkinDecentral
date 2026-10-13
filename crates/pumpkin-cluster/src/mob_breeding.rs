use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionKind, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart,
    ClientOrigin, Condition, Effect, Expected, LivePlayerLease, SemanticIntent, SpatialEpoch,
    Target, TruthView, UndoProcedure, Value,
};
use crate::entity_body::{EntityBody, EntityPayload};
use crate::entity_inventory_admission::EntityInventoryAdmission;
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::living_property::{LivingProperty, LivingPropertyValue};
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobBirthAdmission {
    pub chunk: ChunkKey,
    pub body: EntityBody,
    pub inventory: EntityInventoryAdmission,
}

impl MobBirthAdmission {
    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: ChunkPart::Entity(self.body.id),
            inventory: None,
        }
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.target()).chain(
            self.inventory
                .entries(self.chunk, self.body.id)
                .map(|(target, _)| target),
        )
    }

    fn effect(&self) -> Effect {
        Effect::EntityAdmission {
            target: self.target(),
            body: self.body.clone(),
            inventory: self.inventory.clone(),
        }
    }

    fn valid(&self, source: PeerGeneration, reference: ActionRef) -> bool {
        self.body.id.origin == source.server
            && self.body.id.generation == source.generation
            && self.body.birth.source == source
            && self.body.birth.action == reference
            && self.body.spatial_epoch == SpatialEpoch::Action(reference)
            && self.body.owner.generation != 0
            && !self.body.birth.uuid.is_nil()
            && self.body.belongs_to(self.body.id, self.chunk)
            && self.body.valid_at(reference.tick)
            && self.inventory.imported_revision == 0
            && self.inventory.valid()
            && self.inventory.cells.iter().all(|cell| {
                matches!(&cell.value,
                Value::Inventory(item) if item.is_empty())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobBreedingCredit {
    pub breeder: LivePlayerLease,
    pub chunk: ChunkKey,
    pub statistic_category: u16,
    pub statistic_key: u32,
    pub timestamp: u64,
}

impl MobBreedingCredit {
    fn target(&self, property: LivingProperty) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: ChunkPart::LivingProperty {
                actor: ActorId::Player(self.breeder.gid),
                property,
            },
            inventory: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobBreedingIntent {
    pub parents: [ContactObservation; 2],
    pub issuer: PeerGeneration,
    pub child: MobBirthAdmission,
    pub experience: MobBirthAdmission,
    pub credit: Option<MobBreedingCredit>,
}

fn facet(parent: &ContactObservation, property: LivingProperty) -> Target {
    Target::Chunk {
        key: parent.chunk,
        part: ChunkPart::LivingProperty {
            actor: parent.actor(),
            property,
        },
        inventory: None,
    }
}

impl MobBreedingIntent {
    pub fn is_valid(&self, tick: TickStamp) -> bool {
        let reference = self.child.body.birth.action;
        let [source, mate] = &self.parents;
        source.valid()
            && mate.valid()
            && source.world == mate.world
            && source.simulation_tick == tick
            && mate.simulation_tick == tick
            && source.identity != mate.identity
            && source.owner == self.issuer
            && self.issuer.generation != 0
            && matches!(source.identity, ContactIdentity::Entity { .. })
            && matches!(mate.identity, ContactIdentity::Entity { .. })
            && reference.tick == tick
            && reference.id.actor == source.actor()
            && matches!(source.identity, ContactIdentity::Entity { id, .. } if reference.id.incarnation == id.generation)
            && source
                .position
                .into_iter()
                .zip(mate.position)
                .map(|(a, b)| (f64::from_bits(a) - f64::from_bits(b)).powi(2))
                .sum::<f64>()
                < 9.0
            && self.child.valid(self.issuer, reference)
            && self.child.body.owner == self.issuer
            && self.child.chunk == source.chunk
            && self.child.body.spatial.position == source.position
            && matches!(&self.child.body.payload, EntityPayload::Living(living)
                if living.mob.as_ref().is_some_and(|mob| mob.reproduction.growth_age == -24000
                    && mob.reproduction.love_ticks == 0 && mob.reproduction.breeding_cooldown == 0))
            && self.experience.valid(self.issuer, reference)
            && self.experience.chunk == source.chunk
            && self.experience.body.spatial.position == source.position
            && self.experience.body.id != self.child.body.id
            && self.experience.body.birth.uuid != self.child.body.birth.uuid
            && matches!(&self.experience.body.payload, EntityPayload::Experience(orb)
                if (1..=7).contains(&orb.amount) && orb.count == 1 && orb.age == 0
                    && orb.health_milli == 5000 && orb.last_lifetime_tick.is_none())
            && self.credit.as_ref().is_none_or(|credit| {
                credit.breeder.incarnation != 0
                    && !credit.breeder.key.0.is_nil()
                    && credit.chunk.world == source.world
            })
    }

    pub fn resolution_targets(&self) -> BTreeSet<Target> {
        self.parents
            .iter()
            .flat_map(|parent| {
                [
                    parent.target(),
                    facet(parent, LivingProperty::Health),
                    facet(parent, LivingProperty::MobReproduction),
                ]
            })
            .chain(self.child.targets())
            .chain(self.experience.targets())
            .chain(self.credit.iter().flat_map(|credit| {
                [
                    credit.target(LivingProperty::Statistic {
                        category: credit.statistic_category,
                        key: credit.statistic_key,
                    }),
                    credit.target(LivingProperty::Advancements),
                    credit.target(LivingProperty::Experience),
                ]
            }))
            .collect()
    }

    pub fn effect_targets(&self) -> BTreeSet<Target> {
        self.parents
            .iter()
            .map(|parent| facet(parent, LivingProperty::MobReproduction))
            .chain(self.child.targets())
            .chain(self.experience.targets())
            .chain(self.credit.iter().flat_map(|credit| {
                [
                    credit.target(LivingProperty::Statistic {
                        category: credit.statistic_category,
                        key: credit.statistic_key,
                    }),
                    credit.target(LivingProperty::Advancements),
                    credit.target(LivingProperty::Experience),
                ]
            }))
            .collect()
    }

    pub fn guards(&self) -> Vec<Condition> {
        let mut guards = Vec::new();
        for (index, parent) in self.parents.iter().enumerate() {
            let ContactIdentity::Entity { birth, .. } = &parent.identity else {
                continue;
            };
            guards.push(Condition {
                target: parent.target(),
                expected: if index == 0 {
                    Expected::ActorControlled {
                        birth: birth.clone(),
                        issuer: self.issuer,
                    }
                } else {
                    Expected::EntityBirthOwned {
                        birth: birth.clone(),
                        owner: parent.owner,
                    }
                },
            });
            guards.push(Condition {
                target: parent.target(),
                expected: Expected::SpatialEpochEquals(parent.epoch),
            });
        }
        guards.extend(
            self.child
                .targets()
                .chain(self.experience.targets())
                .map(|target| Condition {
                    target,
                    expected: Expected::Missing,
                }),
        );
        guards
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        self.is_valid(action.tick)
            && action.id == self.child.body.birth.action.id
            && action.actor_chunk == Some(self.parents[0].chunk)
            && self
                .guards()
                .iter()
                .all(|guard| action.conditions.contains(guard))
            && self
                .credit
                .as_ref()
                .is_none_or(|credit| action.player_leases.contains(&credit.breeder))
    }

    pub fn permits_admission(
        &self,
        target: Target,
        body: &EntityBody,
        inventory: &EntityInventoryAdmission,
    ) -> bool {
        [&self.child, &self.experience].into_iter().any(|birth| {
            birth.target() == target && birth.body == *body && birth.inventory == *inventory
        })
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &S,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.is_valid(self.child.body.birth.action.tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut effects = Vec::new();
        let mut breeder = None;
        for (index, parent) in self.parents.iter().enumerate() {
            let target = parent.target();
            let Some(Cell {
                value: Value::Entity(body),
                ..
            }) = values.get(&target)
            else {
                return Err(ApplyFailure::Condition(target));
            };
            if body.kind != self.child.body.kind {
                return Err(ApplyFailure::Condition(target));
            }
            let health = facet(parent, LivingProperty::Health);
            if !matches!(
                values.get(&health),
                Some(Cell {
                    value: Value::LivingProperty(LivingPropertyValue::Unsigned(1..)),
                    ..
                })
            ) {
                return Err(ApplyFailure::Condition(health));
            }
            let target = facet(parent, LivingProperty::MobReproduction);
            let Some(Cell {
                value:
                    Value::LivingProperty(LivingPropertyValue::MobReproduction(Some(reproduction))),
                ..
            }) = values.get(&target)
            else {
                return Err(ApplyFailure::Condition(target));
            };
            if reproduction.growth_age < 0
                || reproduction.love_ticks == 0
                || reproduction.breeding_cooldown != 0
            {
                return Err(ApplyFailure::Condition(target));
            }
            if index == 0 {
                breeder = reproduction.breeder;
            }
            let mut next = reproduction.clone();
            next.love_ticks = 0;
            next.breeding_cooldown = 6000;
            effects.push(Effect::Put {
                target,
                value: Value::LivingProperty(LivingPropertyValue::MobReproduction(Some(next))),
            });
        }
        effects.push(self.child.effect());
        effects.push(self.experience.effect());
        if let Some(credit) = &self.credit {
            if breeder != Some(credit.breeder.key.0) {
                return Err(ApplyFailure::InvalidAction);
            }
            let target = credit.target(LivingProperty::Statistic {
                category: credit.statistic_category,
                key: credit.statistic_key,
            });
            let Some(Cell {
                value: Value::LivingProperty(LivingPropertyValue::Unsigned(count)),
                ..
            }) = values.get(&target)
            else {
                return Err(ApplyFailure::Condition(target));
            };
            let count = count.saturating_add(1).min(i32::MAX as u32);
            effects.push(Effect::Put {
                target,
                value: Value::LivingProperty(LivingPropertyValue::Unsigned(count)),
            });
            crate::player_advancement_event::append_effects(context, self.child.body.birth.action,
                ActorId::Player(credit.breeder.gid), credit.chunk, values, &mut effects,
                &crate::player_advancement::AdvancementEvent::BredAnimal { parent_type: self.child.body.kind })?;
        }
        Ok(effects)
    }

    pub fn capture<S: ActionState>(self, context: &S) -> Result<Action, ApplyFailure> {
        let reference = self.child.body.birth.action;
        if !self.is_valid(reference.tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let targets = self.resolution_targets();
        let mut values = BTreeMap::new();
        let mut conditions = self.guards();
        for target in &targets {
            if self
                .child
                .targets()
                .chain(self.experience.targets())
                .any(|birth| birth == *target)
            {
                continue;
            }
            let cell = context
                .read(TruthView::Local, *target)
                .ok_or(ApplyFailure::Condition(*target))?;
            if !self.parents.iter().any(|parent| parent.target() == *target) {
                conditions.push(Condition {
                    target: *target,
                    expected: Expected::Exact(cell.clone()),
                });
            }
            values.insert(*target, cell);
        }
        let effects = self.derive(context, &values)?;
        let action = Action {
            edit_authority: None,
            id: reference.id,
            tick: reference.tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: Some(self.parents[0].chunk),
            player_leases: self.credit.iter().map(|credit| credit.breeder).collect(),
            player_chunks: self
                .credit
                .iter()
                .map(|credit| (credit.breeder.key, credit.chunk))
                .collect(),
            stakeholders: targets
                .iter()
                .map(|target| target.stakeholder())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            dependencies: self
                .parents
                .iter()
                .filter_map(|parent| match parent.epoch {
                    SpatialEpoch::Action(reference) => Some(reference),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            conditions,
            effects,
            counterfactual_conditions: targets.iter().copied().collect(),
            counterfactual_effects: self.effect_targets().into_iter().collect(),
            semantic_intent: Some(SemanticIntent::MobBreeding(Box::new(self))),
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
