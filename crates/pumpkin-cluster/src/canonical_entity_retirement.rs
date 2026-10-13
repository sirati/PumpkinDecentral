use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{Action, ActionClass, ActionId, ActionKind, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, ClientOrigin, Condition, Effect, Expected, InventoryId, SpatialEpoch, Target, TruthView, UndoProcedure, Value};
use crate::canonical_inventory::InventorySlotBinding;
use crate::entity_body::{EntityBirth, EntityBody};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityInventoryDisposition {
    Destroy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingDeathRetirement {
    pub death_cause: ActionRef,
    pub epoch: SpatialEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRetirementIntent {
    pub target: Target,
    pub birth: EntityBirth,
    pub owner: PeerGeneration,
    pub kind: u16,
    pub inventory: Vec<InventorySlotBinding>,
    pub disposition: EntityInventoryDisposition,
    pub living_death: Option<LivingDeathRetirement>,
}

impl EntityRetirementIntent {
    pub fn valid(&self) -> bool {
        let Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None } = self.target else { return false; };
        self.owner.generation != 0 && self.birth.source.generation == entity.generation
            && self.birth.source.server == entity.origin && !self.birth.uuid.is_nil()
            && self.inventory.len() <= 35
            && self.inventory.iter().map(|binding| binding.target).collect::<BTreeSet<_>>().len() == self.inventory.len()
            && self.inventory.iter().all(|binding| binding.is_valid()
                && matches!(binding.target, Target::Chunk { key: chunk, part: ChunkPart::Entity(id), inventory: Some(InventoryId::Entity { .. }) }
                    if chunk == key && id == entity))
    }

    pub fn relation_targets(&self) -> Vec<Target> {
        let Target::Chunk { key, part: ChunkPart::Entity(entity), .. } = self.target else { return Vec::new(); };
        std::iter::once(crate::actor_relation::ActorRelation::Mount)
            .chain((0..32).map(crate::actor_relation::ActorRelation::Passenger))
            .map(|relation| Target::Chunk { key, part: ChunkPart::ActorRelation { actor: ActorId::Entity(entity), relation }, inventory: None }).collect()
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.target).chain(self.inventory.iter().map(|binding| binding.target)).chain(self.relation_targets())
    }

    pub fn death_targets(&self) -> Vec<Target> {
        let Target::Chunk { key, part: ChunkPart::Entity(entity), .. } = self.target else { return Vec::new(); };
        if self.living_death.is_none() { return Vec::new(); }
        use crate::living_property::LivingProperty as P;
        [P::Health, P::DeathCause, P::DeathInventoryProcessed, P::DeathTicks].into_iter().map(|property|
            Target::Chunk { key, part: ChunkPart::LivingProperty { actor: ActorId::Entity(entity), property }, inventory: None }).collect()
    }

    pub fn validate_body<S: ActionState>(&self, state: &S, body: &EntityBody) -> Result<(), ApplyFailure> {
        let Target::Chunk { key, part: ChunkPart::Entity(entity), .. } = self.target else { return Err(ApplyFailure::InvalidAction); };
        if !self.valid() || !body.belongs_to(entity, key) || body.birth != self.birth || body.owner != self.owner || body.kind != self.kind {
            return Err(ApplyFailure::InvalidAction);
        }
        match (&body.payload, &self.living_death) {
            (crate::entity_body::EntityPayload::Living(living), Some(death)) if body.spatial_epoch == death.epoch
                && living.combat.health_milli == 0 && living.combat.timing.death_inventory_processed
                && living.combat.timing.death_cause == Some(death.death_cause) && living.combat.timing.death_ticks >= 20 => {}
            (crate::entity_body::EntityPayload::Living(_), _) | (_, Some(_)) => return Err(ApplyFailure::InvalidAction),
            _ => {}
        }
        let layout = state.entity_inventory_layout(body)?.into_iter().collect::<BTreeSet<_>>();
        let supplied = self.inventory.iter().map(|binding| binding.slot.loc).collect::<BTreeSet<_>>();
        if supplied != layout || supplied.len() != self.inventory.len() { return Err(ApplyFailure::InvalidAction); }
        Ok(())
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        if let Some(terminal) = crate::object_tick::terminal_witness(action) {
            return self.valid() && self.living_death.is_none() && self.inventory.is_empty()
                && *self == terminal.retirement() && crate::object_tick::valid_terminal_action(action);
        }
        if let Some(terminal) = crate::vehicle_terminal::witness(action) {
            return self.valid() && self.living_death.is_none() && *self == terminal.retirement()
                && crate::vehicle_terminal::valid_terminal_action(action);
        }
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        let kind = if self.living_death.is_some() { ActionKind::Actor } else { ActionKind::PlayerAdministration };
        self.valid() && action.kind == kind && action.class == ActionClass::Undoable
            && matches!(self.target, Target::Chunk { key, part: ChunkPart::Entity(entity), .. }
                if action.id.actor == ActorId::Entity(entity) && action.id.incarnation == entity.generation && action.actor_chunk == Some(key))
            && action.effects.iter().filter(|effect| matches!(effect, Effect::RetireEntity { .. })).count() == 1
            && action.effects.iter().all(|effect| matches!(effect, Effect::RetireEntity { intent } if intent == self)
                || matches!(effect, Effect::Put { value: Value::ActorRelation(None), .. }))
            && (action.effects.len() == 1 || crate::actor_relation::owner_detach_all(action))
            && action.conditions.iter().any(|condition| condition.target == self.target
                && matches!(&condition.expected, Expected::EntityBirthKind { birth, owner, kind }
                    if *birth == self.birth && *owner == self.owner && *kind == self.kind))
            && self.inventory.iter().all(|binding| action.conditions.iter().any(|condition|
                condition.target == binding.target && matches!(&condition.expected, Expected::Exact(cell)
                    if matches!(&cell.value, Value::Inventory(item) if item.loc == binding.slot.loc))))
            && self.relation_targets().iter().all(|target| action.conditions.iter().any(|condition|
                condition.target == *target && matches!(&condition.expected, Expected::Exact(Cell { value: Value::ActorRelation(_), .. }))))
            && self.living_death.as_ref().is_none_or(|death| !death.death_cause.tick.is_after(action.tick)
                && action.dependencies.contains(&death.death_cause)
                && action.conditions.contains(&Condition { target: self.target, expected: Expected::SpatialEpochEquals(death.epoch) })
                && self.death_targets().iter().all(|target| action.conditions.iter().any(|condition| condition.target == *target
                    && match target {
                        Target::Chunk { part: ChunkPart::LivingProperty { property: P::Health, .. }, .. } => matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(V::Unsigned(0)), .. })),
                        Target::Chunk { part: ChunkPart::LivingProperty { property: P::DeathCause, .. }, .. } => matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(V::ActionReference(Some(cause))), .. }) if *cause == death.death_cause),
                        Target::Chunk { part: ChunkPart::LivingProperty { property: P::DeathInventoryProcessed, .. }, .. } => matches!(&condition.expected, Expected::Exact(Cell { value: Value::LivingProperty(V::Boolean(true)), .. })),
                        Target::Chunk { part: ChunkPart::LivingProperty { property: P::DeathTicks, .. }, .. } => condition.expected == Expected::LivingUnsignedAtLeast(20),
                        _ => false,
                    })))
    }

    pub fn capture_action<S: ActionState>(&self, state: &S, id: ActionId, tick: TickStamp, client_origin: ClientOrigin) -> Result<Action, ApplyFailure> {
        self.capture_action_with_detach(state, id, tick, client_origin, None)
    }

    pub fn capture_action_with_detach<S: ActionState>(&self, state: &S, id: ActionId, tick: TickStamp, client_origin: ClientOrigin,
        detach: Option<crate::actor_relation::OwnerDetachPlan>) -> Result<Action, ApplyFailure> {
        let Some(Cell { value: Value::Entity(body), .. }) = state.read(TruthView::Local, self.target) else { return Err(ApplyFailure::Condition(self.target)); };
        self.validate_body(state, &body)?;
        if id.actor != ActorId::Entity(body.id) || id.incarnation != body.id.generation { return Err(ApplyFailure::InvalidAction); }
        let mut conditions = vec![Condition { target: self.target, expected: Expected::EntityBirthKind {
            birth: self.birth.clone(), owner: self.owner, kind: self.kind,
        } }, Condition { target: self.target, expected: Expected::ActorControlled { birth: self.birth.clone(), issuer: self.owner } }];
        for target in self.targets().filter(|target| *target != self.target).chain(self.death_targets()) {
            let cell = state.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
            let floor = matches!(target, Target::Chunk { part: ChunkPart::LivingProperty { property: crate::living_property::LivingProperty::DeathTicks, .. }, .. });
            conditions.push(Condition { target, expected: if floor { Expected::LivingUnsignedAtLeast(20) } else { Expected::Exact(cell) } });
        }
        let mut dependencies = vec![self.birth.action];
        if let Some(death) = &self.living_death {
            dependencies.push(death.death_cause);
            conditions.push(Condition { target: self.target, expected: Expected::SpatialEpochEquals(death.epoch) });
        }
        let mut effects = Vec::new();
        let mut player_leases = Vec::new();
        if let Some(detach) = detach { conditions.extend(detach.conditions); effects.extend(detach.effects); player_leases = detach.leases; }
        conditions.sort_by_key(|condition| condition.target); conditions.dedup();
        effects.push(Effect::RetireEntity { intent: self.clone() });
        let stakeholders = conditions.iter().map(|condition| condition.target.stakeholder())
            .chain(effects.iter().flat_map(Effect::targets).map(Target::stakeholder)).collect::<BTreeSet<_>>().into_iter().collect();
        let Target::Chunk { key, .. } = self.target else { return Err(ApplyFailure::InvalidAction); };
        let action = Action {
            id, tick, edit_authority: None, class: ActionClass::Undoable, chunk_transition: false,
            actor_chunk: Some(key), player_leases, player_chunks: Vec::new(), stakeholders, dependencies,
            conditions, effects, counterfactual_conditions: Vec::new(), counterfactual_effects: Vec::new(), semantic_intent: None,
            factual_result: None, undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: if self.living_death.is_some() { ActionKind::Actor } else { ActionKind::PlayerAdministration },
            client_origin, presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}
