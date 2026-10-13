use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::action_graph::{GroundAnchor, TickGraphSnapshot};
use crate::canonical_action::{ActionRef, ApplyFailure, Cell, ChunkPart, Target, Value};
use crate::entity_body::EntityBirth;
use crate::protocol::{ChunkKey, GlobalEntityId};
use crate::realm_completion::{RealmCompletionLedger, RealmCompletionSnapshot};
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityHistoryEvidence {
    pub snapshot_cut: TickStamp,
    pub graphs: Vec<TickGraphSnapshot>,
    pub anchors: Vec<(Target, GroundAnchor)>,
    pub completion: RealmCompletionSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityDisposition {
    Removed,
    Located(ChunkKey, crate::realm_completion::PeerGeneration),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityActorLocationBinding {
    pub action: ActionRef,
    pub birth: EntityBirth,
    pub issuer: crate::realm_completion::PeerGeneration,
    pub captured_source: Target,
    pub resolved_source: Target,
    pub transitions: Vec<ActionRef>,
}

pub struct EntityBindingProofContext {
    events: BTreeMap<GlobalEntityId, BTreeMap<ActionRef, crate::entity_body::EntityGroundEvent>>,
    transitions: BTreeSet<(GlobalEntityId, ActionRef)>,
    outcomes: BTreeMap<ActionRef, crate::canonical_action::AppliedAction>,
}

impl EntityBindingProofContext {
    pub fn from_frames<'a>(
        frames: impl IntoIterator<Item = &'a EntityHistoryEvidence>,
    ) -> Result<Self, ApplyFailure> {
        let mut context = Self {
            events: BTreeMap::new(),
            transitions: BTreeSet::new(),
            outcomes: BTreeMap::new(),
        };
        let mut births = BTreeMap::new();
        let mut uuids = BTreeMap::new();
        let mut cores = BTreeMap::new();
        let mut event_count = 0;
        let mut pending = Vec::new();
        for (index, frame) in frames.into_iter().enumerate() {
            if index >= 128 {
                return Err(ApplyFailure::InvalidAction);
            }
            pending.push(frame);
        }
        while !pending.is_empty() {
            let position = pending
                .iter()
                .position(|frame| {
                    frame
                        .graphs
                        .iter()
                        .flat_map(|graph| &graph.nodes)
                        .all(|node| {
                            node.actor_binding.as_ref().is_none_or(|binding| {
                                binding
                                    .transitions
                                    .iter()
                                    .all(|reference| context.outcomes.contains_key(reference))
                            })
                        })
                })
                .ok_or(ApplyFailure::InvalidAction)?;
            let frame = pending.remove(position);
            for (id, birth) in frame.births()? {
                if births
                    .insert(id, birth.clone())
                    .is_some_and(|old| old != birth)
                    || uuids.insert(birth.uuid, id).is_some_and(|old| old != id)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            let nodes = frame
                .graphs
                .iter()
                .flat_map(|graph| {
                    graph.nodes.iter().map(move |node| {
                        (
                            ActionRef {
                                tick: graph.tick,
                                id: node.action.id,
                            },
                            node,
                        )
                    })
                })
                .collect::<BTreeMap<_, _>>();
            for (reference, node) in &nodes {
                if cores
                    .insert(*reference, node.action.clone())
                    .is_some_and(|old| old != node.action)
                    || cores.len() > 8_192
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            for (id, event) in frame.fixed_departures_with_context(Some(&context))? {
                let reference = event.action;
                let result = nodes
                    .get(&reference)
                    .and_then(|node| node.factual.result.as_ref())
                    .ok_or(ApplyFailure::InvalidAction)?;
                if context
                    .outcomes
                    .insert(reference, result.clone())
                    .is_some_and(|old| old != *result)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                match context
                    .events
                    .entry(id)
                    .or_default()
                    .insert(reference, event.clone())
                {
                    Some(old) if old != event => return Err(ApplyFailure::InvalidAction),
                    Some(_) => {}
                    None => {
                        event_count += 1;
                        if event_count > 8_192 {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                if nodes.get(&reference).is_some_and(|node| {
                    node.action.chunk_transition && node.action.effects.iter().any(|effect| {
                            matches!(effect, crate::canonical_action::Effect::MoveEntityBody {
                                source: Target::Chunk { part: ChunkPart::Entity(source), inventory: None, .. },
                                destination: Target::Chunk { part: ChunkPart::Entity(destination), inventory: None, .. },
                                ..
                            } if *source == id && *destination == id)
                        })
                }) {
                    context.transitions.insert((id, reference));
                }
            }
        }
        Ok(context)
    }

    pub fn binding_candidate(
        &self,
        action: &crate::canonical_action::Action,
    ) -> Result<Option<EntityActorLocationBinding>, ApplyFailure> {
        binding_from_events(action, &self.events, &self.transitions)
    }

    pub fn binding_chunks(
        &self,
        action: &crate::canonical_action::Action,
        binding: &EntityActorLocationBinding,
    ) -> Result<BTreeSet<ChunkKey>, ApplyFailure> {
        if self.binding_candidate(action)?.as_ref() != Some(binding) {
            return Err(ApplyFailure::InvalidAction);
        }
        let crate::actor::ActorId::Entity(entity) = action.id.actor else {
            return Err(ApplyFailure::InvalidAction);
        };
        let mut chunks = BTreeSet::new();
        for reference in &binding.transitions {
            let result = self
                .outcomes
                .get(reference)
                .ok_or(ApplyFailure::InvalidAction)?;
            for write in &result.writes {
                if let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(id),
                    inventory: None,
                } = write.target
                    && id == entity
                {
                    chunks.insert(key);
                }
            }
        }
        Ok(chunks)
    }
}

impl EntityActorLocationBinding {
    pub fn interpret_core(
        &self,
        core: &crate::canonical_action::Action,
    ) -> Result<crate::canonical_action::Action, ApplyFailure> {
        use crate::canonical_action::{Effect, Expected, Stakeholder, UndoProcedure};
        core.validate()?;
        if self.action
            != (ActionRef {
                tick: core.tick,
                id: core.id,
            })
            || self.transitions.is_empty()
            || self
                .transitions
                .iter()
                .any(|reference| !core.tick.is_after(reference.tick))
            || core.undo != vec![UndoProcedure::RestoreWrittenTargets]
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let (
            Target::Chunk {
                key: captured,
                part: ChunkPart::Entity(id),
                inventory: None,
            },
            Target::Chunk {
                key: resolved,
                part: ChunkPart::Entity(destination),
                inventory: None,
            },
        ) = (self.captured_source, self.resolved_source)
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if id != destination
            || core.id.actor != crate::actor::ActorId::Entity(id)
            || core.actor_chunk != Some(captured)
            || core.captured_entity_source()
                != Some((self.captured_source, &self.birth, self.issuer))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut interpreted = core.clone();
        interpreted.actor_chunk = Some(resolved);
        interpreted.factual_result = None;
        for condition in &mut interpreted.conditions {
            if condition.target == self.captured_source
                && matches!(&condition.expected,
                Expected::ActorControlled { birth, issuer } if *birth == self.birth && *issuer == self.issuer)
            {
                condition.target = self.resolved_source;
            } else if condition.target == self.captured_source
                && matches!(&condition.expected,
                Expected::DroppedItem { birth, .. } if *birth == self.birth)
            {
                condition.target = self.resolved_source;
            } else if condition.target == self.captured_source
                && matches!(&condition.expected, Expected::SpatialEpochEquals(_))
            {
                condition.target = self.resolved_source;
            } else if condition.target == self.captured_source
                && matches!(&condition.expected, Expected::DroppedItemStack { birth, owner, .. }
                    if *birth == self.birth && *owner == self.issuer)
            {
                condition.target = self.resolved_source;
            }
        }
        if let Some(crate::canonical_action::SemanticIntent::ObjectTick(intent)) = &mut interpreted.semantic_intent {
            if intent.body_target() != self.captured_source || intent.birth != self.birth || intent.owner != self.issuer {
                return Err(ApplyFailure::InvalidAction);
            }
            let old_targets = intent.targets();
            intent.chunk = resolved;
            let new_targets = intent.targets();
            for (old, new) in old_targets.into_iter().zip(new_targets) {
                for condition in &mut interpreted.conditions {
                    if condition.target == old { condition.target = new; }
                }
                for effect in &mut interpreted.effects {
                    if let Effect::Put { target, value: crate::canonical_action::Value::ObjectProperty(_) } = effect
                        && *target == old { *target = new; }
                }
                for target in interpreted.counterfactual_conditions.iter_mut().chain(&mut interpreted.counterfactual_effects) {
                    if *target == old { *target = new; }
                }
            }
        }
        for effect in &mut interpreted.effects {
            if let Effect::UpdateVehicleProperty {
                target, authority, ..
            } = effect
                && authority.birth == self.birth
                && authority.body_owner == self.issuer
                && crate::vehicle_property::body_target(*target)
                    .is_some_and(|(body, _)| body == self.captured_source)
            {
                let Target::Chunk { key, .. } = self.resolved_source else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let Target::Chunk {
                    key: target_key, ..
                } = target
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                *target_key = key;
            }
            if let Effect::UpdateMobContinuation {
                target, authority, ..
            } = effect
                && authority.birth == self.birth
                && authority.body_owner == self.issuer
                && crate::living_property::body_target(*target)
                    .is_some_and(|(body, _)| body == self.captured_source)
            {
                let Target::Chunk { key, .. } = self.resolved_source else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let Target::Chunk {
                    key: target_key, ..
                } = target
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                *target_key = key;
            }
            if let Effect::AdvanceExperienceLifetime {
                target,
                birth,
                owner,
            } = effect
                && *target == self.captured_source
                && *birth == self.birth
                && *owner == self.issuer
            {
                *target = self.resolved_source;
            }
            if let Effect::ChangeDroppedItem {
                target,
                birth,
                body_owner,
                ..
            } = effect
                && *target == self.captured_source
                && *birth == self.birth
                && *body_owner == self.issuer
            {
                *target = self.resolved_source;
            }
        }
        if let Some(crate::canonical_action::SemanticIntent::InventoryMove(intent)) =
            &mut interpreted.semantic_intent
            && intent.source == self.captured_source
        {
            if core.class != crate::canonical_action::ActionClass::Replayable
                || core.conditions.iter().any(|condition| {
                    condition.target == self.captured_source
                        && !matches!(
                            condition.expected,
                            Expected::ActorControlled { .. }
                                | Expected::DroppedItem { .. }
                                | Expected::SpatialEpochEquals(_)
                        )
                })
                || core.effects.iter().any(|effect| {
                    effect
                        .targets()
                        .any(|target| target == self.captured_source)
                        && !matches!(effect, Effect::MoveItems { source, .. }
                            if *source == self.captured_source)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            intent.source = self.resolved_source;
            for effect in &mut interpreted.effects {
                if let Effect::MoveItems { source, .. } = effect
                    && *source == self.captured_source
                {
                    *source = self.resolved_source;
                }
            }
        }
        if let Some(crate::canonical_action::SemanticIntent::InventoryGestures(intent)) =
            &mut interpreted.semantic_intent
        {
            if core.class != crate::canonical_action::ActionClass::Replayable {
                return Err(ApplyFailure::InvalidAction);
            }
            let mut replacements = BTreeMap::new();
            for binding in &mut intent.bindings {
                if let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(entity),
                    inventory: Some(inventory),
                } = binding.target
                    && entity == id
                {
                    if key != captured
                        || !matches!(binding.slot.target, crate::inventory::InventoryTarget::Entity(entity) if entity == id.into())
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let next = Target::Chunk {
                        key: resolved,
                        part: ChunkPart::Entity(id),
                        inventory: Some(inventory),
                    };
                    replacements.insert(binding.target, next);
                    binding.target = next;
                }
            }
            for equipment in &mut intent.entity_equipment {
                if equipment.entity != id { continue; }
                if equipment.birth != self.birth || equipment.living != self.captured_source {
                    return Err(ApplyFailure::InvalidAction);
                }
                let mut prior = equipment.property_targets();
                prior.extend(equipment.storage_property_targets());
                equipment.living = self.resolved_source;
                for (_, target) in &mut equipment.slots {
                    *target = *replacements.get(target).ok_or(ApplyFailure::InvalidAction)?;
                }
                for target in &mut equipment.storage_slots {
                    *target = *replacements.get(target).ok_or(ApplyFailure::InvalidAction)?;
                }
                for target in prior {
                    let Target::Chunk { part, inventory, .. } = target else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    replacements.insert(target, Target::Chunk { key: resolved, part, inventory });
                }
            }
            for condition in &mut interpreted.conditions {
                if condition.target == self.captured_source
                    && matches!(&condition.expected, Expected::EntityBirthOwned { birth, owner } | Expected::EntityBirthKind { birth, owner, .. }
                        if *birth == self.birth && *owner == self.issuer) {
                    condition.target = self.resolved_source;
                }
                if let Some(next) = replacements.get(&condition.target) {
                    condition.target = *next;
                }
            }
            for effect in &mut interpreted.effects {
                match effect {
                    Effect::Put {
                        target,
                        value: crate::canonical_action::Value::Inventory(_)
                            | crate::canonical_action::Value::LivingProperty(_),
                        ..
                    } => {
                        if let Some(next) = replacements.get(target) {
                            *target = *next;
                        }
                    }
                    _ if effect
                        .targets()
                        .any(|target| replacements.contains_key(&target)) =>
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    _ => {}
                }
            }
            for (old, new) in replacements {
                if core.counterfactual_conditions.contains(&old) {
                    interpreted.counterfactual_conditions.push(new);
                }
                if core.counterfactual_effects.contains(&old) {
                    interpreted.counterfactual_effects.push(new);
                }
            }
            interpreted.counterfactual_effects.sort_unstable();
            interpreted.counterfactual_effects.dedup();
        }
        interpreted
            .conditions
            .sort_by_key(|condition| condition.target);
        interpreted
            .counterfactual_conditions
            .push(self.captured_source);
        interpreted
            .counterfactual_conditions
            .push(self.resolved_source);
        interpreted.counterfactual_conditions.sort_unstable();
        interpreted.counterfactual_conditions.dedup();
        if interpreted.effects.iter().any(|effect| {
            effect
                .targets()
                .any(|target| target == self.resolved_source)
        }) {
            interpreted
                .counterfactual_effects
                .push(self.captured_source);
            interpreted
                .counterfactual_effects
                .push(self.resolved_source);
            interpreted.counterfactual_effects.sort_unstable();
            interpreted.counterfactual_effects.dedup();
        }
        interpreted.stakeholders = interpreted
            .targets()
            .into_iter()
            .chain(interpreted.counterfactual_conditions.iter().copied())
            .chain(interpreted.counterfactual_effects.iter().copied())
            .map(|target| target.stakeholder())
            .chain(std::iter::once(Stakeholder::Chunk(resolved)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        interpreted.validate()?;
        Ok(interpreted)
    }

    pub fn validate_against(
        &self,
        action: &crate::canonical_action::Action,
        history: &EntityHistoryEvidence,
    ) -> Result<(), ApplyFailure> {
        if history.actor_location_binding_candidate(action)?.as_ref() != Some(self) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }
}

impl EntityHistoryEvidence {
    pub fn fixed_dependency_outcomes(
        &self,
        proofs: Option<&EntityBindingProofContext>,
    ) -> Result<BTreeMap<ActionRef, bool>, ApplyFailure> {
        self.validate_with_context(proofs)?;
        let actions = self
            .graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().map(move |node| {
                    (
                        ActionRef {
                            tick: graph.tick,
                            id: node.action.id,
                        },
                        node.action.clone(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .import_snapshot(&self.completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(self
            .graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().filter_map(|node| {
                    let reference = ActionRef {
                        tick: graph.tick,
                        id: node.action.id,
                    };
                    (node.applied_realms == node.required_realms
                        && !node.required_realms.is_empty()
                        && ledger.action_complete(reference)
                        && node
                            .required_realms
                            .iter()
                            .all(|realm| ledger.realm_complete(graph.tick, *realm)))
                    .then_some((reference, node.factual.result.is_some()))
                })
            })
            .collect())
    }

    pub fn actor_location_binding_candidate(
        &self,
        action: &crate::canonical_action::Action,
    ) -> Result<Option<EntityActorLocationBinding>, ApplyFailure> {
        EntityBindingProofContext::from_frames([self])?.binding_candidate(action)
    }
}

fn binding_from_events(
    action: &crate::canonical_action::Action,
    events: &BTreeMap<GlobalEntityId, BTreeMap<ActionRef, crate::entity_body::EntityGroundEvent>>,
    transition_refs: &BTreeSet<(GlobalEntityId, ActionRef)>,
) -> Result<Option<EntityActorLocationBinding>, ApplyFailure> {
    action.validate()?;
    let crate::actor::ActorId::Entity(id) = action.id.actor else {
        return Ok(None);
    };
    let Some((captured_source, birth, issuer)) = action.captured_entity_source() else {
        return Ok(None);
    };
    let Target::Chunk {
        key,
        part: ChunkPart::Entity(written),
        inventory: None,
    } = captured_source
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    if written != id || action.actor_chunk != Some(key) {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut events = events
        .get(&id)
        .into_iter()
        .flat_map(|events| events.values())
        .filter_map(|event| {
            (event.birth == *birth && action.tick.is_after(event.action.tick))
                .then_some(event.clone())
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| {
        (
            std::cmp::Reverse(action.tick.distance_since(event.action.tick)),
            event.action.id,
        )
    });
    let mut current = (key, issuer);
    let mut transitions = Vec::new();
    for event in events {
        if event.before != Some(current) {
            continue;
        }
        let Some(next) = event.after else {
            return Ok(None);
        };
        if next.1 != issuer {
            return Ok(None);
        }
        if !transition_refs.contains(&(id, event.action)) {
            return Err(ApplyFailure::InvalidAction);
        }
        current = next;
        transitions.push(event.action);
    }
    if transitions.is_empty() {
        return Ok(None);
    }
    Ok(Some(EntityActorLocationBinding {
        action: ActionRef {
            tick: action.tick,
            id: action.id,
        },
        birth: birth.clone(),
        issuer,
        captured_source,
        resolved_source: Target::Chunk {
            key: current.0,
            part: ChunkPart::Entity(id),
            inventory: None,
        },
        transitions,
    }))
}

impl EntityHistoryEvidence {
    pub fn births(&self) -> Result<BTreeMap<GlobalEntityId, EntityBirth>, ApplyFailure> {
        let mut births = BTreeMap::new();
        let mut uuids = BTreeMap::new();
        for node in self.graphs.iter().flat_map(|graph| &graph.nodes) {
            for write in node.factual.result.iter().flat_map(|result| &result.writes) {
                for cell in [&write.before, &write.after].into_iter().flatten() {
                    let Value::Entity(body) = &cell.value else {
                        continue;
                    };
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Entity(id),
                        inventory: None,
                    } = write.target
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if !body.belongs_to(id, key)
                        || births
                            .insert(id, body.birth.clone())
                            .is_some_and(|previous| previous != body.birth)
                        || uuids
                            .insert(body.birth.uuid, id)
                            .is_some_and(|previous| previous != id)
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
        }
        Ok(births)
    }

    pub fn validate(&self) -> Result<(), ApplyFailure> {
        self.validate_with_context(None)
    }

    pub fn validate_with_context(
        &self,
        proofs: Option<&EntityBindingProofContext>,
    ) -> Result<(), ApplyFailure> {
        if self.graphs.len() > 4096 || self.anchors.len() > 65_536 {
            return Err(ApplyFailure::InvalidAction);
        }
        self.births()?;
        let mut actions = BTreeMap::new();
        let mut ticks = BTreeSet::new();
        let mut targets = BTreeSet::new();
        for graph in &self.graphs {
            if !ticks.insert(graph.tick) || self.snapshot_cut.distance_since(graph.tick) >= 32_768 {
                return Err(ApplyFailure::InvalidAction);
            }
            let mut rebuilt = crate::action_graph::TickActionGraph::default();
            for node in &graph.nodes {
                let reference = ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                };
                if actions.len() >= 8192
                    || node.action.tick != graph.tick
                    || actions.insert(reference, node.action.clone()).is_some()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let action = match &node.actor_binding {
                    None => node.action.clone(),
                    Some(binding) => {
                        proofs
                            .ok_or(ApplyFailure::InvalidAction)?
                            .binding_chunks(&node.action, binding)?;
                        binding.interpret_core(&node.action)?
                    }
                };
                rebuilt.insert(action.clone())?;
                if let Some(actor_chunk) = node.action.actor_chunk {
                    rebuilt
                        .actions
                        .get_mut(&node.action.id)
                        .ok_or(ApplyFailure::InvalidAction)?
                        .realms
                        .insert(crate::action_graph::ActionRealm::Chunk(actor_chunk));
                }
                targets.extend(
                    action
                        .targets()
                        .into_iter()
                        .chain(action.counterfactual_conditions.iter().copied())
                        .chain(action.counterfactual_effects.iter().copied()),
                );
            }
            for node in &graph.nodes {
                if rebuilt.actions[&node.action.id].realms != node.required_realms
                    || !node.applied_realms.is_subset(&node.required_realms)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                rebuilt.validate_factual_replacement(
                    node.action.id,
                    node.wrapper,
                    &node.factual,
                )?;
            }
        }
        let mut anchored = BTreeSet::new();
        for (target, anchor) in &self.anchors {
            if !anchored.insert(*target)
                || self.snapshot_cut.distance_since(anchor.settled_through) >= 32_768
                || anchor.value.as_ref().is_some_and(|cell| matches!(&cell.value, Value::Entity(body) if !body.valid_at(anchor.settled_through)))
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if anchored != targets {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut writes = BTreeMap::<
            Target,
            Vec<(
                u16,
                u32,
                crate::canonical_action::ActionId,
                crate::canonical_action::AppliedWrite,
            )>,
        >::new();
        let anchors = self
            .anchors
            .iter()
            .map(|(target, anchor)| (*target, anchor))
            .collect::<BTreeMap<_, _>>();
        for graph in &self.graphs {
            for node in &graph.nodes {
                let Some(result) = &node.factual.result else {
                    continue;
                };
                for write in &result.writes {
                    if !node.applied_realms.contains(&write.target.action_realm()) {
                        continue;
                    }
                    let anchor = anchors
                        .get(&write.target)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let distance = graph.tick.distance_since(anchor.settled_through);
                    if distance == 0 || distance >= 32_768 {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    writes.entry(write.target).or_default().push((
                        distance,
                        node.semantic_order.ok_or(ApplyFailure::InvalidAction)?,
                        node.action.id,
                        write.clone(),
                    ));
                }
            }
        }
        for (target, mut writes) in writes {
            let mut value = anchors[&target].value.clone();
            writes.sort_by_key(|(distance, order, id, _)| (*distance, *order, *id));
            for (_, _, _, write) in writes {
                if write.before != value {
                    return Err(ApplyFailure::InvalidAction);
                }
                value = write.after;
            }
        }
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .import_snapshot(&self.completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        for node in self.graphs.iter().flat_map(|graph| &graph.nodes) {
            if let Some(binding) = &node.actor_binding {
                let expected = proofs
                    .ok_or(ApplyFailure::InvalidAction)?
                    .binding_chunks(&node.action, binding)?
                    .into_iter()
                    .map(crate::canonical_action::Stakeholder::Chunk)
                    .collect::<BTreeSet<_>>();
                let keys = ledger
                    .virtual_actor_requirements_for(binding.action)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if keys
                    .iter()
                    .map(|key| key.stakeholder)
                    .collect::<BTreeSet<_>>()
                    != expected
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        Ok(())
    }

    pub fn capture(
        state: &crate::action_graph::ActionGraphs,
        completion: &RealmCompletionLedger,
        snapshot_cut: TickStamp,
        chunks: &BTreeSet<ChunkKey>,
    ) -> Option<Self> {
        let mut by_chunk = BTreeMap::<ChunkKey, Vec<ActionRef>>::new();
        let mut nodes = BTreeMap::new();
        for (tick, graph) in &state.ticks {
            for node in graph.actions.values() {
                let reference = ActionRef {
                    tick: *tick,
                    id: node.action.id,
                };
                nodes.insert(reference, node);
                for chunk in node
                    .semantic_action()
                    .stakeholders
                    .iter()
                    .filter_map(|stakeholder| match stakeholder {
                        crate::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
                        _ => None,
                    })
                    .chain(node.action.player_chunks.iter().map(|(_, chunk)| *chunk))
                    .chain(node.action.actor_chunk)
                {
                    by_chunk.entry(chunk).or_default().push(reference);
                }
            }
        }
        let mut known_chunks = chunks.clone();
        let mut chunk_queue = chunks
            .iter()
            .copied()
            .collect::<std::collections::VecDeque<_>>();
        let mut action_queue = std::collections::VecDeque::new();
        let mut selected = BTreeSet::new();
        while let Some(chunk) = chunk_queue.pop_front() {
            action_queue.extend(by_chunk.get(&chunk).into_iter().flatten().copied());
            while let Some(reference) = action_queue.pop_front() {
                if !selected.insert(reference) {
                    continue;
                }
                if selected.len() > 8192 {
                    return None;
                }
                let node = nodes.get(&reference)?;
                for chunk in node
                    .semantic_action()
                    .stakeholders
                    .iter()
                    .filter_map(|stakeholder| match stakeholder {
                        crate::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
                        _ => None,
                    })
                    .chain(node.action.player_chunks.iter().map(|(_, chunk)| *chunk))
                    .chain(node.action.actor_chunk)
                {
                    if known_chunks.insert(chunk) {
                        chunk_queue.push_back(chunk);
                    }
                }
                action_queue.extend(
                    node.action
                        .dependencies
                        .iter()
                        .filter(|dependency| nodes.contains_key(dependency))
                        .copied(),
                );
            }
        }
        let selected = state
            .ticks
            .iter()
            .filter_map(|(tick, graph)| {
                let nodes = graph
                    .actions
                    .values()
                    .filter(|node| {
                        selected.contains(&ActionRef {
                            tick: *tick,
                            id: node.action.id,
                        })
                    })
                    .collect::<Vec<_>>();
                (!nodes.is_empty()).then_some((*tick, nodes))
            })
            .collect::<Vec<_>>();
        if selected.len() > 4096 {
            return None;
        }
        let graphs = selected
            .into_iter()
            .map(|(tick, graph)| TickGraphSnapshot {
                tick,
                nodes: graph
                    .into_iter()
                    .map(|node| crate::action_graph::GraphNodeSnapshot {
                        actor_binding: node.actor_binding.clone(),
                        action: node.action.clone(),
                        factual: node.factual.clone(),
                        wrapper: node.wrapper,
                        semantic_order: node.semantic_order,
                        required_realms: node.realms.clone(),
                        applied_realms: node.applied_realms.clone(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let targets = graphs
            .iter()
            .flat_map(|graph| graph.nodes.iter())
            .flat_map(|node| {
                node.action
                    .targets()
                    .into_iter()
                    .chain(node.action.counterfactual_conditions.iter().copied())
                    .chain(node.action.counterfactual_effects.iter().copied())
                    .chain(
                        node.actor_binding
                            .iter()
                            .map(|binding| binding.resolved_source),
                    )
            })
            .collect::<BTreeSet<_>>();
        if graphs.len() > 4096 || targets.len() > 65_536 {
            return None;
        }
        let anchors = targets
            .into_iter()
            .map(|target| Some((target, state.ground_anchors.get(&target)?.clone())))
            .collect::<Option<Vec<_>>>()?;
        let references = graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().map(|node| ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                })
            })
            .collect();
        Some(Self {
            snapshot_cut,
            graphs,
            anchors,
            completion: completion.snapshot_for_actions(snapshot_cut, &references)?,
        })
    }

    pub fn validate_source(
        &self,
        chunk: ChunkKey,
        id: GlobalEntityId,
        birth: &EntityBirth,
        reference: ActionRef,
        observed: &BTreeMap<Target, Option<Cell>>,
    ) -> Result<EntityDisposition, ApplyFailure> {
        if self.graphs.len() > 4096
            || self.anchors.len() > 65_536
            || birth.uuid.is_nil()
            || birth.source.server != id.origin
            || self.snapshot_cut.distance_since(reference.tick) >= 32_768
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.births()?;
        let mut actions = BTreeMap::new();
        let mut selected = None;
        for graph in &self.graphs {
            if self.snapshot_cut.distance_since(graph.tick) >= 32_768 {
                return Err(ApplyFailure::InvalidAction);
            }
            for node in &graph.nodes {
                let at = ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                };
                if node.action.tick != graph.tick
                    || actions.len() >= 8192
                    || actions.insert(at, node.action.clone()).is_some()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                if at == reference {
                    selected = Some(node);
                }
            }
        }
        let node = selected.ok_or(ApplyFailure::InvalidAction)?;
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        let result = node
            .factual
            .result
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?;
        if node.semantic_order.is_none()
            || !node.applied_realms.contains(&target.action_realm())
            || result.id != reference.id
            || result.tick != reference.tick
            || !result.writes.iter().any(|write| {
                write.target == target
                    && (matches!(write.before.as_ref(), Some(Cell { value: Value::Entity(body), .. })
                        if body.id == id && &body.birth == birth)
                        || matches!(write.after.as_ref(), Some(Cell { value: Value::Entity(body), .. })
                            if body.id == id && &body.birth == birth && body.birth.action == reference))
            })
            || result
                .writes
                .iter()
                .flat_map(|write| [&write.before, &write.after])
                .flatten()
                .any(|cell| {
                    matches!(&cell.value, Value::Entity(body)
                    if body.id == id && &body.birth != birth)
                })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .import_snapshot(&self.completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if !ledger.action_complete(reference)
            || node
                .required_realms
                .iter()
                .any(|realm| !ledger.realm_complete(reference.tick, *realm))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut graphs = self.graphs.clone();
        for graph in &mut graphs {
            for node in &mut graph.nodes {
                node.applied_realms.retain(|realm| {
                    crate::action_graph::action_realm_held_by_chunk(&node.action, *realm, chunk)
                });
            }
        }
        let anchors = self
            .anchors
            .iter()
            .filter(|(target, _)| {
                target.action_realm().held_by_chunk(chunk)
                    || actions.values().any(|action| {
                        crate::action_graph::action_target_held_by_chunk(action, *target, chunk)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        crate::action_graph::validate_chunk_graph_snapshot(
            chunk,
            self.snapshot_cut,
            &graphs,
            &anchors,
            observed,
        )?;
        let mut destination = None;
        for write in &result.writes {
            let Some(Cell {
                value: Value::Entity(body),
                ..
            }) = &write.after
            else {
                continue;
            };
            if body.id != id {
                continue;
            }
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(written),
                inventory: None,
            } = write.target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if written != id
                || !body.belongs_to(id, key)
                || destination.replace((key, body.owner)).is_some()
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        Ok(
            destination.map_or(EntityDisposition::Removed, |(chunk, owner)| {
                EntityDisposition::Located(chunk, owner)
            }),
        )
    }

    pub fn references(&self) -> BTreeSet<ActionRef> {
        self.graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().map(|node| ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                })
            })
            .collect()
    }

    pub fn fixed_departures(
        &self,
    ) -> Result<Vec<(GlobalEntityId, crate::entity_body::EntityGroundEvent)>, ApplyFailure> {
        self.fixed_departures_with_context(None)
    }

    pub fn fixed_departures_with_context(
        &self,
        proofs: Option<&EntityBindingProofContext>,
    ) -> Result<Vec<(GlobalEntityId, crate::entity_body::EntityGroundEvent)>, ApplyFailure> {
        self.validate_with_context(proofs)?;
        let actions = self
            .graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().map(move |node| {
                    (
                        ActionRef {
                            tick: graph.tick,
                            id: node.action.id,
                        },
                        node.action.clone(),
                    )
                })
            })
            .collect();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .import_snapshot(&self.completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let mut events = Vec::new();
        for graph in &self.graphs {
            for node in &graph.nodes {
                let reference = ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                };
                if node.semantic_order.is_none()
                    || !ledger.action_complete(reference)
                    || node
                        .required_realms
                        .iter()
                        .any(|realm| !ledger.realm_complete(graph.tick, *realm))
                {
                    continue;
                }
                let Some(result) = &node.factual.result else {
                    continue;
                };
                let mut bodies =
                    BTreeMap::<GlobalEntityId, crate::entity_body::EntityGroundEvent>::new();
                for write in &result.writes {
                    let Target::Chunk {
                        key,
                        part: ChunkPart::Entity(id),
                        inventory: None,
                    } = write.target
                    else {
                        continue;
                    };
                    for (before, cell) in [(true, &write.before), (false, &write.after)] {
                        let Some(Cell {
                            value: Value::Entity(body),
                            ..
                        }) = cell
                        else {
                            continue;
                        };
                        if !body.belongs_to(id, key) {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        let event = bodies.entry(id).or_insert_with(|| {
                            crate::entity_body::EntityGroundEvent {
                                action: reference,
                                birth: body.birth.clone(),
                                before: None,
                                after: None,
                            }
                        });
                        if event.birth != body.birth {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        let slot = if before {
                            &mut event.before
                        } else {
                            &mut event.after
                        };
                        if slot.replace((key, body.owner)).is_some() {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                for (id, event) in bodies {
                    let Some((source, _)) = event.before else {
                        continue;
                    };
                    if event.before != event.after
                        && node.applied_realms.contains(
                            &Target::Chunk {
                                key: source,
                                part: ChunkPart::Entity(id),
                                inventory: None,
                            }
                            .action_realm(),
                        )
                    {
                        events.push((id, event));
                    }
                }
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::{
        Action, ActionClass, ActionId, ActionKind, ClientOrigin, Condition, Effect, Expected,
        Stakeholder, UndoProcedure,
    };
    use crate::identity::ServerId;
    use crate::protocol::{BlockPos, WorldId};
    use crate::realm_completion::PeerGeneration;

    #[test]
    fn actor_inventory_binding_moves_only_its_entity_slots() {
        use crate::canonical_inventory::{
            InventoryActionCapture, InventoryGestureReplayIntent, InventorySlotBinding,
        };
        use crate::inventory::{
            InvLoc, InventoryCell, InventoryGesture, InventorySlotRef, InventoryTarget,
        };
        let issuer = PeerGeneration {
            server: ServerId(1),
            generation: 7,
        };
        let entity = GlobalEntityId {
            origin: issuer.server,
            local_id: 8,
            generation: 2,
        };
        let captured = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let resolved = ChunkKey::new(captured.world, 1, 0);
        let body = |key| Target::Chunk {
            key,
            part: ChunkPart::Entity(entity),
            inventory: None,
        };
        let inventory = InventoryTarget::Entity(entity.into());
        let make_binding = |slot| InventorySlotBinding {
            slot: InventorySlotRef::new(inventory, InvLoc::new(0, slot)),
            target: Target::Chunk {
                key: captured,
                part: ChunkPart::Entity(entity),
                inventory: Some(crate::canonical_action::InventoryId::Entity { inv: 0, slot }),
            },
            max_stack: 64,
        };
        let first = make_binding(0);
        let second = make_binding(1);
        let id = ActionId {
            actor: ActorId::Entity(entity),
            incarnation: 2,
            sequence: 3,
            family: 2,
        };
        let birth = EntityBirth {
            source: issuer,
            uuid: uuid::Uuid::from_u128(8),
            action: ActionRef {
                tick: TickStamp(1),
                id,
            },
        };
        let owners = crate::canonical_action::OwnedStakeholderSet {
            local: BTreeMap::from([
                (
                    first.target,
                    Cell {
                        revision: 4,
                        value: Value::Inventory(InventoryCell {
                            loc: first.slot.loc,
                            item: 9,
                            count: 3,
                            nbt: Vec::new(),
                        }),
                    },
                ),
                (
                    second.target,
                    Cell {
                        revision: 4,
                        value: Value::Inventory(InventoryCell::empty(second.slot.loc)),
                    },
                ),
            ]),
            ground: BTreeMap::new(),
        };
        let intent = InventoryGestureReplayIntent {
            entity_equipment: Vec::new(),
            gestures: vec![InventoryGesture::Transfer {
                source: first.slot,
                destination: second.slot,
                count: 1,
                destination_limit: 64,
            }],
            bindings: vec![first.clone(), second.clone()],
            equipment: Vec::new(),
        };
        let action = intent
            .capture_action(
                &owners,
                InventoryActionCapture {
                    edit_authority: None,
                    id,
                    tick: TickStamp(3),
                    actor_chunk: Some(captured),
                    player_leases: Vec::new(),
                    player_chunks: Vec::new(),
                    guards: vec![Condition {
                        target: body(captured),
                        expected: Expected::ActorControlled {
                            birth: birth.clone(),
                            issuer,
                        },
                    }],
                    client_origin: ClientOrigin::None,
                },
            )
            .unwrap();
        let binding = EntityActorLocationBinding {
            action: ActionRef {
                tick: action.tick,
                id,
            },
            birth,
            issuer,
            captured_source: body(captured),
            resolved_source: body(resolved),
            transitions: vec![ActionRef {
                tick: TickStamp(2),
                id,
            }],
        };
        let interpreted = binding.interpret_core(&action).unwrap();
        let Some(crate::canonical_action::SemanticIntent::InventoryGestures(intent)) =
            &interpreted.semantic_intent
        else {
            unreachable!()
        };
        assert!(
            intent.bindings.iter().all(
                |binding| matches!(binding.target, Target::Chunk { key, .. } if key == resolved)
            )
        );
        assert!(interpreted.conditions.iter().all(
            |condition| matches!(condition.target, Target::Chunk { key, .. } if key == resolved)
        ));
        assert!(interpreted.counterfactual_effects.contains(&first.target));
        assert!(interpreted.counterfactual_effects.contains(&Target::Chunk {
            key: resolved,
            part: ChunkPart::Entity(entity),
            inventory: Some(crate::canonical_action::InventoryId::Entity { inv: 0, slot: 0 })
        }));
        assert_eq!(action.actor_chunk, Some(captured));
    }

    #[test]
    fn interpretation_moves_only_actor_relative_guard_and_preserves_core() {
        let issuer = PeerGeneration {
            server: ServerId(1),
            generation: 7,
        };
        let entity = GlobalEntityId {
            origin: issuer.server,
            local_id: 8,
            generation: 2,
        };
        let source_chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let destination_chunk = ChunkKey::new(source_chunk.world, 1, 0);
        let source = Target::Chunk {
            key: source_chunk,
            part: ChunkPart::Entity(entity),
            inventory: None,
        };
        let destination = Target::Chunk {
            key: destination_chunk,
            part: ChunkPart::Entity(entity),
            inventory: None,
        };
        let fixed = Target::Chunk {
            key: source_chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 80, z: 1 }),
            inventory: None,
        };
        let birth = EntityBirth {
            source: issuer,
            uuid: uuid::Uuid::from_u128(8),
            action: ActionRef {
                tick: TickStamp(1),
                id: ActionId {
                    actor: ActorId::Peer(issuer.server),
                    incarnation: 7,
                    sequence: 1,
                    family: 8,
                },
            },
        };
        let action = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Entity(entity),
                incarnation: 2,
                sequence: 3,
                family: 8,
            },
            tick: TickStamp(3),
            actor_chunk: Some(source_chunk),
            chunk_transition: false,
            class: ActionClass::Undoable,
            kind: ActionKind::World,
            stakeholders: vec![Stakeholder::Chunk(source_chunk)],
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            conditions: vec![
                Condition {
                    target: source,
                    expected: Expected::ActorControlled {
                        birth: birth.clone(),
                        issuer,
                    },
                },
                Condition {
                    target: fixed,
                    expected: Expected::Missing,
                },
            ],
            effects: vec![Effect::Put {
                target: fixed,
                value: Value::Block(1),
            }],
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            semantic_intent: None,
            factual_result: None,
            dependencies: Vec::new(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().unwrap();
        let original = action.clone();
        let binding = EntityActorLocationBinding {
            action: ActionRef {
                tick: action.tick,
                id: action.id,
            },
            birth,
            issuer,
            captured_source: source,
            resolved_source: destination,
            transitions: vec![ActionRef {
                tick: TickStamp(2),
                id: ActionId {
                    sequence: 2,
                    ..action.id
                },
            }],
        };
        let interpreted = binding.interpret_core(&action).unwrap();
        assert_eq!(action, original);
        assert_eq!(interpreted.actor_chunk, Some(destination_chunk));
        assert!(
            interpreted
                .conditions
                .iter()
                .any(|condition| condition.target == destination
                    && matches!(condition.expected, Expected::ActorControlled { .. }))
        );
        assert!(interpreted.conditions.contains(&original.conditions[1]));
        assert_eq!(interpreted.effects, original.effects);
        assert!(interpreted.counterfactual_conditions.contains(&source));
        assert!(interpreted.counterfactual_conditions.contains(&destination));
        let slot = Target::Chunk {
            key: source_chunk,
            part: ChunkPart::Block(BlockPos { x: 2, y: 80, z: 1 }),
            inventory: Some(crate::canonical_action::InventoryId::Block { inv: 0, slot: 0 }),
        };
        let mut pickup = action.clone();
        pickup.class = ActionClass::Replayable;
        pickup.kind = ActionKind::Item;
        pickup.conditions.truncate(1);
        pickup.conditions.push(Condition {
            target: source,
            expected: Expected::DroppedItem {
                birth: binding.birth.clone(),
                condition: crate::entity_body::DroppedItemCondition::PickupReady,
            },
        });
        pickup.conditions.push(Condition {
            target: slot,
            expected: Expected::Missing,
        });
        pickup.effects = vec![Effect::MoveItems {
            source,
            destination: slot,
            count: 1,
            max_stack: 64,
        }];
        pickup.semantic_intent = Some(crate::canonical_action::SemanticIntent::InventoryMove(
            crate::canonical_action::InventoryReplayIntent {
                source,
                item: 1,
                components: vec![1],
                count: crate::canonical_action::InventoryCountRule::All,
                ordered_destinations: vec![crate::canonical_action::InventoryReplayCandidate {
                    target: slot,
                    max_stack: 64,
                }],
                selector_revision: 0,
                destination_policy: crate::canonical_action::InventoryDestinationPolicy::Ordered,
                equipment: Vec::new(),
            },
        ));
        pickup.counterfactual_conditions = vec![source, slot];
        pickup.counterfactual_effects = vec![source, slot];
        let moved_pickup = binding.interpret_core(&pickup).unwrap();
        assert_eq!(
            moved_pickup.effects,
            vec![Effect::MoveItems {
                source: destination,
                destination: slot,
                count: 1,
                max_stack: 64,
            }]
        );
        let Some(crate::canonical_action::SemanticIntent::InventoryMove(intent)) =
            &moved_pickup.semantic_intent
        else {
            unreachable!()
        };
        assert_eq!(intent.source, destination);
        assert_eq!(intent.ordered_destinations[0].target, slot);
        assert_eq!(pickup.actor_chunk, Some(source_chunk));
        assert!(moved_pickup.counterfactual_effects.contains(&source));
        assert!(moved_pickup.counterfactual_effects.contains(&destination));
        let transition = binding.transitions[0];
        let body = Cell {
            revision: 2,
            value: Value::Entity((crate::entity_body::EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 0,
                relation_versions: Default::default(),
                passengers: Default::default(),
                id: entity,
                birth: binding.birth.clone(),
                spatial_epoch: crate::canonical_action::SpatialEpoch::Action(binding.birth.action),
                owner: issuer,
                kind: 2,
                spatial: crate::entity_body::EntitySpatial {
                    world: destination_chunk.world,
                    position: [17.0_f64.to_bits(), 80.0_f64.to_bits(), 1.0_f64.to_bits()],
                    velocity_micro: [0; 3],
                    yaw: 0,
                    pitch: 0,
                    on_ground: true,
                },
                payload: crate::entity_body::EntityPayload::Item(crate::entity_body::DroppedItem {
                    item: 1,
                    count: 1,
                    components: vec![1],
                    age: 0,
                    last_lifetime_tick: None,
                    pickup_delay: 0,
                    health_milli: 5000,
                    never_despawn: false,
                    never_pickup: false,
                    fire_immune: false,
                }),
            }).into()),
        };
        let replay_values = BTreeMap::from([
            (destination, body.clone()),
            (
                slot,
                Cell {
                    revision: 7,
                    value: Value::Inventory(crate::inventory::InventoryCell {
                        loc: crate::inventory::InvLoc { inv: 0, slot: 0 },
                        item: 0,
                        count: 0,
                        nbt: Vec::new(),
                    }),
                },
            ),
        ]);
        assert_eq!(
            intent.derive_effects(&replay_values).unwrap(),
            moved_pickup.effects
        );
        pickup.conditions.push(Condition {
            target: source,
            expected: Expected::Exact(body.clone()),
        });
        assert_eq!(
            binding.interpret_core(&pickup),
            Err(ApplyFailure::InvalidAction)
        );
        pickup.conditions.pop();
        assert_eq!(binding.interpret_core(&pickup).unwrap(), moved_pickup);
        let proofs = EntityBindingProofContext {
            events: BTreeMap::from([(
                entity,
                BTreeMap::from([(
                    transition,
                    crate::entity_body::EntityGroundEvent {
                        action: transition,
                        birth: binding.birth.clone(),
                        before: Some((source_chunk, issuer)),
                        after: Some((destination_chunk, issuer)),
                    },
                )]),
            )]),
            transitions: BTreeSet::from([(entity, transition)]),
            outcomes: BTreeMap::from([(
                transition,
                crate::canonical_action::AppliedAction {
                    id: transition.id,
                    tick: transition.tick,
                    actor_chunk: Some(source_chunk),
                    view: crate::canonical_action::TruthView::Ground,
                    writes: vec![
                        crate::canonical_action::AppliedWrite {
                            target: source,
                            before: Some(body.clone()),
                            after: None,
                        },
                        crate::canonical_action::AppliedWrite {
                            target: destination,
                            before: None,
                            after: Some(body.clone()),
                        },
                    ],
                },
            )]),
        };
        let mut pickup_graphs = crate::action_graph::ActionGraphs::default();
        for (target, value) in [
            (source, None),
            (destination, Some(body.clone())),
            (slot, Some(replay_values[&slot].clone())),
        ] {
            pickup_graphs
                .install_ground_anchor(
                    target,
                    GroundAnchor {
                        settled_through: TickStamp(2),
                        value,
                    },
                )
                .unwrap();
        }
        pickup_graphs
            .insert_collected(pickup.tick, [pickup.clone()])
            .unwrap();
        let mut pickup_completion = RealmCompletionLedger::default();
        pickup_completion
            .capture_chunk_action(
                issuer,
                &pickup,
                vec![(
                    source_chunk,
                    BTreeSet::from([issuer]),
                    BTreeSet::from([issuer]),
                )],
            )
            .unwrap();
        let pickup_keys = [source_chunk, destination_chunk]
            .into_iter()
            .map(|chunk| {
                let key = crate::realm_completion::RequirementKey::chunk(
                    chunk,
                    &BTreeSet::from([issuer]),
                );
                pickup_completion
                    .capture_with_producers(
                        pickup.tick,
                        key,
                        BTreeSet::from([issuer]),
                        BTreeSet::from([issuer]),
                    )
                    .unwrap();
                key
            })
            .collect();
        pickup_graphs
            .install_actor_binding(
                binding.clone(),
                &proofs,
                &mut pickup_completion,
                pickup_keys,
            )
            .unwrap();
        let pickup_component = BTreeSet::from([pickup.id]);
        let picked = crate::action_solver::solve_indexed_component(
            &pickup_graphs,
            pickup.tick,
            &pickup_component,
            7,
            |_| Some(true),
        )
        .unwrap();
        let crate::action_solver::SolvedVariant::Applied { result, .. } =
            &picked.variants[&pickup.id]
        else {
            panic!("bound pickup must replay");
        };
        assert!(
            result
                .writes
                .iter()
                .any(|write| write.target == destination && write.after.is_none())
        );
        assert!(result.writes.iter().any(|write| write.target == slot && matches!(&write.after,
            Some(Cell { value: Value::Inventory(item), .. }) if item.item == 1 && item.count == 1 && item.nbt == vec![1])));
        assert!(result.writes.iter().all(|write| write.target != source));
        let pickup_result = result.clone();
        crate::action_solver::install_solved_component(
            pickup_graphs.ticks.get_mut(&pickup.tick).unwrap(),
            &picked,
        )
        .unwrap();
        let graph = pickup_graphs.ticks.get_mut(&pickup.tick).unwrap();
        graph
            .mark_local_applied(pickup.id, 0, pickup_result.clone())
            .unwrap();
        graph.mark_stable(pickup.id).unwrap();
        let pickup_realms = graph.actions[&pickup.id].realms.clone();
        for realm in pickup_realms {
            pickup_graphs
                .mark_ground_realm_applied(binding.action, realm)
                .unwrap();
        }
        pickup_graphs
            .install_chunk_frontier(
                source_chunk,
                crate::action_graph::ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(2),
                },
            )
            .unwrap();
        let mut pickup_ground = BTreeMap::from([
            (source, None),
            (destination, Some(body.clone())),
            (slot, Some(replay_values[&slot].clone())),
        ]);
        for write in pickup_result.writes {
            pickup_ground.insert(write.target, write.after);
        }
        assert!(
            pickup_graphs
                .drain_grounded_tick(pickup.tick, true, &pickup_ground)
                .is_none()
        );
        pickup_graphs
            .install_chunk_frontier(
                destination_chunk,
                crate::action_graph::ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(2),
                },
            )
            .unwrap();
        assert!(
            pickup_graphs
                .drain_grounded_tick(pickup.tick, true, &pickup_ground)
                .is_some()
        );
        assert_eq!(
            pickup_graphs.chunk_frontiers[&source_chunk].settled_through,
            pickup.tick
        );
        assert_eq!(
            pickup_graphs.chunk_frontiers[&destination_chunk].settled_through,
            pickup.tick
        );
        let mut graphs = crate::action_graph::ActionGraphs::default();
        for target in [source, fixed] {
            graphs
                .install_ground_anchor(
                    target,
                    GroundAnchor {
                        settled_through: TickStamp(2),
                        value: None,
                    },
                )
                .unwrap();
        }
        graphs
            .install_ground_anchor(
                destination,
                GroundAnchor {
                    settled_through: TickStamp(2),
                    value: Some(body),
                },
            )
            .unwrap();
        graphs
            .insert_collected(action.tick, [action.clone()])
            .unwrap();
        let holders = BTreeSet::from([issuer]);
        let keys = BTreeSet::from([
            crate::realm_completion::RequirementKey::chunk(source_chunk, &holders),
            crate::realm_completion::RequirementKey::chunk(destination_chunk, &holders),
        ]);
        let mut completion = RealmCompletionLedger::default();
        completion
            .capture_chunk_action(
                issuer,
                &action,
                vec![(source_chunk, holders.clone(), holders.clone())],
            )
            .unwrap();
        let destination_key =
            crate::realm_completion::RequirementKey::chunk(destination_chunk, &holders);
        completion
            .capture_with_producers(action.tick, destination_key, holders.clone(), holders)
            .unwrap();
        graphs
            .install_actor_binding(binding.clone(), &proofs, &mut completion, keys)
            .unwrap();
        let node = &graphs.ticks[&action.tick].actions[&action.id];
        assert_eq!(node.action, original);
        assert_eq!(node.semantic_action().actor_chunk, Some(destination_chunk));
        let solved = crate::action_solver::solve_indexed_component(
            &graphs,
            action.tick,
            &BTreeSet::from([action.id]),
            7,
            |_| Some(true),
        )
        .unwrap();
        let crate::action_solver::SolvedVariant::Applied { result, .. } =
            &solved.variants[&action.id]
        else {
            panic!("bound actor action was undone");
        };
        assert_eq!(result.actor_chunk, Some(destination_chunk));
        crate::action_solver::install_solved_component(
            graphs.ticks.get_mut(&action.tick).unwrap(),
            &solved,
        )
        .unwrap();
        let snapshot = graphs.snapshot_for_chunk(destination_chunk);
        let anchors = graphs.snapshot_anchors_for_graphs(destination_chunk, &snapshot);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].nodes[0].action, original);
        let projected = crate::action_graph::project_chunk_graph_snapshot_ground_with_context(
            destination_chunk,
            action.tick,
            &snapshot,
            &anchors,
            Some((&proofs, &completion)),
        )
        .unwrap();
        assert!(
            crate::action_graph::project_chunk_graph_snapshot_ground(
                destination_chunk,
                action.tick,
                &snapshot,
                &anchors
            )
            .is_err()
        );
        let mut imported = crate::action_graph::ActionGraphs::default();
        imported
            .merge_chunk_snapshot_with_context(
                destination_chunk,
                action.tick,
                &snapshot,
                &anchors,
                &projected,
                Some((&proofs, &completion)),
            )
            .unwrap();
        assert_eq!(
            imported.ticks[&action.tick].actions[&action.id].action,
            original
        );
        assert_eq!(
            imported.ticks[&action.tick].actions[&action.id]
                .semantic_action()
                .actor_chunk,
            Some(destination_chunk)
        );
        let incomplete = RealmCompletionLedger::default();
        let mut rejected = crate::action_graph::ActionGraphs::default();
        assert!(
            rejected
                .merge_chunk_snapshot_with_context(
                    destination_chunk,
                    action.tick,
                    &snapshot,
                    &anchors,
                    &projected,
                    Some((&proofs, &incomplete))
                )
                .is_err()
        );
        assert!(rejected.ticks.is_empty());
        completion
            .note_finish(
                issuer,
                crate::realm_completion::HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([(
                        crate::realm_completion::RequirementKey::chunk(
                            source_chunk,
                            &BTreeSet::from([issuer]),
                        ),
                        BTreeSet::from([action.id]),
                    )]),
                },
            )
            .unwrap();
        let mut after = graphs.ground_anchors[&destination].value.clone().unwrap();
        after.revision = 1;
        let mut before = after.clone();
        let Value::Entity(before_body) = &mut before.value else {
            unreachable!()
        };
        before_body.spatial.position[0] = 1.0_f64.to_bits();
        let Value::Entity(after_body) = &after.value else {
            unreachable!()
        };
        let mut transition_core = action.clone();
        transition_core.id = transition.id;
        transition_core.tick = transition.tick;
        transition_core.chunk_transition = true;
        transition_core.kind = ActionKind::Actor;
        transition_core.class = ActionClass::Infallible;
        transition_core.conditions.clear();
        transition_core.stakeholders = BTreeSet::from([
            Stakeholder::Chunk(source_chunk),
            Stakeholder::Chunk(destination_chunk),
        ])
        .into_iter()
        .collect();
        transition_core.effects = vec![Effect::MoveEntityBody {
            source,
            destination,
            spatial: after_body.spatial.clone(),
            authority: None,
        }];
        transition_core.validate().unwrap();
        let mut root_graph = crate::action_graph::TickActionGraph::default();
        root_graph.insert(transition_core.clone()).unwrap();
        let required = root_graph.actions[&transition.id].realms.clone();
        let root_node = crate::action_graph::GraphNodeSnapshot {
            action: transition_core.clone(),
            actor_binding: None,
            factual: crate::action_graph::FactualVariant {
                conditions: Vec::new(),
                effects: transition_core.effects.clone(),
                result: Some(crate::canonical_action::AppliedAction {
                    id: transition.id,
                    tick: transition.tick,
                    actor_chunk: Some(source_chunk),
                    view: crate::canonical_action::TruthView::Local,
                    writes: vec![
                        crate::canonical_action::AppliedWrite {
                            target: source,
                            before: Some(before.clone()),
                            after: None,
                        },
                        crate::canonical_action::AppliedWrite {
                            target: destination,
                            before: None,
                            after: Some(after.clone()),
                        },
                    ],
                }),
            },
            wrapper: crate::action_graph::SemanticWrapper::Applied,
            semantic_order: Some(0),
            required_realms: required.clone(),
            applied_realms: required,
        };
        let mut root_ledger = RealmCompletionLedger::default();
        let root_keys = root_ledger
            .capture_chunk_action(
                issuer,
                &transition_core,
                vec![
                    (
                        source_chunk,
                        BTreeSet::from([issuer]),
                        BTreeSet::from([issuer]),
                    ),
                    (
                        destination_chunk,
                        BTreeSet::from([issuer]),
                        BTreeSet::from([issuer]),
                    ),
                ],
            )
            .unwrap();
        root_ledger
            .note_finish(
                issuer,
                crate::realm_completion::HolisticFinish {
                    full_source_slot: u64::from((transition.tick).0),
                    acquisition_prefix: None,
                    tick: transition.tick,
                    issued: root_keys
                        .into_iter()
                        .map(|key| (key, BTreeSet::from([transition.id])))
                        .collect(),
                },
            )
            .unwrap();
        let root = EntityHistoryEvidence {
            snapshot_cut: transition.tick,
            graphs: vec![crate::action_graph::TickGraphSnapshot {
                tick: transition.tick,
                nodes: vec![root_node],
            }],
            anchors: vec![
                (
                    source,
                    GroundAnchor {
                        settled_through: TickStamp(1),
                        value: Some(before),
                    },
                ),
                (
                    destination,
                    GroundAnchor {
                        settled_through: TickStamp(1),
                        value: None,
                    },
                ),
            ],
            completion: root_ledger
                .snapshot_for_actions(transition.tick, &BTreeSet::from([transition]))
                .unwrap(),
        };
        root.validate().unwrap();
        assert_eq!(
            root.fixed_dependency_outcomes(None)
                .unwrap()
                .get(&transition),
            Some(&true)
        );
        let mut partial_root = root.clone();
        partial_root.graphs[0].nodes[0]
            .applied_realms
            .remove(&destination.action_realm());
        assert!(
            !partial_root
                .fixed_dependency_outcomes(None)
                .unwrap()
                .contains_key(&transition)
        );
        let mut unfinished_root = root.clone();
        unfinished_root.completion.finishes.clear();
        assert!(
            !unfinished_root
                .fixed_dependency_outcomes(None)
                .unwrap()
                .contains_key(&transition)
        );
        let mut retained = crate::action_graph::ActionGraphs::default();
        retained
            .install_dependency_history(std::sync::Arc::new(root.clone()), None)
            .unwrap();
        assert_eq!(retained.dependency_outcome(transition), Some(true));
        assert_eq!(
            retained
                .dependency_history_for(&BTreeSet::from([transition]))
                .unwrap()
                .len(),
            1
        );
        retained.retain_dependency_roots(&BTreeSet::from([transition]));
        assert_eq!(retained.dependency_outcome(transition), Some(true));
        retained.advance_dependency_history_cut(TickStamp(transition.tick.0.wrapping_add(32768)));
        assert_eq!(retained.dependency_outcome(transition), None);
        retained.advance_dependency_history_cut(transition.tick);
        assert_eq!(retained.dependency_outcome(transition), None);
        assert!(
            retained
                .install_dependency_history(std::sync::Arc::new(root.clone()), None)
                .is_err()
        );
        retained.retain_dependency_roots(&BTreeSet::new());
        assert_eq!(retained.dependency_outcome(transition), None);
        let bound_frame = EntityHistoryEvidence {
            snapshot_cut: action.tick,
            graphs: snapshot,
            anchors: graphs
                .ground_anchors
                .iter()
                .map(|(target, anchor)| {
                    (
                        *target,
                        if *target == destination {
                            GroundAnchor {
                                settled_through: TickStamp(2),
                                value: Some(after.clone()),
                            }
                        } else {
                            anchor.clone()
                        },
                    )
                })
                .collect(),
            completion: completion
                .snapshot_for_actions(action.tick, &BTreeSet::from([binding.action]))
                .unwrap(),
        };
        assert!(EntityBindingProofContext::from_frames([&bound_frame]).is_err());
        for frames in [[&bound_frame, &root], [&root, &bound_frame]] {
            let reconstructed = EntityBindingProofContext::from_frames(frames).unwrap();
            assert_eq!(
                reconstructed.binding_candidate(&action).unwrap(),
                Some(binding.clone())
            );
        }
        let mut wrong = binding;
        wrong.action.tick = TickStamp(4);
        assert_eq!(
            wrong.interpret_core(&action),
            Err(ApplyFailure::InvalidAction)
        );
    }
}
