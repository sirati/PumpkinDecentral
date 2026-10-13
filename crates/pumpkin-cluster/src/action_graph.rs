use crate::structural_map::StructuralMap;
use std::collections::{BTreeMap, BTreeSet};

use crate::actor::ActorId;
use serde::{Deserialize, Serialize};

use crate::canonical_action::{
    Action, ActionContext, ActionId, ActionRef, ActionState, AppliedAction, AppliedWrite,
    ApplyFailure, Cell, ChunkPart, Condition, Effect, InvariantFailure, InventoryId,
    OwnedStakeholderSet, PlayerKey, Stakeholder, Target, TruthView, Value,
};
use crate::identity::GlobalPlayerId;
use crate::protocol::{ChunkKey, GlobalEntityId};
use crate::time::TickStamp;

fn derived_occupancy_write_valid(write: &AppliedWrite, effects: &[Effect]) -> bool {
    let Target::Chunk { key, part: ChunkPart::ActorOccupancy, inventory: None } = write.target else { return false; };
    let (Some(before), Some(after)) = (&write.before, &write.after) else { return false; };
    let (Value::ActorOccupancy(before_root), Value::ActorOccupancy(after_root)) = (&before.value, &after.value) else { return false; };
    let (Some(inverse), Some(forward)) = (before_root.entries.changes(), after_root.entries.changes()) else { return false; };
    if !before_root.valid() || !after_root.valid() || inverse.is_empty()
        || inverse.len() != forward.len()
        || before.revision.checked_add(1) != Some(after.revision)
        || before_root.entries.patch_base() != Some(after_root.entries.commitment())
        || after_root.entries.patch_base() != Some(before_root.entries.commitment()) {
        return false;
    }
    let declared_bodies = effects.iter().flat_map(Effect::targets)
        .filter_map(crate::derived_actor_occupancy::body_target).collect::<BTreeSet<_>>();
    forward.iter().zip(inverse).all(|(change, undo)| {
        change.actor == undo.actor && change.before == undo.after && change.after == undo.before
            && change.before != change.after
            && change.before.iter().chain(&change.after).all(|entry| {
                entry.actor == change.actor && entry.valid() && entry.covers_chunk(key)
                    && declared_bodies.contains(&entry.body_target())
            })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ActionRealm {
    Chunk(ChunkKey),
    Entity(ChunkKey, GlobalEntityId),
    PlayerAvatar(ChunkKey, GlobalPlayerId),
    Inventory(Stakeholder, InventoryRealmId),
    Player(PlayerKey),
}

impl ActionRealm {
    #[must_use]
    pub fn held_by_chunk(self, chunk: ChunkKey) -> bool {
        match self {
            Self::Chunk(key)
            | Self::Entity(key, _)
            | Self::PlayerAvatar(key, _)
            | Self::Inventory(Stakeholder::Chunk(key), _) => key == chunk,
            Self::Inventory(Stakeholder::Player(_), _) | Self::Player(_) => false,
        }
    }
}

pub fn action_realm_held_by_chunk(action: &Action, realm: ActionRealm, chunk: ChunkKey) -> bool {
    match realm {
        ActionRealm::Inventory(Stakeholder::Player(player), _) => action
            .player_chunks
            .binary_search_by_key(&player, |(key, _)| *key)
            .ok()
            .is_some_and(|index| action.player_chunks[index].1 == chunk),
        _ => realm.held_by_chunk(chunk),
    }
}

pub fn action_target_held_by_chunk(action: &Action, target: Target, chunk: ChunkKey) -> bool {
    action_realm_held_by_chunk(action, target.action_realm(), chunk)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InventoryRealmId {
    Player(u8),
    Block(u8),
    Entity(u8),
    Virtual {
        host: crate::identity::ServerId,
        kind: u16,
        session: u64,
        inv: u8,
    },
}

impl InventoryId {
    #[must_use]
    pub const fn action_realm(self) -> InventoryRealmId {
        match self {
            Self::Player { inv, .. } => InventoryRealmId::Player(inv),
            Self::Block { inv, .. } => InventoryRealmId::Block(inv),
            Self::Entity { inv, .. } => InventoryRealmId::Entity(inv),
            Self::Virtual {
                host,
                kind,
                session,
                inv,
                ..
            } => InventoryRealmId::Virtual {
                host,
                kind,
                session,
                inv,
            },
        }
    }
}

impl Target {
    #[must_use]
    pub fn action_realm(self) -> ActionRealm {
        match self {
            Self::Chunk {
                key,
                part: ChunkPart::ObjectProperty { actor, .. },
                ..
            } => ActionRealm::Entity(key, actor),
            Self::Chunk {
                key,
                part: ChunkPart::VehicleProperty { actor, .. },
                ..
            } => ActionRealm::Entity(key, actor),
            Self::Chunk {
                key,
                part:
                    ChunkPart::LivingProperty {
                        actor: ActorId::Player(id),
                        ..
                    } | ChunkPart::ActorTags { actor: ActorId::Player(id) }
                    | ChunkPart::ActorRelation { actor: ActorId::Player(id), .. },
                ..
            } => ActionRealm::PlayerAvatar(key, id),
            Self::Chunk {
                key,
                part:
                    ChunkPart::LivingProperty {
                        actor: ActorId::Entity(id),
                        ..
                    } | ChunkPart::ActorTags { actor: ActorId::Entity(id) }
                    | ChunkPart::ActorRelation { actor: ActorId::Entity(id), .. },
                ..
            } => ActionRealm::Entity(key, id),
            Self::Chunk {
                inventory: Some(id),
                ..
            }
            | Self::Player {
                inventory: Some(id),
                ..
            } => ActionRealm::Inventory(self.stakeholder(), id.action_realm()),
            Self::Chunk {
                key,
                part: ChunkPart::Entity(id),
                ..
            } => ActionRealm::Entity(key, id),
            Self::Chunk {
                key,
                part: ChunkPart::Player(id),
                ..
            } => ActionRealm::PlayerAvatar(key, id),
            Self::Chunk { key, .. } => ActionRealm::Chunk(key),
            Self::Player { key, .. } => ActionRealm::Player(key),
        }
    }
}

#[derive(Clone, Default)]
pub struct LocationMultisets {
    pub factual_conditions: LocationPostings,
    pub factual_effects: LocationPostings,
    pub counterfactual_conditions: LocationPostings,
    pub counterfactual_effects: LocationPostings,
}

pub use crate::location_postings::LocationPostings;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EdgeReason {
    FactualEffectCondition(Target),
    InfallibleAvatarOrder(Target),
    InfallibleAvatarGrouping(Target),
    InfallibleScheduledOrder(Target),
    InfallibleScheduledGrouping(Target),
    Declared(ActionRef),
}

impl LocationMultisets {
    fn is_empty(&self) -> bool {
        self.factual_conditions.is_empty()
            && self.factual_effects.is_empty()
            && self.counterfactual_conditions.is_empty()
            && self.counterfactual_effects.is_empty()
    }
}

#[derive(Clone)]
pub struct GraphAction {
    pub action: Action,
    pub actor_binding: Option<crate::entity_history::EntityActorLocationBinding>,
    interpreted_action: Option<Action>,
    pub factual: FactualVariant,
    pub wrapper: SemanticWrapper,
    pub local_order: Option<u32>,
    pub local_result: Option<AppliedAction>,
    pub local_generation: Option<u64>,
    pub semantic_generation: Option<u64>,
    pub semantic_order: Option<u32>,
    pub generation: u64,
    pub stable: bool,
    pub applied_realms: BTreeSet<ActionRealm>,
    pub realms: BTreeSet<ActionRealm>,
    pub dependents: BTreeSet<ActionId>,
    pub prerequisites: BTreeSet<ActionId>,
}

impl GraphAction {
    pub fn semantic_action(&self) -> &Action {
        self.interpreted_action.as_ref().unwrap_or(&self.action)
    }

    fn indexed_conditions(&self) -> BTreeSet<Target> {
        [&self.action, self.semantic_action()]
            .into_iter()
            .flat_map(|action| {
                action
                    .conditions
                    .iter()
                    .map(|condition| condition.target)
                    .chain(action.counterfactual_conditions.iter().copied())
            })
            .collect()
    }

    fn indexed_effects(&self) -> BTreeSet<Target> {
        [&self.action, self.semantic_action()]
            .into_iter()
            .flat_map(|action| {
                action
                    .effects
                    .iter()
                    .flat_map(Effect::targets)
                    .chain(
                        self.factual
                            .result
                            .iter()
                            .flat_map(|result| result.writes.iter().map(|write| write.target)),
                    )
                    .chain(action.counterfactual_effects.iter().copied())
            })
            .collect()
    }

    pub(crate) fn indexed_targets(&self) -> BTreeSet<Target> {
        self.indexed_conditions()
            .into_iter()
            .chain(self.indexed_effects())
            .collect()
    }

    #[must_use]
    pub fn is_ground_applied(&self) -> bool {
        self.applied_realms == self.realms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactualVariant {
    pub conditions: Vec<Condition>,
    pub effects: Vec<Effect>,
    pub result: Option<AppliedAction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SemanticWrapper {
    Applied,
    Undone,
    Redone,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNodeSnapshot {
    pub action: Action,
    pub actor_binding: Option<crate::entity_history::EntityActorLocationBinding>,
    pub factual: FactualVariant,
    pub wrapper: SemanticWrapper,
    pub semantic_order: Option<u32>,
    pub required_realms: BTreeSet<ActionRealm>,
    pub applied_realms: BTreeSet<ActionRealm>,
}

impl GraphNodeSnapshot {
    fn indexed_targets(&self) -> BTreeSet<Target> {
        self.action
            .targets()
            .into_iter()
            .chain(self.action.counterfactual_conditions.iter().copied())
            .chain(self.action.counterfactual_effects.iter().copied())
            .chain(
                self.actor_binding
                    .iter()
                    .map(|binding| binding.resolved_source),
            )
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickGraphSnapshot {
    pub tick: TickStamp,
    pub nodes: Vec<GraphNodeSnapshot>,
}

pub fn project_chunk_graph_snapshot_ground(
    chunk: ChunkKey,
    cut: TickStamp,
    graphs: &[TickGraphSnapshot],
    anchors: &[(Target, GroundAnchor)],
) -> Result<BTreeMap<Target, Option<Cell>>, ApplyFailure> {
    project_chunk_graph_snapshot_ground_with_context(chunk, cut, graphs, anchors, None)
}

pub fn project_chunk_graph_snapshot_ground_with_context(
    chunk: ChunkKey,
    cut: TickStamp,
    graphs: &[TickGraphSnapshot],
    anchors: &[(Target, GroundAnchor)],
    proof_context: Option<(
        &crate::entity_history::EntityBindingProofContext,
        &crate::realm_completion::RealmCompletionLedger,
    )>,
) -> Result<BTreeMap<Target, Option<Cell>>, ApplyFailure> {
    project_chunk_graph_snapshot_with_context(chunk, cut, graphs, anchors, proof_context, false)
}

pub fn project_chunk_graph_snapshot_local_with_context(
    chunk: ChunkKey,
    cut: TickStamp,
    graphs: &[TickGraphSnapshot],
    anchors: &[(Target, GroundAnchor)],
    proof_context: Option<(
        &crate::entity_history::EntityBindingProofContext,
        &crate::realm_completion::RealmCompletionLedger,
    )>,
) -> Result<BTreeMap<Target, Option<Cell>>, ApplyFailure> {
    project_chunk_graph_snapshot_with_context(chunk, cut, graphs, anchors, proof_context, true)
}

fn project_chunk_graph_snapshot_with_context(
    chunk: ChunkKey,
    cut: TickStamp,
    graphs: &[TickGraphSnapshot],
    anchors: &[(Target, GroundAnchor)],
    proof_context: Option<(
        &crate::entity_history::EntityBindingProofContext,
        &crate::realm_completion::RealmCompletionLedger,
    )>,
    local: bool,
) -> Result<BTreeMap<Target, Option<Cell>>, ApplyFailure> {
    let mut anchors_by_target = BTreeMap::new();
    for (target, anchor) in anchors {
        if matches!(target, Target::Chunk { part: ChunkPart::ActorOccupancy, inventory: None, .. })
            && !matches!(&anchor.value, Some(Cell { value: Value::ActorOccupancy(root), .. }) if root.valid() && root.is_complete()) {
            return Err(ApplyFailure::Type(*target));
        }
        if matches!(
            target,
            Target::Chunk {
                part: ChunkPart::BlockSection(_),
                inventory: None,
                ..
            }
        ) && anchor
            .value
            .as_ref()
            .is_none_or(|cell| crate::block_section::project_writes(*target, cell, &[]).is_err())
        {
            return Err(ApplyFailure::Type(*target));
        }
        if matches!(
            target,
            Target::Chunk {
                part: ChunkPart::LightSection(_),
                inventory: None,
                ..
            }
        ) && !matches!(anchor.value.as_ref(), Some(Cell { value: Value::LightSection { sky, block }, .. }) if sky.len() == 2_048 && block.len() == 2_048)
        {
            return Err(ApplyFailure::Type(*target));
        }
        if anchors_by_target.insert(*target, anchor).is_some()
            || cut.distance_since(anchor.settled_through) >= 32_768
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let mut seen_ticks = BTreeSet::new();
    let mut required_targets = BTreeSet::new();
    let mut writes = BTreeMap::<Target, Vec<(u16, u32, ActionId, AppliedWrite)>>::new();
    let mut section_writes =
        BTreeMap::<Target, BTreeMap<(u16, u32, ActionId), Vec<AppliedWrite>>>::new();
    let mut living_aliases = BTreeMap::<Target, Vec<Target>>::new();
    for target in anchors_by_target.keys() {
        if let Some(body) = crate::derived_living::actor_body_target(*target) {
            living_aliases.entry(body).or_default().push(*target);
        } else if matches!(
            target,
            Target::Chunk {
                part: ChunkPart::Player(_) | ChunkPart::Entity(_),
                inventory: None,
                ..
            }
        ) {
            living_aliases.entry(*target).or_default().push(*target);
        }
    }
    let mut living_writes =
        BTreeMap::<Target, BTreeMap<(u16, u32, ActionId), Vec<AppliedWrite>>>::new();
    let mut interpreted = BTreeMap::new();
    for graph in graphs {
        if !seen_ticks.insert(graph.tick) || cut.distance_since(graph.tick) >= 32_768 {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut rebuilt = TickActionGraph::default();
        for node in &graph.nodes {
            if node.action.tick != graph.tick
                || !node.applied_realms.is_subset(&node.required_realms)
                || node.action.validate().is_err()
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let action = match &node.actor_binding {
                None => {
                    if !node.applied_realms.is_empty()
                        && proof_context.is_some_and(|(proofs, _)| {
                            proofs
                                .binding_candidate(&node.action)
                                .is_ok_and(|binding| binding.is_some())
                        })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    node.action.clone()
                }
                Some(binding) => {
                    let (proofs, ledger) = proof_context.ok_or(ApplyFailure::InvalidAction)?;
                    let required = proofs
                        .binding_chunks(&node.action, binding)?
                        .into_iter()
                        .map(Stakeholder::Chunk)
                        .collect::<BTreeSet<_>>();
                    let keys = ledger
                        .virtual_actor_requirements_for(binding.action)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    if keys
                        .iter()
                        .map(|key| key.stakeholder)
                        .collect::<BTreeSet<_>>()
                        != required
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if !node.applied_realms.is_empty()
                        && (!ledger.action_complete(binding.action)
                            || node
                                .required_realms
                                .iter()
                                .any(|realm| !ledger.realm_complete(binding.action.tick, *realm)))
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    binding.interpret_core(&node.action)?
                }
            };
            required_targets.extend(
                action
                    .targets()
                    .into_iter()
                    .chain(action.counterfactual_conditions.iter().copied())
                    .chain(action.counterfactual_effects.iter().copied())
                    .chain(action.effects.iter().flat_map(Effect::targets)
                        .filter_map(crate::derived_actor_occupancy::root_alias))
                    .filter(|target| action_target_held_by_chunk(&action, *target, chunk)),
            );
            rebuilt.insert(action.clone())?;
            if let Some(actor_chunk) = node.action.actor_chunk {
                rebuilt
                    .actions
                    .get_mut(&node.action.id)
                    .ok_or(ApplyFailure::InvalidAction)?
                    .realms
                    .insert(ActionRealm::Chunk(actor_chunk));
            }
            interpreted.insert(
                ActionRef {
                    tick: graph.tick,
                    id: action.id,
                },
                action,
            );
        }
        for node in &graph.nodes {
            let action = &interpreted[&ActionRef {
                tick: graph.tick,
                id: node.action.id,
            }];
            rebuilt.validate_factual_replacement(node.action.id, node.wrapper, &node.factual)?;
            if rebuilt.actions[&node.action.id].realms != node.required_realms
                || node
                    .applied_realms
                    .iter()
                    .any(|realm| !action_realm_held_by_chunk(action, *realm, chunk))
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let realms = if local { &node.required_realms } else { &node.applied_realms };
            for realm in realms.iter().filter(|realm| action_realm_held_by_chunk(action, **realm, chunk)) {
                let Some(result) = &node.factual.result else {
                    continue;
                };
                if result.id != node.action.id || result.tick != graph.tick {
                    return Err(ApplyFailure::InvalidAction);
                }
                for write in &result.writes {
                    if write.target.action_realm() != *realm {
                        continue;
                    }
                    let body = crate::derived_living::actor_body_target(write.target)
                        .unwrap_or(write.target);
                    for alias in living_aliases.get(&body).into_iter().flatten() {
                        let anchor = anchors_by_target[alias];
                        let distance = graph.tick.distance_since(anchor.settled_through);
                        if distance == 0 || distance >= 32_768 {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        living_writes
                            .entry(*alias)
                            .or_default()
                            .entry((
                                distance,
                                node.semantic_order.ok_or(ApplyFailure::InvalidAction)?,
                                node.action.id,
                            ))
                            .or_default()
                            .push(write.clone());
                    }
                    for section in effect_location_aliases(write.target).skip(1) {
                        if let Some(anchor) = anchors_by_target.get(&section) {
                            let distance = graph.tick.distance_since(anchor.settled_through);
                            if distance == 0 || distance >= 32_768 {
                                return Err(ApplyFailure::InvalidAction);
                            }
                            section_writes
                                .entry(section)
                                .or_default()
                                .entry((
                                    distance,
                                    node.semantic_order.ok_or(ApplyFailure::InvalidAction)?,
                                    node.action.id,
                                ))
                                .or_default()
                                .push(write.clone());
                        }
                    }
                    let anchor = anchors_by_target
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
    }
    let anchor_targets = anchors_by_target.keys().copied().collect::<BTreeSet<_>>();
    if !required_targets.is_subset(&anchor_targets)
        || anchor_targets.difference(&required_targets).any(|target| {
            !matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::Whole,
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::Block(_),
                    ..
                } | Target::Chunk {
                    part: ChunkPart::BlockSection(_),
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::LightSection(_),
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::BlockEntity(_),
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::Player(_),
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::Entity(_),
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::LivingProperty { .. },
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::VehicleProperty { .. },
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::ObjectProperty { .. },
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::ActorOccupancy,
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::ActorTags { .. },
                    inventory: None,
                    ..
                } | Target::Chunk {
                    part: ChunkPart::ActorRelation { .. },
                    inventory: None,
                    ..
                } | Target::Chunk {
                    inventory: Some(_),
                    ..
                } | Target::Player {
                    inventory: Some(_),
                    ..
                }
            )
        })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    for graph in graphs {
        for node in &graph.nodes {
            let action = &interpreted[&ActionRef {
                tick: graph.tick,
                id: node.action.id,
            }];
            for target in action
                .targets()
                .into_iter()
                .chain(action.counterfactual_conditions.iter().copied())
                .chain(action.counterfactual_effects.iter().copied())
                .filter(|target| action_target_held_by_chunk(action, *target, chunk))
            {
                let anchor = anchors_by_target
                    .get(&target)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let distance = graph.tick.distance_since(anchor.settled_through);
                if distance == 0 || distance >= 32_768 {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
    }
    let mut projected = BTreeMap::new();
    for (target, anchor) in anchors_by_target {
        let mut value = anchor.value.clone();
        if let Some(actions) = living_writes.remove(&target) {
            for (_, writes) in actions {
                value = if crate::derived_living::actor_body_target(target).is_some() {
                    crate::derived_living::project_facet_writes(target, value.as_ref(), &writes)?
                } else {
                    crate::derived_living::project_body_writes(target, value.as_ref(), &writes)?
                };
            }
            projected.insert(target, value);
            continue;
        }
        if matches!(
            target,
            Target::Chunk {
                part: ChunkPart::BlockSection(_),
                inventory: None,
                ..
            }
        ) {
            for (_, writes) in section_writes.remove(&target).unwrap_or_default() {
                value = Some(crate::block_section::project_writes(
                    target,
                    value.as_ref().ok_or(ApplyFailure::Type(target))?,
                    &writes,
                )?);
            }
            projected.insert(target, value);
            continue;
        }
        let mut target_writes = writes.remove(&target).unwrap_or_default();
        target_writes.sort_by_key(|(distance, order, id, _)| (*distance, *order, *id));
        for (_, _, _, write) in target_writes {
            if write.before != value {
                return Err(ApplyFailure::InvalidAction);
            }
            value = if let Some(Cell { revision, value: Value::ActorOccupancy(after) }) = &write.after {
                let Some(Cell { value: Value::ActorOccupancy(prior), .. }) = &value else {
                    return Err(ApplyFailure::Type(target));
                };
                let resolved = after.resolve_against(prior).ok_or(ApplyFailure::Type(target))?;
                let Some(Cell { value: Value::ActorOccupancy(before), .. }) = &write.before else {
                    return Err(ApplyFailure::Type(target));
                };
                if before.resolve_against(&resolved).as_ref() != Some(prior) {
                    return Err(ApplyFailure::InvalidAction);
                }
                Some(Cell { revision: *revision, value: Value::ActorOccupancy(resolved) })
            } else { write.after };
        }
        projected.insert(target, value);
    }
    Ok(projected)
}

pub fn validate_chunk_graph_snapshot(
    chunk: ChunkKey,
    cut: TickStamp,
    graphs: &[TickGraphSnapshot],
    anchors: &[(Target, GroundAnchor)],
    observed_ground: &BTreeMap<Target, Option<Cell>>,
) -> Result<(), ApplyFailure> {
    let projected = project_chunk_graph_snapshot_ground(chunk, cut, graphs, anchors)?;
    (projected == *observed_ground)
        .then_some(())
        .ok_or(ApplyFailure::InvalidAction)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionImpact {
    Satisfied,
    Violated,
    NoWrite,
    Unknown,
}

#[derive(Clone)]
pub struct SemanticDelta {
    pub reference: ActionRef,
    pub previous_order: Option<u32>,
    pub next_order: Option<u32>,
    pub core: Action,
    pub previous: Option<AppliedAction>,
    pub next: Option<AppliedAction>,
    pub next_conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaFailure {
    Missing(Target),
    Invariant(InvariantFailure),
    StaleReplay(Target),
    Commit,
}

pub struct StagedLocalDeltas {
    pub writes: Vec<AppliedWrite>,
    pub applied: BTreeMap<ActionRef, AppliedAction>,
    pub steps: Vec<LocalDeltaStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalDeltaDirection {
    Undo,
    Apply,
}

pub struct LocalDeltaStep {
    pub reference: ActionRef,
    pub direction: LocalDeltaDirection,
    pub writes: Vec<AppliedWrite>,
}

pub fn stage_backward_local_deltas<S: ActionState>(
    owners: &S,
    current_tick: TickStamp,
    deltas: &[SemanticDelta],
) -> Result<StagedLocalDeltas, DeltaFailure> {
    if deltas.iter().any(|delta| {
        current_tick.distance_since(delta.reference.tick) >= 32_768
            || delta.previous.is_some() != delta.previous_order.is_some()
            || delta.next.is_some() != delta.next_order.is_some()
            || delta.previous.iter().chain(&delta.next).any(|applied| {
                applied.id != delta.reference.id
                    || applied.tick != delta.reference.tick
                    || applied.view != TruthView::Local
                    || applied
                        .writes
                        .iter()
                        .map(|write| write.target)
                        .collect::<BTreeSet<_>>()
                        .len()
                        != applied.writes.len()
            })
    }) {
        return Err(DeltaFailure::Commit);
    }
    let mut targets = deltas
        .iter()
        .flat_map(|delta| {
            delta
                .previous
                .iter()
                .chain(&delta.next)
                .flat_map(|applied| applied.writes.iter().map(|write| write.target))
                .chain(
                    delta
                        .next_conditions
                        .iter()
                        .map(|condition| condition.target),
                )
        })
        .collect::<BTreeSet<_>>();
    let sections = targets
        .iter()
        .copied()
        .flat_map(effect_location_aliases)
        .filter(|target| {
            matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::BlockSection(_),
                    inventory: None,
                    ..
                }
            ) && owners.available(TruthView::Local, *target)
        })
        .collect::<BTreeSet<_>>();
    targets.extend(sections);
    let bodies = targets
        .iter()
        .filter_map(|target| crate::derived_living::actor_body_target(*target))
        .collect::<BTreeSet<_>>();
    targets.extend(bodies);
    let mut mirror = OwnedStakeholderSet::default();
    let mut before = BTreeMap::new();
    for target in &targets {
        if !owners.available(TruthView::Local, *target) {
            return Err(DeltaFailure::Missing(*target));
        }
        let cell = owners.read(TruthView::Local, *target);
        before.insert(*target, cell.clone());
        if let Some(cell) = cell {
            mirror.local.insert(*target, cell);
        }
    }
    for delta in deltas {
        if delta.reference.tick != delta.core.tick || delta.reference.id != delta.core.id {
            return Err(DeltaFailure::Commit);
        }
    }
    let mut ticks = deltas
        .iter()
        .map(|delta| delta.reference.tick)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ticks.sort_by_key(|tick| current_tick.distance_since(*tick));
    let mut steps = Vec::new();
    for tick in &ticks {
        let mut previous = deltas
            .iter()
            .filter(|delta| delta.reference.tick == *tick && delta.previous.is_some())
            .collect::<Vec<_>>();
        previous.sort_by_key(|delta| {
            (
                std::cmp::Reverse(delta.previous_order.unwrap_or_default()),
                std::cmp::Reverse(delta.reference.id),
            )
        });
        for delta in previous {
            let previous = delta.previous.as_ref().ok_or(DeltaFailure::Commit)?;
            let undo =
                crate::canonical_action::prepare_semantic_undo(&mirror, &delta.core, previous)
                    .map_err(DeltaFailure::Invariant)?;
            let section_updates = mirror
                .local
                .iter()
                .filter_map(|(target, cell)| {
                    matches!(
                        target,
                        Target::Chunk {
                            part: ChunkPart::BlockSection(_),
                            inventory: None,
                            ..
                        }
                    )
                    .then(|| {
                        crate::block_section::project_undo(*target, cell, &undo)
                            .map(|cell| (*target, cell))
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DeltaFailure::Commit)?;
            let living_updates = mirror
                .local
                .iter()
                .filter_map(|(target, cell)| {
                    if crate::derived_living::actor_body_target(*target).is_some() {
                        Some(
                            crate::derived_living::project_facet_writes(*target, Some(cell), &undo)
                                .map(|value| (*target, value)),
                        )
                    } else if matches!(&cell.value, Value::Living(_) | Value::Entity(_)) {
                        Some(
                            crate::derived_living::project_body_undo(
                                *target,
                                Some(cell),
                                &previous.writes,
                            )
                            .map(|value| (*target, value)),
                        )
                    } else {
                        None
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DeltaFailure::Commit)?;
            mirror
                .commit(
                    ActionContext {
                        id: delta.core.id,
                        tick: delta.core.tick,
                        actor_chunk: delta.core.actor_chunk,
                    },
                    TruthView::Local,
                    &undo,
                )
                .map_err(|_| DeltaFailure::Commit)?;
            mirror.local.extend(section_updates);
            for (target, cell) in living_updates {
                if let Some(cell) = cell {
                    mirror.local.insert(target, cell);
                } else {
                    mirror.local.remove(&target);
                }
            }
            steps.push(LocalDeltaStep {
                reference: delta.reference,
                direction: LocalDeltaDirection::Undo,
                writes: undo,
            });
        }
    }
    let mut applied = BTreeMap::new();
    for tick in ticks.into_iter().rev() {
        let mut next = deltas
            .iter()
            .filter(|delta| delta.reference.tick == tick && delta.next.is_some())
            .collect::<Vec<_>>();
        next.sort_by_key(|delta| (delta.next_order.unwrap_or_default(), delta.reference.id));
        for delta in next {
            let next = delta.next.as_ref().ok_or(DeltaFailure::Commit)?;
            if next.id != delta.reference.id
                || next.tick != delta.reference.tick
                || next.view != TruthView::Local
            {
                return Err(DeltaFailure::Commit);
            }
            for condition in &delta.next_conditions {
                if !owners.available(TruthView::Local, condition.target) {
                    return Err(DeltaFailure::Missing(condition.target));
                }
                if !condition
                    .expected
                    .matches(mirror.read(TruthView::Local, condition.target).as_ref())
                {
                    return Err(DeltaFailure::StaleReplay(condition.target));
                }
            }
            let writes = next
                .writes
                .iter()
                .map(|write| {
                    let current = mirror.read(TruthView::Local, write.target);
                    if current.as_ref().map(|cell| &cell.value)
                        != write.before.as_ref().map(|cell| &cell.value)
                    {
                        return Err(DeltaFailure::StaleReplay(write.target));
                    }
                    let after = match (&current, &write.after) {
                        (Some(current), Some(after)) => Some(Cell {
                            revision: current
                                .revision
                                .checked_add(1)
                                .ok_or(DeltaFailure::StaleReplay(write.target))?,
                            value: after.value.clone(),
                        }),
                        (None, Some(after)) => Some(Cell {
                            revision: 0,
                            value: after.value.clone(),
                        }),
                        (_, None) => None,
                    };
                    let mut current = current;
                    if matches!(write.target, Target::Chunk { part: ChunkPart::ActorOccupancy, inventory: None, .. }) {
                        let value = write.before.as_ref().ok_or(DeltaFailure::StaleReplay(write.target))?.value.clone();
                        current.as_mut().ok_or(DeltaFailure::StaleReplay(write.target))?.value = value;
                    }
                    Ok(AppliedWrite {
                        target: write.target,
                        before: current,
                        after,
                    })
                })
                .collect::<Result<Vec<_>, DeltaFailure>>()?;
            let section_updates = mirror
                .local
                .iter()
                .filter_map(|(target, cell)| {
                    matches!(
                        target,
                        Target::Chunk {
                            part: ChunkPart::BlockSection(_),
                            inventory: None,
                            ..
                        }
                    )
                    .then(|| {
                        crate::block_section::project_writes(*target, cell, &writes)
                            .map(|cell| (*target, cell))
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DeltaFailure::Commit)?;
            let living_updates = mirror
                .local
                .iter()
                .filter_map(|(target, cell)| {
                    if crate::derived_living::actor_body_target(*target).is_some() {
                        Some(
                            crate::derived_living::project_facet_writes(
                                *target,
                                Some(cell),
                                &writes,
                            )
                            .map(|value| (*target, value)),
                        )
                    } else if matches!(&cell.value, Value::Living(_) | Value::Entity(_)) {
                        Some(
                            crate::derived_living::project_body_writes(
                                *target,
                                Some(cell),
                                &writes,
                            )
                            .map(|value| (*target, value)),
                        )
                    } else {
                        None
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DeltaFailure::Commit)?;
            mirror
                .commit(
                    ActionContext {
                        id: delta.core.id,
                        tick: delta.core.tick,
                        actor_chunk: delta.core.actor_chunk,
                    },
                    TruthView::Local,
                    &writes,
                )
                .map_err(|_| DeltaFailure::Commit)?;
            mirror.local.extend(section_updates);
            for (target, cell) in living_updates {
                if let Some(cell) = cell {
                    mirror.local.insert(target, cell);
                } else {
                    mirror.local.remove(&target);
                }
            }
            steps.push(LocalDeltaStep {
                reference: delta.reference,
                direction: LocalDeltaDirection::Apply,
                writes: writes.clone(),
            });
            applied.insert(
                delta.reference,
                AppliedAction {
                    id: next.id,
                    tick: next.tick,
                    actor_chunk: next.actor_chunk,
                    view: TruthView::Local,
                    writes,
                },
            );
        }
    }
    let writes = targets
        .into_iter()
        .filter_map(|target| {
            let prior = before.get(&target).cloned().flatten();
            let after = mirror.read(TruthView::Local, target);
            (prior != after).then_some(AppliedWrite {
                target,
                before: prior,
                after,
            })
        })
        .map(|write| crate::actor_occupancy::compact_write(write).ok_or(DeltaFailure::Commit))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StagedLocalDeltas {
        writes,
        applied,
        steps,
    })
}

#[derive(Clone, Default)]
pub struct TickActionGraph {
    pub actions: StructuralMap<ActionId, GraphAction>,
    pub realms: StructuralMap<ActionRealm, LocationMultisets>,
    pub edge_reasons: StructuralMap<(ActionId, ActionId), StructuralMap<EdgeReason, u16>>,
    declared_dependents: StructuralMap<ActionId, BTreeSet<ActionId>>,
    grounded_targets: LocationPostings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroundReadyPlan {
    pub components: Vec<Vec<ActionRef>>,
    pub unapplied_realms: BTreeMap<ActionRef, BTreeSet<ActionRealm>>,
    pub semantic_generations: BTreeMap<ActionRef, u64>,
}

impl GroundReadyPlan {
    pub fn writes_for_realm(
        &self,
        graph: &TickActionGraph,
        realm: ActionRealm,
    ) -> Result<Vec<(ActionRef, Vec<AppliedWrite>)>, ApplyFailure> {
        self.components
            .iter()
            .flatten()
            .filter(|reference| {
                self.unapplied_realms
                    .get(reference)
                    .is_some_and(|realms| realms.contains(&realm))
            })
            .map(|reference| {
                let node = graph
                    .actions
                    .get(&reference.id)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if node.action.tick != reference.tick
                    || !node.stable
                    || self.semantic_generations.get(reference) != node.semantic_generation.as_ref()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let writes = node
                    .factual
                    .result
                    .as_ref()
                    .map(|result| {
                        result
                            .writes
                            .iter()
                            .filter(|write| write.target.action_realm() == realm)
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                Ok((*reference, writes))
            })
            .collect()
    }
}

impl TickActionGraph {
    fn index_grounded_action(&mut self, id: ActionId) {
        if let Some(node) = self.actions.get(&id)
            && node.is_ground_applied()
        {
            for target in node.indexed_targets() {
                self.grounded_targets.insert_reference(target, id);
            }
        }
    }

    fn rebuild_grounded_targets(&mut self) {
        self.grounded_targets.clear();
        for id in self.actions.keys().copied().collect::<Vec<_>>() {
            self.index_grounded_action(id);
        }
    }
    pub fn invalidate_resolution_context(&mut self) -> BTreeSet<ActionId> {
        let ids = self.actions.iter().filter_map(|(id, node)| {
            (!node.is_ground_applied()).then_some(*id)
        }).collect::<BTreeSet<_>>();
        for id in &ids {
            let node = self.actions.get_mut(id).unwrap();
            node.stable = false;
            node.semantic_generation = None;
        }
        ids
    }

    fn invalidate_affected(&mut self, seeds: &BTreeSet<ActionId>) {
        for id in self
            .affected_components(seeds)
            .into_iter()
            .flat_map(|component| component.into_iter())
        {
            if let Some(node) = self.actions.get_mut(&id) {
                node.stable = false;
            }
        }
    }

    pub fn condition_impact(
        &self,
        producer: ActionId,
        consumer: ActionId,
        target: Target,
    ) -> Option<ConditionImpact> {
        let producer = self.actions.get(&producer)?;
        let conditions = self
            .actions
            .get(&consumer)?
            .factual
            .conditions
            .iter()
            .filter(|condition| condition.target == target)
            .collect::<Vec<_>>();
        if conditions.is_empty() {
            return None;
        }
        if !producer
            .factual
            .effects
            .iter()
            .any(|effect| effect.targets().any(|affected| affected == target))
        {
            return None;
        }
        Some(match producer.factual.result.as_ref() {
            Some(result) => match result.writes.iter().find(|write| write.target == target) {
                Some(write)
                    if conditions
                        .iter()
                        .all(|condition| condition.expected.matches(write.after.as_ref())) =>
                {
                    ConditionImpact::Satisfied
                }
                Some(_) => ConditionImpact::Violated,
                None => ConditionImpact::NoWrite,
            },
            None => ConditionImpact::Unknown,
        })
    }

    pub fn mark_stable(&mut self, id: ActionId) -> Result<(), ApplyFailure> {
        let node = self
            .actions
            .get_mut(&id)
            .ok_or(ApplyFailure::InvalidAction)?;
        let consistent = match node.wrapper {
            SemanticWrapper::Undone => {
                node.factual.conditions.is_empty()
                    && node.factual.effects.is_empty()
                    && node.factual.result.is_none()
            }
            SemanticWrapper::Applied | SemanticWrapper::Redone | SemanticWrapper::Replayed => {
                node.factual.result.as_ref().is_some_and(|result| {
                    result.id == id
                        && result.tick == node.action.tick
                        && result.view == TruthView::Local
                        && result.actor_chunk == node.semantic_action().actor_chunk
                })
            }
        };
        let local_complete = node.local_generation == Some(node.generation)
            && match node.wrapper {
                SemanticWrapper::Undone => {
                    node.local_order.is_none() && node.local_result.is_none()
                }
                _ => node.local_order.is_some() && node.local_result.is_some(),
            };
        let semantic_complete = node.semantic_generation == Some(node.generation)
            && match node.wrapper {
                SemanticWrapper::Undone => node.semantic_order.is_none(),
                _ => node.semantic_order.is_some(),
            };
        if !consistent || !local_complete || !semantic_complete {
            return Err(ApplyFailure::InvalidAction);
        }
        node.stable = true;
        Ok(())
    }

    pub fn mark_local_applied(
        &mut self,
        id: ActionId,
        order: u32,
        result: AppliedAction,
    ) -> Result<(), ApplyFailure> {
        let node = self
            .actions
            .get_mut(&id)
            .ok_or(ApplyFailure::InvalidAction)?;
        if result.id != id
            || result.tick != node.action.tick
            || result.view != TruthView::Local
            || result.actor_chunk != node.semantic_action().actor_chunk
        {
            return Err(ApplyFailure::InvalidAction);
        }
        node.local_order = Some(order);
        node.local_result = Some(result);
        node.local_generation = Some(node.generation);
        Ok(())
    }

    pub fn mark_local_undone(&mut self, id: ActionId) -> Result<(), ApplyFailure> {
        let node = self
            .actions
            .get_mut(&id)
            .ok_or(ApplyFailure::InvalidAction)?;
        node.local_order = None;
        node.local_result = None;
        node.local_generation = Some(node.generation);
        Ok(())
    }

    #[must_use]
    pub fn ground_ready_plan(
        &self,
        tick: TickStamp,
        realm_complete: impl Fn(ActionRealm) -> bool,
        committed: impl Fn(ActionRef) -> bool,
    ) -> GroundReadyPlan {
        let components = self.affected_components(&self.actions.keys().copied().collect());
        let mut eligible = components
            .iter()
            .enumerate()
            .filter_map(|(index, component)| {
                (component
                    .iter()
                    .any(|id| !self.actions[id].is_ground_applied())
                    && component.iter().all(|id| {
                        self.actions.get(id).is_some_and(|node| {
                            node.stable
                                && (node.is_ground_applied()
                                    || node.realms.iter().all(|realm| realm_complete(*realm)))
                        })
                    }))
                .then_some(index)
            })
            .collect::<BTreeSet<_>>();
        let component_for = components
            .iter()
            .enumerate()
            .flat_map(|(index, component)| component.iter().map(move |id| (*id, index)))
            .collect::<BTreeMap<_, _>>();
        loop {
            let before = eligible.len();
            let currently_eligible = eligible.clone();
            eligible.retain(|index| {
                components[*index].iter().all(|id| {
                    let node = &self.actions[id];
                    let prerequisites = node
                        .prerequisites
                        .iter()
                        .copied()
                        .map(|id| ActionRef { tick, id })
                        .chain(node.action.dependencies.iter().copied());
                    let predecessors_ready = prerequisites.into_iter().all(|dependency| {
                        committed(dependency)
                            || dependency.tick == tick
                                && self
                                    .actions
                                    .get(&dependency.id)
                                    .is_some_and(GraphAction::is_ground_applied)
                            || dependency.tick == tick
                                && component_for
                                    .get(&dependency.id)
                                    .is_some_and(|component| currently_eligible.contains(component))
                    });
                    let counterfactuals_safe =
                        self.counterfactual_blockers(*id)
                            .into_iter()
                            .all(|blocker| {
                                let Some(other) = self.actions.get(&blocker) else {
                                    return false;
                                };
                                other.is_ground_applied()
                                    || other.stable
                                        && other.realms.iter().all(|realm| realm_complete(*realm))
                                        && other
                                            .prerequisites
                                            .iter()
                                            .copied()
                                            .map(|id| ActionRef { tick, id })
                                            .chain(other.action.dependencies.iter().copied())
                                            .all(|dependency| {
                                                committed(dependency)
                                                    || dependency.tick == tick
                                                        && self
                                                            .actions
                                                            .get(&dependency.id)
                                                            .is_some_and(|prior| {
                                                                prior.is_ground_applied()
                                                            })
                                                    || dependency.tick == tick
                                                        && component_for
                                                            .get(&dependency.id)
                                                            .is_some_and(|component| {
                                                                currently_eligible
                                                                    .contains(component)
                                                            })
                                            })
                            });
                    predecessors_ready && counterfactuals_safe
                })
            });
            if eligible.len() == before {
                break;
            }
        }
        let all_ordinary_eligible = components.iter().enumerate().all(|(index, component)| {
            component
                .iter()
                .all(|id| self.actions[id].is_ground_applied())
                || component
                    .iter()
                    .all(|id| self.actions[id].action.chunk_transition)
                || eligible.contains(&index)
        });
        if !all_ordinary_eligible {
            eligible.retain(|index| {
                components[*index]
                    .iter()
                    .all(|id| !self.actions[id].action.chunk_transition)
            });
        }
        let mut remaining = eligible;
        let mut ordered = Vec::new();
        while !remaining.is_empty() {
            let mut candidates = remaining
                .iter()
                .copied()
                .filter(|index| {
                    components[*index].iter().all(|id| {
                        let node = &self.actions[id];
                        node.prerequisites
                            .iter()
                            .copied()
                            .map(|dependency| ActionRef {
                                tick,
                                id: dependency,
                            })
                            .chain(node.action.dependencies.iter().copied())
                            .all(|dependency| {
                                committed(dependency)
                                    || dependency.tick == tick
                                        && self
                                            .actions
                                            .get(&dependency.id)
                                            .is_some_and(GraphAction::is_ground_applied)
                                    || dependency.tick == tick
                                        && component_for.get(&dependency.id).is_some_and(|other| {
                                            *other == *index || !remaining.contains(other)
                                        })
                            })
                    })
                })
                .filter(|index| {
                    !components[*index]
                        .iter()
                        .any(|id| self.actions[id].action.chunk_transition)
                        || !remaining.iter().any(|other| {
                            other != index
                                && components[*other]
                                    .iter()
                                    .any(|id| !self.actions[id].action.chunk_transition)
                        })
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|index| components[*index].first().copied());
            let Some(next) = candidates.first().copied() else {
                return GroundReadyPlan {
                    components: Vec::new(),
                    unapplied_realms: BTreeMap::new(),
                    semantic_generations: BTreeMap::new(),
                };
            };
            remaining.remove(&next);
            ordered.push(next);
        }
        let components = ordered
            .into_iter()
            .map(|index| {
                let mut actions = components[index].iter().copied().collect::<Vec<_>>();
                actions.sort_by_key(|id| {
                    let node = &self.actions[id];
                    (
                        node.action.chunk_transition,
                        node.semantic_order.unwrap_or_default(),
                        *id,
                    )
                });
                actions
                    .into_iter()
                    .filter(|id| !self.actions[id].is_ground_applied())
                    .map(|id| ActionRef { tick, id })
                    .collect()
            })
            .collect::<Vec<Vec<ActionRef>>>();
        let unapplied_realms = components
            .iter()
            .flatten()
            .filter_map(|reference| {
                let node = &self.actions[&reference.id];
                let realms = node
                    .realms
                    .difference(&node.applied_realms)
                    .copied()
                    .collect::<BTreeSet<_>>();
                (!realms.is_empty()).then_some((*reference, realms))
            })
            .collect();
        let semantic_generations = components
            .iter()
            .flatten()
            .filter_map(|reference| {
                self.actions[&reference.id]
                    .semantic_generation
                    .map(|generation| (*reference, generation))
            })
            .collect();
        GroundReadyPlan {
            components,
            unapplied_realms,
            semantic_generations,
        }
    }

    #[must_use]
    pub fn affected_components(&self, seeds: &BTreeSet<ActionId>) -> Vec<BTreeSet<ActionId>> {
        let mut region = BTreeSet::new();
        let mut frontier = seeds.iter().copied().collect::<Vec<_>>();
        while let Some(id) = frontier.pop() {
            let Some(node) = self.actions.get(&id) else {
                continue;
            };
            if region.insert(id) {
                frontier.extend(node.dependents.iter().chain(&node.prerequisites).copied());
            }
        }
        let mut seen = BTreeSet::new();
        let mut order = Vec::new();
        for root in &region {
            let mut stack = vec![(*root, false)];
            while let Some((id, exiting)) = stack.pop() {
                if exiting {
                    order.push(id);
                    continue;
                }
                if !seen.insert(id) {
                    continue;
                }
                stack.push((id, true));
                if let Some(node) = self.actions.get(&id) {
                    stack.extend(
                        node.dependents
                            .iter()
                            .rev()
                            .filter(|next| region.contains(next))
                            .map(|next| (*next, false)),
                    );
                }
            }
        }
        let mut assigned = BTreeSet::new();
        let mut components = Vec::new();
        for root in order.into_iter().rev() {
            if !assigned.insert(root) {
                continue;
            }
            let mut component = BTreeSet::from([root]);
            let mut stack = vec![root];
            while let Some(id) = stack.pop() {
                if let Some(node) = self.actions.get(&id) {
                    for predecessor in &node.prerequisites {
                        if region.contains(predecessor) && assigned.insert(*predecessor) {
                            component.insert(*predecessor);
                            stack.push(*predecessor);
                        }
                    }
                }
            }
            components.push(component);
        }
        components
    }

    pub fn replace_factual(
        &mut self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: FactualVariant,
    ) -> Result<BTreeSet<ActionId>, ApplyFailure> {
        self.replace_factual_inner(id, wrapper, replacement, None)
    }

    pub fn replace_factual_for_recovery(
        &mut self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: FactualVariant,
        recovery: &crate::action_solver::CertifiedRecoveryConstraints,
    ) -> Result<BTreeSet<ActionId>, ApplyFailure> {
        self.replace_factual_inner(id, wrapper, replacement, Some(recovery))
    }

    fn replace_factual_inner(
        &mut self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: FactualVariant,
        recovery: Option<&crate::action_solver::CertifiedRecoveryConstraints>,
    ) -> Result<BTreeSet<ActionId>, ApplyFailure> {
        if let Some(recovery) = recovery {
            self.validate_factual_replacement_for_recovery(id, wrapper, &replacement, recovery)?;
        } else {
            self.validate_factual_replacement(id, wrapper, &replacement)?;
        }
        let node = self.actions.get(&id).ok_or(ApplyFailure::InvalidAction)?;
        let old = node.factual.clone();
        let mut changed = BTreeSet::from([id]);
        let old_conditions = condition_groups(&old.conditions);
        let new_conditions = condition_groups(&replacement.conditions);
        let old_effects = effect_groups(&old.effects, old.result.as_ref());
        let new_effects = effect_groups(&replacement.effects, replacement.result.as_ref());
        for (target, predicates) in &old_conditions {
            let next = new_conditions.get(target).map_or(&[][..], Vec::as_slice);
            if predicates.as_slice() != next {
                changed.extend(
                    self.realms[&target.action_realm()]
                        .factual_effects
                        .get(target)
                        .into_iter()
                        .flatten()
                        .map(|(peer, _)| *peer)
                        .filter(|peer| *peer != id),
                );
            }
            for _ in next.len()..predicates.len() {
                self.remove_factual_condition(id, *target, &mut changed);
            }
        }
        for (target, effects) in &old_effects {
            let next = new_effects.get(target).map_or(&[][..], Vec::as_slice);
            if effects.as_slice() != next {
                changed.extend(
                    self.realms[&target.action_realm()]
                        .factual_conditions
                        .get(target)
                        .into_iter()
                        .flatten()
                        .map(|(peer, _)| *peer)
                        .filter(|peer| *peer != id),
                );
            }
            for _ in next.len()..effects.len() {
                self.remove_factual_effect(id, *target, &mut changed);
            }
        }
        for (target, predicates) in &new_conditions {
            let prior_len = old_conditions.get(target).map_or(0, Vec::len);
            for _ in prior_len..predicates.len() {
                self.add_factual_condition(id, *target, &mut changed);
            }
        }
        for (target, effects) in &new_effects {
            let prior_len = old_effects.get(target).map_or(0, Vec::len);
            for _ in prior_len..effects.len() {
                self.add_factual_effect(id, *target, &mut changed);
            }
        }
        let node = self
            .actions
            .get_mut(&id)
            .ok_or(ApplyFailure::InvalidAction)?;
        node.factual = replacement;
        node.wrapper = wrapper;
        node.generation += 1;
        node.stable = false;
        node.semantic_generation = None;
        node.semantic_order = None;
        self.invalidate_affected(&changed);
        Ok(changed)
    }

    fn configure_actor_binding(
        &mut self,
        id: ActionId,
        binding: crate::entity_history::EntityActorLocationBinding,
    ) -> Result<(), ApplyFailure> {
        let node = self.actions.get(&id).ok_or(ApplyFailure::InvalidAction)?;
        if node.actor_binding.as_ref() == Some(&binding) {
            return Ok(());
        }
        if node.actor_binding.is_some()
            || !node.applied_realms.is_empty()
            || node.generation == u64::MAX
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let interpreted = binding.interpret_core(&node.action)?;
        let mut orders = Vec::new();
        for target in avatar_effect_targets(&interpreted) {
            for previous in self
                .realms
                .get(&target.action_realm())
                .and_then(|realm| realm.factual_effects.get(&target))
                .into_iter()
                .flatten()
                .map(|(previous, _)| *previous)
                .filter(|previous| *previous != id)
            {
                let other = self
                    .actions
                    .get(&previous)
                    .ok_or(ApplyFailure::InvalidAction)?
                    .semantic_action();
                if !avatar_effect_targets(other).contains(&target) {
                    continue;
                }
                validate_avatar_effect_pair(&interpreted, other)?;
                let earlier = if other.chunk_transition != interpreted.chunk_transition {
                    !other.chunk_transition
                } else {
                    id.sequence.wrapping_sub(previous.sequence)
                        < previous.sequence.wrapping_sub(id.sequence)
                };
                let (source, destination) = if earlier {
                    (previous, id)
                } else {
                    (id, previous)
                };
                orders.push((target, source, destination));
            }
        }
        let conditions = interpreted
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(interpreted.counterfactual_conditions.iter().copied())
            .collect::<BTreeSet<_>>();
        let effects = interpreted
            .effects
            .iter()
            .flat_map(Effect::targets)
            .chain(interpreted.counterfactual_effects.iter().copied())
            .collect::<BTreeSet<_>>();
        for target in &conditions {
            let index = &mut self
                .realms
                .entry(target.action_realm())
                .or_default()
                .counterfactual_conditions;
            if !index
                .get(target)
                .is_some_and(|entries| entries.contains_key(&id))
            {
                add_posting(index, *target, id);
            }
        }
        for target in &effects {
            let index = &mut self
                .realms
                .entry(target.action_realm())
                .or_default()
                .counterfactual_effects;
            if !index
                .get(target)
                .is_some_and(|entries| entries.contains_key(&id))
            {
                add_posting(index, *target, id);
            }
        }
        let node = self
            .actions
            .get_mut(&id)
            .ok_or(ApplyFailure::InvalidAction)?;
        node.realms.extend(
            conditions
                .iter()
                .chain(&effects)
                .map(|target| target.action_realm()),
        );
        node.realms
            .extend(interpreted.actor_chunk.map(ActionRealm::Chunk));
        node.actor_binding = Some(binding);
        node.interpreted_action = Some(interpreted);
        for (target, source, destination) in orders {
            for (from, to, reason) in [
                (
                    source,
                    destination,
                    EdgeReason::InfallibleAvatarOrder(target),
                ),
                (
                    destination,
                    source,
                    EdgeReason::InfallibleAvatarGrouping(target),
                ),
            ] {
                self.edge_reasons
                    .entry((from, to))
                    .or_default()
                    .entry(reason)
                    .or_insert(1);
                self.actions
                    .get_mut(&from)
                    .ok_or(ApplyFailure::InvalidAction)?
                    .dependents
                    .insert(to);
                self.actions
                    .get_mut(&to)
                    .ok_or(ApplyFailure::InvalidAction)?
                    .prerequisites
                    .insert(from);
            }
        }
        Ok(())
    }

    pub fn validate_factual_replacement(
        &self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: &FactualVariant,
    ) -> Result<(), ApplyFailure> {
        self.validate_factual_replacement_inner(id, wrapper, replacement, false)
    }

    pub fn validate_factual_replacement_for_recovery(
        &self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: &FactualVariant,
        recovery: &crate::action_solver::CertifiedRecoveryConstraints,
    ) -> Result<(), ApplyFailure> {
        recovery.validate_replacement(
            self.actions.get(&id).ok_or(ApplyFailure::InvalidAction)?,
            replacement,
        )?;
        self.validate_factual_replacement_inner(id, wrapper, replacement, true)
    }

    fn validate_factual_replacement_inner(
        &self,
        id: ActionId,
        wrapper: SemanticWrapper,
        replacement: &FactualVariant,
        recovery: bool,
    ) -> Result<(), ApplyFailure> {
        let node = self.actions.get(&id).ok_or(ApplyFailure::InvalidAction)?;
        let interpreted = node.semantic_action();
        let allowed_conditions = node
            .semantic_action()
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(interpreted.counterfactual_conditions.iter().copied())
            .collect::<BTreeSet<_>>();
        let allowed_effects = node
            .semantic_action()
            .effects
            .iter()
            .flat_map(|effect| effect.targets())
            .chain(interpreted.counterfactual_effects.iter().copied())
            .collect::<BTreeSet<_>>();
        let conditions = replacement
            .conditions
            .iter()
            .map(|condition| condition.target)
            .collect::<BTreeSet<_>>();
        let wrapper_valid = match wrapper {
            SemanticWrapper::Undone => {
                node.action.class != crate::canonical_action::ActionClass::Infallible
                    && replacement.conditions.is_empty()
                    && replacement.effects.is_empty()
                    && replacement.result.is_none()
            }
            _ => true,
        };
        if !wrapper_valid
            || !recovery
                && !node.applied_realms.is_empty()
                && (wrapper != node.wrapper || *replacement != node.factual)
            || replacement
                .conditions
                .iter()
                .enumerate()
                .any(|(index, condition)| replacement.conditions[..index].contains(condition))
            || wrapper != SemanticWrapper::Undone
                && interpreted.conditions.iter().any(|condition| {
                    matches!(
                        condition.expected,
                        crate::canonical_action::Expected::ActorControlled { .. }
                            | crate::canonical_action::Expected::VehicleTerminal(_)
                            | crate::canonical_action::Expected::ObjectTerminal(_)
                            | crate::canonical_action::Expected::EntityPlacement(_)
                            | crate::canonical_action::Expected::HangingOccupancyClear(_)
                            | crate::canonical_action::Expected::EntitiesOccupancyClear(_)
                            | crate::canonical_action::Expected::ActorOccupancyDomain(_)
                            | crate::canonical_action::Expected::HopperCollector { .. }
                            | crate::canonical_action::Expected::DroppedItem { .. }
                            | crate::canonical_action::Expected::EntityBirthOwned { .. }
                            | crate::canonical_action::Expected::EntityBirthKind { .. }
                            | crate::canonical_action::Expected::DroppedItemStack { .. }
                            | crate::canonical_action::Expected::SpatialEpochEquals(_)
                            | crate::canonical_action::Expected::ExperienceOrb { .. }
                    ) && !replacement.conditions.contains(condition)
                        && !(matches!(
                            node.action.semantic_intent,
                            Some(crate::canonical_action::SemanticIntent::MergeDroppedItems { .. })
                        ) && replacement.conditions.iter().any(|candidate| {
                            candidate.target == condition.target
                                && match (&condition.expected, &candidate.expected) {
                                    (
                                        crate::canonical_action::Expected::DroppedItemStack {
                                            birth: a,
                                            owner: b,
                                            epoch: c,
                                            item: d,
                                            components: e,
                                            ..
                                        },
                                        crate::canonical_action::Expected::DroppedItemStack {
                                            birth: x,
                                            owner: y,
                                            epoch: z,
                                            item: w,
                                            components: v,
                                            ..
                                        },
                                    ) => (a, b, c, d, e) == (x, y, z, w, v),
                                    _ => false,
                                }
                        }))
                })
            || node.generation == u64::MAX
            || conditions
                .iter()
                .any(|target| !allowed_conditions.contains(target))
            || replacement
                .effects
                .iter()
                .flat_map(|effect| effect.targets())
                .any(|target| {
                    !allowed_effects.contains(&target)
                        || node.action.class != crate::canonical_action::ActionClass::Infallible
                            && !conditions.contains(&target)
                })
            || replacement.result.as_ref().is_some_and(|result| {
                result.id != id
                    || result.tick != node.action.tick
                    || result.view != crate::canonical_action::TruthView::Local
                    || result.actor_chunk != interpreted.actor_chunk
                    || result.writes.iter().any(|write| {
                        !replacement
                            .effects
                            .iter()
                            .any(|effect| effect.targets().any(|target| target == write.target))
                            && !((allowed_conditions.contains(&write.target)
                                || allowed_effects.contains(&write.target)
                                || replacement.effects.iter().flat_map(Effect::targets)
                                    .filter_map(crate::derived_actor_occupancy::root_alias)
                                    .any(|target| target == write.target))
                                && derived_occupancy_write_valid(write, &replacement.effects))
                    })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    fn remove_factual_condition(
        &mut self,
        id: ActionId,
        target: Target,
        changed: &mut BTreeSet<ActionId>,
    ) {
        let peers = self.realms[&target.action_realm()]
            .factual_effects
            .get(&target)
            .cloned()
            .unwrap_or_default();
        for (peer, count) in peers {
            if peer != id {
                self.adjust_edge(
                    peer,
                    id,
                    EdgeReason::FactualEffectCondition(target),
                    count,
                    false,
                );
                changed.insert(peer);
            }
        }
        subtract_posting(
            &mut self
                .realms
                .get_mut(&target.action_realm())
                .expect("indexed realm")
                .factual_conditions,
            target,
            id,
        );
    }

    fn remove_factual_effect(
        &mut self,
        id: ActionId,
        target: Target,
        changed: &mut BTreeSet<ActionId>,
    ) {
        let peers = self.realms[&target.action_realm()]
            .factual_conditions
            .get(&target)
            .cloned()
            .unwrap_or_default();
        for (peer, count) in peers {
            if peer != id {
                self.adjust_edge(
                    id,
                    peer,
                    EdgeReason::FactualEffectCondition(target),
                    count,
                    false,
                );
                changed.insert(peer);
            }
        }
        subtract_posting(
            &mut self
                .realms
                .get_mut(&target.action_realm())
                .expect("indexed realm")
                .factual_effects,
            target,
            id,
        );
    }

    fn add_factual_condition(
        &mut self,
        id: ActionId,
        target: Target,
        changed: &mut BTreeSet<ActionId>,
    ) {
        let peers = self
            .realms
            .entry(target.action_realm())
            .or_default()
            .factual_effects
            .get(&target)
            .cloned()
            .unwrap_or_default();
        for (peer, count) in peers {
            if peer != id {
                self.adjust_edge(
                    peer,
                    id,
                    EdgeReason::FactualEffectCondition(target),
                    count,
                    true,
                );
                changed.insert(peer);
            }
        }
        add_posting(
            &mut self
                .realms
                .entry(target.action_realm())
                .or_default()
                .factual_conditions,
            target,
            id,
        );
    }

    fn add_factual_effect(
        &mut self,
        id: ActionId,
        target: Target,
        changed: &mut BTreeSet<ActionId>,
    ) {
        let peers = self
            .realms
            .entry(target.action_realm())
            .or_default()
            .factual_conditions
            .get(&target)
            .cloned()
            .unwrap_or_default();
        for (peer, count) in peers {
            if peer != id {
                self.adjust_edge(
                    id,
                    peer,
                    EdgeReason::FactualEffectCondition(target),
                    count,
                    true,
                );
                changed.insert(peer);
            }
        }
        add_posting(
            &mut self
                .realms
                .entry(target.action_realm())
                .or_default()
                .factual_effects,
            target,
            id,
        );
    }

    fn adjust_edge(
        &mut self,
        source: ActionId,
        destination: ActionId,
        reason: EdgeReason,
        count: u16,
        add: bool,
    ) {
        if add {
            *self
                .edge_reasons
                .entry((source, destination))
                .or_default()
                .entry(reason)
                .or_default() += count;
            self.actions
                .get_mut(&source)
                .expect("indexed source")
                .dependents
                .insert(destination);
            self.actions
                .get_mut(&destination)
                .expect("indexed destination")
                .prerequisites
                .insert(source);
            return;
        }
        let reasons = self
            .edge_reasons
            .get_mut(&(source, destination))
            .expect("indexed edge");
        let remaining = reasons.get_mut(&reason).expect("indexed edge reason");
        *remaining = remaining.checked_sub(count).expect("indexed edge count");
        if *remaining == 0 {
            reasons.remove(&reason);
        }
        if reasons.is_empty() {
            self.edge_reasons.remove(&(source, destination));
            self.actions
                .get_mut(&source)
                .expect("indexed source")
                .dependents
                .remove(&destination);
            self.actions
                .get_mut(&destination)
                .expect("indexed destination")
                .prerequisites
                .remove(&source);
        }
    }

    #[must_use]
    pub fn counterfactual_blockers(&self, id: ActionId) -> BTreeSet<ActionId> {
        let Some(node) = self.actions.get(&id) else {
            return BTreeSet::new();
        };
        let mut blockers = BTreeSet::new();
        for target in node
            .factual
            .conditions
            .iter()
            .map(|condition| condition.target)
        {
            if let Some(index) = self.realms.get(&target.action_realm()) {
                blockers.extend(
                    index
                        .counterfactual_effects
                        .get(&target)
                        .into_iter()
                        .flatten()
                        .map(|(id, _)| *id),
                );
            }
        }
        for target in
            node.factual.effects.iter().flat_map(|effect| {
                indexed_factual_effect_targets(effect, node.factual.result.as_ref())
            })
        {
            if let Some(index) = self.realms.get(&target.action_realm()) {
                blockers.extend(
                    index
                        .counterfactual_conditions
                        .get(&target)
                        .into_iter()
                        .flatten()
                        .map(|(id, _)| *id),
                );
            }
        }
        for target in node.indexed_conditions() {
            if let Some(index) = self.realms.get(&target.action_realm()) {
                blockers.extend(
                    index
                        .factual_effects
                        .get(&target)
                        .into_iter()
                        .flatten()
                        .map(|(id, _)| *id),
                );
            }
        }
        for target in node
            .indexed_effects()
            .into_iter()
            .flat_map(effect_location_aliases)
        {
            if let Some(index) = self.realms.get(&target.action_realm()) {
                blockers.extend(
                    index
                        .factual_conditions
                        .get(&target)
                        .into_iter()
                        .flatten()
                        .map(|(id, _)| *id),
                );
            }
        }
        blockers.remove(&id);
        blockers
    }

    pub fn insert(&mut self, action: Action) -> Result<BTreeSet<ActionId>, ApplyFailure> {
        action.validate()?;
        if self.actions.contains_key(&action.id) {
            return Err(ApplyFailure::InvalidAction);
        }
        let id = action.id;
        let conditions = action
            .conditions
            .iter()
            .map(|condition| condition.target)
            .collect::<BTreeSet<_>>();
        let effects = action
            .effects
            .iter()
            .flat_map(|effect| {
                indexed_factual_effect_targets(effect, action.factual_result.as_ref())
            })
            .collect::<Vec<_>>();
        let effect_locations = effects.iter().copied().collect::<BTreeSet<_>>();
        let counterfactual_conditions = action
            .counterfactual_conditions
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let counterfactual_effects = action
            .counterfactual_effects
            .iter()
            .copied()
            .chain(
                action
                    .effects
                    .iter()
                    .filter(|effect| effect.has_variable_write_footprint())
                    .flat_map(Effect::targets),
            )
            .flat_map(effect_location_aliases)
            .collect::<BTreeSet<_>>();
        let mut realms: BTreeSet<_> = conditions
            .iter()
            .chain(&effects)
            .chain(&counterfactual_conditions)
            .chain(&counterfactual_effects)
            .map(|target| target.action_realm())
            .collect();
        if let Some(actor_chunk) = action.actor_chunk {
            realms.insert(ActionRealm::Chunk(actor_chunk));
        }
        let mut dependencies = BTreeMap::<(ActionId, ActionId), BTreeMap<EdgeReason, u16>>::new();
        let mut prerequisites = BTreeSet::new();
        for dependency in &action.dependencies {
            if dependency.tick == action.tick && dependency.id == id {
                return Err(ApplyFailure::InvalidAction);
            }
            if dependency.tick != action.tick {
                continue;
            }
            prerequisites.insert(dependency.id);
            self.declared_dependents
                .entry(dependency.id)
                .or_default()
                .insert(id);
            if self.actions.contains_key(&dependency.id) {
                add_edge_reason(
                    &mut dependencies,
                    dependency.id,
                    id,
                    EdgeReason::Declared(*dependency),
                    1,
                );
            }
        }
        if let Some(waiting) = self.declared_dependents.get(&id) {
            for dependent in waiting {
                add_edge_reason(
                    &mut dependencies,
                    id,
                    *dependent,
                    EdgeReason::Declared(ActionRef {
                        tick: action.tick,
                        id,
                    }),
                    1,
                );
            }
        }
        for condition in &action.conditions {
            let target = condition.target;
            let index = self.realms.entry(target.action_realm()).or_default();
            for (previous, count) in index.factual_effects.get(&target).into_iter().flatten() {
                add_edge_reason(
                    &mut dependencies,
                    *previous,
                    id,
                    EdgeReason::FactualEffectCondition(target),
                    *count,
                );
                prerequisites.insert(*previous);
            }
            add_posting(&mut index.factual_conditions, target, id);
        }
        for target in &effects {
            let index = self.realms.entry(target.action_realm()).or_default();
            if let Some(source_order) = action.effects.iter().find_map(|effect| match effect {
                Effect::ScheduleTick {
                    target: scheduled_target,
                    tick,
                } if scheduled_target == target => Some((1_u8, tick.source_order)),
                Effect::DrainScheduledTicks {
                    target: scheduled_target,
                    ..
                } if scheduled_target == target => Some((0_u8, 0)),
                Effect::CompleteScheduledTick {
                    target: scheduled_target,
                    fact,
                } if scheduled_target == target => Some((0_u8, fact.source_order)),
                _ => None,
            }) {
                for previous in index
                    .factual_effects
                    .get(target)
                    .into_iter()
                    .flatten()
                    .map(|(id, _)| *id)
                {
                    let Some(other) = self.actions.get(&previous) else {
                        continue;
                    };
                    let Some(previous_order) =
                        other.action.effects.iter().find_map(|effect| match effect {
                            Effect::ScheduleTick {
                                target: scheduled_target,
                                tick,
                            } if scheduled_target == target => Some((1_u8, tick.source_order)),
                            Effect::DrainScheduledTicks {
                                target: scheduled_target,
                                ..
                            } if scheduled_target == target => Some((0_u8, 0)),
                            Effect::CompleteScheduledTick {
                                target: scheduled_target,
                                fact,
                            } if scheduled_target == target => Some((0_u8, fact.source_order)),
                            _ => None,
                        })
                    else {
                        continue;
                    };
                    let earlier = (previous_order, previous) < (source_order, id);
                    let (source, destination) = if earlier {
                        (previous, id)
                    } else {
                        (id, previous)
                    };
                    let pure_queue = |action: &Action| {
                        action.class == crate::canonical_action::ActionClass::Infallible
                            && action.conditions.is_empty()
                            && action.effects.iter().all(|effect| {
                                matches!(
                                    effect,
                                    Effect::ScheduleTick { .. }
                                        | Effect::DrainScheduledTicks { .. }
                                )
                            })
                    };
                    let ordered = pure_queue(&action) && pure_queue(&other.action);
                    add_edge_reason(
                        &mut dependencies,
                        source,
                        destination,
                        if ordered {
                            EdgeReason::InfallibleScheduledOrder(*target)
                        } else {
                            EdgeReason::InfallibleScheduledGrouping(*target)
                        },
                        1,
                    );
                    add_edge_reason(
                        &mut dependencies,
                        destination,
                        source,
                        EdgeReason::InfallibleScheduledGrouping(*target),
                        1,
                    );
                    if ordered && destination == id {
                        prerequisites.insert(previous);
                    }
                }
            }
            if action.effects.iter().any(|effect| {
                matches!(
                    effect,
                    Effect::MovePlayerAvatar { .. }
                        | Effect::ResetPlayerLiving { .. }
                        | Effect::RespawnPlayerAvatar { .. }
                        | Effect::MoveEntityBody { .. }
                        | Effect::ChangeDroppedItem {
                            change: crate::entity_body::DroppedItemChange::AdvanceLifetime,
                            ..
                        }
                ) && effect.targets().any(|affected| affected == *target)
            }) {
                for previous in index
                    .factual_effects
                    .get(target)
                    .into_iter()
                    .flatten()
                    .map(|(id, _)| *id)
                {
                    let Some(other) = self.actions.get(&previous) else {
                        continue;
                    };
                    if !other.action.effects.iter().any(|effect| {
                        matches!(
                            effect,
                            Effect::MovePlayerAvatar { .. }
                                | Effect::ResetPlayerLiving { .. }
                                | Effect::RespawnPlayerAvatar { .. }
                                | Effect::MoveEntityBody { .. }
                                | Effect::ChangeDroppedItem {
                                    change: crate::entity_body::DroppedItemChange::AdvanceLifetime,
                                    ..
                                }
                        ) && effect.targets().any(|affected| affected == *target)
                    }) {
                        continue;
                    }
                    validate_avatar_effect_pair(&action, &other.action)?;
                    let distance_from_previous = id.sequence.wrapping_sub(previous.sequence);
                    let distance_from_next = previous.sequence.wrapping_sub(id.sequence);
                    let earlier = if other.action.chunk_transition != action.chunk_transition {
                        !other.action.chunk_transition
                    } else {
                        distance_from_previous < distance_from_next
                    };
                    let (source, destination) = if earlier {
                        (previous, id)
                    } else {
                        (id, previous)
                    };
                    add_edge_reason(
                        &mut dependencies,
                        source,
                        destination,
                        EdgeReason::InfallibleAvatarOrder(*target),
                        1,
                    );
                    add_edge_reason(
                        &mut dependencies,
                        destination,
                        source,
                        EdgeReason::InfallibleAvatarGrouping(*target),
                        1,
                    );
                    if destination == id {
                        prerequisites.insert(previous);
                    }
                }
            }
            for (previous, count) in index.factual_conditions.get(target).into_iter().flatten() {
                add_edge_reason(
                    &mut dependencies,
                    id,
                    *previous,
                    EdgeReason::FactualEffectCondition(*target),
                    *count,
                );
            }
            add_posting(&mut index.factual_effects, *target, id);
        }
        for target in &conditions {
            add_posting(
                &mut self
                    .realms
                    .entry(target.action_realm())
                    .or_default()
                    .counterfactual_conditions,
                *target,
                id,
            );
        }
        for target in &effect_locations {
            add_posting(
                &mut self
                    .realms
                    .entry(target.action_realm())
                    .or_default()
                    .counterfactual_effects,
                *target,
                id,
            );
        }
        for target in counterfactual_conditions.difference(&conditions) {
            add_posting(
                &mut self
                    .realms
                    .entry(target.action_realm())
                    .or_default()
                    .counterfactual_conditions,
                *target,
                id,
            );
        }
        let variable_effects = action
            .effects
            .iter()
            .filter(|effect| effect.has_variable_write_footprint())
            .flat_map(indexed_effect_targets)
            .collect::<BTreeSet<_>>();
        for target in counterfactual_effects.iter().filter(|target| {
            !effect_locations.contains(target) || variable_effects.contains(target)
        }) {
            add_posting(
                &mut self
                    .realms
                    .entry(target.action_realm())
                    .or_default()
                    .counterfactual_effects,
                *target,
                id,
            );
        }
        self.actions.insert(
            id,
            GraphAction {
                actor_binding: None,
                interpreted_action: None,
                factual: FactualVariant {
                    conditions: action.conditions.clone(),
                    effects: action.effects.clone(),
                    result: action.factual_result.clone(),
                },
                action,
                wrapper: SemanticWrapper::Applied,
                local_order: None,
                local_result: None,
                local_generation: None,
                semantic_generation: None,
                semantic_order: None,
                generation: 0,
                stable: false,
                applied_realms: BTreeSet::new(),
                realms,
                dependents: BTreeSet::new(),
                prerequisites,
            },
        );
        let mut newly_connected = BTreeSet::from([id]);
        for ((source, destination), reasons) in dependencies {
            if source == destination {
                continue;
            }
            let stored = self.edge_reasons.entry((source, destination)).or_default();
            for (reason, count) in reasons {
                *stored.entry(reason).or_default() += count;
            }
            if let Some(node) = self.actions.get_mut(&source) {
                node.dependents.insert(destination);
            }
            if let Some(node) = self.actions.get_mut(&destination) {
                node.prerequisites.insert(source);
            }
            newly_connected.insert(source);
            newly_connected.insert(destination);
        }
        self.invalidate_affected(&newly_connected);
        Ok(newly_connected)
    }

    pub fn remove(&mut self, id: ActionId) -> Option<Action> {
        let node = self.actions.remove(&id)?;
        if node.is_ground_applied() {
            for target in node.indexed_targets() {
                self.grounded_targets.remove_action(target, id);
            }
        }
        self.declared_dependents.remove(&id);
        for dependency in &node.action.dependencies {
            if dependency.tick != node.action.tick {
                continue;
            }
            if let Some(waiting) = self.declared_dependents.get_mut(&dependency.id) {
                waiting.remove(&id);
                if waiting.is_empty() {
                    self.declared_dependents.remove(&dependency.id);
                }
            }
        }
        for target in node
            .factual
            .conditions
            .iter()
            .map(|condition| condition.target)
        {
            if let Some(realm) = self.realms.get_mut(&target.action_realm()) {
                remove_location(&mut realm.factual_conditions, target, id);
            }
        }
        for target in
            node.factual.effects.iter().flat_map(|effect| {
                indexed_factual_effect_targets(effect, node.factual.result.as_ref())
            })
        {
            if let Some(realm) = self.realms.get_mut(&target.action_realm()) {
                remove_location(&mut realm.factual_effects, target, id);
            }
        }
        for target in node.indexed_conditions() {
            if let Some(realm) = self.realms.get_mut(&target.action_realm()) {
                remove_location(&mut realm.counterfactual_conditions, target, id);
            }
        }
        for target in node
            .indexed_effects()
            .into_iter()
            .flat_map(effect_location_aliases)
        {
            if let Some(realm) = self.realms.get_mut(&target.action_realm()) {
                remove_location(&mut realm.counterfactual_effects, target, id);
            }
        }
        for previous in &node.prerequisites {
            if let Some(previous) = self.actions.get_mut(previous) {
                previous.dependents.remove(&id);
            }
        }
        for dependent in &node.dependents {
            if let Some(dependent) = self.actions.get_mut(dependent) {
                dependent.prerequisites.remove(&id);
            }
        }
        self.edge_reasons
            .retain(|(source, destination), _| *source != id && *destination != id);
        for realm in node.realms {
            if self
                .realms
                .get(&realm)
                .is_some_and(LocationMultisets::is_empty)
            {
                self.realms.remove(&realm);
            }
        }
        Some(node.action)
    }
}

fn add_posting(index: &mut LocationPostings, target: Target, id: ActionId) {
    index.add(target, id);
}

fn condition_groups(
    conditions: &[Condition],
) -> BTreeMap<Target, Vec<&crate::canonical_action::Expected>> {
    let mut groups = BTreeMap::<Target, Vec<&crate::canonical_action::Expected>>::new();
    for condition in conditions {
        groups
            .entry(condition.target)
            .or_default()
            .push(&condition.expected);
    }
    groups
}

fn effect_groups(
    effects: &[Effect],
    result: Option<&AppliedAction>,
) -> BTreeMap<Target, Vec<Effect>> {
    let mut groups = BTreeMap::<Target, Vec<Effect>>::new();
    for effect in effects {
        for target in indexed_factual_effect_targets(effect, result) {
            groups.entry(target).or_default().push(effect.clone());
        }
    }
    groups
}

fn effect_location_aliases(target: Target) -> impl Iterator<Item = Target> {
    let section = match target {
        Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            inventory: None,
        } => Some(Target::Chunk {
            key,
            part: ChunkPart::BlockSection(position.y.div_euclid(16)),
            inventory: None,
        }),
        Target::Chunk { key, part: ChunkPart::LivingProperty { actor, property: crate::living_property::LivingProperty::PermanentAttributeModifiers(attribute) }, inventory: None } =>
            Some(Target::Chunk { key, part: ChunkPart::LivingProperty { actor, property: crate::living_property::LivingProperty::Attribute(attribute) }, inventory: None }),
        _ => None,
    };
    std::iter::once(target).chain(section).chain(crate::derived_actor_occupancy::root_alias(target))
}

fn indexed_effect_targets(effect: &Effect) -> impl Iterator<Item = Target> + '_ {
    effect.targets().flat_map(effect_location_aliases)
}

fn indexed_factual_effect_targets<'a>(
    effect: &'a Effect,
    result: Option<&'a AppliedAction>,
) -> impl Iterator<Item = Target> + 'a {
    let mut targets = effect
        .targets()
        .filter(move |target| {
            !effect.has_variable_write_footprint()
                || result
                    .is_some_and(|result| result.writes.iter().any(|write| write.target == *target))
        })
        .flat_map(effect_location_aliases)
        .collect::<BTreeSet<_>>();
    targets.retain(|target| !matches!(target, Target::Chunk { part: ChunkPart::ActorOccupancy, inventory: None, .. }) || result.is_some_and(|result| result.writes.iter().any(|write| write.target == *target)));
    let species_bodies = targets
        .iter()
        .filter_map(|target| match target {
            Target::Chunk {
                part:
                    ChunkPart::LivingProperty {
                        property:
                            crate::living_property::LivingProperty::MobSpecies
                            | crate::living_property::LivingProperty::MobSpeciesProperty(_),
                        ..
                    },
                ..
            } => crate::derived_living::actor_body_target(*target),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if let Some(result) = result {
        targets.extend(result.writes.iter().filter_map(|write| {
            match write.target {
                Target::Chunk {
                    part:
                        ChunkPart::LivingProperty {
                            property:
                                crate::living_property::LivingProperty::MobSpecies
                                | crate::living_property::LivingProperty::MobSpeciesProperty(_),
                            ..
                        },
                    ..
                } if crate::derived_living::actor_body_target(write.target)
                    .is_some_and(|body| species_bodies.contains(&body)) =>
                {
                    Some(write.target)
                }
                _ => None,
            }
        }));
    }
    targets.into_iter()
}

fn subtract_posting(index: &mut LocationPostings, target: Target, id: ActionId) {
    index.subtract(target, id, 1);
}

fn add_edge_reason(
    edges: &mut BTreeMap<(ActionId, ActionId), BTreeMap<EdgeReason, u16>>,
    source: ActionId,
    destination: ActionId,
    reason: EdgeReason,
    count: u16,
) {
    *edges
        .entry((source, destination))
        .or_default()
        .entry(reason)
        .or_default() += count;
}

fn remove_location(index: &mut LocationPostings, target: Target, id: ActionId) {
    index.remove_action(target, id);
}

fn avatar_effect_targets(action: &Action) -> BTreeSet<Target> {
    action
        .effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::MovePlayerAvatar { .. }
                    | Effect::ResetPlayerLiving { .. }
                    | Effect::RespawnPlayerAvatar { .. }
                    | Effect::MoveEntityBody { .. }
                    | Effect::ChangeDroppedItem {
                        change: crate::entity_body::DroppedItemChange::AdvanceLifetime,
                        ..
                    }
            )
        })
        .flat_map(Effect::targets)
        .collect()
}

fn validate_avatar_effect_pair(action: &Action, previous: &Action) -> Result<(), ApplyFailure> {
    if previous.id.actor != action.id.actor
        || previous.id.incarnation != action.id.incarnation
        || previous.chunk_transition && action.chunk_transition
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let distance_from_previous = action.id.sequence.wrapping_sub(previous.id.sequence);
    let distance_from_next = previous.id.sequence.wrapping_sub(action.id.sequence);
    if distance_from_previous.min(distance_from_next) > 4_096 || distance_from_previous == 0 {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(())
}

#[derive(Clone, Default)]
pub struct ActionGraphs {
    pub ticks: StructuralMap<TickStamp, TickActionGraph>,
    pub ground_anchors: StructuralMap<Target, GroundAnchor>,
    pub chunk_frontiers: StructuralMap<ChunkKey, ChunkGroundFrontier>,
    pub player_inventory_frontiers:
        StructuralMap<(PlayerKey, InventoryRealmId, GlobalPlayerId, u64), PlayerInventoryGroundFrontier>,
    pub player_avatar_admissions: StructuralMap<Target, crate::canonical_action::LivePlayerLease>,
    pub player_avatar_admitted_at: StructuralMap<Target, TickStamp>,
    grounded_ticks: std::sync::Arc<BTreeSet<TickStamp>>,
    by_target: LocationPostings<ActionRef>,
    by_actor: StructuralMap<ActorId, StructuralMap<ActionRef, ()>>,
    by_chunk: StructuralMap<ChunkKey, StructuralMap<ActionRef, ()>>,
    action_chunk_index: StructuralMap<ActionRef, BTreeSet<ChunkKey>>,
    declared_dependents: StructuralMap<ActionRef, BTreeSet<ActionRef>>,
    dependency_history: StructuralMap<
        ActionRef,
        (
            bool,
            std::sync::Arc<crate::entity_history::EntityHistoryEvidence>,
        ),
    >,
    unavailable_dependency_history: std::sync::Arc<BTreeSet<ActionRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroundAnchor {
    pub settled_through: TickStamp,
    pub value: Option<Cell>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkGroundFrontier {
    pub epoch: u64,
    pub settled_through: TickStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerInventoryGroundFrontier {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub accepted_chunk: ChunkKey,
    pub domain_epoch: u64,
    pub settled_through: TickStamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolicAnchorFailure {
    MissingGround(Target),
    Unsettled(ActionRef),
    Inconsistent(ActionRef, Target),
    InvalidTick,
}

fn affected_actors(action: &Action) -> BTreeSet<ActorId> {
    let mut actors = BTreeSet::from([action.id.actor]);
    actors.extend(action.player_leases.iter().map(|lease| ActorId::Player(lease.gid)));
    for target in action.targets().into_iter().chain(action.counterfactual_conditions.iter().copied()).chain(action.counterfactual_effects.iter().copied()) {
        match target {
            Target::Player { lease: Some(lease), .. } => { actors.insert(ActorId::Player(lease.gid)); }
            Target::Chunk { part: ChunkPart::Player(gid), .. } => { actors.insert(ActorId::Player(gid)); }
            Target::Chunk { part: ChunkPart::Entity(id) | ChunkPart::VehicleProperty { actor: id, .. } | ChunkPart::ObjectProperty { actor: id, .. }, .. } => { actors.insert(ActorId::Entity(id)); }
            Target::Chunk { part: ChunkPart::LivingProperty { actor, .. } | ChunkPart::ActorRelation { actor, .. }, .. } => { actors.insert(actor); }
            _ => {}
        }
    }
    actors
}

pub fn affected_chunks(action: &Action) -> BTreeSet<ChunkKey> {
    action.actor_chunk.into_iter()
        .chain(action.player_chunks.iter().map(|(_, chunk)| *chunk))
        .chain(action.stakeholders.iter().filter_map(|stakeholder| match stakeholder { Stakeholder::Chunk(chunk) => Some(*chunk), _ => None }))
        .chain(action.targets().into_iter().chain(action.counterfactual_conditions.iter().copied()).chain(action.counterfactual_effects.iter().copied()).filter_map(|target| match target { Target::Chunk { key, .. } => Some(key), _ => None }))
        .collect()
}

impl ActionGraphs {
    pub fn actor_action_refs(&self, actor: ActorId) -> impl Iterator<Item = ActionRef> + '_ {
        self.by_actor.get(&actor).into_iter().flat_map(|references| references.keys().copied())
    }

    pub fn unsettled_actor_actions(&self, actor: ActorId) -> impl Iterator<Item = ActionRef> + '_ {
        self.actor_action_refs(actor)
            .filter(|reference| self.ticks.get(&reference.tick).and_then(|graph| graph.actions.get(&reference.id)).is_some_and(|node| !node.is_ground_applied()))
    }

    pub fn chunk_action_refs(&self, chunk: ChunkKey) -> impl Iterator<Item = ActionRef> + '_ {
        self.by_chunk.get(&chunk).into_iter().flat_map(|references| references.keys().copied())
    }

    pub fn install_dependency_history(
        &mut self,
        evidence: std::sync::Arc<crate::entity_history::EntityHistoryEvidence>,
        proofs: Option<&crate::entity_history::EntityBindingProofContext>,
    ) -> Result<(), ApplyFailure> {
        let outcomes = evidence.fixed_dependency_outcomes(proofs)?;
        if outcomes.is_empty() {
            return Err(ApplyFailure::InvalidAction);
        }
        if self
            .dependency_history
            .keys()
            .copied()
            .chain(outcomes.keys().copied())
            .collect::<BTreeSet<_>>()
            .len()
            > 8192
        {
            return Err(ApplyFailure::Capacity);
        }
        let nodes = evidence
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
        for (reference, accepted) in &outcomes {
            if self.unavailable_dependency_history.contains(reference) {
                return Err(ApplyFailure::InvalidAction);
            }
            let proof = nodes[reference];
            if self
                .ticks
                .get(&reference.tick)
                .and_then(|graph| graph.actions.get(&reference.id))
                .is_some_and(|node| {
                    node.action != proof.action
                        || node.factual != proof.factual
                        || node.actor_binding != proof.actor_binding
                        || node.realms != proof.required_realms
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if let Some((previous_outcome, previous)) = self.dependency_history.get(reference) {
                let previous_node = previous
                    .graphs
                    .iter()
                    .flat_map(|graph| &graph.nodes)
                    .find(|node| {
                        node.action.tick == reference.tick && node.action.id == reference.id
                    })
                    .ok_or(ApplyFailure::InvalidAction)?;
                if previous_outcome != accepted
                    || previous_node.action != proof.action
                    || previous_node.factual != proof.factual
                    || previous_node.actor_binding != proof.actor_binding
                    || previous_node.required_realms != proof.required_realms
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        for (reference, accepted) in outcomes {
            self.dependency_history
                .insert(reference, (accepted, evidence.clone()));
        }
        Ok(())
    }

    pub fn dependency_outcome(&self, reference: ActionRef) -> Option<bool> {
        if let Some(node) = self
            .ticks
            .get(&reference.tick)
            .and_then(|graph| graph.actions.get(&reference.id))
        {
            return (node.semantic_generation == Some(node.generation) || node.is_ground_applied())
                .then_some(node.factual.result.is_some());
        }
        if self.unavailable_dependency_history.contains(&reference) {
            return None;
        }
        self.dependency_history
            .get(&reference)
            .map(|(accepted, _)| *accepted)
    }

    pub fn dependency_history_for(
        &self,
        references: &BTreeSet<ActionRef>,
    ) -> Result<Vec<std::sync::Arc<crate::entity_history::EntityHistoryEvidence>>, ApplyFailure>
    {
        let mut seen = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut pending = references
            .iter()
            .copied()
            .collect::<std::collections::VecDeque<_>>();
        let mut frames = Vec::new();
        while let Some(reference) = pending.pop_front() {
            if !visited.insert(reference) {
                continue;
            }
            if visited.len() > 8192 {
                return Err(ApplyFailure::Capacity);
            }
            let Some((_, evidence)) = self.dependency_history.get(&reference) else {
                if self
                    .ticks
                    .get(&reference.tick)
                    .is_some_and(|graph| graph.actions.contains_key(&reference.id))
                {
                    continue;
                }
                return Err(ApplyFailure::InvalidAction);
            };
            let node = evidence
                .graphs
                .iter()
                .flat_map(|graph| &graph.nodes)
                .find(|node| node.action.tick == reference.tick && node.action.id == reference.id)
                .ok_or(ApplyFailure::InvalidAction)?;
            pending.extend(node.action.dependencies.iter().copied());
            if seen.insert(std::sync::Arc::as_ptr(evidence) as usize) {
                if frames.len() >= 128 {
                    return Err(ApplyFailure::Capacity);
                }
                frames.push(evidence.clone());
            }
        }
        Ok(frames)
    }

    pub fn retain_dependency_roots(&mut self, roots: &BTreeSet<ActionRef>) {
        let mut retained = roots.clone();
        retained.extend(self.declared_dependents.keys().copied());
        let mut pending = retained.iter().copied().collect::<Vec<_>>();
        while let Some(reference) = pending.pop() {
            let Some((_, frame)) = self.dependency_history.get(&reference) else {
                continue;
            };
            let Some(node) = frame
                .graphs
                .iter()
                .find(|graph| graph.tick == reference.tick)
                .and_then(|graph| {
                    graph
                        .nodes
                        .iter()
                        .find(|node| node.action.id == reference.id)
                })
            else {
                continue;
            };
            for dependency in &node.action.dependencies {
                if retained.insert(*dependency) {
                    pending.push(*dependency);
                }
            }
        }
        self.dependency_history
            .retain(|reference, _| retained.contains(reference));
        std::sync::Arc::make_mut(&mut self.unavailable_dependency_history)
            .retain(|reference| self.dependency_history.contains_key(reference));
    }

    pub fn advance_dependency_history_cut(&mut self, current: TickStamp) {
        std::sync::Arc::make_mut(&mut self.unavailable_dependency_history).extend(
            self.dependency_history
                .keys()
                .filter(|reference| current.distance_since(reference.tick) >= 32768)
                .copied(),
        );
    }

    pub fn dependency_history_needed(&self, tick: TickStamp) -> bool {
        self.ticks.get(&tick).is_some_and(|graph| {
            graph.actions.values().any(|node| {
                self.declared_dependents
                    .get(&ActionRef {
                        tick,
                        id: node.action.id,
                    })
                    .is_some_and(|dependents| !dependents.is_empty())
                    || node
                        .action
                        .effects
                        .iter()
                        .any(|effect| matches!(effect, Effect::ScheduleTick { .. }))
            })
        })
    }

    pub fn install_actor_binding(
        &mut self,
        binding: crate::entity_history::EntityActorLocationBinding,
        proofs: &crate::entity_history::EntityBindingProofContext,
        completion: &mut crate::realm_completion::RealmCompletionLedger,
        destination_requirements: BTreeSet<crate::realm_completion::RequirementKey>,
    ) -> Result<BTreeSet<ActionRef>, ApplyFailure> {
        let reference = binding.action;
        let graph = self
            .ticks
            .get(&reference.tick)
            .ok_or(ApplyFailure::InvalidAction)?;
        let node = graph
            .actions
            .get(&reference.id)
            .ok_or(ApplyFailure::InvalidAction)?;
        if !node.applied_realms.is_empty()
            || node.generation == u64::MAX
            || proofs.binding_candidate(&node.action)?.as_ref() != Some(&binding)
            || node
                .actor_binding
                .as_ref()
                .is_some_and(|existing| existing != &binding)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if node.actor_binding.as_ref() == Some(&binding) {
            return Ok(BTreeSet::new());
        }
        let interpreted = binding.interpret_core(&node.action)?;
        let binding_stakeholders = proofs
            .binding_chunks(&node.action, &binding)?
            .into_iter()
            .map(Stakeholder::Chunk)
            .collect::<BTreeSet<_>>();
        let cut = TickStamp(reference.tick.0.wrapping_sub(1));
        let source = self
            .ground_anchors
            .get(&binding.captured_source)
            .ok_or(ApplyFailure::InvalidAction)?;
        let destination = self
            .ground_anchors
            .get(&binding.resolved_source)
            .ok_or(ApplyFailure::InvalidAction)?;
        let guard = crate::canonical_action::Expected::ActorControlled {
            birth: binding.birth.clone(),
            issuer: binding.issuer,
        };
        if source.settled_through != cut
            || destination.settled_through != cut
            || source.value.is_some()
            || !guard.matches(destination.value.as_ref())
            || destination_requirements.is_empty()
            || destination_requirements
                .iter()
                .map(|key| key.stakeholder)
                .collect::<BTreeSet<_>>()
                != binding_stakeholders
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut ledger = completion.clone();
        ledger
            .capture_virtual_actor_requirements(reference, destination_requirements)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let mut candidate = graph.clone();
        let conditions = interpreted
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(interpreted.counterfactual_conditions.iter().copied())
            .collect::<BTreeSet<_>>();
        let effects = interpreted
            .effects
            .iter()
            .flat_map(Effect::targets)
            .chain(interpreted.counterfactual_effects.iter().copied())
            .collect::<BTreeSet<_>>();
        let replacement = FactualVariant {
            conditions: interpreted.conditions.clone(),
            effects: interpreted.effects.clone(),
            result: None,
        };
        candidate.configure_actor_binding(reference.id, binding)?;
        let changed =
            candidate.replace_factual(reference.id, SemanticWrapper::Replayed, replacement)?;
        self.ticks.insert(reference.tick, candidate);
        for stakeholder in binding_stakeholders {
            if let Stakeholder::Chunk(chunk) = stakeholder {
                self.by_chunk.entry(chunk).or_default().insert(reference, ());
                self.action_chunk_index.entry(reference).or_default().insert(chunk);
            }
        }
        for target in conditions
            .into_iter()
            .chain(effects.into_iter().flat_map(effect_location_aliases))
        {
            self.by_target.insert_reference(target, reference);
        }
        *completion = ledger;
        Ok(changed
            .into_iter()
            .map(|id| ActionRef {
                tick: reference.tick,
                id,
            })
            .collect())
    }

    pub fn install_player_avatar_admission(
        &mut self,
        target: Target,
        lease: crate::canonical_action::LivePlayerLease,
        anchor: GroundAnchor,
    ) -> Result<(), ApplyFailure> {
        let admitted_at = anchor.settled_through;
        self.install_player_avatar_admission_at(target, lease, admitted_at, anchor)
    }

    pub fn seed_player_avatar_bootstrap(
        &mut self,
        target: Target,
        lease: crate::canonical_action::LivePlayerLease,
        admitted_at: TickStamp,
    ) -> Result<(), ApplyFailure> {
        let Target::Chunk {
            key,
            part: ChunkPart::Player(gid),
            inventory: None,
        } = target
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let frontier = self
            .chunk_frontiers
            .get(&key)
            .ok_or(ApplyFailure::InvalidAction)?;
        if gid != lease.gid
            || lease.incarnation == 0
            || lease.key.0.is_nil()
            || !admitted_at.is_after(frontier.settled_through)
            || self.player_avatar_admissions.contains_key(&target)
            || self.player_avatar_admitted_at.contains_key(&target)
            || self.ground_anchors.get(&target).is_some_and(|anchor| {
                anchor.value.is_some() || anchor.settled_through.is_after(frontier.settled_through)
            })
            || self.ticks.values().any(|graph| {
                graph.actions.values().any(|node| {
                    node.indexed_targets().into_iter().any(|indexed| {
                        crate::derived_living::actor_body_target(indexed).unwrap_or(indexed)
                            == target
                    })
                })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let cut = self
            .ground_anchors
            .get(&target)
            .map_or(frontier.settled_through, |anchor| anchor.settled_through);
        self.install_ground_anchor(
            target,
            GroundAnchor {
                settled_through: cut,
                value: None,
            },
        )?;
        self.player_avatar_admissions.insert(target, lease);
        self.player_avatar_admitted_at.insert(target, admitted_at);
        Ok(())
    }

    pub fn seed_entity_admission_bootstrap(
        &mut self,
        action: &Action,
    ) -> Result<Vec<Target>, ApplyFailure> {
        action.validate()?;
        let mut targets = BTreeMap::new();
        for effect in &action.effects {
            let Effect::EntityAdmission { target, body, inventory } = effect else { continue; };
            let Target::Chunk { key, part: ChunkPart::Entity(id), inventory: None } = target
                else { return Err(ApplyFailure::InvalidAction); };
            let cut = self.chunk_frontiers.get(key)
                .ok_or(ApplyFailure::MissingStakeholder(Stakeholder::Chunk(*key)))?.settled_through;
            if *id != body.id || !action.tick.is_after(cut) { return Err(ApplyFailure::InvalidAction); }
            for target in std::iter::once(*target).chain(inventory.entries(*key, *id).map(|(target, _)| target)) {
                if targets.insert(target, cut).is_some() { return Err(ApplyFailure::InvalidAction); }
            }
        }
        if targets.is_empty() { return Err(ApplyFailure::InvalidAction); }
        for (candidate, cut) in &targets {
            if self
                .ground_anchors
                .get(candidate)
                .is_some_and(|anchor| anchor.value.is_some() || anchor.settled_through != *cut)
                || self.ticks.values().any(|graph| {
                    graph
                        .actions
                        .values()
                        .any(|node| node.indexed_targets().contains(candidate))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let created = targets
            .into_iter()
            .filter(|(target, _)| !self.ground_anchors.contains_key(target))
            .collect::<Vec<_>>();
        for (target, cut) in &created {
            self.ground_anchors.insert(
                *target,
                GroundAnchor {
                    settled_through: *cut,
                    value: None,
                },
            );
        }
        Ok(created.into_iter().map(|(target, _)| target).collect())
    }

    pub fn seed_player_inventory_bootstrap(
        &mut self,
        action: &Action,
    ) -> Result<
        (
            Vec<Target>,
            Vec<(PlayerKey, InventoryRealmId, GlobalPlayerId, u64)>,
        ),
        ApplyFailure,
    > {
        action.validate()?;
        let [
            Effect::PlayerAvatarLifecycle {
                target: body,
                intent:
                    crate::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        lease, inventory, ..
                    },
            },
        ] = action.effects.as_slice()
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let anchor = self
            .ground_anchors
            .get(body)
            .filter(|anchor| anchor.value.is_none() && action.tick.is_after(anchor.settled_through))
            .ok_or(ApplyFailure::InvalidAction)?;
        if self.player_avatar_admissions.get(body) != Some(lease)
            || self.player_avatar_admitted_at.get(body) != Some(&action.tick)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let cut = anchor.settled_through;
        let chunk = action.actor_chunk.ok_or(ApplyFailure::InvalidAction)?;
        let targets = inventory.targets(*lease).collect::<Vec<_>>();
        let mut frontiers = BTreeMap::new();
        for target in &targets {
            let ActionRealm::Inventory(Stakeholder::Player(key), realm) = target.action_realm()
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let index = (key, realm, lease.gid, lease.incarnation);
            let frontier = PlayerInventoryGroundFrontier {
                lease: *lease,
                accepted_chunk: chunk,
                domain_epoch: lease.incarnation,
                settled_through: cut,
            };
            if self
                .ground_anchors
                .get(target)
                .is_some_and(|old| old.value.is_some() || old.settled_through != cut)
                || self
                    .player_inventory_frontiers
                    .get(&index)
                    .is_some_and(|old| *old != frontier)
                || self.ticks.values().any(|graph| {
                    graph
                        .actions
                        .values()
                        .any(|node| node.indexed_targets().contains(target))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            frontiers.insert(index, frontier);
        }
        let created_frontiers = frontiers
            .keys()
            .filter(|key| !self.player_inventory_frontiers.contains_key(key))
            .copied()
            .collect();
        let created_anchors = targets
            .into_iter()
            .filter(|target| !self.ground_anchors.contains_key(target))
            .collect::<Vec<_>>();
        self.player_inventory_frontiers.extend(frontiers);
        for target in &created_anchors {
            self.ground_anchors.insert(
                *target,
                GroundAnchor {
                    settled_through: cut,
                    value: None,
                },
            );
        }
        Ok((created_anchors, created_frontiers))
    }

    pub fn install_player_avatar_admission_at(
        &mut self,
        target: Target,
        lease: crate::canonical_action::LivePlayerLease,
        admitted_at: TickStamp,
        anchor: GroundAnchor,
    ) -> Result<(), ApplyFailure> {
        if !matches!(target, Target::Chunk { part: ChunkPart::Player(gid), inventory: None, .. } if gid == lease.gid)
            || lease.incarnation == 0
            || !anchor.value.as_ref().is_none_or(|cell| {
                matches!(&cell.value, Value::Living(state) if state.belongs_to(crate::actor::ActorId::Player(lease.gid)))
            })
            || self.player_avatar_admissions.get(&target).is_some_and(|current| *current != lease)
            || self.player_avatar_admitted_at.get(&target).is_some_and(|current| *current != admitted_at)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.install_ground_anchor(target, anchor)?;
        self.player_avatar_admissions.insert(target, lease);
        self.player_avatar_admitted_at.insert(target, admitted_at);
        Ok(())
    }

    pub fn retire_player_avatar_admission(
        &mut self,
        lease: crate::canonical_action::LivePlayerLease,
    ) -> bool {
        if self
            .player_avatar_admissions
            .iter()
            .any(|(target, current)| {
                *current == lease
                    && self
                        .ground_anchors
                        .get(target)
                        .is_none_or(|anchor| anchor.value.is_some())
            })
            || self.ticks.values().any(|graph| {
                graph
                    .actions
                    .values()
                    .any(|node| node.action.player_leases.contains(&lease))
            })
        {
            return false;
        }
        self.player_avatar_admissions.retain(|target, current| {
            if *current == lease {
                self.player_avatar_admitted_at.remove(target);
                false
            } else {
                true
            }
        });
        true
    }
    pub fn retire_player_inventory_lease(
        &mut self,
        lease: crate::canonical_action::LivePlayerLease,
    ) -> bool {
        if self.ticks.values().any(|graph| {
            graph
                .actions
                .values()
                .any(|node| node.action.player_leases.contains(&lease))
        }) {
            return false;
        }
        self.player_inventory_frontiers
            .retain(|_, frontier| frontier.lease != lease);
        self.ground_anchors.retain(|target, _| {
            !matches!(target, Target::Player { lease: Some(present), .. } if *present == lease)
        });
        true
    }

    pub fn install_player_inventory_frontier(
        &mut self,
        realm: InventoryRealmId,
        frontier: PlayerInventoryGroundFrontier,
    ) -> Result<(), ApplyFailure> {
        if frontier.lease.incarnation == 0
            || frontier.domain_epoch != frontier.lease.incarnation
            || self
                .player_inventory_frontiers
                .get(&(
                    frontier.lease.key,
                    realm,
                    frontier.lease.gid,
                    frontier.lease.incarnation,
                ))
                .is_some_and(|existing| *existing != frontier)
            || self.ground_anchors.iter().any(|(target, anchor)| {
                matches!(target, Target::Player { key, lease: Some(lease), inventory: Some(id) }
                    if *key == frontier.lease.key && *lease == frontier.lease && id.action_realm() == realm)
                    && frontier
                        .settled_through
                        .distance_since(anchor.settled_through)
                        >= 32_768
            })
            || self.ticks.iter().any(|(tick, graph)| {
                let distance = tick.distance_since(frontier.settled_through);
                (distance == 0 || distance >= 32_768)
                    && graph.actions.values().any(|node| {
                        node.action.player_leases.contains(&frontier.lease)
                            && node.realms.contains(&ActionRealm::Inventory(
                                Stakeholder::Player(frontier.lease.key),
                                realm,
                            ))
                    })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.player_inventory_frontiers.insert(
            (
                frontier.lease.key,
                realm,
                frontier.lease.gid,
                frontier.lease.incarnation,
            ),
            frontier,
        );
        Ok(())
    }

    pub fn seed_player_inventory_target_anchor(
        &mut self,
        target: Target,
        lease: crate::canonical_action::LivePlayerLease,
        value: Option<Cell>,
    ) -> Result<(), ApplyFailure> {
        let Target::Player {
            key,
            lease: Some(target_lease),
            inventory: Some(_),
        } = target
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if key != lease.key || target_lease != lease {
            return Err(ApplyFailure::InvalidAction);
        }
        let ActionRealm::Inventory(Stakeholder::Player(_), realm) = target.action_realm() else {
            return Err(ApplyFailure::InvalidAction);
        };
        let frontier = self
            .player_inventory_frontiers
            .get(&(key, realm, lease.gid, lease.incarnation))
            .copied()
            .filter(|frontier| frontier.lease == lease)
            .ok_or(ApplyFailure::InvalidAction)?;
        self.install_ground_anchor(
            target,
            GroundAnchor {
                settled_through: frontier.settled_through,
                value,
            },
        )
    }

    pub fn advance_empty_ground_tick(
        &mut self,
        tick: TickStamp,
        target: Target,
        closed: bool,
    ) -> bool {
        let occupied = self.ticks.get(&tick).is_some_and(|graph| {
            graph.actions.values().any(|node| match target {
                Target::Chunk { key, .. } => {
                    node.action.stakeholders.contains(&Stakeholder::Chunk(key))
                }
                Target::Player {
                    lease: Some(lease), ..
                } => {
                    node.action.player_leases.contains(&lease)
                        && node.realms.contains(&target.action_realm())
                }
                Target::Player { .. } => false,
            })
        });
        if !closed || occupied {
            return false;
        }
        let previous = TickStamp(tick.0.wrapping_sub(1));
        let player_frontier = match target {
            Target::Player {
                key,
                lease: Some(lease),
                inventory: Some(id),
            } => {
                if lease.key != key {
                    return false;
                }
                let key = (key, id.action_realm(), lease.gid, lease.incarnation);
                let Some(frontier) = self.player_inventory_frontiers.get(&key) else {
                    return false;
                };
                if frontier.lease != lease {
                    return false;
                }
                let frontier_tick = frontier.settled_through;
                if frontier_tick != previous && frontier_tick != tick {
                    return false;
                }
                Some(key)
            }
            Target::Player { .. } => return false,
            Target::Chunk { key, .. } => {
                if self.chunk_frontiers.get(&key).is_none_or(|frontier| {
                    frontier.settled_through != previous && frontier.settled_through != tick
                }) {
                    return false;
                }
                None
            }
        };
        let Some(anchor) = self.ground_anchors.get_mut(&target) else {
            return false;
        };
        if anchor.settled_through != previous {
            return false;
        }
        anchor.settled_through = tick;
        if let Some(key) = player_frontier {
            self.player_inventory_frontiers
                .get_mut(&key)
                .unwrap()
                .settled_through = tick;
        } else if let Target::Chunk { key, .. } = target {
            self.chunk_frontiers.get_mut(&key).unwrap().settled_through = tick;
        }
        true
    }

    pub fn advance_empty_chunk_frontier(
        &mut self,
        tick: TickStamp,
        chunk: ChunkKey,
        closed: bool,
    ) -> bool {
        if !closed
            || self.ticks.get(&tick).is_some_and(|graph| {
                graph.actions.values().any(|node| {
                    node.realms
                        .iter()
                        .any(|realm| action_realm_held_by_chunk(&node.action, *realm, chunk))
                })
            })
        {
            return false;
        }
        let Some(frontier) = self.chunk_frontiers.get_mut(&chunk) else {
            return false;
        };
        if frontier.settled_through != TickStamp(tick.0.wrapping_sub(1)) {
            return false;
        }
        frontier.settled_through = tick;
        true
    }

    pub fn advance_empty_player_inventory_frontier(
        &mut self,
        tick: TickStamp,
        key: (PlayerKey, InventoryRealmId, GlobalPlayerId, u64),
        closed: bool,
    ) -> bool {
        let realm = ActionRealm::Inventory(Stakeholder::Player(key.0), key.1);
        if !closed
            || self.ticks.get(&tick).is_some_and(|graph| {
                graph.actions.values().any(|node| {
                    node.action.player_leases.iter().any(|lease| {
                        lease.key == key.0 && lease.gid == key.2 && lease.incarnation == key.3
                    }) && node.realms.contains(&realm)
                })
            })
        {
            return false;
        }
        let Some(frontier) = self.player_inventory_frontiers.get_mut(&key) else {
            return false;
        };
        if frontier.settled_through != TickStamp(tick.0.wrapping_sub(1)) {
            return false;
        }
        frontier.settled_through = tick;
        true
    }

    pub fn catch_up_chunk_target_anchor(
        &mut self,
        target: Target,
        observed_ground: Option<Cell>,
    ) -> bool {
        let Target::Chunk { key, .. } = target else {
            return false;
        };
        let Some(frontier) = self.chunk_frontiers.get(&key) else {
            return false;
        };
        let Some(anchor) = self.ground_anchors.get(&target) else {
            return false;
        };
        if anchor.value != observed_ground {
            return false;
        }
        let distance = frontier
            .settled_through
            .distance_since(anchor.settled_through);
        if distance >= 32_768
            || self.ticks.iter().any(|(tick, graph)| {
                let from_anchor = tick.distance_since(anchor.settled_through);
                let through_frontier = frontier.settled_through.distance_since(*tick);
                from_anchor > 0
                    && from_anchor < 32_768
                    && through_frontier < 32_768
                    && graph.actions.values().any(|node| {
                        node.realms
                            .iter()
                            .any(|realm| action_realm_held_by_chunk(&node.action, *realm, key))
                    })
            })
        {
            return false;
        }
        self.ground_anchors.insert(
            target,
            GroundAnchor {
                settled_through: frontier.settled_through,
                value: observed_ground,
            },
        );
        true
    }

    pub fn catch_up_player_target_anchor(
        &mut self,
        target: Target,
        observed_ground: Option<Cell>,
    ) -> bool {
        let Target::Player {
            key,
            lease: Some(lease),
            inventory: Some(id),
        } = target
        else {
            return false;
        };
        let realm = ActionRealm::Inventory(Stakeholder::Player(key), id.action_realm());
        let Some(frontier) = self.player_inventory_frontiers.get(&(
            key,
            id.action_realm(),
            lease.gid,
            lease.incarnation,
        )) else {
            return false;
        };
        let Some(anchor) = self.ground_anchors.get(&target) else {
            return false;
        };
        if anchor.value != observed_ground {
            return false;
        }
        let distance = frontier
            .settled_through
            .distance_since(anchor.settled_through);
        if distance >= 32_768
            || self.ticks.iter().any(|(tick, graph)| {
                let from_anchor = tick.distance_since(anchor.settled_through);
                let through_frontier = frontier.settled_through.distance_since(*tick);
                from_anchor > 0
                    && from_anchor < 32_768
                    && through_frontier < 32_768
                    && graph.actions.values().any(|node| {
                        node.action.player_leases.contains(&lease) && node.realms.contains(&realm)
                    })
            })
        {
            return false;
        }
        self.ground_anchors.insert(
            target,
            GroundAnchor {
                settled_through: frontier.settled_through,
                value: observed_ground,
            },
        );
        true
    }

    pub fn install_chunk_frontier(
        &mut self,
        chunk: ChunkKey,
        frontier: ChunkGroundFrontier,
    ) -> Result<(), ApplyFailure> {
        if self
            .chunk_frontiers
            .get(&chunk)
            .is_some_and(|current| *current != frontier)
            || self.ground_anchors.iter().any(|(target, anchor)| {
                matches!(target, Target::Chunk { key, .. } if *key == chunk)
                    && frontier
                        .settled_through
                        .distance_since(anchor.settled_through)
                        >= 32_768
            })
            || self.ticks.iter().any(|(tick, graph)| {
                let distance = tick.distance_since(frontier.settled_through);
                (distance == 0 || distance >= 32_768)
                    && graph.actions.values().any(|node| {
                        node.realms
                            .iter()
                            .any(|realm| action_realm_held_by_chunk(&node.action, *realm, chunk))
                    })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.chunk_frontiers.insert(chunk, frontier);
        Ok(())
    }

    pub fn seed_chunk_target_anchor(
        &mut self,
        target: Target,
        chunk: ChunkKey,
        value: Option<Cell>,
    ) -> Result<(), ApplyFailure> {
        if !target.action_realm().held_by_chunk(chunk) {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(body) = crate::derived_living::actor_body_target(target) {
            let anchor = self
                .ground_anchors
                .get(&body)
                .ok_or(ApplyFailure::InvalidAction)?;
            let projected = anchor
                .value
                .as_ref()
                .and_then(|cell| crate::derived_living::actor_facet_cell(cell, target));
            return self.install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: anchor.settled_through,
                    value: projected,
                },
            );
        }
        let frontier = self
            .chunk_frontiers
            .get(&chunk)
            .copied()
            .ok_or(ApplyFailure::InvalidAction)?;
        self.install_ground_anchor(
            target,
            GroundAnchor {
                settled_through: frontier.settled_through,
                value,
            },
        )
    }

    pub fn merge_chunk_snapshot(
        &mut self,
        chunk: ChunkKey,
        cut: TickStamp,
        graphs: &[TickGraphSnapshot],
        anchors: &[(Target, GroundAnchor)],
        observed_ground: &BTreeMap<Target, Option<Cell>>,
    ) -> Result<BTreeSet<ActionRef>, ApplyFailure> {
        self.merge_chunk_snapshot_with_context(chunk, cut, graphs, anchors, observed_ground, None)
    }

    pub fn merge_chunk_snapshot_with_context(
        &mut self,
        chunk: ChunkKey,
        cut: TickStamp,
        graphs: &[TickGraphSnapshot],
        anchors: &[(Target, GroundAnchor)],
        observed_ground: &BTreeMap<Target, Option<Cell>>,
        proof_context: Option<(
            &crate::entity_history::EntityBindingProofContext,
            &crate::realm_completion::RealmCompletionLedger,
        )>,
    ) -> Result<BTreeSet<ActionRef>, ApplyFailure> {
        if project_chunk_graph_snapshot_ground_with_context(
            chunk,
            cut,
            graphs,
            anchors,
            proof_context,
        )? != *observed_ground
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut candidate = self.clone();
        let graph_targets = graphs
            .iter()
            .flat_map(|graph| graph.nodes.iter())
            .flat_map(|node| {
                node.indexed_targets()
                    .into_iter()
                    .chain(
                        node.actor_binding
                            .iter()
                            .map(|binding| binding.resolved_source),
                    )
                    .filter(|target| action_target_held_by_chunk(&node.action, *target, chunk))
            })
            .collect::<BTreeSet<_>>();
        for (target, anchor) in anchors {
            if !graph_targets.contains(target) {
                let frontier_tick = match target {
                    Target::Chunk { key, .. } if *key == chunk => candidate
                        .chunk_frontiers
                        .get(&chunk)
                        .map(|frontier| frontier.settled_through),
                    Target::Player {
                        key,
                        lease: Some(lease),
                        inventory: Some(id),
                    } => {
                        let mut frontiers = candidate.player_inventory_frontiers.iter().filter_map(
                            |((player, realm, _, _), frontier)| {
                                (*player == *key
                                    && frontier.lease == *lease
                                    && *realm == id.action_realm()
                                    && frontier.accepted_chunk == chunk)
                                    .then_some(frontier.settled_through)
                            },
                        );
                        let first = frontiers.next();
                        if frontiers.next().is_some() {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        first
                    }
                    _ => None,
                }
                .ok_or(ApplyFailure::InvalidAction)?;
                if frontier_tick.distance_since(anchor.settled_through) >= 32_768 {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            match candidate.ground_anchors.get(target) {
                Some(existing) if existing != anchor => return Err(ApplyFailure::InvalidAction),
                Some(_) => {}
                None => candidate.install_ground_anchor(*target, anchor.clone())?,
            }
        }
        let mut changed = BTreeSet::new();
        for snapshot in graphs {
            let fresh = snapshot
                .nodes
                .iter()
                .filter_map(|node| {
                    let existing = candidate
                        .ticks
                        .get(&snapshot.tick)
                        .and_then(|graph| graph.actions.get(&node.action.id));
                    match existing {
                        Some(existing) if existing.action == node.action => None,
                        Some(_) => Some(Err(ApplyFailure::InvalidAction)),
                        None => Some(Ok(node.action.clone())),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            changed.extend(
                candidate
                    .insert_collected(snapshot.tick, fresh)?
                    .into_iter()
                    .map(|id| ActionRef {
                        tick: snapshot.tick,
                        id,
                    }),
            );
            for imported in &snapshot.nodes {
                if let Some(binding) = &imported.actor_binding {
                    candidate
                        .ticks
                        .get_mut(&snapshot.tick)
                        .ok_or(ApplyFailure::InvalidAction)?
                        .configure_actor_binding(imported.action.id, binding.clone())?;
                    candidate.by_target.insert_reference(
                        binding.resolved_source,
                        ActionRef {
                            tick: snapshot.tick,
                            id: imported.action.id,
                        },
                    );
                }
                let graph = candidate
                    .ticks
                    .get_mut(&snapshot.tick)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let current = graph
                    .actions
                    .get(&imported.action.id)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if current.actor_binding != imported.actor_binding {
                    return Err(ApplyFailure::InvalidAction);
                }
                if !current.applied_realms.is_empty()
                    && (current.wrapper != imported.wrapper
                        || current.factual != imported.factual
                        || current.semantic_order != imported.semantic_order)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                if current.local_result.is_some()
                    && current.factual != imported.factual
                    && current.applied_realms.is_empty()
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let local_projection = imported.factual.result.as_ref().map(|result| {
                    let mut result = result.clone();
                    result.writes.retain(|write| {
                        imported
                            .applied_realms
                            .contains(&write.target.action_realm())
                    });
                    result
                });
                if !imported.applied_realms.is_empty() {
                    match (&current.local_result, &local_projection) {
                        (Some(existing), Some(incoming))
                            if incoming.writes.iter().any(|write| {
                                existing
                                    .writes
                                    .iter()
                                    .find(|stored| stored.target == write.target)
                                    .is_some_and(|stored| stored != write)
                            }) =>
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        (Some(_), None) => return Err(ApplyFailure::InvalidAction),
                        _ => {}
                    }
                }
                if current.wrapper != imported.wrapper || current.factual != imported.factual {
                    changed.extend(
                        graph
                            .replace_factual(
                                imported.action.id,
                                imported.wrapper,
                                imported.factual.clone(),
                            )?
                            .into_iter()
                            .map(|id| ActionRef {
                                tick: snapshot.tick,
                                id,
                            }),
                    );
                }
                let current = graph
                    .actions
                    .get_mut(&imported.action.id)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if !imported.applied_realms.is_empty() {
                    current
                        .applied_realms
                        .extend(imported.applied_realms.iter().copied());
                    current.semantic_order = imported.semantic_order;
                    current.semantic_generation = Some(current.generation);
                    current.local_order = imported.semantic_order;
                    current.local_result = match (current.local_result.take(), local_projection) {
                        (Some(mut existing), Some(incoming)) => {
                            for write in incoming.writes {
                                if !existing
                                    .writes
                                    .iter()
                                    .any(|stored| stored.target == write.target)
                                {
                                    existing.writes.push(write);
                                }
                            }
                            existing.writes.sort_by_key(|write| write.target);
                            Some(existing)
                        }
                        (None, incoming) => incoming,
                        (existing, None) => existing,
                    };
                    current.local_generation = Some(current.generation);
                    current.stable = true;
                }
            }
            candidate
                .ticks
                .get_mut(&snapshot.tick)
                .ok_or(ApplyFailure::InvalidAction)?
                .rebuild_grounded_targets();
        }
        if candidate.ticks.values().any(|graph| {
            graph
                .actions
                .values()
                .any(|node| !node.applied_realms.is_empty() && !node.stable)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        *self = candidate;
        Ok(changed)
    }

    #[must_use]
    pub fn snapshot_for_chunk(&self, chunk: ChunkKey) -> Vec<TickGraphSnapshot> {
        self.ticks
            .iter()
            .filter(|(_, graph)| {
                graph.actions.values().any(|node| {
                    node.semantic_action()
                        .stakeholders
                        .binary_search(&Stakeholder::Chunk(chunk))
                        .is_ok()
                })
            })
            .map(|(tick, graph)| TickGraphSnapshot {
                tick: *tick,
                nodes: graph
                    .actions
                    .values()
                    .map(|node| GraphNodeSnapshot {
                        action: node.action.clone(),
                        actor_binding: node.actor_binding.clone(),
                        factual: node.factual.clone(),
                        wrapper: node.wrapper,
                        semantic_order: node.semantic_order,
                        required_realms: node.realms.clone(),
                        applied_realms: node
                            .applied_realms
                            .iter()
                            .copied()
                            .filter(|realm| {
                                action_realm_held_by_chunk(node.semantic_action(), *realm, chunk)
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect()
    }

    pub fn snapshot_for_chunk_at_cut(
        &self,
        chunk: ChunkKey,
        cut: TickStamp,
    ) -> Result<Vec<TickGraphSnapshot>, ApplyFailure> {
        let snapshots = self.snapshot_for_chunk(chunk);
        let anchors = self.snapshot_anchors_for_graphs(chunk, &snapshots);
        if anchors
            .iter()
            .any(|(_, anchor)| anchor.settled_through.is_after(cut))
            || snapshots.iter().any(|graph| {
                graph.tick.is_after(cut)
                    && graph
                        .nodes
                        .iter()
                        .any(|node| !node.applied_realms.is_empty())
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut snapshots = snapshots
            .into_iter()
            .filter(|graph| !graph.tick.is_after(cut))
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|graph| std::cmp::Reverse(cut.distance_since(graph.tick)));
        Ok(snapshots)
    }

    #[must_use]
    pub fn snapshot_anchors_for_graphs(
        &self,
        chunk: ChunkKey,
        graphs: &[TickGraphSnapshot],
    ) -> Vec<(Target, GroundAnchor)> {
        let mut targets = graphs
            .iter()
            .flat_map(|graph| graph.nodes.iter())
            .flat_map(|node| {
                node.indexed_targets()
                    .into_iter()
                    .chain(
                        node.actor_binding
                            .iter()
                            .map(|binding| binding.resolved_source),
                    )
                    .filter(|target| action_target_held_by_chunk(&node.action, *target, chunk))
            })
            .collect::<BTreeSet<_>>();
        targets.extend(self.ground_anchors.keys().copied().filter(|target| {
            if let Target::Chunk { key, .. } = target {
                return *key == chunk && self.chunk_frontiers.contains_key(key);
            }
            let Target::Player {
                key,
                lease: Some(lease),
                inventory: Some(id),
            } = target
            else {
                return false;
            };
            self.player_inventory_frontiers
                .iter()
                .any(|((player, realm, _, _), frontier)| {
                    *player == *key
                        && frontier.lease == *lease
                        && *realm == id.action_realm()
                        && frontier.accepted_chunk == chunk
                })
        }));
        targets
            .into_iter()
            .filter_map(|target| {
                self.ground_anchors
                    .get(&target)
                    .cloned()
                    .map(|anchor| (target, anchor))
            })
            .collect()
    }

    pub fn retire_grounded_history(&mut self, current: TickStamp) {
        std::sync::Arc::make_mut(&mut self.grounded_ticks)
            .retain(|tick| current.distance_since(*tick) < 32_768);
    }

    pub fn mark_ground_realm_applied(
        &mut self,
        reference: ActionRef,
        realm: ActionRealm,
    ) -> Result<(), ApplyFailure> {
        let graph = self
            .ticks
            .get_mut(&reference.tick)
            .ok_or(ApplyFailure::InvalidAction)?;
        let node = graph
            .actions
            .get_mut(&reference.id)
            .ok_or(ApplyFailure::InvalidAction)?;
        if !node.stable || !node.realms.contains(&realm) {
            return Err(ApplyFailure::InvalidAction);
        }
        let was_grounded = node.is_ground_applied();
        node.applied_realms.insert(realm);
        if !was_grounded && node.is_ground_applied() {
            graph.index_grounded_action(reference.id);
        }
        Ok(())
    }

    pub fn ground_drain_targets(&self, tick: TickStamp) -> Option<BTreeSet<Target>> {
        let mut targets = self
            .ticks
            .get(&tick)?
            .actions
            .values()
            .flat_map(|node| node.indexed_targets().into_iter())
            .flat_map(effect_location_aliases)
            .filter(|target| {
                !matches!(
                    target,
                    Target::Chunk {
                        part: ChunkPart::BlockSection(_),
                        inventory: None,
                        ..
                    }
                ) || self.ground_anchors.contains_key(target)
            })
            .collect::<BTreeSet<_>>();
        let bodies = targets
            .iter()
            .filter_map(|target| crate::derived_living::actor_body_target(*target))
            .collect::<BTreeSet<_>>();
        targets.extend(bodies);
        let facets = self
            .ground_anchors
            .keys()
            .filter(|target| {
                crate::derived_living::actor_body_target(**target)
                    .is_some_and(|body| targets.contains(&body))
            })
            .copied()
            .collect::<Vec<_>>();
        targets.extend(facets);
        Some(targets)
    }

    pub fn drain_grounded_tick(
        &mut self,
        tick: TickStamp,
        closed: bool,
        observed_ground: &BTreeMap<Target, Option<Cell>>,
    ) -> Option<Vec<Action>> {
        let graph = self.ticks.get(&tick)?;
        if !closed
            || graph.actions.is_empty()
            || graph.actions.values().any(|node| !node.is_ground_applied())
        {
            return None;
        }
        let targets = self.ground_drain_targets(tick)?;
        if observed_ground.keys().copied().collect::<BTreeSet<_>>() != targets {
            return None;
        }
        for target in &targets {
            if self.ground_anchors.get(target)?.settled_through != TickStamp(tick.0.wrapping_sub(1))
            {
                return None;
            }
            let expected = self
                .symbolic_anchor_for(TickStamp(tick.0.wrapping_add(1)), *target)
                .ok()?;
            if observed_ground.get(target) != Some(&expected) {
                return None;
            }
        }
        let mut player_realms = BTreeSet::new();
        let mut chunks = BTreeSet::new();
        for node in graph.actions.values() {
            chunks.extend(
                node.action
                    .stakeholders
                    .iter()
                    .chain(&node.semantic_action().stakeholders)
                    .filter_map(|stakeholder| match stakeholder {
                        Stakeholder::Chunk(chunk) => Some(*chunk),
                        Stakeholder::Player(_) => None,
                    }),
            );
            for realm in &node.realms {
                let ActionRealm::Inventory(Stakeholder::Player(key), inventory_realm) = realm
                else {
                    continue;
                };
                let lease = node
                    .action
                    .player_leases
                    .iter()
                    .find(|lease| lease.key == *key)?;
                let accepted_chunk = node
                    .action
                    .player_chunks
                    .iter()
                    .find_map(|(player, chunk)| (*player == *key).then_some(*chunk))?;
                let frontier_key = (*key, *inventory_realm, lease.gid, lease.incarnation);
                let frontier = self.player_inventory_frontiers.get(&frontier_key)?;
                if frontier.lease != *lease
                    || frontier.accepted_chunk != accepted_chunk
                    || frontier.settled_through != TickStamp(tick.0.wrapping_sub(1))
                {
                    return None;
                }
                player_realms.insert(frontier_key);
            }
        }
        if chunks.iter().any(|chunk| {
            self.chunk_frontiers.get(chunk).is_none_or(|frontier| {
                frontier.settled_through != TickStamp(tick.0.wrapping_sub(1))
            })
        }) {
            return None;
        }
        let ids = graph.actions.keys().copied().collect::<Vec<_>>();
        let actions = ids
            .into_iter()
            .map(|id| self.remove_grounded(ActionRef { tick, id }))
            .collect::<Option<Vec<_>>>()?;
        for (target, value) in observed_ground {
            self.ground_anchors.insert(
                *target,
                GroundAnchor {
                    settled_through: tick,
                    value: value.clone(),
                },
            );
        }
        for key in player_realms {
            self.player_inventory_frontiers
                .get_mut(&key)?
                .settled_through = tick;
        }
        for chunk in chunks {
            self.chunk_frontiers.get_mut(&chunk)?.settled_through = tick;
        }
        std::sync::Arc::make_mut(&mut self.grounded_ticks).insert(tick);
        Some(actions)
    }

    pub fn symbolic_anchor_for(
        &self,
        tick: TickStamp,
        target: Target,
    ) -> Result<Option<Cell>, SymbolicAnchorFailure> {
        let anchor = self
            .ground_anchors
            .get(&target)
            .ok_or(SymbolicAnchorFailure::MissingGround(target))?;
        let span = tick.distance_since(anchor.settled_through);
        if span == 0 || span >= 32_768 {
            return Err(SymbolicAnchorFailure::InvalidTick);
        }
        let mut references = self
            .by_target
            .get(&target)
            .into_iter()
            .flat_map(|references| references.keys())
            .filter_map(|reference| {
                let distance = reference.tick.distance_since(anchor.settled_through);
                (distance > 0 && distance < span).then_some((*reference, distance))
            })
            .collect::<Vec<_>>();
        references.sort_by_key(|(reference, distance)| {
            let node = &self.ticks[&reference.tick].actions[&reference.id];
            (
                *distance,
                node.semantic_order.unwrap_or_default(),
                reference.id,
            )
        });
        let mut value = anchor.value.clone();
        for (reference, _) in references {
            let node = &self.ticks[&reference.tick].actions[&reference.id];
            if !node.stable {
                return Err(SymbolicAnchorFailure::Unsettled(reference));
            }
            let Some(result) = &node.factual.result else {
                continue;
            };
            if crate::derived_living::actor_body_target(target).is_some() {
                value = crate::derived_living::project_facet_writes(
                    target,
                    value.as_ref(),
                    &result.writes,
                )
                .map_err(|_| SymbolicAnchorFailure::Inconsistent(reference, target))?;
                continue;
            }
            if value
                .as_ref()
                .is_some_and(|cell| matches!(&cell.value, Value::Living(_) | Value::Entity(_)))
            {
                value = crate::derived_living::project_body_writes(
                    target,
                    value.as_ref(),
                    &result.writes,
                )
                .map_err(|_| SymbolicAnchorFailure::Inconsistent(reference, target))?;
                continue;
            }
            if matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::BlockSection(_),
                    inventory: None,
                    ..
                }
            ) {
                let before = value
                    .as_ref()
                    .ok_or(SymbolicAnchorFailure::Inconsistent(reference, target))?;
                value = Some(
                    crate::block_section::project_writes(target, before, &result.writes)
                        .map_err(|_| SymbolicAnchorFailure::Inconsistent(reference, target))?,
                );
                continue;
            }
            if let Some(write) = result.writes.iter().find(|write| write.target == target) {
                if write.before != value {
                    return Err(SymbolicAnchorFailure::Inconsistent(reference, target));
                }
                value = write.after.clone();
            }
        }
        Ok(value)
    }

    pub fn cascade_to_current(
        &self,
        current_tick: TickStamp,
        seeds: impl IntoIterator<Item = ActionRef>,
    ) -> Result<Vec<(TickStamp, BTreeSet<ActionRef>)>, ApplyFailure> {
        let mut pending = seeds.into_iter().collect::<Vec<_>>();
        let mut affected = BTreeMap::<TickStamp, BTreeSet<ActionRef>>::new();
        while let Some(reference) = pending.pop() {
            if current_tick.distance_since(reference.tick) >= 32_768 {
                return Err(ApplyFailure::InvalidAction);
            }
            if !self
                .ticks
                .get(&reference.tick)
                .is_some_and(|graph| graph.actions.contains_key(&reference.id))
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if affected
                .entry(reference.tick)
                .or_default()
                .insert(reference)
            {
                pending.extend(
                    self.later_dependents(reference)
                        .into_iter()
                        .filter(|dependent| current_tick.distance_since(dependent.tick) < 32_768),
                );
            }
        }
        let mut affected = affected.into_iter().collect::<Vec<_>>();
        affected.sort_by_key(|(tick, _)| std::cmp::Reverse(current_tick.distance_since(*tick)));
        Ok(affected)
    }

    pub fn install_ground_anchor(
        &mut self,
        target: Target,
        anchor: GroundAnchor,
    ) -> Result<(), ApplyFailure> {
        if !target.is_valid()
            || anchor.value.as_ref().is_some_and(|cell| matches!(&cell.value, crate::canonical_action::Value::Entity(body) if !body.valid_at(anchor.settled_through)))
            || self.ground_anchors.get(&target).is_some_and(|current| {
                if anchor.settled_through == current.settled_through {
                    anchor.value != current.value
                } else {
                    !anchor.settled_through.is_after(current.settled_through)
                }
            })
            || self.by_target.get(&target).is_some_and(|references| {
                references
                    .keys()
                    .any(|reference| !reference.tick.is_after(anchor.settled_through))
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.ground_anchors.insert(target, anchor);
        Ok(())
    }

    #[must_use]
    pub fn later_dependents(&self, source: ActionRef) -> Vec<ActionRef> {
        let mut dependents = self
            .declared_dependents
            .get(&source)
            .into_iter()
            .flatten()
            .filter(|candidate| {
                let distance = candidate.tick.distance_since(source.tick);
                distance != 0 && distance < 32_768
            })
            .copied()
            .collect::<BTreeSet<_>>();
        if let Some(action) = self
            .ticks
            .get(&source.tick)
            .and_then(|graph| graph.actions.get(&source.id))
        {
            for target in action
                .action
                .targets()
                .into_iter()
                .chain(action.action.counterfactual_conditions.iter().copied())
                .chain(action.action.counterfactual_effects.iter().copied())
            {
                dependents.extend(
                    self.by_target
                        .get(&target)
                        .into_iter()
                        .flat_map(|references| references.keys())
                        .filter(|candidate| {
                            let distance = candidate.tick.distance_since(source.tick);
                            distance != 0 && distance < 32_768
                        })
                        .copied(),
                );
            }
        }
        let mut dependents = dependents.into_iter().collect::<Vec<_>>();
        dependents
            .sort_by_key(|candidate| (candidate.tick.distance_since(source.tick), candidate.id));
        dependents
    }

    pub fn remove_grounded(&mut self, reference: ActionRef) -> Option<Action> {
        let graph = self.ticks.get_mut(&reference.tick)?;
        let indexed_targets = graph.actions.get(&reference.id)?.indexed_targets();
        let action = graph.remove(reference.id)?;
        for actor in affected_actors(&action) {
            if let Some(references) = self.by_actor.get_mut(&actor) {
                references.remove(&reference);
                if references.is_empty() { self.by_actor.remove(&actor); }
            }
        }
        for chunk in self.action_chunk_index.remove(&reference).unwrap_or_default() {
            if let Some(references) = self.by_chunk.get_mut(&chunk) {
                references.remove(&reference);
                if references.is_empty() { self.by_chunk.remove(&chunk); }
            }
        }
        for target in indexed_targets
            .into_iter()
            .flat_map(effect_location_aliases)
        {
            self.by_target.remove_action(target, reference);
        }
        for dependency in &action.dependencies {
            if let Some(dependents) = self.declared_dependents.get_mut(dependency) {
                dependents.remove(&reference);
                if dependents.is_empty() {
                    self.declared_dependents.remove(dependency);
                }
            }
        }
        self.declared_dependents.remove(&reference);
        if graph.actions.is_empty() {
            self.ticks.remove(&reference.tick);
        }
        Some(action)
    }

    pub fn insert_collected(
        &mut self,
        tick: TickStamp,
        actions: impl IntoIterator<Item = Action>,
    ) -> Result<BTreeSet<ActionId>, ApplyFailure> {
        if self.grounded_ticks.contains(&tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut incoming = BTreeMap::new();
        for action in actions {
            if action.tick != tick || action.validate().is_err() {
                return Err(ApplyFailure::InvalidAction);
            }
            for target in action
                .targets()
                .into_iter()
                .chain(action.counterfactual_conditions.iter().copied())
                .chain(action.counterfactual_effects.iter().copied())
            {
                if let Target::Chunk {
                    part: ChunkPart::Player(gid),
                    inventory: None,
                    ..
                } = target
                {
                    let Some(lease) = self.player_avatar_admissions.get(&target) else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    let Some(anchor) = self.ground_anchors.get(&target) else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if lease.gid != gid
                        || !action.player_leases.contains(lease)
                        || !tick.is_after(anchor.settled_through)
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
            if incoming
                .insert(action.id, action.clone())
                .is_some_and(|prior| prior != action)
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut actions = Vec::new();
        for action in incoming.into_values() {
            match self
                .ticks
                .get(&tick)
                .and_then(|graph| graph.actions.get(&action.id))
            {
                Some(existing) if existing.action == action => {}
                Some(_) => return Err(ApplyFailure::InvalidAction),
                None => {
                    if action
                        .stakeholders
                        .iter()
                        .any(|stakeholder| match stakeholder {
                            Stakeholder::Chunk(chunk) => {
                                self.chunk_frontiers.get(chunk).is_some_and(|frontier| {
                                    frontier.settled_through.distance_since(tick) < 32_768
                                })
                            }
                            Stakeholder::Player(_) => false,
                        })
                        || action
                            .targets()
                            .into_iter()
                            .chain(action.counterfactual_conditions.iter().copied())
                            .chain(action.counterfactual_effects.iter().copied())
                            .any(|target| {
                                self.ground_anchors.get(&target).is_some_and(|anchor| {
                                    anchor.settled_through.distance_since(tick) < 32_768
                                }) || match target {
                                    Target::Chunk { key, .. } => {
                                        self.chunk_frontiers.get(&key).is_some_and(|frontier| {
                                            frontier.settled_through.distance_since(tick) < 32_768
                                        })
                                    }
                                    Target::Player {
                                        key,
                                        lease: Some(lease),
                                        inventory: Some(id),
                                    } => {
                                        action.player_leases.contains(&lease) && {
                                            self.player_inventory_frontiers
                                                .get(&(
                                                    key,
                                                    id.action_realm(),
                                                    lease.gid,
                                                    lease.incarnation,
                                                ))
                                                .is_some_and(|frontier| {
                                                    frontier.settled_through.distance_since(tick)
                                                        < 32_768
                                                })
                                        }
                                    }
                                    Target::Player {
                                        inventory: None, ..
                                    }
                                    | Target::Player {
                                        lease: None,
                                        inventory: Some(_),
                                        ..
                                    } => false,
                                }
                            })
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    actions.push(action);
                }
            }
        }
        if let Some(graph) = self.ticks.get(&tick) {
            if actions.iter().any(|action| {
                action
                    .targets()
                    .into_iter()
                    .chain(action.counterfactual_conditions.iter().copied())
                    .chain(action.counterfactual_effects.iter().copied())
                    .any(|target| {
                        graph
                            .grounded_targets
                            .get(&target)
                            .is_some_and(|posting| !posting.is_empty())
                    })
                    || action.dependencies.iter().any(|dependency| {
                        dependency.tick == tick
                            && graph
                                .actions
                                .get(&dependency.id)
                                .is_some_and(GraphAction::is_ground_applied)
                    })
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        // Check every fallible graph-insert rule before mutating any posting.
        // Looking up only matching avatar locations keeps intake proportional
        // to actual conflicts rather than cloning the growing tick graph for
        // every arriving batch.
        let mut incoming_avatars = BTreeMap::<Target, Vec<&Action>>::new();
        for action in &actions {
            if action
                .dependencies
                .iter()
                .any(|dependency| dependency.tick == tick && dependency.id == action.id)
            {
                return Err(ApplyFailure::InvalidAction);
            }
            for target in avatar_effect_targets(action) {
                if let Some(existing) = self
                    .ticks
                    .get(&tick)
                    .and_then(|graph| graph.realms.get(&target.action_realm()))
                    .and_then(|realm| realm.factual_effects.get(&target))
                {
                    for previous in existing.keys() {
                        if let Some(prior) = self.ticks[&tick].actions.get(previous)
                            && avatar_effect_targets(&prior.action).contains(&target)
                        {
                            validate_avatar_effect_pair(action, &prior.action)?;
                        }
                    }
                }
                for prior in incoming_avatars.entry(target).or_default().iter() {
                    validate_avatar_effect_pair(action, prior)?;
                }
                incoming_avatars.get_mut(&target).unwrap().push(action);
            }
        }
        let graph = self.ticks.entry(tick).or_default();
        let mut connected = BTreeSet::new();
        for action in &actions {
            connected.extend(
                graph
                    .insert(action.clone())
                    .expect("preflighted tick graph insertion changed without external mutation"),
            );
        }
        for action in actions {
            let reference = ActionRef {
                tick,
                id: action.id,
            };
            for actor in affected_actors(&action) {
                self.by_actor.entry(actor).or_default().insert(reference, ());
            }
            let chunks = affected_chunks(&action);
            for &chunk in &chunks {
                self.by_chunk.entry(chunk).or_default().insert(reference, ());
            }
            self.action_chunk_index.insert(reference, chunks);
            for target in action
                .targets()
                .into_iter()
                .chain(action.counterfactual_conditions.iter().copied())
                .chain(action.counterfactual_effects.iter().copied())
            {
                self.by_target.insert_reference(target, reference);
            }
            for target in action
                .effects
                .iter()
                .flat_map(indexed_effect_targets)
                .chain(
                    action
                        .counterfactual_effects
                        .iter()
                        .copied()
                        .flat_map(effect_location_aliases),
                )
            {
                self.by_target.insert_reference(target, reference);
            }
            for dependency in &action.dependencies {
                self.declared_dependents
                    .entry(*dependency)
                    .or_default()
                    .insert(reference);
            }
        }
        Ok(connected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::{
        ActionClass, ActionKind, ClientOrigin, Condition, Effect, Expected, UndoProcedure, Value,
    };
    use crate::identity::ServerId;
    use crate::protocol::{BlockPos, WorldId};

    fn block(chunk_x: i32, x: i32) -> Target {
        Target::Chunk {
            key: ChunkKey::new(WorldId::new(0, 0), chunk_x, 0),
            part: ChunkPart::Block(BlockPos { x, y: 64, z: 0 }),
            inventory: None,
        }
    }

    #[test]
    fn avatar_bootstrap_seeds_absence_before_explicit_admission() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let lease = crate::canonical_action::LivePlayerLease {
            key: crate::canonical_action::PlayerKey(uuid::Uuid::from_u128(7)),
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(4),
            },
            incarnation: 3,
        };
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let mut graphs = ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(2),
                },
            )
            .unwrap();
        graphs
            .seed_player_avatar_bootstrap(target, lease, TickStamp(6))
            .unwrap();
        assert_eq!(
            graphs.ground_anchors[&target],
            GroundAnchor {
                settled_through: TickStamp(2),
                value: None
            }
        );
        assert_eq!(graphs.player_avatar_admitted_at[&target], TickStamp(6));
        assert!(
            graphs
                .seed_player_avatar_bootstrap(target, lease, TickStamp(6))
                .is_err()
        );
        graphs.ground_anchors.insert(
            target,
            GroundAnchor {
                settled_through: TickStamp(4),
                value: None,
            },
        );
        assert!(graphs.retire_player_avatar_admission(lease));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(4));
        graphs
            .chunk_frontiers
            .get_mut(&chunk)
            .unwrap()
            .settled_through = TickStamp(5);
        let relog = crate::canonical_action::LivePlayerLease {
            incarnation: 4,
            ..lease
        };
        graphs
            .seed_player_avatar_bootstrap(target, relog, TickStamp(6))
            .unwrap();
        assert_eq!(
            graphs.ground_anchors[&target],
            GroundAnchor {
                settled_through: TickStamp(4),
                value: None
            }
        );
        assert_eq!(graphs.player_avatar_admissions[&target], relog);
        let mut occupied = ActionGraphs::default();
        occupied
            .install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(2),
                },
            )
            .unwrap();
        occupied.ground_anchors.insert(
            target,
            GroundAnchor {
                settled_through: TickStamp(2),
                value: Some(Cell {
                    revision: 1,
                    value: Value::Block(1),
                }),
            },
        );
        assert!(
            occupied
                .seed_player_avatar_bootstrap(target, lease, TickStamp(6))
                .is_err()
        );
        assert!(occupied.player_avatar_admissions.is_empty());
    }

    #[test]
    fn variable_transform_indexes_only_actual_factual_writes() {
        use crate::canonical_inventory::{
            InventorySlotBinding, ItemTransformIntent, ItemTransformOperation,
        };
        use crate::inventory::{InvLoc, InventorySlotRef, InventoryTarget};
        let key = crate::canonical_action::PlayerKey(uuid::Uuid::from_bytes([8; 16]));
        let lease = crate::canonical_action::LivePlayerLease {
            key,
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(4),
            },
            incarnation: 3,
        };
        let binding = |slot| InventorySlotBinding {
            slot: InventorySlotRef::new(
                InventoryTarget::LeasedPlayer {
                    key: key.0,
                    player: lease.gid,
                    incarnation: lease.incarnation,
                },
                InvLoc::new(0, slot),
            ),
            target: Target::Player {
                key,
                lease: Some(lease),
                inventory: Some(InventoryId::Player { inv: 0, slot }),
            },
            max_stack: 64,
        };
        let source = binding(0);
        let first = binding(1);
        let second = binding(2);
        let effect = Effect::TransformItems {
            intent: ItemTransformIntent {
                source: source.clone(),
                item: 1,
                components: vec![0],
                operation: ItemTransformOperation::Consume { count: 1 },
                remainder_destinations: vec![first.clone(), second.clone()],
            },
        };
        let mut result = AppliedAction {
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence: 1,
                family: 2,
            },
            tick: TickStamp(4),
            actor_chunk: None,
            view: TruthView::Local,
            writes: vec![AppliedWrite {
                target: source.target,
                before: None,
                after: None,
            }],
        };
        assert_eq!(
            indexed_factual_effect_targets(&effect, Some(&result)).collect::<BTreeSet<_>>(),
            BTreeSet::from([source.target])
        );
        assert_eq!(
            indexed_effect_targets(&effect).collect::<BTreeSet<_>>(),
            BTreeSet::from([source.target, first.target, second.target])
        );
        result.writes.push(AppliedWrite {
            target: second.target,
            before: None,
            after: None,
        });
        assert_eq!(
            effect_groups(std::slice::from_ref(&effect), Some(&result))
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([source.target, second.target])
        );
        assert!(
            indexed_factual_effect_targets(&effect, None)
                .next()
                .is_none()
        );
    }

    #[test]
    fn block_entity_anchor_projects_through_chunk_transfer() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::BlockEntity(BlockPos { x: 1, y: 64, z: 2 }),
            inventory: None,
        };
        let cell = Cell {
            revision: 4,
            value: Value::BlockEntity(vec![10, 0, 0, 0]),
        };
        let anchors = vec![(
            target,
            GroundAnchor {
                settled_through: TickStamp(12),
                value: Some(cell.clone()),
            },
        )];
        assert_eq!(
            project_chunk_graph_snapshot_ground(chunk, TickStamp(12), &[], &anchors).unwrap(),
            BTreeMap::from([(target, Some(cell))])
        );
    }

    #[test]
    fn player_inventory_anchor_requires_its_own_incarnation_frontier() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let key = PlayerKey(uuid::Uuid::from_u128(91));
        let lease = crate::canonical_action::LivePlayerLease {
            key,
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(3),
            },
            incarnation: 11,
        };
        let target = Target::Player {
            key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv: 0, slot: 1 }),
        };
        let mut graphs = ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 7,
                    settled_through: TickStamp(90),
                },
            )
            .unwrap();
        assert_eq!(
            graphs.seed_player_inventory_target_anchor(target, lease, None),
            Err(ApplyFailure::InvalidAction)
        );
        graphs
            .install_player_inventory_frontier(
                InventoryRealmId::Player(0),
                PlayerInventoryGroundFrontier {
                    lease,
                    accepted_chunk: chunk,
                    domain_epoch: 11,
                    settled_through: TickStamp(88),
                },
            )
            .unwrap();
        let stale_lease = crate::canonical_action::LivePlayerLease {
            incarnation: 10,
            ..lease
        };
        assert_eq!(
            graphs.seed_player_inventory_target_anchor(target, stale_lease, None),
            Err(ApplyFailure::InvalidAction)
        );
        graphs
            .seed_player_inventory_target_anchor(target, lease, None)
            .unwrap();
        assert_eq!(
            graphs.ground_anchors[&target].settled_through,
            TickStamp(88)
        );
    }

    #[test]
    fn chunk_snapshot_carries_the_frozen_player_inventory_leg() {
        let key = PlayerKey(uuid::Uuid::from_u128(92));
        let target = Target::Player {
            key,
            lease: Some(crate::canonical_action::LivePlayerLease {
                key,
                gid: GlobalPlayerId {
                    server: ServerId(1),
                    player: crate::identity::PlayerSlot(1),
                },
                incarnation: 1,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot: 4 }),
        };
        let empty =
            crate::inventory::InventoryCell::empty(crate::inventory::InvLoc { inv: 0, slot: 4 });
        let stone = crate::inventory::InventoryCell {
            item: 1,
            count: 1,
            ..empty.clone()
        };
        let before = Cell {
            revision: 0,
            value: Value::Inventory(empty),
        };
        let after = Cell {
            revision: 1,
            value: Value::Inventory(stone.clone()),
        };
        let mut change = action(1, vec![target], vec![target]);
        change.conditions[0].expected = Expected::Exact(before.clone());
        change.effects[0] = Effect::Put {
            target,
            value: Value::Inventory(stone),
        };
        change.kind = ActionKind::Inventory;
        change.factual_result = Some(AppliedAction {
            id: change.id,
            tick: change.tick,
            actor_chunk: change.actor_chunk,
            view: TruthView::Local,
            writes: vec![AppliedWrite {
                target,
                before: Some(before.clone()),
                after: Some(after.clone()),
            }],
        });
        change.validate().unwrap();
        let chunk = change.player_chunks[0].1;
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(change.tick, [change.clone()])
            .unwrap();
        mark_projected_stable(graphs.ticks.get_mut(&change.tick).unwrap(), change.id);
        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: change.tick,
                    id: change.id,
                },
                target.action_realm(),
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: TickStamp(change.tick.0.wrapping_sub(1)),
                    value: Some(before),
                },
            )
            .unwrap();
        let snapshot = graphs.snapshot_for_chunk(chunk);
        assert!(
            graphs
                .snapshot_for_chunk_at_cut(chunk, TickStamp(change.tick.0.wrapping_sub(1)))
                .is_err()
        );
        assert_eq!(
            graphs
                .snapshot_for_chunk_at_cut(chunk, change.tick)
                .unwrap(),
            snapshot
        );
        assert!(
            snapshot[0].nodes[0]
                .applied_realms
                .contains(&target.action_realm())
        );
        let anchors = graphs.snapshot_anchors_for_graphs(chunk, &snapshot);
        let observed = BTreeMap::from([(target, Some(after))]);
        validate_chunk_graph_snapshot(chunk, change.tick, &snapshot, &anchors, &observed).unwrap();
        graphs
            .install_player_inventory_frontier(
                InventoryRealmId::Player(0),
                PlayerInventoryGroundFrontier {
                    lease: change.player_leases[0],
                    accepted_chunk: chunk,
                    domain_epoch: change.player_leases[0].incarnation,
                    settled_through: TickStamp(change.tick.0.wrapping_sub(1)),
                },
            )
            .unwrap();
        graphs
            .install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(change.tick.0.wrapping_sub(1)),
                },
            )
            .unwrap();
        assert_eq!(
            graphs.drain_grounded_tick(change.tick, true, &observed),
            Some(vec![change.clone()])
        );
        let drained = graphs.snapshot_for_chunk(chunk);
        assert!(drained.is_empty());
        assert!(
            graphs
                .snapshot_for_chunk_at_cut(chunk, TickStamp(change.tick.0.wrapping_sub(1)))
                .is_err()
        );
        assert!(
            graphs
                .snapshot_for_chunk_at_cut(chunk, change.tick)
                .unwrap()
                .is_empty()
        );
        let accepted_anchors = graphs.snapshot_anchors_for_graphs(chunk, &drained);
        assert_eq!(accepted_anchors.len(), 1);
        assert_eq!(accepted_anchors[0].1.settled_through, change.tick);
        validate_chunk_graph_snapshot(chunk, change.tick, &drained, &accepted_anchors, &observed)
            .unwrap();
        let mut imported = ActionGraphs::default();
        assert!(
            imported
                .merge_chunk_snapshot(chunk, change.tick, &drained, &accepted_anchors, &observed)
                .is_err()
        );
        imported
            .install_chunk_frontier(chunk, graphs.chunk_frontiers[&chunk])
            .unwrap();
        imported
            .install_player_inventory_frontier(
                InventoryRealmId::Player(0),
                graphs.player_inventory_frontiers[&(
                    key,
                    InventoryRealmId::Player(0),
                    change.player_leases[0].gid,
                    change.player_leases[0].incarnation,
                )],
            )
            .unwrap();
        imported
            .merge_chunk_snapshot(chunk, change.tick, &drained, &accepted_anchors, &observed)
            .unwrap();
        let mut corrupted = observed.clone();
        corrupted.insert(target, None);
        assert_eq!(
            validate_chunk_graph_snapshot(
                chunk,
                change.tick,
                &drained,
                &accepted_anchors,
                &corrupted
            ),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(
            graphs.player_inventory_frontiers[&(
                key,
                InventoryRealmId::Player(0),
                change.player_leases[0].gid,
                change.player_leases[0].incarnation,
            )]
                .settled_through,
            change.tick
        );
        let mut stale = change.clone();
        stale.tick = TickStamp(change.tick.0.wrapping_sub(1));
        stale.id.sequence = 2;
        stale.factual_result = None;
        assert!(graphs.insert_collected(stale.tick, [stale]).is_err());
    }

    #[test]
    fn player_inventory_anchors_are_isolated_by_login_incarnation() {
        let key = PlayerKey(uuid::Uuid::from_u128(801));
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let first = crate::canonical_action::LivePlayerLease {
            key,
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(4),
            },
            incarnation: 12,
        };
        let second = crate::canonical_action::LivePlayerLease {
            incarnation: 13,
            ..first
        };
        let target = |lease| Target::Player {
            key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv: 0, slot: 1 }),
        };
        let mut graphs = ActionGraphs::default();
        for (lease, tick, count) in [(first, 20, 1), (second, 40, 2)] {
            graphs
                .install_player_inventory_frontier(
                    InventoryRealmId::Player(0),
                    PlayerInventoryGroundFrontier {
                        lease,
                        accepted_chunk: chunk,
                        domain_epoch: lease.incarnation,
                        settled_through: TickStamp(tick),
                    },
                )
                .unwrap();
            graphs
                .seed_player_inventory_target_anchor(
                    target(lease),
                    lease,
                    Some(Cell {
                        revision: tick.into(),
                        value: Value::Inventory(crate::inventory::InventoryCell {
                            loc: crate::inventory::InvLoc { inv: 0, slot: 1 },
                            item: 1,
                            count,
                            nbt: Vec::new(),
                        }),
                    }),
                )
                .unwrap();
        }
        assert_eq!(graphs.ground_anchors.len(), 2);
        assert_eq!(
            graphs.ground_anchors[&target(first)].settled_through,
            TickStamp(20)
        );
        assert_eq!(
            graphs.ground_anchors[&target(second)].settled_through,
            TickStamp(40)
        );
        assert!(graphs.advance_empty_ground_tick(TickStamp(21), target(first), true));
        assert_eq!(
            graphs.ground_anchors[&target(first)].settled_through,
            TickStamp(21)
        );
        assert_eq!(
            graphs.ground_anchors[&target(second)].settled_through,
            TickStamp(40)
        );
        assert!(graphs.retire_player_inventory_lease(first));
        assert!(!graphs.ground_anchors.contains_key(&target(first)));
        assert_eq!(
            graphs.ground_anchors[&target(second)].settled_through,
            TickStamp(40)
        );
    }

    #[test]
    fn block_effect_posts_to_section_without_grouping_other_block_conditions() {
        let first = block(0, 1);
        let other = block(0, 2);
        let Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            ..
        } = first
        else {
            unreachable!();
        };
        let section = Target::Chunk {
            key,
            part: ChunkPart::BlockSection(position.y.div_euclid(16)),
            inventory: None,
        };
        let writer = action(1, vec![first], vec![first]);
        let mut reader = action(2, vec![section], Vec::new());
        reader.conditions[0].expected = crate::canonical_action::Expected::Exact(Cell {
            revision: 0,
            value: Value::BlockSection(vec![0; 4096]),
        });
        let unrelated = action(3, vec![other], Vec::new());
        let mut graph = TickActionGraph::default();
        graph.insert(writer.clone()).unwrap();
        graph.insert(reader.clone()).unwrap();
        graph.insert(unrelated.clone()).unwrap();
        let index = &graph.realms[&section.action_realm()];
        assert!(index.factual_effects[&section].contains_key(&writer.id));
        assert!(graph.actions[&reader.id].prerequisites.contains(&writer.id));
        assert!(
            !graph.actions[&unrelated.id]
                .prerequisites
                .contains(&writer.id)
        );
        graph.remove(writer.id).unwrap();
        assert!(
            !graph.realms[&section.action_realm()]
                .factual_effects
                .contains_key(&section)
        );
    }

    #[test]
    fn historical_section_reconstructs_prior_block_action_and_keeps_cut() {
        let target = block(0, 1);
        let Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            ..
        } = target
        else {
            unreachable!();
        };
        let section = Target::Chunk {
            key,
            part: ChunkPart::BlockSection(position.y.div_euclid(16)),
            inventory: None,
        };
        let original = Cell {
            revision: 4,
            value: Value::Block(0),
        };
        let palette = Cell {
            revision: 9,
            value: Value::BlockSection(vec![0; 4096]),
        };
        let mut graphs = ActionGraphs::default();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(original.clone()),
                },
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                section,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(palette.clone()),
                },
            )
            .unwrap();
        let issued = action(1, vec![target], vec![target]);
        graphs
            .insert_collected(issued.tick, [issued.clone()])
            .unwrap();
        let mut state = OwnedStakeholderSet::default();
        state.local.insert(target, original);
        let result =
            crate::canonical_action::prepare_action(&state, &issued, TruthView::Local).unwrap();
        let expected =
            crate::block_section::project_writes(section, &palette, &result.writes).unwrap();
        let node = graphs
            .ticks
            .get_mut(&issued.tick)
            .unwrap()
            .actions
            .get_mut(&issued.id)
            .unwrap();
        node.factual.result = Some(result);
        node.stable = true;
        node.semantic_order = Some(0);
        assert_eq!(
            graphs.symbolic_anchor_for(TickStamp(8), section),
            Ok(Some(expected))
        );
        assert_eq!(
            graphs.symbolic_anchor_for(TickStamp(7), section),
            Ok(Some(palette))
        );
        assert!(
            graphs
                .ground_drain_targets(issued.tick)
                .unwrap()
                .contains(&section)
        );
    }

    fn action(sequence: u16, conditions: Vec<Target>, effects: Vec<Target>) -> Action {
        let player_leases: Vec<crate::canonical_action::LivePlayerLease> = conditions
            .iter()
            .chain(&effects)
            .filter_map(|target| match target {
                Target::Player {
                    lease: Some(lease),
                    inventory: Some(_),
                    ..
                } => Some(*lease),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let player_chunks = player_leases
            .iter()
            .map(|lease: &crate::canonical_action::LivePlayerLease| {
                (
                    lease.key,
                    ChunkKey::new(crate::protocol::WorldId::new(0, 0), 0, 0),
                )
            })
            .collect::<Vec<_>>();
        let stakeholders = conditions
            .iter()
            .chain(&effects)
            .map(|target| target.stakeholder())
            .chain(
                player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(ServerId(sequence)),
                incarnation: 0,
                sequence,
                family: 0,
            },
            tick: TickStamp(7),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases,
            player_chunks,
            stakeholders,
            dependencies: Vec::new(),
            conditions: conditions
                .into_iter()
                .map(|target| Condition {
                    target,
                    expected: Expected::BlockState(0),
                })
                .collect(),
            effects: effects
                .into_iter()
                .map(|target| Effect::Put {
                    target,
                    value: Value::Block(sequence),
                })
                .collect(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        }
    }

    #[test]
    fn local_snapshot_projection_keeps_ungrounded_factual_cut_and_rejects_tampering() {
        let target = block(0, 1);
        let Target::Chunk { key: chunk, .. } = target else { unreachable!() };
        let before = Cell { revision: 0, value: Value::Block(0) };
        let after = Cell { revision: 1, value: Value::Block(1) };
        let mut change = action(1, vec![target], vec![target]);
        change.factual_result = Some(AppliedAction {
            id: change.id, tick: change.tick, actor_chunk: None, view: TruthView::Local,
            writes: vec![AppliedWrite { target, before: Some(before.clone()), after: Some(after.clone()) }],
        });
        let mut graphs = ActionGraphs::default();
        graphs.insert_collected(change.tick, [change.clone()]).unwrap();
        mark_projected_stable(graphs.ticks.get_mut(&change.tick).unwrap(), change.id);
        graphs.install_ground_anchor(target, GroundAnchor {
            settled_through: TickStamp(change.tick.0.wrapping_sub(1)), value: Some(before.clone()),
        }).unwrap();
        let mut snapshot = graphs.snapshot_for_chunk(chunk);
        let anchors = graphs.snapshot_anchors_for_graphs(chunk, &snapshot);
        assert_eq!(project_chunk_graph_snapshot_ground(chunk, change.tick, &snapshot, &anchors).unwrap()[&target], Some(before));
        assert_eq!(project_chunk_graph_snapshot_local_with_context(chunk, change.tick, &snapshot, &anchors, None).unwrap()[&target], Some(after));
        assert!(project_chunk_graph_snapshot_local_with_context(chunk, TickStamp(change.tick.0.wrapping_sub(1)), &snapshot, &anchors, None).is_err());
        snapshot[0].nodes[0].factual.result.as_mut().unwrap().writes[0].before.as_mut().unwrap().revision = 9;
        assert!(project_chunk_graph_snapshot_local_with_context(chunk, change.tick, &snapshot, &anchors, None).is_err());
    }

    #[test]
    fn scheduled_queue_effects_order_by_source_even_when_received_backwards() {
        use crate::canonical_action::{ScheduledTickFact, ScheduledTickKind};
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Whole,
            inventory: None,
        };
        let make = |sequence, source_order| {
            let mut action = action(sequence, Vec::new(), vec![target]);
            action.class = ActionClass::Infallible;
            action.kind = ActionKind::ScheduledTick;
            action.effects = vec![Effect::ScheduleTick {
                target,
                tick: ScheduledTickFact {
                    executor: crate::realm_completion::PeerGeneration {
                        server: crate::identity::ServerId(0),
                        generation: 1,
                    },
                    origin: crate::canonical_action::ScheduledTickOrigin::Action {
                        cause: ActionRef {
                            tick: action.tick,
                            id: action.id,
                        },
                        producer: crate::realm_completion::PeerGeneration {
                            server: action.id.actor.origin(),
                            generation: 1,
                        },
                    },
                    kind: ScheduledTickKind::Block,
                    id: sequence,
                    position: BlockPos { x: 1, y: 64, z: 1 },
                    due: TickStamp(9),
                    priority: 0,
                    source_order,
                },
            }];
            action
        };
        let first = make(10, 5);
        let second = make(9, 6);
        let mut graph = TickActionGraph::default();
        graph.insert(second.clone()).unwrap();
        graph.insert(first.clone()).unwrap();
        assert!(
            graph.edge_reasons[&(first.id, second.id)]
                .contains_key(&EdgeReason::InfallibleScheduledOrder(target))
        );
        assert!(graph.actions[&second.id].prerequisites.contains(&first.id));
        let mut drain = action(11, Vec::new(), vec![target]);
        drain.class = ActionClass::Infallible;
        drain.kind = ActionKind::ScheduledTick;
        drain.effects = vec![Effect::DrainScheduledTicks {
            target,
            through: drain.tick,
        }];
        graph.insert(drain.clone()).unwrap();
        assert!(
            graph.edge_reasons[&(drain.id, first.id)]
                .contains_key(&EdgeReason::InfallibleScheduledOrder(target))
        );
        assert!(
            graph.edge_reasons[&(drain.id, second.id)]
                .contains_key(&EdgeReason::InfallibleScheduledOrder(target))
        );
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 2, y: 64, z: 2 }),
            inventory: None,
        };
        let mut failed_callback = make(12, 4);
        failed_callback.class = ActionClass::Undoable;
        failed_callback.conditions.push(Condition {
            target: block,
            expected: Expected::BlockState(0),
        });
        let independent = make(13, 9);
        for arrivals in [
            [failed_callback.clone(), independent.clone()],
            [independent.clone(), failed_callback.clone()],
        ] {
            let mut mixed = TickActionGraph::default();
            for action in arrivals {
                mixed.insert(action).unwrap();
            }
            assert!(
                !mixed.edge_reasons[&(failed_callback.id, independent.id)]
                    .contains_key(&EdgeReason::InfallibleScheduledOrder(target))
            );
            let anchors = BTreeMap::from([
                (
                    target,
                    Some(Cell {
                        revision: 0,
                        value: Value::ScheduledQueue(Vec::new()),
                    }),
                ),
                (
                    block,
                    Some(Cell {
                        revision: 0,
                        value: Value::Block(1),
                    }),
                ),
            ]);
            let solved = crate::action_solver::solve_component(
                &mixed,
                &BTreeSet::from([failed_callback.id, independent.id]),
                &anchors,
                31,
                |_| Some(true),
            )
            .unwrap();
            assert_eq!(
                solved.variants[&failed_callback.id],
                crate::action_solver::SolvedVariant::Undone
            );
            assert!(matches!(
                solved.variants[&independent.id],
                crate::action_solver::SolvedVariant::Applied { .. }
            ));
        }
    }

    fn with_factual_result(mut action: Action) -> Action {
        action.factual_result = Some(AppliedAction {
            id: action.id,
            tick: action.tick,
            actor_chunk: action.actor_chunk,
            view: TruthView::Local,
            writes: action
                .effects
                .iter()
                .flat_map(Effect::targets)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|target| AppliedWrite {
                    target,
                    before: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                    after: Some(Cell {
                        revision: 1,
                        value: Value::Block(action.id.sequence),
                    }),
                })
                .collect(),
        });
        action
    }

    fn mark_projected_stable(graph: &mut TickActionGraph, id: ActionId) {
        let node = graph.actions.get_mut(&id).unwrap();
        node.semantic_generation = Some(node.generation);
        node.semantic_order = node.factual.result.as_ref().map(|_| 0);
        if let Some(result) = graph.actions[&id].factual.result.clone() {
            graph.mark_local_applied(id, 0, result).unwrap();
        } else {
            graph.mark_local_undone(id).unwrap();
        }
        graph.mark_stable(id).unwrap();
    }

    #[test]
    fn actor_and_chunk_indexes_follow_joint_actions_grounding_and_retained_versions() {
        let left = block(0, 1);
        let right = block(1, 17);
        let unrelated = block(9, 145);
        let first = with_factual_result(action(1, vec![left, right], vec![left, right]));
        let second = action(2, vec![unrelated], vec![unrelated]);
        let reference = ActionRef { tick: first.tick, id: first.id };
        let chunks = affected_chunks(&first);
        let mut graphs = ActionGraphs::default();
        graphs.insert_collected(first.tick, [first.clone(), second]).unwrap();
        let retained = graphs.clone();
        assert_eq!(graphs.actor_action_refs(first.id.actor).collect::<Vec<_>>(), vec![reference]);
        for chunk in chunks {
            assert_eq!(graphs.chunk_action_refs(chunk).collect::<Vec<_>>(), vec![reference]);
        }
        let graph = graphs.ticks.get_mut(&first.tick).unwrap();
        mark_projected_stable(graph, first.id);
        let realms = graph.actions[&first.id].realms.clone();
        for realm in realms { graphs.mark_ground_realm_applied(reference, realm).unwrap(); }
        assert!(graphs.unsettled_actor_actions(first.id.actor).next().is_none());
        assert_eq!(graphs.actor_action_refs(first.id.actor).next(), Some(reference));
        assert_eq!(graphs.remove_grounded(reference), Some(first.clone()));
        assert!(graphs.actor_action_refs(first.id.actor).next().is_none());
        assert_eq!(retained.unsettled_actor_actions(first.id.actor).next(), Some(reference));
        assert_eq!(graphs.ticks[&reference.tick].actions.len(), 1);
    }

    #[test]
    fn late_conflict_connects_only_opposite_types_and_keeps_joint_realm_index() {
        let shared = block(0, 1);
        let other = block(1, 17);
        let first = action(1, vec![shared], vec![shared]);
        let second = action(2, vec![shared, other], vec![shared, other]);
        let third = action(3, vec![other], vec![other]);
        let mut graph = TickActionGraph::default();
        graph.insert(first.clone()).unwrap();
        graph.insert(second.clone()).unwrap();
        graph.insert(third.clone()).unwrap();
        let shared_index = &graph.realms[&shared.action_realm()];
        assert_eq!(
            shared_index.factual_conditions[&shared],
            BTreeMap::from([(first.id, 1), (second.id, 1)])
        );
        assert_eq!(
            shared_index.factual_effects[&shared],
            BTreeMap::from([(first.id, 1), (second.id, 1)])
        );
        assert!(graph.actions[&second.id].dependents.contains(&third.id));
        assert!(graph.actions[&third.id].prerequisites.contains(&second.id));
        assert!(graph.actions[&first.id].dependents.contains(&second.id));
        assert!(graph.actions[&second.id].dependents.contains(&first.id));
        graph.remove(second.id).unwrap();
        assert!(!graph.actions[&first.id].dependents.contains(&second.id));
        assert!(!graph.actions[&third.id].prerequisites.contains(&second.id));
        assert!(
            !graph.realms[&other.action_realm()]
                .factual_effects
                .contains_key(&other)
                || graph.realms[&other.action_realm()].factual_effects[&other]
                    == BTreeMap::from([(third.id, 1)])
        );
    }

    #[test]
    fn three_way_conflict_graph_is_independent_of_arrival_order() {
        let target = block(0, 1);
        let actions = [
            action(1, vec![target], vec![target]),
            action(2, vec![target], vec![target]),
            action(3, vec![target], vec![target]),
        ];
        let mut expected = None;
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let mut graph = TickActionGraph::default();
            for index in order {
                graph.insert(actions[index].clone()).unwrap();
            }
            let current = graph.edge_reasons.clone();
            match &expected {
                Some(expected) => assert_eq!(&current, expected),
                None => expected = Some(current),
            }
        }
    }

    #[test]
    fn factual_effect_distinguishes_support_from_invalidated_condition() {
        let target = block(0, 1);
        let producer = with_factual_result(action(1, vec![target], vec![target]));
        let mut supported = action(2, vec![target], vec![target]);
        supported.conditions[0].expected = Expected::BlockState(1);
        let invalidated = action(3, vec![target], vec![target]);
        let mut graph = TickActionGraph::default();
        graph.insert(producer.clone()).unwrap();
        graph.insert(supported.clone()).unwrap();
        graph.insert(invalidated.clone()).unwrap();
        assert_eq!(
            graph.condition_impact(producer.id, supported.id, target),
            Some(ConditionImpact::Satisfied)
        );
        let mut partly_supported = action(4, vec![target], vec![]);
        partly_supported.conditions[0].expected = Expected::BlockState(1);
        partly_supported.conditions.push(Condition {
            target,
            expected: Expected::Exact(Cell {
                revision: 0,
                value: Value::Block(1),
            }),
        });
        graph.insert(partly_supported.clone()).unwrap();
        assert_eq!(
            graph.condition_impact(producer.id, partly_supported.id, target),
            Some(ConditionImpact::Violated)
        );
        assert_eq!(
            graph.condition_impact(producer.id, invalidated.id, target),
            Some(ConditionImpact::Violated)
        );
        assert_eq!(
            graph.condition_impact(supported.id, producer.id, target),
            Some(ConditionImpact::Unknown)
        );
    }

    #[test]
    fn late_conflict_invalidates_prior_semantic_resolution() {
        let shared = block(0, 1);
        let first = with_factual_result(action(1, vec![shared], vec![shared]));
        let second = with_factual_result(action(2, vec![shared], vec![shared]));
        let unrelated_target = block(1, 17);
        let unrelated =
            with_factual_result(action(3, vec![unrelated_target], vec![unrelated_target]));
        let mut graph = TickActionGraph::default();
        graph.insert(first.clone()).unwrap();
        graph.insert(unrelated.clone()).unwrap();
        mark_projected_stable(&mut graph, first.id);
        mark_projected_stable(&mut graph, unrelated.id);
        graph.insert(second.clone()).unwrap();
        assert!(!graph.actions[&first.id].stable);
        assert!(!graph.actions[&second.id].stable);
        assert!(graph.actions[&unrelated.id].stable);
        mark_projected_stable(&mut graph, first.id);
        mark_projected_stable(&mut graph, second.id);
        graph
            .replace_factual(
                second.id,
                SemanticWrapper::Undone,
                FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        assert!(!graph.actions[&first.id].stable);
        assert!(graph.actions[&unrelated.id].stable);
    }

    #[test]
    fn ground_anchor_cannot_advance_past_unresolved_action() {
        let target = block(0, 1);
        let issued = action(1, vec![target], vec![target]);
        let mut graphs = ActionGraphs::default();
        let baseline = GroundAnchor {
            settled_through: TickStamp(6),
            value: Some(Cell {
                revision: 4,
                value: Value::Block(0),
            }),
        };
        graphs
            .install_ground_anchor(target, baseline.clone())
            .unwrap();
        graphs
            .insert_collected(issued.tick, [issued.clone()])
            .unwrap();
        assert_eq!(
            graphs.install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: issued.tick,
                    value: Some(Cell {
                        revision: 5,
                        value: Value::Block(1),
                    }),
                },
            ),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(graphs.ground_anchors[&target], baseline);
        graphs
            .remove_grounded(ActionRef {
                tick: issued.tick,
                id: issued.id,
            })
            .unwrap();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: issued.tick,
                    value: Some(Cell {
                        revision: 5,
                        value: Value::Block(1),
                    }),
                },
            )
            .unwrap();
        assert_eq!(graphs.ground_anchors[&target].settled_through, issued.tick);
    }

    #[test]
    fn empty_certified_tick_advances_anchor_and_rejects_late_action() {
        let target = block(0, 1);
        let other = block(0, 2);
        let mut graphs = ActionGraphs::default();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 4,
                        value: Value::Block(0),
                    }),
                },
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                other,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 2,
                        value: Value::Block(0),
                    }),
                },
            )
            .unwrap();
        let Target::Chunk { key, .. } = target else {
            unreachable!()
        };
        graphs
            .install_chunk_frontier(
                key,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(6),
                },
            )
            .unwrap();
        assert!(!graphs.advance_empty_ground_tick(TickStamp(7), target, false));
        assert!(!graphs.advance_empty_ground_tick(TickStamp(8), target, true));
        assert!(graphs.advance_empty_ground_tick(TickStamp(7), target, true));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(7));
        assert_eq!(graphs.chunk_frontiers[&key].settled_through, TickStamp(7));
        let late_untouched = action(3, vec![other], vec![other]);
        assert!(
            graphs
                .insert_collected(late_untouched.tick, [late_untouched])
                .is_err()
        );
        assert!(graphs.advance_empty_ground_tick(TickStamp(7), other, true));
        assert_eq!(graphs.ground_anchors[&other].settled_through, TickStamp(7));
        let late = action(1, vec![target], vec![target]);
        assert!(graphs.insert_collected(late.tick, [late]).is_err());
        let mut next = action(2, vec![target], vec![target]);
        next.tick = TickStamp(8);
        assert!(graphs.insert_collected(next.tick, [next]).is_ok());
    }

    #[test]
    fn empty_chunk_cut_cannot_skip_another_target_action() {
        let first = block(0, 1);
        let second = block(0, 2);
        let Target::Chunk { key, .. } = first else {
            unreachable!()
        };
        let mut graphs = ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                key,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(6),
                },
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                first,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                },
            )
            .unwrap();
        graphs
            .insert_collected(TickStamp(7), [action(1, vec![second], vec![second])])
            .unwrap();
        assert!(!graphs.advance_empty_ground_tick(TickStamp(7), first, true));
        assert_eq!(graphs.chunk_frontiers[&key].settled_through, TickStamp(6));
    }

    #[test]
    fn symbolic_anchor_folds_prior_graph_without_world_reads() {
        let target = block(0, 1);
        let mut graphs = ActionGraphs::default();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                },
            )
            .unwrap();
        let prior = with_factual_result(action(1, vec![target], vec![target]));
        graphs
            .insert_collected(prior.tick, [prior.clone()])
            .unwrap();
        assert_eq!(
            graphs.symbolic_anchor_for(TickStamp(8), target),
            Err(SymbolicAnchorFailure::Unsettled(ActionRef {
                tick: prior.tick,
                id: prior.id,
            }))
        );
        mark_projected_stable(graphs.ticks.get_mut(&prior.tick).unwrap(), prior.id);
        assert_eq!(
            graphs.symbolic_anchor_for(TickStamp(8), target),
            Ok(Some(Cell {
                revision: 1,
                value: Value::Block(1),
            }))
        );
    }

    #[test]
    fn counterfactual_location_is_indexed_without_becoming_factual_conflict() {
        let source = block(0, 1);
        let possible_shift_slot = block(1, 17);
        let mut shift = action(1, vec![source], vec![source]);
        shift.counterfactual_conditions.push(possible_shift_slot);
        shift.counterfactual_effects.push(possible_shift_slot);
        shift.stakeholders.push(possible_shift_slot.stakeholder());
        let ordinary = action(2, vec![possible_shift_slot], vec![possible_shift_slot]);
        let mut graph = TickActionGraph::default();
        graph.insert(shift.clone()).unwrap();
        graph.insert(ordinary.clone()).unwrap();
        let index = &graph.realms[&possible_shift_slot.action_realm()];
        assert_eq!(
            index.counterfactual_conditions[&possible_shift_slot][&shift.id],
            1
        );
        assert_eq!(
            index.counterfactual_effects[&possible_shift_slot][&shift.id],
            1
        );
        assert!(!graph.actions[&shift.id].dependents.contains(&ordinary.id));
        assert!(!graph.actions[&ordinary.id].dependents.contains(&shift.id));
        assert_eq!(
            graph.counterfactual_blockers(ordinary.id),
            BTreeSet::from([shift.id])
        );
        graph.remove(shift.id).unwrap();
        assert!(graph.counterfactual_blockers(ordinary.id).is_empty());
    }

    #[test]
    fn invalid_collected_batch_does_not_install_earlier_valid_actions() {
        let target = block(0, 1);
        let valid = action(1, vec![target], vec![target]);
        let mut invalid = action(2, vec![target], vec![target]);
        invalid.stakeholders.clear();
        let mut graphs = ActionGraphs::default();
        assert_eq!(
            graphs.insert_collected(TickStamp(7), [valid, invalid]),
            Err(ApplyFailure::InvalidAction)
        );
        assert!(!graphs.ticks.contains_key(&TickStamp(7)));
    }

    #[test]
    fn recovered_action_delivery_is_idempotent_but_equivocation_is_rejected() {
        let target = block(0, 1);
        let issued = action(1, vec![target], vec![target]);
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(issued.tick, [issued.clone()])
            .unwrap();
        assert!(
            graphs
                .insert_collected(issued.tick, [issued.clone(), issued.clone()])
                .unwrap()
                .is_empty()
        );
        let mut changed = issued.clone();
        changed.effects[0] = Effect::Put {
            target,
            value: Value::Block(3),
        };
        assert_eq!(
            graphs.insert_collected(issued.tick, [changed]),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(graphs.ticks[&issued.tick].actions.len(), 1);
    }

    #[test]
    fn explicit_dependency_connects_when_predecessor_arrives_late() {
        let first_target = block(0, 1);
        let second_target = block(1, 17);
        let predecessor = action(1, vec![first_target], vec![first_target]);
        let mut dependent = action(2, vec![second_target], vec![second_target]);
        dependent
            .dependencies
            .push(crate::canonical_action::ActionRef {
                tick: predecessor.tick,
                id: predecessor.id,
            });
        let mut graph = TickActionGraph::default();
        graph.insert(dependent.clone()).unwrap();
        assert!(
            graph.actions[&dependent.id]
                .prerequisites
                .contains(&predecessor.id)
        );
        graph.insert(predecessor.clone()).unwrap();
        assert!(
            graph.actions[&predecessor.id]
                .dependents
                .contains(&dependent.id)
        );
        graph.remove(predecessor.id).unwrap();
        assert!(
            !graph.actions[&dependent.id]
                .prerequisites
                .contains(&predecessor.id)
        );
    }

    #[test]
    fn later_tick_cascade_uses_wrapping_time_and_stops_after_ground_drain() {
        let shared = block(0, 1);
        let unrelated = block(1, 17);
        let mut first = action(1, vec![shared], vec![shared]);
        first.tick = TickStamp(u16::MAX);
        let mut dependent = action(2, vec![shared], vec![shared]);
        dependent.tick = TickStamp(0);
        dependent.dependencies.push(ActionRef {
            tick: first.tick,
            id: first.id,
        });
        let mut transitive = action(4, vec![shared], vec![shared]);
        transitive.tick = TickStamp(1);
        let unrelated = action(3, vec![unrelated], vec![unrelated]);
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(first.tick, [first.clone()])
            .unwrap();
        graphs
            .insert_collected(dependent.tick, [dependent.clone()])
            .unwrap();
        graphs
            .insert_collected(transitive.tick, [transitive.clone()])
            .unwrap();
        let independent = graphs
            .insert_collected(unrelated.tick, [unrelated.clone()])
            .unwrap();
        assert_eq!(independent, BTreeSet::from([unrelated.id]));
        let source = ActionRef {
            tick: first.tick,
            id: first.id,
        };
        assert_eq!(
            graphs.later_dependents(source),
            vec![
                ActionRef {
                    tick: dependent.tick,
                    id: dependent.id
                },
                ActionRef {
                    tick: transitive.tick,
                    id: transitive.id
                }
            ]
        );
        assert_eq!(
            graphs.cascade_to_current(TickStamp(1), [source]).unwrap(),
            vec![
                (first.tick, BTreeSet::from([source])),
                (
                    dependent.tick,
                    BTreeSet::from([ActionRef {
                        tick: dependent.tick,
                        id: dependent.id
                    }])
                ),
                (
                    transitive.tick,
                    BTreeSet::from([ActionRef {
                        tick: transitive.tick,
                        id: transitive.id
                    }])
                )
            ]
        );
        graphs.remove_grounded(source).unwrap();
        assert!(graphs.later_dependents(source).is_empty());
    }

    #[test]
    fn factual_condition_replacement_preserves_posting_multiplicity() {
        let shared = block(0, 1);
        let producer = action(1, vec![shared], vec![shared]);
        let mut consumer = action(2, vec![shared], vec![]);
        consumer.conditions.push(Condition {
            target: shared,
            expected: Expected::Exact(Cell {
                revision: 1,
                value: Value::Block(0),
            }),
        });
        let mut graph = TickActionGraph::default();
        graph.insert(producer.clone()).unwrap();
        graph.insert(consumer.clone()).unwrap();
        let reason = EdgeReason::FactualEffectCondition(shared);
        assert_eq!(graph.edge_reasons[&(producer.id, consumer.id)][&reason], 2);
        let mut reversed = TickActionGraph::default();
        reversed.insert(consumer.clone()).unwrap();
        reversed.insert(producer.clone()).unwrap();
        assert_eq!(
            reversed.edge_reasons[&(producer.id, consumer.id)][&reason],
            2
        );
        graph
            .replace_factual(
                consumer.id,
                SemanticWrapper::Replayed,
                FactualVariant {
                    conditions: vec![consumer.conditions[0].clone()],
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        assert_eq!(graph.edge_reasons[&(producer.id, consumer.id)][&reason], 1);
        assert_eq!(
            graph.realms[&shared.action_realm()].factual_conditions[&shared][&consumer.id],
            1
        );
        graph
            .replace_factual(
                consumer.id,
                SemanticWrapper::Redone,
                FactualVariant {
                    conditions: consumer.conditions.clone(),
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        assert_eq!(graph.edge_reasons[&(producer.id, consumer.id)][&reason], 2);
        graph
            .replace_factual(
                consumer.id,
                SemanticWrapper::Undone,
                FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        assert!(!graph.edge_reasons.contains_key(&(producer.id, consumer.id)));
        assert!(
            !graph.realms[&shared.action_realm()].factual_conditions[&shared]
                .contains_key(&consumer.id)
        );
        graph.remove(consumer.id).unwrap();
    }

    #[test]
    fn one_edge_retains_two_independent_effect_reasons_until_action_drains() {
        let shared = block(0, 1);
        let prerequisite = action(1, vec![shared], vec![shared]);
        let mut producer = action(2, vec![shared], vec![shared]);
        producer.effects.push(Effect::Put {
            target: shared,
            value: Value::Block(4),
        });
        let mut graph = TickActionGraph::default();
        graph.insert(prerequisite.clone()).unwrap();
        graph.insert(producer.clone()).unwrap();
        assert_eq!(
            graph.edge_reasons[&(producer.id, prerequisite.id)]
                [&EdgeReason::FactualEffectCondition(shared)],
            2
        );
        graph
            .replace_factual(
                producer.id,
                SemanticWrapper::Replayed,
                FactualVariant {
                    conditions: producer.conditions.clone(),
                    effects: vec![producer.effects[0].clone()],
                    result: None,
                },
            )
            .unwrap();
        assert_eq!(
            graph.edge_reasons[&(producer.id, prerequisite.id)]
                [&EdgeReason::FactualEffectCondition(shared)],
            1
        );
        graph.remove(producer.id).unwrap();
        assert!(
            !graph
                .edge_reasons
                .contains_key(&(producer.id, prerequisite.id))
        );
        assert!(graph.actions[&prerequisite.id].prerequisites.is_empty());
    }

    #[test]
    fn replay_moves_only_its_factual_edges_and_keeps_counterfactual_slot() {
        let old_slot = block(0, 1);
        let new_slot = block(1, 17);
        let mut replayable = action(1, vec![old_slot], vec![old_slot]);
        replayable.counterfactual_conditions.push(new_slot);
        replayable.counterfactual_effects.push(new_slot);
        replayable.stakeholders.push(new_slot.stakeholder());
        let observer = action(2, vec![new_slot], vec![new_slot]);
        let mut graph = TickActionGraph::default();
        graph.insert(replayable.clone()).unwrap();
        graph.insert(observer.clone()).unwrap();
        assert!(
            !graph
                .edge_reasons
                .contains_key(&(replayable.id, observer.id))
        );
        graph
            .replace_factual(
                replayable.id,
                SemanticWrapper::Replayed,
                FactualVariant {
                    conditions: vec![Condition {
                        target: new_slot,
                        expected: Expected::BlockState(0),
                    }],
                    effects: vec![Effect::Put {
                        target: new_slot,
                        value: Value::Block(1),
                    }],
                    result: None,
                },
            )
            .unwrap();
        assert!(
            graph
                .edge_reasons
                .contains_key(&(replayable.id, observer.id))
        );
        assert_eq!(graph.actions[&replayable.id].generation, 1);
        assert_eq!(
            graph.actions[&replayable.id].wrapper,
            SemanticWrapper::Replayed
        );
        assert_eq!(
            graph.realms[&new_slot.action_realm()].counterfactual_effects[&new_slot]
                [&replayable.id],
            1
        );
        graph
            .replace_factual(
                replayable.id,
                SemanticWrapper::Redone,
                FactualVariant {
                    conditions: replayable.conditions.clone(),
                    effects: replayable.effects.clone(),
                    result: None,
                },
            )
            .unwrap();
        assert!(
            !graph
                .edge_reasons
                .contains_key(&(replayable.id, observer.id))
        );
        assert_eq!(graph.actions[&replayable.id].generation, 2);
        assert_eq!(
            graph.realms[&new_slot.action_realm()].counterfactual_effects[&new_slot]
                [&replayable.id],
            1
        );
    }

    #[test]
    fn undone_action_keeps_original_locations_for_possible_redo() {
        let target = block(0, 1);
        let first = action(1, vec![target], vec![target]);
        let second = action(2, vec![target], vec![target]);
        let mut graph = TickActionGraph::default();
        graph.insert(first.clone()).unwrap();
        graph.insert(second.clone()).unwrap();
        graph
            .replace_factual(
                first.id,
                SemanticWrapper::Undone,
                FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        let index = &graph.realms[&target.action_realm()];
        assert!(
            !index.factual_conditions.contains_key(&target)
                || !index.factual_conditions[&target].contains_key(&first.id)
        );
        assert_eq!(index.counterfactual_conditions[&target][&first.id], 1);
        assert_eq!(index.counterfactual_effects[&target][&first.id], 1);
        assert!(graph.counterfactual_blockers(second.id).contains(&first.id));
        graph
            .replace_factual(
                first.id,
                SemanticWrapper::Redone,
                FactualVariant {
                    conditions: first.conditions.clone(),
                    effects: first.effects.clone(),
                    result: None,
                },
            )
            .unwrap();
        assert!(graph.actions[&first.id].dependents.contains(&second.id));
    }

    #[test]
    fn cycle_resolution_scope_excludes_independent_action() {
        let shared = block(0, 1);
        let separate = block(1, 17);
        let first = action(1, vec![shared], vec![shared]);
        let second = action(2, vec![shared], vec![shared]);
        let unrelated = action(3, vec![separate], vec![separate]);
        let mut graph = TickActionGraph::default();
        graph.insert(first.clone()).unwrap();
        graph.insert(second.clone()).unwrap();
        graph.insert(unrelated.clone()).unwrap();
        assert_eq!(
            graph.affected_components(&BTreeSet::from([first.id])),
            vec![BTreeSet::from([first.id, second.id])]
        );
        assert_eq!(
            graph.affected_components(&BTreeSet::from([unrelated.id])),
            vec![BTreeSet::from([unrelated.id])]
        );
    }

    #[test]
    fn exact_backward_undo_restores_causal_cells_across_ticks() {
        let target = block(0, 1);
        let cell = |revision, state| Cell {
            revision,
            value: Value::Block(state),
        };
        let mut earlier = action(1, vec![target], vec![target]);
        earlier.tick = TickStamp(7);
        let mut later = action(2, vec![target], vec![target]);
        later.tick = TickStamp(8);
        let prior = |action: &Action, before, after| AppliedAction {
            id: action.id,
            tick: action.tick,
            actor_chunk: None,
            view: TruthView::Local,
            writes: vec![AppliedWrite {
                target,
                before: Some(before),
                after: Some(after),
            }],
        };
        let mut local = OwnedStakeholderSet::default();
        local.local.insert(target, cell(12, 3));
        let deltas = [
            SemanticDelta {
                reference: ActionRef {
                    tick: earlier.tick,
                    id: earlier.id,
                },
                previous_order: Some(0),
                next_order: None,
                previous: Some(prior(&earlier, cell(10, 1), cell(11, 2))),
                core: earlier,
                next: None,
                next_conditions: Vec::new(),
            },
            SemanticDelta {
                reference: ActionRef {
                    tick: later.tick,
                    id: later.id,
                },
                previous_order: Some(0),
                next_order: None,
                previous: Some(prior(&later, cell(11, 2), cell(12, 3))),
                core: later,
                next: None,
                next_conditions: Vec::new(),
            },
        ];
        let staged = stage_backward_local_deltas(&local, TickStamp(8), &deltas).unwrap();
        assert_eq!(staged.steps.len(), 2);
        assert_eq!(staged.steps[0].writes[0].after, Some(cell(11, 2)));
        assert_eq!(staged.steps[1].writes[0].after, Some(cell(10, 1)));
        assert_eq!(staged.writes[0].after, Some(cell(10, 1)));
        assert_eq!(local.local[&target], cell(12, 3));
    }

    #[test]
    fn backward_delta_plan_undoes_later_and_earlier_before_replay() {
        let target = Target::Player {
            key: PlayerKey(uuid::Uuid::from_u128(99)),
            lease: None,
            inventory: None,
        };
        let cell = |revision, value| crate::canonical_action::Cell {
            revision,
            value: Value::Scalar(value),
        };
        let mut earlier = action(1, Vec::new(), Vec::new());
        earlier.stakeholders = vec![target.stakeholder()];
        earlier.conditions = vec![Condition {
            target,
            expected: Expected::ScalarAtLeast(2),
        }];
        earlier.effects = vec![Effect::Add { target, amount: 2 }];
        earlier
            .undo
            .push(crate::canonical_action::UndoProcedure::AdjustScalar { target });
        earlier.kind = ActionKind::PlayerAdministration;
        earlier.validate().unwrap();
        let mut later = earlier.clone();
        later.id.sequence = 2;
        later.tick = TickStamp(8);
        later.effects = vec![Effect::Add { target, amount: -2 }];
        later.validate().unwrap();
        let prior = |action: &Action, before, after| AppliedAction {
            id: action.id,
            tick: action.tick,
            actor_chunk: None,
            view: TruthView::Local,
            writes: vec![AppliedWrite {
                target,
                before: Some(before),
                after: Some(after),
            }],
        };
        let mut local = OwnedStakeholderSet::default();
        local.local.insert(target, cell(3, 2));
        let deltas = [
            SemanticDelta {
                reference: ActionRef {
                    tick: earlier.tick,
                    id: earlier.id,
                },
                previous_order: Some(0),
                next_order: None,
                core: earlier.clone(),
                previous: Some(prior(&earlier, cell(1, 2), cell(2, 4))),
                next: None,
                next_conditions: Vec::new(),
            },
            SemanticDelta {
                reference: ActionRef {
                    tick: later.tick,
                    id: later.id,
                },
                previous_order: Some(0),
                next_order: Some(0),
                core: later.clone(),
                previous: Some(prior(&later, cell(2, 4), cell(3, 2))),
                next: Some(prior(&later, cell(4, 2), cell(5, 0))),
                next_conditions: later.conditions.clone(),
            },
        ];
        let changes = stage_backward_local_deltas(&local, TickStamp(8), &deltas).unwrap();
        assert_eq!(
            changes
                .steps
                .iter()
                .map(|step| (step.reference.tick, step.direction))
                .collect::<Vec<_>>(),
            vec![
                (later.tick, LocalDeltaDirection::Undo),
                (earlier.tick, LocalDeltaDirection::Undo),
                (later.tick, LocalDeltaDirection::Apply)
            ]
        );
        assert_eq!(changes.steps[0].writes[0].before, Some(cell(3, 2)));
        assert_eq!(changes.steps[2].writes[0].after, Some(cell(6, 0)));
        assert_eq!(
            changes.writes,
            vec![AppliedWrite {
                target,
                before: Some(cell(3, 2)),
                after: Some(cell(6, 0)),
            }]
        );
        assert_eq!(local.local[&target], cell(3, 2));
        let mut strict = deltas.to_vec();
        strict[1].next_conditions = vec![Condition {
            target,
            expected: Expected::Exact(cell(2, 2)),
        }];
        assert!(matches!(
            stage_backward_local_deltas(&local, TickStamp(8), &strict),
            Err(DeltaFailure::StaleReplay(changed)) if changed == target
        ));
    }

    #[test]
    fn ground_plan_advances_complete_realm_without_promoting_incomplete_peer() {
        let ready_target = block(0, 1);
        let waiting_target = block(1, 17);
        let ready = with_factual_result(action(1, vec![ready_target], vec![ready_target]));
        let waiting = with_factual_result(action(2, vec![waiting_target], vec![waiting_target]));
        let mut graph = TickActionGraph::default();
        graph.insert(ready.clone()).unwrap();
        graph.insert(waiting.clone()).unwrap();
        mark_projected_stable(&mut graph, ready.id);
        mark_projected_stable(&mut graph, waiting.id);
        let plan = graph.ground_ready_plan(
            ready.tick,
            |realm| realm == ready_target.action_realm(),
            |_| false,
        );
        assert_eq!(
            plan.components,
            vec![vec![ActionRef {
                tick: ready.tick,
                id: ready.id,
            }]]
        );
        let complete = graph.ground_ready_plan(ready.tick, |_| true, |_| false);
        assert_eq!(complete.components.len(), 2);
    }

    #[test]
    fn grounded_action_remains_indexed_until_closed_tick_drains() {
        let first_target = block(0, 1);
        let second_target = block(1, 17);
        let first = with_factual_result(action(1, vec![first_target], vec![first_target]));
        let second = with_factual_result(action(2, vec![second_target], vec![second_target]));
        let mut graphs = ActionGraphs::default();
        for target in [first_target, second_target] {
            graphs.ground_anchors.insert(
                target,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                },
            );
            if let Target::Chunk { key, .. } = target {
                graphs
                    .install_chunk_frontier(
                        key,
                        ChunkGroundFrontier {
                            epoch: 1,
                            settled_through: TickStamp(6),
                        },
                    )
                    .unwrap();
            }
        }
        graphs
            .insert_collected(first.tick, [first.clone(), second.clone()])
            .unwrap();
        let graph = graphs.ticks.get_mut(&first.tick).unwrap();
        mark_projected_stable(graph, first.id);
        mark_projected_stable(graph, second.id);

        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: first.tick,
                    id: first.id,
                },
                first_target.action_realm(),
            )
            .unwrap();
        assert_eq!(
            graphs.ticks[&first.tick].validate_factual_replacement(
                first.id,
                SemanticWrapper::Undone,
                &FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            ),
            Err(ApplyFailure::InvalidAction)
        );
        let conflicting = with_factual_result(action(3, vec![first_target], vec![first_target]));
        assert_eq!(
            graphs.insert_collected(first.tick, [conflicting]),
            Err(ApplyFailure::InvalidAction)
        );
        let third_target = block(2, 33);
        let mut dependent = with_factual_result(action(4, vec![third_target], vec![third_target]));
        dependent.dependencies.push(ActionRef {
            tick: first.tick,
            id: first.id,
        });
        assert_eq!(
            graphs.insert_collected(first.tick, [dependent]),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(graphs.ticks[&first.tick].actions.len(), 2);
        assert!(graphs.ticks[&first.tick].actions.contains_key(&first.id));
        assert!(
            graphs
                .drain_grounded_tick(first.tick, true, &BTreeMap::new())
                .is_none()
        );
        assert_eq!(
            graphs.ticks[&first.tick]
                .ground_ready_plan(first.tick, |_| true, |_| false)
                .components,
            vec![vec![ActionRef {
                tick: second.tick,
                id: second.id
            }]],
        );

        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: second.tick,
                    id: second.id,
                },
                second_target.action_realm(),
            )
            .unwrap();
        let observed = BTreeMap::from([
            (
                first_target,
                Some(Cell {
                    revision: 1,
                    value: Value::Block(1),
                }),
            ),
            (
                second_target,
                Some(Cell {
                    revision: 1,
                    value: Value::Block(2),
                }),
            ),
        ]);
        assert!(
            graphs
                .drain_grounded_tick(first.tick, false, &observed)
                .is_none()
        );
        let mut stale = observed.clone();
        stale.insert(
            first_target,
            Some(Cell {
                revision: 0,
                value: Value::Block(0),
            }),
        );
        assert!(
            graphs
                .drain_grounded_tick(first.tick, true, &stale)
                .is_none()
        );
        graphs
            .ground_anchors
            .get_mut(&first_target)
            .unwrap()
            .settled_through = TickStamp(5);
        assert!(
            graphs
                .drain_grounded_tick(first.tick, true, &observed)
                .is_none()
        );
        graphs
            .ground_anchors
            .get_mut(&first_target)
            .unwrap()
            .settled_through = TickStamp(6);
        assert_eq!(
            graphs
                .drain_grounded_tick(first.tick, true, &observed)
                .unwrap()
                .len(),
            2
        );
        assert!(!graphs.ticks.contains_key(&first.tick));
        for target in [first_target, second_target] {
            let Target::Chunk { key, .. } = target else {
                unreachable!()
            };
            assert_eq!(graphs.chunk_frontiers[&key].settled_through, first.tick);
            let snapshot = graphs.snapshot_for_chunk(key);
            let anchors = graphs.snapshot_anchors_for_graphs(key, &snapshot);
            let expected = BTreeMap::from([(target, observed[&target].clone())]);
            validate_chunk_graph_snapshot(key, first.tick, &snapshot, &anchors, &expected).unwrap();
        }
        assert_eq!(
            graphs.ground_anchors[&first_target].value,
            observed[&first_target]
        );
        assert_eq!(
            graphs.symbolic_anchor_for(TickStamp(8), first_target),
            Ok(observed[&first_target].clone())
        );
        assert_eq!(
            graphs.insert_collected(first.tick, [first]),
            Err(ApplyFailure::InvalidAction)
        );
    }

    #[test]
    fn ground_plan_never_promotes_unmaterialized_action() {
        let target = block(0, 1);
        let pending = action(1, vec![target], vec![target]);
        let mut graph = TickActionGraph::default();
        graph.insert(pending.clone()).unwrap();
        assert_eq!(
            graph.mark_stable(pending.id),
            Err(ApplyFailure::InvalidAction)
        );
        assert!(
            graph
                .ground_ready_plan(pending.tick, |_| true, |_| false)
                .components
                .is_empty()
        );
    }

    #[test]
    fn joint_ground_promotion_requires_every_realm_complete() {
        let first = block(0, 1);
        let second = block(1, 17);
        let joint = with_factual_result(action(1, vec![first, second], vec![first, second]));
        let mut graph = TickActionGraph::default();
        graph.insert(joint.clone()).unwrap();
        mark_projected_stable(&mut graph, joint.id);
        assert!(
            graph
                .ground_ready_plan(joint.tick, |realm| realm == first.action_realm(), |_| false)
                .components
                .is_empty()
        );
        assert_eq!(
            graph
                .ground_ready_plan(joint.tick, |_| true, |_| false)
                .components,
            vec![vec![ActionRef {
                tick: joint.tick,
                id: joint.id,
            }]]
        );
    }

    #[test]
    fn joint_ground_application_retains_unapplied_realm_and_freezes_resolution() {
        let first = block(0, 1);
        let second = block(1, 17);
        let joint = with_factual_result(action(1, vec![first, second], vec![first, second]));
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(joint.tick, [joint.clone()])
            .unwrap();
        mark_projected_stable(graphs.ticks.get_mut(&joint.tick).unwrap(), joint.id);
        let reference = ActionRef {
            tick: joint.tick,
            id: joint.id,
        };
        graphs
            .mark_ground_realm_applied(reference, first.action_realm())
            .unwrap();
        let node = &graphs.ticks[&joint.tick].actions[&joint.id];
        assert!(!node.is_ground_applied());
        assert_eq!(node.applied_realms, BTreeSet::from([first.action_realm()]));
        assert!(
            graphs.ticks[&joint.tick]
                .ground_ready_plan(joint.tick, |realm| realm == first.action_realm(), |_| false)
                .components
                .is_empty()
        );
        let remaining =
            graphs.ticks[&joint.tick].ground_ready_plan(joint.tick, |_| true, |_| false);
        assert_eq!(remaining.components, vec![vec![reference]]);
        assert_eq!(
            remaining.unapplied_realms.get(&reference),
            Some(&BTreeSet::from([second.action_realm()]))
        );
        assert!(
            remaining
                .writes_for_realm(&graphs.ticks[&joint.tick], first.action_realm())
                .unwrap()
                .is_empty()
        );
        let second_writes = remaining
            .writes_for_realm(&graphs.ticks[&joint.tick], second.action_realm())
            .unwrap();
        assert_eq!(second_writes.len(), 1);
        assert_eq!(second_writes[0].0, reference);
        assert_eq!(second_writes[0].1.len(), 1);
        assert_eq!(second_writes[0].1[0].target, second);
        assert_eq!(
            graphs.ticks[&joint.tick].validate_factual_replacement(
                joint.id,
                SemanticWrapper::Undone,
                &FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            ),
            Err(ApplyFailure::InvalidAction)
        );
        graphs
            .mark_ground_realm_applied(reference, second.action_realm())
            .unwrap();
        assert!(graphs.ticks[&joint.tick].actions[&joint.id].is_ground_applied());
    }

    #[test]
    fn ground_plan_excludes_applied_predecessor_from_connected_component() {
        let first_target = block(0, 1);
        let second_target = block(1, 17);
        let first = with_factual_result(action(1, vec![first_target], vec![first_target]));
        let mut second = with_factual_result(action(2, vec![second_target], vec![second_target]));
        second.dependencies.push(ActionRef {
            tick: first.tick,
            id: first.id,
        });
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(first.tick, [first.clone(), second.clone()])
            .unwrap();
        let graph = graphs.ticks.get_mut(&first.tick).unwrap();
        mark_projected_stable(graph, first.id);
        mark_projected_stable(graph, second.id);
        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: first.tick,
                    id: first.id,
                },
                first_target.action_realm(),
            )
            .unwrap();
        let plan = graphs.ticks[&first.tick].ground_ready_plan(first.tick, |_| true, |_| false);
        assert_eq!(
            plan.components,
            vec![vec![ActionRef {
                tick: second.tick,
                id: second.id,
            }]]
        );
        assert!(
            plan.writes_for_realm(&graphs.ticks[&first.tick], first_target.action_realm())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn single_chunk_snapshot_proves_only_its_applied_joint_leg() {
        let first = block(0, 1);
        let second = block(1, 17);
        let joint = with_factual_result(action(1, vec![first, second], vec![first, second]));
        let mut graphs = ActionGraphs::default();
        graphs
            .insert_collected(joint.tick, [joint.clone()])
            .unwrap();
        mark_projected_stable(graphs.ticks.get_mut(&joint.tick).unwrap(), joint.id);
        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: joint.tick,
                    id: joint.id,
                },
                first.action_realm(),
            )
            .unwrap();
        graphs.ground_anchors.insert(
            first,
            GroundAnchor {
                settled_through: TickStamp(joint.tick.0.wrapping_sub(1)),
                value: Some(Cell {
                    revision: 0,
                    value: Value::Block(0),
                }),
            },
        );
        let chunk = match first {
            Target::Chunk { key, .. } => key,
            _ => unreachable!(),
        };
        let snapshot = graphs.snapshot_for_chunk(chunk);
        let anchors = graphs.snapshot_anchors_for_graphs(chunk, &snapshot);
        assert_eq!(snapshot[0].nodes[0].applied_realms.len(), 1);
        assert!(snapshot[0].nodes[0].required_realms.len() > 1);
        let observed = BTreeMap::from([(
            first,
            Some(Cell {
                revision: 1,
                value: Value::Block(1),
            }),
        )]);
        assert_eq!(
            project_chunk_graph_snapshot_ground(chunk, joint.tick, &snapshot, &anchors).unwrap(),
            observed
        );
        validate_chunk_graph_snapshot(chunk, joint.tick, &snapshot, &anchors, &observed).unwrap();
        let mut imported = ActionGraphs::default();
        imported
            .merge_chunk_snapshot(chunk, joint.tick, &snapshot, &anchors, &observed)
            .unwrap();
        let imported_node = &imported.ticks[&joint.tick].actions[&joint.id];
        assert_eq!(
            imported_node.applied_realms,
            BTreeSet::from([first.action_realm()])
        );
        assert_eq!(imported_node.local_result.as_ref().unwrap().writes.len(), 1);
        assert_eq!(
            imported_node.factual.result.as_ref().unwrap().writes.len(),
            2
        );
        graphs
            .mark_ground_realm_applied(
                ActionRef {
                    tick: joint.tick,
                    id: joint.id,
                },
                second.action_realm(),
            )
            .unwrap();
        graphs.ground_anchors.insert(
            second,
            GroundAnchor {
                settled_through: TickStamp(joint.tick.0.wrapping_sub(1)),
                value: Some(Cell {
                    revision: 0,
                    value: Value::Block(0),
                }),
            },
        );
        let second_chunk = match second {
            Target::Chunk { key, .. } => key,
            _ => unreachable!(),
        };
        let second_snapshot = graphs.snapshot_for_chunk(second_chunk);
        let second_anchors = graphs.snapshot_anchors_for_graphs(second_chunk, &second_snapshot);
        let second_observed = BTreeMap::from([(
            second,
            Some(Cell {
                revision: 1,
                value: Value::Block(1),
            }),
        )]);
        imported
            .merge_chunk_snapshot(
                second_chunk,
                joint.tick,
                &second_snapshot,
                &second_anchors,
                &second_observed,
            )
            .unwrap();
        let imported_node = &imported.ticks[&joint.tick].actions[&joint.id];
        assert_eq!(imported_node.applied_realms, imported_node.realms);
        assert_eq!(imported_node.local_result.as_ref().unwrap().writes.len(), 2);
        assert_eq!(
            imported
                .symbolic_anchor_for(TickStamp(joint.tick.0.wrapping_add(1)), first)
                .unwrap(),
            observed[&first]
        );
        let changed = BTreeMap::from([(
            first,
            Some(Cell {
                revision: 1,
                value: Value::Block(2),
            }),
        )]);
        assert_eq!(
            validate_chunk_graph_snapshot(chunk, joint.tick, &snapshot, &anchors, &changed),
            Err(ApplyFailure::InvalidAction)
        );
        let mut rejected = ActionGraphs::default();
        assert_eq!(
            rejected.merge_chunk_snapshot(chunk, joint.tick, &snapshot, &anchors, &changed),
            Err(ApplyFailure::InvalidAction)
        );
        assert!(rejected.ticks.is_empty());
        let mut conflicting_local = graphs.clone();
        let conflict_node = conflicting_local
            .ticks
            .get_mut(&joint.tick)
            .unwrap()
            .actions
            .get_mut(&joint.id)
            .unwrap();
        let mut old_projection = conflict_node.local_result.clone().unwrap();
        old_projection.writes[0].after = Some(Cell {
            revision: 9,
            value: Value::Block(9),
        });
        conflict_node.local_result = Some(old_projection.clone());
        assert_eq!(
            conflicting_local
                .merge_chunk_snapshot(chunk, joint.tick, &snapshot, &anchors, &observed,),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(
            conflicting_local.ticks[&joint.tick].actions[&joint.id].local_result,
            Some(old_projection)
        );
        assert_eq!(
            project_chunk_graph_snapshot_ground(chunk, joint.tick, &snapshot, &[]),
            Err(ApplyFailure::InvalidAction)
        );
    }

    #[test]
    fn chunk_frontier_seeds_untouched_target_without_skipping_late_action() {
        let target = block(0, 1);
        let chunk = match target {
            Target::Chunk { key, .. } => key,
            _ => unreachable!(),
        };
        let issued = action(1, vec![target], vec![target]);
        let mut graphs = ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 3,
                    settled_through: TickStamp(issued.tick.0.wrapping_sub(1)),
                },
            )
            .unwrap();
        let baseline = Some(Cell {
            revision: 0,
            value: Value::Block(0),
        });
        graphs
            .seed_chunk_target_anchor(target, chunk, baseline.clone())
            .unwrap();
        graphs
            .insert_collected(issued.tick, [issued.clone()])
            .unwrap();
        assert_eq!(
            graphs.symbolic_anchor_for(issued.tick, target),
            Ok(baseline)
        );
        assert_eq!(
            graphs.install_chunk_frontier(
                chunk,
                ChunkGroundFrontier {
                    epoch: 3,
                    settled_through: TickStamp(issued.tick.0.wrapping_add(1)),
                },
            ),
            Err(ApplyFailure::InvalidAction)
        );
    }

    #[test]
    fn independent_transition_ground_waits_for_all_ordinary_actions() {
        let ordinary_target = block(0, 1);
        let transition_target = block(1, 17);
        let ordinary = with_factual_result(action(1, vec![ordinary_target], vec![ordinary_target]));
        let mut transition =
            with_factual_result(action(2, vec![transition_target], vec![transition_target]));
        transition.class = crate::canonical_action::ActionClass::Infallible;
        transition.chunk_transition = true;
        transition.conditions.clear();
        let mut graph = TickActionGraph::default();
        graph.insert(ordinary.clone()).unwrap();
        graph.insert(transition.clone()).unwrap();
        mark_projected_stable(&mut graph, ordinary.id);
        mark_projected_stable(&mut graph, transition.id);
        assert!(
            graph
                .ground_ready_plan(
                    ordinary.tick,
                    |realm| realm == transition_target.action_realm(),
                    |_| false,
                )
                .components
                .is_empty()
        );
        assert_eq!(
            graph
                .ground_ready_plan(ordinary.tick, |_| true, |_| false)
                .components,
            vec![
                vec![ActionRef {
                    tick: ordinary.tick,
                    id: ordinary.id,
                }],
                vec![ActionRef {
                    tick: transition.tick,
                    id: transition.id,
                }],
            ],
        );
        graph
            .actions
            .get_mut(&ordinary.id)
            .unwrap()
            .applied_realms
            .insert(ordinary_target.action_realm());
        assert_eq!(
            graph
                .ground_ready_plan(ordinary.tick, |_| true, |_| false)
                .components,
            vec![vec![ActionRef {
                tick: transition.tick,
                id: transition.id,
            }]],
        );
    }

    #[test]
    fn ground_components_follow_declared_dependency_before_id_order() {
        let first_target = block(0, 1);
        let second_target = block(1, 17);
        let producer = with_factual_result(action(2, vec![first_target], vec![first_target]));
        let mut consumer = action(1, vec![second_target], vec![second_target]);
        consumer.dependencies.push(ActionRef {
            tick: producer.tick,
            id: producer.id,
        });
        let consumer = with_factual_result(consumer);
        let mut graph = TickActionGraph::default();
        graph.insert(consumer.clone()).unwrap();
        graph.insert(producer.clone()).unwrap();
        mark_projected_stable(&mut graph, producer.id);
        mark_projected_stable(&mut graph, consumer.id);
        assert_eq!(
            graph
                .ground_ready_plan(producer.tick, |_| true, |_| false)
                .components,
            vec![
                vec![ActionRef {
                    tick: producer.tick,
                    id: producer.id,
                }],
                vec![ActionRef {
                    tick: consumer.tick,
                    id: consumer.id,
                }],
            ],
        );
    }

    #[test]
    fn counterfactual_blocker_waits_for_cross_tick_dependency() {
        let source = block(0, 1);
        let possible = block(1, 17);
        let ordinary = with_factual_result(action(1, vec![possible], vec![possible]));
        let mut replayable = action(2, vec![source], vec![source]);
        replayable.counterfactual_conditions.push(possible);
        replayable.counterfactual_effects.push(possible);
        replayable.stakeholders.push(possible.stakeholder());
        replayable.dependencies.push(ActionRef {
            tick: TickStamp(6),
            id: action(3, vec![source], vec![source]).id,
        });
        let replayable = with_factual_result(replayable);
        let mut graph = TickActionGraph::default();
        graph.insert(ordinary.clone()).unwrap();
        graph.insert(replayable.clone()).unwrap();
        mark_projected_stable(&mut graph, ordinary.id);
        mark_projected_stable(&mut graph, replayable.id);
        let incomplete = graph.ground_ready_plan(ordinary.tick, |_| true, |_| false);
        assert!(incomplete.components.is_empty());
        let complete = graph.ground_ready_plan(ordinary.tick, |_| true, |_| true);
        assert_eq!(complete.components.len(), 2);
    }

    #[test]
    fn undone_action_cannot_ground_before_joint_conflict_is_complete() {
        let shared = block(0, 1);
        let remote = block(1, 17);
        let single = action(1, vec![shared], vec![shared]);
        let joint = with_factual_result(action(2, vec![shared, remote], vec![shared, remote]));
        let mut graph = TickActionGraph::default();
        graph.insert(single.clone()).unwrap();
        graph.insert(joint.clone()).unwrap();
        graph
            .replace_factual(
                single.id,
                SemanticWrapper::Undone,
                FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            )
            .unwrap();
        mark_projected_stable(&mut graph, single.id);
        mark_projected_stable(&mut graph, joint.id);
        assert!(graph.counterfactual_blockers(single.id).contains(&joint.id));
        assert!(
            graph
                .ground_ready_plan(
                    single.tick,
                    |realm| realm == shared.action_realm(),
                    |_| false,
                )
                .components
                .is_empty()
        );
    }

    #[test]
    fn empty_tick_frontier_catches_up_only_unchanged_ground_anchor() {
        let target = block(0, 1);
        let Target::Chunk { key, .. } = target else {
            unreachable!()
        };
        let value = Some(Cell {
            revision: 4,
            value: Value::Block(3),
        });
        let mut graphs = ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                key,
                ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(6),
                },
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                target,
                GroundAnchor {
                    settled_through: TickStamp(6),
                    value: value.clone(),
                },
            )
            .unwrap();
        assert!(!graphs.advance_empty_chunk_frontier(TickStamp(7), key, false));
        assert!(graphs.advance_empty_chunk_frontier(TickStamp(7), key, true));
        assert!(!graphs.catch_up_chunk_target_anchor(
            target,
            Some(Cell {
                revision: 5,
                value: Value::Block(3),
            }),
        ));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(6));
        assert!(graphs.catch_up_chunk_target_anchor(target, value));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(7));
    }

    #[test]
    fn empty_player_tick_requires_its_own_frontier_and_exact_ground_cell() {
        let key = PlayerKey(uuid::Uuid::from_u128(104));
        let lease = crate::canonical_action::LivePlayerLease {
            key,
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(4),
            },
            incarnation: 9,
        };
        let target = Target::Player {
            key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv: 0, slot: 2 }),
        };
        let mut graphs = ActionGraphs::default();
        let frontier_key = (
            key,
            InventoryRealmId::Player(0),
            lease.gid,
            lease.incarnation,
        );
        graphs
            .install_player_inventory_frontier(
                InventoryRealmId::Player(0),
                PlayerInventoryGroundFrontier {
                    lease,
                    accepted_chunk: ChunkKey::new(WorldId::new(0, 0), 0, 0),
                    domain_epoch: lease.incarnation,
                    settled_through: TickStamp(6),
                },
            )
            .unwrap();
        let value = Some(Cell {
            revision: 3,
            value: Value::Inventory(crate::inventory::InventoryCell::empty(
                crate::inventory::InvLoc { inv: 0, slot: 2 },
            )),
        });
        graphs
            .seed_player_inventory_target_anchor(target, lease, value.clone())
            .unwrap();
        assert!(
            !graphs.advance_empty_player_inventory_frontier(TickStamp(7), frontier_key, false,)
        );
        assert!(graphs.advance_empty_player_inventory_frontier(TickStamp(7), frontier_key, true,));
        let changed_revision = value.clone().map(|mut cell| {
            cell.revision += 1;
            cell
        });
        assert!(!graphs.catch_up_player_target_anchor(target, changed_revision));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(6));
        assert!(graphs.catch_up_player_target_anchor(target, value));
        assert_eq!(graphs.ground_anchors[&target].settled_through, TickStamp(7));
    }
}
