use std::collections::{BTreeMap, BTreeSet};

use crate::action_graph::{
    ActionGraphs, ActionRealm, DeltaFailure, FactualVariant, GroundReadyPlan, SemanticDelta,
    SemanticWrapper, StagedLocalDeltas, SymbolicAnchorFailure, TickActionGraph,
    stage_backward_local_deltas,
};
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionRef, ActionState, AppliedAction, AppliedWrite,
    ApplyFailure, Cell, Condition, Effect, Expected, OwnedStakeholderSet, SemanticIntent, Target,
    TruthView, Value, prepare_action,
};
use crate::order::order_actors;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SolveFailure {
    MissingAction(ActionId),
    MissingAnchor(Target),
    MissingDependency(ActionRef),
    InfallibleConflict(ActionId),
    PriorUnsettled(ActionRef),
    InconsistentAnchor(Target),
    InvalidComponent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SolvedVariant {
    Applied {
        conditions: Vec<Condition>,
        effects: Vec<Effect>,
        result: AppliedAction,
        replayed: bool,
    },
    Undone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SolvedComponent {
    pub component: BTreeSet<ActionId>,
    pub variants: BTreeMap<ActionId, SolvedVariant>,
    pub causal_order: Vec<ActionId>,
}

#[derive(Default)]
pub struct CertifiedRecoveryConstraints {
    outcomes: BTreeMap<ActionRef, RecoveryPinnedOutcome>,
}

struct RecoveryPinnedOutcome {
    core: Action,
    accepted: bool,
    realms: BTreeSet<ActionRealm>,
    conditions: Vec<Condition>,
    writes: Vec<AppliedWrite>,
}

impl CertifiedRecoveryConstraints {
    pub(crate) fn validate_replacement(
        &self,
        node: &crate::action_graph::GraphAction,
        factual: &FactualVariant,
    ) -> Result<(), ApplyFailure> {
        let reference = ActionRef {
            tick: node.action.tick,
            id: node.action.id,
        };
        let Some(pin) = self.outcomes.get(&reference) else {
            return if node.applied_realms.is_empty() || node.factual == *factual {
                Ok(())
            } else {
                Err(ApplyFailure::InvalidAction)
            };
        };
        if pin.core != node.action || pin.accepted != factual.result.is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        let conditions = |factual: &FactualVariant, realms: &BTreeSet<ActionRealm>| {
            factual
                .conditions
                .iter()
                .filter(|condition| realms.contains(&condition.target.action_realm()))
                .cloned()
                .collect::<Vec<_>>()
        };
        let writes = |factual: &FactualVariant, realms: &BTreeSet<ActionRealm>| {
            factual
                .result
                .iter()
                .flat_map(|result| &result.writes)
                .filter(|write| realms.contains(&write.target.action_realm()))
                .cloned()
                .collect::<Vec<_>>()
        };
        if conditions(factual, &pin.realms) != pin.conditions
            || writes(factual, &pin.realms) != pin.writes
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let other = node
            .applied_realms
            .difference(&pin.realms)
            .copied()
            .collect::<BTreeSet<_>>();
        if !other.is_empty()
            && (node.factual.result.is_some() != factual.result.is_some()
                || conditions(&node.factual, &other) != conditions(factual, &other)
                || writes(&node.factual, &other) != writes(factual, &other))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }
    pub fn from_history(
        chunk: crate::protocol::ChunkKey,
        frame: &crate::entity_history::EntityHistoryEvidence,
        proofs: Option<&crate::entity_history::EntityBindingProofContext>,
    ) -> Result<Self, ApplyFailure> {
        frame.validate_with_context(proofs)?;
        let actions = frame
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
        let mut ledger = crate::realm_completion::RealmCompletionLedger::default();
        ledger
            .import_snapshot(&frame.completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let mut outcomes = BTreeMap::new();
        for graph in &frame.graphs {
            for node in &graph.nodes {
                let reference = ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                };
                let interpreted = if let Some(binding) = &node.actor_binding {
                    binding.interpret_core(&node.action)?
                } else {
                    node.action.clone()
                };
                let realms = node
                    .applied_realms
                    .iter()
                    .copied()
                    .filter(|realm| {
                        crate::action_graph::action_realm_held_by_chunk(&interpreted, *realm, chunk)
                    })
                    .collect::<BTreeSet<_>>();
                if realms.is_empty() {
                    continue;
                }
                if !ledger.action_complete(reference)
                    || !node
                        .required_realms
                        .iter()
                        .all(|realm| ledger.realm_complete(graph.tick, *realm))
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let writes = node
                    .factual
                    .result
                    .as_ref()
                    .into_iter()
                    .flat_map(|result| &result.writes)
                    .filter(|write| realms.contains(&write.target.action_realm()))
                    .cloned()
                    .collect();
                let conditions = node
                    .factual
                    .conditions
                    .iter()
                    .filter(|condition| realms.contains(&condition.target.action_realm()))
                    .cloned()
                    .collect();
                outcomes.insert(
                    reference,
                    RecoveryPinnedOutcome {
                        core: node.action.clone(),
                        accepted: node.factual.result.is_some(),
                        realms,
                        conditions,
                        writes,
                    },
                );
            }
        }
        Ok(Self { outcomes })
    }
}

pub struct SemanticResolutionPass {
    pub graphs: ActionGraphs,
    pub deltas: Vec<SemanticDelta>,
    pub touched: Vec<ActionRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionPassFailure {
    Graph(ApplyFailure),
    Solve(SolveFailure),
}

pub struct PreparedLocalTurn {
    pub graphs: ActionGraphs,
    pub deltas: StagedLocalDeltas,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalTurnFailure {
    Resolution(ResolutionPassFailure),
    Delta(DeltaFailure),
    Graph(ApplyFailure),
}

pub struct StagedGroundDeltas {
    pub writes: Vec<AppliedWrite>,
    pub actions: Vec<(ActionRef, Vec<AppliedWrite>)>,
    pub applied_realms: Vec<(ActionRef, ActionRealm)>,
}

impl StagedGroundDeltas {
    pub fn validate(&self) -> bool {
        let mut folded = BTreeMap::<Target, AppliedWrite>::new();
        let mut actions = BTreeSet::new();
        let mut realms = BTreeSet::new();
        for (reference, writes) in &self.actions {
            if !actions.insert(*reference) {
                return false;
            }
            for write in writes {
                let Some(entry) = folded.get_mut(&write.target) else {
                    folded.insert(write.target, write.clone());
                    continue;
                };
                if entry.after != write.before {
                    return false;
                }
                entry.after = write.after.clone();
            }
        }
        for (reference, realm) in &self.applied_realms {
            if !actions.contains(reference) || !realms.insert((*reference, *realm)) {
                return false;
            }
        }
        if self.actions.iter().any(|(reference, writes)| {
            writes
                .iter()
                .any(|write| !realms.contains(&(*reference, write.target.action_realm())))
        }) {
            return false;
        }
        folded.into_values().collect::<Vec<_>>() == self.writes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroundDeltaFailure {
    MissingAction(ActionRef),
    MissingTarget(Target),
    ChangedGround(Target),
    Commit(ApplyFailure),
}

pub fn stage_ground_ready<S: ActionState>(
    graph: &TickActionGraph,
    plan: &GroundReadyPlan,
    held: impl Fn(ActionRealm) -> bool,
    owners: &S,
) -> Result<StagedGroundDeltas, GroundDeltaFailure> {
    let mut before = BTreeMap::<Target, Option<Cell>>::new();
    let mut after = BTreeMap::<Target, Option<Cell>>::new();
    let mut actions = Vec::new();
    let mut applied_realms = Vec::new();
    for reference in plan.components.iter().flatten() {
        let node = graph
            .actions
            .get(&reference.id)
            .filter(|node| node.action.tick == reference.tick && node.stable)
            .ok_or(GroundDeltaFailure::MissingAction(*reference))?;
        if plan.semantic_generations.get(reference) != node.semantic_generation.as_ref() {
            return Err(GroundDeltaFailure::MissingAction(*reference));
        }
        let realms = plan
            .unapplied_realms
            .get(reference)
            .ok_or(GroundDeltaFailure::MissingAction(*reference))?;
        if *realms
            != node
                .realms
                .difference(&node.applied_realms)
                .copied()
                .collect::<BTreeSet<_>>()
        {
            return Err(GroundDeltaFailure::MissingAction(*reference));
        }
        for realm in realms {
            if held(*realm) {
                applied_realms.push((*reference, *realm));
            }
        }
        let Some(result) = node.factual.result.as_ref() else {
            actions.push((*reference, Vec::new()));
            continue;
        };
        for condition in &node.factual.conditions {
            if !realms.contains(&condition.target.action_realm())
                || !held(condition.target.action_realm())
            {
                continue;
            }
            let current = if let Some(value) = after.get(&condition.target) {
                value.clone()
            } else {
                if !owners.available(TruthView::Ground, condition.target) {
                    return Err(GroundDeltaFailure::MissingTarget(condition.target));
                }
                owners.read(TruthView::Ground, condition.target)
            };
            if !condition.expected.matches(current.as_ref()) {
                return Err(GroundDeltaFailure::ChangedGround(condition.target));
            }
        }
        let mut action_writes = Vec::new();
        for write in &result.writes {
            if !realms.contains(&write.target.action_realm()) || !held(write.target.action_realm())
            {
                continue;
            }
            let current = if let Some(value) = after.get(&write.target) {
                value.clone()
            } else {
                if !owners.available(TruthView::Ground, write.target) {
                    return Err(GroundDeltaFailure::MissingTarget(write.target));
                }
                let value = owners.read(TruthView::Ground, write.target);
                before.insert(write.target, value.clone());
                value
            };
            if current != write.before {
                return Err(GroundDeltaFailure::ChangedGround(write.target));
            }
            action_writes.push(write.clone());
            let next = if matches!(write.target, Target::Chunk { part: crate::canonical_action::ChunkPart::ActorOccupancy, inventory: None, .. }) {
                let current = current.as_ref().ok_or(GroundDeltaFailure::ChangedGround(write.target))?;
                Some(crate::actor_occupancy::materialize_write(write, current).ok_or(GroundDeltaFailure::ChangedGround(write.target))?)
            } else { write.after.clone() };
            after.insert(write.target, next);
        }
        actions.push((*reference, action_writes));
    }
    let writes = after
        .into_iter()
        .map(|(target, after)| AppliedWrite {
            target,
            before: before.remove(&target).unwrap_or(None),
            after,
        })
        .map(|write| crate::actor_occupancy::compact_write(write).ok_or(GroundDeltaFailure::Commit(ApplyFailure::InvalidAction)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StagedGroundDeltas {
        writes,
        actions,
        applied_realms,
    })
}

pub fn prepare_local_turn<S: ActionState>(
    graphs: &ActionGraphs,
    current_tick: TickStamp,
    changed: impl IntoIterator<Item = ActionRef>,
    cluster_seed: u64,
    held: impl Fn(ActionRealm) -> bool,
    owners: &S,
) -> Result<PreparedLocalTurn, LocalTurnFailure> {
    let pass = resolve_collected_actions_with_context(
        graphs,
        current_tick,
        changed,
        cluster_seed,
        held,
        owners,
    )
    .map_err(LocalTurnFailure::Resolution)?;
    let deltas = stage_backward_local_deltas(owners, current_tick, &pass.deltas)
        .map_err(LocalTurnFailure::Delta)?;
    let mut staged = pass.graphs;
    for reference in pass.touched {
        let graph = staged
            .ticks
            .get_mut(&reference.tick)
            .ok_or(LocalTurnFailure::Graph(ApplyFailure::InvalidAction))?;
        let node = graph
            .actions
            .get(&reference.id)
            .ok_or(LocalTurnFailure::Graph(ApplyFailure::InvalidAction))?;
        if node.semantic_generation != Some(node.generation) {
            continue;
        }
        if node.local_generation != Some(node.generation) {
            let order = node.semantic_order;
            let previous = node.local_result.clone();
            match (order, deltas.applied.get(&reference).or(previous.as_ref())) {
                (Some(order), Some(applied)) => graph
                    .mark_local_applied(reference.id, order, applied.clone())
                    .map_err(LocalTurnFailure::Graph)?,
                (None, None) => graph
                    .mark_local_undone(reference.id)
                    .map_err(LocalTurnFailure::Graph)?,
                _ => return Err(LocalTurnFailure::Graph(ApplyFailure::InvalidAction)),
            }
        }
        graph
            .mark_stable(reference.id)
            .map_err(LocalTurnFailure::Graph)?;
    }
    Ok(PreparedLocalTurn {
        graphs: staged,
        deltas,
    })
}

pub fn resolve_collected_actions(
    graphs: &ActionGraphs,
    current_tick: TickStamp,
    changed: impl IntoIterator<Item = ActionRef>,
    cluster_seed: u64,
    held: impl Fn(ActionRealm) -> bool,
) -> Result<SemanticResolutionPass, ResolutionPassFailure> {
    resolve_collected_actions_with_context(
        graphs,
        current_tick,
        changed,
        cluster_seed,
        held,
        &OwnedStakeholderSet::default(),
    )
}

pub fn resolve_collected_actions_with_context<S: ActionState>(
    graphs: &ActionGraphs,
    current_tick: TickStamp,
    changed: impl IntoIterator<Item = ActionRef>,
    cluster_seed: u64,
    held: impl Fn(ActionRealm) -> bool,
    context: &S,
) -> Result<SemanticResolutionPass, ResolutionPassFailure> {
    let mut staged = graphs.clone();
    let cascade = staged
        .cascade_to_current(current_tick, changed)
        .map_err(ResolutionPassFailure::Graph)?;
    let mut touched = BTreeSet::new();
    for (tick, references) in cascade {
        let mut seeds = references
            .iter()
            .map(|reference| reference.id)
            .collect::<BTreeSet<_>>();
        let mut visited = Vec::new();
        loop {
            let components = staged
                .ticks
                .get(&tick)
                .ok_or(ResolutionPassFailure::Solve(SolveFailure::InvalidComponent))?
                .affected_components(&seeds);
            if components.iter().all(|component| {
                component.iter().all(|id| {
                    let node = &staged.ticks[&tick].actions[id];
                    node.semantic_generation == Some(node.generation)
                })
            }) {
                touched.extend(
                    components
                        .iter()
                        .flat_map(|component| component.iter())
                        .map(|id| ActionRef { tick, id: *id }),
                );
                break;
            }
            let signature = components
                .iter()
                .flat_map(|component| component.iter())
                .map(|id| {
                    let node = &staged.ticks[&tick].actions[id];
                    (*id, node.wrapper, node.factual.clone())
                })
                .collect::<Vec<_>>();
            if visited.contains(&signature) {
                return Err(ResolutionPassFailure::Solve(SolveFailure::InvalidComponent));
            }
            visited.push(signature);
            let mut resolved = false;
            for component in components {
                touched.extend(component.iter().map(|id| ActionRef { tick, id: *id }));
                if component.iter().all(|id| {
                    let node = &staged.ticks[&tick].actions[id];
                    node.semantic_generation == Some(node.generation)
                }) {
                    continue;
                }
                let solved = solve_indexed_component_with_context(
                    &staged,
                    tick,
                    &component,
                    cluster_seed,
                    |dependency| staged.dependency_outcome(dependency),
                    context,
                )
                .map_err(ResolutionPassFailure::Solve)?;
                seeds.extend(
                    install_solved_component(staged.ticks.get_mut(&tick).unwrap(), &solved)
                        .map_err(ResolutionPassFailure::Graph)?,
                );
                resolved = true;
            }
            if !resolved {
                break;
            }
        }
    }
    let mut deltas = Vec::new();
    for reference in &touched {
        let original = &graphs.ticks[&reference.tick].actions[&reference.id];
        if original.local_result.as_ref().is_some_and(|result| {
            result
                .writes
                .iter()
                .any(|write| !held(write.target.action_realm()))
        }) {
            return Err(ResolutionPassFailure::Graph(ApplyFailure::InvalidAction));
        }
        let next = &staged.ticks[&reference.tick].actions[&reference.id];
        let next_result = next.factual.result.clone().map(|mut result| {
            result
                .writes
                .retain(|write| held(write.target.action_realm()));
            result
        });
        let next_order = next_result.as_ref().and(next.semantic_order);
        if original.local_result != next_result || original.local_order != next_order {
            deltas.push(SemanticDelta {
                reference: *reference,
                previous_order: original.local_order,
                next_order,
                core: original.action.clone(),
                previous: original.local_result.clone(),
                next: next_result,
                next_conditions: next
                    .factual
                    .conditions
                    .iter()
                    .filter(|condition| held(condition.target.action_realm()))
                    .cloned()
                    .collect(),
            });
        }
    }
    Ok(SemanticResolutionPass {
        graphs: staged,
        deltas,
        touched: touched.into_iter().collect(),
    })
}

pub fn solve_indexed_component(
    graphs: &ActionGraphs,
    tick: TickStamp,
    component: &BTreeSet<ActionId>,
    cluster_seed: u64,
    dependency_accepted: impl Fn(ActionRef) -> Option<bool>,
) -> Result<SolvedComponent, SolveFailure> {
    solve_indexed_component_with_context(
        graphs,
        tick,
        component,
        cluster_seed,
        dependency_accepted,
        &OwnedStakeholderSet::default(),
    )
}

pub fn solve_indexed_component_with_context<S: ActionState>(
    graphs: &ActionGraphs,
    tick: TickStamp,
    component: &BTreeSet<ActionId>,
    cluster_seed: u64,
    dependency_accepted: impl Fn(ActionRef) -> Option<bool>,
    context: &S,
) -> Result<SolvedComponent, SolveFailure> {
    let graph = graphs
        .ticks
        .get(&tick)
        .ok_or(SolveFailure::InvalidComponent)?;
    let mut targets = BTreeSet::new();
    for id in component {
        let action = graph
            .actions
            .get(id)
            .ok_or(SolveFailure::MissingAction(*id))?
            .semantic_action();
        if action.tick != tick {
            return Err(SolveFailure::InvalidComponent);
        }
        targets.extend(action.targets());
        targets.extend(action.counterfactual_conditions.iter().copied());
        targets.extend(action.counterfactual_effects.iter().copied());
    }
    let bodies = targets
        .iter()
        .filter_map(|target| crate::derived_living::actor_body_target(*target)
            .or_else(|| crate::frame_inventory_projection::body_target(*target)))
        .collect::<BTreeSet<_>>();
    targets.extend(bodies);
    let anchors = targets
        .into_iter()
        .map(|target| {
            graphs
                .symbolic_anchor_for(tick, target)
                .map(|value| (target, value))
                .map_err(|failure| match failure {
                    SymbolicAnchorFailure::MissingGround(target) => {
                        SolveFailure::MissingAnchor(target)
                    }
                    SymbolicAnchorFailure::Unsettled(reference) => {
                        SolveFailure::PriorUnsettled(reference)
                    }
                    SymbolicAnchorFailure::Inconsistent(_, target) => {
                        SolveFailure::InconsistentAnchor(target)
                    }
                    SymbolicAnchorFailure::InvalidTick => SolveFailure::InvalidComponent,
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    solve_component_with_context(
        graph,
        component,
        &anchors,
        cluster_seed,
        dependency_accepted,
        context,
    )
}

fn solved_order(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
) -> Result<BTreeMap<ActionId, u32>, ApplyFailure> {
    if solved.component.is_empty()
        || solved.variants.keys().copied().collect::<BTreeSet<_>>() != solved.component
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let accepted = solved
        .variants
        .iter()
        .filter_map(|(id, variant)| matches!(variant, SolvedVariant::Applied { .. }).then_some(*id))
        .collect::<BTreeSet<_>>();
    let order = solved
        .causal_order
        .iter()
        .enumerate()
        .map(|(index, id)| {
            Ok((
                *id,
                u32::try_from(index).map_err(|_| ApplyFailure::InvalidAction)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    if order.len() != accepted.len() || order.keys().copied().collect::<BTreeSet<_>>() != accepted {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut seen_transition = false;
    for id in &solved.causal_order {
        let action = &graph
            .actions
            .get(id)
            .ok_or(ApplyFailure::InvalidAction)?
            .action;
        if action.chunk_transition {
            seen_transition = true;
        } else if seen_transition {
            return Err(ApplyFailure::InvalidAction);
        }
        if action.dependencies.iter().any(|dependency| {
            solved.component.contains(&dependency.id)
                && dependency.tick == action.tick
                && order
                    .get(&dependency.id)
                    .is_none_or(|prior| *prior >= order[id])
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    Ok(order)
}

fn prepared_replacements(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
) -> Result<
    (
        BTreeMap<ActionId, u32>,
        Vec<(ActionId, SemanticWrapper, FactualVariant)>,
    ),
    ApplyFailure,
> {
    prepared_replacements_recovery(graph, solved, None)
}

fn prepared_replacements_recovery(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
    recovery: Option<&CertifiedRecoveryConstraints>,
) -> Result<
    (
        BTreeMap<ActionId, u32>,
        Vec<(ActionId, SemanticWrapper, FactualVariant)>,
    ),
    ApplyFailure,
> {
    let order = solved_order(graph, solved)?;
    let mut replacements = Vec::new();
    for (id, variant) in &solved.variants {
        let prior = graph.actions.get(id).ok_or(ApplyFailure::InvalidAction)?;
        let (wrapper, factual) = match variant {
            SolvedVariant::Undone => (
                SemanticWrapper::Undone,
                FactualVariant {
                    conditions: Vec::new(),
                    effects: Vec::new(),
                    result: None,
                },
            ),
            SolvedVariant::Applied {
                conditions,
                effects,
                result,
                replayed,
            } => (
                if *replayed {
                    SemanticWrapper::Replayed
                } else if prior.wrapper == SemanticWrapper::Undone {
                    SemanticWrapper::Redone
                } else {
                    SemanticWrapper::Applied
                },
                FactualVariant {
                    conditions: conditions.clone(),
                    effects: effects.clone(),
                    result: Some(result.clone()),
                },
            ),
        };
        if let Some(recovery) = recovery {
            graph.validate_factual_replacement_for_recovery(*id, wrapper, &factual, recovery)?;
        } else {
            graph.validate_factual_replacement(*id, wrapper, &factual)?;
        }
        replacements.push((*id, wrapper, factual));
    }
    Ok((order, replacements))
}

pub fn semantic_deltas(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
) -> Result<Vec<SemanticDelta>, ApplyFailure> {
    semantic_deltas_recovery(graph, solved, None)
}

pub fn semantic_deltas_for_recovery(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
    recovery: &CertifiedRecoveryConstraints,
) -> Result<Vec<SemanticDelta>, ApplyFailure> {
    semantic_deltas_recovery(graph, solved, Some(recovery))
}

fn semantic_deltas_recovery(
    graph: &TickActionGraph,
    solved: &SolvedComponent,
    recovery: Option<&CertifiedRecoveryConstraints>,
) -> Result<Vec<SemanticDelta>, ApplyFailure> {
    let (order, _) = prepared_replacements_recovery(graph, solved, recovery)?;
    let mut deltas = Vec::new();
    for (id, variant) in &solved.variants {
        let prior = graph.actions.get(id).ok_or(ApplyFailure::InvalidAction)?;
        let previous = prior.local_result.clone();
        if prior.local_order.is_some() != previous.is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        let (next, next_order, next_conditions) = match variant {
            SolvedVariant::Undone => (None, None, Vec::new()),
            SolvedVariant::Applied {
                result, conditions, ..
            } => (
                Some(result.clone()),
                Some(*order.get(id).ok_or(ApplyFailure::InvalidAction)?),
                conditions.clone(),
            ),
        };
        if previous != next || prior.local_order != next_order {
            deltas.push(SemanticDelta {
                reference: ActionRef {
                    tick: prior.action.tick,
                    id: *id,
                },
                previous_order: prior.local_order,
                next_order,
                core: prior.action.clone(),
                previous,
                next,
                next_conditions,
            });
        }
    }
    Ok(deltas)
}

pub fn install_solved_component(
    graph: &mut TickActionGraph,
    solved: &SolvedComponent,
) -> Result<BTreeSet<ActionId>, ApplyFailure> {
    let (order, replacements) = prepared_replacements(graph, solved)?;
    let mut changed = BTreeSet::new();
    for (id, wrapper, factual) in replacements {
        let prior = &graph.actions[&id];
        if prior.wrapper != wrapper || prior.factual != factual {
            changed.extend(graph.replace_factual(id, wrapper, factual)?);
        }
    }
    for id in &solved.component {
        let node = graph
            .actions
            .get_mut(id)
            .ok_or(ApplyFailure::InvalidAction)?;
        node.semantic_generation = Some(node.generation);
        node.semantic_order = order.get(id).copied();
    }
    Ok(changed)
}

pub fn install_solved_component_for_recovery(
    graph: &mut TickActionGraph,
    solved: &SolvedComponent,
    recovery: &CertifiedRecoveryConstraints,
) -> Result<BTreeSet<ActionId>, ApplyFailure> {
    let (order, replacements) = prepared_replacements_recovery(graph, solved, Some(recovery))?;
    let mut changed = BTreeSet::new();
    for (id, wrapper, factual) in replacements {
        let prior = &graph.actions[&id];
        if prior.wrapper != wrapper || prior.factual != factual {
            changed.extend(graph.replace_factual_for_recovery(id, wrapper, factual, recovery)?);
        }
    }
    for id in &solved.component {
        let node = graph
            .actions
            .get_mut(id)
            .ok_or(ApplyFailure::InvalidAction)?;
        node.semantic_generation = Some(node.generation);
        node.semantic_order = order.get(id).copied();
    }
    Ok(changed)
}

#[derive(Clone)]
struct AcceptedVariant {
    action: Action,
    result: AppliedAction,
    replayed: bool,
}

#[derive(Clone)]
struct SearchState {
    mirror: OwnedStakeholderSet,
    remaining: BTreeSet<ActionId>,
    accepted: BTreeMap<ActionId, AcceptedVariant>,
    order: Vec<ActionId>,
}

struct RegistrySemanticState<'a, S> {
    mirror: &'a OwnedStakeholderSet,
    context: &'a S,
    parent_charges: Option<&'a BTreeMap<ActionRef, u64>>,
}

impl<S: ActionState> ActionState for RegistrySemanticState<'_, S> {
    fn wake_rest_stat(&self) -> Result<(u16, u32), crate::canonical_action::ApplyFailure> { self.context.wake_rest_stat() }

    fn validate_wake_beds(&self, world: crate::protocol::WorldId, sleeping: crate::item_action::PlayerSleepingState,
        beds: &[crate::sleep_action::BedRelease]) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.context.validate_wake_beds(world, sleeping, beds)
    }

    fn validate_morning(&self, world: crate::protocol::WorldId, morning: crate::world_time::MorningTransition, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.context.validate_morning(world, morning, source)
    }

    fn world_tool_companion(&self, location: crate::world_command::BlockLocation, current: u16) -> Result<Option<crate::world_command::ToolCompanion>, ApplyFailure> {
        self.context.world_tool_companion(location, current)
    }
    fn world_tool_attributes(&self) -> Result<crate::world_command::ToolAttributes, ApplyFailure> {
        self.context.world_tool_attributes()
    }
    fn world_tool_transform(&self, operation: crate::world_command::ToolOperation, current: u16, item: u16) -> Result<Option<u16>, ApplyFailure> {
        self.context.world_tool_transform(operation, current, item)
    }
    fn world_tool_eye_height(&self, living: &crate::item_action::ActorLivingState) -> Result<f64, ApplyFailure> {
        self.context.world_tool_eye_height(living)
    }
    fn world_tool_light_plans(&self, location: crate::world_command::BlockLocation, operation: crate::world_command::ToolOperation, current: u16) -> Result<Vec<crate::world_action::WorldLightPlan>, ApplyFailure> {
        self.context.world_tool_light_plans(location, operation, current)
    }
    fn actor_occupancy_cut(&self, target: Target, value: &Value) -> Result<(crate::realm_completion::PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        self.context.actor_occupancy_cut(target, value)
    }
    fn actor_collision_shape(&self, target: Target, value: &Value) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> { self.context.actor_collision_shape(target, value) }
    fn validate_world_border(&self, witness: crate::world_time::WorldBorderWitness, source: crate::canonical_action::ActionRef) -> Result<(), crate::canonical_action::ApplyFailure> {
        self.context.validate_world_border(witness, source)
    }

    fn combat_parent_damaged(&self, parent: ActionRef, victim: &crate::item_contact::ContactIdentity) -> Option<bool> {
        self.parent_charges.and_then(|charges| charges.contains_key(&parent).then_some(true))
            .or_else(|| self.context.combat_parent_damaged(parent, victim))
    }
    fn combat_parent_charge(&self, parent: ActionRef) -> Option<u64> {
        self.parent_charges.and_then(|charges| charges.get(&parent).copied())
            .or_else(|| self.context.combat_parent_charge(parent))
    }
    fn resolve_world_respawn(
        &self,
        plan: crate::world_respawn::WorldRespawnPlan,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Option<crate::world_respawn::WorldRespawnResolution>, ApplyFailure> {
        self.context.resolve_world_respawn(plan, values)
    }

    fn scheduled_executor(&self, chunk: crate::protocol::ChunkKey) -> Result<crate::realm_completion::PeerGeneration, ApplyFailure> {
        self.context.scheduled_executor(chunk)
    }
    fn block_segment_obstructs(
        &self,
        state: u16,
        position: crate::protocol::BlockPos,
        start: [u64; 3],
        end: [u64; 3],
    ) -> Result<bool, ApplyFailure> {
        self.context
            .block_segment_obstructs(state, position, start, end)
    }
    fn fall_damage_factor(&self, state: u16, sneaking: bool) -> Result<u16, ApplyFailure> {
        self.context.fall_damage_factor(state, sneaking)
    }
    fn environmental_contact(&self, intent: &crate::combat_action::EnvironmentalDamageIntent, values: &std::collections::BTreeMap<Target,Cell>)
        -> Result<crate::combat_action::EnvironmentalContactFacts,ApplyFailure> { self.context.environmental_contact(intent,values) }
    fn environmental_hazard(
        &self,
        kind: crate::combat_action::EnvironmentalHazardKind,
        block_state: u16,
    ) -> Result<Option<crate::combat_action::EnvironmentalHazardFacts>, ApplyFailure> {
        self.context.environmental_hazard(kind, block_state)
    }
    fn status_effect_facts(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<crate::combat_action::CombatStatusFacts, ApplyFailure> {
        self.context.status_effect_facts(effects)
    }
    fn status_effect_modifiers(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<crate::combat_action::CombatAttributeModifier>, ApplyFailure> {
        self.context.status_effect_modifiers(effects)
    }
    fn derive_item_use(
        &self,
        intent: &crate::item_use::ItemUseIntent,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.context.derive_item_use(intent, id, tick, values)
    }
    fn crossbow_items(&self, intent: &crate::crossbow_items::CrossbowItems, id: ActionId, tick: TickStamp,
        values: &BTreeMap<Target, Cell>) -> Result<BTreeMap<Target, crate::inventory::InventoryCell>, ApplyFailure> {
        self.context.crossbow_items(intent, id, tick, values)
    }
    fn vehicle_activator(&self, state: u16) -> Result<Option<bool>, ApplyFailure> {
        self.context.vehicle_activator(state)
    }
    fn vehicle_drop_item(&self, body: &crate::entity_body::EntityBody)
        -> Result<crate::inventory::InventoryCell, ApplyFailure> {
        self.context.vehicle_drop_item(body)
    }
    fn issued_item_birth(&self, identity: &crate::entity_origin::EntityOriginIdentity,
        cause: crate::canonical_action::ActionRef, owner: crate::realm_completion::PeerGeneration,
        spatial: crate::entity_body::EntitySpatial, cell: &crate::inventory::InventoryCell, pickup_delay: u16)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.context.issued_item_birth(identity, cause, owner, spatial, cell, pickup_delay)
    }
    fn dropped_item_body(&self, template: &crate::entity_body::EntityBody, source: &crate::inventory::InventoryCell)
        -> Result<crate::entity_body::EntityBody, ApplyFailure> {
        self.context.dropped_item_body(template, source)
    }
    fn recipe_known(&self, id: &str) -> bool { self.context.recipe_known(id) }
    fn action_source_slot(&self, reference: crate::canonical_action::ActionRef) -> Option<u64> {
        self.context.action_source_slot(reference)
    }
    fn player_advancement_outcome(&self, event: &crate::player_advancement::AdvancementEvent,
        state: &crate::player_advancement::PlayerAdvancementState, achieved_millis: u64)
        -> Result<crate::player_advancement::AdvancementOutcome, crate::canonical_action::ApplyFailure> {
        self.context.player_advancement_outcome(event, state, achieved_millis)
    }

    fn issuer_entity_admission_authority(&self, reference: crate::canonical_action::ActionRef,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.context.issuer_entity_admission_authority(reference, witness)
    }
    fn issuer_entity_initialization_matches(&self,
        witness: &crate::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        self.context.issuer_entity_initialization_matches(witness)
    }
    fn world_rules(&self, witness: crate::world_rules::WorldRulesWitness, tick: crate::time::TickStamp)
        -> Result<crate::world_rules::WorldRulesSnapshot, ApplyFailure> {
        self.context.world_rules(witness, tick)
    }
    fn death_inventory_rule(&self, action: crate::canonical_action::ActionRef, rules: crate::world_rules::WorldRulesWitness, keep_inventory: bool, target: crate::canonical_action::Target)
        -> Result<(), ApplyFailure> {
        self.context.death_inventory_rule(action, rules, keep_inventory, target)
    }
    fn death_inventory_drop(&self, template: &crate::entity_body::EntityBody, source: &crate::inventory::InventoryCell)
        -> Result<Option<crate::entity_body::EntityBody>, ApplyFailure> {
        self.context.death_inventory_drop(template, source)
    }
    fn derive_projectile_tick(&self, intent: &crate::projectile_tick::ProjectileTickIntent, id: crate::canonical_action::ActionId, tick: crate::time::TickStamp,
        values: &BTreeMap<Target, crate::canonical_action::Cell>) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        self.context.derive_projectile_tick(intent, id, tick, values)
    }
    fn mending_repair(&self, source: &crate::inventory::InventoryCell, experience: u32)
        -> Result<(crate::canonical_inventory::ItemTransformOutput, u32), ApplyFailure> {
        self.context.mending_repair(source, experience)
    }
    fn item_cooldown_group(&self, source: &crate::inventory::InventoryCell) -> Result<String, ApplyFailure> {
        self.context.item_cooldown_group(source)
    }
    fn derive_block_inventory_tick(&self, intent: &crate::canonical_block_inventory_tick::BlockInventoryTickIntent,
        values: &BTreeMap<Target, Cell>) -> Result<crate::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
        self.context.derive_block_inventory_tick(intent, values)
    }
    fn machine_genesis(&self, chunk: crate::protocol::ChunkKey) -> Result<[u8; 32], ApplyFailure> { self.context.machine_genesis(chunk) }
    fn machine_tick_footprint(&self, identity: crate::action_identity::MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<crate::action_command::ActionFootprint, ApplyFailure> { self.context.machine_tick_footprint(identity, values) }
    fn derive_machine_tick(&self, identity: crate::action_identity::MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<Vec<crate::canonical_action::AppliedWrite>, ApplyFailure> { self.context.derive_machine_tick(identity, values) }
    fn menu_inventory_transform(&self, kind: crate::canonical_menu_transform::MenuInventoryTransformKind,
        selection: &crate::canonical_menu::MenuSelection, inputs: &[crate::inventory::InventoryCell])
        -> Result<crate::canonical_menu_transform::MenuInventoryTransformOutput, ApplyFailure> {
        self.context.menu_inventory_transform(kind, selection, inputs)
    }
    fn item_transform(
        &self,
        source: &crate::inventory::InventoryCell,
        operation: &crate::canonical_inventory::ItemTransformOperation,
        id: ActionId,
        tick: TickStamp,
    ) -> Result<crate::canonical_inventory::ItemTransformOutput, ApplyFailure> {
        self.context.item_transform(source, operation, id, tick)
    }
    fn derive_world_light(
        &self,
        plan: crate::world_action::WorldLightPlan,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.context.derive_world_light(plan, values)
    }
    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        self.mirror.read(view, target)
    }
    fn borrow(&self, view: TruthView, target: Target) -> Option<std::borrow::Cow<'_, Cell>> {
        self.mirror.borrow(view, target)
    }
    fn validate_live_player_lease(&self, lease: crate::canonical_action::LivePlayerLease) -> bool {
        self.context.validate_live_player_lease(lease)
    }
    fn inventory_limit(
        &self,
        source: &crate::inventory::InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        self.context.inventory_limit(source, target)
    }
    fn entity_equipment_compatible(
        &self,
        kind: u16,
        slots: &[(crate::canonical_inventory::InventoryEquipmentSlot, crate::inventory::InventoryCell)],
    ) -> Result<(), ApplyFailure> {
        self.context.entity_equipment_compatible(kind, slots)
    }

    fn validate_block_inventory_layout(&self, block: Target, state: u16, block_entity: &Cell,
        bindings: &[crate::canonical_inventory::InventorySlotBinding]) -> Result<(), ApplyFailure> {
        self.context.validate_block_inventory_layout(block, state, block_entity, bindings)
    }
    fn entity_inventory_storage_capacity(
        &self,
        kind: u16,
        facets: &[(crate::mob_species_property::MobSpeciesProperty, crate::mob_species_property::MobSpeciesPropertyValue)],
        storage_slots: u16,
    ) -> Result<u16, ApplyFailure> {
        self.context.entity_inventory_storage_capacity(kind, facets, storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        body: &crate::entity_body::EntityBody,
    ) -> Result<Vec<crate::inventory::InvLoc>, ApplyFailure> {
        self.context.entity_inventory_layout(body)
    }
    fn entity_inventory_limit(
        &self,
        body: &crate::entity_body::EntityBody,
        item: &crate::inventory::InventoryCell,
        target: Target,
    ) -> Result<u8, ApplyFailure> {
        self.context.entity_inventory_limit(body, item, target)
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        self.context.inventory_equipment_attribute_ids()
    }
    fn inventory_equipment_facts(
        &self,
        cells: &[(
            crate::canonical_inventory::InventoryEquipmentSlot,
            crate::inventory::InventoryCell,
        )],
    ) -> Result<crate::combat_action::CombatEquipmentFacts, ApplyFailure> {
        self.context.inventory_equipment_facts(cells)
    }
    fn commit(
        &mut self,
        _: crate::canonical_action::ActionContext,
        _: TruthView,
        _: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

pub fn solve_component(
    graph: &TickActionGraph,
    component: &BTreeSet<ActionId>,
    anchors: &BTreeMap<Target, Option<Cell>>,
    cluster_seed: u64,
    dependency_accepted: impl Fn(ActionRef) -> Option<bool>,
) -> Result<SolvedComponent, SolveFailure> {
    solve_component_with_context(
        graph,
        component,
        anchors,
        cluster_seed,
        dependency_accepted,
        &OwnedStakeholderSet::default(),
    )
}

pub fn solve_component_with_context<S: ActionState>(
    graph: &TickActionGraph,
    component: &BTreeSet<ActionId>,
    anchors: &BTreeMap<Target, Option<Cell>>,
    cluster_seed: u64,
    dependency_accepted: impl Fn(ActionRef) -> Option<bool>,
    context: &S,
) -> Result<SolvedComponent, SolveFailure> {
    solve_component_with_recovery(
        graph,
        component,
        anchors,
        cluster_seed,
        dependency_accepted,
        context,
        &CertifiedRecoveryConstraints::default(),
    )
}

pub fn solve_component_with_recovery<S: ActionState>(
    graph: &TickActionGraph,
    component: &BTreeSet<ActionId>,
    anchors: &BTreeMap<Target, Option<Cell>>,
    cluster_seed: u64,
    dependency_accepted: impl Fn(ActionRef) -> Option<bool>,
    context: &S,
    recovery: &CertifiedRecoveryConstraints,
) -> Result<SolvedComponent, SolveFailure> {
    let Some(first) = component.first().copied() else {
        return Err(SolveFailure::InvalidComponent);
    };
    let tick = graph
        .actions
        .get(&first)
        .ok_or(SolveFailure::MissingAction(first))?
        .action
        .tick;
    let mut targets = BTreeSet::new();
    for id in component {
        let node = graph
            .actions
            .get(id)
            .ok_or(SolveFailure::MissingAction(*id))?;
        if recovery
            .outcomes
            .get(&ActionRef { tick, id: *id })
            .is_some_and(|pin| pin.core != node.action)
        {
            return Err(SolveFailure::InvalidComponent);
        }
        let action = graph
            .actions
            .get(id)
            .ok_or(SolveFailure::MissingAction(*id))?
            .semantic_action();
        if action.tick != tick {
            return Err(SolveFailure::InvalidComponent);
        }
        targets.extend(action.targets());
        targets.extend(action.counterfactual_conditions.iter().copied());
        targets.extend(action.counterfactual_effects.iter().copied());
        for dependency in &action.dependencies {
            if dependency.tick == tick && component.contains(&dependency.id) {
                continue;
            }
            if dependency_accepted(*dependency).is_none() {
                return Err(SolveFailure::MissingDependency(*dependency));
            }
        }
    }
    let bodies = targets
        .iter()
        .filter_map(|target| crate::derived_living::actor_body_target(*target)
            .or_else(|| crate::frame_inventory_projection::body_target(*target)))
        .collect::<BTreeSet<_>>();
    targets.extend(bodies);
    let occupancy = targets.iter().filter_map(|target| crate::derived_actor_occupancy::root_alias(*target)).collect::<BTreeSet<_>>();
    targets.extend(occupancy);
    let mut mirror = OwnedStakeholderSet::default();
    for target in targets {
        let value = anchors
            .get(&target)
            .ok_or(SolveFailure::MissingAnchor(target))?;
        if let Some(value) = value {
            mirror.local.insert(target, value.clone());
        }
    }
    let mut ranked = component.iter().copied().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        let left_actor = graph.actions[left].action.id.actor;
        let right_actor = graph.actions[right].action.id.actor;
        let mandatory = |id| {
            recovery
                .outcomes
                .get(&ActionRef { tick, id })
                .is_some_and(|pin| pin.accepted)
        };
        mandatory(*right)
            .cmp(&mandatory(*left))
            .then_with(|| order_actors(cluster_seed, tick, right_actor, left_actor))
            .then_with(|| right.cmp(left))
    });
    let rank = ranked
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![SearchState {
        mirror,
        remaining: component.clone(),
        accepted: BTreeMap::new(),
        order: Vec::new(),
    }];
    let mut best: Option<SearchState> = None;
    while let Some(state) = pending.pop() {
        if best.as_ref().is_some_and(|current| {
            ranked
                .iter()
                .map(|id| state.accepted.contains_key(id) || state.remaining.contains(id))
                .cmp(ranked.iter().map(|id| current.accepted.contains_key(id)))
                .is_lt()
        }) {
            continue;
        }
        if state.remaining.is_empty() {
            if best
                .as_ref()
                .is_none_or(|current| better(&state, current, &ranked))
            {
                best = Some(state);
            }
            continue;
        }
        let mut branches = Vec::new();
        for id in &ranked {
            if !state.remaining.contains(id) {
                continue;
            }
            let core = graph.actions[id].semantic_action();
            let pin = recovery.outcomes.get(&ActionRef { tick, id: *id });
            if pin.is_some_and(|pin| !pin.accepted) {
                continue;
            }
            if core.chunk_transition
                && state
                    .remaining
                    .iter()
                    .any(|other| other != id && !graph.actions[other].action.chunk_transition)
            {
                continue;
            }
            if !core.dependencies.iter().all(|dependency| {
                if dependency.tick == tick && component.contains(&dependency.id) {
                    state.accepted.contains_key(&dependency.id)
                } else {
                    dependency_accepted(*dependency) == Some(true)
                }
            }) {
                continue;
            }
            if let Some(SemanticIntent::Combat(intent)) = &core.semantic_intent
                && let Some(dependency) = &intent.damage_parent
            {
                let damaging = if let Some(parent) = state.accepted.get(&dependency.parent.id) {
                    crate::combat_action::damaging_parent(&parent.action, &parent.result, &dependency.victim)
                } else if !component.contains(&dependency.parent.id) {
                    graph.actions.get(&dependency.parent.id).is_some_and(|parent| {
                        parent.factual.result.as_ref().is_some_and(|result| {
                            crate::combat_action::damaging_parent(&parent.semantic_action(), result, &dependency.victim)
                        })
                    })
                } else {
                    false
                };
                if !damaging {
                    continue;
                }
            }
            if !graph.actions[id].prerequisites.iter().all(|prior| {
                !graph
                    .edge_reasons
                    .get(&(*prior, *id))
                    .is_some_and(|reasons| {
                        reasons.keys().any(|reason| {
                            matches!(
                                reason,
                                crate::action_graph::EdgeReason::InfallibleAvatarOrder(_)
                                    | crate::action_graph::EdgeReason::InfallibleScheduledOrder(_)
                            )
                        })
                    })
                    || state.accepted.contains_key(prior)
            }) {
                continue;
            }
            let mut parent_charges = BTreeMap::new();
            if let Some(SemanticIntent::Combat(intent)) = &core.semantic_intent
                && let Some(dependency) = &intent.damage_parent
            {
                let charge = if let Some(parent) = state.accepted.get(&dependency.parent.id) {
                    crate::combat_action::resolved_attack_charge(&parent.action)
                } else {
                    graph.actions.get(&dependency.parent.id).and_then(|parent| {
                        let mut factual = parent.semantic_action().clone();
                        factual.conditions = parent.factual.conditions.clone();
                        crate::combat_action::resolved_attack_charge(&factual)
                    })
                };
                if let Some(charge) = charge { parent_charges.insert(dependency.parent, charge); }
            }
            let symbolic = RegistrySemanticState {
                mirror: &state.mirror,
                context,
                parent_charges: Some(&parent_charges),
            };
            for (candidate, replayed) in candidates(core, &state.mirror, &symbolic) {
                if pin.is_some_and(|pin| {
                    candidate
                        .conditions
                        .iter()
                        .filter(|condition| pin.realms.contains(&condition.target.action_realm()))
                        .cloned()
                        .collect::<Vec<_>>()
                        != pin.conditions
                }) {
                    continue;
                }
                let Ok(result) = prepare_action(&symbolic, &candidate, TruthView::Local) else {
                    continue;
                };
                if pin.is_some_and(|pin| {
                    let writes = result
                        .writes
                        .iter()
                        .filter(|write| pin.realms.contains(&write.target.action_realm()))
                        .cloned()
                        .collect::<Vec<_>>();
                    writes != pin.writes
                }) {
                    continue;
                }
                if !pairwise_compatible(*id, &candidate, &result, &state.accepted, &rank) {
                    continue;
                }
                let mut next = state.clone();
                let living_updates = state
                    .mirror
                    .local
                    .iter()
                    .filter_map(|(target, cell)| {
                        if crate::derived_living::actor_body_target(*target).is_some() {
                            Some(
                                crate::derived_living::project_facet_writes(
                                    *target,
                                    Some(cell),
                                    &result.writes,
                                )
                                .map(|value| (*target, value)),
                            )
                        } else if matches!(&cell.value, Value::Living(_) | Value::Entity(_)) {
                            Some(
                                crate::derived_living::project_body_writes(
                                    *target,
                                    Some(cell),
                                    &result.writes,
                                )
                                .map(|value| (*target, value)),
                            )
                        } else {
                            None
                        }
                    })
                    .collect::<Result<Vec<_>, _>>();
                let Ok(living_updates) = living_updates else {
                    continue;
                };
                if next
                    .mirror
                    .commit(
                        crate::canonical_action::ActionContext {
                            id: candidate.id,
                            tick: candidate.tick,
                            actor_chunk: candidate.actor_chunk,
                        },
                        TruthView::Local,
                        &result.writes,
                    )
                    .is_err()
                {
                    continue;
                }
                let section_updates = next
                    .mirror
                    .local
                    .iter()
                    .filter_map(|(target, cell)| {
                        matches!(
                            target,
                            Target::Chunk {
                                part: crate::canonical_action::ChunkPart::BlockSection(_),
                                inventory: None,
                                ..
                            }
                        )
                        .then(|| {
                            crate::block_section::project_writes(*target, cell, &result.writes)
                                .map(|projected| (*target, projected))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>();
                let Ok(section_updates) = section_updates else {
                    continue;
                };
                next.mirror.local.extend(section_updates);
                for (target, cell) in living_updates {
                    if let Some(cell) = cell {
                        next.mirror.local.insert(target, cell);
                    } else {
                        next.mirror.local.remove(&target);
                    }
                }
                next.remaining.remove(id);
                next.order.push(*id);
                next.accepted.insert(
                    *id,
                    AcceptedVariant {
                        action: candidate,
                        result,
                        replayed,
                    },
                );
                branches.push(next);
            }
        }
        for id in state.remaining.iter().rev() {
            if graph.actions[id].action.class == ActionClass::Infallible
                || recovery
                    .outcomes
                    .get(&ActionRef { tick, id: *id })
                    .is_some_and(|pin| pin.accepted)
            {
                continue;
            }
            let mut next = state.clone();
            next.remaining.remove(id);
            branches.push(next);
        }
        pending.extend(branches.into_iter().rev());
    }
    let best = best.ok_or_else(|| {
        component
            .iter()
            .find(|id| graph.actions[id].action.class == ActionClass::Infallible)
            .copied()
            .map_or(
                SolveFailure::InvalidComponent,
                SolveFailure::InfallibleConflict,
            )
    })?;
    let variants = component
        .iter()
        .map(|id| {
            let variant = best
                .accepted
                .get(id)
                .map_or(SolvedVariant::Undone, |accepted| SolvedVariant::Applied {
                    conditions: accepted.action.conditions.clone(),
                    effects: accepted.action.effects.clone(),
                    result: accepted.result.clone(),
                    replayed: accepted.replayed,
                });
            (*id, variant)
        })
        .collect();
    Ok(SolvedComponent {
        component: component.clone(),
        variants,
        causal_order: best.order,
    })
}

fn better(candidate: &SearchState, current: &SearchState, ranked: &[ActionId]) -> bool {
    let candidate_score = ranked
        .iter()
        .map(|id| candidate.accepted.contains_key(id))
        .collect::<Vec<_>>();
    let current_score = ranked
        .iter()
        .map(|id| current.accepted.contains_key(id))
        .collect::<Vec<_>>();
    if candidate_score != current_score {
        return candidate_score > current_score;
    }
    let candidate_replays = ranked
        .iter()
        .map(|id| {
            candidate
                .accepted
                .get(id)
                .is_some_and(|entry| entry.replayed)
        })
        .collect::<Vec<_>>();
    let current_replays = ranked
        .iter()
        .map(|id| current.accepted.get(id).is_some_and(|entry| entry.replayed))
        .collect::<Vec<_>>();
    candidate_replays < current_replays
        || candidate_replays == current_replays && candidate.order < current.order
}

fn candidates<S: ActionState>(
    core: &Action,
    mirror: &OwnedStakeholderSet,
    context: &S,
) -> Vec<(Action, bool)> {
    let mut original = core.clone();
    original.factual_result = None;
    let mut candidates = vec![(original, false)];
    let Some(intent) = &core.semantic_intent else {
        return candidates;
    };
    let mut values = BTreeMap::new();
    for target in core
        .counterfactual_conditions
        .iter()
        .chain(&core.counterfactual_effects)
    {
        if let Some(cell) = mirror.read(TruthView::Local, *target) {
            values.insert(*target, cell);
        }
    }
    let (effects, selectable) = match intent {
        SemanticIntent::CreativeInventory(intent) => {
            let Ok(effects) = intent.effects(context, &values) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::World(intent) => {
            let symbolic = RegistrySemanticState { mirror, context, parent_charges: None };
            let Ok(effects) = intent.derive_effects_with(&values, &symbolic) else {
                return candidates;
            };
            let retained_menu_guards = intent.steps.iter().filter_map(|step| match step {
                crate::world_action::WorldReplayStep::MenuSession(menu) => Some(menu), _ => None,
            }).flat_map(|menu| menu.targets().into_iter().filter(move |target| *target != menu.target)).collect::<BTreeSet<_>>();
            (effects, intent.targets().filter(|target| !retained_menu_guards.contains(target)).collect::<BTreeSet<_>>())
        }
        SemanticIntent::InventoryMove(intent)
        | SemanticIntent::Pickup {
            inventory: intent, ..
        } => {
            let Ok(effects) = intent.derive_effects_with_context(&values, context) else {
                return candidates;
            };
            let selectable = intent.targets().collect::<BTreeSet<_>>();
            (effects, selectable)
        }
        SemanticIntent::InventoryGestures(intent) => {
            let Ok(effects) = intent.derive_effects_with_context(&values, context) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::InventoryClose(intent) => {
            if intent.derive(context, ActionRef { tick: core.tick, id: core.id }, &values).is_err() {
                return candidates;
            }
            (vec![Effect::CloseInventory { intent: intent.clone() }],
                intent.targets().into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::MerchantTrade(intent) => {
            if intent.derive(context, ActionRef { tick: core.tick, id: core.id }, &values).is_err() { return candidates; }
            let selectable = intent.targets().into_iter().filter(|target|
                *target != intent.menu.viewer.target() && *target != intent.menu.merchant.target()
                && !intent.health_targets().contains(target)
                && !intent.rewards.iter().any(|reward| reward.target == *target)).collect::<BTreeSet<_>>();
            (vec![Effect::TradeItems { intent: intent.clone() }], selectable)
        }
        SemanticIntent::MenuInventoryTransform(intent) => {
            let symbolic = RegistrySemanticState { mirror, context, parent_charges: None };
            if intent.derive(&symbolic, &values).is_err() { return candidates; }
            (vec![Effect::TransformMenuInventory { intent: intent.clone() }],
                intent.bindings().into_iter().map(|binding| binding.target).collect::<BTreeSet<_>>())
        }
        SemanticIntent::MenuSessionChange(intent) => {
            let Ok(effects) = intent.derive(&values) else { return candidates; };
            (effects, [intent.target].into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::RecipeInventory(intent) => {
            if intent.derive(&values).is_err() {
                return candidates;
            }
            (
                vec![Effect::CraftItems {
                    intent: intent.clone(),
                }],
                intent.targets().into_iter().collect::<BTreeSet<_>>(),
            )
        }
        SemanticIntent::VehicleTick(intent) => {
            let Ok(effects) = intent.derive_with(context, core.tick, &values) else { return candidates; };
            (effects, intent.targets().into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::ItemUse(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::DropItem(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::DeathInventory(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else { return candidates; };
            (effects, intent.targets())
        }
        SemanticIntent::ProjectileTick(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else { return candidates; };
            (effects, intent.targets())
        }
        SemanticIntent::CollectProjectile(intent) => {
            if intent.derive(context, &values).is_err() { return candidates; }
            (vec![Effect::CollectProjectile { intent: intent.clone() }], intent.targets())
        }
        SemanticIntent::CollectExperience(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::ExperienceDamage(intent) => {
            let Ok(effects) = intent.derive(&values) else {
                return candidates;
            };
            (
                effects,
                [intent.target].into_iter().collect::<BTreeSet<_>>(),
            )
        }
        SemanticIntent::MergeExperience(intent) => {
            let Ok(effects) = intent.derive(&values) else {
                return candidates;
            };
            (
                effects,
                intent.targets().into_iter().collect::<BTreeSet<_>>(),
            )
        }
        SemanticIntent::Object(intent) => {
            let Ok(effects) = intent.derive(core.id, core.tick, &values) else {
                return candidates;
            };
            (effects, intent.targets().collect::<BTreeSet<_>>())
        }
        SemanticIntent::Respawn(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else { return candidates; };
            let Ok(targets) = intent.targets() else { return candidates; };
            (effects, targets.into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::AreaCloud(intent) => {
            let Ok(effects) = intent.derive(context, core.id, core.tick, &values) else { return candidates; };
            (effects, intent.targets())
        }
        SemanticIntent::Wake(intent) => {
            if intent.validate_beds(context).is_err() { return candidates; }
            if let Some(morning) = intent.morning {
                if context.validate_morning(intent.chunk.world, morning, ActionRef { tick: core.tick, id: core.id }).is_err() { return candidates; }
            }
            let Ok(effects) = intent.effects(core.id, core.tick) else { return candidates; };
            (effects, intent.targets().into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::ActorTags(intent) => {
            let Ok(effects) = intent.derive(core.id, core.tick, &values) else { return candidates; };
            (effects, intent.targets().into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::MobBreeding(intent) => {
            let Ok(effects) = intent.derive(context, &values) else { return candidates; };
            (effects, intent.resolution_targets())
        }
        SemanticIntent::ObjectTick(intent) => {
            let Ok(effects) = intent.derive(core.id, core.tick, &values) else { return candidates; };
            (effects, intent.targets().into_iter().collect::<BTreeSet<_>>())
        }
        SemanticIntent::Combat(intent) => {
            for target in intent.targets() {
                if let Some(cell) = mirror.read(TruthView::Local, target) {
                    values.insert(target, cell);
                }
            }
            let Ok(effects) = intent.derive_with(context, core.id, core.tick, &values) else {
                return candidates;
            };
            (effects, intent.resolution_targets())
        }
        SemanticIntent::MergeDroppedItems { contact, max_stack } => {
            let Ok(effects) = crate::item_lifecycle::derive_merge(contact, *max_stack, &values)
            else {
                return candidates;
            };
            (
                effects,
                [contact.source.target(), contact.destination.target()]
                    .into_iter()
                    .collect(),
            )
        }
    };
    let mut replay = core.clone();
    replay.factual_result = None;
    replay.effects = effects;
    if matches!(intent, SemanticIntent::Respawn(_)) {
        replay.chunk_transition = replay.effects.iter().any(|effect| matches!(effect,
            Effect::RespawnPlayerAvatar { source: Target::Chunk { key: source, .. }, destination: Target::Chunk { key: destination, .. }, .. }
                if source != destination));
    }
    if matches!(intent, SemanticIntent::MergeDroppedItems { .. }) {
        for condition in &mut replay.conditions {
            if let Expected::DroppedItemStack { count, .. } = &mut condition.expected
                && let Some(Cell {
                    value: Value::Entity(body),
                    ..
                }) = values.get(&condition.target)
                && let crate::entity_body::EntityPayload::Item(item) = &body.payload
            {
                *count = item.count;
            }
        }
    }
    let touched = replay
        .effects
        .iter()
        .flat_map(Effect::targets)
        .collect::<BTreeSet<_>>();
    replay.conditions.retain(|condition| {
        matches!(
            condition.expected,
            Expected::ActorControlled { .. }
                | Expected::VehicleTerminal(_) | Expected::ObjectTerminal(_)
                | Expected::EntityPlacement(_)
                | Expected::HopperCollector { .. }
                | Expected::EntityBirthOwned { .. }
                | Expected::EntityBirthKind { .. }
                | Expected::DroppedItemStack { .. }
                | Expected::DroppedItem { .. }
                | Expected::SpatialEpochEquals(_)
                | Expected::LivingUnsignedAtLeast(_)
                | Expected::ExperienceOrb { .. }
                | Expected::ScheduledTickFact { .. }
                | Expected::ScheduledTickPresence { .. }
                | Expected::ScheduledTickPresenceExcept { .. }
                | Expected::ActorOccupancyClear(_)
                | Expected::HangingOccupancyClear(_)
                | Expected::EntitiesOccupancyClear(_)
                | Expected::ActorOccupancyDomain(_)
        ) || matches!(
            intent,
            SemanticIntent::ItemUse(_)
        ) && matches!(condition.expected, Expected::InventoryItem { .. })
            || matches!(intent, SemanticIntent::DropItem(drop) if drop.selected.is_none())
                && matches!(condition.expected, Expected::InventoryItem { .. })
            || !selectable.contains(&condition.target)
    });
    let reads = if matches!(
        intent,
        SemanticIntent::RecipeInventory(_)
            | SemanticIntent::MenuInventoryTransform(_)
            | SemanticIntent::MenuSessionChange(_)
            | SemanticIntent::MerchantTrade(_)
            | SemanticIntent::InventoryClose(_)
            | SemanticIntent::CreativeInventory(_)
            | SemanticIntent::InventoryGestures(_)
            | SemanticIntent::Combat(_)
            | SemanticIntent::VehicleTick(_)
            | SemanticIntent::World(_)
            | SemanticIntent::Respawn(_)
            | SemanticIntent::ActorTags(_)
            | SemanticIntent::Wake(_)
            | SemanticIntent::AreaCloud(_)
            | SemanticIntent::ItemUse(_)
            | SemanticIntent::DropItem(_)
            | SemanticIntent::DeathInventory(_)
            | SemanticIntent::ProjectileTick(_)
            | SemanticIntent::CollectProjectile(_)
            | SemanticIntent::CollectExperience(_)
    ) {
        selectable
    } else {
        touched
    };
    for target in reads {
        if replay.conditions.iter().any(|condition| {
            condition.target == target
                && matches!(
                    condition.expected,
                    Expected::LivingUnsignedAtLeast(_)
                        | Expected::ScheduledTickFact { .. }
                        | Expected::ScheduledTickPresence { .. }
                | Expected::ScheduledTickPresenceExcept { .. }
                        | Expected::ActorOccupancyClear(_)
                | Expected::HangingOccupancyClear(_)
                | Expected::EntitiesOccupancyClear(_)
                | Expected::ActorOccupancyDomain(_)
                )
        }) {
            continue;
        }
        if matches!(intent, SemanticIntent::World(_))
            && replay.conditions.iter().any(|condition| {
                condition.target == target
                    && matches!(condition.expected, Expected::SpatialEpochEquals(_))
            })
        {
            continue;
        }
        if matches!(
            intent,
            SemanticIntent::ItemUse(_)
        ) || matches!(intent, SemanticIntent::DropItem(drop) if drop.selected.is_none()) {
            if replay.conditions.iter().any(|condition| {
            condition.target == target
                && matches!(condition.expected, Expected::InventoryItem { .. })
            }) { continue; }
        }
        if matches!(intent, SemanticIntent::MergeDroppedItems { .. }) {
            continue;
        }
        if matches!(intent, SemanticIntent::CollectExperience(_))
            && replay.conditions.iter().any(|condition| {
                condition.target == target
                    && matches!(
                        condition.expected,
                        Expected::ExperienceOrb { .. } | Expected::SpatialEpochEquals(_)
                    )
            })
        {
            continue;
        }
        let expected = match mirror.read(TruthView::Local, target) {
            Some(cell)
                if matches!(intent, SemanticIntent::World(_))
                    && matches!(
                        target,
                        Target::Chunk {
                            part: crate::canonical_action::ChunkPart::BlockSection(_)
                                | crate::canonical_action::ChunkPart::LightSection(_),
                            inventory: None,
                            ..
                        }
                    ) =>
            {
                Expected::RevisionEquals(cell.revision)
            }
            Some(cell) => Expected::Exact(cell),
            None if matches!(intent, SemanticIntent::World(_))
                || matches!(intent, SemanticIntent::Respawn(_)) && matches!(target, Target::Chunk { part: crate::canonical_action::ChunkPart::Player(_) | crate::canonical_action::ChunkPart::Entity(_), inventory: None, .. })
                || matches!(intent, SemanticIntent::ProjectileTick(projectile) if projectile.birth_targets().contains(&target))
                || matches!(intent, SemanticIntent::ProjectileTick(projectile) if projectile.teleport_destinations().contains(&target))
                || matches!(intent, SemanticIntent::DeathInventory(death) if death.drops.iter().any(|drop| drop.destination == target))
                || matches!(intent, SemanticIntent::DropItem(drop) if drop.destination == target)
                || matches!(intent, SemanticIntent::ItemUse(use_intent) if use_intent.launch.as_ref().is_some_and(|launch|
                    launch.identities.iter().any(|identity| target == (Target::Chunk { key: launch.spawn_chunk,
                        part: crate::canonical_action::ChunkPart::Entity(identity.id), inventory: None })))) =>
            {
                Expected::Missing
            }
            None => return candidates,
        };
        replay.conditions.push(Condition { target, expected });
    }
    replay.conditions.sort_by_key(|condition| condition.target);
    if replay.conditions != core.conditions || replay.effects != core.effects {
        candidates.push((replay, true));
    }
    candidates
}

fn pairwise_compatible(
    candidate_id: ActionId,
    candidate: &Action,
    result: &AppliedAction,
    accepted: &BTreeMap<ActionId, AcceptedVariant>,
    rank: &BTreeMap<ActionId, usize>,
) -> bool {
    for (other_id, other) in accepted {
        let follows_other = candidate
            .dependencies
            .iter()
            .any(|dependency| dependency.tick == candidate.tick && dependency.id == *other_id);
        for write in &result.writes {
            if !candidate.chunk_transition
                && !follows_other
                && rank[&candidate_id] < rank[other_id]
                && other.action.conditions.iter().any(|condition| {
                    condition.target == write.target
                        && !condition.expected.matches(write.after.as_ref())
                })
            {
                return false;
            }
        }
        for write in &other.result.writes {
            if rank[other_id] < rank[&candidate_id]
                && candidate.conditions.iter().any(|condition| {
                    condition.target == write.target
                        && !condition.expected.matches(write.after.as_ref())
                })
            {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::{
        ActionClass, ActionKind, ChunkPart, ClientOrigin, InventoryCountRule, InventoryId,
        InventoryReplayCandidate, InventoryReplayIntent, PlayerKey, SemanticIntent, Stakeholder,
        UndoProcedure, Value,
    };
    use crate::identity::ServerId;
    use crate::inventory::{InvLoc, InventoryCell};
    use crate::protocol::{BlockPos, ChunkKey, WorldId};
    use crate::time::TickStamp;
    use uuid::Uuid;

    fn block(x: i32) -> Target {
        Target::Chunk {
            key: ChunkKey::new(WorldId::new(0, 0), 0, 0),
            part: ChunkPart::Block(BlockPos { x, y: 64, z: 0 }),
            inventory: None,
        }
    }

    fn action(actor: ServerId, conditions: &[(Target, u16)], effects: &[(Target, u16)]) -> Action {
        let player_leases: Vec<crate::canonical_action::LivePlayerLease> = conditions
            .iter()
            .map(|(target, _)| target)
            .chain(effects.iter().map(|(target, _)| target))
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
                (lease.key, ChunkKey::new(WorldId::new(0, 0), 0, 0))
            })
            .collect::<Vec<_>>();
        let stakeholders = conditions
            .iter()
            .map(|(target, _)| target.stakeholder())
            .chain(effects.iter().map(|(target, _)| target.stakeholder()))
            .chain(
                player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<Stakeholder>>()
            .into_iter()
            .collect();
        Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(actor),
                incarnation: 0,
                sequence: actor.0,
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
                .iter()
                .map(|(target, value)| Condition {
                    target: *target,
                    expected: Expected::BlockState(*value),
                })
                .collect(),
            effects: effects
                .iter()
                .map(|(target, value)| Effect::Put {
                    target: *target,
                    value: Value::Block(*value),
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

    fn ranked_peers(seed: u64) -> Vec<ServerId> {
        let mut peers = vec![ServerId(1), ServerId(2), ServerId(3)];
        peers.sort_by(|left, right| {
            order_actors(
                seed,
                TickStamp(7),
                ActorId::Peer(*right),
                ActorId::Peer(*left),
            )
        });
        peers
    }

    #[test]
    fn collected_three_way_tick_resolves_once_independent_of_arrival_order() {
        let target = block(1);
        let actions = [
            action(ServerId(1), &[(target, 0)], &[(target, 1)]),
            action(ServerId(2), &[(target, 0)], &[(target, 2)]),
            action(ServerId(3), &[(target, 0)], &[(target, 3)]),
        ];
        let mut expected = None;
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let mut graphs = ActionGraphs::default();
            graphs.ground_anchors.insert(
                target,
                crate::action_graph::GroundAnchor {
                    settled_through: TickStamp(6),
                    value: anchors(&[target])[&target].clone(),
                },
            );
            graphs
                .insert_collected(TickStamp(7), order.map(|index| actions[index].clone()))
                .unwrap();
            let pass = resolve_collected_actions(
                &graphs,
                TickStamp(7),
                actions.iter().map(|action| ActionRef {
                    tick: action.tick,
                    id: action.id,
                }),
                29,
                |_| true,
            )
            .unwrap();
            let outcomes = pass.graphs.ticks[&TickStamp(7)]
                .actions
                .iter()
                .map(|(id, node)| (*id, node.wrapper, node.factual.clone()))
                .collect::<Vec<_>>();
            assert_eq!(pass.deltas.len(), 1);
            assert!(
                graphs.ticks[&TickStamp(7)]
                    .actions
                    .values()
                    .all(|node| node.semantic_generation.is_none())
            );
            match &expected {
                Some(expected) => assert_eq!(&outcomes, expected),
                None => expected = Some(outcomes),
            }
        }
    }

    #[test]
    fn late_third_conflict_repairs_local_truth_without_promoting_partial_tick() {
        let target = block(1);
        let actions = [
            action(ServerId(1), &[(target, 0)], &[(target, 1)]),
            action(ServerId(2), &[(target, 0)], &[(target, 2)]),
            action(ServerId(3), &[(target, 0)], &[(target, 3)]),
        ];
        let baseline = Cell {
            revision: 0,
            value: Value::Block(0),
        };
        let mut graphs = ActionGraphs::default();
        graphs.ground_anchors.insert(
            target,
            crate::action_graph::GroundAnchor {
                settled_through: TickStamp(6),
                value: Some(baseline.clone()),
            },
        );
        let mut owned = OwnedStakeholderSet::default();
        owned.local.insert(target, baseline);
        graphs
            .insert_collected(TickStamp(7), actions[..2].iter().cloned())
            .unwrap();
        let first = prepare_local_turn(
            &graphs,
            TickStamp(7),
            actions[..2].iter().map(|action| ActionRef {
                tick: action.tick,
                id: action.id,
            }),
            29,
            |_| true,
            &owned,
        )
        .unwrap();
        for write in first.deltas.writes {
            if let Some(after) = write.after {
                owned.local.insert(write.target, after);
            } else {
                owned.local.remove(&write.target);
            }
        }
        graphs = first.graphs;
        assert!(
            graphs.ticks[&TickStamp(7)]
                .actions
                .values()
                .all(|node| node.stable && !node.is_ground_applied())
        );
        graphs
            .insert_collected(TickStamp(7), [actions[2].clone()])
            .unwrap();
        let late = prepare_local_turn(
            &graphs,
            TickStamp(8),
            [ActionRef {
                tick: actions[2].tick,
                id: actions[2].id,
            }],
            29,
            |_| true,
            &owned,
        )
        .unwrap();
        for write in late.deltas.writes {
            if let Some(after) = write.after {
                owned.local.insert(write.target, after);
            } else {
                owned.local.remove(&write.target);
            }
        }
        let final_graph = late.graphs.ticks.get(&TickStamp(7)).unwrap();
        assert!(
            final_graph
                .actions
                .values()
                .all(|node| node.stable && !node.is_ground_applied()),
            "{:#?}",
            final_graph
                .actions
                .iter()
                .map(|(id, node)| (
                    id,
                    node.stable,
                    node.generation,
                    node.local_generation,
                    node.semantic_generation,
                    node.wrapper
                ))
                .collect::<Vec<_>>()
        );
        let accepted = final_graph
            .actions
            .values()
            .find_map(|node| node.factual.result.as_ref())
            .unwrap();
        assert_eq!(owned.local.get(&target), accepted.writes[0].after.as_ref());
        owned.ground.insert(
            target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let plan = final_graph.ground_ready_plan(TickStamp(7), |_| true, |_| false);
        let staged = stage_ground_ready(final_graph, &plan, |_| true, &owned).unwrap();
        assert_eq!(staged.writes.len(), 1);
        assert_eq!(
            staged.writes[0].after.as_ref().unwrap().value,
            accepted.writes[0].after.as_ref().unwrap().value
        );
        assert_eq!(owned.ground.get(&target).unwrap().value, Value::Block(0));
        assert_eq!(staged.applied_realms.len(), 3);
        owned.ground.get_mut(&target).unwrap().revision = 1;
        assert!(matches!(
            stage_ground_ready(final_graph, &plan, |_| true, &owned),
            Err(GroundDeltaFailure::ChangedGround(changed)) if changed == target
        ));
        owned.ground.insert(
            target,
            Cell {
                revision: 1,
                value: Value::Block(9),
            },
        );
        assert!(matches!(
            stage_ground_ready(final_graph, &plan, |_| true, &owned),
            Err(GroundDeltaFailure::ChangedGround(changed)) if changed == target
        ));
        let loser = final_graph
            .actions
            .iter()
            .find(|(_, node)| node.factual.result.is_none())
            .map(|(id, _)| ActionRef {
                tick: TickStamp(7),
                id: *id,
            })
            .unwrap();
        let loser_plan = GroundReadyPlan {
            components: vec![vec![loser]],
            unapplied_realms: BTreeMap::from([(loser, BTreeSet::from([target.action_realm()]))]),
            semantic_generations: BTreeMap::from([(
                loser,
                final_graph.actions[&loser.id].semantic_generation.unwrap(),
            )]),
        };
        let dropped = stage_ground_ready(final_graph, &loser_plan, |_| true, &owned).unwrap();
        assert!(dropped.writes.is_empty());
        assert_eq!(dropped.applied_realms, vec![(loser, target.action_realm())]);
    }

    #[test]
    fn same_tick_causal_block_chain_grounds_in_one_atomic_delta() {
        let target = block(1);
        let first = action(ServerId(1), &[(target, 0)], &[(target, 1)]);
        let mut second = action(ServerId(2), &[(target, 1)], &[(target, 2)]);
        second.dependencies.push(ActionRef {
            tick: first.tick,
            id: first.id,
        });
        let mut graphs = ActionGraphs::default();
        graphs.ground_anchors.insert(
            target,
            crate::action_graph::GroundAnchor {
                settled_through: TickStamp(6),
                value: Some(Cell {
                    revision: 0,
                    value: Value::Block(0),
                }),
            },
        );
        graphs
            .insert_collected(TickStamp(7), [first.clone(), second.clone()])
            .unwrap();
        let mut owned = OwnedStakeholderSet::default();
        owned.local.insert(
            target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        owned.ground = owned.local.clone();
        let local = prepare_local_turn(
            &graphs,
            TickStamp(7),
            [first, second].map(|action| ActionRef {
                tick: action.tick,
                id: action.id,
            }),
            29,
            |_| true,
            &owned,
        )
        .unwrap();
        assert_eq!(local.deltas.writes.len(), 1);
        assert_eq!(
            local.deltas.writes[0].after.as_ref().unwrap().value,
            Value::Block(2)
        );
        let graph = &local.graphs.ticks[&TickStamp(7)];
        assert!(
            graph
                .actions
                .values()
                .all(|node| node.factual.result.is_some())
        );
        let plan = graph.ground_ready_plan(TickStamp(7), |_| true, |_| false);
        let ground = stage_ground_ready(graph, &plan, |_| true, &owned).unwrap();
        assert_eq!(ground.writes.len(), 1);
        assert_eq!(
            ground.writes[0].after.as_ref().unwrap().value,
            Value::Block(2)
        );
        assert_eq!(ground.applied_realms.len(), 2);
        assert_eq!(ground.actions.len(), 2);
        assert_eq!(ground.actions[0].1[0].after.as_ref().unwrap().revision, 1);
        assert_eq!(ground.actions[1].1[0].after.as_ref().unwrap().revision, 2);
        assert!(ground.validate());
        let mut incomplete = ground;
        incomplete.actions.pop();
        assert!(!incomplete.validate());
    }

    #[test]
    fn ground_stage_checks_read_only_condition_before_world_write() {
        let read = block(1);
        let write = block(2);
        let action = action(ServerId(1), &[(read, 0), (write, 0)], &[(write, 1)]);
        let mut graphs = ActionGraphs::default();
        for target in [read, write] {
            graphs.ground_anchors.insert(
                target,
                crate::action_graph::GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                },
            );
        }
        graphs
            .insert_collected(action.tick, [action.clone()])
            .unwrap();
        let mut owned = OwnedStakeholderSet::default();
        for target in [read, write] {
            owned.local.insert(
                target,
                Cell {
                    revision: 0,
                    value: Value::Block(0),
                },
            );
        }
        owned.ground = owned.local.clone();
        let local = prepare_local_turn(
            &graphs,
            action.tick,
            [ActionRef {
                tick: action.tick,
                id: action.id,
            }],
            29,
            |_| true,
            &owned,
        )
        .unwrap();
        let graph = &local.graphs.ticks[&action.tick];
        let plan = graph.ground_ready_plan(action.tick, |_| true, |_| false);
        owned.ground.get_mut(&read).unwrap().value = Value::Block(9);
        assert!(matches!(
            stage_ground_ready(graph, &plan, |_| true, &owned),
            Err(GroundDeltaFailure::ChangedGround(target)) if target == read
        ));
        assert_eq!(owned.ground[&write].value, Value::Block(0));
    }

    #[test]
    fn joint_action_projects_only_locally_held_realm() {
        let local_target = block(1);
        let remote_target = Target::Chunk {
            key: ChunkKey::new(WorldId::new(0, 0), 1, 0),
            part: ChunkPart::Block(BlockPos { x: 17, y: 64, z: 0 }),
            inventory: None,
        };
        let action = action(
            ServerId(1),
            &[(local_target, 0), (remote_target, 0)],
            &[(local_target, 1), (remote_target, 1)],
        );
        let mut graphs = ActionGraphs::default();
        for target in [local_target, remote_target] {
            graphs.ground_anchors.insert(
                target,
                crate::action_graph::GroundAnchor {
                    settled_through: TickStamp(6),
                    value: anchors(&[target])[&target].clone(),
                },
            );
        }
        graphs
            .insert_collected(action.tick, [action.clone()])
            .unwrap();
        let pass = resolve_collected_actions(
            &graphs,
            action.tick,
            [ActionRef {
                tick: action.tick,
                id: action.id,
            }],
            29,
            |realm| realm == local_target.action_realm(),
        )
        .unwrap();
        let factual = pass.graphs.ticks[&action.tick].actions[&action.id]
            .factual
            .result
            .as_ref()
            .unwrap();
        assert_eq!(factual.writes.len(), 2);
        assert_eq!(pass.deltas.len(), 1);
        let projection = pass.deltas[0].next.as_ref().unwrap();
        assert_eq!(projection.writes.len(), 1);
        assert_eq!(projection.writes[0].target, local_target);
        let mut local = OwnedStakeholderSet::default();
        local.local.insert(
            local_target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let staged =
            crate::action_graph::stage_backward_local_deltas(&local, action.tick, &pass.deltas)
                .unwrap();
        assert_eq!(staged.writes.len(), 1);
        assert_eq!(staged.writes[0].target, local_target);
        local.ground.insert(
            local_target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let turn = prepare_local_turn(
            &graphs,
            action.tick,
            [ActionRef {
                tick: action.tick,
                id: action.id,
            }],
            29,
            |realm| realm == local_target.action_realm(),
            &local,
        )
        .unwrap();
        let graph = &turn.graphs.ticks[&action.tick];
        let plan = graph.ground_ready_plan(action.tick, |_| true, |_| false);
        let ground = stage_ground_ready(
            graph,
            &plan,
            |realm| realm == local_target.action_realm(),
            &local,
        )
        .unwrap();
        assert_eq!(ground.writes.len(), 1);
        assert_eq!(ground.writes[0].target, local_target);
        assert_eq!(
            ground.applied_realms,
            vec![(
                ActionRef {
                    tick: action.tick,
                    id: action.id
                },
                local_target.action_realm()
            )]
        );
        let mut graph_after_first = turn.graphs;
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        graph_after_first
            .mark_ground_realm_applied(reference, local_target.action_realm())
            .unwrap();
        local
            .ground
            .insert(local_target, ground.writes[0].after.clone().unwrap());
        local.ground.insert(
            remote_target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let graph = &graph_after_first.ticks[&action.tick];
        let plan = graph.ground_ready_plan(action.tick, |_| true, |_| false);
        let mut stale = plan.clone();
        stale
            .unapplied_realms
            .get_mut(&reference)
            .unwrap()
            .insert(local_target.action_realm());
        assert!(matches!(
            stage_ground_ready(graph, &stale, |_| true, &local),
            Err(GroundDeltaFailure::MissingAction(candidate)) if candidate == reference
        ));
        let mut stale = plan.clone();
        *stale.semantic_generations.get_mut(&reference).unwrap() += 1;
        assert!(matches!(
            stage_ground_ready(graph, &stale, |_| true, &local),
            Err(GroundDeltaFailure::MissingAction(candidate)) if candidate == reference
        ));
        let remaining = stage_ground_ready(graph, &plan, |_| true, &local).unwrap();
        assert_eq!(remaining.writes.len(), 1);
        assert_eq!(remaining.writes[0].target, remote_target);
        assert_eq!(
            remaining.applied_realms,
            vec![(reference, remote_target.action_realm())]
        );
    }

    fn anchors(targets: &[Target]) -> BTreeMap<Target, Option<Cell>> {
        targets
            .iter()
            .map(|target| {
                (
                    *target,
                    Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                )
            })
            .collect()
    }

    fn solve(
        actions: &[Action],
        anchor: &BTreeMap<Target, Option<Cell>>,
        seed: u64,
    ) -> SolvedComponent {
        let mut graph = TickActionGraph::default();
        for action in actions {
            graph.insert(action.clone()).unwrap();
        }
        let component = actions.iter().map(|action| action.id).collect();
        solve_component(&graph, &component, anchor, seed, |_| Some(true)).unwrap()
    }

    #[test]
    fn scheduled_ticks_compose_in_source_order_across_reverse_arrival() {
        use crate::canonical_action::{ScheduledTickFact, ScheduledTickKind};
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Whole,
            inventory: None,
        };
        let make = |peer: ServerId, order: u64| {
            let mut action = action(peer, &[], &[(target, 0)]);
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
                    id: peer.0,
                    position: BlockPos {
                        x: peer.0 as i32,
                        y: 64,
                        z: 1,
                    },
                    due: TickStamp(9),
                    priority: 0,
                    source_order: order,
                },
            }];
            action
        };
        let first = make(ServerId(1), 4);
        let second = make(ServerId(2), 5);
        let mut drain = action(ServerId(3), &[], &[(target, 0)]);
        drain.class = ActionClass::Infallible;
        drain.kind = ActionKind::ScheduledTick;
        drain.effects = vec![Effect::DrainScheduledTicks {
            target,
            through: drain.tick,
        }];
        let anchor = BTreeMap::from([(
            target,
            Some(Cell {
                revision: 0,
                value: Value::ScheduledQueue(vec![ScheduledTickFact {
                    executor: crate::realm_completion::PeerGeneration {
                        server: crate::identity::ServerId(0),
                        generation: 1,
                    },
                    origin: crate::canonical_action::ScheduledTickOrigin::GenesisPrimary,
                    kind: ScheduledTickKind::Block,
                    id: 4,
                    position: BlockPos { x: 4, y: 64, z: 1 },
                    due: TickStamp(7),
                    priority: 0,
                    source_order: 1,
                }]),
            }),
        )]);
        let solved = solve(&[second.clone(), first.clone(), drain.clone()], &anchor, 31);
        assert_eq!(solved.causal_order, vec![drain.id, first.id, second.id]);
        let SolvedVariant::Applied { result, .. } = &solved.variants[&second.id] else {
            panic!("second schedule must apply");
        };
        let Some(Cell {
            value: Value::ScheduledQueue(queue),
            ..
        }) = &result.writes[0].after
        else {
            panic!("queue write missing");
        };
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0].id, 1);
        assert_eq!(queue[1].id, 2);
    }

    #[test]
    fn causal_cycle_keeps_feasible_lower_rank_action() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let x = block(1);
        let y = block(2);
        let high = action(ranked[0], &[(x, 0), (y, 1)], &[(x, 1)]);
        let low = action(ranked[1], &[(x, 0), (y, 0)], &[(y, 1)]);
        let result = solve(&[high.clone(), low.clone()], &anchors(&[x, y]), seed);
        assert_eq!(result.variants[&high.id], SolvedVariant::Undone);
        assert!(matches!(
            result.variants[&low.id],
            SolvedVariant::Applied { .. }
        ));
    }

    #[test]
    fn whole_condition_support_keeps_both_lower_rank_producers() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let x = block(1);
        let p = block(2);
        let z = block(3);
        let high = action(ranked[0], &[(x, 0), (p, 1), (z, 0)], &[(z, 1)]);
        let middle = action(ranked[1], &[(x, 0)], &[(x, 1)]);
        let low = action(ranked[2], &[(x, 1), (p, 0)], &[(p, 1)]);
        let result = solve(
            &[high.clone(), middle.clone(), low.clone()],
            &anchors(&[x, p, z]),
            seed,
        );
        assert_eq!(result.variants[&high.id], SolvedVariant::Undone);
        assert!(matches!(
            result.variants[&middle.id],
            SolvedVariant::Applied { .. }
        ));
        assert!(matches!(
            result.variants[&low.id],
            SolvedVariant::Applied { .. }
        ));
    }

    #[test]
    fn three_way_place_winner_is_independent_of_arrival_order() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let target = block(1);
        let actions = ranked
            .iter()
            .enumerate()
            .map(|(index, actor)| action(*actor, &[(target, 0)], &[(target, index as u16 + 1)]))
            .collect::<Vec<_>>();
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let ordered = order
                .iter()
                .map(|index| actions[*index].clone())
                .collect::<Vec<_>>();
            let result = solve(&ordered, &anchors(&[target]), seed);
            assert!(matches!(
                result.variants[&actions[0].id],
                SolvedVariant::Applied { .. }
            ));
            assert_eq!(result.variants[&actions[1].id], SolvedVariant::Undone);
            assert_eq!(result.variants[&actions[2].id], SolvedVariant::Undone);
        }
    }

    #[test]
    fn later_half_stack_move_replays_against_remaining_source() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let key = PlayerKey(Uuid::from_u128(1));
        let slot = |index| Target::Player {
            key,
            lease: Some(crate::canonical_action::LivePlayerLease {
                key,
                gid: crate::identity::GlobalPlayerId {
                    server: ServerId(1),
                    player: crate::identity::PlayerSlot(1),
                },
                incarnation: 1,
            }),
            inventory: Some(InventoryId::Player {
                inv: 0,
                slot: index,
            }),
        };
        let source = slot(0);
        let first_destination = slot(1);
        let second_destination = slot(2);
        let cell = |index, count| Cell {
            revision: 0,
            value: Value::Inventory(if count == 0 {
                InventoryCell::empty(InvLoc {
                    inv: 0,
                    slot: index,
                })
            } else {
                InventoryCell {
                    loc: InvLoc {
                        inv: 0,
                        slot: index,
                    },
                    item: 1,
                    count,
                    nbt: vec![4],
                }
            }),
        };
        let first = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(ranked[0]),
                incarnation: 0,
                sequence: ranked[0].0,
                family: 0,
            },
            tick: TickStamp(7),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: vec![crate::canonical_action::LivePlayerLease {
                key,
                gid: crate::identity::GlobalPlayerId {
                    server: ServerId(1),
                    player: crate::identity::PlayerSlot(1),
                },
                incarnation: 1,
            }],
            player_chunks: vec![(key, ChunkKey::new(WorldId::new(0, 0), 0, 0))],
            stakeholders: vec![
                Stakeholder::Chunk(ChunkKey::new(WorldId::new(0, 0), 0, 0)),
                Stakeholder::Player(key),
            ],
            dependencies: Vec::new(),
            conditions: vec![
                Condition {
                    target: source,
                    expected: Expected::Exact(cell(0, 3)),
                },
                Condition {
                    target: first_destination,
                    expected: Expected::Exact(cell(1, 0)),
                },
            ],
            effects: vec![Effect::MoveItems {
                source,
                destination: first_destination,
                count: 2,
                max_stack: 64,
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Inventory,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        let mut second = first.clone();
        second.id.actor = ActorId::Peer(ranked[1]);
        second.id.sequence = ranked[1].0;
        second.conditions[1].target = second_destination;
        second.effects[0] = Effect::MoveItems {
            source,
            destination: second_destination,
            count: 2,
            max_stack: 64,
        };
        second.counterfactual_conditions = vec![source, second_destination];
        second.counterfactual_effects = vec![source, second_destination];
        second.semantic_intent = Some(SemanticIntent::InventoryMove(InventoryReplayIntent {
            source,
            item: 1,
            components: vec![4],
            count: InventoryCountRule::Half,
            ordered_destinations: vec![InventoryReplayCandidate {
                target: second_destination,
                max_stack: 64,
            }],
            selector_revision: 0,
            destination_policy: crate::canonical_action::InventoryDestinationPolicy::Ordered,
            equipment: Vec::new(),
        }));
        second.class = ActionClass::Replayable;
        let result = solve(
            &[first.clone(), second.clone()],
            &BTreeMap::from([
                (source, Some(cell(0, 3))),
                (first_destination, Some(cell(1, 0))),
                (second_destination, Some(cell(2, 0))),
            ]),
            seed,
        );
        assert!(matches!(
            result.variants[&first.id],
            SolvedVariant::Applied { .. }
        ));
        let SolvedVariant::Applied {
            effects,
            result: applied,
            replayed: true,
            ..
        } = &result.variants[&second.id]
        else {
            panic!("half-stack move was not replayed");
        };
        assert_eq!(
            effects,
            &vec![Effect::MoveItems {
                source,
                destination: second_destination,
                count: 1,
                max_stack: 64,
            }]
        );
        assert!(applied.writes.iter().any(|write| {
            write.target == source
                && matches!(
                    write.after.as_ref().map(|cell| &cell.value),
                    Some(Value::Inventory(item)) if item.count == 0
                )
        }));
    }

    #[test]
    fn absent_anchor_is_not_treated_as_missing_world_cell() {
        let target = block(1);
        let action = action(ServerId(1), &[(target, 0)], &[(target, 1)]);
        let mut graph = TickActionGraph::default();
        graph.insert(action.clone()).unwrap();
        let component = BTreeSet::from([action.id]);
        assert_eq!(
            solve_component(&graph, &component, &BTreeMap::new(), 29, |_| Some(true)),
            Err(SolveFailure::MissingAnchor(target))
        );
    }

    #[test]
    fn inventory_replay_retargets_when_original_destination_fills() {
        use crate::canonical_action::{
            ActionKind, ClientOrigin, InventoryCountRule, InventoryReplayCandidate,
            InventoryReplayIntent, SemanticIntent, UndoProcedure,
        };
        use crate::inventory::{InvLoc, InventoryCell};
        use uuid::Uuid;

        let key = crate::canonical_action::PlayerKey(Uuid::from_u128(41));
        let slot = |index| Target::Player {
            key,
            lease: Some(crate::canonical_action::LivePlayerLease {
                key,
                gid: crate::identity::GlobalPlayerId {
                    server: ServerId(1),
                    player: crate::identity::PlayerSlot(1),
                },
                incarnation: 1,
            }),
            inventory: Some(InventoryId::Player {
                inv: 0,
                slot: index,
            }),
        };
        let source = slot(0);
        let first_destination = slot(1);
        let second_destination = slot(2);
        let cell = |index, count| Cell {
            revision: 0,
            value: Value::Inventory(if count == 0 {
                InventoryCell::empty(InvLoc {
                    inv: 0,
                    slot: index,
                })
            } else {
                InventoryCell {
                    loc: InvLoc {
                        inv: 0,
                        slot: index,
                    },
                    item: 1,
                    count,
                    nbt: vec![4],
                }
            }),
        };
        let ranked = ranked_peers(29);
        let mut first = action(ranked[0], &[], &[]);
        first.kind = ActionKind::Inventory;
        first.player_leases = vec![crate::canonical_action::LivePlayerLease {
            key,
            gid: crate::identity::GlobalPlayerId {
                server: ServerId(1),
                player: crate::identity::PlayerSlot(1),
            },
            incarnation: 1,
        }];
        first.player_chunks = vec![(key, ChunkKey::new(WorldId::new(0, 0), 0, 0))];
        first.stakeholders = vec![
            Stakeholder::Chunk(ChunkKey::new(WorldId::new(0, 0), 0, 0)),
            Stakeholder::Player(key),
        ];
        first.conditions = vec![
            Condition {
                target: source,
                expected: Expected::Exact(cell(0, 3)),
            },
            Condition {
                target: first_destination,
                expected: Expected::Exact(cell(1, 0)),
            },
        ];
        first.effects = vec![Effect::MoveItems {
            source,
            destination: first_destination,
            count: 2,
            max_stack: 2,
        }];
        first.undo = vec![UndoProcedure::RestoreWrittenTargets];
        first.client_origin = ClientOrigin::None;
        let mut second = first.clone();
        second.id.actor = ActorId::Peer(ranked[1]);
        second.id.sequence = ranked[1].0;
        second.effects = vec![Effect::MoveItems {
            source,
            destination: first_destination,
            count: 2,
            max_stack: 2,
        }];
        second.counterfactual_conditions = vec![source, first_destination, second_destination];
        second.counterfactual_effects = second.counterfactual_conditions.clone();
        second.semantic_intent = Some(SemanticIntent::InventoryMove(InventoryReplayIntent {
            source,
            item: 1,
            components: vec![4],
            count: InventoryCountRule::Half,
            ordered_destinations: vec![
                InventoryReplayCandidate {
                    target: first_destination,
                    max_stack: 2,
                },
                InventoryReplayCandidate {
                    target: second_destination,
                    max_stack: 64,
                },
            ],
            selector_revision: 0,
            destination_policy: crate::canonical_action::InventoryDestinationPolicy::Ordered,
            equipment: Vec::new(),
        }));
        second.class = ActionClass::Replayable;
        let result = solve(
            &[first.clone(), second.clone()],
            &BTreeMap::from([
                (source, Some(cell(0, 3))),
                (first_destination, Some(cell(1, 0))),
                (second_destination, Some(cell(2, 0))),
            ]),
            29,
        );
        let SolvedVariant::Applied {
            effects,
            conditions,
            replayed: true,
            ..
        } = &result.variants[&second.id]
        else {
            panic!("retargeted move did not replay");
        };
        assert_eq!(effects.len(), 1);
        assert!(
            matches!(effects[0], Effect::MoveItems { destination, count: 1, .. } if destination == second_destination)
        );
        assert!(
            !conditions
                .iter()
                .any(|condition| condition.target == first_destination)
        );
    }

    #[test]
    fn infallible_action_applies_without_a_condition() {
        let target = block(1);
        let mut transition = action(ServerId(1), &[(target, 1)], &[(target, 2)]);
        transition.class = ActionClass::Infallible;
        transition.chunk_transition = true;
        transition.conditions.clear();
        let mut graph = TickActionGraph::default();
        graph.insert(transition.clone()).unwrap();
        let solved = solve_component(
            &graph,
            &BTreeSet::from([transition.id]),
            &anchors(&[target]),
            29,
            |_| Some(true),
        )
        .unwrap();
        assert!(matches!(
            solved.variants[&transition.id],
            SolvedVariant::Applied { .. }
        ));
    }

    #[test]
    fn recovery_pinned_low_hash_outcome_cannot_be_undone() {
        let target = block(1);
        let peers = ranked_peers(29);
        let winner = action(peers[0], &[(target, 0)], &[(target, 2)]);
        let pinned = action(peers[2], &[(target, 0)], &[(target, 3)]);
        let initial = anchors(&[target]);
        let mut mirror = OwnedStakeholderSet::default();
        mirror
            .local
            .insert(target, initial[&target].clone().unwrap());
        let result = prepare_action(&mirror, &pinned, TruthView::Local).unwrap();
        let recovery = CertifiedRecoveryConstraints {
            outcomes: BTreeMap::from([(
                ActionRef {
                    tick: pinned.tick,
                    id: pinned.id,
                },
                RecoveryPinnedOutcome {
                    core: pinned.clone(),
                    accepted: true,
                    realms: BTreeSet::from([target.action_realm()]),
                    conditions: pinned.conditions.clone(),
                    writes: result.writes,
                },
            )]),
        };
        let mut graph = TickActionGraph::default();
        graph.insert(winner.clone()).unwrap();
        graph.insert(pinned.clone()).unwrap();
        let solved = solve_component_with_recovery(
            &graph,
            &BTreeSet::from([winner.id, pinned.id]),
            &initial,
            29,
            |_| Some(true),
            &OwnedStakeholderSet::default(),
            &recovery,
        )
        .unwrap();
        assert!(matches!(
            solved.variants[&pinned.id],
            SolvedVariant::Applied { .. }
        ));
        assert_eq!(solved.variants[&winner.id], SolvedVariant::Undone);
    }

    #[test]
    fn recovery_rejects_a_divergent_pinned_written_cell() {
        let target = block(1);
        let core = action(ServerId(1), &[(target, 0)], &[(target, 2)]);
        let initial = anchors(&[target]);
        let mut mirror = OwnedStakeholderSet::default();
        mirror
            .local
            .insert(target, initial[&target].clone().unwrap());
        let mut result = prepare_action(&mirror, &core, TruthView::Local).unwrap();
        result.writes[0].after.as_mut().unwrap().value = Value::Block(99);
        let recovery = CertifiedRecoveryConstraints {
            outcomes: BTreeMap::from([(
                ActionRef {
                    tick: core.tick,
                    id: core.id,
                },
                RecoveryPinnedOutcome {
                    core: core.clone(),
                    accepted: true,
                    realms: BTreeSet::from([target.action_realm()]),
                    conditions: core.conditions.clone(),
                    writes: result.writes,
                },
            )]),
        };
        let mut graph = TickActionGraph::default();
        graph.insert(core.clone()).unwrap();
        assert_eq!(
            solve_component_with_recovery(
                &graph,
                &BTreeSet::from([core.id]),
                &initial,
                29,
                |_| Some(true),
                &OwnedStakeholderSet::default(),
                &recovery
            ),
            Err(SolveFailure::InvalidComponent)
        );
    }

    #[test]
    fn recovery_replays_unpinned_inventory_leg_in_another_world() {
        let source_chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let destination_chunk = ChunkKey::new(WorldId::new(0, 2), 0, 0);
        let slot = |key, x, index| Target::Chunk {
            key,
            part: ChunkPart::Block(BlockPos { x, y: 64, z: 0 }),
            inventory: Some(InventoryId::Block {
                inv: 0,
                slot: index,
            }),
        };
        let source = slot(source_chunk, 0, 0);
        let other_source = slot(destination_chunk, 1, 0);
        let first_destination = slot(destination_chunk, 0, 0);
        let second_destination = slot(destination_chunk, 0, 1);
        let cell = |index, count| Cell {
            revision: 0,
            value: Value::Inventory(if count == 0 {
                InventoryCell::empty(InvLoc::new(0, index))
            } else {
                InventoryCell {
                    loc: InvLoc::new(0, index),
                    item: 1,
                    count,
                    nbt: vec![4],
                }
            }),
        };
        let initial = BTreeMap::from([
            (source, Some(cell(0, 3))),
            (other_source, Some(cell(0, 2))),
            (first_destination, Some(cell(0, 0))),
            (second_destination, Some(cell(1, 0))),
        ]);
        let peers = ranked_peers(29);
        let mut competitor = action(peers[0], &[], &[]);
        competitor.kind = ActionKind::Inventory;
        competitor.stakeholders = vec![Stakeholder::Chunk(destination_chunk)];
        competitor.conditions = vec![
            Condition {
                target: other_source,
                expected: Expected::Exact(cell(0, 2)),
            },
            Condition {
                target: first_destination,
                expected: Expected::Exact(cell(0, 0)),
            },
        ];
        competitor
            .conditions
            .sort_by_key(|condition| condition.target);
        competitor.effects = vec![Effect::MoveItems {
            source: other_source,
            destination: first_destination,
            count: 2,
            max_stack: 2,
        }];
        let mut pinned = action(peers[2], &[], &[]);
        pinned.kind = ActionKind::Inventory;
        pinned.class = ActionClass::Replayable;
        pinned.stakeholders = vec![
            Stakeholder::Chunk(source_chunk),
            Stakeholder::Chunk(destination_chunk),
        ];
        pinned.conditions = vec![
            Condition {
                target: source,
                expected: Expected::Exact(cell(0, 3)),
            },
            Condition {
                target: first_destination,
                expected: Expected::Exact(cell(0, 0)),
            },
        ];
        pinned.conditions.sort_by_key(|condition| condition.target);
        pinned.effects = vec![Effect::MoveItems {
            source,
            destination: first_destination,
            count: 2,
            max_stack: 2,
        }];
        pinned.counterfactual_conditions = vec![source, first_destination, second_destination];
        pinned.counterfactual_conditions.sort_unstable();
        pinned.counterfactual_effects = pinned.counterfactual_conditions.clone();
        pinned.semantic_intent = Some(SemanticIntent::InventoryMove(InventoryReplayIntent {
            source,
            item: 1,
            components: vec![4],
            count: InventoryCountRule::Exact(2),
            ordered_destinations: vec![
                InventoryReplayCandidate {
                    target: first_destination,
                    max_stack: 2,
                },
                InventoryReplayCandidate {
                    target: second_destination,
                    max_stack: 2,
                },
            ],
            selector_revision: 0,
            destination_policy: crate::canonical_action::InventoryDestinationPolicy::Ordered,
            equipment: Vec::new(),
        }));
        let mut mirror = OwnedStakeholderSet::default();
        mirror.local.extend(
            initial
                .iter()
                .filter_map(|(target, value)| value.clone().map(|cell| (*target, cell))),
        );
        let result = prepare_action(&mirror, &pinned, TruthView::Local).unwrap();
        let recovery = CertifiedRecoveryConstraints {
            outcomes: BTreeMap::from([(
                ActionRef {
                    tick: pinned.tick,
                    id: pinned.id,
                },
                RecoveryPinnedOutcome {
                    core: pinned.clone(),
                    accepted: true,
                    realms: BTreeSet::from([source.action_realm()]),
                    conditions: pinned
                        .conditions
                        .iter()
                        .filter(|condition| condition.target == source)
                        .cloned()
                        .collect(),
                    writes: result
                        .writes
                        .iter()
                        .filter(|write| write.target == source)
                        .cloned()
                        .collect(),
                },
            )]),
        };
        let mut graph = TickActionGraph::default();
        graph.insert(competitor.clone()).unwrap();
        graph.insert(pinned.clone()).unwrap();
        let original = solve_component(&graph, &BTreeSet::from([pinned.id]), &initial, 29, |_| {
            Some(true)
        })
        .unwrap();
        install_solved_component(&mut graph, &original).unwrap();
        let node = graph.actions.get_mut(&pinned.id).unwrap();
        node.applied_realms.insert(source.action_realm());
        node.local_result = Some(result.clone());
        node.local_order = Some(0);
        let solved = solve_component_with_recovery(
            &graph,
            &BTreeSet::from([competitor.id, pinned.id]),
            &initial,
            29,
            |_| Some(true),
            &OwnedStakeholderSet::default(),
            &recovery,
        )
        .unwrap();
        assert!(matches!(
            solved.variants[&competitor.id],
            SolvedVariant::Applied { .. }
        ));
        let SolvedVariant::Applied {
            result,
            replayed: true,
            ..
        } = &solved.variants[&pinned.id]
        else {
            panic!("unpinned leg did not replay");
        };
        assert!(
            result
                .writes
                .iter()
                .any(|write| write.target == second_destination)
        );
        assert!(
            !result
                .writes
                .iter()
                .any(|write| write.target == first_destination)
        );
        assert_eq!(
            result
                .writes
                .iter()
                .find(|write| write.target == source)
                .unwrap()
                .after,
            Some(Cell {
                revision: 1,
                value: Value::Inventory(InventoryCell {
                    loc: InvLoc::new(0, 0),
                    item: 1,
                    count: 1,
                    nbt: vec![4],
                })
            })
        );
        let old_local = graph.actions[&pinned.id].local_result.clone();
        let deltas = semantic_deltas_for_recovery(&graph, &solved, &recovery).unwrap();
        assert_eq!(
            deltas
                .iter()
                .find(|delta| delta.reference.id == pinned.id)
                .unwrap()
                .previous,
            old_local
        );
        install_solved_component_for_recovery(&mut graph, &solved, &recovery).unwrap();
        assert_eq!(graph.actions[&pinned.id].local_result, old_local);
        assert!(
            graph.actions[&pinned.id]
                .applied_realms
                .contains(&source.action_realm())
        );
    }

    #[test]
    fn chunk_transition_is_last_even_when_it_has_higher_hash_rank() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let first_target = block(1);
        let second_target = block(2);
        let mut transition = action(ranked[0], &[(first_target, 0)], &[(first_target, 1)]);
        transition.class = ActionClass::Infallible;
        transition.chunk_transition = true;
        transition.conditions.clear();
        let ordinary = action(ranked[1], &[(second_target, 0)], &[(second_target, 1)]);
        let result = solve(
            &[transition.clone(), ordinary.clone()],
            &anchors(&[first_target, second_target]),
            seed,
        );
        assert_eq!(result.causal_order, vec![ordinary.id, transition.id]);
    }

    #[test]
    fn independent_chunk_transitions_both_follow_ordinary_actions() {
        let seed = 29;
        let ranked = ranked_peers(seed);
        let targets = [block(1), block(2), block(3)];
        let mut first = action(ranked[0], &[(targets[0], 0)], &[(targets[0], 1)]);
        first.class = ActionClass::Infallible;
        first.chunk_transition = true;
        first.conditions.clear();
        let mut second = action(ranked[1], &[(targets[1], 0)], &[(targets[1], 1)]);
        second.class = ActionClass::Infallible;
        second.chunk_transition = true;
        second.conditions.clear();
        let ordinary = action(ranked[2], &[(targets[2], 0)], &[(targets[2], 1)]);
        let result = solve(
            &[first.clone(), second.clone(), ordinary.clone()],
            &anchors(&targets),
            seed,
        );
        assert_eq!(result.causal_order.first(), Some(&ordinary.id));
        assert_eq!(result.causal_order.len(), 3);
        assert!(result.causal_order.contains(&first.id));
        assert!(result.causal_order.contains(&second.id));
    }

    #[test]
    fn invalid_joint_replay_does_not_mutate_any_graph_node() {
        let targets = [block(1), block(2), block(3)];
        let first = action(ServerId(1), &[(targets[0], 0)], &[(targets[0], 1)]);
        let second = action(ServerId(2), &[(targets[1], 0)], &[(targets[1], 1)]);
        let mut graph = TickActionGraph::default();
        graph.insert(first.clone()).unwrap();
        graph.insert(second.clone()).unwrap();
        let mut solved = solve_component(
            &graph,
            &BTreeSet::from([first.id, second.id]),
            &anchors(&targets),
            29,
            |_| Some(true),
        )
        .unwrap();
        let SolvedVariant::Applied { effects, .. } = solved.variants.get_mut(&second.id).unwrap()
        else {
            panic!("independent action did not apply");
        };
        effects.push(Effect::Put {
            target: targets[2],
            value: Value::Block(3),
        });
        let before_edges = graph.edge_reasons.clone();
        assert_eq!(
            install_solved_component(&mut graph, &solved),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(graph.edge_reasons, before_edges);
        assert_eq!(graph.actions[&first.id].generation, 0);
        assert_eq!(graph.actions[&second.id].generation, 0);
    }

    #[test]
    fn solved_conflict_installs_one_terminal_winner_and_ground_candidate() {
        let target = block(1);
        let ranked = ranked_peers(29);
        let winner = action(ranked[0], &[(target, 0)], &[(target, 1)]);
        let loser = action(ranked[1], &[(target, 0)], &[(target, 2)]);
        let mut graph = TickActionGraph::default();
        graph.insert(winner.clone()).unwrap();
        graph.insert(loser.clone()).unwrap();
        let solved = solve_component(
            &graph,
            &BTreeSet::from([winner.id, loser.id]),
            &anchors(&[target]),
            29,
            |_| Some(true),
        )
        .unwrap();
        let deltas = semantic_deltas(&graph, &solved).unwrap();
        assert_eq!(deltas.len(), 1);
        let mut local = OwnedStakeholderSet::default();
        local.local.insert(
            target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let projection =
            crate::action_graph::stage_backward_local_deltas(&local, winner.tick, &deltas).unwrap();
        assert_eq!(projection.writes.len(), 1);
        assert_eq!(projection.writes[0].target, target);
        assert_eq!(
            projection.writes[0].after.as_ref().map(|cell| &cell.value),
            Some(&Value::Block(1))
        );
        install_solved_component(&mut graph, &solved).unwrap();
        assert_eq!(
            graph.mark_stable(winner.id),
            Err(ApplyFailure::InvalidAction)
        );
        graph
            .mark_local_applied(
                winner.id,
                0,
                projection.applied[&ActionRef {
                    tick: winner.tick,
                    id: winner.id,
                }]
                    .clone(),
            )
            .unwrap();
        graph.mark_local_undone(loser.id).unwrap();
        graph.mark_stable(winner.id).unwrap();
        graph.mark_stable(loser.id).unwrap();
        assert_eq!(graph.actions[&loser.id].wrapper, SemanticWrapper::Undone);
        assert_eq!(graph.actions[&winner.id].wrapper, SemanticWrapper::Applied);
        let plan = graph.ground_ready_plan(winner.tick, |_| true, |_| false);
        assert_eq!(
            plan.components
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                ActionRef {
                    tick: winner.tick,
                    id: winner.id
                },
                ActionRef {
                    tick: loser.tick,
                    id: loser.id
                },
            ]),
        );
    }

    #[test]
    fn late_conflict_undo_uses_local_token_not_issuer_factual_witness() {
        let target = block(1);
        let ranked = ranked_peers(29);
        let mut earlier_local = action(ranked[1], &[(target, 0)], &[(target, 2)]);
        let winner = action(ranked[0], &[(target, 0)], &[(target, 1)]);
        let mut anchor = OwnedStakeholderSet::default();
        anchor.local.insert(
            target,
            Cell {
                revision: 0,
                value: Value::Block(0),
            },
        );
        let factual = prepare_action(&anchor, &earlier_local, TruthView::Local).unwrap();
        earlier_local.factual_result = Some(factual.clone());
        let mut local_token = factual;
        local_token.writes[0].before.as_mut().unwrap().revision = 10;
        local_token.writes[0].after.as_mut().unwrap().revision = 11;
        let mut graph = TickActionGraph::default();
        graph.insert(earlier_local.clone()).unwrap();
        graph
            .mark_local_applied(earlier_local.id, 0, local_token.clone())
            .unwrap();
        graph.insert(winner.clone()).unwrap();
        let solved = solve_component(
            &graph,
            &BTreeSet::from([earlier_local.id, winner.id]),
            &anchors(&[target]),
            29,
            |_| Some(true),
        )
        .unwrap();
        let deltas = semantic_deltas(&graph, &solved).unwrap();
        let undo = deltas
            .iter()
            .find(|delta| delta.reference.id == earlier_local.id)
            .unwrap();
        assert_eq!(undo.previous, Some(local_token));
        assert_eq!(undo.next, None);
        let mut local = OwnedStakeholderSet::default();
        local.local.insert(
            target,
            Cell {
                revision: 11,
                value: Value::Block(2),
            },
        );
        let staged =
            crate::action_graph::stage_backward_local_deltas(&local, winner.tick, &deltas).unwrap();
        assert_eq!(staged.writes[0].before.as_ref().unwrap().revision, 11);
        assert_eq!(staged.writes[0].after.as_ref().unwrap().revision, 11);
        assert_eq!(
            staged.writes[0].after.as_ref().unwrap().value,
            Value::Block(1)
        );
        let projected = &staged.applied[&ActionRef {
            tick: winner.tick,
            id: winner.id,
        }];
        assert_eq!(projected.writes[0].before.as_ref().unwrap().revision, 10);
        assert_eq!(projected.writes[0].after.as_ref().unwrap().revision, 11);
    }

    #[test]
    fn ground_component_keeps_transition_after_ordinary_causal_action() {
        let target = block(1);
        let ordinary = action(ServerId(1), &[(target, 0)], &[(target, 1)]);
        let mut transition = action(ServerId(2), &[(target, 1)], &[(target, 2)]);
        transition.class = ActionClass::Infallible;
        transition.chunk_transition = true;
        transition.conditions.clear();
        transition.dependencies.push(ActionRef {
            tick: ordinary.tick,
            id: ordinary.id,
        });
        let mut graph = TickActionGraph::default();
        graph.insert(ordinary.clone()).unwrap();
        graph.insert(transition.clone()).unwrap();
        let solved = solve_component(
            &graph,
            &BTreeSet::from([ordinary.id, transition.id]),
            &anchors(&[target]),
            29,
            |_| Some(true),
        )
        .unwrap();
        assert_eq!(solved.causal_order, vec![ordinary.id, transition.id]);
        install_solved_component(&mut graph, &solved).unwrap();
        for (index, id) in solved.causal_order.iter().enumerate() {
            let SolvedVariant::Applied { result, .. } = &solved.variants[id] else {
                panic!("infallible chain was undone");
            };
            graph
                .mark_local_applied(*id, index as u32, result.clone())
                .unwrap();
            graph.mark_stable(*id).unwrap();
        }
        assert_eq!(
            graph
                .ground_ready_plan(ordinary.tick, |_| true, |_| false)
                .components,
            vec![vec![
                ActionRef {
                    tick: ordinary.tick,
                    id: ordinary.id
                },
                ActionRef {
                    tick: transition.tick,
                    id: transition.id
                },
            ]],
        );
    }

    #[test]
    fn indexed_solver_uses_prior_semantic_frontier_for_next_tick() {
        let target = block(1);
        let prior = action(ServerId(1), &[(target, 0)], &[(target, 1)]);
        let mut current = action(ServerId(2), &[(target, 1)], &[(target, 2)]);
        current.tick = TickStamp(8);
        current.conditions[0].expected = Expected::Exact(Cell {
            revision: 1,
            value: Value::Block(1),
        });
        let mut graphs = ActionGraphs::default();
        graphs
            .install_ground_anchor(
                target,
                crate::action_graph::GroundAnchor {
                    settled_through: TickStamp(6),
                    value: Some(Cell {
                        revision: 0,
                        value: Value::Block(0),
                    }),
                },
            )
            .unwrap();
        graphs
            .insert_collected(prior.tick, [prior.clone()])
            .unwrap();
        let first =
            solve_indexed_component(&graphs, prior.tick, &BTreeSet::from([prior.id]), 29, |_| {
                Some(true)
            })
            .unwrap();
        let graph = graphs.ticks.get_mut(&prior.tick).unwrap();
        install_solved_component(graph, &first).unwrap();
        let SolvedVariant::Applied { result, .. } = &first.variants[&prior.id] else {
            panic!("prior action did not apply");
        };
        graph
            .mark_local_applied(prior.id, 0, result.clone())
            .unwrap();
        graph.mark_stable(prior.id).unwrap();
        graphs
            .insert_collected(current.tick, [current.clone()])
            .unwrap();
        let second = solve_indexed_component(
            &graphs,
            current.tick,
            &BTreeSet::from([current.id]),
            29,
            |_| Some(true),
        )
        .unwrap();
        assert!(matches!(
            second.variants[&current.id],
            SolvedVariant::Applied { .. }
        ));
    }
}
