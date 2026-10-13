use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    Action, ActionRef, ActionState, ApplyFailure, Cell, Condition, Target,
};
use pumpkin_cluster::time::TickStamp;

use super::cluster_world_callback_policy::{CallbackPolicyOwner, CallbackPolicyRequest};
use super::cluster_world_callback_snapshot::CallbackTickSnapshot;
use super::cluster_world_callbacks::{CallbackCapture, WorldUseContext};
use super::cluster_world_environment_callbacks::EnvironmentCallbackContext;

struct BlockEntityCallbackContext {
    seed: super::cluster_inventory_block_tick::FrozenBlockEntityTick,
    reference: ActionRef,
    producer: pumpkin_cluster::realm_completion::PeerGeneration,
    world: Arc<crate::world::World>,
    chunk: pumpkin_cluster::protocol::ChunkKey,
    position: pumpkin_util::math::position::BlockPos,
}

impl BlockEntityCallbackContext {
    fn complete<S: ActionState>(
        &self,
        view: &S,
        reserved: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    ) -> Result<Action, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{
            Effect, Expected, SemanticIntent, TruthView, Value,
        };
        let mut action = self
            .seed
            .bind_with_view(view, self.reference, self.producer, reserved)?;
        let values = action
            .counterfactual_conditions
            .iter()
            .filter_map(|target| {
                view.read(TruthView::Local, *target)
                    .map(|cell| (*target, cell))
            })
            .collect::<BTreeMap<_, _>>();
        let mut resolution = crate::block::neighbor::BlockCallbackResolution::default();
        for effect in &action.effects {
            if let Effect::TickBlockInventory { intent } = effect {
                for (target, after) in intent.derive_writes(view, &values)?.writes {
                    if let (
                        Target::Chunk {
                            part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
                            ..
                        },
                        Some(Value::Block(after)),
                    ) = (target, after)
                    {
                        let Some(Cell {
                            value: Value::Block(before),
                            ..
                        }) = values.get(&target)
                        else {
                            return Err(ApplyFailure::Condition(target));
                        };
                        resolution.writes.push((
                            pumpkin_util::math::position::BlockPos::new(
                                position.x, position.y, position.z,
                            ),
                            pumpkin_data::BlockStateId::new(*before)
                                .ok_or(ApplyFailure::Type(target))?,
                            pumpkin_data::BlockStateId::new(after)
                                .ok_or(ApplyFailure::Type(target))?,
                        ));
                    }
                }
            }
        }
        let plans = super::cluster_world_callbacks::resolution_light_plans(
            &self.world,
            self.chunk.world,
            &resolution,
        )?;
        if plans.is_empty() {
            return Ok(action);
        }
        for plan in &plans {
            for target in plan
                .section_targets()?
                .into_iter()
                .chain(plan.light_targets()?)
            {
                let cell = view
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                if !action.conditions.iter().any(|guard| guard.target == target) {
                    action.conditions.push(Condition {
                        target,
                        expected: Expected::RevisionEquals(cell.revision),
                    });
                }
                if !action.stakeholders.contains(&target.stakeholder()) {
                    action.stakeholders.push(target.stakeholder());
                }
            }
        }
        let Some(SemanticIntent::World(intent)) = &mut action.semantic_intent else {
            return Err(ApplyFailure::InvalidAction);
        };
        intent.steps.extend(
            plans
                .into_iter()
                .map(pumpkin_cluster::world_action::WorldReplayStep::DerivedLight),
        );
        let values = intent
            .targets()
            .filter_map(|target| {
                view.read(TruthView::Local, target)
                    .map(|cell| (target, cell))
            })
            .collect();
        action.effects = intent.derive_effects_with(&values, view)?;
        action.counterfactual_conditions = intent.read_targets().into_iter().collect();
        action.counterfactual_effects = intent.effect_targets().into_iter().collect();
        action.stakeholders.sort();
        action.validate()?;
        Ok(action)
    }
}

enum CallbackContext {
    BlockEntity(BlockEntityCallbackContext),
    ObjectTerminal(super::cluster_object_terminal::ObjectTerminalContext),
    PlayerUse(WorldUseContext),
    Mining(super::cluster_world_mining_context::MiningCallbackContext),
    Environment(EnvironmentCallbackContext),
}

impl CallbackContext {
    fn capture<S: ActionState>(
        &self,
        view: &S,
        snapshot: &mut crate::block::neighbor::NeighborSnapshot,
        facts: &mut BTreeMap<Target, Option<Cell>>,
    ) -> Result<CallbackCapture, super::cluster_world_callbacks::CallbackCaptureError> {
        let world = match self {
            Self::BlockEntity(context) => &context.world,
            Self::PlayerUse(context) => &context.world,
            Self::Environment(context) => &context.world,
            Self::Mining(context) => &context.world,
            Self::ObjectTerminal(context) => &context.world,
        };
        snapshot.lava_tick_delay = Some(if world.dimension.fast_lava { 10 } else { 30 });
        match self {
            Self::BlockEntity(_) => Ok(CallbackCapture::Ready(Default::default())),
            Self::PlayerUse(context) => {
                super::cluster_world_callbacks::capture_use_context(view, context, snapshot, facts)
                    .map(|(_, capture)| capture)
            }
            Self::Environment(context) => context.capture(view, snapshot, facts),
            Self::Mining(context) => context.capture(view, snapshot, facts),
            Self::ObjectTerminal(context) => context.capture(view, snapshot, facts),
        }
    }

    fn complete<S: ActionState>(
        &self,
        view: &S,
        facts: &mut BTreeMap<Target, Option<Cell>>,
        resolution: &crate::block::neighbor::BlockCallbackResolution,
        reserved: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    ) -> Result<Action, ApplyFailure> {
        match self {
            Self::BlockEntity(context) => context.complete(view, reserved),
            Self::PlayerUse(context) => super::cluster_world_callbacks::complete_use_context(
                view, context, facts, resolution,
            ),
            Self::Environment(context) => context.complete(view, facts, resolution),
            Self::Mining(context) => context.complete(view, facts, resolution),
            Self::ObjectTerminal(context) => context.complete(view, facts, resolution),
        }
    }

    fn location(
        &self,
    ) -> (
        pumpkin_cluster::protocol::WorldId,
        pumpkin_util::math::position::BlockPos,
    ) {
        match self {
            Self::BlockEntity(context) => (context.chunk.world, context.position),
            Self::PlayerUse(context) => (context.world_id, context.position),
            Self::Environment(context) => (context.chunk.world, context.position),
            Self::Mining(context) => context.location(),
            Self::ObjectTerminal(context) => context.location(),
        }
    }

    fn producer(&self) -> pumpkin_cluster::realm_completion::PeerGeneration {
        match self {
            Self::BlockEntity(context) => context.producer,
            Self::PlayerUse(context) => context.producer,
            Self::Environment(context) => context.producer,
            Self::Mining(context) => context.producer,
            Self::ObjectTerminal(context) => context.producer,
        }
    }

    fn reject(&self, server: &Arc<super::Server>) {
        match self {
            Self::PlayerUse(context) => super::cluster_item_use::reject(server, &context.proposal),
            Self::Mining(context) => context.reject(server),
            Self::BlockEntity(_) | Self::Environment(_) | Self::ObjectTerminal(_) => {}
        }
    }
}

struct UseWork {
    context: CallbackContext,
    fixed: Arc<CallbackTickSnapshot>,
    draft: ActorActionDraft,
    snapshot: crate::block::neighbor::NeighborSnapshot,
    facts: BTreeMap<Target, Option<Cell>>,
    guards: Vec<Condition>,
    loot: crate::world::loot::LootContextParameters,
    environment: Option<Arc<super::cluster_environment::EnvironmentView>>,
}

pub(super) struct CompletedCallbackWork {
    pub action: Action,
    issued: bool,
    fixed: Arc<CallbackTickSnapshot>,
    draft: ActorActionDraft,
    context: CallbackContext,
    guards: Vec<Condition>,
    loot: crate::world::loot::LootContextParameters,
    environment: Option<Arc<super::cluster_environment::EnvironmentView>>,
}

impl CompletedCallbackWork {
    pub(super) fn footprint(
        &self,
    ) -> std::collections::BTreeSet<pumpkin_cluster::canonical_action::Stakeholder> {
        self.action
            .targets()
            .into_iter()
            .chain(self.action.counterfactual_conditions.iter().copied())
            .chain(self.action.counterfactual_effects.iter().copied())
            .map(|target| target.stakeholder())
            .chain(self.action.stakeholders.iter().copied())
            .collect()
    }

    pub(super) fn replace_draft(&mut self, draft: ActorActionDraft) {
        assert!(!self.issued);
        self.draft = draft;
    }

    pub(super) fn is_issued(&self) -> bool {
        self.issued
    }
    pub(super) fn dependency_roots(&self) -> impl Iterator<Item = ActionRef> + '_ {
        self.action.dependencies.iter().copied()
    }

    pub(super) fn view(&self) -> impl ActionState + '_ {
        self.draft.view(self.fixed.as_ref())
    }

    pub(super) fn issue(
        &mut self,
    ) -> Result<(), pumpkin_cluster::actor_action_scope::ScopeFailure> {
        if self.issued {
            return Ok(());
        }
        self.draft
            .issue(self.fixed.as_ref(), self.action.clone())
            .map_err(|(reason, _)| reason)?;
        self.action = self.draft.actions().last().unwrap().clone();
        self.issued = true;
        if let CallbackContext::PlayerUse(context) = &mut self.context
            && let Some(menu) = &mut context.menu
            && let Some(presentation) = menu.presentation.take()
        {
            menu.permit
                .take()
                .expect("menu presentation reserved before source issue")
                .send(
                    super::cluster_invsee::LocalInventoryLifecycle::CanonicalOpenPresentation(
                        presentation,
                    ),
                );
        }
        Ok(())
    }

    pub(super) fn draft(&self) -> &ActorActionDraft {
        &self.draft
    }
}

pub(super) struct CallbackWorkOwner {
    work: HashMap<ActionRef, UseWork>,
    policy: CallbackPolicyOwner,
    diagnostics: HashMap<
        (
            pumpkin_cluster::protocol::WorldId,
            pumpkin_util::math::position::BlockPos,
            u8,
        ),
        std::time::Instant,
    >,
}

impl CallbackWorkOwner {
    pub(super) fn resume_environment(
        &mut self,
        completed: CompletedCallbackWork,
        draft: ActorActionDraft,
    ) -> Result<(), CompletedCallbackWork> {
        if !self.has_capacity()
            || !matches!(
                &completed.context,
                CallbackContext::Environment(_) | CallbackContext::BlockEntity(_)
            )
        {
            return Err(completed);
        }
        let reference = ActionRef {
            id: completed.action.id,
            tick: completed.action.tick,
        };
        if self.work.contains_key(&reference) {
            return Err(completed);
        }
        self.work.insert(
            reference,
            UseWork {
                context: completed.context,
                fixed: completed.fixed,
                draft,
                snapshot: Default::default(),
                facts: BTreeMap::new(),
                guards: completed.guards,
                loot: completed.loot,
                environment: completed.environment,
            },
        );
        Ok(())
    }
    pub(super) fn dependency_roots(&self) -> Vec<ActionRef> {
        self.work
            .values()
            .filter_map(|work| match &work.context {
                CallbackContext::Environment(context) => {
                    context.scheduled.and_then(|fact| match fact.origin {
                        pumpkin_cluster::canonical_action::ScheduledTickOrigin::Action {
                            cause,
                            ..
                        } => Some(cause),
                        pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary => {
                            None
                        }
                    })
                }
                CallbackContext::BlockEntity(_)
                | CallbackContext::PlayerUse(_)
                | CallbackContext::Mining(_) => None,
                CallbackContext::ObjectTerminal(context) => Some(context.body.birth.action),
            })
            .collect()
    }

    pub(super) fn new() -> Self {
        Self {
            work: HashMap::new(),
            policy: CallbackPolicyOwner::new(),
            diagnostics: HashMap::new(),
        }
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.work.len() < 1024
    }

    pub(super) fn remaining_capacity(&self) -> usize {
        1024 - self.work.len()
    }

    pub(super) fn blocked_actor(
        &self,
        actor: pumpkin_cluster::actor::ActorId,
        incarnation: u64,
    ) -> bool {
        self.work
            .keys()
            .any(|reference| reference.id.actor == actor && reference.id.incarnation == incarnation)
    }

    pub(super) fn blocked(&self, actor: pumpkin_cluster::actor::ActorId, incarnation: u64) -> bool {
        self.blocked_actor(actor, incarnation)
    }

    pub(super) fn oldest_pending_tick(&self) -> Option<TickStamp> {
        self.work
            .keys()
            .map(|reference| reference.tick)
            .reduce(|oldest, tick| if oldest.is_after(tick) { tick } else { oldest })
    }

    pub(super) fn oldest_reference(
        &self,
        actor: pumpkin_cluster::actor::ActorId,
        incarnation: u64,
    ) -> Option<ActionRef> {
        self.work
            .keys()
            .copied()
            .filter(|reference| {
                reference.id.actor == actor && reference.id.incarnation == incarnation
            })
            .reduce(|oldest, candidate| {
                if oldest.tick.is_after(candidate.tick)
                    || oldest.tick == candidate.tick
                        && oldest.id.sequence.wrapping_sub(candidate.id.sequence) < 32768
                {
                    candidate
                } else {
                    oldest
                }
            })
    }

    pub(super) fn stage_mining(
        &mut self,
        input: super::cluster_world_mining_context::FrozenMiningInput,
        producer: pumpkin_cluster::realm_completion::PeerGeneration,
        world: Arc<crate::world::World>,
    ) -> Result<(), super::cluster_world_mining_context::FrozenMiningInput> {
        let reference = ActionRef {
            id: input.proposal.id,
            tick: input.proposal.tick,
        };
        if self.work.contains_key(&reference) {
            return Ok(());
        }
        if !self.has_capacity() {
            return Err(input);
        }
        let loot = input.loot;
        let guards = vec![
            input.context.selected.clone(),
            Condition {
                target: input.context.source.target,
                expected: pumpkin_cluster::canonical_action::Expected::Exact(
                    input.context.source_cell.clone(),
                ),
            },
        ];
        self.work.insert(
            reference,
            UseWork {
                context: CallbackContext::Mining(
                    super::cluster_world_mining_context::MiningCallbackContext {
                        proposal: input.proposal,
                        producer,
                        world,
                        context: input.context,
                    },
                ),
                fixed: input.snapshot,
                draft: input.draft,
                snapshot: Default::default(),
                facts: BTreeMap::new(),
                guards,
                loot,
                environment: None,
            },
        );
        Ok(())
    }

    pub(super) fn capture_use_from_snapshot(
        &mut self,
        original: Arc<CallbackTickSnapshot>,
        server: &Arc<super::Server>,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        draft: &ActorActionDraft,
        proposal: &pumpkin_cluster::canonical_action::ActionProposal,
        tick: TickStamp,
        executors: &BTreeMap<
            pumpkin_cluster::protocol::ChunkKey,
            pumpkin_cluster::realm_completion::PeerGeneration,
        >,
    ) -> Result<(), ApplyFailure> {
        if !self.has_capacity() {
            return Err(ApplyFailure::InvalidAction);
        }
        let view = draft.view(original.as_ref());
        let (lease, binding, source, selected, _) =
            super::cluster_item_use::normalize_source(&view, original.as_ref(), proposal)?;
        let mut context = super::cluster_world_callbacks::normalize_use_on_block(
            server,
            &view,
            owners.inventory,
            original.as_ref(),
            proposal,
            tick,
        )?;
        let Target::Chunk {
            key: actor_chunk, ..
        } = context.actor_target
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let menu_target = Target::Chunk {
            key: actor_chunk,
            part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                actor: proposal.id.actor,
                property: pumpkin_cluster::living_property::LivingProperty::MenuSession,
            },
            inventory: None,
        };
        let scope = [
            context.actor_target,
            binding.target,
            selected.target,
            menu_target,
        ];
        // Select only already-captured facts and bindings from the shared original
        // actor phase. Retained work never grafts later current-owner facts.
        let mut fixed = original
            .clone()
            .with_scope(owners, &scope, &[lease], std::slice::from_ref(&binding))?
            .with_scheduled_executors(executors);
        if let Some(slot) = fixed.original_source_slot() {
            super::cluster_world_block_use::prepare_block_menu(
                original.as_ref(),
                &view,
                owners.inventory,
                &mut context,
                slot,
            )?;
            let mut policy_scope = scope.to_vec();
            policy_scope.extend(context.policy_facts.iter().map(|(target, _)| *target));
            fixed = fixed
                .with_scope(
                    owners,
                    &policy_scope,
                    &[lease],
                    std::slice::from_ref(&binding),
                )?
                .with_scheduled_executors(executors);
            if let Some(admission) = context
                .menu
                .as_ref()
                .and_then(|menu| menu.intent.admission())
            {
                fixed = fixed.with_reserved_generated_menu_inventory(
                    &admission,
                    ActionRef {
                        id: proposal.id,
                        tick,
                    },
                    slot,
                )?;
            }
        }
        let mut loot = crate::world::loot::LootContextParameters::default();
        if let pumpkin_cluster::canonical_action::Value::Living(living) = &context.actor.value {
            let luck_id = u16::from(pumpkin_data::attributes::Attributes::LUCK.id);
            if let Some(base) = living.combat.effective_attributes.get(&luck_id) {
                loot.luck = living
                    .combat
                    .equipment
                    .attribute(luck_id, f64::from_bits(*base)) as f32;
            }
        }
        if let pumpkin_cluster::canonical_action::Value::Inventory(item) = &source.value {
            loot.tool = Some(
                super::cluster_invsee::item_stack_from_inventory_cell(item)
                    .map_err(|_| ApplyFailure::Inventory(binding.target))?,
            );
        }
        if !self.stage(
            context,
            fixed,
            draft.clone(),
            Condition {
                target: binding.target,
                expected: pumpkin_cluster::canonical_action::Expected::Exact(source),
            },
            selected,
            loot,
        ) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }

    pub(super) fn stage_block_entity(
        &mut self,
        seed: super::cluster_inventory_block_tick::FrozenBlockEntityTick,
        reference: ActionRef,
        producer: pumpkin_cluster::realm_completion::PeerGeneration,
        world: Arc<crate::world::World>,
        chunk: pumpkin_cluster::protocol::ChunkKey,
        position: pumpkin_util::math::position::BlockPos,
        fixed: Arc<CallbackTickSnapshot>,
        draft: ActorActionDraft,
    ) -> Result<(), ApplyFailure> {
        if !self.has_capacity() {
            return Err(ApplyFailure::Capacity);
        }
        if reference.tick != fixed.original_tick()
            || reference.id.actor != pumpkin_cluster::actor::ActorId::Peer(producer.server)
            || reference.id.incarnation != producer.generation
            || producer.generation == 0
        {
            return Err(ApplyFailure::InvalidAction);
        }
        seed.captured_resource_usage()?;
        self.work.entry(reference).or_insert_with(|| UseWork {
            context: CallbackContext::BlockEntity(BlockEntityCallbackContext {
                seed,
                reference,
                producer,
                world,
                chunk,
                position,
            }),
            fixed,
            draft,
            snapshot: Default::default(),
            facts: BTreeMap::new(),
            guards: Vec::new(),
            loot: Default::default(),
            environment: None,
        });
        Ok(())
    }

    pub(super) fn stage_object_terminal(
        &mut self,
        context: super::cluster_object_terminal::ObjectTerminalContext,
        fixed: Arc<CallbackTickSnapshot>,
        draft: ActorActionDraft,
        loot: crate::world::loot::LootContextParameters,
    ) -> Result<(), ApplyFailure> {
        if !self.has_capacity() || !context.valid() {
            return Err(ApplyFailure::Capacity);
        }
        self.work
            .entry(context.reference)
            .or_insert_with(|| UseWork {
                context: CallbackContext::ObjectTerminal(context),
                fixed,
                draft,
                snapshot: Default::default(),
                facts: BTreeMap::new(),
                guards: Vec::new(),
                loot,
                environment: None,
            });
        Ok(())
    }

    pub(super) fn stage_environment(
        &mut self,
        context: EnvironmentCallbackContext,
        fixed: Arc<CallbackTickSnapshot>,
        draft: ActorActionDraft,
        loot: crate::world::loot::LootContextParameters,
        primary: bool,
    ) -> Result<(), ApplyFailure> {
        if !self.has_capacity()
            || !context.valid()
            || matches!(
                context.root,
                super::cluster_world_callbacks::CallbackRoot::Random { .. }
            ) && !primary
            || context.scheduled.is_some_and(|fact| {
                matches!(
                    fact.origin,
                    pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary
                )
            }) && !primary
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.work
            .entry(context.reference)
            .or_insert_with(|| UseWork {
                context: CallbackContext::Environment(context),
                fixed,
                draft,
                snapshot: Default::default(),
                facts: BTreeMap::new(),
                guards: Vec::new(),
                loot,
                environment: None,
            });
        Ok(())
    }

    pub(super) fn stage(
        &mut self,
        context: WorldUseContext,
        fixed: Arc<CallbackTickSnapshot>,
        draft: ActorActionDraft,
        source: Condition,
        selected: Condition,
        loot: crate::world::loot::LootContextParameters,
    ) -> bool {
        let reference = ActionRef {
            id: context.proposal.id,
            tick: context.tick,
        };
        if self.work.contains_key(&reference) {
            return true;
        }
        if !self.has_capacity() {
            return false;
        }
        self.work.insert(
            reference,
            UseWork {
                context: CallbackContext::PlayerUse(context),
                fixed,
                draft,
                snapshot: Default::default(),
                facts: BTreeMap::new(),
                guards: vec![source, selected],
                loot,
                environment: None,
            },
        );
        true
    }

    pub(super) fn oldest_tick(&self, current: TickStamp) -> TickStamp {
        self.work.keys().fold(current, |oldest, reference| {
            if oldest.is_after(reference.tick) {
                reference.tick
            } else {
                oldest
            }
        })
    }

    pub(super) fn advance(
        &mut self,
        server: &Arc<super::Server>,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        allocator: Option<&mut pumpkin_cluster::entity_origin::EntityOriginAllocator>,
    ) -> Vec<(ActionRef, Result<CompletedCallbackWork, ApplyFailure>)> {
        self.policy.drain();
        let mut allocator = allocator;
        let mut finished = Vec::new();
        let references: Vec<_> = self.work.keys().copied().collect();
        for reference in references {
            let work = self.work.get_mut(&reference).unwrap();
            if work.environment.is_none() {
                let Some(environment) = work.fixed.original_environment(work.context.location().0)
                else {
                    continue;
                };
                work.loot.world_time = environment.time.world_age as u64;
                work.loot.is_raining = Some(environment.weather.raining);
                work.loot.is_thundering = Some(environment.weather.thundering);
                work.environment = Some(environment);
            }
            if let CallbackContext::PlayerUse(context) = &mut work.context
                && let Some(menu) = &mut context.menu
                && menu.permit.is_none()
            {
                let Ok(permit) = super::cluster_invsee::reserve_canonical_open_presentation(server)
                else {
                    continue;
                };
                menu.permit = Some(permit);
            }
            for _ in 0..64 {
                let view = work.draft.view(work.fixed.as_ref());
                let capture = work
                    .context
                    .capture(&view, &mut work.snapshot, &mut work.facts);
                match capture {
                    Ok(CallbackCapture::Ready(resolution)) => {
                        let source = work.context.producer();
                        let mut births = Vec::new();
                        let mut birth_count = 0usize;
                        let world = work.context.location().0;
                        let mut unavailable = false;
                        for output in &resolution.breaks {
                            let Some(partitions) =
                                crate::world::loot::partition_loot_facts(&output.items, 4096)
                            else {
                                unavailable = true;
                                break;
                            };
                            let count = partitions.len() + usize::from(output.experience != 0);
                            if count == 0 {
                                continue;
                            }
                            let chunk = pumpkin_cluster::protocol::ChunkKey::new(
                                world,
                                output.position.0.x.div_euclid(16),
                                output.position.0.z.div_euclid(16),
                            );
                            let Ok(primary) = view.scheduled_executor(chunk) else {
                                unavailable = true;
                                break;
                            };
                            birth_count = birth_count.saturating_add(count);
                            births.push((output, chunk, primary, count));
                        }
                        if let CallbackContext::BlockEntity(context) = &work.context {
                            birth_count = birth_count.saturating_add(context.seed.birth_count());
                        }
                        if let CallbackContext::ObjectTerminal(context) = &work.context {
                            birth_count =
                                birth_count.saturating_add(context.extra_birth_count(&resolution));
                        }
                        if unavailable || birth_count > 4096 {
                            break;
                        }
                        let identities = if birth_count == 0 {
                            Arc::from([])
                        } else {
                            let Some(allocator) = allocator.as_deref_mut() else {
                                break;
                            };
                            let Ok(identities) = allocator.reserve(source, reference, birth_count)
                            else {
                                break;
                            };
                            identities
                        };
                        let action = work
                            .context
                            .complete(&view, &mut work.facts, &resolution, &identities)
                            .and_then(|mut action| {
                                let mut offset = 0;
                                for (output, chunk, primary, count) in &births {
                                    let reserved = identities[offset..offset + count]
                                        .iter()
                                        .map(|identity| {
                                            super::cluster_item_birth::ReservedItemBirth {
                                                id: identity.id,
                                                uuid: identity.uuid,
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    super::cluster_item_birth::append_break_births(
                                        &mut action,
                                        *chunk,
                                        output,
                                        source,
                                        *primary,
                                        &reserved,
                                    )
                                    .map_err(|_| ApplyFailure::InvalidAction)?;
                                    offset += count;
                                }
                                if let CallbackContext::ObjectTerminal(context) = &work.context {
                                    context.append_births(
                                        &view,
                                        &mut action,
                                        &resolution,
                                        &identities[offset..],
                                    )?;
                                }
                                for condition in work.guards.iter().cloned() {
                                    if !action.conditions.contains(&condition) {
                                        action.conditions.push(condition);
                                    }
                                }
                                if let CallbackContext::Environment(context) = &work.context {
                                    context.finalize(&mut action)?;
                                }
                                if let CallbackContext::Mining(context) = &work.context {
                                    context.finalize(&view, &mut action, &resolution)?;
                                }
                                action.validate()?;
                                Ok(action)
                            });
                        if let Err(ApplyFailure::Condition(target)) = &action
                            && !view.available(
                                pumpkin_cluster::canonical_action::TruthView::Local,
                                *target,
                            )
                        {
                            if let Ok(expanded) =
                                work.fixed
                                    .expand_history(graphs, reference.tick, &[*target])
                            {
                                work.fixed = expanded;
                                continue;
                            }
                            break;
                        }
                        finished.push((reference, action));
                        break;
                    }
                    Err(
                        super::cluster_world_callbacks::CallbackCaptureError::InvalidWrite(target)
                        | super::cluster_world_callbacks::CallbackCaptureError::MissingLight(target),
                    ) if !view
                        .available(pumpkin_cluster::canonical_action::TruthView::Local, target) =>
                    {
                        if let Ok(expanded) =
                            work.fixed.expand_history(graphs, reference.tick, &[target])
                        {
                            work.fixed = expanded;
                            continue;
                        }
                        break;
                    }
                    Ok(CallbackCapture::Unsupported) | Err(_) => {
                        finished.push((reference, Err(ApplyFailure::InvalidAction)));
                        break;
                    }
                    Ok(CallbackCapture::Policy(requests)) => {
                        let mut incomplete = false;
                        for request in requests {
                            if let Some(decision) = self.policy.take(reference, &request.query) {
                                if !decision.install(&request, &mut work.snapshot) {
                                    finished.push((reference, Err(ApplyFailure::InvalidAction)));
                                    incomplete = true;
                                    break;
                                }
                            } else if let Some(policy) = CallbackPolicyRequest::capture(
                                reference,
                                &request,
                                &work.snapshot,
                                &work.loot,
                            ) {
                                self.policy.stage(server, reference, policy);
                                incomplete = true;
                            } else {
                                finished.push((reference, Err(ApplyFailure::InvalidAction)));
                                incomplete = true;
                                break;
                            }
                        }
                        if incomplete {
                            break;
                        }
                    }
                }
            }
        }
        finished.into_iter().map(|(reference, result)| {
            let work = self.work.remove(&reference).unwrap();
            if result.is_err() {
                let reason = match &result {
                    Err(ApplyFailure::Condition(_)) => 1,
                    Err(ApplyFailure::Type(_)) => 2,
                    Err(ApplyFailure::Inventory(_)) => 3,
                    _ => 0,
                };
                let (world, position) = work.context.location();
                let key = (world, position, reason);
                let now = std::time::Instant::now();
                if self
                    .diagnostics
                    .get(&key)
                    .is_none_or(|last| now.duration_since(*last).as_secs() >= 300)
                    && (self.diagnostics.len() < 1024 || self.diagnostics.contains_key(&key))
                {
                    self.diagnostics.insert(key, now);
                    tracing::warn!(?reference, world=?key.0, position=?key.1, reason=?result.as_ref().err(), "cluster world callback input rejected");
                }
                work.context.reject(server);
            }
            self.policy.discard(reference);
            (reference, result.map(|action| CompletedCallbackWork {
                issued: false,
                action,
                fixed: work.fixed,
                draft: work.draft,
                context: work.context,
                guards: work.guards,
                loot: work.loot,
                environment: work.environment,
            }))
        }).collect()
    }
}
