use pumpkin_data::block_properties::HorizontalFacing;
use pumpkin_data::fluid::Fluid;
use pumpkin_data::tag::Taggable;
use pumpkin_data::{Block, BlockDirection, BlockId, BlockState, BlockStateId};
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockAccessor;
use pumpkin_world::world::BlockFlags;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use super::NeighborUpdateWorld;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockCallbackEffect {
    PlayerBreak {
        position: BlockPos,
        flags: BlockFlags,
        context: PlayerBreakContext,
    },
    FluidTransition(super::fluid::owned::FluidWrite),
    BreakBlock {
        position: BlockPos,
        flags: BlockFlags,
    },
    SetBlock {
        position: BlockPos,
        state: BlockStateId,
        flags: BlockFlags,
    },
    ScheduleBlock {
        position: BlockPos,
        block: BlockId,
        delay: u8,
        priority: TickPriority,
    },
    ScheduleFluid {
        position: BlockPos,
        id: u16,
        delay: u8,
        priority: TickPriority,
    },
    NotifyNeighbor {
        position: BlockPos,
        source: BlockId,
    },
    NotifyNeighbors {
        position: BlockPos,
        except: Option<BlockDirection>,
    },
    NotifyNeighborsAt {
        position: BlockPos,
        source: BlockId,
        except: Option<BlockDirection>,
    },
    Sound {
        position: BlockPos,
        sound: pumpkin_data::sound::Sound,
        volume_milli: u16,
        pitch_milli: u16,
    },
    GameEvent {
        position: BlockPos,
        event: pumpkin_data::game_event::GameEvent,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlayerBreakContext {
    pub state: BlockStateId,
    pub creative: bool,
    pub silk_touch: bool,
    pub water_evaporates: bool,
}

pub struct PlayerBrokenIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub context: PlayerBreakContext,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockCallbackEvent {
    Scheduled,
    StateReplaced {
        old_state: BlockStateId,
        moved: bool,
    },
    Placed {
        state: BlockStateId,
        old_state: BlockStateId,
        moved: bool,
    },
    Prepare {
        state: BlockStateId,
        flags: BlockFlags,
    },
    Neighbor {
        source: BlockId,
        notify: bool,
    },
}

pub struct BlockCallbackIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub event: BlockCallbackEvent,
}

pub fn apply_legacy_callback_effects(
    world: &std::sync::Arc<crate::world::World>,
    effects: Vec<BlockCallbackEffect>,
) -> Option<()> {
    if effects
        .iter()
        .any(|effect| matches!(effect, BlockCallbackEffect::PlayerBreak { .. }))
    {
        return None;
    }
    if effects.iter().any(|effect| matches!(effect, BlockCallbackEffect::ScheduleFluid { id, .. } if Fluid::from_id(*id).is_none())) { return None; }
    for effect in effects {
        match effect {
            BlockCallbackEffect::FluidTransition(write) => {
                world.set_block_state(&write.position, write.after, BlockFlags::NOTIFY_ALL);
            }
            BlockCallbackEffect::Sound {
                position,
                sound,
                volume_milli,
                pitch_milli,
            } => {
                world.play_sound_fine(
                    sound,
                    pumpkin_data::sound::SoundCategory::Blocks,
                    &position.to_centered_f64(),
                    f32::from(volume_milli) / 1000.0,
                    f32::from(pitch_milli) / 1000.0,
                );
            }
            BlockCallbackEffect::GameEvent { position, event } => {
                world.emit_game_event(event.name(), position.to_centered_f64());
            }
            BlockCallbackEffect::PlayerBreak { .. } => return None,
            BlockCallbackEffect::BreakBlock { position, flags } => {
                world.break_block(&position, None, flags);
            }
            BlockCallbackEffect::SetBlock {
                position,
                state,
                flags,
            } => {
                world.set_block_state(&position, state, flags);
            }
            BlockCallbackEffect::ScheduleBlock {
                position,
                block,
                delay,
                priority,
            } => {
                world.schedule_block_tick(Block::from_id(block), position, delay, priority);
            }
            BlockCallbackEffect::ScheduleFluid {
                position,
                id,
                delay,
                priority,
            } => {
                world.schedule_fluid_tick(
                    Fluid::from_id(id).expect("validated callback fluid"),
                    position,
                    delay,
                    priority,
                );
            }
            BlockCallbackEffect::NotifyNeighbor { position, source } => {
                world.update_neighbor(&position, Block::from_id(source));
            }
            BlockCallbackEffect::NotifyNeighbors { position, except } => {
                world.update_neighbors(&position, except);
            }
            BlockCallbackEffect::NotifyNeighborsAt {
                position,
                source,
                except,
            } => {
                world.update_neighbors_at(&position, Block::from_id(source), except);
            }
        }
    }
    Some(())
}

pub fn apply_legacy_scheduled_intent(
    callback: &dyn super::BlockBehaviour,
    args: super::OnScheduledTickArgs<'_>,
) {
    let effects = callback
        .scheduled_tick_intent(super::ScheduledTickIntentArgs {
            world: args.world,
            block: args.block,
            position: args.position,
        })
        .expect("callback has a pure scheduled adapter");
    apply_legacy_callback_effects(args.world, effects)
        .expect("pure scheduled callback effects are valid");
}

pub fn apply_legacy_replaced_intent(
    callback: &dyn super::BlockBehaviour,
    args: super::OnStateReplacedArgs<'_>,
) {
    let effects = callback
        .callback_intent(BlockCallbackIntentArgs {
            world: args.world,
            block: args.block,
            position: args.position,
            event: BlockCallbackEvent::StateReplaced {
                old_state: args.old_state_id,
                moved: args.moved,
            },
        })
        .expect("callback has a pure replacement adapter");
    apply_legacy_callback_effects(args.world, effects)
        .expect("pure replacement callback effects are valid");
}

pub fn apply_legacy_neighbor_intent(
    callback: &dyn super::BlockBehaviour,
    args: super::OnNeighborUpdateArgs<'_>,
) {
    let effects = callback
        .callback_intent(BlockCallbackIntentArgs {
            world: args.world,
            block: args.block,
            position: args.position,
            event: BlockCallbackEvent::Neighbor {
                source: args.source_block.id,
                notify: args.notify,
            },
        })
        .expect("callback has a pure neighbor adapter");
    apply_legacy_callback_effects(args.world, effects)
        .expect("pure neighbor callback effects are valid");
}

pub fn support_tick_intent(
    args: super::ScheduledTickIntentArgs<'_>,
    can_place: impl FnOnce(&dyn BlockAccessor, &BlockPos) -> bool,
) -> Option<Vec<BlockCallbackEffect>> {
    Some(if can_place(args.world, args.position) {
        Vec::new()
    } else {
        vec![BlockCallbackEffect::BreakBlock {
            position: *args.position,
            flags: BlockFlags::empty(),
        }]
    })
}

pub fn support_callback_intent(
    callback: &dyn super::BlockBehaviour,
    args: BlockCallbackIntentArgs<'_>,
) -> Option<Vec<BlockCallbackEffect>> {
    match args.event {
        BlockCallbackEvent::Scheduled => {
            callback.scheduled_tick_intent(super::ScheduledTickIntentArgs {
                world: args.world,
                block: args.block,
                position: args.position,
            })
        }
        BlockCallbackEvent::StateReplaced { .. }
        | BlockCallbackEvent::Placed { .. }
        | BlockCallbackEvent::Prepare { .. }
        | BlockCallbackEvent::Neighbor { .. } => Some(Vec::new()),
    }
}

pub fn resolve_scheduled_intent(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    budget: usize,
) -> Option<(
    Vec<NeighborFact>,
    Vec<NeighborEffect>,
    Vec<BlockCallbackEffect>,
)> {
    resolve_scheduled_intent_captured(registry, snapshot, block, position, budget).0
}

pub fn resolve_scheduled_intent_captured(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    budget: usize,
) -> (
    Option<(
        Vec<NeighborFact>,
        Vec<NeighborEffect>,
        Vec<BlockCallbackEffect>,
    )>,
    Vec<CallbackRequest>,
) {
    let queries = Rc::new(RefCell::new(Vec::new()));
    let result = (|| {
        let view = NeighborTranscript::with_queries(snapshot, budget, queries.clone())?;
        if view.get_block(position) != block {
            let (facts, schedules) = view.finish()?;
            return Some((facts, schedules, Vec::new()));
        }
        let effects = match registry.get_pumpkin_block(block.id) {
            Some(callback) => callback.callback_intent(BlockCallbackIntentArgs {
                world: &view,
                block,
                position,
                event: BlockCallbackEvent::Scheduled,
            })?,
            None => Vec::new(),
        };
        if effects.len() > budget {
            return None;
        }
        let (facts, schedules) = view.finish()?;
        Some((facts, schedules, effects))
    })();
    let queries = Rc::try_unwrap(queries)
        .expect("exclusive scheduled queries")
        .into_inner();
    (result, queries)
}

pub fn resolve_fluid_intent_captured(
    snapshot: NeighborSnapshot,
    fluid: u16,
    position: BlockPos,
    rules: super::fluid::owned::FluidRules,
    budget: usize,
) -> (
    Option<(
        Vec<NeighborFact>,
        Vec<NeighborEffect>,
        Vec<BlockCallbackEffect>,
    )>,
    Vec<CallbackRequest>,
) {
    struct View<'a>(&'a NeighborSnapshot);
    impl super::fluid::owned::FluidPhaseView for View<'_> {
        fn block(&self, position: BlockPos) -> Option<BlockStateId> {
            self.0.blocks.get(&position).copied()
        }
        fn policy(&self, policy: super::fluid::owned::FluidPolicy) -> Option<bool> {
            self.0.fluid_policy.get(&policy).copied()
        }
    }
    match super::fluid::owned::scheduled(&View(&snapshot), fluid, position, rules, budget) {
        Ok(plan) => (
            Some((
                plan.reads
                    .into_iter()
                    .map(|(position, state)| NeighborFact::Block(position, state))
                    .collect(),
                Vec::new(),
                plan.writes
                    .into_iter()
                    .map(BlockCallbackEffect::FluidTransition)
                    .collect(),
            )),
            Vec::new(),
        ),
        Err(super::fluid::owned::FluidUnavailable::Block(position)) => (
            None,
            vec![CallbackRequest {
                query: CallbackQuery::Block(position),
                private_blocks: HashMap::new(),
            }],
        ),
        Err(super::fluid::owned::FluidUnavailable::Policy(policy)) => (
            None,
            vec![CallbackRequest {
                query: CallbackQuery::FluidPolicy(policy),
                private_blocks: HashMap::new(),
            }],
        ),
        Err(_) => (None, Vec::new()),
    }
}

pub fn resolve_random_intent(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    random: &mut pumpkin_util::random::RandomGenerator,
    ambient_darkness: u8,
    budget: usize,
) -> Option<(
    Vec<NeighborFact>,
    Vec<NeighborEffect>,
    Vec<BlockCallbackEffect>,
)> {
    resolve_random_intent_captured(
        registry,
        snapshot,
        block,
        position,
        random,
        ambient_darkness,
        budget,
    )
    .0
}

pub fn resolve_random_intent_captured(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    random: &mut pumpkin_util::random::RandomGenerator,
    ambient_darkness: u8,
    budget: usize,
) -> (
    Option<(
        Vec<NeighborFact>,
        Vec<NeighborEffect>,
        Vec<BlockCallbackEffect>,
    )>,
    Vec<CallbackRequest>,
) {
    let queries = Rc::new(RefCell::new(Vec::new()));
    let result = (|| {
        let view = NeighborTranscript::with_queries(snapshot, budget, queries.clone())?;
        if view.get_block(position) != block {
            let (facts, schedules) = view.finish()?;
            return Some((facts, schedules, Vec::new()));
        }
        let effects = match registry.get_pumpkin_block(block.id) {
            Some(callback) => callback.random_tick_intent(super::RandomTickIntentArgs {
                world: &view,
                block,
                position,
                random,
                ambient_darkness,
            })?,
            None => Vec::new(),
        };
        if effects.len() > budget {
            return None;
        }
        let (facts, schedules) = view.finish()?;
        Some((facts, schedules, effects))
    })();
    let queries = Rc::try_unwrap(queries)
        .expect("exclusive random queries")
        .into_inner();
    (result, queries)
}

pub fn resolve_normal_use_intent(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    can_modify_world: bool,
    sneaking: bool,
    facing: HorizontalFacing,
    budget: usize,
) -> Option<(
    super::registry::BlockActionResult,
    Vec<NeighborFact>,
    Vec<NeighborEffect>,
    Vec<BlockCallbackEffect>,
)> {
    resolve_normal_use_intent_captured(
        registry,
        snapshot,
        block,
        position,
        can_modify_world,
        sneaking,
        facing,
        budget,
    )
    .0
}

pub fn resolve_normal_use_intent_captured(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    block: &Block,
    position: &BlockPos,
    can_modify_world: bool,
    sneaking: bool,
    facing: HorizontalFacing,
    budget: usize,
) -> (
    Option<(
        super::registry::BlockActionResult,
        Vec<NeighborFact>,
        Vec<NeighborEffect>,
        Vec<BlockCallbackEffect>,
    )>,
    Vec<CallbackRequest>,
) {
    let queries = Rc::new(RefCell::new(Vec::new()));
    let result = (|| {
        let view = NeighborTranscript::with_queries(snapshot, budget, queries.clone())?;
        if view.get_block(position) != block {
            return None;
        }
        let (result, effects) = match registry.get_pumpkin_block(block.id) {
            Some(callback) => callback.normal_use_intent(super::NormalUseIntentArgs {
                world: &view,
                block,
                position,
                can_modify_world,
                sneaking,
                facing,
            })?,
            None => (super::registry::BlockActionResult::Pass, Vec::new()),
        };
        if effects.len() > budget {
            return None;
        }
        let (facts, schedules) = view.finish()?;
        Some((result, facts, schedules, effects))
    })();
    let queries = Rc::try_unwrap(queries)
        .expect("exclusive normal-use queries")
        .into_inner();
    (result, queries)
}

pub struct PlacementRequest<'a> {
    pub block: &'a Block,
    pub position: BlockPos,
    pub direction: BlockDirection,
    pub facing: HorizontalFacing,
    pub replacing: super::BlockIsReplacing,
    pub hit: [f32; 3],
}

pub fn resolve_placement_intent_captured(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    request: &PlacementRequest<'_>,
    budget: usize,
) -> (
    Option<(BlockStateId, Vec<NeighborFact>, Vec<NeighborEffect>)>,
    Vec<CallbackRequest>,
) {
    let queries = Rc::new(RefCell::new(Vec::new()));
    let result = (|| {
        if request
            .hit
            .iter()
            .any(|coordinate| !coordinate.is_finite() || !(0.0..=1.0).contains(coordinate))
        {
            return None;
        }
        let view = NeighborTranscript::with_queries(snapshot, budget, queries.clone())?;
        let state = match registry.get_pumpkin_block(request.block.id) {
            Some(callback) => callback.placement_intent(super::PlacementIntentArgs {
                world: &view,
                block: request.block,
                position: &request.position,
                direction: request.direction,
                facing: request.facing,
                replacing: request.replacing,
                hit: request.hit,
            })?,
            None => request.block.default_state.id,
        };
        if state.to_block() != request.block {
            return None;
        }
        let (facts, schedules) = view.finish()?;
        Some((state, facts, schedules))
    })();
    let queries = Rc::try_unwrap(queries)
        .expect("exclusive placement queries")
        .into_inner();
    (result, queries)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NeighborFact {
    Block(BlockPos, BlockStateId),
    BlockEntity(BlockPos, Option<pumpkin_cluster::canonical_action::Cell>),
    Brightness(BlockPos, u8, u8),
    Scheduled(BlockPos, u16, bool),
    ScheduledExcept(
        BlockPos,
        u16,
        bool,
        pumpkin_cluster::canonical_action::ScheduledTickFact,
    ),
    FluidScheduled(BlockPos, u16, bool),
    FluidScheduledExcept(
        BlockPos,
        u16,
        bool,
        pumpkin_cluster::canonical_action::ScheduledTickFact,
    ),
    Power(BlockPos, HorizontalFacing, bool, u8),
    ReceivesPower(BlockPos, bool),
    EmitsPower(u16, BlockStateId, BlockDirection, bool),
    Physics(BlockPos, BlockPos, bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NeighborEffect {
    ScheduleBlock {
        position: BlockPos,
        id: u16,
        delay: u8,
        priority: TickPriority,
    },
    ScheduleFluid {
        position: BlockPos,
        id: u16,
        delay: u8,
        priority: TickPriority,
    },
}

#[derive(Clone, Default)]
pub struct NeighborSnapshot {
    pub blocks: HashMap<BlockPos, BlockStateId>,
    pub processing_tick: Option<pumpkin_cluster::canonical_action::ScheduledTickFact>,
    pub lava_tick_delay: Option<u8>,
    pub block_entities: HashMap<BlockPos, Option<pumpkin_cluster::canonical_action::Cell>>,
    pub brightness: HashMap<(BlockPos, u8), u8>,
    pub scheduled: HashMap<(BlockPos, u16), bool>,
    pub fluid_scheduled: HashMap<(BlockPos, u16), bool>,
    pub power: HashMap<(BlockPos, u8, bool), CapturedNumericPower>,
    pub directional_power: HashMap<(BlockPos, u8), CapturedNumericPower>,
    pub receives_power: HashMap<BlockPos, CapturedPower>,
    pub emits_power: HashMap<(u16, BlockStateId, u8), bool>,
    pub physics_allowed: HashMap<(BlockPos, BlockPos), bool>,
    pub natural_breaks: HashMap<BlockPos, CapturedNaturalBreak>,
    pub fluid_policy: HashMap<super::fluid::owned::FluidPolicy, bool>,
}

#[derive(Clone)]
pub struct CapturedNaturalBreak {
    pub before: BlockStateId,
    pub cancelled: bool,
    pub loot: Option<crate::world::loot::BlockLootFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NaturalBreakResult {
    pub position: BlockPos,
    pub before: BlockStateId,
    pub items: Vec<crate::world::loot::LootItemFact>,
    pub experience: u32,
    pub particles: bool,
}

#[derive(Clone)]
pub struct CapturedPower {
    pub value: bool,
    pub reads: Vec<(BlockPos, BlockStateId)>,
    pub entity_reads: Vec<(BlockPos, Option<pumpkin_cluster::canonical_action::Cell>)>,
}

#[derive(Clone)]
pub struct CapturedNumericPower {
    pub value: u8,
    pub reads: Vec<(BlockPos, BlockStateId)>,
    pub entity_reads: Vec<(BlockPos, Option<pumpkin_cluster::canonical_action::Cell>)>,
}

pub struct NeighborResolution {
    pub writes: Vec<(BlockPos, BlockStateId, BlockStateId)>,
    pub facts: Vec<NeighborFact>,
    pub effects: Vec<NeighborEffect>,
}

pub fn resolve_neighbor_states(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    initial_writes: &[(BlockPos, BlockStateId)],
    budget: usize,
) -> Option<NeighborResolution> {
    use std::collections::VecDeque;
    if initial_writes.len() > budget {
        return None;
    }
    let original = snapshot.blocks.clone();
    let mut view = NeighborTranscript::new(snapshot, budget)?;
    let mut queue = VecDeque::new();
    for &(position, state) in initial_writes {
        if !original.contains_key(&position) {
            return None;
        }
        view.snapshot.blocks.insert(position, state);
        view.private_blocks.insert(position, state);
        queue.push_back(position);
    }
    let mut visits = 0usize;
    while let Some(source) = queue.pop_front() {
        for direction in BlockDirection::abstract_block_update_order() {
            visits = visits.checked_add(1)?;
            if visits > budget {
                return None;
            }
            let offset = direction.to_offset();
            let position = BlockPos::new(
                source.0.x.checked_add(offset.x)?,
                source.0.y.checked_add(offset.y)?,
                source.0.z.checked_add(offset.z)?,
            );
            let state = view.get_block_state_id(&position);
            let source_state = view.get_block_state_id(&source);
            let next = registry.get_state_for_neighbor_update(
                &view,
                Block::from_state_id(state),
                state,
                &position,
                direction.opposite(),
                &source,
                source_state,
            );
            if view.invalid.get() {
                return None;
            }
            if next != state {
                view.snapshot.blocks.insert(position, next);
                view.private_blocks.insert(position, next);
                queue.push_back(position);
            }
        }
    }
    let mut writes: Vec<_> = view
        .snapshot
        .blocks
        .iter()
        .filter_map(|(position, &after)| {
            let before = *original.get(position)?;
            (before != after).then_some((*position, before, after))
        })
        .collect();
    writes.sort_by_key(|(position, _, _)| (position.0.x, position.0.y, position.0.z));
    let (facts, effects) = view.finish()?;
    Some(NeighborResolution {
        writes,
        facts,
        effects,
    })
}

pub struct NeighborTranscript {
    snapshot: NeighborSnapshot,
    scheduled: HashMap<(BlockPos, u16), Cell<bool>>,
    fluid_scheduled: HashMap<(BlockPos, u16), Cell<bool>>,
    facts: RefCell<Vec<NeighborFact>>,
    effects: RefCell<Vec<NeighborEffect>>,
    capacity: usize,
    invalid: Cell<bool>,
    queries: Rc<RefCell<Vec<CallbackRequest>>>,
    private_blocks: HashMap<BlockPos, BlockStateId>,
    request_weight: Cell<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CallbackQuery {
    Block(BlockPos),
    BlockEntity(BlockPos),
    Brightness(BlockPos, u8),
    BlockTick(BlockPos, u16),
    FluidTick(BlockPos, u16),
    Power(BlockPos, u8, bool),
    DirectionalPower(BlockPos, u8),
    ReceivesPower(BlockPos),
    EmitsPower(u16, BlockStateId, u8),
    Physics(BlockPos, BlockPos),
    NaturalBreak(BlockPos, bool),
    FluidPolicy(super::fluid::owned::FluidPolicy),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallbackRequest {
    pub query: CallbackQuery,
    pub private_blocks: HashMap<BlockPos, BlockStateId>,
}

impl NeighborTranscript {
    pub fn new(snapshot: NeighborSnapshot, capacity: usize) -> Option<Self> {
        Self::with_queries(snapshot, capacity, Rc::new(RefCell::new(Vec::new())))
    }

    fn with_queries(
        snapshot: NeighborSnapshot,
        capacity: usize,
        queries: Rc<RefCell<Vec<CallbackRequest>>>,
    ) -> Option<Self> {
        if capacity == 0 || capacity > 65_536 {
            return None;
        }
        Some(Self {
            scheduled: snapshot
                .scheduled
                .iter()
                .map(|(&key, &value)| (key, Cell::new(value)))
                .collect(),
            fluid_scheduled: snapshot
                .fluid_scheduled
                .iter()
                .map(|(&key, &value)| (key, Cell::new(value)))
                .collect(),
            snapshot,
            facts: RefCell::new(Vec::with_capacity(capacity)),
            effects: RefCell::new(Vec::with_capacity(capacity)),
            capacity,
            invalid: Cell::new(false),
            queries,
            private_blocks: HashMap::new(),
            request_weight: Cell::new(0),
        })
    }

    fn block_scheduled_fact(&self, position: BlockPos, id: u16, present: bool) {
        if let Some(fact) = self.snapshot.processing_tick.filter(|fact| {
            fact.kind == pumpkin_cluster::canonical_action::ScheduledTickKind::Block
                && fact.id == id
                && fact.position.x == position.0.x
                && fact.position.y == position.0.y
                && fact.position.z == position.0.z
        }) {
            self.fact(NeighborFact::ScheduledExcept(position, id, present, fact));
        } else {
            self.fact(NeighborFact::Scheduled(position, id, present));
        }
    }

    fn fluid_scheduled_fact(&self, position: BlockPos, id: u16, present: bool) {
        if let Some(fact) = self.snapshot.processing_tick.filter(|fact| {
            fact.kind == pumpkin_cluster::canonical_action::ScheduledTickKind::Fluid
                && fact.id == id
                && fact.position.x == position.0.x
                && fact.position.y == position.0.y
                && fact.position.z == position.0.z
        }) {
            self.fact(NeighborFact::FluidScheduledExcept(
                position, id, present, fact,
            ));
        } else {
            self.fact(NeighborFact::FluidScheduled(position, id, present));
        }
    }

    fn fact(&self, fact: NeighborFact) {
        let mut facts = self.facts.borrow_mut();
        if facts.len() == self.capacity {
            self.invalid.set(true);
        } else {
            facts.push(fact);
        }
    }

    fn missing(&self) {
        self.invalid.set(true);
    }

    fn request(&self, query: CallbackQuery) {
        self.missing();
        let mut queries = self.queries.borrow_mut();
        if queries
            .iter()
            .any(|request| request.query == query && request.private_blocks == self.private_blocks)
        {
            return;
        }
        let Some(weight) = self
            .request_weight
            .get()
            .checked_add(1)
            .and_then(|weight| weight.checked_add(self.private_blocks.len()))
        else {
            return;
        };
        if weight > self.capacity {
            return;
        }
        self.request_weight.set(weight);
        queries.push(CallbackRequest {
            query,
            private_blocks: self.private_blocks.clone(),
        });
    }

    fn effect(&self, effect: NeighborEffect) {
        let mut effects = self.effects.borrow_mut();
        if effects.len() == self.capacity {
            self.missing();
        } else {
            effects.push(effect);
        }
    }

    pub fn finish(self) -> Option<(Vec<NeighborFact>, Vec<NeighborEffect>)> {
        if self.invalid.get() {
            return None;
        }
        let facts = self.facts.into_inner();
        let effects = self.effects.into_inner();
        Some((facts, effects))
    }
}

impl BlockAccessor for NeighborTranscript {
    fn get_block_state_id(&self, position: &BlockPos) -> BlockStateId {
        let Some(&id) = self.snapshot.blocks.get(position) else {
            self.request(CallbackQuery::Block(*position));
            return Block::AIR.default_state.id;
        };
        self.fact(NeighborFact::Block(*position, id));
        id
    }
    fn get_block(&self, position: &BlockPos) -> &'static Block {
        Block::from_state_id(self.get_block_state_id(position))
    }
    fn get_block_state(&self, position: &BlockPos) -> &'static BlockState {
        BlockState::from_id(self.get_block_state_id(position))
    }
    fn get_block_and_state(&self, position: &BlockPos) -> (&'static Block, &'static BlockState) {
        BlockState::from_id_with_block(self.get_block_state_id(position))
    }
}

impl NeighborUpdateWorld for NeighborTranscript {
    fn get_directional_power(&self, position: &BlockPos, direction: BlockDirection) -> u8 {
        let Some(evidence) = self
            .snapshot
            .directional_power
            .get(&(*position, direction as u8))
        else {
            self.request(CallbackQuery::DirectionalPower(*position, direction as u8));
            return 0;
        };
        if evidence.value > 15 || evidence.reads.is_empty() || evidence.reads.len() > self.capacity
        {
            self.missing();
            return 0;
        }
        for &(read, expected) in &evidence.reads {
            if self.get_block_state_id(&read) != expected {
                self.request(CallbackQuery::DirectionalPower(*position, direction as u8));
                return 0;
            }
        }
        for (position, cell) in &evidence.entity_reads {
            self.fact(NeighborFact::BlockEntity(*position, cell.clone()));
        }
        evidence.value
    }

    fn receives_redstone_power(&self, position: &BlockPos) -> bool {
        let Some(evidence) = self.snapshot.receives_power.get(position) else {
            self.request(CallbackQuery::ReceivesPower(*position));
            return false;
        };
        if evidence.reads.is_empty() || evidence.reads.len() > self.capacity {
            self.missing();
            return false;
        }
        for &(read, expected) in &evidence.reads {
            if self.get_block_state_id(&read) != expected {
                self.request(CallbackQuery::ReceivesPower(*position));
                return false;
            }
        }
        let value = evidence.value;
        self.fact(NeighborFact::ReceivesPower(*position, value));
        for (position, cell) in &evidence.entity_reads {
            self.fact(NeighborFact::BlockEntity(*position, cell.clone()));
        }
        value
    }
    fn get_raw_brightness(&self, position: &BlockPos, ambient_darkness: u8) -> u8 {
        let Some(&value) = self.snapshot.brightness.get(&(*position, ambient_darkness)) else {
            self.request(CallbackQuery::Brightness(*position, ambient_darkness));
            return 0;
        };
        self.fact(NeighborFact::Brightness(*position, ambient_darkness, value));
        value
    }
    fn emits_redstone_power(
        &self,
        block: &Block,
        state: &BlockState,
        direction: BlockDirection,
    ) -> bool {
        let Some(&value) =
            self.snapshot
                .emits_power
                .get(&(u16::from(block.id), state.id, direction as u8))
        else {
            self.request(CallbackQuery::EmitsPower(
                u16::from(block.id),
                state.id,
                direction as u8,
            ));
            return false;
        };
        self.fact(NeighborFact::EmitsPower(
            u16::from(block.id),
            state.id,
            direction,
            value,
        ));
        value
    }
    fn get_power_on_side(
        &self,
        position: &BlockPos,
        side: HorizontalFacing,
        only_gate: bool,
    ) -> u8 {
        let Some(evidence) = self.snapshot.power.get(&(*position, side as u8, only_gate)) else {
            self.request(CallbackQuery::Power(*position, side as u8, only_gate));
            return 0;
        };
        if evidence.value > 15 || evidence.reads.is_empty() || evidence.reads.len() > self.capacity
        {
            self.missing();
            return 0;
        }
        for &(read, expected) in &evidence.reads {
            if self.get_block_state_id(&read) != expected {
                self.request(CallbackQuery::Power(*position, side as u8, only_gate));
                return 0;
            }
        }
        let value = evidence.value;
        self.fact(NeighborFact::Power(*position, side, only_gate, value));
        for (position, cell) in &evidence.entity_reads {
            self.fact(NeighborFact::BlockEntity(*position, cell.clone()));
        }
        value
    }
    fn schedule_block_tick(
        &self,
        block: &Block,
        position: BlockPos,
        delay: u8,
        priority: TickPriority,
    ) {
        let key = (position, u16::from(block.id));
        let Some(scheduled) = self.scheduled.get(&key) else {
            self.request(CallbackQuery::BlockTick(position, key.1));
            return;
        };
        let previous = scheduled.replace(true);
        self.block_scheduled_fact(position, key.1, previous);
        if previous {
            return;
        }
        self.effect(NeighborEffect::ScheduleBlock {
            position,
            id: u16::from(block.id),
            delay,
            priority,
        });
    }
    fn schedule_fluid_tick(
        &self,
        fluid: &Fluid,
        position: BlockPos,
        delay: u8,
        priority: TickPriority,
    ) {
        let Some(scheduled) = self.fluid_scheduled.get(&(position, fluid.id)) else {
            self.request(CallbackQuery::FluidTick(position, fluid.id));
            return;
        };
        let previous = scheduled.replace(true);
        self.fluid_scheduled_fact(position, fluid.id, previous);
        if previous {
            return;
        }
        self.effect(NeighborEffect::ScheduleFluid {
            position,
            id: fluid.id,
            delay,
            priority,
        });
    }
    fn is_block_tick_scheduled(&self, position: &BlockPos, block: &Block) -> bool {
        let Some(scheduled) = self.scheduled.get(&(*position, u16::from(block.id))) else {
            self.request(CallbackQuery::BlockTick(*position, u16::from(block.id)));
            return false;
        };
        let value = scheduled.get();
        self.block_scheduled_fact(*position, u16::from(block.id), value);
        value
    }
}

#[derive(Default)]
pub struct BlockCallbackResolution {
    pub writes: Vec<(BlockPos, BlockStateId, BlockStateId)>,
    pub block_entity_removals: Vec<(BlockPos, pumpkin_cluster::canonical_action::Cell)>,
    pub facts: Vec<NeighborFact>,
    pub schedules: Vec<NeighborEffect>,
    pub listeners: Vec<BlockPos>,
    pub poi: Vec<(BlockPos, BlockId)>,
    pub light: Vec<BlockPos>,
    pub breaks: Vec<NaturalBreakResult>,
    pub presentation: Vec<BlockCallbackEffect>,
}

pub fn resolve_callback_effects(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    effects: Vec<BlockCallbackEffect>,
    budget: usize,
) -> Option<BlockCallbackResolution> {
    resolve_callback_effects_captured(registry, snapshot, effects, budget).0
}

pub fn resolve_callback_effects_captured(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    effects: Vec<BlockCallbackEffect>,
    budget: usize,
) -> (Option<BlockCallbackResolution>, Vec<CallbackRequest>) {
    let queries = Rc::new(RefCell::new(Vec::new()));
    let result =
        resolve_callback_effects_inner(registry, snapshot, effects, budget, queries.clone());
    let queries = Rc::try_unwrap(queries)
        .expect("exclusive callback queries")
        .into_inner();
    (result, queries)
}

fn resolve_callback_effects_inner(
    registry: &super::registry::BlockRegistry,
    snapshot: NeighborSnapshot,
    effects: Vec<BlockCallbackEffect>,
    budget: usize,
    queries: Rc<RefCell<Vec<CallbackRequest>>>,
) -> Option<BlockCallbackResolution> {
    use std::collections::{HashSet, VecDeque};
    enum Work {
        PlayerBroken(BlockPos, PlayerBreakContext),
        Effect(BlockCallbackEffect),
        Hook(BlockPos, BlockId, BlockCallbackEvent),
        NeighborState(BlockPos, BlockPos, BlockDirection, BlockFlags),
        PostSet(
            BlockPos,
            BlockId,
            BlockId,
            BlockStateId,
            BlockStateId,
            BlockFlags,
        ),
        Physics(BlockPos, BlockPos, BlockId),
    }
    fn offset(position: BlockPos, direction: BlockDirection) -> Option<BlockPos> {
        let delta = direction.to_offset();
        Some(BlockPos::new(
            position.0.x.checked_add(delta.x)?,
            position.0.y.checked_add(delta.y)?,
            position.0.z.checked_add(delta.z)?,
        ))
    }
    if effects.len() > budget {
        return None;
    }
    let original = snapshot.blocks.clone();
    let mut view = NeighborTranscript::with_queries(snapshot, budget, queries)?;
    let mut work: VecDeque<_> = effects.into_iter().map(Work::Effect).collect();
    let mut listeners = HashSet::new();
    let mut player_breaks = std::collections::HashSet::new();
    let mut block_entity_removals = HashMap::new();
    let mut breaks = Vec::new();
    let mut break_outputs = 0usize;
    let mut visits = 0usize;
    let mut presentation = Vec::new();
    while let Some(current) = work.pop_front() {
        visits = visits.checked_add(1)?;
        if visits > budget {
            return None;
        }
        let mut next = Vec::new();
        match current {
            Work::Effect(
                effect
                @ (BlockCallbackEffect::Sound { .. } | BlockCallbackEffect::GameEvent { .. }),
            ) => {
                presentation.push(effect);
            }
            Work::Effect(BlockCallbackEffect::PlayerBreak {
                position,
                flags,
                context,
            }) => {
                if view.get_block_state_id(&position) != context.state {
                    return None;
                }
                player_breaks.insert(position);
                next.push(Work::Effect(BlockCallbackEffect::BreakBlock {
                    position,
                    flags,
                }));
                next.push(Work::PlayerBroken(position, context));
            }
            Work::PlayerBroken(position, context) => {
                player_breaks.remove(&position);
                if view.get_block_state_id(&position) != context.state {
                    if let Some(callback) = registry.get_pumpkin_block(context.state.to_block_id())
                    {
                        next.extend(
                            callback
                                .player_broken_intent(PlayerBrokenIntentArgs {
                                    world: &view,
                                    block: context.state.to_block(),
                                    position: &position,
                                    context,
                                })?
                                .into_iter()
                                .map(Work::Effect),
                        );
                    }
                }
            }
            Work::Effect(BlockCallbackEffect::BreakBlock {
                position,
                mut flags,
            }) => {
                let before = view.get_block_state_id(&position);
                if before.to_state().is_air() {
                    continue;
                }
                let Some(decision) = view.snapshot.natural_breaks.get(&position) else {
                    view.request(CallbackQuery::NaturalBreak(
                        position,
                        !flags.contains(BlockFlags::SKIP_DROPS),
                    ));
                    return None;
                };
                if decision.before != before {
                    return None;
                }
                if decision.cancelled {
                    continue;
                }
                let waterlogged = before.to_block().is_waterlogged(before);
                if (before.to_state().block_entity_type != u16::MAX
                    && !before
                        .to_block()
                        .has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS))
                    || (!waterlogged
                        && crate::world::World::get_fluid_from_state_id(before) != &Fluid::EMPTY)
                {
                    return None;
                }
                let (items, experience) = match &decision.loot {
                    _ if flags.contains(BlockFlags::SKIP_DROPS) => (Vec::new(), 0),
                    Some(loot) => {
                        flags.remove(BlockFlags::SKIP_DROPS);
                        (
                            crate::world::loot::partition_loot_facts(
                                &loot.items,
                                budget.saturating_sub(visits),
                            )?,
                            loot.experience,
                        )
                    }
                    None => {
                        flags.insert(BlockFlags::SKIP_DROPS);
                        (Vec::new(), 0)
                    }
                };
                break_outputs = break_outputs.checked_add(items.len())?.checked_add(1)?;
                if break_outputs > budget {
                    return None;
                }
                breaks.push(NaturalBreakResult {
                    position,
                    before,
                    items,
                    experience,
                    particles: before.to_block() != &Block::FIRE
                        && before.to_block() != &Block::SOUL_FIRE,
                });
                next.push(Work::Effect(if waterlogged {
                    BlockCallbackEffect::FluidTransition(super::fluid::owned::FluidWrite {
                        position,
                        before,
                        after: Block::WATER.default_state.id,
                        replace_with_drops: false,
                        delay: Some(5),
                        fizz: false,
                    })
                } else {
                    BlockCallbackEffect::SetBlock {
                        position,
                        state: BlockStateId::AIR,
                        flags,
                    }
                }));
            }
            Work::Effect(
                effect @ (BlockCallbackEffect::SetBlock { .. }
                | BlockCallbackEffect::FluidTransition(_)),
            ) => {
                let (position, state, flags, fluid) = match effect {
                    BlockCallbackEffect::SetBlock {
                        position,
                        state,
                        flags,
                    } => (position, state, flags, false),
                    BlockCallbackEffect::FluidTransition(write) => {
                        let before = view.get_block_state_id(&write.position);
                        if before != write.before {
                            return None;
                        }
                        if write.replace_with_drops {
                            next.push(Work::Effect(BlockCallbackEffect::BreakBlock {
                                position: write.position,
                                flags: BlockFlags::NOTIFY_ALL,
                            }));
                            next.push(Work::Effect(BlockCallbackEffect::FluidTransition(
                                super::fluid::owned::FluidWrite {
                                    before: BlockStateId::AIR,
                                    replace_with_drops: false,
                                    ..write
                                },
                            )));
                            for step in next.into_iter().rev() {
                                work.push_front(step);
                            }
                            continue;
                        }
                        if let Some(delay) = write.delay {
                            let fluid = Fluid::from_state_id(write.after)?.to_flowing();
                            view.schedule_fluid_tick(
                                fluid,
                                write.position,
                                delay,
                                TickPriority::Normal,
                            );
                        }
                        if write.fizz {
                            presentation.push(BlockCallbackEffect::Sound {
                                position: write.position,
                                sound: pumpkin_data::sound::Sound::BlockLavaExtinguish,
                                volume_milli: 500,
                                pitch_milli: 2600,
                            });
                        }
                        (write.position, write.after, BlockFlags::NOTIFY_ALL, true)
                    }
                    _ => unreachable!(),
                };
                let before = view.get_block_state_id(&position);
                if before == state && !flags.contains(BlockFlags::FORCE_STATE) {
                    continue;
                }
                let old = before.to_block();
                let new = state.to_block();
                let bed_removal = old != new
                    && old.has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
                    && state.to_state().is_air();
                if bed_removal {
                    if pumpkin_data::block_properties::WhiteBedLikeProperties::from_state_id(before)
                        .occupied
                    {
                        return None;
                    }
                    let Some(entity) = view.snapshot.block_entities.get(&position).cloned() else {
                        view.request(CallbackQuery::BlockEntity(position));
                        return None;
                    };
                    view.fact(NeighborFact::BlockEntity(position, entity.clone()));
                    if let Some(entity) = entity {
                        block_entity_removals.insert(position, entity);
                    }
                }
                if (old != new
                    && !bed_removal
                    && (old.default_state.block_entity_type != u16::MAX
                        || new.default_state.block_entity_type != u16::MAX))
                    || (!fluid
                        && crate::world::World::get_fluid_from_state_id(before)
                            != crate::world::World::get_fluid_from_state_id(state))
                    || (state.has_analog_output_signal()
                        && new != &Block::RESPAWN_ANCHOR
                        && new != &Block::END_PORTAL_FRAME
                        && !matches!(
                            new.id,
                            BlockId::COPPER_BULB
                                | BlockId::EXPOSED_COPPER_BULB
                                | BlockId::WEATHERED_COPPER_BULB
                                | BlockId::OXIDIZED_COPPER_BULB
                                | BlockId::WAXED_COPPER_BULB
                                | BlockId::WAXED_EXPOSED_COPPER_BULB
                                | BlockId::WAXED_WEATHERED_COPPER_BULB
                                | BlockId::WAXED_OXIDIZED_COPPER_BULB
                        ))
                {
                    return None;
                }
                view.snapshot.blocks.insert(position, state);
                view.private_blocks.insert(position, state);
                let moved = flags.contains(BlockFlags::MOVED);
                if old != new
                    && (flags.contains(BlockFlags::NOTIFY_NEIGHBORS) || moved)
                    && !(player_breaks.contains(&position)
                        && (old.has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
                            || old.has_tag(&pumpkin_data::tag::Block::MINECRAFT_DOORS)))
                {
                    next.push(Work::Hook(
                        position,
                        old.id,
                        BlockCallbackEvent::StateReplaced {
                            old_state: before,
                            moved,
                        },
                    ));
                }
                if old != new && !flags.contains(BlockFlags::SKIP_BLOCK_ADDED_CALLBACK) {
                    next.push(Work::Hook(
                        position,
                        new.id,
                        BlockCallbackEvent::Placed {
                            state,
                            old_state: before,
                            moved,
                        },
                    ));
                }
                next.push(Work::PostSet(
                    position, old.id, new.id, before, state, flags,
                ));
            }
            Work::PostSet(position, old, new, before, state, flags) => {
                if view.get_block_state_id(&position) != state {
                    continue;
                }
                if flags.contains(BlockFlags::NOTIFY_LISTENERS) {
                    listeners.insert(position);
                }
                if state.has_analog_output_signal() {
                    for direction in BlockDirection::horizontal() {
                        let direction = BlockDirection::from_cardinal_direction(direction);
                        let first = offset(position, direction)?;
                        let first_state = view.get_block_state_id(&first);
                        if first_state.to_block() == &Block::COMPARATOR {
                            next.push(Work::Physics(first, position, new));
                        } else if first_state.to_state().is_solid_block() {
                            let second = offset(first, direction)?;
                            if view.get_block(&second) == &Block::COMPARATOR {
                                next.push(Work::Physics(second, position, new));
                            }
                        }
                    }
                }
                if flags.contains(BlockFlags::NOTIFY_NEIGHBORS) {
                    for direction in BlockDirection::update_order() {
                        next.push(Work::Physics(offset(position, direction)?, position, old));
                    }
                }
                let moved = flags.contains(BlockFlags::MOVED);
                if !moved {
                    let mut prepare_flags = flags;
                    prepare_flags.remove(
                        BlockFlags::NOTIFY_NEIGHBORS
                            | BlockFlags::SKIP_REDSTONE_WIRE_STATE_REPLACEMENT,
                    );
                    next.push(Work::Hook(
                        position,
                        old,
                        BlockCallbackEvent::Prepare {
                            state: before,
                            flags: prepare_flags,
                        },
                    ));
                    for direction in BlockDirection::abstract_block_update_order() {
                        next.push(Work::NeighborState(
                            offset(position, direction)?,
                            position,
                            direction.opposite(),
                            prepare_flags,
                        ));
                    }
                    next.push(Work::Hook(
                        position,
                        new,
                        BlockCallbackEvent::Prepare {
                            state,
                            flags: prepare_flags,
                        },
                    ));
                }
            }

            Work::Effect(BlockCallbackEffect::ScheduleBlock {
                position,
                block,
                delay,
                priority,
            }) => {
                view.schedule_block_tick(Block::from_id(block), position, delay, priority);
            }
            Work::Effect(BlockCallbackEffect::ScheduleFluid {
                position,
                id,
                delay,
                priority,
            }) => {
                view.schedule_fluid_tick(Fluid::from_id(id)?, position, delay, priority);
            }
            Work::Effect(BlockCallbackEffect::NotifyNeighbor { position, source }) => {
                next.push(Work::Physics(position, position, source));
            }
            Work::Effect(BlockCallbackEffect::NotifyNeighbors { position, except }) => {
                let source = view.get_block(&position).id;
                for direction in BlockDirection::update_order() {
                    if except == Some(direction) {
                        continue;
                    }
                    next.push(Work::Physics(
                        offset(position, direction)?,
                        position,
                        source,
                    ));
                }
            }
            Work::Effect(BlockCallbackEffect::NotifyNeighborsAt {
                position,
                source,
                except,
            }) => {
                for direction in BlockDirection::update_order() {
                    if except == Some(direction) {
                        continue;
                    }
                    next.push(Work::Physics(
                        offset(position, direction)?,
                        position,
                        source,
                    ));
                }
            }
            Work::Physics(position, source_position, source) => {
                let Some(&allowed) = view
                    .snapshot
                    .physics_allowed
                    .get(&(position, source_position))
                else {
                    view.request(CallbackQuery::Physics(position, source_position));
                    return None;
                };
                view.fact(NeighborFact::Physics(position, source_position, allowed));
                if !allowed {
                    continue;
                }
                let state = view.get_block_state_id(&position);
                let fluid = crate::world::World::get_fluid_from_state_id(state);
                if fluid != &Fluid::EMPTY {
                    let delay = if fluid.matches_type(&Fluid::WATER) {
                        5
                    } else if fluid.matches_type(&Fluid::LAVA) {
                        match view.snapshot.lava_tick_delay {
                            Some(delay @ (10 | 30)) => delay,
                            _ => return None,
                        }
                    } else {
                        return None;
                    };
                    view.schedule_fluid_tick(
                        fluid.to_flowing(),
                        position,
                        delay,
                        TickPriority::Normal,
                    );
                }
                next.push(Work::Hook(
                    position,
                    state.to_block_id(),
                    BlockCallbackEvent::Neighbor {
                        source,
                        notify: false,
                    },
                ));
            }
            Work::Hook(position, block, event) => {
                if let Some(callback) = registry.get_pumpkin_block(block) {
                    let effects = callback.callback_intent(BlockCallbackIntentArgs {
                        world: &view,
                        block: Block::from_id(block),
                        position: &position,
                        event,
                    })?;
                    if effects.len() > budget.saturating_sub(visits) {
                        return None;
                    }
                    next.extend(effects.into_iter().map(Work::Effect));
                }
            }
            Work::NeighborState(position, source, direction, flags) => {
                let before = view.get_block_state_id(&position);
                let source_state = view.get_block_state_id(&source);
                let after = registry.get_state_for_neighbor_update(
                    &view,
                    before.to_block(),
                    before,
                    &position,
                    direction,
                    &source,
                    source_state,
                );
                if before != after {
                    if after.to_state().is_air() {
                        next.push(Work::Effect(BlockCallbackEffect::BreakBlock {
                            position,
                            flags,
                        }));
                    } else {
                        next.push(Work::Effect(BlockCallbackEffect::SetBlock {
                            position,
                            state: after,
                            flags,
                        }));
                    }
                }
            }
        }
        if view.invalid.get() {
            return None;
        }
        if work.len().checked_add(next.len())? > budget {
            return None;
        }
        for step in next.into_iter().rev() {
            work.push_front(step);
        }
    }
    let mut writes: Vec<_> = view
        .snapshot
        .blocks
        .iter()
        .filter_map(|(position, &after)| {
            let before = *original.get(position)?;
            (before != after).then_some((*position, before, after))
        })
        .collect();
    writes.sort_by_key(|(position, _, _)| (position.0.x, position.0.y, position.0.z));
    let poi = writes
        .iter()
        .map(|(position, _, after)| (*position, after.to_block_id()))
        .collect();
    let light = writes
        .iter()
        .filter(|(_, before, after)| {
            pumpkin_world::lighting::LightEngine::has_different_light_properties(
                before.to_state(),
                after.to_state(),
            )
        })
        .map(|(position, _, _)| *position)
        .collect();
    let mut listeners: Vec<_> = listeners.into_iter().collect();
    listeners.sort_by_key(|position| (position.0.x, position.0.y, position.0.z));
    let (facts, schedules) = view.finish()?;
    Some(BlockCallbackResolution {
        writes,
        block_entity_removals: {
            let mut values: Vec<_> = block_entity_removals.into_iter().collect();
            values.sort_by_key(|(position, _)| (position.0.x, position.0.y, position.0.z));
            values
        },
        facts,
        schedules,
        listeners,
        poi,
        light,
        breaks,
        presentation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::blocks::redstone::observer::ObserverBlock;
    use crate::block::{BlockBehaviour, GetStateForNeighborUpdateArgs};
    use pumpkin_data::FacingExt;
    use pumpkin_data::block_properties::ObserverLikeProperties;

    struct CertifiedNoBreakHook;

    #[pumpkin_macros::canonical_callback_defaults]
    impl BlockBehaviour for CertifiedNoBreakHook {}

    struct LegacyBreakHook;

    #[pumpkin_macros::canonical_callback_defaults]
    impl BlockBehaviour for LegacyBreakHook {
        fn broken(&self, _args: crate::block::BrokenArgs<'_>) {}
    }

    #[test]
    fn pure_break_defaults_require_absence_of_a_legacy_override() {
        let position = BlockPos::new(0, 80, 0);
        let view = NeighborTranscript::new(NeighborSnapshot::default(), 64).unwrap();
        let args = || PlayerBrokenIntentArgs {
            world: &view,
            block: &Block::STONE,
            position: &position,
            context: PlayerBreakContext {
                state: Block::STONE.default_state.id,
                creative: false,
                silk_touch: false,
                water_evaporates: false,
            },
        };
        assert_eq!(
            CertifiedNoBreakHook.player_broken_intent(args()),
            Some(Vec::new())
        );
        assert_eq!(LegacyBreakHook.player_broken_intent(args()), None);
    }

    #[test]
    fn ice_break_uses_captured_floor_and_typed_water_transition() {
        let position = BlockPos::new(0, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position.down(), Block::STONE.default_state.id);
        let view = NeighborTranscript::new(snapshot, 64).unwrap();
        let context = PlayerBreakContext {
            state: Block::ICE.default_state.id,
            creative: false,
            silk_touch: false,
            water_evaporates: false,
        };
        let resolve = |context| {
            crate::block::blocks::ice::IceBlock.player_broken_intent(PlayerBrokenIntentArgs {
                world: &view,
                block: &Block::ICE,
                position: &position,
                context,
            })
        };
        assert_eq!(
            resolve(context),
            Some(vec![BlockCallbackEffect::FluidTransition(
                super::super::fluid::owned::FluidWrite {
                    position,
                    before: BlockStateId::AIR,
                    after: Block::WATER.default_state.id,
                    replace_with_drops: false,
                    delay: Some(5),
                    fizz: false
                }
            )])
        );
        assert_eq!(
            resolve(PlayerBreakContext {
                silk_touch: true,
                ..context
            }),
            Some(Vec::new())
        );
        assert_eq!(
            resolve(PlayerBreakContext {
                water_evaporates: true,
                ..context
            }),
            Some(Vec::new())
        );
        assert!(view.finish().unwrap().0.contains(&NeighborFact::Block(
            position.down(),
            Block::STONE.default_state.id
        )));
    }

    #[test]
    fn repeated_fluid_neighbor_updates_schedule_one_tick_from_captured_presence() {
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let fluid = Fluid::WATER.to_flowing();
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, Block::WATER.default_state.id);
        snapshot.physics_allowed.insert((position, position), true);
        snapshot.fluid_scheduled.insert((position, fluid.id), false);
        let effect = BlockCallbackEffect::NotifyNeighbor {
            position,
            source: Block::STONE.id,
        };
        let resolve = |snapshot| {
            resolve_callback_effects(
                &registry,
                snapshot,
                vec![effect.clone(), effect.clone()],
                128,
            )
            .unwrap()
        };
        let result = resolve(snapshot.clone());
        assert!(result.writes.is_empty());
        assert_eq!(
            result.schedules,
            vec![NeighborEffect::ScheduleFluid {
                position,
                id: fluid.id,
                delay: 5,
                priority: TickPriority::Normal,
            }]
        );
        snapshot.fluid_scheduled.insert((position, fluid.id), true);
        assert!(resolve(snapshot).schedules.is_empty());
    }

    #[test]
    fn waterlogged_break_keeps_source_water_and_schedules_flow_with_the_drop() {
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let mut properties =
            pumpkin_data::block_properties::OakStairsLikeProperties::default(&Block::OAK_STAIRS);
        properties.waterlogged = true;
        let before = properties.to_state_id(&Block::OAK_STAIRS);
        let mut snapshot = NeighborSnapshot::default();
        for x in -1..=1 {
            for y in 79..=81 {
                for z in -1..=1 {
                    let neighbor = BlockPos::new(x, y, z);
                    snapshot.blocks.insert(neighbor, BlockStateId::AIR);
                    snapshot.physics_allowed.insert((neighbor, position), false);
                }
            }
        }
        snapshot.blocks.insert(position, before);
        snapshot.natural_breaks.insert(
            position,
            CapturedNaturalBreak {
                before,
                cancelled: false,
                loot: None,
            },
        );
        let fluid = Fluid::WATER.to_flowing();
        snapshot.fluid_scheduled.insert((position, fluid.id), false);
        let result = resolve_callback_effects(
            &registry,
            snapshot.clone(),
            vec![BlockCallbackEffect::BreakBlock {
                position,
                flags: BlockFlags::NOTIFY_ALL,
            }],
            256,
        )
        .expect("waterlogged solid break is a finite callback");
        assert_eq!(
            result.writes,
            vec![(position, before, Block::WATER.default_state.id)]
        );
        assert_eq!(result.breaks.len(), 1);
        assert_eq!(result.breaks[0].before, before);
        assert_eq!(
            result.schedules,
            vec![NeighborEffect::ScheduleFluid {
                position,
                id: fluid.id,
                delay: 5,
                priority: TickPriority::Normal,
            }]
        );
        assert_eq!(snapshot.blocks[&position], before);
    }

    #[test]
    fn support_loss_stages_air_drop_and_particles_as_one_candidate() {
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let before = Block::WHITE_CARPET.default_state.id;
        let mut snapshot = NeighborSnapshot::default();
        for x in -1..=1 {
            for y in 79..=81 {
                for z in -1..=1 {
                    snapshot
                        .blocks
                        .insert(BlockPos::new(x, y, z), BlockStateId::AIR);
                }
            }
        }
        snapshot.blocks.insert(position, before);
        let loot = crate::world::loot::block_loot_facts(
            &Block::WHITE_CARPET,
            17,
            29,
            true,
            &crate::world::loot::LootContextParameters {
                block_state: Some(before.to_state()),
                position: Some(position.to_f64()),
                killed_by_player: Some(false),
                ..Default::default()
            },
        );
        snapshot.natural_breaks.insert(
            position,
            CapturedNaturalBreak {
                before,
                cancelled: false,
                loot: Some(loot),
            },
        );
        let (_, _, effects) = resolve_scheduled_intent(
            &registry,
            snapshot.clone(),
            &Block::WHITE_CARPET,
            &position,
            64,
        )
        .unwrap();
        let result =
            resolve_callback_effects(&registry, snapshot.clone(), effects.clone(), 64).unwrap();
        assert_eq!(result.writes, vec![(position, before, BlockStateId::AIR)]);
        assert_eq!(
            result.breaks,
            vec![NaturalBreakResult {
                position,
                before,
                items: vec![crate::world::loot::LootItemFact {
                    item: pumpkin_data::item::Item::WHITE_CARPET.id,
                    count: 1
                }],
                experience: 0,
                particles: true,
            }]
        );
        assert_eq!(snapshot.blocks[&position], before);
        snapshot
            .natural_breaks
            .get_mut(&position)
            .unwrap()
            .cancelled = true;
        let cancelled =
            resolve_callback_effects(&registry, snapshot.clone(), effects.clone(), 64).unwrap();
        assert!(cancelled.writes.is_empty() && cancelled.breaks.is_empty());
        snapshot.natural_breaks.get_mut(&position).unwrap().before = Block::STONE.default_state.id;
        assert!(resolve_callback_effects(&registry, snapshot, effects, 64).is_none());
    }

    #[test]
    fn lamp_power_edges_delay_off_and_publish_on_with_exact_read_witnesses() {
        use crate::block::blocks::redstone::redstone_lamp::RedstoneLamp;
        use pumpkin_data::block_properties::RedstoneOreLikeProperties;
        let position = BlockPos::new(0, 80, 0);
        let source = position.up();
        let mut properties = RedstoneOreLikeProperties::default(&Block::REDSTONE_LAMP);
        properties.lit = true;
        let lit = properties.to_state_id(&Block::REDSTONE_LAMP);
        let mut snapshot = NeighborSnapshot::default();
        snapshot.blocks.insert(position, lit);
        snapshot.blocks.insert(source, BlockStateId::AIR);
        snapshot.receives_power.insert(
            position,
            CapturedPower {
                value: false,
                reads: vec![(source, BlockStateId::AIR)],
                entity_reads: Vec::new(),
            },
        );
        let view = NeighborTranscript::new(snapshot.clone(), 32).unwrap();
        let effects = RedstoneLamp
            .callback_intent(BlockCallbackIntentArgs {
                world: &view,
                block: &Block::REDSTONE_LAMP,
                position: &position,
                event: BlockCallbackEvent::Neighbor {
                    source: Block::AIR.id,
                    notify: false,
                },
            })
            .unwrap();
        assert_eq!(
            effects,
            vec![BlockCallbackEffect::ScheduleBlock {
                position,
                block: Block::REDSTONE_LAMP.id,
                delay: 4,
                priority: TickPriority::Normal,
            }]
        );
        let (facts, _) = view.finish().unwrap();
        assert!(facts.contains(&NeighborFact::Block(source, BlockStateId::AIR)));
        let (_, _, off) = resolve_scheduled_intent(
            &super::super::registry::default_registry(),
            snapshot.clone(),
            &Block::REDSTONE_LAMP,
            &position,
            32,
        )
        .unwrap();
        properties.lit = false;
        let unlit = properties.to_state_id(&Block::REDSTONE_LAMP);
        assert_eq!(
            off,
            vec![BlockCallbackEffect::SetBlock {
                position,
                state: unlit,
                flags: BlockFlags::NOTIFY_LISTENERS
            }]
        );
        snapshot.blocks.insert(position, unlit);
        snapshot
            .blocks
            .insert(source, Block::REDSTONE_BLOCK.default_state.id);
        snapshot.receives_power.insert(
            position,
            CapturedPower {
                value: true,
                reads: vec![(source, Block::REDSTONE_BLOCK.default_state.id)],
                entity_reads: Vec::new(),
            },
        );
        let mut view = NeighborTranscript::new(snapshot, 32).unwrap();
        let on = RedstoneLamp
            .callback_intent(BlockCallbackIntentArgs {
                world: &view,
                block: &Block::REDSTONE_LAMP,
                position: &position,
                event: BlockCallbackEvent::Neighbor {
                    source: Block::REDSTONE_BLOCK.id,
                    notify: false,
                },
            })
            .unwrap();
        assert_eq!(
            on,
            vec![BlockCallbackEffect::SetBlock {
                position,
                state: lit,
                flags: BlockFlags::NOTIFY_LISTENERS
            }]
        );
        view.snapshot.blocks.insert(source, BlockStateId::AIR);
        assert!(!view.receives_redstone_power(&position));
        assert!(view.finish().is_none());
    }

    #[test]
    fn dirt_transition_and_observer_schedule_are_one_private_resolution() {
        let registry = super::super::registry::default_registry();
        let center = BlockPos::new(0, 80, 0);
        let observer = BlockPos::new(1, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        for x in -2..=2 {
            for y in 78..=82 {
                for z in -2..=2 {
                    snapshot
                        .blocks
                        .insert(BlockPos::new(x, y, z), Block::AIR.default_state.id);
                }
            }
        }
        snapshot
            .blocks
            .insert(center, Block::DIRT_PATH.default_state.id);
        let mut properties = ObserverLikeProperties::default(&Block::OBSERVER);
        properties.facing = pumpkin_data::block_properties::Facing::West;
        properties.powered = false;
        snapshot
            .blocks
            .insert(observer, properties.to_state_id(&Block::OBSERVER));
        snapshot
            .scheduled
            .insert((observer, u16::from(Block::OBSERVER.id)), false);
        for direction in BlockDirection::update_order() {
            snapshot
                .physics_allowed
                .insert((center.offset(direction.to_offset()), center), true);
        }
        let (_, _, effects) = resolve_scheduled_intent(
            &registry,
            snapshot.clone(),
            &Block::DIRT_PATH,
            &center,
            1024,
        )
        .unwrap();
        let result =
            resolve_callback_effects(&registry, snapshot.clone(), effects.clone(), 1024).unwrap();
        assert_eq!(
            result.writes,
            vec![(
                center,
                Block::DIRT_PATH.default_state.id,
                Block::DIRT.default_state.id
            )]
        );
        assert_eq!(result.listeners, vec![center]);
        assert_eq!(result.poi, vec![(center, Block::DIRT.id)]);
        assert_eq!(
            result.schedules,
            vec![NeighborEffect::ScheduleBlock {
                position: observer,
                id: u16::from(Block::OBSERVER.id),
                delay: 2,
                priority: TickPriority::Normal
            }]
        );
        assert_eq!(snapshot.blocks[&center], Block::DIRT_PATH.default_state.id);
        snapshot.physics_allowed.clear();
        assert!(resolve_callback_effects(&registry, snapshot, effects, 1024).is_none());
    }

    #[test]
    fn unsupported_block_entity_leg_refuses_whole_callback_candidate() {
        let registry = super::super::registry::default_registry();
        let center = BlockPos::new(0, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot.blocks.insert(center, Block::AIR.default_state.id);
        assert!(
            resolve_callback_effects(
                &registry,
                snapshot,
                vec![BlockCallbackEffect::SetBlock {
                    position: center,
                    state: Block::CHEST.default_state.id,
                    flags: BlockFlags::NOTIFY_ALL
                }],
                16
            )
            .is_none()
        );
    }

    #[test]
    fn support_loss_declares_break_and_requires_owned_drop_resolution() {
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, Block::WHITE_CARPET.default_state.id);
        snapshot
            .blocks
            .insert(position.down(), Block::AIR.default_state.id);
        let (_, _, effects) = resolve_scheduled_intent(
            &registry,
            snapshot.clone(),
            &Block::WHITE_CARPET,
            &position,
            32,
        )
        .unwrap();
        assert_eq!(
            effects,
            vec![BlockCallbackEffect::BreakBlock {
                position,
                flags: BlockFlags::empty()
            }]
        );
        assert!(resolve_callback_effects(&registry, snapshot.clone(), effects, 32).is_none());
        snapshot
            .blocks
            .insert(position.down(), Block::STONE.default_state.id);
        let (_, _, effects) =
            resolve_scheduled_intent(&registry, snapshot, &Block::WHITE_CARPET, &position, 32)
                .unwrap();
        assert!(effects.is_empty());
    }

    #[test]
    fn target_reset_and_lightning_decay_emit_exact_ordered_requests() {
        use pumpkin_data::block_properties::{LightningRodLikeProperties, TargetProperties};
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let mut target = TargetProperties::default(&Block::TARGET);
        target.power = 7;
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, target.to_state_id(&Block::TARGET));
        let (_, _, effects) =
            resolve_scheduled_intent(&registry, snapshot, &Block::TARGET, &position, 16).unwrap();
        let BlockCallbackEffect::SetBlock { state, flags, .. } = effects[0] else {
            panic!("target reset must write its state");
        };
        assert_eq!(TargetProperties::from_state_id(state).power, 0);
        assert_eq!(flags, BlockFlags::NOTIFY_ALL);
        assert_eq!(effects.len(), 1);
        let mut rod = LightningRodLikeProperties::default(&Block::LIGHTNING_ROD);
        rod.powered = true;
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, rod.to_state_id(&Block::LIGHTNING_ROD));
        let (_, _, effects) =
            resolve_scheduled_intent(&registry, snapshot, &Block::LIGHTNING_ROD, &position, 16)
                .unwrap();
        let BlockCallbackEffect::SetBlock { state, .. } = effects[0] else {
            panic!("rod decay must precede notifications");
        };
        assert!(!LightningRodLikeProperties::from_state_id(state).powered);
        assert!(
            matches!(effects[1], BlockCallbackEffect::NotifyNeighbors { position: source, .. } if source == position)
        );
        assert!(matches!(
            effects[2],
            BlockCallbackEffect::NotifyNeighbors { .. }
        ));
        assert_eq!(effects.len(), 3);
    }

    #[test]
    fn dirt_path_scheduled_callback_returns_full_transition_request() {
        let registry = super::super::registry::default_registry();
        let position = BlockPos::new(0, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, Block::DIRT_PATH.default_state.id);
        let (facts, schedules, effects) =
            resolve_scheduled_intent(&registry, snapshot, &Block::DIRT_PATH, &position, 8).unwrap();
        assert_eq!(
            facts,
            vec![NeighborFact::Block(
                position,
                Block::DIRT_PATH.default_state.id
            )]
        );
        assert!(schedules.is_empty());
        assert_eq!(
            effects,
            vec![BlockCallbackEffect::SetBlock {
                position,
                state: Block::DIRT.default_state.id,
                flags: BlockFlags::NOTIFY_ALL
            }]
        );
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, Block::REDSTONE_TORCH.default_state.id);
        assert!(
            resolve_scheduled_intent(&registry, snapshot, &Block::REDSTONE_TORCH, &position, 8)
                .is_none()
        );
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, Block::STONE.default_state.id);
        let (_, _, effects) =
            resolve_scheduled_intent(&registry, snapshot, &Block::DIRT_PATH, &position, 8).unwrap();
        assert!(effects.is_empty());
        assert!(
            resolve_scheduled_intent(
                &registry,
                NeighborSnapshot::default(),
                &Block::DIRT_PATH,
                &position,
                8
            )
            .is_none()
        );
    }

    #[test]
    fn fence_cascade_resolves_against_private_changed_blocks() {
        let center = BlockPos::new(0, 80, 0);
        let fence = BlockPos::new(1, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        for x in -2..=3 {
            for y in 78..=82 {
                for z in -2..=2 {
                    snapshot
                        .blocks
                        .insert(BlockPos::new(x, y, z), Block::AIR.default_state.id);
                }
            }
        }
        snapshot
            .blocks
            .insert(fence, Block::OAK_FENCE.default_state.id);
        let registry = super::super::registry::default_registry();
        let resolved = resolve_neighbor_states(
            &registry,
            snapshot.clone(),
            &[(center, Block::STONE.default_state.id)],
            1024,
        )
        .unwrap();
        let (_, before, after) = resolved
            .writes
            .iter()
            .find(|(position, _, _)| *position == fence)
            .unwrap();
        assert_eq!(*before, Block::OAK_FENCE.default_state.id);
        assert!(pumpkin_data::block_properties::OakFenceLikeProperties::from_state_id(*after).west);
        assert_eq!(snapshot.blocks[&fence], *before);
        assert!(resolved.effects.is_empty());
        assert!(
            resolve_neighbor_states(
                &registry,
                snapshot.clone(),
                &[(center, Block::STONE.default_state.id)],
                1
            )
            .is_none()
        );
        snapshot.blocks.remove(&BlockPos::new(2, 80, 0));
        assert!(
            resolve_neighbor_states(
                &registry,
                snapshot,
                &[(center, Block::STONE.default_state.id)],
                1024
            )
            .is_none()
        );
    }

    #[test]
    fn observer_callback_records_schedule_without_world_mutation() {
        let position = BlockPos::new(0, 80, 0);
        let state = Block::OBSERVER.default_state.id;
        let direction = ObserverLikeProperties::from_state_id(state)
            .facing
            .to_block_direction();
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .scheduled
            .insert((position, u16::from(Block::OBSERVER.id)), false);
        let view = NeighborTranscript::new(snapshot, 4).unwrap();
        let result = ObserverBlock.get_state_for_neighbor_update(GetStateForNeighborUpdateArgs {
            world: &view,
            block: &Block::OBSERVER,
            state_id: state,
            position: &position,
            direction,
            neighbor_position: &position.offset(direction.to_offset()),
            neighbor_state_id: Block::AIR.default_state.id,
        });
        assert_eq!(result, state);
        let (facts, effects) = view.finish().unwrap();
        assert_eq!(
            facts,
            vec![NeighborFact::Scheduled(position, u16::from(Block::OBSERVER.id), false); 2]
        );
        assert_eq!(
            effects,
            vec![NeighborEffect::ScheduleBlock {
                position,
                id: u16::from(Block::OBSERVER.id),
                delay: 2,
                priority: TickPriority::Normal,
            }]
        );
    }

    #[test]
    fn observer_pulse_preserves_order_and_only_schedules_on_rising_edge() {
        let position = BlockPos::new(0, 80, 0);
        let registry = super::super::registry::default_registry();
        for powered in [false, true] {
            let mut properties = ObserverLikeProperties::default(&Block::OBSERVER);
            properties.powered = powered;
            let state = properties.to_state_id(&Block::OBSERVER);
            let mut snapshot = NeighborSnapshot::default();
            snapshot.blocks.insert(position, state);
            let (_, schedules, effects) =
                resolve_scheduled_intent(&registry, snapshot, &Block::OBSERVER, &position, 16)
                    .unwrap();
            assert!(schedules.is_empty());
            let BlockCallbackEffect::SetBlock {
                state: next, flags, ..
            } = effects[0]
            else {
                panic!("block must precede notifications");
            };
            assert_eq!(flags, BlockFlags::NOTIFY_LISTENERS);
            assert_eq!(
                ObserverLikeProperties::from_state_id(next).powered,
                !powered
            );
            let notification_start = if powered {
                1
            } else {
                assert_eq!(
                    effects[1],
                    BlockCallbackEffect::ScheduleBlock {
                        position,
                        block: Block::OBSERVER.id,
                        delay: 2,
                        priority: TickPriority::Normal
                    }
                );
                2
            };
            assert!(matches!(
                effects[notification_start],
                BlockCallbackEffect::NotifyNeighbor {
                    source: pumpkin_data::BlockId::OBSERVER,
                    ..
                }
            ));
            assert!(matches!(
                effects[notification_start + 1],
                BlockCallbackEffect::NotifyNeighbors { .. }
            ));
            assert_eq!(effects.len(), notification_start + 2);
        }
    }

    #[test]
    fn missing_query_and_transcript_overflow_cannot_publish_partial_effects() {
        let position = BlockPos::new(1, 80, 1);
        let view = NeighborTranscript::new(NeighborSnapshot::default(), 1).unwrap();
        view.schedule_block_tick(&Block::OBSERVER, position, 2, TickPriority::Normal);
        view.get_block_state_id(&position);
        assert!(view.finish().is_none());
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .scheduled
            .insert((position, u16::from(Block::OBSERVER.id)), false);
        snapshot
            .fluid_scheduled
            .insert((position, Fluid::WATER.id), false);
        let view = NeighborTranscript::new(snapshot, 1).unwrap();
        view.schedule_block_tick(&Block::OBSERVER, position, 2, TickPriority::Normal);
        view.schedule_fluid_tick(&Fluid::WATER, position, 1, TickPriority::High);
        assert!(view.finish().is_none());
        assert!(NeighborTranscript::new(NeighborSnapshot::default(), 65_537).is_none());
    }

    #[test]
    fn schedule_queries_observe_prior_private_effect_and_deduplicate() {
        let position = BlockPos::new(0, 80, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .scheduled
            .insert((position, u16::from(Block::OBSERVER.id)), false);
        let view = NeighborTranscript::new(snapshot, 8).unwrap();
        assert!(!view.is_block_tick_scheduled(&position, &Block::OBSERVER));
        view.schedule_block_tick(&Block::OBSERVER, position, 2, TickPriority::Normal);
        assert!(view.is_block_tick_scheduled(&position, &Block::OBSERVER));
        view.schedule_block_tick(&Block::OBSERVER, position, 7, TickPriority::High);
        let (_, effects) = view.finish().unwrap();
        assert_eq!(
            effects,
            vec![NeighborEffect::ScheduleBlock {
                position,
                id: u16::from(Block::OBSERVER.id),
                delay: 2,
                priority: TickPriority::Normal,
            }]
        );
    }
}
