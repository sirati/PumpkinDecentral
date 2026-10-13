use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::canonical_action::{
    ActionState, AppliedWrite, ChunkPart, Target, TruthView, Value,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_data::BlockStateId;

pub(super) use super::cluster_world_callback_light::{
    derive_section_light, derived_light_changes, validate_derived_light_writes,
};

pub(crate) fn bedrock_block_actor(
    state: BlockStateId,
    position: pumpkin_util::math::position::BlockPos,
    document: &pumpkin_nbt::compound::NbtCompound,
) -> Option<pumpkin_nbt::compound::NbtCompound> {
    use pumpkin_data::tag::Taggable;
    if let Some(chest) = crate::world::bedrock_chest_block_actor(state, position) {
        return Some(chest);
    }
    if document.get_string("id")? != "minecraft:bed"
        || !state
            .to_block()
            .has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
    {
        return None;
    }
    let color =
        pumpkin_data::item::JavaToBedrockItemMapping::from_java_item_id(state.to_block().item_id)?
            .bedrock_data
            .try_into()
            .ok()?;
    let mut actor = pumpkin_nbt::compound::NbtCompound::new();
    actor.put_string("id", "Bed".to_string());
    actor.put_int("x", position.0.x);
    actor.put_int("y", position.0.y);
    actor.put_int("z", position.0.z);
    actor.put_byte("color", color);
    actor.put_bool("isMovable", true);
    Some(actor)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CallbackCaptureError {
    MissingChunk(ChunkKey),
    MissingLight(Target),
    InvalidWrite(Target),
    Budget,
}

pub(super) enum CallbackCapture {
    Ready(crate::block::neighbor::BlockCallbackResolution),
    Policy(Vec<crate::block::neighbor::CallbackRequest>),
    Unsupported,
}

#[derive(Clone, Copy)]
pub(super) enum CallbackRoot {
    NormalUse {
        can_modify_world: bool,
        sneaking: bool,
        facing: pumpkin_data::block_properties::HorizontalFacing,
    },
    Scheduled,
    FluidScheduled {
        fluid: u16,
        rules: crate::block::fluid::owned::FluidRules,
    },
    Random {
        seed: u64,
        ambient_darkness: u8,
    },
}

pub(super) fn capture_root<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    position: pumpkin_util::math::position::BlockPos,
    root: CallbackRoot,
    capacity: usize,
) -> Result<(crate::block::registry::BlockActionResult, CallbackCapture), CallbackCaptureError> {
    use crate::block::neighbor::{CallbackQuery, CallbackRequest};
    use crate::block::registry::BlockActionResult;
    capture_snapshot_request(
        state,
        world,
        registry,
        snapshot,
        facts,
        &CallbackRequest {
            query: CallbackQuery::Block(position),
            private_blocks: Default::default(),
        },
        capacity,
    )?;
    let block = snapshot.blocks[&position].to_block();
    for _ in 0..capacity {
        let (result, requests) = match root {
            CallbackRoot::NormalUse {
                can_modify_world,
                sneaking,
                facing,
            } => crate::block::neighbor::resolve_normal_use_intent_captured(
                registry,
                snapshot.clone(),
                block,
                &position,
                can_modify_world,
                sneaking,
                facing,
                capacity,
            ),
            CallbackRoot::Scheduled => {
                let (result, requests) = crate::block::neighbor::resolve_scheduled_intent_captured(
                    registry,
                    snapshot.clone(),
                    block,
                    &position,
                    capacity,
                );
                (
                    result.map(|(reads, schedules, effects)| {
                        (BlockActionResult::Success, reads, schedules, effects)
                    }),
                    requests,
                )
            }
            CallbackRoot::FluidScheduled { fluid, rules } => {
                let (result, requests) = crate::block::neighbor::resolve_fluid_intent_captured(
                    snapshot.clone(),
                    fluid,
                    position,
                    rules,
                    capacity,
                );
                (
                    result.map(|(reads, schedules, effects)| {
                        (BlockActionResult::Success, reads, schedules, effects)
                    }),
                    requests,
                )
            }
            CallbackRoot::Random {
                seed,
                ambient_darkness,
            } => {
                let mut random = pumpkin_util::random::RandomGenerator::Xoroshiro(
                    pumpkin_util::random::xoroshiro128::Xoroshiro::from_seed(seed),
                );
                let (result, requests) = crate::block::neighbor::resolve_random_intent_captured(
                    registry,
                    snapshot.clone(),
                    block,
                    &position,
                    &mut random,
                    ambient_darkness,
                    capacity,
                );
                (
                    result.map(|(reads, schedules, effects)| {
                        (BlockActionResult::Success, reads, schedules, effects)
                    }),
                    requests,
                )
            }
        };
        if let Some((result, reads, schedules, effects)) = result {
            let mut capture =
                capture_effects(state, world, registry, snapshot, facts, &effects, capacity)?;
            if let CallbackCapture::Ready(resolution) = &mut capture {
                if resolution.facts.len().saturating_add(reads.len()) > capacity
                    || resolution.schedules.len().saturating_add(schedules.len()) > capacity
                {
                    return Err(CallbackCaptureError::Budget);
                }
                resolution.facts.extend(reads);
                resolution.schedules.extend(schedules);
            }
            return Ok((result, capture));
        }
        if requests.is_empty() {
            return Ok((BlockActionResult::Pass, CallbackCapture::Unsupported));
        }
        let mut policy = Vec::new();
        for request in requests {
            if !capture_snapshot_request(
                state, world, registry, snapshot, facts, &request, capacity,
            )? {
                policy.push(request);
            }
        }
        if !policy.is_empty() {
            return Ok((BlockActionResult::Consume, CallbackCapture::Policy(policy)));
        }
    }
    Err(CallbackCaptureError::Budget)
}

pub(super) fn capture_effects<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    effects: &[crate::block::neighbor::BlockCallbackEffect],
    capacity: usize,
) -> Result<CallbackCapture, CallbackCaptureError> {
    if effects.len() > capacity || capacity == 0 {
        return Err(CallbackCaptureError::Budget);
    }
    for _ in 0..capacity {
        let (resolution, requests) = crate::block::neighbor::resolve_callback_effects_captured(
            registry,
            snapshot.clone(),
            effects.to_vec(),
            capacity,
        );
        if let Some(resolution) = resolution {
            return Ok(CallbackCapture::Ready(resolution));
        }
        if requests.is_empty() {
            return Ok(CallbackCapture::Unsupported);
        }
        let mut policy = Vec::new();
        for request in requests {
            if !capture_snapshot_request(
                state, world, registry, snapshot, facts, &request, capacity,
            )? {
                policy.push(request);
            }
        }
        if !policy.is_empty() {
            return Ok(CallbackCapture::Policy(policy));
        }
    }
    Err(CallbackCaptureError::Budget)
}

pub(super) fn capture_placement<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    request: &crate::block::neighbor::PlacementRequest<'_>,
    capacity: usize,
) -> Result<CallbackCapture, CallbackCaptureError> {
    for _ in 0..capacity {
        let (placement, requests) = crate::block::neighbor::resolve_placement_intent_captured(
            registry,
            snapshot.clone(),
            request,
            capacity,
        );
        if let Some((placed, reads, schedules)) = placement {
            let effect = crate::block::neighbor::BlockCallbackEffect::SetBlock {
                position: request.position,
                state: placed,
                flags: pumpkin_world::world::BlockFlags::NOTIFY_ALL,
            };
            let mut capture =
                capture_effects(state, world, registry, snapshot, facts, &[effect], capacity)?;
            if let CallbackCapture::Ready(resolution) = &mut capture {
                resolution.facts.extend(reads);
                resolution.schedules.extend(schedules);
            }
            return Ok(capture);
        }
        if requests.is_empty() {
            return Ok(CallbackCapture::Unsupported);
        }
        let mut policy = Vec::new();
        for request in requests {
            if !capture_snapshot_request(
                state, world, registry, snapshot, facts, &request, capacity,
            )? {
                policy.push(request);
            }
        }
        if !policy.is_empty() {
            return Ok(CallbackCapture::Policy(policy));
        }
    }
    Err(CallbackCaptureError::Budget)
}

pub(super) fn capture_normal_use<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    position: pumpkin_util::math::position::BlockPos,
    can_modify_world: bool,
    sneaking: bool,
    facing: pumpkin_data::block_properties::HorizontalFacing,
    capacity: usize,
) -> Result<(crate::block::registry::BlockActionResult, CallbackCapture), CallbackCaptureError> {
    capture_root(
        state,
        world,
        registry,
        snapshot,
        facts,
        position,
        CallbackRoot::NormalUse {
            can_modify_world,
            sneaking,
            facing,
        },
        capacity,
    )
}

pub(super) fn capture_break<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    position: pumpkin_util::math::position::BlockPos,
    capacity: usize,
) -> Result<CallbackCapture, CallbackCaptureError> {
    capture_effects(
        state,
        world,
        registry,
        snapshot,
        facts,
        &[crate::block::neighbor::BlockCallbackEffect::BreakBlock {
            position,
            flags: pumpkin_world::world::BlockFlags::NOTIFY_ALL,
        }],
        capacity,
    )
}

struct PowerCapture<'a> {
    state: &'a dyn pumpkin_cluster::canonical_action::ActionState,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &'a crate::block::registry::BlockRegistry,
    reads: std::cell::RefCell<Vec<(pumpkin_util::math::position::BlockPos, BlockStateId)>>,
    entity_reads: std::cell::RefCell<
        Vec<(
            pumpkin_util::math::position::BlockPos,
            Option<pumpkin_cluster::canonical_action::Cell>,
        )>,
    >,
    invalid: std::cell::Cell<bool>,
    missing: std::cell::Cell<Option<Target>>,
    capacity: usize,
}

impl pumpkin_world::world::BlockAccessor for PowerCapture<'_> {
    fn get_block_state_id(
        &self,
        position: &pumpkin_util::math::position::BlockPos,
    ) -> BlockStateId {
        let key = ChunkKey::new(
            self.world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        );
        let pos = pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        };
        let target = Target::Chunk {
            key,
            part: ChunkPart::Block(pos),
            inventory: None,
        };
        let Some(cell) = self.state.borrow(TruthView::Local, target) else {
            if !self.state.available(TruthView::Local, target) {
                self.missing.set(Some(target));
            }
            self.invalid.set(true);
            return BlockStateId::AIR;
        };
        let Value::Block(id) = cell.value else {
            self.invalid.set(true);
            return BlockStateId::AIR;
        };
        let Some(state) = BlockStateId::new(id) else {
            self.invalid.set(true);
            return BlockStateId::AIR;
        };
        let mut reads = self.reads.borrow_mut();
        if reads.len() == self.capacity {
            self.invalid.set(true);
        } else {
            reads.push((*position, state));
        }
        state
    }
    fn get_block(
        &self,
        position: &pumpkin_util::math::position::BlockPos,
    ) -> &'static pumpkin_data::Block {
        self.get_block_state_id(position).to_block()
    }
    fn get_block_state(
        &self,
        position: &pumpkin_util::math::position::BlockPos,
    ) -> &'static pumpkin_data::BlockState {
        self.get_block_state_id(position).to_state()
    }
    fn get_block_and_state(
        &self,
        position: &pumpkin_util::math::position::BlockPos,
    ) -> (
        &'static pumpkin_data::Block,
        &'static pumpkin_data::BlockState,
    ) {
        let state = self.get_block_state_id(position);
        (state.to_block(), state.to_state())
    }
}

impl crate::block::RedstonePowerWorld for PowerCapture<'_> {
    fn power_registry(&self) -> &crate::block::registry::BlockRegistry {
        self.registry
    }
    fn block_entity_signal(
        &self,
        position: &pumpkin_util::math::position::BlockPos,
        signal: crate::block::BlockEntitySignal,
    ) -> u8 {
        let key = ChunkKey::new(
            self.world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        );
        let pos = pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        };
        let target = Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(pos),
            inventory: None,
        };
        let cell = self.state.read(TruthView::Local, target);
        if !self.state.available(TruthView::Local, target) {
            self.missing.set(Some(target));
            self.invalid.set(true);
            return 0;
        }
        let mut reads = self.entity_reads.borrow_mut();
        if reads.len() == self.capacity {
            self.invalid.set(true);
            return 0;
        }
        reads.push((*position, cell.clone()));
        let Some(cell) = cell else {
            self.invalid.set(true);
            return 0;
        };
        let Value::BlockEntity(bytes) = &cell.value else {
            self.invalid.set(true);
            return 0;
        };
        let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(
            bytes.as_slice(),
        ));
        let Ok(document) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) else {
            self.invalid.set(true);
            return 0;
        };
        match signal {
            crate::block::BlockEntitySignal::Comparator => {
                let output = document.root_tag.get_int("OutputSignal").unwrap_or(0);
                if !(0..=15).contains(&output) {
                    self.invalid.set(true);
                    return 0;
                }
                output as u8
            }
            crate::block::BlockEntitySignal::TestBlockPowered => {
                if document.root_tag.get_bool("powered").unwrap_or(false) {
                    15
                } else {
                    0
                }
            }
            crate::block::BlockEntitySignal::TrappedChestViewers
            | crate::block::BlockEntitySignal::JukeboxPlaying => {
                self.invalid.set(true);
                0
            }
        }
    }
}

pub(super) fn capture_received_power<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    position: pumpkin_util::math::position::BlockPos,
    capacity: usize,
) -> Result<crate::block::neighbor::CapturedPower, CallbackCaptureError> {
    if capacity == 0 || capacity > 65_536 {
        return Err(CallbackCaptureError::Budget);
    }
    let capture = PowerCapture {
        state,
        world,
        registry,
        reads: std::cell::RefCell::new(Vec::new()),
        entity_reads: std::cell::RefCell::new(Vec::new()),
        invalid: std::cell::Cell::new(false),
        missing: std::cell::Cell::new(None),
        capacity,
    };
    let value = crate::block::blocks::redstone::block_receives_redstone_power(&capture, &position);
    if capture.invalid.get() {
        return Err(capture.missing.get().map_or(
            CallbackCaptureError::Budget,
            CallbackCaptureError::InvalidWrite,
        ));
    }
    Ok(crate::block::neighbor::CapturedPower {
        value,
        reads: capture.reads.into_inner(),
        entity_reads: capture.entity_reads.into_inner(),
    })
}

pub(super) fn capture_side_power<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    position: pumpkin_util::math::position::BlockPos,
    side: pumpkin_data::block_properties::HorizontalFacing,
    only_gate: bool,
    capacity: usize,
) -> Result<crate::block::neighbor::CapturedNumericPower, CallbackCaptureError> {
    if capacity == 0 || capacity > 65_536 {
        return Err(CallbackCaptureError::Budget);
    }
    let capture = PowerCapture {
        state,
        world,
        registry,
        reads: std::cell::RefCell::new(Vec::new()),
        entity_reads: std::cell::RefCell::new(Vec::new()),
        invalid: std::cell::Cell::new(false),
        missing: std::cell::Cell::new(None),
        capacity,
    };
    let value = crate::block::blocks::redstone::abstract_redstone_gate::get_power_on_side(
        &capture, &position, side, only_gate,
    );
    if capture.invalid.get() {
        return Err(capture.missing.get().map_or(
            CallbackCaptureError::Budget,
            CallbackCaptureError::InvalidWrite,
        ));
    }
    Ok(crate::block::neighbor::CapturedNumericPower {
        value,
        reads: capture.reads.into_inner(),
        entity_reads: capture.entity_reads.into_inner(),
    })
}

pub(super) fn capture_directional_power<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    position: pumpkin_util::math::position::BlockPos,
    direction: pumpkin_data::BlockDirection,
    capacity: usize,
) -> Result<crate::block::neighbor::CapturedNumericPower, CallbackCaptureError> {
    if capacity == 0 || capacity > 65_536 {
        return Err(CallbackCaptureError::Budget);
    }
    let capture = PowerCapture {
        state,
        world,
        registry,
        reads: std::cell::RefCell::new(Vec::new()),
        entity_reads: std::cell::RefCell::new(Vec::new()),
        invalid: std::cell::Cell::new(false),
        missing: std::cell::Cell::new(None),
        capacity,
    };
    let (block, block_state) =
        pumpkin_world::world::BlockAccessor::get_block_and_state(&capture, &position);
    let value = crate::block::blocks::redstone::get_redstone_power(
        block,
        block_state,
        &capture,
        &position,
        direction,
    );
    if capture.invalid.get() {
        return Err(capture.missing.get().map_or(
            CallbackCaptureError::Budget,
            CallbackCaptureError::InvalidWrite,
        ));
    }
    Ok(crate::block::neighbor::CapturedNumericPower {
        value,
        reads: capture.reads.into_inner(),
        entity_reads: capture.entity_reads.into_inner(),
    })
}

pub(super) fn capture_snapshot_query<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    query: &crate::block::neighbor::CallbackQuery,
    capacity: usize,
) -> Result<bool, CallbackCaptureError> {
    use crate::block::neighbor::CallbackQuery;
    use pumpkin_cluster::canonical_action::ScheduledTickKind;
    let target_for = |position: pumpkin_util::math::position::BlockPos, part| Target::Chunk {
        key: ChunkKey::new(
            world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        ),
        part,
        inventory: None,
    };
    let block_part = |position: pumpkin_util::math::position::BlockPos| {
        ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        })
    };
    let mut capture_block = |position| -> Result<BlockStateId, CallbackCaptureError> {
        let target = target_for(position, block_part(position));
        if facts.len() >= capacity && !facts.contains_key(&target) {
            return Err(CallbackCaptureError::Budget);
        }
        let cell = state.read(TruthView::Local, target);
        let Some(Value::Block(id)) = cell.as_ref().map(|cell| &cell.value) else {
            return Err(CallbackCaptureError::InvalidWrite(target));
        };
        let block = BlockStateId::new(*id).ok_or(CallbackCaptureError::InvalidWrite(target))?;
        facts.insert(target, cell);
        snapshot.blocks.insert(position, block);
        Ok(block)
    };
    match *query {
        CallbackQuery::BlockEntity(position) => {
            let target = target_for(
                position,
                ChunkPart::BlockEntity(pumpkin_cluster::protocol::BlockPos {
                    x: position.0.x,
                    y: position.0.y,
                    z: position.0.z,
                }),
            );
            if !state.available(TruthView::Local, target) {
                return Err(CallbackCaptureError::InvalidWrite(target));
            }
            if facts.len() >= capacity && !facts.contains_key(&target) {
                return Err(CallbackCaptureError::Budget);
            }
            let cell = state.read(TruthView::Local, target);
            if cell
                .as_ref()
                .is_some_and(|cell| !matches!(cell.value, Value::BlockEntity(_)))
            {
                return Err(CallbackCaptureError::InvalidWrite(target));
            }
            facts.insert(target, cell.clone());
            snapshot.block_entities.insert(position, cell);
        }
        CallbackQuery::Block(position) => {
            capture_block(position)?;
        }
        CallbackQuery::ReceivesPower(position) => {
            let power = capture_received_power(state, world, registry, position, capacity)?;
            for &(position, _) in &power.reads {
                capture_block(position)?;
            }
            snapshot.receives_power.insert(position, power);
        }
        CallbackQuery::DirectionalPower(position, direction) => {
            let direction = match direction {
                0 => pumpkin_data::BlockDirection::Down,
                1 => pumpkin_data::BlockDirection::Up,
                2 => pumpkin_data::BlockDirection::North,
                3 => pumpkin_data::BlockDirection::South,
                4 => pumpkin_data::BlockDirection::West,
                5 => pumpkin_data::BlockDirection::East,
                _ => return Err(CallbackCaptureError::Budget),
            };
            let power =
                capture_directional_power(state, world, registry, position, direction, capacity)?;
            for &(position, _) in &power.reads {
                capture_block(position)?;
            }
            snapshot
                .directional_power
                .insert((position, direction as u8), power);
        }
        CallbackQuery::Power(position, side, only_gate) => {
            let side = match side {
                0 => pumpkin_data::block_properties::HorizontalFacing::North,
                1 => pumpkin_data::block_properties::HorizontalFacing::South,
                2 => pumpkin_data::block_properties::HorizontalFacing::West,
                3 => pumpkin_data::block_properties::HorizontalFacing::East,
                _ => return Err(CallbackCaptureError::Budget),
            };
            let power =
                capture_side_power(state, world, registry, position, side, only_gate, capacity)?;
            for &(position, _) in &power.reads {
                capture_block(position)?;
            }
            snapshot
                .power
                .insert((position, side as u8, only_gate), power);
        }
        CallbackQuery::EmitsPower(block, state_id, direction) => {
            let direction = match direction {
                0 => pumpkin_data::BlockDirection::Down,
                1 => pumpkin_data::BlockDirection::Up,
                2 => pumpkin_data::BlockDirection::North,
                3 => pumpkin_data::BlockDirection::South,
                4 => pumpkin_data::BlockDirection::West,
                5 => pumpkin_data::BlockDirection::East,
                _ => return Err(CallbackCaptureError::Budget),
            };
            if state_id.to_block().id.as_u16() != block {
                return Err(CallbackCaptureError::Budget);
            }
            snapshot.emits_power.insert(
                (block, state_id, direction as u8),
                registry.emits_redstone_power(state_id.to_block(), state_id.to_state(), direction),
            );
        }
        CallbackQuery::BlockTick(position, id) | CallbackQuery::FluidTick(position, id) => {
            let target = target_for(position, ChunkPart::Whole);
            if facts.len() >= capacity && !facts.contains_key(&target) {
                return Err(CallbackCaptureError::Budget);
            }
            let cell = state.read(TruthView::Local, target);
            let Some(Value::ScheduledQueue(queue)) = cell.as_ref().map(|cell| &cell.value) else {
                return Err(CallbackCaptureError::InvalidWrite(target));
            };
            let kind = if matches!(query, CallbackQuery::BlockTick(..)) {
                ScheduledTickKind::Block
            } else {
                ScheduledTickKind::Fluid
            };
            let present = queue.iter().any(|tick| {
                Some(*tick) != snapshot.processing_tick
                    && tick.kind == kind
                    && tick.id == id
                    && tick.position.x == position.0.x
                    && tick.position.y == position.0.y
                    && tick.position.z == position.0.z
            });
            facts.insert(target, cell);
            if kind == ScheduledTickKind::Block {
                snapshot.scheduled.insert((position, id), present);
            } else {
                snapshot.fluid_scheduled.insert((position, id), present);
            }
        }
        CallbackQuery::Brightness(position, ambient_darkness) => {
            let target = target_for(
                position,
                ChunkPart::LightSection(position.0.y.div_euclid(16)),
            );
            if facts.len() >= capacity && !facts.contains_key(&target) {
                return Err(CallbackCaptureError::Budget);
            }
            let cell = state.read(TruthView::Local, target);
            let Some(Value::LightSection { sky, block }) = cell.as_ref().map(|cell| &cell.value)
            else {
                return Err(CallbackCaptureError::MissingLight(target));
            };
            if sky.len() != 2048 || block.len() != 2048 {
                return Err(CallbackCaptureError::InvalidWrite(target));
            }
            let index = ((position.0.y.rem_euclid(16) << 8)
                | (position.0.z.rem_euclid(16) << 4)
                | position.0.x.rem_euclid(16)) as usize;
            let nibble = |bytes: &[u8]| (bytes[index / 2] >> ((index % 2) * 4)) & 15;
            let value = nibble(sky)
                .saturating_sub(ambient_darkness)
                .max(nibble(block));
            facts.insert(target, cell);
            snapshot
                .brightness
                .insert((position, ambient_darkness), value);
        }
        CallbackQuery::Physics(..)
        | CallbackQuery::NaturalBreak(..)
        | CallbackQuery::FluidPolicy(..) => return Ok(false),
    }
    Ok(true)
}

struct CallbackQueryOverlay<'a, S> {
    state: &'a S,
    world: pumpkin_cluster::protocol::WorldId,
    blocks: &'a std::collections::HashMap<pumpkin_util::math::position::BlockPos, BlockStateId>,
}

impl<S: pumpkin_cluster::canonical_action::ActionState>
    pumpkin_cluster::canonical_action::ActionState for CallbackQueryOverlay<'_, S>
{
    fn machine_genesis(
        &self,
        _chunk: pumpkin_cluster::protocol::ChunkKey,
    ) -> Result<[u8; 32], pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.machine_genesis(_chunk)
    }

    fn machine_tick_footprint(
        &self,
        _identity: pumpkin_cluster::action_identity::MachineActionId,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        pumpkin_cluster::action_command::ActionFootprint,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.machine_tick_footprint(_identity, _values)
    }

    fn derive_machine_tick(
        &self,
        _identity: pumpkin_cluster::action_identity::MachineActionId,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        Vec<pumpkin_cluster::canonical_action::AppliedWrite>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.derive_machine_tick(_identity, _values)
    }

    fn world_tool_companion(
        &self,
        _location: pumpkin_cluster::world_command::BlockLocation,
        _current: u16,
    ) -> Result<
        Option<pumpkin_cluster::world_command::ToolCompanion>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.world_tool_companion(_location, _current)
    }

    fn world_tool_light_plans(
        &self,
        _location: pumpkin_cluster::world_command::BlockLocation,
        _operation: pumpkin_cluster::world_command::ToolOperation,
        _current: u16,
    ) -> Result<
        Vec<pumpkin_cluster::world_action::WorldLightPlan>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state
            .world_tool_light_plans(_location, _operation, _current)
    }

    fn world_tool_attributes(
        &self,
    ) -> Result<
        pumpkin_cluster::world_command::ToolAttributes,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.world_tool_attributes()
    }

    fn world_tool_transform(
        &self,
        _operation: pumpkin_cluster::world_command::ToolOperation,
        _current: u16,
        _item: u16,
    ) -> Result<Option<u16>, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.world_tool_transform(_operation, _current, _item)
    }

    fn world_tool_eye_height(
        &self,
        _living: &pumpkin_cluster::item_action::ActorLivingState,
    ) -> Result<f64, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.world_tool_eye_height(_living)
    }

    fn environmental_contact(
        &self,
        _intent: &pumpkin_cluster::combat_action::EnvironmentalDamageIntent,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        pumpkin_cluster::combat_action::EnvironmentalContactFacts,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.environmental_contact(_intent, _values)
    }

    fn derive_cloud_potion(
        &self,
        _intent: &pumpkin_cluster::area_cloud::AreaCloudIntent,
        _victim: &pumpkin_cluster::item_contact::ContactObservation,
        _id: pumpkin_cluster::canonical_action::ActionId,
        _tick: pumpkin_cluster::time::TickStamp,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        pumpkin_cluster::area_cloud::CloudPotionOutcome,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state
            .derive_cloud_potion(_intent, _victim, _id, _tick, _values)
    }

    fn derive_block_inventory_tick(
        &self,
        _intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.derive_block_inventory_tick(_intent, _values)
    }

    fn actor_occupancy_cut(
        &self,
        target: pumpkin_cluster::canonical_action::Target,
        value: &pumpkin_cluster::canonical_action::Value,
    ) -> Result<
        (
            pumpkin_cluster::realm_completion::PeerGeneration,
            pumpkin_cluster::actor_occupancy::ActorOccupancyCut,
        ),
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.actor_occupancy_cut(target, value)
    }

    fn actor_collision_shape(
        &self,
        target: pumpkin_cluster::canonical_action::Target,
        _value: &pumpkin_cluster::canonical_action::Value,
    ) -> Result<
        pumpkin_cluster::actor_occupancy::CollisionShape,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.actor_collision_shape(target, _value)
    }

    fn combat_parent_damaged(
        &self,
        _parent: pumpkin_cluster::canonical_action::ActionRef,
        _victim: &pumpkin_cluster::item_contact::ContactIdentity,
    ) -> Option<bool> {
        self.state.combat_parent_damaged(_parent, _victim)
    }

    fn combat_parent_charge(
        &self,
        _parent: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Option<u64> {
        self.state.combat_parent_charge(_parent)
    }

    fn entity_equipment_compatible(
        &self,
        _kind: u16,
        _slots: &[(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
            pumpkin_cluster::inventory::InventoryCell,
        )],
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.entity_equipment_compatible(_kind, _slots)
    }

    fn entity_inventory_storage_capacity(
        &self,
        _kind: u16,
        _facets: &[(
            pumpkin_cluster::mob_species_property::MobSpeciesProperty,
            pumpkin_cluster::mob_species_property::MobSpeciesPropertyValue,
        )],
        _storage_slots: u16,
    ) -> Result<u16, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state
            .entity_inventory_storage_capacity(_kind, _facets, _storage_slots)
    }

    fn entity_inventory_layout(
        &self,
        _body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<
        Vec<pumpkin_cluster::inventory::InvLoc>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.entity_inventory_layout(_body)
    }

    fn entity_inventory_limit(
        &self,
        _body: &pumpkin_cluster::entity_body::EntityBody,
        _item: &pumpkin_cluster::inventory::InventoryCell,
        _target: pumpkin_cluster::canonical_action::Target,
    ) -> Result<u8, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.entity_inventory_limit(_body, _item, _target)
    }

    fn fall_damage_factor(
        &self,
        _state: u16,
        _sneaking: bool,
    ) -> Result<u16, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.fall_damage_factor(_state, _sneaking)
    }

    fn environmental_hazard(
        &self,
        _kind: pumpkin_cluster::combat_action::EnvironmentalHazardKind,
        _block_state: u16,
    ) -> Result<
        Option<pumpkin_cluster::combat_action::EnvironmentalHazardFacts>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.environmental_hazard(_kind, _block_state)
    }

    fn status_effect_facts(
        &self,
        _effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<
        pumpkin_cluster::combat_action::CombatStatusFacts,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.status_effect_facts(_effects)
    }

    fn status_effect_modifiers(
        &self,
        _effects: &pumpkin_cluster::active_effects::ActiveEffectSet,
    ) -> Result<
        Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.status_effect_modifiers(_effects)
    }

    fn validate_block_inventory_layout(
        &self,
        _block: pumpkin_cluster::canonical_action::Target,
        _state: u16,
        _block_entity: &pumpkin_cluster::canonical_action::Cell,
        _bindings: &[pumpkin_cluster::canonical_inventory::InventorySlotBinding],
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state
            .validate_block_inventory_layout(_block, _state, _block_entity, _bindings)
    }

    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        self.state.inventory_equipment_attribute_ids()
    }

    fn derive_item_use(
        &self,
        _intent: &pumpkin_cluster::item_use::ItemUseIntent,
        _id: pumpkin_cluster::canonical_action::ActionId,
        _tick: pumpkin_cluster::time::TickStamp,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        Vec<pumpkin_cluster::canonical_action::Effect>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.derive_item_use(_intent, _id, _tick, _values)
    }

    fn mob_breeding_advancements(
        &self,
        _kind: u16,
        _credit: &pumpkin_cluster::mob_breeding::MobBreedingCredit,
        _state: &pumpkin_cluster::player_advancement::PlayerAdvancementState,
    ) -> Result<
        pumpkin_cluster::player_advancement::PlayerAdvancementState,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.mob_breeding_advancements(_kind, _credit, _state)
    }

    fn crossbow_items(
        &self,
        _intent: &pumpkin_cluster::crossbow_items::CrossbowItems,
        _id: pumpkin_cluster::canonical_action::ActionId,
        _tick: pumpkin_cluster::time::TickStamp,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::inventory::InventoryCell,
        >,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.crossbow_items(_intent, _id, _tick, _values)
    }

    fn vehicle_activator(
        &self,
        _state: u16,
    ) -> Result<Option<bool>, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.vehicle_activator(_state)
    }

    fn vehicle_drop_item(
        &self,
        _body: &pumpkin_cluster::entity_body::EntityBody,
    ) -> Result<
        pumpkin_cluster::inventory::InventoryCell,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.vehicle_drop_item(_body)
    }

    fn dropped_item_body(
        &self,
        _template: &pumpkin_cluster::entity_body::EntityBody,
        _source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<
        pumpkin_cluster::entity_body::EntityBody,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.dropped_item_body(_template, _source)
    }

    fn item_cooldown_group(
        &self,
        _source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<String, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.item_cooldown_group(_source)
    }

    fn item_transform(
        &self,
        _source: &pumpkin_cluster::inventory::InventoryCell,
        _operation: &pumpkin_cluster::canonical_inventory::ItemTransformOperation,
        _id: pumpkin_cluster::canonical_action::ActionId,
        _tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<
        pumpkin_cluster::canonical_inventory::ItemTransformOutput,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.item_transform(_source, _operation, _id, _tick)
    }

    fn menu_inventory_transform(
        &self,
        _kind: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind,
        _selection: &pumpkin_cluster::canonical_menu::MenuSelection,
        _inputs: &[pumpkin_cluster::inventory::InventoryCell],
    ) -> Result<
        pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformOutput,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state
            .menu_inventory_transform(_kind, _selection, _inputs)
    }

    fn mending_repair(
        &self,
        _source: &pumpkin_cluster::inventory::InventoryCell,
        _experience: u32,
    ) -> Result<
        (
            pumpkin_cluster::canonical_inventory::ItemTransformOutput,
            u32,
        ),
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.mending_repair(_source, _experience)
    }

    fn derive_projectile_tick(
        &self,
        _intent: &pumpkin_cluster::projectile_tick::ProjectileTickIntent,
        _id: pumpkin_cluster::canonical_action::ActionId,
        _tick: pumpkin_cluster::time::TickStamp,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        Vec<pumpkin_cluster::canonical_action::Effect>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state
            .derive_projectile_tick(_intent, _id, _tick, _values)
    }

    fn death_inventory_rule(
        &self,
        action: pumpkin_cluster::canonical_action::ActionRef,
        witness: pumpkin_cluster::world_rules::WorldRulesWitness,
        keep_inventory: bool,
        target: pumpkin_cluster::canonical_action::Target,
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state
            .death_inventory_rule(action, witness, keep_inventory, target)
    }

    fn recipe_known(&self, _id: &str) -> bool {
        self.state.recipe_known(_id)
    }

    fn entity_placement_matches(
        &self,
        _intent: &pumpkin_cluster::entity_placement::EntityPlacementIntent,
    ) -> bool {
        self.state.entity_placement_matches(_intent)
    }

    fn falling_damage_kind(
        &self,
        _state: u16,
    ) -> Option<pumpkin_cluster::combat_action::DamageKind> {
        self.state.falling_damage_kind(_state)
    }

    fn primed_tnt_block(&self, _state: u16) -> bool {
        self.state.primed_tnt_block(_state)
    }

    fn primed_tnt_identity(&self) -> Option<(u16, u16)> {
        self.state.primed_tnt_identity()
    }

    fn world_rules(
        &self,
        _witness: pumpkin_cluster::world_rules::WorldRulesWitness,
        _tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<
        pumpkin_cluster::world_rules::WorldRulesSnapshot,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.world_rules(_witness, _tick)
    }

    fn death_inventory_drop(
        &self,
        template: &pumpkin_cluster::entity_body::EntityBody,
        source: &pumpkin_cluster::inventory::InventoryCell,
    ) -> Result<
        Option<pumpkin_cluster::entity_body::EntityBody>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.death_inventory_drop(template, source)
    }

    fn resolve_world_respawn(
        &self,
        _plan: pumpkin_cluster::world_respawn::WorldRespawnPlan,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        Option<pumpkin_cluster::world_respawn::WorldRespawnResolution>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.resolve_world_respawn(_plan, _values)
    }

    fn scheduled_executor(
        &self,
        chunk: pumpkin_cluster::protocol::ChunkKey,
    ) -> Result<
        pumpkin_cluster::realm_completion::PeerGeneration,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.scheduled_executor(chunk)
    }

    fn derive_world_light(
        &self,
        _plan: pumpkin_cluster::world_action::WorldLightPlan,
        _values: &std::collections::BTreeMap<
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::canonical_action::Cell,
        >,
    ) -> Result<
        Vec<pumpkin_cluster::canonical_action::Effect>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.derive_world_light(_plan, _values)
    }

    fn block_segment_obstructs(
        &self,
        _state: u16,
        _position: pumpkin_cluster::protocol::BlockPos,
        _start: [u64; 3],
        _end: [u64; 3],
    ) -> Result<bool, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state
            .block_segment_obstructs(_state, _position, _start, _end)
    }

    fn issuer_entity_admission_authority(
        &self,
        _reference: pumpkin_cluster::canonical_action::ActionRef,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness,
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state
            .issuer_entity_admission_authority(_reference, witness)
    }

    fn issuer_entity_initialization_matches(
        &self,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness,
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.issuer_entity_initialization_matches(witness)
    }

    fn validate_live_player_lease(
        &self,
        _lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> bool {
        self.state.validate_live_player_lease(_lease)
    }

    fn inventory_limit(
        &self,
        _source: &pumpkin_cluster::inventory::InventoryCell,
        _destination: pumpkin_cluster::canonical_action::Target,
    ) -> Result<u8, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.inventory_limit(_source, _destination)
    }

    fn inventory_equipment_facts(
        &self,
        _cells: &[(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentSlot,
            pumpkin_cluster::inventory::InventoryCell,
        )],
    ) -> Result<
        pumpkin_cluster::combat_action::CombatEquipmentFacts,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state.inventory_equipment_facts(_cells)
    }

    fn action_source_slot(
        &self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Option<u64> {
        self.state.action_source_slot(reference)
    }
    fn player_advancement_outcome(
        &self,
        event: &pumpkin_cluster::player_advancement::AdvancementEvent,
        state: &pumpkin_cluster::player_advancement::PlayerAdvancementState,
        achieved_millis: u64,
    ) -> Result<
        pumpkin_cluster::player_advancement::AdvancementOutcome,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        self.state
            .player_advancement_outcome(event, state, achieved_millis)
    }

    fn wake_rest_stat(
        &self,
    ) -> Result<(u16, u32), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.wake_rest_stat()
    }

    fn validate_wake_beds(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
        sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
        beds: &[pumpkin_cluster::sleep_action::BedRelease],
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.validate_wake_beds(world, sleeping, beds)
    }

    fn validate_morning(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
        morning: pumpkin_cluster::world_time::MorningTransition,
        source: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.validate_morning(world, morning, source)
    }

    fn validate_world_border(
        &self,
        witness: pumpkin_cluster::world_time::WorldBorderWitness,
        source: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        self.state.validate_world_border(witness, source)
    }

    fn read(
        &self,
        view: TruthView,
        target: Target,
    ) -> Option<pumpkin_cluster::canonical_action::Cell> {
        let mut cell = self.state.read(view, target)?;
        if view == TruthView::Local {
            if let Target::Chunk {
                key,
                part: ChunkPart::Block(position),
                inventory: None,
            } = target
            {
                if key.world == self.world {
                    let position = pumpkin_util::math::position::BlockPos::new(
                        position.x, position.y, position.z,
                    );
                    if let Some(block) = self.blocks.get(&position) {
                        cell.value = Value::Block(block.as_u16());
                    }
                }
            }
        }
        Some(cell)
    }
    fn available(&self, view: TruthView, target: Target) -> bool {
        self.state.available(view, target)
    }
    fn commit(
        &mut self,
        _context: pumpkin_cluster::canonical_action::ActionContext,
        _view: TruthView,
        _writes: &[AppliedWrite],
    ) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)
    }
}

pub(super) fn capture_snapshot_request<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    world: pumpkin_cluster::protocol::WorldId,
    registry: &crate::block::registry::BlockRegistry,
    snapshot: &mut crate::block::neighbor::NeighborSnapshot,
    facts: &mut BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    request: &crate::block::neighbor::CallbackRequest,
    capacity: usize,
) -> Result<bool, CallbackCaptureError> {
    let overlay = CallbackQueryOverlay {
        state,
        world,
        blocks: &request.private_blocks,
    };
    let mut captured = BTreeMap::new();
    let complete = capture_snapshot_query(
        &overlay,
        world,
        registry,
        snapshot,
        &mut captured,
        &request.query,
        capacity,
    )?;
    for target in captured.keys().copied() {
        if facts.len() >= capacity && !facts.contains_key(&target) {
            return Err(CallbackCaptureError::Budget);
        }
        facts.insert(target, state.read(TruthView::Local, target));
    }
    for &position in request.private_blocks.keys() {
        let target = Target::Chunk {
            key: ChunkKey::new(
                world,
                position.0.x.div_euclid(16),
                position.0.z.div_euclid(16),
            ),
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
                x: position.0.x,
                y: position.0.y,
                z: position.0.z,
            }),
            inventory: None,
        };
        let Some(cell) = state.read(TruthView::Local, target) else {
            return Err(CallbackCaptureError::InvalidWrite(target));
        };
        let Value::Block(id) = cell.value else {
            return Err(CallbackCaptureError::InvalidWrite(target));
        };
        snapshot.blocks.insert(
            position,
            BlockStateId::new(id).ok_or(CallbackCaptureError::InvalidWrite(target))?,
        );
    }
    Ok(complete)
}

pub(super) fn build_callback_action<S: ActionState>(
    state: &S,
    reference: pumpkin_cluster::canonical_action::ActionRef,
    producer: pumpkin_cluster::realm_completion::PeerGeneration,
    actor_chunk: Option<ChunkKey>,
    lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    world: pumpkin_cluster::protocol::WorldId,
    captured: &BTreeMap<Target, Option<pumpkin_cluster::canonical_action::Cell>>,
    resolution: &crate::block::neighbor::BlockCallbackResolution,
) -> Result<
    pumpkin_cluster::canonical_action::Action,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{
        Action, ActionClass, ActionKind, Condition, Effect, Expected, ScheduledTickFact,
        ScheduledTickKind, ScheduledTickOrigin, UndoProcedure,
    };
    let block_target = |position: pumpkin_util::math::position::BlockPos| Target::Chunk {
        key: ChunkKey::new(
            world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        ),
        part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        }),
        inventory: None,
    };
    let mut conditions = BTreeMap::new();
    for (&target, cell) in captured {
        let expected = match cell {
            Some(cell) => match cell.value {
                Value::Block(id) => Expected::BlockState(id),
                Value::BlockSection(_) | Value::LightSection { .. } => {
                    Expected::RevisionEquals(cell.revision)
                }
                Value::ScheduledQueue(_) => continue,
                Value::Entity(ref body)
                    if reference.id.actor == pumpkin_cluster::actor::ActorId::Entity(body.id)
                        && body.owner == producer
                        && matches!(target, Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None }
                        if actor_chunk == Some(key) && entity == body.id) =>
                {
                    Expected::ActorControlled {
                        birth: body.birth.clone(),
                        issuer: producer,
                    }
                }
                _ => Expected::Exact(cell.clone()),
            },
            None => Expected::Missing,
        };
        conditions.insert(target, expected);
    }
    let mut effects = Vec::new();
    for &(position, before, after) in &resolution.writes {
        let target = block_target(position);
        let Some(Some(cell)) = captured.get(&target) else {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                target,
            ));
        };
        if cell.value != Value::Block(before.as_u16()) {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                target,
            ));
        }
        conditions.insert(target, Expected::BlockState(before.as_u16()));
        effects.push(Effect::Put {
            target,
            value: Value::Block(after.as_u16()),
        });
    }
    for (position, before) in &resolution.block_entity_removals {
        let Target::Chunk { key, .. } = block_target(*position) else {
            unreachable!()
        };
        let target = Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(pumpkin_cluster::protocol::BlockPos {
                x: position.0.x,
                y: position.0.y,
                z: position.0.z,
            }),
            inventory: None,
        };
        if captured.get(&target) != Some(&Some(before.clone())) {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                target,
            ));
        }
        conditions.insert(target, Expected::Exact(before.clone()));
        effects.push(Effect::Delete { target });
    }
    for (ordinal, schedule) in resolution.schedules.iter().enumerate() {
        let (position, kind, id, delay, priority) = match *schedule {
            crate::block::neighbor::NeighborEffect::ScheduleBlock {
                position,
                id,
                delay,
                priority,
            } => (position, ScheduledTickKind::Block, id, delay, priority),
            crate::block::neighbor::NeighborEffect::ScheduleFluid {
                position,
                id,
                delay,
                priority,
            } => (position, ScheduledTickKind::Fluid, id, delay, priority),
        };
        let Target::Chunk { key, .. } = block_target(position) else {
            unreachable!()
        };
        effects.push(Effect::ScheduleTick {
            target: Target::Chunk {
                key,
                part: ChunkPart::Whole,
                inventory: None,
            },
            tick: ScheduledTickFact {
                executor: state.scheduled_executor(key)?,
                origin: ScheduledTickOrigin::Action {
                    cause: reference,
                    producer,
                },
                kind,
                id,
                position: pumpkin_cluster::protocol::BlockPos {
                    x: position.0.x,
                    y: position.0.y,
                    z: position.0.z,
                },
                due: pumpkin_cluster::time::TickStamp(
                    reference.tick.0.wrapping_add(u16::from(delay.max(1))),
                ),
                priority: priority as i8,
                source_order: ((u64::from(reference.id.sequence) << 24)
                    | (u64::from(reference.id.family) << 16))
                    | ordinal as u64,
            },
        });
    }
    let mut conditions: Vec<_> = conditions
        .into_iter()
        .map(|(target, expected)| Condition { target, expected })
        .collect();
    let mut schedule_reads = BTreeSet::new();
    for fact in &resolution.facts {
        let (position, kind, id, except) = match *fact {
            crate::block::neighbor::NeighborFact::ScheduledExcept(position, id, _, fact) => {
                (position, ScheduledTickKind::Block, id, Some(fact))
            }
            crate::block::neighbor::NeighborFact::FluidScheduledExcept(position, id, _, fact) => {
                (position, ScheduledTickKind::Fluid, id, Some(fact))
            }
            crate::block::neighbor::NeighborFact::Scheduled(position, id, _) => {
                (position, ScheduledTickKind::Block, id, None)
            }
            crate::block::neighbor::NeighborFact::FluidScheduled(position, id, _) => {
                (position, ScheduledTickKind::Fluid, id, None)
            }
            _ => continue,
        };
        let Target::Chunk { key, .. } = block_target(position) else {
            unreachable!()
        };
        let target = Target::Chunk {
            key,
            part: ChunkPart::Whole,
            inventory: None,
        };
        let position = pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        };
        if !schedule_reads.insert((target, kind, id, position)) {
            continue;
        }
        let Some(Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::ScheduledQueue(queue),
            ..
        })) = captured.get(&target)
        else {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                target,
            ));
        };
        let present = queue.iter().any(|tick| {
            Some(*tick) != except && tick.kind == kind && tick.id == id && tick.position == position
        });
        conditions.push(Condition {
            target,
            expected: match except {
                Some(except) => Expected::ScheduledTickPresenceExcept {
                    kind,
                    id,
                    position,
                    except,
                    present,
                },
                None => Expected::ScheduledTickPresence {
                    kind,
                    id,
                    position,
                    present,
                },
            },
        });
    }
    conditions.sort_by_key(|condition| condition.target);
    let mut stakeholders = BTreeSet::new();
    if let Some(chunk) = actor_chunk {
        stakeholders.insert(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk));
    }
    for condition in &conditions {
        stakeholders.insert(condition.target.stakeholder());
    }
    for effect in &effects {
        let target = match effect {
            Effect::Put { target, .. }
            | Effect::ScheduleTick { target, .. }
            | Effect::Delete { target } => *target,
            _ => unreachable!(),
        };
        stakeholders.insert(target.stakeholder());
    }
    let lease = lease.filter(|lease| conditions.iter().any(|condition| matches!(condition.target,
        Target::Player { key, .. } if key == lease.key)
        || matches!(condition.target, Target::Chunk { part: ChunkPart::Player(gid), .. } if gid == lease.gid)));
    let player_chunks = lease.into_iter().filter(|lease| conditions.iter().any(|condition| matches!(condition.target, Target::Player { key, .. } if key == lease.key)))
        .filter_map(|lease| actor_chunk.map(|chunk| (lease.key, chunk))).collect();
    let mut presentation = Vec::new();
    let presentation_chunk = |position: pumpkin_util::math::position::BlockPos| {
        ChunkKey::new(
            world,
            position.0.x.div_euclid(16),
            position.0.z.div_euclid(16),
        )
    };
    let wire_position =
        |position: pumpkin_util::math::position::BlockPos| pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        };
    for event in &resolution.presentation {
        let intent = match *event {
            crate::block::neighbor::BlockCallbackEffect::Sound {
                position,
                sound,
                volume_milli,
                pitch_milli,
            } => pumpkin_cluster::world_presentation::WorldPresentationIntent::Sound {
                chunk: presentation_chunk(position),
                position: wire_position(position),
                sound: sound as u16,
                volume_milli,
                pitch_milli,
                category: pumpkin_data::sound::SoundCategory::Blocks as u8,
            },
            crate::block::neighbor::BlockCallbackEffect::GameEvent { position, event } => {
                pumpkin_cluster::world_presentation::WorldPresentationIntent::GameEvent {
                    chunk: presentation_chunk(position),
                    position: wire_position(position),
                    event: event as u16,
                }
            }
            _ => return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction),
        };
        stakeholders.insert(pumpkin_cluster::canonical_action::Stakeholder::Chunk(
            intent.chunk(),
        ));
        presentation.push(intent);
    }
    for result in &resolution.breaks {
        if result.particles {
            let chunk = presentation_chunk(result.position);
            stakeholders.insert(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk));
            presentation.push(
                pumpkin_cluster::world_presentation::WorldPresentationIntent::BreakParticles {
                    chunk,
                    position: wire_position(result.position),
                    state: result.before.as_u16(),
                },
            );
        }
    }
    let action = Action {
        edit_authority: None,
        id: reference.id,
        tick: reference.tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        actor_chunk,
        player_leases: lease.into_iter().collect(),
        player_chunks,
        stakeholders: stakeholders.into_iter().collect(),
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::World,
        client_origin: pumpkin_cluster::canonical_action::ClientOrigin::None,
        presentation,
    };
    Ok(action)
}

pub(super) fn append_callback_light<S: pumpkin_cluster::canonical_action::ActionState>(
    state: &S,
    action: &mut pumpkin_cluster::canonical_action::Action,
    plans: &[pumpkin_cluster::world_action::WorldLightPlan],
) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{
        AppliedWrite, ApplyFailure, Cell, Condition, Effect, Expected, Stakeholder,
    };
    let mut candidate = action.clone();
    let writes = candidate
        .effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Put {
                target,
                value: Value::Block(after),
            } => Some((*target, *after)),
            _ => None,
        })
        .map(|(target, after)| {
            let before = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            Ok(AppliedWrite {
                target,
                after: Some(Cell {
                    revision: before
                        .revision
                        .checked_add(1)
                        .ok_or(ApplyFailure::InvalidAction)?,
                    value: Value::Block(after),
                }),
                before: Some(before),
            })
        })
        .collect::<Result<Vec<_>, ApplyFailure>>()?;
    let mut centers = BTreeSet::new();
    let mut factual = BTreeMap::new();
    for &plan in plans {
        if !centers.insert(plan.center) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut projected = BTreeMap::new();
        for target in plan
            .section_targets()?
            .into_iter()
            .chain(plan.light_targets()?)
        {
            let before = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            let value = if matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::BlockSection(_),
                    ..
                }
            ) {
                pumpkin_cluster::block_section::project_writes(target, &before, &writes)?
            } else {
                before.clone()
            };
            factual.insert(target, before);
            projected.insert(target, value);
        }
        candidate
            .effects
            .extend(state.derive_world_light(plan, &projected)?);
    }
    for (target, before) in factual {
        let expected = Expected::RevisionEquals(before.revision);
        if let Some(condition) = candidate
            .conditions
            .iter()
            .find(|condition| condition.target == target)
        {
            if condition.expected != expected {
                return Err(ApplyFailure::Condition(target));
            }
        } else {
            candidate.conditions.push(Condition { target, expected });
        }
        let stakeholder = target.stakeholder();
        if !candidate.stakeholders.contains(&stakeholder) {
            candidate.stakeholders.push(stakeholder);
        }
    }
    for center in centers {
        if !candidate.stakeholders.contains(&Stakeholder::Chunk(center)) {
            candidate.stakeholders.push(Stakeholder::Chunk(center));
        }
    }
    candidate.stakeholders.sort();
    candidate.validate()?;
    *action = candidate;
    Ok(())
}

pub(super) use super::cluster_world_block_use::{
    WorldUseContext, capture_use_context, complete_use_context, materialize_use_on_block,
    normalize_use_on_block,
};

pub(super) fn resolution_light_plans(
    runtime_world: &crate::world::World,
    world: pumpkin_cluster::protocol::WorldId,
    resolution: &crate::block::neighbor::BlockCallbackResolution,
) -> Result<
    Vec<pumpkin_cluster::world_action::WorldLightPlan>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let mut light_centers = BTreeSet::new();
    for &(position, before, after) in &resolution.writes {
        if pumpkin_world::lighting::LightEngine::has_different_light_properties(
            before.to_state(),
            after.to_state(),
        ) {
            for z in -1..=1 {
                for x in -1..=1 {
                    light_centers.insert(ChunkKey::new(
                        world,
                        position
                            .0
                            .x
                            .div_euclid(16)
                            .checked_add(x)
                            .ok_or(ApplyFailure::InvalidAction)?,
                        position
                            .0
                            .z
                            .div_euclid(16)
                            .checked_add(z)
                            .ok_or(ApplyFailure::InvalidAction)?,
                    ));
                }
            }
        }
    }
    let mode = match runtime_world.level.lighting_config {
        pumpkin_config::lighting::LightingEngineConfig::Default => {
            pumpkin_cluster::world_action::WorldLightMode::Default
        }
        pumpkin_config::lighting::LightingEngineConfig::Full => {
            pumpkin_cluster::world_action::WorldLightMode::Full
        }
        pumpkin_config::lighting::LightingEngineConfig::Dark => {
            pumpkin_cluster::world_action::WorldLightMode::Dark
        }
    };
    let plans = light_centers
        .into_iter()
        .map(|center| {
            pumpkin_cluster::world_action::WorldLightPlan {
                center,
                min_section_y: runtime_world.dimension.min_y.div_euclid(16),
                section_count: (runtime_world.dimension.height / 16)
                    .try_into()
                    .unwrap_or(0),
                has_skylight: runtime_world.dimension.has_skylight,
                mode,
                config_version: 1,
            }
            .with_config_fingerprint()
        })
        .collect::<Vec<_>>();
    Ok(plans)
}

pub(super) fn append_resolution_light<S: pumpkin_cluster::canonical_action::ActionState>(
    view: &S,
    runtime_world: &crate::world::World,
    world: pumpkin_cluster::protocol::WorldId,
    action: &mut pumpkin_cluster::canonical_action::Action,
    resolution: &crate::block::neighbor::BlockCallbackResolution,
) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    append_callback_light(
        view,
        action,
        &resolution_light_plans(runtime_world, world, resolution)?,
    )
}
