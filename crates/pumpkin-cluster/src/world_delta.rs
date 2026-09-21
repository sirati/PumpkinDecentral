use std::collections::HashMap;

use core::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::buckets::BucketTable;
use crate::identity::{ActionActor, ActionSeq, ServerId};
use crate::interact::InteractGroup;
use crate::inventory::InventoryStack;
use crate::order::order_action_actors;
use crate::protocol::{BlockPos, BlockUndo, ChunkAddr, EntityRef, StreamKind};
use crate::reconcile::ReconcilePlan;
use crate::time::TickStamp;

pub const WORLD_DELTA_FRAME_PREFIX: usize = 5;
pub const WORLD_DELTA_MAGIC: [u8; 4] = [0x57, 0x44, 0x4C, 0x54];
pub const WORLD_DELTA_TAG_RANDOM_TICK: u8 = 1;
pub const WORLD_DELTA_TAG_REDSTONE: u8 = 2;
pub const WORLD_DELTA_TAG_EXPLOSION: u8 = 3;
pub const WORLD_DELTA_TAG_CAUSAL_REDSTONE: u8 = 4;
pub const WORLD_DELTA_TAG_TRANSACTIONAL_EXPLOSION: u8 = 5;
pub const WORLD_DELTA_MAX_EDITS: usize = 256;
pub const WORLD_DELTA_NO_INVENTORY: u8 = u8::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockDelta {
    pub pos: BlockPos,
    pub old_state: u16,
    pub new_state: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldActionRef {
    pub owner: ServerId,
    pub tick: TickStamp,
    pub ordinal: u16,
    pub anchor: BlockPos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldCause {
    Entity { owner: ServerId, entity: EntityRef },
    Interact(InteractGroup),
    Cascade(WorldActionRef),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldActionGroup {
    pub reference: WorldActionRef,
    pub cause: WorldCause,
}

impl WorldActionGroup {
    #[must_use]
    pub const fn entity(
        owner: ServerId,
        tick: TickStamp,
        ordinal: u16,
        anchor: BlockPos,
        entity: EntityRef,
    ) -> Self {
        Self {
            reference: WorldActionRef {
                owner,
                tick,
                ordinal,
                anchor,
            },
            cause: WorldCause::Entity { owner, entity },
        }
    }

    #[must_use]
    pub const fn interact(
        owner: ServerId,
        ordinal: u16,
        group: InteractGroup,
    ) -> Self {
        Self {
            reference: WorldActionRef {
                owner,
                tick: group.tick,
                ordinal,
                anchor: group.anchor,
            },
            cause: WorldCause::Interact(group),
        }
    }

    #[must_use]
    pub const fn cascade(
        owner: ServerId,
        tick: TickStamp,
        ordinal: u16,
        anchor: BlockPos,
        cause: WorldActionRef,
    ) -> Self {
        Self {
            reference: WorldActionRef {
                owner,
                tick,
                ordinal,
                anchor,
            },
            cause: WorldCause::Cascade(cause),
        }
    }

    #[must_use]
    pub fn is_valid(self) -> bool {
        match self.cause {
            WorldCause::Entity { owner, entity } => owner == self.reference.owner && entity.owner == owner,
            WorldCause::Interact(group) => group.tick == self.reference.tick,
            WorldCause::Cascade(_) => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldDependentKind {
    Comparator,
    Observer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldDependency {
    pub cause: WorldActionRef,
    pub kind: WorldDependentKind,
    pub target: BlockPos,
    pub delay_ticks: u8,
    pub fire_tick: TickStamp,
    pub expected_old_state: u16,
    pub new_state: u16,
}

impl WorldDependency {
    #[must_use]
    pub const fn new(
        cause: WorldActionRef,
        kind: WorldDependentKind,
        target: BlockPos,
        delay_ticks: u8,
        expected_old_state: u16,
        new_state: u16,
    ) -> Self {
        Self {
            cause,
            kind,
            target,
            delay_ticks,
            fire_tick: TickStamp(cause.tick.0.wrapping_add(delay_ticks as u16)),
            expected_old_state,
            new_state,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.expected_old_state != self.new_state
            && self.fire_tick.0 == self.cause.tick.0.wrapping_add(self.delay_ticks as u16)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalRedstoneUpdate {
    pub group: WorldActionGroup,
    pub chunk: ChunkAddr,
    pub edits: Vec<BlockDelta>,
    pub dependencies: Vec<WorldDependency>,
}

impl CausalRedstoneUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.group.reference.tick
    }

    #[must_use]
    pub const fn owner(&self) -> ServerId {
        self.group.reference.owner
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.group.is_valid()
            && !self.edits.is_empty()
            && self.edits.iter().all(|edit| {
                edit.old_state != edit.new_state
                    && chunk_of_block(edit.pos.x, edit.pos.z) == self.chunk
            })
            && self.dependencies.iter().all(|dependency| {
                dependency.cause == self.group.reference && dependency.is_valid()
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplosionDropRef {
    pub entity: EntityRef,
    pub source: BlockPos,
    pub stack: InventoryStack,
}

impl ExplosionDropRef {
    #[must_use]
    pub fn is_valid_for(&self, owner: ServerId, chunk: ChunkAddr) -> bool {
        self.entity.owner == owner
            && !self.stack.is_empty()
            && chunk_of_block(self.source.x, self.source.z) == chunk
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplosionEntityDamageRef {
    pub target: EntityRef,
    pub expected_health_milli: u32,
    pub new_health_milli: u32,
}

impl ExplosionEntityDamageRef {
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.new_health_milli <= self.expected_health_milli
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionalExplosionUpdate {
    pub group: WorldActionGroup,
    pub chunk: ChunkAddr,
    pub center: BlockPos,
    pub edits: Vec<BlockDelta>,
    pub drops: Vec<ExplosionDropRef>,
    pub damages: Vec<ExplosionEntityDamageRef>,
}

impl TransactionalExplosionUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.group.reference.tick
    }

    #[must_use]
    pub const fn owner(&self) -> ServerId {
        self.group.reference.owner
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.group.is_valid()
            && (!self.edits.is_empty() || !self.drops.is_empty() || !self.damages.is_empty())
            && self.edits.iter().all(|edit| {
                edit.old_state != edit.new_state
                    && chunk_of_block(edit.pos.x, edit.pos.z) == self.chunk
            })
            && self
                .drops
                .iter()
                .all(|drop| drop.is_valid_for(self.owner(), self.chunk))
            && self.damages.iter().all(|damage| damage.is_valid())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionalExplosionUndo {
    pub blocks: Vec<(BlockPos, BlockUndo)>,
    pub spawned_drops: Vec<EntityRef>,
    pub damages: Vec<(EntityRef, u32)>,
}

#[must_use]
pub fn transactional_explosion_undo(
    update: &TransactionalExplosionUpdate,
) -> TransactionalExplosionUndo {
    TransactionalExplosionUndo {
        blocks: update
            .edits
            .iter()
            .map(|edit| (edit.pos, make_delta_undo(edit.old_state)))
            .collect(),
        spawned_drops: update.drops.iter().map(|drop| drop.entity).collect(),
        damages: update
            .damages
            .iter()
            .map(|damage| (damage.target, damage.expected_health_milli))
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RandomTickDelta {
    pub holder: ServerId,
    pub chunk: ChunkAddr,
    pub tick: TickStamp,
    pub edits: Vec<BlockDelta>,
}

impl RandomTickDelta {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn chunk(&self) -> ChunkAddr {
        self.chunk
    }

    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.tick
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.edits.len()
    }

    pub fn normalize(&mut self) {
        normalize_edits(&mut self.edits);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedstoneFallibleUpdate {
    pub holder: ServerId,
    pub chunk: ChunkAddr,
    pub tick: TickStamp,
    pub trigger: BlockPos,
    pub edits: Vec<BlockDelta>,
}

impl RedstoneFallibleUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn chunk(&self) -> ChunkAddr {
        self.chunk
    }

    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.tick
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.edits.len()
    }

    pub fn normalize(&mut self) {
        normalize_edits(&mut self.edits);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplosionFallibleUpdate {
    pub holder: ServerId,
    pub chunk: ChunkAddr,
    pub tick: TickStamp,
    pub center: BlockPos,
    pub edits: Vec<BlockDelta>,
}

impl ExplosionFallibleUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn chunk(&self) -> ChunkAddr {
        self.chunk
    }

    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.tick
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.edits.len()
    }

    pub fn normalize(&mut self) {
        normalize_edits(&mut self.edits);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldDelta {
    RandomTick(RandomTickDelta),
    Redstone(RedstoneFallibleUpdate),
    Explosion(ExplosionFallibleUpdate),
    CausalRedstone(CausalRedstoneUpdate),
    TransactionalExplosion(TransactionalExplosionUpdate),
}

impl WorldDelta {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::RandomTick(_) => WORLD_DELTA_TAG_RANDOM_TICK,
            Self::Redstone(_) => WORLD_DELTA_TAG_REDSTONE,
            Self::Explosion(_) => WORLD_DELTA_TAG_EXPLOSION,
            Self::CausalRedstone(_) => WORLD_DELTA_TAG_CAUSAL_REDSTONE,
            Self::TransactionalExplosion(_) => WORLD_DELTA_TAG_TRANSACTIONAL_EXPLOSION,
        }
    }

    #[must_use]
    pub fn chunk(&self) -> ChunkAddr {
        match self {
            Self::RandomTick(update) => update.chunk,
            Self::Redstone(update) => update.chunk,
            Self::Explosion(update) => update.chunk,
            Self::CausalRedstone(update) => update.chunk,
            Self::TransactionalExplosion(update) => update.chunk,
        }
    }

    #[must_use]
    pub fn tick(&self) -> TickStamp {
        match self {
            Self::RandomTick(update) => update.tick,
            Self::Redstone(update) => update.tick,
            Self::Explosion(update) => update.tick,
            Self::CausalRedstone(update) => update.tick(),
            Self::TransactionalExplosion(update) => update.tick(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldDeltaCodecError {
    pub message: String,
}

impl core::fmt::Display for WorldDeltaCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorldDeltaCodecError {}

fn codec_error(context: &str, error: postcard::Error) -> WorldDeltaCodecError {
    WorldDeltaCodecError {
        message: format!("{context}: {error}"),
    }
}

fn frame_error(message: &str) -> WorldDeltaCodecError {
    WorldDeltaCodecError {
        message: message.to_string(),
    }
}

#[must_use]
pub fn chunk_of_block(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr {
        x: x.div_euclid(16),
        z: z.div_euclid(16),
    }
}

pub fn normalize_edits(edits: &mut Vec<BlockDelta>) {
    edits.retain(|edit| edit.old_state != edit.new_state);
    edits.sort_by(|left, right| {
        (left.pos.x, left.pos.y, left.pos.z, left.old_state, left.new_state).cmp(&(
            right.pos.x,
            right.pos.y,
            right.pos.z,
            right.old_state,
            right.new_state,
        ))
    });
    edits.dedup_by_key(|edit| edit.pos);
    edits.truncate(WORLD_DELTA_MAX_EDITS);
}

#[must_use]
pub fn capture_random_tick_delta(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    mut edits: Vec<BlockDelta>,
) -> Option<RandomTickDelta> {
    normalize_edits(&mut edits);
    if edits.is_empty() {
        None
    } else {
        Some(RandomTickDelta {
            holder,
            chunk,
            tick,
            edits,
        })
    }
}

#[must_use]
pub fn capture_redstone_update(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    trigger: BlockPos,
    mut edits: Vec<BlockDelta>,
) -> Option<RedstoneFallibleUpdate> {
    normalize_edits(&mut edits);
    if edits.is_empty() {
        None
    } else {
        Some(RedstoneFallibleUpdate {
            holder,
            chunk,
            tick,
            trigger,
            edits,
        })
    }
}

#[must_use]
pub fn capture_explosion_update(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    center: BlockPos,
    mut edits: Vec<BlockDelta>,
) -> Option<ExplosionFallibleUpdate> {
    normalize_edits(&mut edits);
    if edits.is_empty() {
        None
    } else {
        Some(ExplosionFallibleUpdate {
            holder,
            chunk,
            tick,
            center,
            edits,
        })
    }
}

#[must_use]
pub fn capture_causal_redstone_update(
    group: WorldActionGroup,
    chunk: ChunkAddr,
    mut edits: Vec<BlockDelta>,
    mut dependencies: Vec<WorldDependency>,
) -> Option<CausalRedstoneUpdate> {
    normalize_edits(&mut edits);
    dependencies.retain(|dependency| dependency.is_valid());
    dependencies.sort_by(|left, right| {
        (
            left.fire_tick.0,
            left.target.x,
            left.target.y,
            left.target.z,
            left.kind as u8,
        )
            .cmp(&(
                right.fire_tick.0,
                right.target.x,
                right.target.y,
                right.target.z,
                right.kind as u8,
            ))
    });
    dependencies.dedup_by_key(|dependency| (dependency.kind as u8, dependency.target, dependency.fire_tick));
    let update = CausalRedstoneUpdate {
        group,
        chunk,
        edits,
        dependencies,
    };
    update.is_valid().then_some(update)
}

#[must_use]
pub fn capture_transactional_explosion_update(
    group: WorldActionGroup,
    chunk: ChunkAddr,
    center: BlockPos,
    mut edits: Vec<BlockDelta>,
    mut drops: Vec<ExplosionDropRef>,
    mut damages: Vec<ExplosionEntityDamageRef>,
) -> Option<TransactionalExplosionUpdate> {
    normalize_edits(&mut edits);
    drops.sort_by_key(|drop| (drop.entity.owner, drop.entity.local_id));
    drops.dedup_by_key(|drop| drop.entity);
    damages.sort_by_key(|damage| (damage.target.owner, damage.target.local_id));
    damages.dedup_by_key(|damage| damage.target);
    let update = TransactionalExplosionUpdate {
        group,
        chunk,
        center,
        edits,
        drops,
        damages,
    };
    update.is_valid().then_some(update)
}

pub fn encode_random_tick(update: &RandomTickDelta) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_allocvec(update).map_err(|error| codec_error("encode random tick", error))
}

pub fn decode_random_tick(bytes: &[u8]) -> Result<RandomTickDelta, WorldDeltaCodecError> {
    postcard::from_bytes(bytes).map_err(|error| codec_error("decode random tick", error))
}

pub fn decode_random_tick_prefix(
    bytes: &[u8],
) -> Result<(RandomTickDelta, &[u8]), WorldDeltaCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| codec_error("decode random tick", error))
}

pub fn encode_random_tick_into(
    update: &RandomTickDelta,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_extend(update, out).map_err(|error| codec_error("encode random tick", error))
}

pub fn encode_random_tick_to_slice<'out>(
    update: &RandomTickDelta,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], WorldDeltaCodecError> {
    postcard::to_slice(update, out).map_err(|error| codec_error("encode random tick", error))
}

pub fn encoded_random_tick_len(update: &RandomTickDelta) -> Result<usize, WorldDeltaCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| codec_error("size random tick", error))
}

pub fn encode_redstone(update: &RedstoneFallibleUpdate) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_allocvec(update).map_err(|error| codec_error("encode redstone", error))
}

pub fn decode_redstone(bytes: &[u8]) -> Result<RedstoneFallibleUpdate, WorldDeltaCodecError> {
    postcard::from_bytes(bytes).map_err(|error| codec_error("decode redstone", error))
}

pub fn decode_redstone_prefix(
    bytes: &[u8],
) -> Result<(RedstoneFallibleUpdate, &[u8]), WorldDeltaCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| codec_error("decode redstone", error))
}

pub fn encode_redstone_into(
    update: &RedstoneFallibleUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_extend(update, out).map_err(|error| codec_error("encode redstone", error))
}

pub fn encode_redstone_to_slice<'out>(
    update: &RedstoneFallibleUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], WorldDeltaCodecError> {
    postcard::to_slice(update, out).map_err(|error| codec_error("encode redstone", error))
}

pub fn encoded_redstone_len(update: &RedstoneFallibleUpdate) -> Result<usize, WorldDeltaCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| codec_error("size redstone", error))
}

pub fn encode_explosion(update: &ExplosionFallibleUpdate) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_allocvec(update).map_err(|error| codec_error("encode explosion", error))
}

pub fn decode_explosion(bytes: &[u8]) -> Result<ExplosionFallibleUpdate, WorldDeltaCodecError> {
    postcard::from_bytes(bytes).map_err(|error| codec_error("decode explosion", error))
}

pub fn decode_explosion_prefix(
    bytes: &[u8],
) -> Result<(ExplosionFallibleUpdate, &[u8]), WorldDeltaCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| codec_error("decode explosion", error))
}

pub fn encode_explosion_into(
    update: &ExplosionFallibleUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_extend(update, out).map_err(|error| codec_error("encode explosion", error))
}

pub fn encode_explosion_to_slice<'out>(
    update: &ExplosionFallibleUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], WorldDeltaCodecError> {
    postcard::to_slice(update, out).map_err(|error| codec_error("encode explosion", error))
}

pub fn encoded_explosion_len(
    update: &ExplosionFallibleUpdate,
) -> Result<usize, WorldDeltaCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| codec_error("size explosion", error))
}

pub fn encode_causal_redstone(
    update: &CausalRedstoneUpdate,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_allocvec(update).map_err(|error| codec_error("encode causal redstone", error))
}

pub fn decode_causal_redstone(
    bytes: &[u8],
) -> Result<CausalRedstoneUpdate, WorldDeltaCodecError> {
    postcard::from_bytes(bytes).map_err(|error| codec_error("decode causal redstone", error))
}

pub fn decode_causal_redstone_prefix(
    bytes: &[u8],
) -> Result<(CausalRedstoneUpdate, &[u8]), WorldDeltaCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| codec_error("decode causal redstone", error))
}

pub fn encode_causal_redstone_into(
    update: &CausalRedstoneUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_extend(update, out).map_err(|error| codec_error("encode causal redstone", error))
}

pub fn encode_causal_redstone_to_slice<'out>(
    update: &CausalRedstoneUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], WorldDeltaCodecError> {
    postcard::to_slice(update, out).map_err(|error| codec_error("encode causal redstone", error))
}

pub fn encoded_causal_redstone_len(
    update: &CausalRedstoneUpdate,
) -> Result<usize, WorldDeltaCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| codec_error("size causal redstone", error))
}

pub fn encode_transactional_explosion(
    update: &TransactionalExplosionUpdate,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_allocvec(update)
        .map_err(|error| codec_error("encode transactional explosion", error))
}

pub fn decode_transactional_explosion(
    bytes: &[u8],
) -> Result<TransactionalExplosionUpdate, WorldDeltaCodecError> {
    postcard::from_bytes(bytes)
        .map_err(|error| codec_error("decode transactional explosion", error))
}

pub fn decode_transactional_explosion_prefix(
    bytes: &[u8],
) -> Result<(TransactionalExplosionUpdate, &[u8]), WorldDeltaCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| codec_error("decode transactional explosion", error))
}

pub fn encode_transactional_explosion_into(
    update: &TransactionalExplosionUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| codec_error("encode transactional explosion", error))
}

pub fn encode_transactional_explosion_to_slice<'out>(
    update: &TransactionalExplosionUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], WorldDeltaCodecError> {
    postcard::to_slice(update, out)
        .map_err(|error| codec_error("encode transactional explosion", error))
}

pub fn encoded_transactional_explosion_len(
    update: &TransactionalExplosionUpdate,
) -> Result<usize, WorldDeltaCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| codec_error("size transactional explosion", error))
}

#[must_use]
pub fn is_world_delta_frame(bytes: &[u8]) -> bool {
    bytes.len() > WORLD_DELTA_FRAME_PREFIX && bytes.starts_with(&WORLD_DELTA_MAGIC[..])
}

fn push_frame_tag(out: Vec<u8>, tag: u8) -> Vec<u8> {
    let mut framed = Vec::with_capacity(out.len().saturating_add(WORLD_DELTA_FRAME_PREFIX));
    framed.extend_from_slice(&WORLD_DELTA_MAGIC);
    framed.push(tag);
    framed.extend_from_slice(&out);
    framed
}

pub fn encode_frame(update: &WorldDelta) -> Result<Vec<u8>, WorldDeltaCodecError> {
    let (tag, body) = match update {
        WorldDelta::RandomTick(inner) => (
            WORLD_DELTA_TAG_RANDOM_TICK,
            postcard::to_allocvec(inner).map_err(|error| codec_error("encode frame", error))?,
        ),
        WorldDelta::Redstone(inner) => (
            WORLD_DELTA_TAG_REDSTONE,
            postcard::to_allocvec(inner).map_err(|error| codec_error("encode frame", error))?,
        ),
        WorldDelta::Explosion(inner) => (
            WORLD_DELTA_TAG_EXPLOSION,
            postcard::to_allocvec(inner).map_err(|error| codec_error("encode frame", error))?,
        ),
        WorldDelta::CausalRedstone(inner) => (
            WORLD_DELTA_TAG_CAUSAL_REDSTONE,
            postcard::to_allocvec(inner).map_err(|error| codec_error("encode frame", error))?,
        ),
        WorldDelta::TransactionalExplosion(inner) => (
            WORLD_DELTA_TAG_TRANSACTIONAL_EXPLOSION,
            postcard::to_allocvec(inner).map_err(|error| codec_error("encode frame", error))?,
        ),
    };
    Ok(push_frame_tag(body, tag))
}

pub fn encode_frame_into(
    update: &WorldDelta,
    out: Vec<u8>,
) -> Result<Vec<u8>, WorldDeltaCodecError> {
    let bytes = encode_frame(update)?;
    let mut out = out;
    out.extend_from_slice(&bytes);
    Ok(out)
}

pub fn encoded_frame_len(update: &WorldDelta) -> Result<usize, WorldDeltaCodecError> {
    let body = match update {
        WorldDelta::RandomTick(inner) => encoded_random_tick_len(inner)?,
        WorldDelta::Redstone(inner) => encoded_redstone_len(inner)?,
        WorldDelta::Explosion(inner) => encoded_explosion_len(inner)?,
        WorldDelta::CausalRedstone(inner) => encoded_causal_redstone_len(inner)?,
        WorldDelta::TransactionalExplosion(inner) => encoded_transactional_explosion_len(inner)?,
    };
    Ok(WORLD_DELTA_FRAME_PREFIX.saturating_add(body))
}

pub fn decode_frame(bytes: &[u8]) -> Result<WorldDelta, WorldDeltaCodecError> {
    if !is_world_delta_frame(bytes) {
        return Err(frame_error("decode frame: missing world delta magic"));
    }
    let tag = bytes[WORLD_DELTA_MAGIC.len()];
    let body = &bytes[WORLD_DELTA_FRAME_PREFIX..];
    match tag {
        WORLD_DELTA_TAG_RANDOM_TICK => decode_random_tick(body).map(WorldDelta::RandomTick),
        WORLD_DELTA_TAG_REDSTONE => decode_redstone(body).map(WorldDelta::Redstone),
        WORLD_DELTA_TAG_EXPLOSION => decode_explosion(body).map(WorldDelta::Explosion),
        WORLD_DELTA_TAG_CAUSAL_REDSTONE => decode_causal_redstone(body).map(WorldDelta::CausalRedstone),
        WORLD_DELTA_TAG_TRANSACTIONAL_EXPLOSION => {
            decode_transactional_explosion(body).map(WorldDelta::TransactionalExplosion)
        }
        _ => Err(frame_error("decode frame: unknown world delta tag")),
    }
}

pub fn decode_frame_prefix(bytes: &[u8]) -> Result<(WorldDelta, &[u8]), WorldDeltaCodecError> {
    if !is_world_delta_frame(bytes) {
        return Err(frame_error("decode frame: missing world delta magic"));
    }
    let tag = bytes[WORLD_DELTA_MAGIC.len()];
    let body = &bytes[WORLD_DELTA_FRAME_PREFIX..];
    match tag {
        WORLD_DELTA_TAG_RANDOM_TICK => decode_random_tick_prefix(body)
            .map(|(update, rest)| (WorldDelta::RandomTick(update), rest)),
        WORLD_DELTA_TAG_REDSTONE => decode_redstone_prefix(body)
            .map(|(update, rest)| (WorldDelta::Redstone(update), rest)),
        WORLD_DELTA_TAG_EXPLOSION => decode_explosion_prefix(body)
            .map(|(update, rest)| (WorldDelta::Explosion(update), rest)),
        WORLD_DELTA_TAG_CAUSAL_REDSTONE => decode_causal_redstone_prefix(body)
            .map(|(update, rest)| (WorldDelta::CausalRedstone(update), rest)),
        WORLD_DELTA_TAG_TRANSACTIONAL_EXPLOSION => decode_transactional_explosion_prefix(body)
            .map(|(update, rest)| (WorldDelta::TransactionalExplosion(update), rest)),
        _ => Err(frame_error("decode frame: unknown world delta tag")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RandomTickDecision {
    Converged,
    Apply { undo: BlockUndo },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallibleDecision {
    Apply { undo: BlockUndo },
    Conflict { expected: u16, current: u16 },
}

#[must_use]
pub const fn make_delta_undo(current_state: u16) -> BlockUndo {
    BlockUndo {
        old_state: current_state,
        count_before: WORLD_DELTA_NO_INVENTORY,
    }
}

#[must_use]
pub fn apply_random_tick_edit(current_state: u16, edit: &BlockDelta) -> RandomTickDecision {
    if current_state == edit.new_state {
        RandomTickDecision::Converged
    } else {
        RandomTickDecision::Apply {
            undo: make_delta_undo(current_state),
        }
    }
}

#[must_use]
pub fn apply_fallible_edit(current_state: u16, edit: &BlockDelta) -> FallibleDecision {
    if current_state == edit.old_state {
        FallibleDecision::Apply {
            undo: make_delta_undo(current_state),
        }
    } else {
        FallibleDecision::Conflict {
            expected: edit.old_state,
            current: current_state,
        }
    }
}

pub fn apply_random_tick_to_map(
    states: &mut HashMap<BlockPos, u16>,
    update: &RandomTickDelta,
) -> Vec<(BlockPos, BlockUndo)> {
    let mut undos = Vec::with_capacity(update.edits.len());
    for edit in &update.edits {
        let current = states.get(&edit.pos).copied().unwrap_or(edit.old_state);
        if let RandomTickDecision::Apply { undo } = apply_random_tick_edit(current, edit) {
            states.insert(edit.pos, edit.new_state);
            undos.push((edit.pos, undo));
        }
    }
    undos
}

pub fn apply_fallible_to_map(
    states: &mut HashMap<BlockPos, u16>,
    edits: &[BlockDelta],
) -> (Vec<(BlockPos, BlockUndo)>, Vec<(BlockPos, u16, u16)>) {
    let mut applied = Vec::new();
    let mut conflicts = Vec::new();
    for edit in edits {
        let current = states.get(&edit.pos).copied().unwrap_or(edit.old_state);
        match apply_fallible_edit(current, edit) {
            FallibleDecision::Apply { undo } => {
                states.insert(edit.pos, edit.new_state);
                applied.push((edit.pos, undo));
            }
            FallibleDecision::Conflict { expected, current } => {
                conflicts.push((edit.pos, expected, current));
            }
        }
    }
    (applied, conflicts)
}

pub fn revert_undos_to_map(
    states: &mut HashMap<BlockPos, u16>,
    undos: &[(BlockPos, BlockUndo)],
) {
    for (pos, undo) in undos {
        states.insert(*pos, undo.old_state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FallibleClaim {
    pub actor: ActionActor,
    pub seq: ActionSeq,
    pub tick: TickStamp,
    pub trigger: BlockPos,
    pub pos: BlockPos,
    pub undo: BlockUndo,
    pub new_state: u16,
}

#[must_use]
pub fn elect_fallible_claim(
    cluster_seed: u64,
    left: &FallibleClaim,
    right: &FallibleClaim,
) -> FallibleClaim {
    match order_action_actors(
        cluster_seed,
        left.tick,
        left.actor,
        right.actor,
    ) {
        Ordering::Less => *left,
        Ordering::Greater => *right,
        Ordering::Equal => {
            let by_seq = left.seq.cmp(&right.seq);
            if by_seq != Ordering::Equal {
                return if by_seq == Ordering::Less { *left } else { *right };
            }
            let left_key = (
                (left.trigger.x, left.trigger.y, left.trigger.z),
                left.new_state,
                (left.pos.x, left.pos.y, left.pos.z),
                left.undo.old_state,
            );
            let right_key = (
                (right.trigger.x, right.trigger.y, right.trigger.z),
                right.new_state,
                (right.pos.x, right.pos.y, right.pos.z),
                right.undo.old_state,
            );
            if left_key <= right_key {
                *left
            } else {
                *right
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct WorldDeltaAcceptance {
    table: BucketTable,
}

impl WorldDeltaAcceptance {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn table(&self) -> &BucketTable {
        &self.table
    }

    pub fn require(&mut self, tick: TickStamp, chunk: ChunkAddr, holders: &[u16]) {
        self.table.require(tick, chunk, holders);
    }

    pub fn accept(&mut self, tick: TickStamp, chunk: ChunkAddr, peer: u16, new_holders: &[u16]) {
        self.table.accept(tick, chunk, peer, new_holders);
    }

    #[must_use]
    pub fn is_complete(&self, tick: TickStamp) -> bool {
        self.table.is_tick_complete(tick)
    }

    #[must_use]
    pub fn missing(&self, tick: TickStamp, chunk: ChunkAddr) -> Vec<u16> {
        self.table.missing(tick, chunk)
    }

    pub fn remove_tick(&mut self, tick: TickStamp) -> bool {
        self.table.remove_tick(tick)
    }

    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.table.pending_ticks()
    }
}

#[must_use]
pub fn loser_revert_plan(losers: &[(BlockPos, BlockUndo)]) -> ReconcilePlan {
    ReconcilePlan::from_loser_undos(losers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(id: u16) -> ServerId {
        ServerId(id)
    }

    fn pos(x: i32, y: i32, z: i32) -> BlockPos {
        BlockPos { x, y, z }
    }

    fn edit(x: i32, y: i32, z: i32, old_state: u16, new_state: u16) -> BlockDelta {
        BlockDelta {
            pos: pos(x, y, z),
            old_state,
            new_state,
        }
    }

    fn sample_random_tick() -> RandomTickDelta {
        RandomTickDelta {
            holder: holder(2),
            chunk: ChunkAddr { x: 0, z: 0 },
            tick: TickStamp(41),
            edits: vec![edit(1, 64, 1, 7, 8), edit(2, 64, 3, 9, 10)],
        }
    }

    fn sample_redstone() -> RedstoneFallibleUpdate {
        RedstoneFallibleUpdate {
            holder: holder(3),
            chunk: ChunkAddr { x: 0, z: -1 },
            tick: TickStamp(42),
            trigger: pos(4, 64, -20),
            edits: vec![edit(4, 64, -21, 100, 115)],
        }
    }

    fn sample_explosion() -> ExplosionFallibleUpdate {
        ExplosionFallibleUpdate {
            holder: holder(5),
            chunk: ChunkAddr { x: 1, z: 1 },
            tick: TickStamp(43),
            center: pos(20, 65, 20),
            edits: vec![edit(20, 65, 21, 12, 0), edit(21, 65, 20, 13, 0)],
        }
    }

    fn entity(origin: u16, owner: u16, local_id: i32, chunk: ChunkAddr) -> EntityRef {
        EntityRef {
            origin: holder(origin),
            owner: holder(owner),
            local_id,
            chunk,
        }
    }

    fn causal_group() -> WorldActionGroup {
        WorldActionGroup::entity(
            holder(3),
            TickStamp(50),
            2,
            pos(4, 64, -20),
            entity(2, 3, 9, ChunkAddr { x: 0, z: -2 }),
        )
    }

    fn sample_causal_redstone() -> CausalRedstoneUpdate {
        let group = causal_group();
        capture_causal_redstone_update(
            group,
            ChunkAddr { x: 0, z: -2 },
            vec![edit(4, 64, -21, 100, 115)],
            vec![WorldDependency::new(
                group.reference,
                WorldDependentKind::Comparator,
                pos(5, 64, -21),
                2,
                120,
                121,
            )],
        )
        .unwrap()
    }

    fn sample_transactional_explosion() -> TransactionalExplosionUpdate {
        let group = causal_group();
        capture_transactional_explosion_update(
            group,
            ChunkAddr { x: 0, z: -2 },
            pos(4, 64, -20),
            vec![edit(4, 64, -21, 100, 0)],
            vec![ExplosionDropRef {
                entity: entity(3, 3, 10, ChunkAddr { x: 0, z: -2 }),
                source: pos(4, 64, -21),
                stack: InventoryStack {
                    item: 5,
                    count: 2,
                    nbt: vec![1],
                },
            }],
            vec![ExplosionEntityDamageRef {
                target: entity(8, 8, 12, ChunkAddr { x: 0, z: -2 }),
                expected_health_milli: 10_000,
                new_health_milli: 7_500,
            }],
        )
        .unwrap()
    }

    #[test]
    fn random_tick_roundtrips_all_forms() {
        let update = sample_random_tick();
        let bytes = encode_random_tick(&update).unwrap();
        assert_eq!(decode_random_tick(&bytes).unwrap(), update);
        let into_bytes = encode_random_tick_into(&update, Vec::new()).unwrap();
        assert_eq!(into_bytes, bytes);
        assert_eq!(encoded_random_tick_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_random_tick_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_random_tick_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
    }

    #[test]
    fn redstone_roundtrips_all_forms() {
        let update = sample_redstone();
        let bytes = encode_redstone(&update).unwrap();
        assert_eq!(decode_redstone(&bytes).unwrap(), update);
        let into_bytes = encode_redstone_into(&update, Vec::new()).unwrap();
        assert_eq!(into_bytes, bytes);
        assert_eq!(encoded_redstone_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_redstone_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_redstone_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
    }

    #[test]
    fn explosion_roundtrips_all_forms() {
        let update = sample_explosion();
        let bytes = encode_explosion(&update).unwrap();
        assert_eq!(decode_explosion(&bytes).unwrap(), update);
        let into_bytes = encode_explosion_into(&update, Vec::new()).unwrap();
        assert_eq!(into_bytes, bytes);
        assert_eq!(encoded_explosion_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_explosion_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_explosion_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
    }

    #[test]
    fn causal_redstone_roundtrips_and_keeps_dependency() {
        let update = sample_causal_redstone();
        let bytes = encode_causal_redstone(&update).unwrap();
        assert_eq!(decode_causal_redstone(&bytes).unwrap(), update);
        assert_eq!(encode_causal_redstone_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_causal_redstone_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_causal_redstone_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_causal_redstone_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
        assert_eq!(update.dependencies[0].fire_tick, TickStamp(52));
    }

    #[test]
    fn transactional_explosion_roundtrips_and_has_complete_undo() {
        let update = sample_transactional_explosion();
        let bytes = encode_transactional_explosion(&update).unwrap();
        assert_eq!(decode_transactional_explosion(&bytes).unwrap(), update);
        assert_eq!(encode_transactional_explosion_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_transactional_explosion_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_transactional_explosion_to_slice(&update, &mut slice)
            .unwrap()
            .len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_transactional_explosion_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
        let undo = transactional_explosion_undo(&update);
        assert_eq!(undo.blocks, vec![(pos(4, 64, -21), make_delta_undo(100))]);
        assert_eq!(undo.spawned_drops, vec![entity(3, 3, 10, ChunkAddr { x: 0, z: -2 })]);
        assert_eq!(undo.damages, vec![(entity(8, 8, 12, ChunkAddr { x: 0, z: -2 }), 10_000)]);
    }

    #[test]
    fn causal_owner_and_chunk_preconditions_reject_invalid_groups() {
        let group = WorldActionGroup::entity(
            holder(3),
            TickStamp(50),
            2,
            pos(4, 64, -20),
            entity(2, 4, 9, ChunkAddr { x: 0, z: -2 }),
        );
        assert!(capture_causal_redstone_update(
            group,
            ChunkAddr { x: 0, z: -2 },
            vec![edit(4, 64, -21, 100, 115)],
            Vec::new(),
        )
        .is_none());
        assert!(capture_transactional_explosion_update(
            causal_group(),
            ChunkAddr { x: 0, z: -2 },
            pos(4, 64, -20),
            vec![edit(20, 64, 20, 1, 0)],
            Vec::new(),
            Vec::new(),
        )
        .is_none());
    }

    #[test]
    fn frame_carries_every_kind() {
        for update in [
            WorldDelta::RandomTick(sample_random_tick()),
            WorldDelta::Redstone(sample_redstone()),
            WorldDelta::Explosion(sample_explosion()),
            WorldDelta::CausalRedstone(sample_causal_redstone()),
            WorldDelta::TransactionalExplosion(sample_transactional_explosion()),
        ] {
            let bytes = encode_frame(&update).unwrap();
            assert!(is_world_delta_frame(&bytes));
            assert_eq!(encoded_frame_len(&update).unwrap(), bytes.len());
            assert_eq!(decode_frame(&bytes).unwrap(), update);
            let appended = encode_frame_into(&update, vec![9_u8]).unwrap();
            assert_eq!(appended[0], 9);
            assert_eq!(&appended[1..], bytes);
            let (back, rest) = decode_frame_prefix(&bytes).unwrap();
            assert_eq!(back, update);
            assert!(rest.is_empty());
        }
    }

    #[test]
    fn frame_rejects_non_delta_and_unknown_tag() {
        assert!(!is_world_delta_frame(&[]));
        assert!(!is_world_delta_frame(&[0x57, 0x44]));
        assert!(!is_world_delta_frame(&encode_random_tick(&sample_random_tick()).unwrap()));
        assert!(decode_frame(&[]).is_err());
        let mut bad = encode_frame(&WorldDelta::RandomTick(sample_random_tick())).unwrap();
        bad[WORLD_DELTA_MAGIC.len()] = 0x7F;
        assert!(decode_frame(&bad).is_err());
        assert!(decode_frame_prefix(&bad).is_err());
    }

    #[test]
    fn chunk_of_block_handles_negatives() {
        assert_eq!(chunk_of_block(0, 0), ChunkAddr { x: 0, z: 0 });
        assert_eq!(chunk_of_block(15, 15), ChunkAddr { x: 0, z: 0 });
        assert_eq!(chunk_of_block(16, -1), ChunkAddr { x: 1, z: -1 });
        assert_eq!(chunk_of_block(-17, -17), ChunkAddr { x: -2, z: -2 });
    }

    #[test]
    fn capture_drops_noops_and_empty() {
        let chunk = ChunkAddr { x: 0, z: 0 };
        assert!(capture_random_tick_delta(holder(1), chunk, TickStamp(1), Vec::new()).is_none());
        assert!(
            capture_random_tick_delta(holder(1), chunk, TickStamp(1), vec![edit(0, 0, 0, 5, 5)]).is_none()
        );
        let kept = capture_random_tick_delta(
            holder(1),
            chunk,
            TickStamp(1),
            vec![edit(0, 0, 0, 5, 5), edit(1, 1, 1, 5, 6)],
        )
        .unwrap();
        assert_eq!(kept.edits.len(), 1);
        assert_eq!(kept.edits[0].new_state, 6);
        assert!(
            capture_redstone_update(holder(1), chunk, TickStamp(1), pos(0, 0, 0), Vec::new())
                .is_none()
        );
        assert!(
            capture_explosion_update(holder(1), chunk, TickStamp(1), pos(0, 0, 0), Vec::new())
                .is_none()
        );
    }

    #[test]
    fn capture_sorts_and_caps() {
        let chunk = ChunkAddr { x: 0, z: 0 };
        let mut edits = Vec::new();
        for index in 0..(WORLD_DELTA_MAX_EDITS + 40) {
            let index_u16 = u16::try_from(index).unwrap_or(u16::MAX);
            edits.push(edit(
                300 - i32::from(index_u16),
                64,
                0,
                index_u16,
                index_u16.saturating_add(1),
            ));
        }
        let kept =
            capture_random_tick_delta(holder(1), chunk, TickStamp(7), edits).expect("edits survive capture");
        assert_eq!(kept.len(), WORLD_DELTA_MAX_EDITS);
        let mut sorted = kept.edits.clone();
        sorted.sort_by(|left, right| {
            (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
        });
        assert_eq!(kept.edits, sorted);
    }

    #[test]
    fn random_tick_apply_converges_and_reverts() {
        let update = sample_random_tick();
        let mut states = HashMap::new();
        states.insert(pos(1, 64, 1), 7);
        states.insert(pos(2, 64, 3), 10);
        let undos = apply_random_tick_to_map(&mut states, &update);
        assert_eq!(undos.len(), 1);
        assert_eq!(states[&pos(1, 64, 1)], 8);
        assert_eq!(undos[0].1, make_delta_undo(7));
        revert_undos_to_map(&mut states, &undos);
        assert_eq!(states[&pos(1, 64, 1)], 7);
    }

    #[test]
    fn redstone_apply_accepts_expected_and_reverts_loss() {
        let update = sample_redstone();
        let mut states = HashMap::new();
        states.insert(pos(4, 64, -21), 100);
        let (applied, conflicts) = apply_fallible_to_map(&mut states, &update.edits);
        assert_eq!(applied.len(), 1);
        assert!(conflicts.is_empty());
        assert_eq!(states[&pos(4, 64, -21)], 115);
        revert_undos_to_map(&mut states, &applied);
        assert_eq!(states[&pos(4, 64, -21)], 100);

        states.insert(pos(4, 64, -21), 99);
        let (applied, conflicts) = apply_fallible_to_map(&mut states, &update.edits);
        assert!(applied.is_empty());
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0], (pos(4, 64, -21), 100, 99));
        assert_eq!(states[&pos(4, 64, -21)], 99);
    }

    #[test]
    fn explosion_apply_reverts_through_reconcile_plan() {
        let update = sample_explosion();
        let mut states = HashMap::new();
        states.insert(pos(20, 65, 21), 12);
        states.insert(pos(21, 65, 20), 13);
        let (applied, conflicts) = apply_fallible_to_map(&mut states, &update.edits);
        assert_eq!(applied.len(), 2);
        assert!(conflicts.is_empty());
        assert_eq!(states[&pos(20, 65, 21)], 0);
        let plan = loser_revert_plan(&applied);
        assert_eq!(plan.len(), 2);
        let mut restored = Vec::new();
        plan.apply_blocks(|block, old| restored.push((block, old)));
        for (block, old) in restored {
            states.insert(block, old);
        }
        assert_eq!(states[&pos(20, 65, 21)], 12);
        assert_eq!(states[&pos(21, 65, 20)], 13);
    }

    #[test]
    fn fallible_decision_names_conflict_evidence() {
        let target = edit(0, 64, 0, 11, 12);
        assert_eq!(
            apply_fallible_edit(11, &target),
            FallibleDecision::Apply {
                undo: make_delta_undo(11)
            }
        );
        assert_eq!(
            apply_fallible_edit(9, &target),
            FallibleDecision::Conflict {
                expected: 11,
                current: 9
            }
        );
        assert_eq!(
            apply_random_tick_edit(12, &target),
            RandomTickDecision::Converged
        );
        assert_eq!(
            apply_random_tick_edit(11, &target),
            RandomTickDecision::Apply {
                undo: make_delta_undo(11)
            }
        );
    }

    #[test]
    fn election_is_deterministic_across_orders() {
        let first = FallibleClaim {
            actor: ActionActor::Server(holder(2)),
            seq: ActionSeq(3),
            tick: TickStamp(9),
            trigger: pos(0, 64, 0),
            pos: pos(1, 64, 0),
            undo: make_delta_undo(7),
            new_state: 8,
        };
        let second = FallibleClaim {
            actor: ActionActor::Server(holder(1)),
            seq: ActionSeq(4),
            tick: TickStamp(9),
            trigger: pos(5, 64, 5),
            pos: pos(1, 64, 0),
            undo: make_delta_undo(7),
            new_state: 9,
        };
        let seed = 0x9E37_79B9_7F4A_7C15;
        let expected = match order_action_actors(
            seed,
            TickStamp(9),
            first.actor,
            second.actor,
        ) {
            Ordering::Less => first,
            Ordering::Greater => second,
            Ordering::Equal => {
                if (first.trigger.x, first.trigger.y, first.trigger.z, first.new_state)
                    <= (second.trigger.x, second.trigger.y, second.trigger.z, second.new_state)
                {
                    first
                } else {
                    second
                }
            }
        };
        assert_eq!(elect_fallible_claim(seed, &first, &second), expected);
        assert_eq!(elect_fallible_claim(seed, &second, &first), expected);
    }

    #[test]
    fn acceptance_tracks_holders_per_chunk_tick() {
        let mut acceptance = WorldDeltaAcceptance::new();
        let tick = TickStamp(12);
        let chunk = ChunkAddr { x: 3, z: -2 };
        acceptance.require(tick, chunk, &[1, 2]);
        assert!(!acceptance.is_complete(tick));
        assert_eq!(acceptance.missing(tick, chunk), vec![1, 2]);
        acceptance.accept(tick, chunk, 1, &[]);
        assert!(!acceptance.is_complete(tick));
        acceptance.accept(tick, chunk, 2, &[]);
        assert!(acceptance.is_complete(tick));
        assert_eq!(acceptance.pending_ticks(), 1);
        assert!(acceptance.remove_tick(tick));
        assert!(acceptance.is_complete(tick));
        assert!(acceptance.table().is_tick_complete(tick));
    }
}
