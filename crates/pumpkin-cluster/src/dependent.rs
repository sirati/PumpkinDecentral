use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::buckets::BucketTable;
use crate::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use crate::protocol::{BlockPos, BlockUndo, ChunkAddr, StreamKind};
use crate::reconcile::ReconcilePlan;
use crate::time::TickStamp;

pub const DEPENDENT_NO_INVENTORY: u8 = u8::MAX;
pub const DEPENDENT_KIND_COMPARATOR: u8 = 1;
pub const DEPENDENT_KIND_OBSERVER: u8 = 2;
pub const DEPENDENT_MAX_UPDATES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependentCause {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
}

impl DependentCause {
    #[must_use]
    pub const fn new(
        gid: GlobalPlayerId,
        seq: PlayerSeq,
        tick: TickStamp,
        pos: BlockPos,
    ) -> Self {
        Self {
            gid,
            seq,
            tick,
            pos,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparatorDependentUpdate {
    pub holder: ServerId,
    pub chunk: ChunkAddr,
    pub tick: TickStamp,
    pub target: BlockPos,
    pub cause: DependentCause,
    pub delay_ticks: u8,
    pub fire_tick: TickStamp,
    pub expected_old_state: u16,
    pub new_state: u16,
}

impl ComparatorDependentUpdate {
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
    pub const fn fire_tick(&self) -> TickStamp {
        self.fire_tick
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserverDependentUpdate {
    pub holder: ServerId,
    pub chunk: ChunkAddr,
    pub tick: TickStamp,
    pub target: BlockPos,
    pub cause: DependentCause,
    pub delay_ticks: u8,
    pub fire_tick: TickStamp,
    pub expected_old_state: u16,
    pub new_state: u16,
}

impl ObserverDependentUpdate {
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
    pub const fn fire_tick(&self) -> TickStamp {
        self.fire_tick
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependentUpdate {
    Comparator(ComparatorDependentUpdate),
    Observer(ObserverDependentUpdate),
}

impl DependentUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub fn chunk(&self) -> ChunkAddr {
        match *self {
            Self::Comparator(update) => update.chunk,
            Self::Observer(update) => update.chunk,
        }
    }

    #[must_use]
    pub fn tick(&self) -> TickStamp {
        match *self {
            Self::Comparator(update) => update.tick,
            Self::Observer(update) => update.tick,
        }
    }

    #[must_use]
    pub fn fire_tick(&self) -> TickStamp {
        match *self {
            Self::Comparator(update) => update.fire_tick,
            Self::Observer(update) => update.fire_tick,
        }
    }

    #[must_use]
    pub fn target(&self) -> BlockPos {
        match *self {
            Self::Comparator(update) => update.target,
            Self::Observer(update) => update.target,
        }
    }

    #[must_use]
    pub fn cause(&self) -> DependentCause {
        match *self {
            Self::Comparator(update) => update.cause,
            Self::Observer(update) => update.cause,
        }
    }

    #[must_use]
    pub fn expected_old_state(&self) -> u16 {
        match *self {
            Self::Comparator(update) => update.expected_old_state,
            Self::Observer(update) => update.expected_old_state,
        }
    }

    #[must_use]
    pub fn new_state(&self) -> u16 {
        match *self {
            Self::Comparator(update) => update.new_state,
            Self::Observer(update) => update.new_state,
        }
    }

    #[must_use]
    pub fn kind_tag(&self) -> u8 {
        match *self {
            Self::Comparator(_) => DEPENDENT_KIND_COMPARATOR,
            Self::Observer(_) => DEPENDENT_KIND_OBSERVER,
        }
    }

    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.expected_old_state() == self.new_state()
    }

    #[must_use]
    pub fn is_due(&self, now: TickStamp) -> bool {
        is_dependent_due(self.fire_tick(), now)
    }
}

impl From<ComparatorDependentUpdate> for DependentUpdate {
    fn from(update: ComparatorDependentUpdate) -> Self {
        Self::Comparator(update)
    }
}

impl From<ObserverDependentUpdate> for DependentUpdate {
    fn from(update: ObserverDependentUpdate) -> Self {
        Self::Observer(update)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependentBatch {
    pub tick: TickStamp,
    pub comparators: Vec<ComparatorDependentUpdate>,
    pub observers: Vec<ObserverDependentUpdate>,
}

impl DependentBatch {
    #[must_use]
    pub fn new(tick: TickStamp) -> Self {
        Self {
            tick,
            comparators: Vec::new(),
            observers: Vec::new(),
        }
    }

    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.comparators.is_empty() && self.observers.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.comparators.len() + self.observers.len()
    }

    pub fn push(&mut self, update: DependentUpdate) {
        match update {
            DependentUpdate::Comparator(inner) => self.comparators.push(inner),
            DependentUpdate::Observer(inner) => self.observers.push(inner),
        }
    }

    pub fn push_comparator(&mut self, update: ComparatorDependentUpdate) {
        self.comparators.push(update);
    }

    pub fn push_observer(&mut self, update: ObserverDependentUpdate) {
        self.observers.push(update);
    }

    pub fn normalize(&mut self) {
        self.comparators.sort_by(|left, right| {
            (
                left.fire_tick.0,
                left.tick.0,
                left.target.x,
                left.target.y,
                left.target.z,
                left.new_state,
                left.cause.gid,
                left.cause.seq,
            )
                .cmp(&(
                    right.fire_tick.0,
                    right.tick.0,
                    right.target.x,
                    right.target.y,
                    right.target.z,
                    right.new_state,
                    right.cause.gid,
                    right.cause.seq,
                ))
        });
        self.comparators
            .dedup_by_key(|update| (update.target, update.fire_tick));
        self.comparators.truncate(DEPENDENT_MAX_UPDATES);
        self.observers.sort_by(|left, right| {
            (
                left.fire_tick.0,
                left.tick.0,
                left.target.x,
                left.target.y,
                left.target.z,
                left.new_state,
                left.cause.gid,
                left.cause.seq,
            )
                .cmp(&(
                    right.fire_tick.0,
                    right.tick.0,
                    right.target.x,
                    right.target.y,
                    right.target.z,
                    right.new_state,
                    right.cause.gid,
                    right.cause.seq,
                ))
        });
        self.observers
            .dedup_by_key(|update| (update.target, update.fire_tick));
        self.observers.truncate(DEPENDENT_MAX_UPDATES);
    }

    pub fn drain_due(&mut self, now: TickStamp) -> Vec<DependentUpdate> {
        let mut due = Vec::new();
        let mut kept_comparators = Vec::with_capacity(self.comparators.len());
        for update in self.comparators.drain(..) {
            if is_dependent_due(update.fire_tick, now) {
                due.push(DependentUpdate::Comparator(update));
            } else {
                kept_comparators.push(update);
            }
        }
        self.comparators = kept_comparators;
        let mut kept_observers = Vec::with_capacity(self.observers.len());
        for update in self.observers.drain(..) {
            if is_dependent_due(update.fire_tick, now) {
                due.push(DependentUpdate::Observer(update));
            } else {
                kept_observers.push(update);
            }
        }
        self.observers = kept_observers;
        sort_dependents(&mut due);
        due
    }
}

#[must_use]
pub fn split_dependent_batch(batch: &DependentBatch) -> Vec<DependentUpdate> {
    let mut out = Vec::with_capacity(batch.len());
    out.extend(
        batch
            .comparators
            .iter()
            .copied()
            .map(DependentUpdate::Comparator),
    );
    out.extend(
        batch
            .observers
            .iter()
            .copied()
            .map(DependentUpdate::Observer),
    );
    sort_dependents(&mut out);
    out
}

#[must_use]
pub fn join_dependent_batch(tick: TickStamp, updates: Vec<DependentUpdate>) -> DependentBatch {
    let mut batch = DependentBatch::new(tick);
    for update in updates {
        batch.push(update);
    }
    batch.normalize();
    batch
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedDependent {
    pub update: DependentUpdate,
    pub undo: BlockUndo,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CauseRevocation {
    pub dropped_pending: Vec<DependentUpdate>,
    pub revert: Vec<(BlockPos, BlockUndo)>,
}

impl CauseRevocation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dropped_pending.is_empty() && self.revert.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.dropped_pending.len() + self.revert.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentCodecError {
    pub message: String,
}

impl core::fmt::Display for DependentCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DependentCodecError {}

fn dependent_codec_error(context: &str, error: postcard::Error) -> DependentCodecError {
    DependentCodecError {
        message: format!("{context}: {error}"),
    }
}

#[must_use]
pub const fn chunk_of_block(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr {
        x: x.div_euclid(16),
        z: z.div_euclid(16),
    }
}

#[must_use]
pub fn fire_tick_for(cause_tick: TickStamp, delay_ticks: u8) -> TickStamp {
    TickStamp(cause_tick.0.wrapping_add(u16::from(delay_ticks)))
}

#[must_use]
pub fn is_dependent_due(fire_tick: TickStamp, now: TickStamp) -> bool {
    now.distance_since(fire_tick) < u16::MAX / 2
}

#[must_use]
pub fn capture_comparator(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    target: BlockPos,
    cause: DependentCause,
    delay_ticks: u8,
    expected_old_state: u16,
    new_state: u16,
) -> Option<ComparatorDependentUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(ComparatorDependentUpdate {
            holder,
            chunk,
            tick,
            target,
            cause,
            delay_ticks,
            fire_tick: fire_tick_for(tick, delay_ticks),
            expected_old_state,
            new_state,
        })
    }
}

#[must_use]
pub fn capture_observer(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    target: BlockPos,
    cause: DependentCause,
    delay_ticks: u8,
    expected_old_state: u16,
    new_state: u16,
) -> Option<ObserverDependentUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(ObserverDependentUpdate {
            holder,
            chunk,
            tick,
            target,
            cause,
            delay_ticks,
            fire_tick: fire_tick_for(tick, delay_ticks),
            expected_old_state,
            new_state,
        })
    }
}

pub fn sort_dependents(updates: &mut [DependentUpdate]) {
    updates.sort_by(|left, right| {
        (
            left.fire_tick().0,
            left.tick().0,
            left.target().x,
            left.target().y,
            left.target().z,
            left.kind_tag(),
            left.new_state(),
            left.cause().gid,
            left.cause().seq,
        )
            .cmp(&(
                right.fire_tick().0,
                right.tick().0,
                right.target().x,
                right.target().y,
                right.target().z,
                right.kind_tag(),
                right.new_state(),
                right.cause().gid,
                right.cause().seq,
            ))
    });
}

pub fn normalize_dependent_updates(updates: &mut Vec<DependentUpdate>) {
    updates.retain(|update| !update.is_noop());
    sort_dependents(updates);
    updates.dedup_by_key(|update| (update.target(), update.kind_tag(), update.fire_tick()));
    updates.truncate(DEPENDENT_MAX_UPDATES);
}

pub fn partition_due(
    updates: Vec<DependentUpdate>,
    now: TickStamp,
) -> (Vec<DependentUpdate>, Vec<DependentUpdate>) {
    let mut due = Vec::new();
    let mut pending = Vec::new();
    for update in updates {
        if update.is_due(now) {
            due.push(update);
        } else {
            pending.push(update);
        }
    }
    (due, pending)
}

pub fn encode_comparator(
    update: &ComparatorDependentUpdate,
) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_allocvec(update).map_err(|error| dependent_codec_error("encode comparator", error))
}

pub fn decode_comparator(bytes: &[u8]) -> Result<ComparatorDependentUpdate, DependentCodecError> {
    postcard::from_bytes(bytes).map_err(|error| dependent_codec_error("decode comparator", error))
}

pub fn decode_comparator_prefix(
    bytes: &[u8],
) -> Result<(ComparatorDependentUpdate, &[u8]), DependentCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| dependent_codec_error("decode comparator", error))
}

pub fn encode_comparator_into(
    update: &ComparatorDependentUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| dependent_codec_error("encode comparator", error))
}

pub fn encode_comparator_to_slice<'out>(
    update: &ComparatorDependentUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], DependentCodecError> {
    postcard::to_slice(update, out)
        .map_err(|error| dependent_codec_error("encode comparator", error))
}

pub fn encoded_comparator_len(
    update: &ComparatorDependentUpdate,
) -> Result<usize, DependentCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| dependent_codec_error("size comparator", error))
}

pub fn encode_observer(update: &ObserverDependentUpdate) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_allocvec(update).map_err(|error| dependent_codec_error("encode observer", error))
}

pub fn decode_observer(bytes: &[u8]) -> Result<ObserverDependentUpdate, DependentCodecError> {
    postcard::from_bytes(bytes).map_err(|error| dependent_codec_error("decode observer", error))
}

pub fn decode_observer_prefix(
    bytes: &[u8],
) -> Result<(ObserverDependentUpdate, &[u8]), DependentCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| dependent_codec_error("decode observer", error))
}

pub fn encode_observer_into(
    update: &ObserverDependentUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| dependent_codec_error("encode observer", error))
}

pub fn encode_observer_to_slice<'out>(
    update: &ObserverDependentUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], DependentCodecError> {
    postcard::to_slice(update, out)
        .map_err(|error| dependent_codec_error("encode observer", error))
}

pub fn encoded_observer_len(
    update: &ObserverDependentUpdate,
) -> Result<usize, DependentCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| dependent_codec_error("size observer", error))
}

pub fn encode_dependent(update: &DependentUpdate) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_allocvec(update).map_err(|error| dependent_codec_error("encode dependent", error))
}

pub fn decode_dependent(bytes: &[u8]) -> Result<DependentUpdate, DependentCodecError> {
    postcard::from_bytes(bytes).map_err(|error| dependent_codec_error("decode dependent", error))
}

pub fn decode_dependent_prefix(
    bytes: &[u8],
) -> Result<(DependentUpdate, &[u8]), DependentCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| dependent_codec_error("decode dependent", error))
}

pub fn encode_dependent_into(
    update: &DependentUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| dependent_codec_error("encode dependent", error))
}

pub fn encode_dependent_to_slice<'out>(
    update: &DependentUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], DependentCodecError> {
    postcard::to_slice(update, out)
        .map_err(|error| dependent_codec_error("encode dependent", error))
}

pub fn encoded_dependent_len(update: &DependentUpdate) -> Result<usize, DependentCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| dependent_codec_error("size dependent", error))
}

pub fn encode_dependent_batch(batch: &DependentBatch) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_allocvec(batch).map_err(|error| dependent_codec_error("encode batch", error))
}

pub fn decode_dependent_batch(bytes: &[u8]) -> Result<DependentBatch, DependentCodecError> {
    postcard::from_bytes(bytes).map_err(|error| dependent_codec_error("decode batch", error))
}

pub fn decode_dependent_batch_prefix(
    bytes: &[u8],
) -> Result<(DependentBatch, &[u8]), DependentCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| dependent_codec_error("decode batch", error))
}

pub fn encode_dependent_batch_into(
    batch: &DependentBatch,
    out: Vec<u8>,
) -> Result<Vec<u8>, DependentCodecError> {
    postcard::to_extend(batch, out).map_err(|error| dependent_codec_error("encode batch", error))
}

pub fn encode_dependent_batch_to_slice<'out>(
    batch: &DependentBatch,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], DependentCodecError> {
    postcard::to_slice(batch, out).map_err(|error| dependent_codec_error("encode batch", error))
}

pub fn encoded_dependent_batch_len(batch: &DependentBatch) -> Result<usize, DependentCodecError> {
    postcard::experimental::serialized_size(batch)
        .map_err(|error| dependent_codec_error("size batch", error))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependentDecision {
    Apply { undo: BlockUndo },
    Wait { fire_tick: TickStamp, now: TickStamp },
    Conflict { expected: u16, current: u16 },
}

#[must_use]
pub const fn make_dependent_undo(current_state: u16) -> BlockUndo {
    BlockUndo {
        old_state: current_state,
        count_before: DEPENDENT_NO_INVENTORY,
    }
}

#[must_use]
pub fn apply_dependent_edit(
    current_state: u16,
    update: &DependentUpdate,
    now: TickStamp,
) -> DependentDecision {
    if !update.is_due(now) {
        DependentDecision::Wait {
            fire_tick: update.fire_tick(),
            now,
        }
    } else if current_state == update.expected_old_state() {
        DependentDecision::Apply {
            undo: make_dependent_undo(current_state),
        }
    } else {
        DependentDecision::Conflict {
            expected: update.expected_old_state(),
            current: current_state,
        }
    }
}

#[derive(Debug, Default)]
pub struct DependentMetrics {
    applied: AtomicU64,
    waiting: AtomicU64,
    conflicted: AtomicU64,
}

impl DependentMetrics {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            applied: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
            conflicted: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn waiting(&self) -> u64 {
        self.waiting.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn conflicted(&self) -> u64 {
        self.conflicted.load(Ordering::Relaxed)
    }
}

#[must_use]
pub fn judge_dependent(
    current_state: u16,
    update: &DependentUpdate,
    now: TickStamp,
    metrics: &DependentMetrics,
) -> DependentDecision {
    let decision = apply_dependent_edit(current_state, update, now);
    match decision {
        DependentDecision::Apply { .. } => {
            metrics.applied.fetch_add(1, Ordering::Relaxed);
        }
        DependentDecision::Wait { .. } => {
            metrics.waiting.fetch_add(1, Ordering::Relaxed);
        }
        DependentDecision::Conflict { .. } => {
            metrics.conflicted.fetch_add(1, Ordering::Relaxed);
        }
    }
    decision
}

pub fn apply_dependents_to_map(
    states: &mut HashMap<BlockPos, u16>,
    updates: &[DependentUpdate],
    now: TickStamp,
) -> (
    Vec<AppliedDependent>,
    Vec<DependentUpdate>,
    Vec<(DependentUpdate, u16, u16)>,
) {
    let mut applied = Vec::new();
    let mut waiting = Vec::new();
    let mut conflicts = Vec::new();
    for update in updates {
        let current = states
            .get(&update.target())
            .copied()
            .unwrap_or(update.expected_old_state());
        match apply_dependent_edit(current, update, now) {
            DependentDecision::Apply { undo } => {
                states.insert(update.target(), update.new_state());
                applied.push(AppliedDependent {
                    update: *update,
                    undo,
                });
            }
            DependentDecision::Wait { .. } => {
                waiting.push(*update);
            }
            DependentDecision::Conflict { expected, current } => {
                conflicts.push((*update, expected, current));
            }
        }
    }
    (applied, waiting, conflicts)
}

pub fn revert_dependent_undos_to_map(
    states: &mut HashMap<BlockPos, u16>,
    undos: &[(BlockPos, BlockUndo)],
) {
    for (pos, undo) in undos {
        states.insert(*pos, undo.old_state);
    }
}

pub fn revoke_cause_for(
    pending: &mut Vec<DependentUpdate>,
    applied: &mut Vec<AppliedDependent>,
    cause: &DependentCause,
) -> CauseRevocation {
    let mut revocation = CauseRevocation::new();
    let mut kept_pending = Vec::with_capacity(pending.len());
    for update in pending.drain(..) {
        if update.cause() == *cause {
            revocation.dropped_pending.push(update);
        } else {
            kept_pending.push(update);
        }
    }
    *pending = kept_pending;
    let mut kept_applied = Vec::with_capacity(applied.len());
    for entry in applied.drain(..) {
        if entry.update.cause() == *cause {
            revocation.revert.push((entry.update.target(), entry.undo));
        } else {
            kept_applied.push(entry);
        }
    }
    *applied = kept_applied;
    revocation
}

#[must_use]
pub fn dependent_loser_revert_plan(losers: &[(BlockPos, BlockUndo)]) -> ReconcilePlan {
    ReconcilePlan::from_loser_undos(losers)
}

#[derive(Debug, Default)]
pub struct DependentAcceptance {
    table: BucketTable,
}

impl DependentAcceptance {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn pos(x: i32, y: i32, z: i32) -> BlockPos {
        BlockPos { x, y, z }
    }

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    fn cause() -> DependentCause {
        DependentCause::new(gid(1, 2), PlayerSeq(7), TickStamp(40), pos(0, 64, 0))
    }

    fn comparator() -> ComparatorDependentUpdate {
        capture_comparator(
            ServerId(1),
            chunk(0, 0),
            TickStamp(40),
            pos(1, 64, 0),
            cause(),
            2,
            100,
            115,
        )
        .unwrap()
    }

    fn observer() -> ObserverDependentUpdate {
        capture_observer(
            ServerId(1),
            chunk(0, 0),
            TickStamp(40),
            pos(2, 64, 0),
            cause(),
            1,
            200,
            201,
        )
        .unwrap()
    }

    #[test]
    fn capture_packs_cause_and_fire_tick() {
        let update = comparator();
        assert_eq!(update.holder, ServerId(1));
        assert_eq!(update.chunk, chunk(0, 0));
        assert_eq!(update.tick, TickStamp(40));
        assert_eq!(update.target, pos(1, 64, 0));
        assert_eq!(update.cause, cause());
        assert_eq!(update.delay_ticks, 2);
        assert_eq!(update.fire_tick, TickStamp(42));
        assert_eq!(fire_tick_for(TickStamp(40), 2), TickStamp(42));
        assert_eq!(fire_tick_for(TickStamp(u16::MAX), 1), TickStamp(0));
        assert_eq!(
            ComparatorDependentUpdate::stream_kind(),
            StreamKind::PlayerWorld
        );
        assert!(
            capture_comparator(
                ServerId(1),
                chunk(0, 0),
                TickStamp(40),
                pos(1, 64, 0),
                cause(),
                2,
                100,
                100
            )
            .is_none()
        );
        assert!(
            capture_observer(
                ServerId(1),
                chunk(0, 0),
                TickStamp(40),
                pos(2, 64, 0),
                cause(),
                1,
                200,
                200
            )
            .is_none()
        );
    }

    #[test]
    fn singles_roundtrip_all_forms() {
        let update = comparator();
        let bytes = encode_comparator(&update).unwrap();
        assert_eq!(decode_comparator(&bytes).unwrap(), update);
        assert_eq!(encode_comparator_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_comparator_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_comparator_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_comparator_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let update = observer();
        let bytes = encode_observer(&update).unwrap();
        assert_eq!(decode_observer(&bytes).unwrap(), update);
        assert_eq!(encode_observer_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_observer_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_observer_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_observer_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
    }

    #[test]
    fn enum_and_batch_roundtrip_all_forms() {
        let update = DependentUpdate::Observer(observer());
        assert_eq!(update.kind_tag(), DEPENDENT_KIND_OBSERVER);
        assert_eq!(update.target(), pos(2, 64, 0));
        assert_eq!(update.chunk(), chunk(0, 0));
        assert_eq!(update.tick(), TickStamp(40));
        assert_eq!(update.fire_tick(), TickStamp(41));
        assert_eq!(update.cause(), cause());
        assert_eq!(update.expected_old_state(), 200);
        assert_eq!(update.new_state(), 201);
        assert!(!update.is_noop());
        let bytes = encode_dependent(&update).unwrap();
        assert_eq!(decode_dependent(&bytes).unwrap(), update);
        assert_eq!(encode_dependent_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_dependent_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_dependent_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_dependent_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let mut batch = DependentBatch::new(TickStamp(40));
        assert!(batch.is_empty());
        batch.push(DependentUpdate::Comparator(comparator()));
        batch.push_observer(observer());
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.comparators.len(), 1);
        assert_eq!(batch.observers.len(), 1);
        assert_eq!(DependentBatch::stream_kind(), StreamKind::PlayerWorld);
        let bytes = encode_dependent_batch(&batch).unwrap();
        assert_eq!(decode_dependent_batch(&bytes).unwrap(), batch);
        assert_eq!(encode_dependent_batch_into(&batch, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_dependent_batch_len(&batch).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_dependent_batch_to_slice(&batch, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_dependent_batch_prefix(&bytes).unwrap();
        assert_eq!(back, batch);
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_comparator(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_dependent(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_dependent_batch(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn due_ordering_holds_across_ticks() {
        let early = DependentUpdate::Observer(observer());
        let late = DependentUpdate::Comparator(comparator());
        assert!(early.is_due(TickStamp(41)));
        assert!(!late.is_due(TickStamp(41)));
        assert!(late.is_due(TickStamp(42)));
        assert!(!early.is_due(TickStamp(40)));
        assert!(is_dependent_due(TickStamp(42), TickStamp(42)));
        assert!(!is_dependent_due(TickStamp(42), TickStamp(41)));

        let mut updates = vec![late, early];
        sort_dependents(&mut updates);
        assert_eq!(updates[0], early);
        assert_eq!(updates[1], late);
        let reversed = vec![early, late];
        let mut forward = reversed.clone();
        sort_dependents(&mut forward);
        assert_eq!(forward, updates);

        let (due, pending) = partition_due(vec![early, late], TickStamp(41));
        assert_eq!(due, vec![early]);
        assert_eq!(pending, vec![late]);
    }

    #[test]
    fn apply_waits_conflicts_and_applies() {
        let metrics = DependentMetrics::new();
        let late = DependentUpdate::Comparator(comparator());
        let early = DependentUpdate::Observer(observer());

        let decision = judge_dependent(200, &early, TickStamp(40), &metrics);
        assert_eq!(
            decision,
            DependentDecision::Wait {
                fire_tick: TickStamp(41),
                now: TickStamp(40)
            }
        );

        let mut states = HashMap::new();
        states.insert(pos(2, 64, 0), 200);
        states.insert(pos(1, 64, 0), 999);
        let (applied, waiting, conflicts) =
            apply_dependents_to_map(&mut states, &[early, late], TickStamp(42));
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].update, early);
        assert_eq!(applied[0].undo, make_dependent_undo(200));
        assert!(waiting.is_empty());
        assert_eq!(states[&pos(2, 64, 0)], 201);
        assert_eq!(states[&pos(1, 64, 0)], 999);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].1, 100);
        assert_eq!(conflicts[0].2, 999);
        assert_eq!(metrics.waiting(), 1);
        revert_dependent_undos_to_map(
            &mut states,
            &[(pos(2, 64, 0), make_dependent_undo(200))],
        );
        assert_eq!(states[&pos(2, 64, 0)], 200);
    }

    #[test]
    fn revoking_cause_drops_pending_and_reverts_applied() {
        let other_cause =
            DependentCause::new(gid(9, 9), PlayerSeq(1), TickStamp(40), pos(9, 64, 9));
        let mut other = observer();
        other.cause = other_cause;
        let other_update = DependentUpdate::Observer(other);

        let mut pending = vec![DependentUpdate::Comparator(comparator()), other_update];
        let mut applied = vec![
            AppliedDependent {
                update: DependentUpdate::Observer(observer()),
                undo: make_dependent_undo(200),
            },
            AppliedDependent {
                update: other_update,
                undo: make_dependent_undo(200),
            },
        ];
        let target = cause();
        let revocation = revoke_cause_for(&mut pending, &mut applied, &target);
        assert_eq!(revocation.dropped_pending.len(), 1);
        assert_eq!(revocation.revert.len(), 1);
        assert_eq!(revocation.revert[0].0, pos(2, 64, 0));
        assert_eq!(revocation.len(), 2);
        assert!(!revocation.is_empty());
        assert_eq!(pending, vec![other_update]);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].update, other_update);

        let mut states = HashMap::new();
        states.insert(pos(2, 64, 0), 201);
        revert_dependent_undos_to_map(&mut states, &revocation.revert);
        assert_eq!(states[&pos(2, 64, 0)], 200);
        let plan = dependent_loser_revert_plan(&revocation.revert);
        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn revoking_unknown_cause_keeps_everything() {
        let mut pending = vec![DependentUpdate::Observer(observer())];
        let mut applied = vec![AppliedDependent {
            update: DependentUpdate::Observer(observer()),
            undo: make_dependent_undo(200),
        }];
        let unknown =
            DependentCause::new(gid(8, 8), PlayerSeq(8), TickStamp(8), pos(8, 64, 8));
        let revocation = revoke_cause_for(&mut pending, &mut applied, &unknown);
        assert!(revocation.is_empty());
        assert_eq!(pending.len(), 1);
        assert_eq!(applied.len(), 1);
    }

    #[test]
    fn batch_normalize_orders_and_dedups() {
        let mut batch = DependentBatch::new(TickStamp(40));
        batch.push(DependentUpdate::Comparator(comparator()));
        batch.push_comparator(comparator());
        batch.push_observer(observer());
        batch.normalize();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.comparators.len(), 1);
        assert_eq!(batch.observers.len(), 1);
        assert_eq!(batch.observers[0].fire_tick, TickStamp(41));
        assert_eq!(batch.comparators[0].fire_tick, TickStamp(42));
    }

    #[test]
    fn split_join_and_drain_due_roundtrip() {
        let mut batch = DependentBatch::new(TickStamp(40));
        batch.push_comparator(comparator());
        batch.push_observer(observer());
        let unified = split_dependent_batch(&batch);
        assert_eq!(unified.len(), 2);
        assert_eq!(unified[0].fire_tick(), TickStamp(41));
        assert_eq!(unified[1].fire_tick(), TickStamp(42));
        let joined = join_dependent_batch(TickStamp(40), unified);
        assert_eq!(joined, batch);

        let due = batch.drain_due(TickStamp(41));
        assert_eq!(due, vec![DependentUpdate::Observer(observer())]);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.comparators.len(), 1);
        let due = batch.drain_due(TickStamp(42));
        assert_eq!(due, vec![DependentUpdate::Comparator(comparator())]);
        assert!(batch.is_empty());
    }

    #[test]
    fn acceptance_gates_tick_completion() {
        let mut acceptance = DependentAcceptance::new();
        let tick = TickStamp(40);
        let target = chunk(0, 0);
        acceptance.require(tick, target, &[3]);
        assert!(!acceptance.is_complete(tick));
        assert_eq!(acceptance.missing(tick, target), vec![3]);
        acceptance.accept(tick, target, 3, &[]);
        assert!(acceptance.is_complete(tick));
        assert_eq!(acceptance.pending_ticks(), 1);
        assert!(acceptance.remove_tick(tick));
    }

    #[test]
    fn chunk_of_block_handles_negatives() {
        assert_eq!(chunk_of_block(0, 0), chunk(0, 0));
        assert_eq!(chunk_of_block(15, 15), chunk(0, 0));
        assert_eq!(chunk_of_block(16, -1), chunk(1, -1));
        assert_eq!(chunk_of_block(-17, -17), chunk(-2, -2));
    }
}
