use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::buckets::BucketTable;
use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::order::order_players;
use crate::protocol::{BlockPos, BlockUndo, ChunkAddr, StreamKind};
use crate::reconcile::ReconcilePlan;
use crate::time::TickStamp;

pub const INTERACT_NO_INVENTORY: u8 = u8::MAX;
pub const INTERACT_KIND_DOOR: u8 = 1;
pub const INTERACT_KIND_TRAPDOOR: u8 = 2;
pub const INTERACT_KIND_END_EYE: u8 = 3;
pub const INTERACT_KIND_ANCHOR: u8 = 4;
pub const INTERACT_MAX_UPDATES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoorToggleUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub new_state: u16,
    pub chunk: ChunkAddr,
}

impl DoorToggleUpdate {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrapdoorToggleUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub new_state: u16,
    pub chunk: ChunkAddr,
}

impl TrapdoorToggleUpdate {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndEyePlaceUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub new_state: u16,
    pub chunk: ChunkAddr,
}

impl EndEyePlaceUpdate {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorChargeUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub new_state: u16,
    pub charges_after: u8,
    pub chunk: ChunkAddr,
}

impl AnchorChargeUpdate {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InteractUpdate {
    Door(DoorToggleUpdate),
    Trapdoor(TrapdoorToggleUpdate),
    EndEye(EndEyePlaceUpdate),
    Anchor(AnchorChargeUpdate),
}

impl InteractUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub fn chunk(&self) -> ChunkAddr {
        match *self {
            Self::Door(update) => update.chunk,
            Self::Trapdoor(update) => update.chunk,
            Self::EndEye(update) => update.chunk,
            Self::Anchor(update) => update.chunk,
        }
    }

    #[must_use]
    pub fn tick(&self) -> TickStamp {
        match *self {
            Self::Door(update) => update.tick,
            Self::Trapdoor(update) => update.tick,
            Self::EndEye(update) => update.tick,
            Self::Anchor(update) => update.tick,
        }
    }

    #[must_use]
    pub fn gid(&self) -> GlobalPlayerId {
        match *self {
            Self::Door(update) => update.gid,
            Self::Trapdoor(update) => update.gid,
            Self::EndEye(update) => update.gid,
            Self::Anchor(update) => update.gid,
        }
    }

    #[must_use]
    pub fn pos(&self) -> BlockPos {
        match *self {
            Self::Door(update) => update.pos,
            Self::Trapdoor(update) => update.pos,
            Self::EndEye(update) => update.pos,
            Self::Anchor(update) => update.pos,
        }
    }

    #[must_use]
    pub fn expected_old_state(&self) -> u16 {
        match *self {
            Self::Door(update) => update.expected_old_state,
            Self::Trapdoor(update) => update.expected_old_state,
            Self::EndEye(update) => update.expected_old_state,
            Self::Anchor(update) => update.expected_old_state,
        }
    }

    #[must_use]
    pub fn new_state(&self) -> u16 {
        match *self {
            Self::Door(update) => update.new_state,
            Self::Trapdoor(update) => update.new_state,
            Self::EndEye(update) => update.new_state,
            Self::Anchor(update) => update.new_state,
        }
    }

    #[must_use]
    pub fn kind_tag(&self) -> u8 {
        match *self {
            Self::Door(_) => INTERACT_KIND_DOOR,
            Self::Trapdoor(_) => INTERACT_KIND_TRAPDOOR,
            Self::EndEye(_) => INTERACT_KIND_END_EYE,
            Self::Anchor(_) => INTERACT_KIND_ANCHOR,
        }
    }

    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.expected_old_state() == self.new_state()
    }
}

impl From<DoorToggleUpdate> for InteractUpdate {
    fn from(update: DoorToggleUpdate) -> Self {
        Self::Door(update)
    }
}

impl From<TrapdoorToggleUpdate> for InteractUpdate {
    fn from(update: TrapdoorToggleUpdate) -> Self {
        Self::Trapdoor(update)
    }
}

impl From<EndEyePlaceUpdate> for InteractUpdate {
    fn from(update: EndEyePlaceUpdate) -> Self {
        Self::EndEye(update)
    }
}

impl From<AnchorChargeUpdate> for InteractUpdate {
    fn from(update: AnchorChargeUpdate) -> Self {
        Self::Anchor(update)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractBatch {
    pub tick: TickStamp,
    pub doors: Vec<DoorToggleUpdate>,
    pub trapdoors: Vec<TrapdoorToggleUpdate>,
    pub end_eyes: Vec<EndEyePlaceUpdate>,
    pub anchors: Vec<AnchorChargeUpdate>,
}

impl InteractBatch {
    #[must_use]
    pub fn new(tick: TickStamp) -> Self {
        Self {
            tick,
            doors: Vec::new(),
            trapdoors: Vec::new(),
            end_eyes: Vec::new(),
            anchors: Vec::new(),
        }
    }

    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.doors.is_empty()
            && self.trapdoors.is_empty()
            && self.end_eyes.is_empty()
            && self.anchors.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.doors.len()
            + self.trapdoors.len()
            + self.end_eyes.len()
            + self.anchors.len()
    }

    pub fn push(&mut self, update: InteractUpdate) {
        match update {
            InteractUpdate::Door(inner) => self.doors.push(inner),
            InteractUpdate::Trapdoor(inner) => self.trapdoors.push(inner),
            InteractUpdate::EndEye(inner) => self.end_eyes.push(inner),
            InteractUpdate::Anchor(inner) => self.anchors.push(inner),
        }
    }

    pub fn push_door(&mut self, update: DoorToggleUpdate) {
        self.doors.push(update);
    }

    pub fn push_trapdoor(&mut self, update: TrapdoorToggleUpdate) {
        self.trapdoors.push(update);
    }

    pub fn push_end_eye(&mut self, update: EndEyePlaceUpdate) {
        self.end_eyes.push(update);
    }

    pub fn push_anchor(&mut self, update: AnchorChargeUpdate) {
        self.anchors.push(update);
    }

    pub fn normalize(&mut self) {
        self.doors.sort_by(|left, right| {
            (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
        });
        self.doors.dedup_by_key(|update| update.pos);
        self.doors.truncate(INTERACT_MAX_UPDATES);
        self.trapdoors.sort_by(|left, right| {
            (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
        });
        self.trapdoors.dedup_by_key(|update| update.pos);
        self.trapdoors.truncate(INTERACT_MAX_UPDATES);
        self.end_eyes.sort_by(|left, right| {
            (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
        });
        self.end_eyes.dedup_by_key(|update| update.pos);
        self.end_eyes.truncate(INTERACT_MAX_UPDATES);
        self.anchors.sort_by(|left, right| {
            (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
        });
        self.anchors.dedup_by_key(|update| update.pos);
        self.anchors.truncate(INTERACT_MAX_UPDATES);
    }
}

#[must_use]
pub const fn chunk_of_block(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr {
        x: x.div_euclid(16),
        z: z.div_euclid(16),
    }
}

pub fn normalize_interact_updates(updates: &mut Vec<InteractUpdate>) {
    updates.retain(|update| !update.is_noop());
    updates.sort_by(|left, right| {
        let left_pos = left.pos();
        let right_pos = right.pos();
        (left_pos.x, left_pos.y, left_pos.z, left.kind_tag()).cmp(&(
            right_pos.x,
            right_pos.y,
            right_pos.z,
            right.kind_tag(),
        ))
    });
    updates.dedup_by_key(|update| (update.pos(), update.kind_tag()));
    updates.truncate(INTERACT_MAX_UPDATES);
}

#[must_use]
pub fn capture_door_toggle(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    chunk: ChunkAddr,
) -> Option<DoorToggleUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(DoorToggleUpdate {
            gid,
            seq,
            tick,
            pos,
            expected_old_state,
            new_state,
            chunk,
        })
    }
}

#[must_use]
pub fn capture_trapdoor_toggle(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    chunk: ChunkAddr,
) -> Option<TrapdoorToggleUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(TrapdoorToggleUpdate {
            gid,
            seq,
            tick,
            pos,
            expected_old_state,
            new_state,
            chunk,
        })
    }
}

#[must_use]
pub fn capture_end_eye(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    chunk: ChunkAddr,
) -> Option<EndEyePlaceUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(EndEyePlaceUpdate {
            gid,
            seq,
            tick,
            pos,
            expected_old_state,
            new_state,
            chunk,
        })
    }
}

#[must_use]
pub fn capture_anchor_charge(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    charges_after: u8,
    chunk: ChunkAddr,
) -> Option<AnchorChargeUpdate> {
    if expected_old_state == new_state {
        None
    } else {
        Some(AnchorChargeUpdate {
            gid,
            seq,
            tick,
            pos,
            expected_old_state,
            new_state,
            charges_after,
            chunk,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteractCodecError {
    pub message: String,
}

impl core::fmt::Display for InteractCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InteractCodecError {}

fn interact_codec_error(context: &str, error: postcard::Error) -> InteractCodecError {
    InteractCodecError {
        message: format!("{context}: {error}"),
    }
}

pub fn encode_door_toggle(update: &DoorToggleUpdate) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(update).map_err(|error| interact_codec_error("encode door", error))
}

pub fn decode_door_toggle(bytes: &[u8]) -> Result<DoorToggleUpdate, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode door", error))
}

pub fn decode_door_toggle_prefix(
    bytes: &[u8],
) -> Result<(DoorToggleUpdate, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode door", error))
}

pub fn encode_door_toggle_into(
    update: &DoorToggleUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(update, out).map_err(|error| interact_codec_error("encode door", error))
}

pub fn encode_door_toggle_to_slice<'out>(
    update: &DoorToggleUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(update, out).map_err(|error| interact_codec_error("encode door", error))
}

pub fn encoded_door_toggle_len(update: &DoorToggleUpdate) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| interact_codec_error("size door", error))
}

pub fn encode_trapdoor_toggle(
    update: &TrapdoorToggleUpdate,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(update).map_err(|error| interact_codec_error("encode trapdoor", error))
}

pub fn decode_trapdoor_toggle(bytes: &[u8]) -> Result<TrapdoorToggleUpdate, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode trapdoor", error))
}

pub fn decode_trapdoor_toggle_prefix(
    bytes: &[u8],
) -> Result<(TrapdoorToggleUpdate, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode trapdoor", error))
}

pub fn encode_trapdoor_toggle_into(
    update: &TrapdoorToggleUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(update, out).map_err(|error| interact_codec_error("encode trapdoor", error))
}

pub fn encode_trapdoor_toggle_to_slice<'out>(
    update: &TrapdoorToggleUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(update, out).map_err(|error| interact_codec_error("encode trapdoor", error))
}

pub fn encoded_trapdoor_toggle_len(
    update: &TrapdoorToggleUpdate,
) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| interact_codec_error("size trapdoor", error))
}

pub fn encode_end_eye(update: &EndEyePlaceUpdate) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(update).map_err(|error| interact_codec_error("encode end eye", error))
}

pub fn decode_end_eye(bytes: &[u8]) -> Result<EndEyePlaceUpdate, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode end eye", error))
}

pub fn decode_end_eye_prefix(
    bytes: &[u8],
) -> Result<(EndEyePlaceUpdate, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode end eye", error))
}

pub fn encode_end_eye_into(
    update: &EndEyePlaceUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(update, out).map_err(|error| interact_codec_error("encode end eye", error))
}

pub fn encode_end_eye_to_slice<'out>(
    update: &EndEyePlaceUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(update, out).map_err(|error| interact_codec_error("encode end eye", error))
}

pub fn encoded_end_eye_len(update: &EndEyePlaceUpdate) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| interact_codec_error("size end eye", error))
}

pub fn encode_anchor_charge(update: &AnchorChargeUpdate) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(update).map_err(|error| interact_codec_error("encode anchor", error))
}

pub fn decode_anchor_charge(bytes: &[u8]) -> Result<AnchorChargeUpdate, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode anchor", error))
}

pub fn decode_anchor_charge_prefix(
    bytes: &[u8],
) -> Result<(AnchorChargeUpdate, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode anchor", error))
}

pub fn encode_anchor_charge_into(
    update: &AnchorChargeUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(update, out).map_err(|error| interact_codec_error("encode anchor", error))
}

pub fn encode_anchor_charge_to_slice<'out>(
    update: &AnchorChargeUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(update, out).map_err(|error| interact_codec_error("encode anchor", error))
}

pub fn encoded_anchor_charge_len(update: &AnchorChargeUpdate) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| interact_codec_error("size anchor", error))
}

pub fn encode_interact(update: &InteractUpdate) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(update).map_err(|error| interact_codec_error("encode interact", error))
}

pub fn decode_interact(bytes: &[u8]) -> Result<InteractUpdate, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode interact", error))
}

pub fn decode_interact_prefix(
    bytes: &[u8],
) -> Result<(InteractUpdate, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode interact", error))
}

pub fn encode_interact_into(
    update: &InteractUpdate,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(update, out).map_err(|error| interact_codec_error("encode interact", error))
}

pub fn encode_interact_to_slice<'out>(
    update: &InteractUpdate,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(update, out).map_err(|error| interact_codec_error("encode interact", error))
}

pub fn encoded_interact_len(update: &InteractUpdate) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| interact_codec_error("size interact", error))
}

pub fn encode_interact_batch(batch: &InteractBatch) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_allocvec(batch).map_err(|error| interact_codec_error("encode batch", error))
}

pub fn decode_interact_batch(bytes: &[u8]) -> Result<InteractBatch, InteractCodecError> {
    postcard::from_bytes(bytes).map_err(|error| interact_codec_error("decode batch", error))
}

pub fn decode_interact_batch_prefix(
    bytes: &[u8],
) -> Result<(InteractBatch, &[u8]), InteractCodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| interact_codec_error("decode batch", error))
}

pub fn encode_interact_batch_into(
    batch: &InteractBatch,
    out: Vec<u8>,
) -> Result<Vec<u8>, InteractCodecError> {
    postcard::to_extend(batch, out).map_err(|error| interact_codec_error("encode batch", error))
}

pub fn encode_interact_batch_to_slice<'out>(
    batch: &InteractBatch,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], InteractCodecError> {
    postcard::to_slice(batch, out).map_err(|error| interact_codec_error("encode batch", error))
}

pub fn encoded_interact_batch_len(batch: &InteractBatch) -> Result<usize, InteractCodecError> {
    postcard::experimental::serialized_size(batch)
        .map_err(|error| interact_codec_error("size batch", error))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractDecision {
    Accept { undo: BlockUndo },
    RejectStale { expected: u16, current: u16 },
}

#[must_use]
pub const fn make_interact_undo(current_state: u16) -> BlockUndo {
    BlockUndo {
        old_state: current_state,
        count_before: INTERACT_NO_INVENTORY,
    }
}

#[must_use]
pub fn apply_interact_edit(current_state: u16, expected_old_state: u16) -> InteractDecision {
    if current_state == expected_old_state {
        InteractDecision::Accept {
            undo: make_interact_undo(current_state),
        }
    } else {
        InteractDecision::RejectStale {
            expected: expected_old_state,
            current: current_state,
        }
    }
}

#[derive(Debug, Default)]
pub struct InteractMetrics {
    accepted: AtomicU64,
    rejected_stale: AtomicU64,
}

impl InteractMetrics {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            accepted: AtomicU64::new(0),
            rejected_stale: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn rejected_stale(&self) -> u64 {
        self.rejected_stale.load(Ordering::Relaxed)
    }
}

#[must_use]
pub fn apply_remote_interact(
    current_state: u16,
    update: &InteractUpdate,
    metrics: &InteractMetrics,
) -> InteractDecision {
    let decision = apply_interact_edit(current_state, update.expected_old_state());
    match decision {
        InteractDecision::Accept { .. } => {
            metrics.accepted.fetch_add(1, Ordering::Relaxed);
        }
        InteractDecision::RejectStale { .. } => {
            metrics.rejected_stale.fetch_add(1, Ordering::Relaxed);
        }
    }
    decision
}

pub fn apply_interact_to_map(
    states: &mut HashMap<BlockPos, u16>,
    update: &InteractUpdate,
) -> Option<(BlockPos, BlockUndo)> {
    let pos = update.pos();
    let current = states.get(&pos).copied().unwrap_or(update.expected_old_state());
    match apply_interact_edit(current, update.expected_old_state()) {
        InteractDecision::Accept { undo } => {
            states.insert(pos, update.new_state());
            Some((pos, undo))
        }
        InteractDecision::RejectStale { .. } => None,
    }
}

fn apply_one_to_map(
    states: &mut HashMap<BlockPos, u16>,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    applied: &mut Vec<(BlockPos, BlockUndo)>,
    conflicts: &mut Vec<(BlockPos, u16, u16)>,
) {
    let current = states.get(&pos).copied().unwrap_or(expected_old_state);
    match apply_interact_edit(current, expected_old_state) {
        InteractDecision::Accept { undo } => {
            states.insert(pos, new_state);
            applied.push((pos, undo));
        }
        InteractDecision::RejectStale { expected, current } => {
            conflicts.push((pos, expected, current));
        }
    }
}

pub fn apply_interact_batch_to_map(
    states: &mut HashMap<BlockPos, u16>,
    batch: &InteractBatch,
) -> (Vec<(BlockPos, BlockUndo)>, Vec<(BlockPos, u16, u16)>) {
    let mut applied = Vec::new();
    let mut conflicts = Vec::new();
    for update in &batch.doors {
        apply_one_to_map(
            states,
            update.pos,
            update.expected_old_state,
            update.new_state,
            &mut applied,
            &mut conflicts,
        );
    }
    for update in &batch.trapdoors {
        apply_one_to_map(
            states,
            update.pos,
            update.expected_old_state,
            update.new_state,
            &mut applied,
            &mut conflicts,
        );
    }
    for update in &batch.end_eyes {
        apply_one_to_map(
            states,
            update.pos,
            update.expected_old_state,
            update.new_state,
            &mut applied,
            &mut conflicts,
        );
    }
    for update in &batch.anchors {
        apply_one_to_map(
            states,
            update.pos,
            update.expected_old_state,
            update.new_state,
            &mut applied,
            &mut conflicts,
        );
    }
    (applied, conflicts)
}

pub fn revert_interact_undos_to_map(
    states: &mut HashMap<BlockPos, u16>,
    undos: &[(BlockPos, BlockUndo)],
) {
    for (pos, undo) in undos {
        states.insert(*pos, undo.old_state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InteractClaim {
    pub gid: GlobalPlayerId,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub undo: BlockUndo,
    pub new_state: u16,
}

#[must_use]
pub fn interact_claim_for(update: &InteractUpdate, undo: BlockUndo) -> InteractClaim {
    InteractClaim {
        gid: update.gid(),
        tick: update.tick(),
        pos: update.pos(),
        undo,
        new_state: update.new_state(),
    }
}

#[must_use]
pub fn elect_interact_claim(
    cluster_seed: u64,
    left: &InteractClaim,
    right: &InteractClaim,
) -> InteractClaim {
    match order_players(cluster_seed, left.tick, left.gid, right.gid) {
        core::cmp::Ordering::Less => *left,
        core::cmp::Ordering::Greater => *right,
        core::cmp::Ordering::Equal => {
            let left_key = (
                left.tick.0,
                (left.pos.x, left.pos.y, left.pos.z),
                left.new_state,
                left.undo.old_state,
            );
            let right_key = (
                right.tick.0,
                (right.pos.x, right.pos.y, right.pos.z),
                right.new_state,
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
pub struct InteractSeqClock {
    next: HashMap<GlobalPlayerId, u16>,
}

impl InteractSeqClock {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn issue(&mut self, gid: GlobalPlayerId) -> PlayerSeq {
        let counter = self.next.entry(gid).or_insert(0);
        let seq = PlayerSeq(*counter);
        *counter = counter.wrapping_add(1);
        seq
    }
}

#[derive(Debug, Default)]
pub struct InteractAcceptance {
    table: BucketTable,
}

impl InteractAcceptance {
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
pub fn interact_loser_revert_plan(losers: &[(BlockPos, BlockUndo)]) -> ReconcilePlan {
    ReconcilePlan::from_loser_undos(losers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn pos(x: i32, y: i32, z: i32) -> BlockPos {
        BlockPos { x, y, z }
    }

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    fn door() -> DoorToggleUpdate {
        capture_door_toggle(
            gid(1, 2),
            PlayerSeq(7),
            TickStamp(9),
            pos(1, 64, 3),
            11,
            12,
            chunk(0, 0),
        )
        .unwrap()
    }

    fn trapdoor() -> TrapdoorToggleUpdate {
        capture_trapdoor_toggle(
            gid(1, 3),
            PlayerSeq(8),
            TickStamp(9),
            pos(2, 64, 3),
            21,
            22,
            chunk(0, 0),
        )
        .unwrap()
    }

    fn end_eye() -> EndEyePlaceUpdate {
        capture_end_eye(
            gid(2, 1),
            PlayerSeq(3),
            TickStamp(10),
            pos(5, 64, 5),
            31,
            32,
            chunk(0, 0),
        )
        .unwrap()
    }

    fn anchor() -> AnchorChargeUpdate {
        capture_anchor_charge(
            gid(2, 2),
            PlayerSeq(4),
            TickStamp(11),
            pos(7, 64, 7),
            41,
            42,
            3,
            chunk(0, 0),
        )
        .unwrap()
    }

    #[test]
    fn capture_packs_fields_and_drops_noop() {
        let update = door();
        assert_eq!(update.gid, gid(1, 2));
        assert_eq!(update.seq, PlayerSeq(7));
        assert_eq!(update.tick, TickStamp(9));
        assert_eq!(update.pos, pos(1, 64, 3));
        assert_eq!(update.expected_old_state, 11);
        assert_eq!(update.new_state, 12);
        assert_eq!(update.chunk, chunk(0, 0));
        assert_eq!(DoorToggleUpdate::stream_kind(), StreamKind::PlayerWorld);
        assert!(
            capture_door_toggle(
                gid(1, 2),
                PlayerSeq(7),
                TickStamp(9),
                pos(1, 64, 3),
                11,
                11,
                chunk(0, 0)
            )
            .is_none()
        );
        assert!(
            capture_trapdoor_toggle(
                gid(1, 3),
                PlayerSeq(8),
                TickStamp(9),
                pos(2, 64, 3),
                21,
                21,
                chunk(0, 0)
            )
            .is_none()
        );
        assert!(
            capture_end_eye(
                gid(2, 1),
                PlayerSeq(3),
                TickStamp(10),
                pos(5, 64, 5),
                31,
                31,
                chunk(0, 0)
            )
            .is_none()
        );
        assert!(
            capture_anchor_charge(
                gid(2, 2),
                PlayerSeq(4),
                TickStamp(11),
                pos(7, 64, 7),
                41,
                41,
                3,
                chunk(0, 0)
            )
            .is_none()
        );
    }

    #[test]
    fn singles_roundtrip_all_forms() {
        let update = door();
        let bytes = encode_door_toggle(&update).unwrap();
        assert_eq!(decode_door_toggle(&bytes).unwrap(), update);
        assert_eq!(encode_door_toggle_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_door_toggle_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_door_toggle_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_door_toggle_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let update = trapdoor();
        let bytes = encode_trapdoor_toggle(&update).unwrap();
        assert_eq!(decode_trapdoor_toggle(&bytes).unwrap(), update);
        assert_eq!(encode_trapdoor_toggle_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_trapdoor_toggle_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_trapdoor_toggle_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_trapdoor_toggle_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let update = end_eye();
        let bytes = encode_end_eye(&update).unwrap();
        assert_eq!(decode_end_eye(&bytes).unwrap(), update);
        assert_eq!(encode_end_eye_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_end_eye_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_end_eye_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_end_eye_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let update = anchor();
        let bytes = encode_anchor_charge(&update).unwrap();
        assert_eq!(decode_anchor_charge(&bytes).unwrap(), update);
        assert_eq!(encode_anchor_charge_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_anchor_charge_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_anchor_charge_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_anchor_charge_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());
        assert_eq!(update.charges_after, 3);
    }

    #[test]
    fn enum_and_batch_roundtrip_all_forms() {
        let update = InteractUpdate::Anchor(anchor());
        assert_eq!(update.kind_tag(), INTERACT_KIND_ANCHOR);
        assert_eq!(update.pos(), pos(7, 64, 7));
        assert_eq!(update.chunk(), chunk(0, 0));
        assert_eq!(update.tick(), TickStamp(11));
        assert_eq!(update.gid(), gid(2, 2));
        assert_eq!(update.expected_old_state(), 41);
        assert_eq!(update.new_state(), 42);
        assert!(!update.is_noop());
        let bytes = encode_interact(&update).unwrap();
        assert_eq!(decode_interact(&bytes).unwrap(), update);
        assert_eq!(encode_interact_into(&update, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_interact_len(&update).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_interact_to_slice(&update, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_interact_prefix(&bytes).unwrap();
        assert_eq!(back, update);
        assert!(rest.is_empty());

        let mut batch = InteractBatch::new(TickStamp(9));
        assert!(batch.is_empty());
        batch.push(InteractUpdate::Door(door()));
        batch.push_trapdoor(trapdoor());
        batch.push_end_eye(end_eye());
        batch.push_anchor(anchor());
        assert_eq!(batch.len(), 4);
        assert_eq!(batch.doors.len(), 1);
        assert_eq!(batch.trapdoors.len(), 1);
        assert_eq!(batch.end_eyes.len(), 1);
        assert_eq!(batch.anchors.len(), 1);
        assert_eq!(InteractBatch::stream_kind(), StreamKind::PlayerWorld);
        let bytes = encode_interact_batch(&batch).unwrap();
        assert_eq!(decode_interact_batch(&bytes).unwrap(), batch);
        assert_eq!(encode_interact_batch_into(&batch, Vec::new()).unwrap(), bytes);
        assert_eq!(encoded_interact_batch_len(&batch).unwrap(), bytes.len());
        let mut slice = vec![0_u8; bytes.len()];
        let used = encode_interact_batch_to_slice(&batch, &mut slice).unwrap().len();
        assert_eq!(&slice[..used], bytes);
        let (back, rest) = decode_interact_batch_prefix(&bytes).unwrap();
        assert_eq!(back, batch);
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_door_toggle(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_interact(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_interact_batch(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn batch_normalize_sorts_dedups_and_caps() {
        let mut updates = vec![
            InteractUpdate::Door(door()),
            InteractUpdate::Door(door()),
            InteractUpdate::Trapdoor(trapdoor()),
        ];
        normalize_interact_updates(&mut updates);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].pos(), pos(1, 64, 3));
        assert_eq!(updates[1].pos(), pos(2, 64, 3));

        let mut batch = InteractBatch::new(TickStamp(9));
        for _ in 0..(INTERACT_MAX_UPDATES + 10) {
            batch.push_end_eye(end_eye());
        }
        batch.push_door(door());
        batch.normalize();
        assert_eq!(batch.end_eyes.len(), 1);
        assert_eq!(batch.doors.len(), 1);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn remote_apply_accepts_expected_and_counts() {
        let metrics = InteractMetrics::new();
        let update = InteractUpdate::Door(door());
        let decision = apply_remote_interact(11, &update, &metrics);
        assert_eq!(
            decision,
            InteractDecision::Accept {
                undo: make_interact_undo(11)
            }
        );
        assert_eq!(metrics.accepted(), 1);
        assert_eq!(metrics.rejected_stale(), 0);
    }

    #[test]
    fn remote_apply_rejects_stale_without_mutation() {
        let metrics = InteractMetrics::new();
        let update = InteractUpdate::Anchor(anchor());
        let decision = apply_remote_interact(99, &update, &metrics);
        assert_eq!(
            decision,
            InteractDecision::RejectStale {
                expected: 41,
                current: 99
            }
        );
        assert_eq!(metrics.accepted(), 0);
        assert_eq!(metrics.rejected_stale(), 1);
    }

    #[test]
    fn batch_apply_and_revert_roundtrip() {
        let mut states = HashMap::new();
        states.insert(pos(1, 64, 3), 11);
        states.insert(pos(2, 64, 3), 77);
        let mut batch = InteractBatch::new(TickStamp(9));
        batch.push(InteractUpdate::Door(door()));
        batch.push(InteractUpdate::Trapdoor(trapdoor()));
        let (applied, conflicts) = apply_interact_batch_to_map(&mut states, &batch);
        assert_eq!(applied.len(), 1);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0], (pos(2, 64, 3), 21, 77));
        assert_eq!(states[&pos(1, 64, 3)], 12);
        assert_eq!(states[&pos(2, 64, 3)], 77);
        revert_interact_undos_to_map(&mut states, &applied);
        assert_eq!(states[&pos(1, 64, 3)], 11);
        let plan = interact_loser_revert_plan(&applied);
        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn election_is_order_independent() {
        let left = interact_claim_for(
            &InteractUpdate::Door(door()),
            make_interact_undo(11),
        );
        let right = interact_claim_for(
            &InteractUpdate::Trapdoor(trapdoor()),
            make_interact_undo(21),
        );
        let forward = elect_interact_claim(1234, &left, &right);
        let backward = elect_interact_claim(1234, &right, &left);
        assert_eq!(forward, backward);
        assert!(forward.pos == left.pos || forward.pos == right.pos);
    }

    #[test]
    fn seq_clock_starts_at_zero_per_player() {
        let mut clock = InteractSeqClock::new();
        let first = gid(1, 2);
        let other = gid(1, 3);
        assert_eq!(clock.issue(first), PlayerSeq(0));
        assert_eq!(clock.issue(first), PlayerSeq(1));
        assert_eq!(clock.issue(other), PlayerSeq(0));
        assert_eq!(clock.issue(first), PlayerSeq(2));
    }

    #[test]
    fn acceptance_gates_tick_completion() {
        let mut acceptance = InteractAcceptance::new();
        let tick = TickStamp(9);
        let target = chunk(0, 0);
        acceptance.require(tick, target, &[1, 2]);
        assert!(!acceptance.is_complete(tick));
        assert_eq!(acceptance.missing(tick, target), vec![1, 2]);
        acceptance.accept(tick, target, 1, &[]);
        assert!(!acceptance.is_complete(tick));
        acceptance.accept(tick, target, 2, &[]);
        assert!(acceptance.is_complete(tick));
        assert_eq!(acceptance.pending_ticks(), 1);
        assert!(acceptance.remove_tick(tick));
        assert!(acceptance.is_complete(tick));
    }

    #[test]
    fn chunk_of_block_handles_negatives() {
        assert_eq!(chunk_of_block(0, 0), chunk(0, 0));
        assert_eq!(chunk_of_block(15, 15), chunk(0, 0));
        assert_eq!(chunk_of_block(16, -1), chunk(1, -1));
        assert_eq!(chunk_of_block(-17, -17), chunk(-2, -2));
    }
}
