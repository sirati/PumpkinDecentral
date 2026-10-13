use bytes::Bytes;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::VecDeque;
use std::num::NonZero;
use std::sync::{Arc, Weak};

use crate::net::java::chunk_data::{CChunkData, ChunkLightExt};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{
    CChunkBatchEnd, CChunkBatchStart, CLightUpdate, CUnloadChunk,
};
use pumpkin_protocol::ser::NetworkWriteExt;
use pumpkin_protocol::{ClientPacket, MultiVersionJavaPacket};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::ChunkData;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use pumpkin_world::level::{Level, SyncChunk};

use crate::net::ClientPlatform;

const MIN_CHUNKS_PER_TICK: f32 = 0.1;
const MAX_CHUNKS_PER_TICK: f32 = 500.0;
const INITIAL_CHUNKS_PER_TICK: f32 = 9.0;
const MAX_CONCURRENT_BATCHES: u16 = 10;

pub struct PreparedChunk {
    pub position: Vector2<i32>,
    pub chunk: SyncChunk,
}

pub struct PreparedBatch {
    pub chunks: Vec<PreparedChunk>,
    pub epoch_snapshot: u32,
    pub target_version: JavaMinecraftVersion,
}

#[derive(Clone)]
pub struct EncodedChunk {
    pub position: Vector2<i32>,
    pub payload: Bytes,
    pub light_payload: Option<Bytes>,
    pub chunk_ref: Weak<ChunkData>,
}

#[derive(Debug)]
struct AwaitingBatchAck {
    epoch: u32,
    positions: Vec<Vector2<i32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchAckResult {
    Applied,
    Missing,
    RevisionMismatch { expected: u32, received: u32 },
}

impl EncodedChunk {
    #[must_use]
    pub fn is_fresh_for(&self, candidate: &PreparedChunk) -> bool {
        let Some(held) = self.chunk_ref.upgrade() else {
            return false;
        };

        self.position == candidate.position && Arc::ptr_eq(&held, &candidate.chunk)
    }
}

#[derive(Debug)]
pub struct ReadyChunkView {
    pub epoch: Option<u32>,
    pub chunks: FxHashSet<Vector2<i32>>,
}

impl ReadyChunkView {
    pub fn contains(&self, epoch: u32, position: &Vector2<i32>) -> bool {
        self.epoch == Some(epoch) && self.chunks.contains(position)
    }
}

#[derive(Debug)]
pub struct ChunkSender {
    pub pending_chunks: FxHashSet<Vector2<i32>>,
    sent_chunks: FxHashSet<Vector2<i32>>,
    /// Committed Bedrock chunks packets are still being encoded.
    awaiting_delivery: FxHashSet<Vector2<i32>>,
    awaiting_batch_ack: VecDeque<AwaitingBatchAck>,
    pub in_flight_batches: u16,
    pub desired_rate: f32,
    pub send_quota: f32,
    pub max_in_flight: u16,
    pub(crate) ready_view: Arc<arc_swap::ArcSwap<ReadyChunkView>>,
    publication_epoch: Option<u32>,
}

impl ChunkSender {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending_chunks: FxHashSet::default(),
            sent_chunks: FxHashSet::default(),
            awaiting_delivery: FxHashSet::default(),
            awaiting_batch_ack: VecDeque::new(),
            in_flight_batches: 0,
            desired_rate: INITIAL_CHUNKS_PER_TICK,
            send_quota: 0.0,
            max_in_flight: MAX_CONCURRENT_BATCHES,
            ready_view: Arc::new(arc_swap::ArcSwap::from_pointee(ReadyChunkView {
                epoch: None,
                chunks: FxHashSet::default(),
            })),
            publication_epoch: None,
        }
    }

    fn publish_ready_view(&self) {
        self.ready_view.store(Arc::new(ReadyChunkView {
            epoch: self.publication_epoch,
            chunks: self
                .sent_chunks
                .difference(&self.awaiting_delivery)
                .copied()
                .collect(),
        }));
    }

    pub fn reset(&mut self) {
        self.pending_chunks.clear();
        self.sent_chunks.clear();
        self.awaiting_delivery.clear();
        self.awaiting_batch_ack.clear();
        self.in_flight_batches = 0;
        self.send_quota = 0.0;
        self.publication_epoch = None;
        self.publish_ready_view();
    }

    #[must_use]
    pub fn is_chunk_sent(&self, pos: &Vector2<i32>) -> bool {
        self.sent_chunks.contains(pos)
    }

    /// Vanilla `ChunkMap.isChunkTracked` -> chunk packet is already queued for this player.
    #[must_use]
    pub fn is_chunk_ready(&self, pos: &Vector2<i32>) -> bool {
        self.sent_chunks.contains(pos) && !self.awaiting_delivery.contains(pos)
    }

    /// Bedrock chunks become ready `send_chunks` has queued them.
    pub fn mark_delivered(&mut self, positions: &[Vector2<i32>]) {
        for pos in positions {
            self.awaiting_delivery.remove(pos);
        }
        self.publish_ready_view();
    }

    #[must_use]
    pub fn sent_chunks_count(&self) -> usize {
        self.sent_chunks.len()
    }

    pub fn on_batch_acknowledged(
        &mut self,
        client_requested_rate: f32,
        current_epoch: u32,
    ) -> BatchAckResult {
        let Some(front) = self.awaiting_batch_ack.front() else {
            return BatchAckResult::Missing;
        };
        if front.epoch != current_epoch {
            return BatchAckResult::RevisionMismatch {
                expected: front.epoch,
                received: current_epoch,
            };
        }
        let Some(delivered) = self.awaiting_batch_ack.pop_front() else {
            return BatchAckResult::Missing;
        };

        self.mark_delivered(&delivered.positions);

        self.in_flight_batches = self.in_flight_batches.saturating_sub(1);
        self.desired_rate = if client_requested_rate.is_nan() {
            MIN_CHUNKS_PER_TICK
        } else {
            client_requested_rate.clamp(MIN_CHUNKS_PER_TICK, MAX_CHUNKS_PER_TICK)
        };

        BatchAckResult::Applied
    }

    pub fn register_batch_for_acknowledgement(&mut self, epoch: u32, positions: Vec<Vector2<i32>>) {
        self.publication_epoch = Some(epoch);
        self.in_flight_batches = self.in_flight_batches.saturating_add(1);
        self.awaiting_batch_ack
            .push_back(AwaitingBatchAck { epoch, positions });
    }

    pub fn enqueue_chunk(&mut self, pos: Vector2<i32>) {
        let was_ready = self.is_chunk_ready(&pos);
        self.sent_chunks.remove(&pos);
        self.awaiting_delivery.remove(&pos);
        for batch in &mut self.awaiting_batch_ack {
            batch.positions.retain(|batch_pos| *batch_pos != pos);
        }
        self.pending_chunks.insert(pos);
        if was_ready {
            self.publish_ready_view();
        }
    }

    pub fn unload_chunk(&mut self, client: &ClientPlatform, pos: Vector2<i32>) {
        let was_ready = self.is_chunk_ready(&pos);
        self.pending_chunks.remove(&pos);
        self.awaiting_delivery.remove(&pos);
        if self.sent_chunks.remove(&pos)
            && let ClientPlatform::Java(java_client) = client
            && !java_client.is_closed()
        {
            java_client.try_send_packet(&CUnloadChunk::new(pos.x, pos.y));
        }
        if was_ready {
            self.publish_ready_view();
        }
    }

    fn collect_sorted_candidates(
        &self,
        level: &Level,
        center: Vector2<i32>,
        view_distance: NonZero<u8>,
    ) -> Vec<PreparedChunk> {
        let quota_limit = self.send_quota.floor() as usize;
        let mut ready = Vec::with_capacity(quota_limit);

        // If pending_chunks is small, sorting it directly avoids scanning offsets.
        if self.pending_chunks.len() <= 16 {
            let mut sorted: Vec<Vector2<i32>> = self.pending_chunks.iter().copied().collect();
            sorted.sort_unstable_by_key(|pos| {
                let dx = (pos.x - center.x).unsigned_abs() as u64;
                let dz = (pos.y - center.y).unsigned_abs() as u64;
                dx * dx + dz * dz
            });

            for pos in sorted {
                if ready.len() >= quota_limit {
                    break;
                }

                if let Some(chunk) = level.loaded_chunks.get(&pos) {
                    ready.push(PreparedChunk {
                        position: pos,
                        chunk: chunk.value().clone(),
                    });
                }
            }
        } else {
            // Re-use precompiled cylindrical chunk view LUT which is already sorted center-outward.
            let offsets = Cylindrical::get_offsets(view_distance.get());
            for &(dx, dy) in offsets {
                if ready.len() >= quota_limit {
                    break;
                }

                let pos = Vector2::new(center.x + i32::from(dx), center.y + i32::from(dy));
                if self.pending_chunks.contains(&pos)
                    && let Some(chunk) = level.loaded_chunks.get(&pos)
                {
                    ready.push(PreparedChunk {
                        position: pos,
                        chunk: chunk.value().clone(),
                    });
                }
            }

            // Fallback for any pending chunks outside the precomputed table
            if ready.is_empty() {
                for &pos in &self.pending_chunks {
                    if ready.len() >= quota_limit {
                        break;
                    }
                    if let Some(chunk) = level.loaded_chunks.get(&pos) {
                        ready.push(PreparedChunk {
                            position: pos,
                            chunk: chunk.value().clone(),
                        });
                    }
                }
            }
        }

        ready
    }

    pub fn prepare_batch(
        &mut self,
        level: &Level,
        player_chunk: Vector2<i32>,
        view_distance: NonZero<u8>,
        epoch: u32,
        version: JavaMinecraftVersion,
    ) -> Option<PreparedBatch> {
        if version >= JavaMinecraftVersion::V_1_20_2 && self.in_flight_batches >= self.max_in_flight
        {
            return None;
        }

        let max_batch = self.desired_rate.max(1.0);
        self.send_quota = (self.send_quota + self.desired_rate).min(max_batch);

        if self.send_quota < 1.0 || self.pending_chunks.is_empty() {
            return None;
        }

        let candidates = self.collect_sorted_candidates(level, player_chunk, view_distance);
        if candidates.is_empty() {
            return None;
        }

        Some(PreparedBatch {
            chunks: candidates,
            epoch_snapshot: epoch,
            target_version: version,
        })
    }

    pub fn encode_batch(
        batch: &PreparedBatch,
        cache: &mut FxHashMap<Vector2<i32>, EncodedChunk>,
    ) -> Vec<EncodedChunk> {
        let version = batch.target_version;
        let cached_map = &*cache;

        let encoded_results: Vec<Option<EncodedChunk>> = batch
            .chunks
            .par_iter()
            .map(|candidate| {
                let pos = candidate.position;
                if let Some(cached) = cached_map.get(&pos)
                    && cached.is_fresh_for(candidate)
                {
                    return Some(cached.clone());
                }

                let chunk = &candidate.chunk;
                let mut chunk_buf = Vec::with_capacity(32 * 1024);
                if chunk_buf
                    .write_var_int(&VarInt(CChunkData::to_id(version)))
                    .is_err()
                {
                    return None;
                }
                if CChunkData(chunk)
                    .write_packet_data(&mut chunk_buf, &version)
                    .is_err()
                {
                    return None;
                }

                let light_payload = if version >= JavaMinecraftVersion::V_1_14
                    && version < JavaMinecraftVersion::V_1_18
                {
                    CLightUpdate::from_chunk(chunk, version)
                        .ok()
                        .and_then(|light_packet| {
                            let mut light_buf = Vec::new();
                            (light_buf
                                .write_var_int(&VarInt(CLightUpdate::to_id(version)))
                                .is_ok()
                                && light_packet
                                    .write_packet_data(&mut light_buf, &version)
                                    .is_ok())
                            .then(|| Bytes::from(light_buf))
                        })
                } else {
                    None
                };

                Some(EncodedChunk {
                    position: pos,
                    payload: Bytes::from(chunk_buf),
                    light_payload,
                    chunk_ref: Arc::downgrade(chunk),
                })
            })
            .collect();

        let mut output = Vec::with_capacity(encoded_results.len());
        for encoded in encoded_results.into_iter().flatten() {
            cache.insert(encoded.position, encoded.clone());
            output.push(encoded);
        }

        output
    }

    pub fn commit_batch(
        &mut self,
        batch: &PreparedBatch,
        encoded_chunks: &[EncodedChunk],
        client: &ClientPlatform,
        current_epoch: u32,
    ) -> Vec<Vector2<i32>> {
        if current_epoch != batch.epoch_snapshot || encoded_chunks.is_empty() {
            return Vec::new();
        }

        let mut dispatched_positions = Vec::with_capacity(encoded_chunks.len());
        let version = batch.target_version;

        if version >= JavaMinecraftVersion::V_1_20_2
            && let ClientPlatform::Java(java_client) = client
        {
            java_client.try_send_packet(&CChunkBatchStart);
        }

        for chunk in encoded_chunks {
            if !self.pending_chunks.contains(&chunk.position) {
                continue;
            }

            client.try_enqueue_packet(chunk.payload.clone());
            if let Some(ref light) = chunk.light_payload {
                client.try_enqueue_packet(light.clone());
            }

            self.pending_chunks.remove(&chunk.position);
            self.sent_chunks.insert(chunk.position);
            dispatched_positions.push(chunk.position);
        }

        let sent_count = dispatched_positions.len();
        if sent_count > 0 {
            if version >= JavaMinecraftVersion::V_1_20_2
                && let ClientPlatform::Java(java_client) = client
            {
                java_client.try_send_packet(&CChunkBatchEnd::new(sent_count as u16));
                self.awaiting_delivery
                    .extend(dispatched_positions.iter().copied());
                self.register_batch_for_acknowledgement(
                    current_epoch,
                    dispatched_positions.clone(),
                );
            }

            self.send_quota -= sent_count as f32;
        }

        self.publication_epoch = Some(current_epoch);
        self.publish_ready_view();
        dispatched_positions
    }

    pub fn commit_encoded_bedrock_batch(
        &mut self,
        batch: &PreparedBatch,
        encoded: &[EncodedChunk],
        current_epoch: u32,
    ) -> Vec<Vector2<i32>> {
        if current_epoch != batch.epoch_snapshot {
            return Vec::new();
        }
        let dispatched = encoded
            .iter()
            .filter_map(|chunk| {
                if !self.pending_chunks.remove(&chunk.position) {
                    return None;
                }
                self.sent_chunks.insert(chunk.position);
                self.awaiting_delivery.insert(chunk.position);
                Some(chunk.position)
            })
            .collect::<Vec<_>>();
        self.send_quota -= dispatched.len() as f32;
        self.publication_epoch = Some(current_epoch);
        self.publish_ready_view();
        dispatched
    }

    /// Marks a prepared Bedrock batch as dispatched and returns its chunks for encoding.
    ///
    /// Bedrock chunks use a different encoder from Java chunks, but they must still move from
    /// `pending_chunks` to `sent_chunks`. Otherwise the same batch is selected every tick and the
    /// Bedrock login flow never reaches its minimum-chunk spawn threshold.
    /// They stay not ready until [`Self::mark_delivered`].
    pub fn commit_bedrock_batch(
        &mut self,
        batch: &PreparedBatch,
        current_epoch: u32,
    ) -> Vec<PreparedChunk> {
        if current_epoch != batch.epoch_snapshot || batch.chunks.is_empty() {
            return Vec::new();
        }

        let mut dispatched_chunks = Vec::with_capacity(batch.chunks.len());
        for candidate in &batch.chunks {
            if !self.pending_chunks.remove(&candidate.position) {
                continue;
            }

            self.sent_chunks.insert(candidate.position);
            self.awaiting_delivery.insert(candidate.position);
            dispatched_chunks.push(PreparedChunk {
                position: candidate.position,
                chunk: candidate.chunk.clone(),
            });
        }

        self.send_quota -= dispatched_chunks.len() as f32;
        self.publication_epoch = Some(current_epoch);
        self.publish_ready_view();
        dispatched_chunks
    }
}

impl Default for ChunkSender {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_bedrock_batches_require_transport_delivery_and_current_epoch() {
        let mut sender = ChunkSender::new();
        let positions = [Vector2::new(9, -4), Vector2::new(10, -4)];
        let encoded = positions
            .iter()
            .map(|position| EncodedChunk {
                position: *position,
                payload: bytes::Bytes::from_static(b"encoded owned chunk"),
                light_payload: None,
                chunk_ref: Weak::new(),
            })
            .collect::<Vec<_>>();
        for position in positions {
            sender.enqueue_chunk(position);
        }
        let batch = PreparedBatch {
            chunks: Vec::new(),
            epoch_snapshot: 3,
            target_version: JavaMinecraftVersion::Unknown,
        };
        assert!(
            sender
                .commit_encoded_bedrock_batch(&batch, &encoded, 4)
                .is_empty()
        );
        assert_eq!(sender.pending_chunks.len(), 2);
        let dispatched = sender.commit_encoded_bedrock_batch(&batch, &encoded, 3);
        assert_eq!(dispatched, positions);
        assert!(sender.pending_chunks.is_empty());
        assert!(
            positions
                .iter()
                .all(|position| !sender.is_chunk_ready(position))
        );
        sender.mark_delivered(&positions[..1]);
        assert!(sender.is_chunk_ready(&positions[0]));
        assert!(!sender.is_chunk_ready(&positions[1]));
        assert!(
            sender
                .commit_encoded_bedrock_batch(&batch, &encoded, 3)
                .is_empty()
        );
        sender.mark_delivered(&positions[1..]);
        assert!(
            positions
                .iter()
                .all(|position| sender.is_chunk_ready(position))
        );
        sender.reset();
        assert!(
            positions
                .iter()
                .all(|position| !sender.is_chunk_ready(position))
        );
    }

    #[test]
    fn immutable_ready_view_tracks_delivery_resend_and_session_reset() {
        let position = Vector2::new(3, -2);
        let mut sender = ChunkSender::new();
        let view = Arc::clone(&sender.ready_view);
        let batch = PreparedBatch {
            chunks: vec![PreparedChunk {
                position,
                chunk: ChunkData::empty_sync(position.x, position.y),
            }],
            epoch_snapshot: 7,
            target_version: JavaMinecraftVersion::V_1_20_2,
        };
        sender.enqueue_chunk(position);
        assert!(!view.load().contains(7, &position));
        sender.commit_bedrock_batch(&batch, 7);
        assert!(!view.load().contains(7, &position));
        sender.mark_delivered(&[position]);
        let prior = view.load_full();
        assert!(prior.contains(7, &position));
        assert!(!prior.contains(8, &position));
        sender.enqueue_chunk(position);
        assert!(!view.load().contains(7, &position));
        assert!(prior.contains(7, &position));
        sender.commit_bedrock_batch(&batch, 7);
        sender.mark_delivered(&[position]);
        assert!(view.load().contains(7, &position));
        sender.reset();
        assert!(view.load().chunks.is_empty());
        assert_eq!(view.load().epoch, None);
    }

    #[test]
    fn bedrock_batch_moves_pending_chunks_to_sent() {
        let position = Vector2::new(3, -2);
        let chunk = ChunkData::empty_sync(position.x, position.y);
        let batch = PreparedBatch {
            chunks: vec![PreparedChunk {
                position,
                chunk: chunk.clone(),
            }],
            epoch_snapshot: 7,
            target_version: JavaMinecraftVersion::V_1_20_2,
        };
        let mut sender = ChunkSender::new();
        sender.enqueue_chunk(position);
        sender.send_quota = 1.0;

        let dispatched = sender.commit_bedrock_batch(&batch, 7);

        assert_eq!(dispatched.len(), 1);
        assert!(Arc::ptr_eq(&dispatched[0].chunk, &chunk));
        assert!(!sender.pending_chunks.contains(&position));
        assert!(sender.is_chunk_sent(&position));
        assert_eq!(sender.sent_chunks_count(), 1);
        assert_eq!(sender.send_quota, 0.0);

        let repeated = sender.commit_bedrock_batch(&batch, 7);

        assert!(repeated.is_empty());
        assert_eq!(sender.sent_chunks_count(), 1);
        assert_eq!(sender.send_quota, 0.0);
    }

    #[test]
    fn bedrock_batch_ignores_a_stale_epoch() {
        let position = Vector2::new(3, -2);
        let batch = PreparedBatch {
            chunks: vec![PreparedChunk {
                position,
                chunk: ChunkData::empty_sync(position.x, position.y),
            }],
            epoch_snapshot: 7,
            target_version: JavaMinecraftVersion::V_1_20_2,
        };
        let mut sender = ChunkSender::new();
        sender.enqueue_chunk(position);

        let dispatched = sender.commit_bedrock_batch(&batch, 8);

        assert!(dispatched.is_empty());
        assert!(sender.pending_chunks.contains(&position));
        assert_eq!(sender.sent_chunks_count(), 0);
    }

    #[test]
    fn watchers_become_ready_on_their_own_delivery() {
        let position = Vector2::new(1, 4);
        let batch = PreparedBatch {
            chunks: vec![PreparedChunk {
                position,
                chunk: ChunkData::empty_sync(position.x, position.y),
            }],
            epoch_snapshot: 0,
            target_version: JavaMinecraftVersion::V_1_20_2,
        };
        let mut first = ChunkSender::new();
        let mut second = ChunkSender::new();
        first.enqueue_chunk(position);
        second.enqueue_chunk(position);
        assert!(!first.is_chunk_ready(&position));
        assert!(!second.is_chunk_ready(&position));

        first.commit_bedrock_batch(&batch, 0);
        assert!(!first.is_chunk_ready(&position));
        first.mark_delivered(&[position]);
        assert!(first.is_chunk_ready(&position));
        assert!(!second.is_chunk_ready(&position));

        second.commit_bedrock_batch(&batch, 0);
        assert!(!second.is_chunk_ready(&position));
        second.mark_delivered(&[position]);
        assert!(second.is_chunk_ready(&position));

        first.enqueue_chunk(position);
        assert!(!first.is_chunk_ready(&position));
        assert!(second.is_chunk_ready(&position));
    }

    #[test]
    fn java_acknowledgements_release_batches_in_fifo_order() {
        let first = Vector2::new(1, 2);
        let second = Vector2::new(3, 4);
        let mut sender = ChunkSender::new();
        sender.sent_chunks.extend([first, second]);
        sender.awaiting_delivery.extend([first, second]);
        sender.awaiting_batch_ack.push_back(AwaitingBatchAck {
            epoch: 7,
            positions: vec![first],
        });
        sender.awaiting_batch_ack.push_back(AwaitingBatchAck {
            epoch: 7,
            positions: vec![second],
        });
        sender.in_flight_batches = 2;

        assert!(!sender.is_chunk_ready(&first));
        assert!(!sender.is_chunk_ready(&second));
        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::Applied
        );
        assert!(sender.is_chunk_ready(&first));
        assert!(!sender.is_chunk_ready(&second));
        assert_eq!(sender.in_flight_batches, 1);
        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::Applied
        );
        assert!(sender.is_chunk_ready(&second));
        assert_eq!(sender.in_flight_batches, 0);
        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::Missing
        );
    }

    #[test]
    fn an_earlier_virtual_batch_cannot_acknowledge_real_chunks() {
        let real = Vector2::new(3, 4);
        let mut sender = ChunkSender::new();
        sender.sent_chunks.insert(real);
        sender.awaiting_delivery.insert(real);
        sender.register_batch_for_acknowledgement(7, Vec::new());
        sender.register_batch_for_acknowledgement(7, vec![real]);

        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::Applied
        );
        assert!(!sender.is_chunk_ready(&real));
        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::Applied
        );
        assert!(sender.is_chunk_ready(&real));
    }

    #[test]
    fn stale_session_acknowledgement_cannot_release_current_chunks() {
        let position = Vector2::new(1, 2);
        let mut sender = ChunkSender::new();
        sender.sent_chunks.insert(position);
        sender.awaiting_delivery.insert(position);
        sender.awaiting_batch_ack.push_back(AwaitingBatchAck {
            epoch: 6,
            positions: vec![position],
        });
        sender.in_flight_batches = 1;

        assert_eq!(
            sender.on_batch_acknowledged(20.0, 7),
            BatchAckResult::RevisionMismatch {
                expected: 6,
                received: 7,
            }
        );
        assert!(!sender.is_chunk_ready(&position));
        assert_eq!(sender.in_flight_batches, 1);
    }

    #[test]
    fn stale_bedrock_delivery_cannot_clear_a_newer_batchs_marker() {
        // Player's Bedrock chunk-send task used to check
        // chunk_send_epoch, then separately lock chunk_sender to call
        // mark_delivered.
        let position = Vector2::new(5, -1);
        let sender = std::sync::Mutex::new(ChunkSender::new());
        let epoch = std::sync::atomic::AtomicU32::new(0);

        {
            let mut sender = sender.lock().unwrap();
            sender.enqueue_chunk(position);
            let batch = PreparedBatch {
                chunks: vec![PreparedChunk {
                    position,
                    chunk: ChunkData::empty_sync(position.x, position.y),
                }],
                epoch_snapshot: 0,
                target_version: JavaMinecraftVersion::V_1_20_2,
            };
            sender.commit_bedrock_batch(&batch, 0)
        };
        let stale_epoch_snapshot = 0;

        // bump the epoch, then reset and re-send for the new
        // world, reusing the same relative chunk position.
        epoch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        {
            let mut sender = sender.lock().unwrap();
            sender.reset();
            sender.enqueue_chunk(position);
            let batch = PreparedBatch {
                chunks: vec![PreparedChunk {
                    position,
                    chunk: ChunkData::empty_sync(position.x, position.y),
                }],
                epoch_snapshot: 1,
                target_version: JavaMinecraftVersion::V_1_20_2,
            };
            sender.commit_bedrock_batch(&batch, 1)
        };
        assert!(!sender.lock().unwrap().is_chunk_ready(&position));

        // Fixed ordering: lock first, then check the epoch under the same lock, before mark_delivered.
        {
            let mut sender = sender.lock().unwrap();
            if epoch.load(std::sync::atomic::Ordering::Relaxed) == stale_epoch_snapshot {
                sender.mark_delivered(&[position]);
            }
        }

        // The newer batch's marker must survive: the chunk is still not ready.
        assert!(!sender.lock().unwrap().is_chunk_ready(&position));
    }
}

pub struct ClusterChunkEncoder {
    sent:
        FxHashMap<Vector2<i32>, Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>>,
    pending: FxHashMap<
        Vector2<i32>,
        (
            Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>,
            tokio::sync::oneshot::Receiver<Result<EncodedChunk, String>>,
        ),
    >,
    ready: FxHashMap<
        Vector2<i32>,
        (
            Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>,
            EncodedChunk,
        ),
    >,
}

#[derive(Clone, Copy)]
enum ClusterChunkEncoding {
    Java(JavaMinecraftVersion),
    Bedrock(i32),
}

impl ClusterChunkEncoder {
    pub fn forget(&mut self, position: Vector2<i32>) {
        self.ready.remove(&position);
        self.sent.remove(&position);
    }
    pub fn take_sent_presentations(
        &mut self,
    ) -> FxHashMap<Vector2<i32>, Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>>
    {
        std::mem::take(&mut self.sent)
    }
    pub fn new() -> Self {
        Self {
            pending: FxHashMap::default(),
            ready: FxHashMap::default(),
            sent: FxHashMap::default(),
        }
    }

    pub fn advance(
        &mut self,
        presentations: &std::collections::HashMap<
            pumpkin_cluster::protocol::ChunkKey,
            Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>,
        >,
        world: pumpkin_cluster::protocol::WorldId,
        positions: &FxHashSet<Vector2<i32>>,
        version: JavaMinecraftVersion,
    ) -> Result<(), String> {
        self.advance_with(
            presentations,
            world,
            positions,
            ClusterChunkEncoding::Java(version),
        )
    }

    pub fn advance_bedrock(
        &mut self,
        presentations: &std::collections::HashMap<
            pumpkin_cluster::protocol::ChunkKey,
            Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>,
        >,
        world: pumpkin_cluster::protocol::WorldId,
        positions: &FxHashSet<Vector2<i32>>,
        dimension: i32,
    ) -> Result<(), String> {
        self.advance_with(
            presentations,
            world,
            positions,
            ClusterChunkEncoding::Bedrock(dimension),
        )
    }

    fn advance_with(
        &mut self,
        presentations: &std::collections::HashMap<
            pumpkin_cluster::protocol::ChunkKey,
            Arc<crate::server::cluster_chunk_presentation::ChunkPresentation>,
        >,
        world: pumpkin_cluster::protocol::WorldId,
        positions: &FxHashSet<Vector2<i32>>,
        encoding: ClusterChunkEncoding,
    ) -> Result<(), String> {
        self.ready.retain(|position, (publication, _)| {
            positions.contains(position)
                && presentations
                    .get(&pumpkin_cluster::protocol::ChunkKey::new(
                        world, position.x, position.y,
                    ))
                    .is_some_and(|presentation| Arc::ptr_eq(presentation, publication))
        });
        let mut complete = Vec::new();
        for (position, (_, receiver)) in &mut self.pending {
            match receiver.try_recv() {
                Ok(result) => complete.push((*position, result)),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    complete.push((*position, Err("chunk encoder worker closed".into())))
                }
            }
        }
        for (position, result) in complete {
            let Some((publication, _)) = self.pending.remove(&position) else {
                continue;
            };
            let encoded = result?;
            let key = pumpkin_cluster::protocol::ChunkKey::new(world, position.x, position.y);
            if positions.contains(&position)
                && presentations
                    .get(&key)
                    .is_some_and(|value| Arc::ptr_eq(value, &publication))
            {
                self.ready.insert(position, (publication, encoded));
            }
        }
        let mut ordered = positions.iter().copied().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|position| (position.x, position.y));
        for position in ordered {
            if self.pending.len() >= 8 {
                break;
            }
            if self.ready.contains_key(&position) || self.pending.contains_key(&position) {
                continue;
            }
            let key = pumpkin_cluster::protocol::ChunkKey::new(world, position.x, position.y);
            let Some(presentation) = presentations.get(&key).cloned() else {
                continue;
            };
            if matches!(encoding, ClusterChunkEncoding::Java(_))
                && !presentation.admission_lighting_valid
                && presentation.lighting.is_none()
            {
                continue;
            }
            let publication = presentation.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.pending.insert(position, (publication, rx));
            rayon::spawn(move || {
                let result = match encoding {
                    ClusterChunkEncoding::Java(version) => {
                        crate::net::java::chunk_data::owned::encode(
                            key,
                            &presentation.image.document().root_tag,
                            &presentation.blocks,
                            &presentation.block_entities,
                            presentation.lighting.as_deref(),
                            version,
                        )
                        .map_err(|error| error.to_string())
                    }
                    ClusterChunkEncoding::Bedrock(dimension) => {
                        crate::net::bedrock::level_chunk::encode_presentation(
                            key,
                            &presentation,
                            dimension,
                        )
                        .map(|payload| (payload, None))
                        .map_err(|error| error.to_string())
                    }
                }
                .map(|(payload, light_payload)| EncodedChunk {
                    position,
                    payload,
                    light_payload,
                    chunk_ref: Weak::new(),
                })
                .map_err(|error| error.to_string());
                let _ = tx.send(result);
            });
        }
        Ok(())
    }
}

impl ChunkSender {
    pub fn prepare_encoded_batch(
        &mut self,
        encoder: &mut ClusterChunkEncoder,
        center: Vector2<i32>,
        epoch: u32,
        version: JavaMinecraftVersion,
    ) -> Option<(PreparedBatch, Vec<EncodedChunk>)> {
        if version >= JavaMinecraftVersion::V_1_20_2 && self.in_flight_batches >= self.max_in_flight
        {
            return None;
        }
        self.send_quota = (self.send_quota + self.desired_rate).min(self.desired_rate.max(1.0));
        let mut candidates = encoder
            .ready
            .keys()
            .filter(|position| self.pending_chunks.contains(position))
            .copied()
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|position| {
            let dx = i64::from(position.x) - i64::from(center.x);
            let dz = i64::from(position.y) - i64::from(center.y);
            (dx * dx + dz * dz, position.x, position.y)
        });
        let encoded = candidates
            .into_iter()
            .take(self.send_quota.floor() as usize)
            .filter_map(|position| {
                encoder.ready.remove(&position).map(|(publication, chunk)| {
                    encoder.sent.insert(position, publication);
                    chunk
                })
            })
            .collect::<Vec<_>>();
        if encoded.is_empty() {
            return None;
        }
        Some((
            PreparedBatch {
                chunks: Vec::new(),
                epoch_snapshot: epoch,
                target_version: version,
            },
            encoded,
        ))
    }
}
