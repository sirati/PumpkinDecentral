use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};

use pumpkin_cluster::chunks::Directory;
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::{ChunkKey, PosUpdate, TickBatch};
#[cfg(test)]
use pumpkin_cluster::wire::MovementBatch;
use pumpkin_cluster::wire::{encode_movement, movement_batch_len};
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub const UDP_PATH_BYTES: usize = 1200;
pub const MAX_DATAGRAM_BYTES: usize = UDP_PATH_BYTES;

/// Position updates packed into a single datagram payload.
pub const MAX_POS_PER_DATAGRAM: usize = 1024;

struct DatagramOutbox {
    peers: Vec<ServerId>,
    datagram_out: mpsc::Sender<(ServerId, Vec<u8>)>,
}

static DATAGRAM_OUTBOX: OnceLock<DatagramOutbox> = OnceLock::new();
static HOLDER_DIRECTORY: LazyLock<arc_swap::ArcSwap<Directory>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(Directory::new()));
static PEER_DATAGRAM_LIMITS: LazyLock<arc_swap::ArcSwap<HashMap<ServerId, usize>>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));
static LOCAL_PLAYER_CHUNKS: LazyLock<arc_swap::ArcSwap<HashMap<GlobalPlayerId, ChunkKey>>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));
static DATAGRAMS_SENT: AtomicU64 = AtomicU64::new(0);
static DATAGRAMS_DROPPED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn datagrams_sent() -> u64 {
    DATAGRAMS_SENT.load(Ordering::Relaxed)
}

#[must_use]
pub fn datagrams_dropped() -> u64 {
    DATAGRAMS_DROPPED.load(Ordering::Relaxed)
}

pub fn install_datagram_outbox(
    peers: Vec<ServerId>,
    datagram_out: mpsc::Sender<(ServerId, Vec<u8>)>,
) {
    let _ = DATAGRAM_OUTBOX.set(DatagramOutbox {
        peers,
        datagram_out,
    });
}

pub fn publish_holder_directory(directory: &Directory) {
    HOLDER_DIRECTORY.store(Arc::new(directory.clone()));
}

pub fn publish_peer_datagram_limits(limits: HashMap<ServerId, usize>) {
    PEER_DATAGRAM_LIMITS.store(Arc::new(limits));
}

#[must_use]
pub fn holders_for_chunk(chunk: ChunkKey) -> Vec<u16> {
    HOLDER_DIRECTORY.load().sorted_holders(&chunk)
}

#[must_use]
pub fn holder_directory() -> Arc<Directory> {
    HOLDER_DIRECTORY.load_full()
}

pub fn publish_local_player_chunks(chunks: HashMap<GlobalPlayerId, ChunkKey>) {
    LOCAL_PLAYER_CHUNKS.store(Arc::new(chunks));
}

#[must_use]
pub fn holders_for_player(gid: GlobalPlayerId) -> Vec<u16> {
    LOCAL_PLAYER_CHUNKS
        .load()
        .get(&gid)
        .map_or_else(Vec::new, |chunk| holders_for_chunk(*chunk))
}

fn note_dropped(reason: &str) {
    DATAGRAMS_DROPPED.fetch_add(1, Ordering::Relaxed);
    debug!(reason = reason, "cluster movement datagram dropped");
}

/// Slices a fused batch's position updates into datagram-sized payloads.
///
/// Each slice keeps the full per-tick pos/head/vel triplet (position,
/// velocity, head yaw/pitch) carried by
/// [`PosUpdate`](pumpkin_cluster::protocol::PosUpdate).
///
/// Returns an empty vector when the batch carries no position updates.
#[must_use]
pub fn pos_payloads(batch: &TickBatch) -> Vec<Vec<u8>> {
    encode_pos_payloads(&batch.pos, MAX_DATAGRAM_BYTES)
}

fn encode_pos_payloads(updates: &[PosUpdate], max_payload: usize) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    let mut start = 0;
    while start < updates.len() {
        let limit = updates
            .len()
            .min(start.saturating_add(MAX_POS_PER_DATAGRAM));
        let mut end = start;
        while end < limit {
            let candidate = &updates[start..=end];
            if movement_batch_len(candidate.len()).is_none_or(|len| len > max_payload) {
                break;
            }
            end += 1;
        }
        if end == start {
            note_dropped("oversize-record");
            start += 1;
            continue;
        }
        match encode_movement(&updates[start..end]) {
            Ok(bytes) => {
                payloads.push(bytes.to_vec());
            }
            Err(error) => {
                warn!(?error, "cluster movement datagram encode failed");
                note_dropped("encode");
            }
        }
        start = end;
    }
    payloads
}

fn update_chunk(update: &PosUpdate) -> ChunkKey {
    ChunkKey::new(
        update.world,
        (update.pos[0].floor() as i32).div_euclid(16),
        (update.pos[2].floor() as i32).div_euclid(16),
    )
}

fn routed_pos_payloads(
    batch: &TickBatch,
    holders: impl Fn(ChunkKey) -> Vec<u16>,
    limits: &HashMap<ServerId, usize>,
) -> Vec<(ServerId, Vec<u8>)> {
    let mut by_peer: BTreeMap<u16, Vec<PosUpdate>> = BTreeMap::new();
    for update in &batch.pos {
        for peer in holders(update_chunk(update)) {
            by_peer.entry(peer).or_default().push(*update);
        }
    }
    let mut routed = Vec::new();
    for (peer, updates) in by_peer {
        let peer = ServerId(peer);
        let max_payload = limits.get(&peer).copied().unwrap_or(MAX_DATAGRAM_BYTES);
        for bytes in encode_pos_payloads(&updates, max_payload) {
            routed.push((peer, bytes));
        }
    }
    routed
}

/// Forwards one fused batch per tick to every peer over QUIC datagrams.
///
/// Decodes the batch, slices its per-tick pos/head/vel updates with
/// [`pos_payloads`] and queues one datagram per payload per peer. Does
/// nothing when no outbox is installed, when there are no peers, or when the
/// batch carries no position updates.
pub fn forward_fused_batch(batch_bytes: &[u8]) {
    let Some(outbox) = DATAGRAM_OUTBOX.get() else {
        return;
    };
    if outbox.peers.is_empty() {
        return;
    }
    let batch = match decode_batch(batch_bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster fused batch decode failed");
            note_dropped("decode");
            return;
        }
    };
    if batch.pos.is_empty() {
        return;
    }
    let directory = HOLDER_DIRECTORY.load();
    let limits = PEER_DATAGRAM_LIMITS.load();
    let mut sent = 0_u64;
    for (peer, payload) in
        routed_pos_payloads(&batch, |chunk| directory.sorted_holders(&chunk), &limits)
    {
        if outbox.datagram_out.try_send((peer, payload)).is_ok() {
            sent = sent.saturating_add(1);
        } else {
            note_dropped("backpressure");
        }
    }
    if sent > 0 {
        DATAGRAMS_SENT.fetch_add(sent, Ordering::Relaxed);
        debug!(
            datagrams = sent,
            peers = outbox.peers.len(),
            bytes = batch_bytes.len(),
            "cluster movement datagrams forwarded"
        );
    }
}

pub fn forward_entity_datagram_to(peer: ServerId, payload: Vec<u8>) {
    let Some(outbox) = DATAGRAM_OUTBOX.get() else {
        return;
    };
    let max_payload = peer_datagram_payload_limit(peer);
    if payload.len() > max_payload {
        note_dropped("oversize");
        return;
    }
    if outbox.datagram_out.try_send((peer, payload)).is_ok() {
        DATAGRAMS_SENT.fetch_add(1, Ordering::Relaxed);
    } else {
        note_dropped("backpressure");
    }
}

#[must_use]
pub fn peer_datagram_payload_limit(peer: ServerId) -> usize {
    PEER_DATAGRAM_LIMITS
        .load()
        .get(&peer)
        .copied()
        .unwrap_or(MAX_DATAGRAM_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{PlayerSeq, PlayerSlot};
    use pumpkin_cluster::protocol::PosUpdate;
    use pumpkin_cluster::time::TickStamp;

    fn sample_pos(player: u16, seq: u16) -> PosUpdate {
        PosUpdate {
            gid: pumpkin_cluster::identity::GlobalPlayerId::new(ServerId(1), PlayerSlot(player)),
            seq: PlayerSeq(seq),
            tick: TickStamp(seq),
            world: pumpkin_cluster::protocol::WorldId::new(0, 7),
            pos: [f64::from(seq), 64.0, 0.0],
            vel: [0.0, 0.0, 0.0],
            yaw: 0.0,
            head_yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
        }
    }

    #[test]
    fn chunks_updates_into_bounded_payloads() {
        let mut batch = TickBatch::new(TickStamp(7));
        for player in 0_u16..40_u16 {
            batch.pos.push(sample_pos(player, player));
        }
        let payloads = pos_payloads(&batch);
        assert!(payloads.len() > 1);
        let mut total = 0_usize;
        for payload in &payloads {
            assert!(payload.len() <= MAX_DATAGRAM_BYTES);
            let datagram =
                MovementBatch::decode(payload.clone().into()).expect("pos datagram decodes");
            total += datagram.len();
        }
        assert_eq!(total, 40);
    }

    #[test]
    fn empty_batch_yields_no_payloads() {
        let batch = TickBatch::new(TickStamp(1));
        assert!(pos_payloads(&batch).is_empty());
    }

    #[test]
    fn movement_payloads_preserve_pos_head_vel() {
        let mut batch = TickBatch::new(TickStamp(9));
        let mut update = sample_pos(3, 6);
        update.pos = [10.0, 65.0, -4.0];
        update.vel = [1.5, -2.25, 0.75];
        update.yaw = 123.0;
        update.head_yaw = 100.0;
        update.pitch = -15.0;
        update.on_ground = false;
        batch.pos.push(update);
        let payloads = pos_payloads(&batch);
        assert_eq!(payloads.len(), 1);
        let datagram =
            MovementBatch::decode(payloads[0].clone().into()).expect("pos datagram decodes");
        assert_eq!(datagram.len(), 1);
        let roundtripped = datagram.updates().next().expect("movement record");
        assert_eq!(roundtripped.pos, [10.0, 65.0, -4.0]);
        assert_eq!(roundtripped.vel, [1.5, -2.25, 0.75]);
        assert_eq!(roundtripped.yaw, 123.0);
        assert_eq!(roundtripped.head_yaw, 100.0);
        assert_eq!(roundtripped.pitch, -15.0);
        assert!(!roundtripped.on_ground);
    }

    #[test]
    fn movement_is_partitioned_by_current_chunk_holders() {
        let mut batch = TickBatch::new(TickStamp(9));
        let mut left = sample_pos(1, 1);
        left.pos = [1.0, 64.0, 1.0];
        let mut right = sample_pos(2, 1);
        right.pos = [17.0, 64.0, 1.0];
        batch.pos.extend([left, right]);
        let routed = routed_pos_payloads(
            &batch,
            |chunk| match chunk.x() {
                0 => vec![2],
                1 => vec![3],
                _ => Vec::new(),
            },
            &HashMap::new(),
        );
        assert_eq!(routed.len(), 2);
        let peer_two = routed
            .iter()
            .find(|(peer, _)| *peer == ServerId(2))
            .unwrap();
        let peer_three = routed
            .iter()
            .find(|(peer, _)| *peer == ServerId(3))
            .unwrap();
        let two = MovementBatch::decode(peer_two.1.clone().into()).unwrap();
        let three = MovementBatch::decode(peer_three.1.clone().into()).unwrap();
        assert_eq!(
            two.updates().map(|update| update.gid).collect::<Vec<_>>(),
            vec![left.gid]
        );
        assert_eq!(
            three.updates().map(|update| update.gid).collect::<Vec<_>>(),
            vec![right.gid]
        );
    }

    #[test]
    fn negotiated_peer_limit_changes_packetization_without_losing_records() {
        let mut batch = TickBatch::new(TickStamp(9));
        for player in 0..30 {
            batch.pos.push(sample_pos(player, player));
        }
        let two_records = movement_batch_len(2).unwrap();
        let limits = HashMap::from([(ServerId(2), two_records)]);
        let routed = routed_pos_payloads(&batch, |_| vec![2], &limits);
        assert!(routed.len() > 1);
        assert!(
            routed
                .iter()
                .all(|(_, payload)| payload.len() <= two_records)
        );
        let delivered: usize = routed
            .iter()
            .map(|(_, payload)| {
                MovementBatch::decode(payload.clone().into())
                    .unwrap()
                    .updates()
                    .count()
            })
            .sum();
        assert_eq!(delivered, batch.pos.len());
    }

    #[test]
    fn forward_without_outbox_is_noop() {
        forward_fused_batch(&[0xFF, 0xFF]);
        assert_eq!(datagrams_sent(), 0);
    }
}
