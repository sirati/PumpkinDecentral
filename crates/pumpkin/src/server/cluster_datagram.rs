//! QUIC datagram send path for fused movement batches.
//!
//! Every server tick the fused movement bank arrives here as one encoded
//! [`TickBatch`](pumpkin_cluster::protocol::TickBatch). Position updates are
//! latency sensitive and loss tolerant, so this module slices them into
//! MTU-fitting [`PosDatagram`](pumpkin_cluster::codec::PosDatagram) payloads
//! and fans each payload out to every mesh peer over QUIC datagrams. Empty
//! ticks and oversized or unencodable chunks are dropped and counted instead
//! of stalling the tick thread; the send path only touches lock-free channel
//! operations and never blocks.
//!
//! Each [`PosUpdate`](pumpkin_cluster::protocol::PosUpdate) carries per-tick
//! pos/head/vel (position, velocity, head yaw/pitch), so one datagram fanout
//! per tick keeps remote ghosts moving, facing, and interpolating without a
//! reliable stream.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::codec::{decode_batch, encode_pos};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::TickBatch;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Conservative QUIC datagram payload ceiling below typical path MTUs.
pub const MAX_DATAGRAM_BYTES: usize = 1200;

/// Position updates packed into a single datagram payload.
pub const MAX_POS_PER_DATAGRAM: usize = 16;

struct DatagramOutbox {
    peers: Vec<ServerId>,
    datagram_out: mpsc::Sender<(ServerId, Vec<u8>)>,
}

static DATAGRAM_OUTBOX: OnceLock<DatagramOutbox> = OnceLock::new();
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
    let mut payloads = Vec::new();
    for chunk in batch.pos.chunks(MAX_POS_PER_DATAGRAM) {
        match encode_pos(chunk) {
            Ok(bytes) => {
                if bytes.len() > MAX_DATAGRAM_BYTES {
                    note_dropped("oversize");
                    continue;
                }
                payloads.push(bytes);
            }
            Err(error) => {
                warn!(%error, "cluster movement datagram encode failed");
                note_dropped("encode");
            }
        }
    }
    payloads
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
    let mut sent = 0_u64;
    for payload in pos_payloads(&batch) {
        for peer in &outbox.peers {
            if outbox.datagram_out.try_send((*peer, payload.clone())).is_ok() {
                sent = sent.saturating_add(1);
            } else {
                note_dropped("backpressure");
            }
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

pub fn forward_entity_datagrams(payloads: &[Vec<u8>]) {
    let Some(outbox) = DATAGRAM_OUTBOX.get() else {
        return;
    };
    if outbox.peers.is_empty() {
        return;
    }
    let mut sent = 0_u64;
    for payload in payloads {
        if payload.len() > MAX_DATAGRAM_BYTES {
            note_dropped("oversize");
            continue;
        }
        for peer in &outbox.peers {
            if outbox.datagram_out.try_send((*peer, payload.clone())).is_ok() {
                sent = sent.saturating_add(1);
            } else {
                note_dropped("backpressure");
            }
        }
    }
    if sent > 0 {
        DATAGRAMS_SENT.fetch_add(sent, Ordering::Relaxed);
        debug!(
            datagrams = sent,
            peers = outbox.peers.len(),
            "cluster entity datagrams forwarded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::codec::decode_pos;
    use pumpkin_cluster::identity::{PlayerSeq, PlayerSlot};
    use pumpkin_cluster::protocol::PosUpdate;
    use pumpkin_cluster::time::TickStamp;

    fn sample_pos(player: u16, seq: u16) -> PosUpdate {
        PosUpdate {
            gid: pumpkin_cluster::identity::GlobalPlayerId::new(ServerId(1), PlayerSlot(player)),
            seq: PlayerSeq(seq),
            tick: TickStamp(seq),
            pos: [f64::from(seq), 64.0, 0.0],
            vel: [0.0, 0.0, 0.0],
            yaw: 0.0,
            pitch: 0.0,
        }
    }

    #[test]
    fn chunks_updates_into_bounded_payloads() {
        let mut batch = TickBatch::new(TickStamp(7));
        for player in 0_u16..40_u16 {
            batch.pos.push(sample_pos(player, player));
        }
        let payloads = pos_payloads(&batch);
        assert_eq!(payloads.len(), 3);
        let mut total = 0_usize;
        for payload in &payloads {
            assert!(payload.len() <= MAX_DATAGRAM_BYTES);
            let datagram = decode_pos(payload).expect("pos datagram decodes");
            assert!(datagram.is_consistent());
            total += datagram.updates.len();
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
        update.pitch = -15.0;
        batch.pos.push(update);
        let payloads = pos_payloads(&batch);
        assert_eq!(payloads.len(), 1);
        let datagram = decode_pos(&payloads[0]).expect("pos datagram decodes");
        assert!(datagram.is_consistent());
        assert_eq!(datagram.updates.len(), 1);
        let roundtripped = datagram.updates[0];
        assert_eq!(roundtripped.pos, [10.0, 65.0, -4.0]);
        assert_eq!(roundtripped.vel, [1.5, -2.25, 0.75]);
        assert_eq!(roundtripped.yaw, 123.0);
        assert_eq!(roundtripped.pitch, -15.0);
    }

    #[test]
    fn forward_without_outbox_is_noop() {
        forward_fused_batch(&[0xFF, 0xFF]);
        assert_eq!(datagrams_sent(), 0);
    }
}
