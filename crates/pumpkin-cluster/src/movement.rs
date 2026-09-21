use std::cell::RefCell;
use std::collections::HashMap;

use crate::banks::Bank;
use crate::codec::PosDatagram;
use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{PosUpdate, TickBatch};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AcceptedPos {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: [f64; 3],
    pub vel: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
}

impl From<&PosUpdate> for AcceptedPos {
    fn from(update: &PosUpdate) -> Self {
        Self {
            gid: update.gid,
            seq: update.seq,
            tick: update.tick,
            pos: update.pos,
            vel: update.vel,
            yaw: update.yaw,
            pitch: update.pitch,
        }
    }
}

#[derive(Debug, Default)]
pub struct RemotePlayerTable {
    pub last_seq: HashMap<GlobalPlayerId, PlayerSeq>,
    pub ghosts: HashMap<GlobalPlayerId, AcceptedPos>,
}

impl RemotePlayerTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_update(&mut self, update: &PosUpdate) -> Option<AcceptedPos> {
        let fresh = match self.last_seq.get(&update.gid) {
            None => true,
            Some(seen) => update.seq.is_newer_than(*seen),
        };
        if !fresh {
            return None;
        }
        let accepted = AcceptedPos::from(update);
        self.last_seq.insert(update.gid, update.seq);
        self.ghosts.insert(update.gid, accepted);
        Some(accepted)
    }

    #[must_use]
    pub fn apply_pos_datagram(&mut self, datagram: &PosDatagram) -> Vec<AcceptedPos> {
        let mut accepted = Vec::with_capacity(datagram.updates.len());
        for update in &datagram.updates {
            if let Some(sample) = self.apply_update(update) {
                accepted.push(sample);
            }
        }
        accepted
    }

    #[must_use]
    pub fn ghost(&self, gid: &GlobalPlayerId) -> Option<&AcceptedPos> {
        self.ghosts.get(gid)
    }

    pub fn remove(&mut self, gid: &GlobalPlayerId) -> Option<AcceptedPos> {
        self.last_seq.remove(gid);
        self.ghosts.remove(gid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ghosts.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ghosts.is_empty()
    }
}

thread_local! {
    static LOCAL_POS_BANK: RefCell<Bank> = RefCell::new(Bank::new());
}

pub fn with_bank<F, R>(mut closure: F) -> R
where
    F: FnMut(&mut Bank) -> R,
{
    LOCAL_POS_BANK
        .try_with(|bank| match bank.try_borrow_mut() {
            Ok(mut guard) => closure(&mut guard),
            Err(_) => closure(&mut Bank::new()),
        })
        .unwrap_or_else(|_| closure(&mut Bank::new()))
}

pub fn sample_local_player(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: [f64; 3],
    vel: [f64; 3],
    yaw: f32,
    pitch: f32,
) {
    let update = PosUpdate {
        gid,
        seq,
        tick,
        pos,
        vel,
        yaw,
        pitch,
    };
    with_bank(|bank| bank.pos.push(update));
}

#[must_use]
pub fn drain_local_samples() -> Vec<PosUpdate> {
    with_bank(|bank| core::mem::take(&mut bank.pos))
}

/// Drains the thread-local position bank and fuses to one update per player.
///
/// Sampling stays lock-free: [`sample_local_player`] only pushes into the
/// thread-local [`Bank`](crate::banks::Bank), so producers never block and
/// take no locks. Call this once per tick to claim everything accumulated
/// since the previous tick.
///
/// Fusion is last-sample-wins per [`GlobalPlayerId`]: a player sampled
/// several times within one tick emits only its newest [`PosUpdate`], which
/// keeps batches small and matches the fuse side's expectation of at most
/// one position per player per tick. Output order is sorted by
/// [`GlobalPlayerId`] so the encoded batch is deterministic. The returned
/// [`TickBatch`] is stamped with `tick`; sampled updates keep the tick they
/// were captured with.
#[must_use]
pub fn fuse_local_batch(tick: TickStamp) -> TickBatch {
    let drained = drain_local_samples();
    let mut latest: HashMap<GlobalPlayerId, PosUpdate> = HashMap::with_capacity(drained.len());
    for update in drained {
        latest.insert(update.gid, update);
    }
    let mut pos: Vec<PosUpdate> = latest.into_values().collect();
    pos.sort_by_key(|update| update.gid);
    let mut batch = TickBatch::new(tick);
    batch.pos = pos;
    batch
}

/// Pumps one fused movement batch through the existing forward shim.
///
/// Drains and fuses via [`fuse_local_batch`], encodes with
/// [`crate::codec::encode_batch`], and hands the bytes to `forward` without
/// blocking. Wire `forward` to the existing `forward_movement_batch` shim so
/// encoded position batches reach the mesh on the fuse task's fanout path.
/// Skips empty ticks and encode failures, takes no locks, and never blocks
/// the tick thread.
///
/// Returns `true` when a batch was forwarded, `false` when there was nothing
/// to send or encoding failed.
#[must_use]
pub fn pump_local_batch(tick: TickStamp, forward: impl FnOnce(Vec<u8>)) -> bool {
    let batch = fuse_local_batch(tick);
    if batch.is_empty() {
        return false;
    }
    match crate::codec::encode_batch(&batch) {
        Ok(bytes) => {
            forward(bytes);
            true
        }
        Err(error) => {
            tracing::warn!(%error, "cluster movement fuse encode failed");
            false
        }
    }
}

#[must_use]
pub fn ingest_channel(
    capacity: usize,
) -> (
    tokio::sync::mpsc::Sender<PosDatagram>,
    tokio::sync::mpsc::Receiver<PosDatagram>,
) {
    tokio::sync::mpsc::channel(capacity.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(1), PlayerSlot(player))
    }

    fn sample(player: u16, seq: u16) -> PosUpdate {
        PosUpdate {
            gid: gid(player),
            seq: PlayerSeq(seq),
            tick: TickStamp(seq),
            pos: [f64::from(seq), 64.0, 0.0],
            vel: [0.0, 0.0, 0.0],
            yaw: 0.0,
            pitch: 0.0,
        }
    }

    #[test]
    fn first_packet_accepts_any_seq() {
        let mut table = RemotePlayerTable::new();
        let update = sample(1, 9000);
        assert_eq!(
            table.apply_update(&update),
            Some(AcceptedPos::from(&update))
        );
    }

    #[test]
    fn stale_and_duplicate_lose() {
        let mut table = RemotePlayerTable::new();
        assert!(table.apply_update(&sample(1, 10)).is_some());
        assert!(table.apply_update(&sample(1, 10)).is_none());
        assert!(table.apply_update(&sample(1, 9)).is_none());
        assert!(table.apply_update(&sample(1, 11)).is_some());
    }

    #[test]
    fn wrap_around_wins() {
        let mut table = RemotePlayerTable::new();
        assert!(table.apply_update(&sample(2, u16::MAX)).is_some());
        assert!(table.apply_update(&sample(2, 0)).is_some());
        assert_eq!(
            table.ghost(&gid(2)).unwrap().pos[0],
            f64::from(0u16)
        );
    }

    #[test]
    fn datagram_filters_mixed_batch() {
        let mut table = RemotePlayerTable::new();
        assert!(table.apply_update(&sample(3, 5)).is_some());
        let datagram = PosDatagram {
            count: 3,
            updates: vec![sample(3, 4), sample(3, 6), sample(4, 1)],
        };
        let accepted = table.apply_pos_datagram(&datagram);
        assert_eq!(accepted.len(), 2);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn remove_forgets_player() {
        let mut table = RemotePlayerTable::new();
        assert!(table.apply_update(&sample(5, 1)).is_some());
        assert!(table.remove(&gid(5)).is_some());
        assert!(table.is_empty());
        assert!(table.apply_update(&sample(5, 1)).is_some());
    }

    #[test]
    fn local_sample_bank_roundtrip() {
        let _ = drain_local_samples();
        sample_local_player(gid(7), PlayerSeq(3), TickStamp(3), [1.0, 2.0, 3.0], [0.0; 3], 90.0, 0.0);
        let drained = drain_local_samples();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].pos, [1.0, 2.0, 3.0]);
        assert!(drain_local_samples().is_empty());
    }
}
