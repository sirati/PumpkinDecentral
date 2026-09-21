use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{BlockPos, BlockUndo, ChunkAddr, PlaceBlockUpdate};
use crate::time::TickStamp;

const SEQ_STRIPES: usize = 256;

static PLACE_SEQ: LazyLock<Box<[AtomicU16]>> =
    LazyLock::new(|| (0..SEQ_STRIPES).map(|_| AtomicU16::new(0)).collect());

fn stripe_for(gid: GlobalPlayerId) -> usize {
    let mixed = gid
        .server
        .0
        .rotate_left(5)
        ^ gid.player.0.wrapping_mul(0x9E37);
    (mixed as usize) % SEQ_STRIPES
}

pub fn next_place_seq(gid: GlobalPlayerId) -> PlayerSeq {
    let stripe = stripe_for(gid);
    PlayerSeq(PLACE_SEQ[stripe].fetch_add(1, Ordering::Relaxed))
}

#[derive(Debug, Default)]
pub struct PlaceSeqClock {
    next: HashMap<GlobalPlayerId, u16>,
}

impl PlaceSeqClock {
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

#[must_use]
pub fn capture_place(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    new_state: u16,
    inv: u8,
    slot: u8,
    item: u16,
    count_before: u8,
    count_after: u8,
    chunk: ChunkAddr,
) -> PlaceBlockUpdate {
    PlaceBlockUpdate {
        gid,
        seq,
        tick,
        pos,
        expected_old_state,
        new_state,
        inv,
        slot,
        item,
        count_before,
        count_after,
        chunk,
    }
}

#[must_use]
pub fn place_inv_loc(update: &PlaceBlockUpdate) -> crate::inventory::InvLoc {
    crate::inventory::InvLoc::new(update.inv, u16::from(update.slot))
}

#[must_use]
pub fn place_inv_precondition_met(
    update: &PlaceBlockUpdate,
    actual_item: u16,
    actual_count: u8,
) -> bool {
    crate::inventory::inv_precondition_met(
        actual_item,
        actual_count,
        update.item,
        update.count_before,
    )
}

#[must_use]
pub fn chunk_of_block(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr {
        x: x.div_euclid(16),
        z: z.div_euclid(16),
    }
}

#[must_use]
pub fn place_count_after(count_before: u8, consume: bool) -> u8 {
    if consume {
        count_before.saturating_sub(1)
    } else {
        count_before
    }
}

#[must_use]
pub fn try_place_count_after(count_before: u8, consume: bool) -> Option<u8> {
    if consume && count_before == 0 {
        None
    } else {
        Some(place_count_after(count_before, consume))
    }
}

#[must_use]
pub const fn place_count_before(count_after: u8, consume: bool) -> u8 {
    if consume {
        count_after.saturating_add(1)
    } else {
        count_after
    }
}

#[must_use]
pub const fn make_place_undo(old_state: u16, count_before: u8) -> BlockUndo {
    BlockUndo {
        old_state,
        count_before,
    }
}

#[must_use]
pub const fn is_place_stale(expected_old_state: u16, current_state: u16) -> bool {
    expected_old_state != current_state
}

#[must_use]
pub const fn is_place_duplicate(new_state: u16, current_state: u16) -> bool {
    new_state == current_state
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemotePlaceVerdict {
    pub accepted: bool,
    pub undo: BlockUndo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePlaceDecision {
    Accept { undo: BlockUndo },
    RejectStale { expected: u16, current: u16 },
    RejectDuplicate { state: u16 },
}

#[derive(Debug, Default)]
pub struct PlaceMetrics {
    accepted: AtomicU64,
    rejected_stale: AtomicU64,
    rejected_duplicate: AtomicU64,
}

impl PlaceMetrics {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            accepted: AtomicU64::new(0),
            rejected_stale: AtomicU64::new(0),
            rejected_duplicate: AtomicU64::new(0),
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

    #[must_use]
    pub fn rejected_duplicate(&self) -> u64 {
        self.rejected_duplicate.load(Ordering::Relaxed)
    }
}

#[must_use]
pub fn apply_remote_place(
    update: &PlaceBlockUpdate,
    local_state: u16,
    local_count: u8,
) -> RemotePlaceVerdict {
    RemotePlaceVerdict {
        accepted: !is_place_duplicate(update.new_state, local_state),
        undo: make_place_undo(local_state, local_count),
    }
}

#[must_use]
pub fn apply_remote_place_expected(
    update: &PlaceBlockUpdate,
    expected_old_state: u16,
    current_state: u16,
    count_before: u8,
    metrics: &PlaceMetrics,
) -> RemotePlaceDecision {
    if is_place_stale(expected_old_state, current_state) {
        metrics.rejected_stale.fetch_add(1, Ordering::Relaxed);
        RemotePlaceDecision::RejectStale {
            expected: expected_old_state,
            current: current_state,
        }
    } else if is_place_duplicate(update.new_state, current_state) {
        metrics
            .rejected_duplicate
            .fetch_add(1, Ordering::Relaxed);
        RemotePlaceDecision::RejectDuplicate {
            state: current_state,
        }
    } else {
        metrics.accepted.fetch_add(1, Ordering::Relaxed);
        RemotePlaceDecision::Accept {
            undo: make_place_undo(current_state, count_before),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn sample_update(new_state: u16) -> PlaceBlockUpdate {
        capture_place(
            gid(1, 1),
            PlayerSeq(3),
            TickStamp(9),
            BlockPos { x: 0, y: 64, z: 0 },
            0,
            new_state,
            crate::inventory::INV_MAIN,
            0,
            40,
            64,
            63,
            ChunkAddr { x: 0, z: 0 },
        )
    }

    #[test]
    fn seq_increases_per_gid() {
        let id = gid(7, 41);
        let first = next_place_seq(id);
        let second = next_place_seq(id);
        assert!(second.is_newer_than(first));
    }

    #[test]
    fn capture_round_trips_fields() {
        let update = capture_place(
            gid(1, 2),
            PlayerSeq(3),
            TickStamp(9),
            BlockPos { x: 1, y: 2, z: 3 },
            0,
            12,
            crate::inventory::INV_MAIN,
            4,
            40,
            64,
            63,
            ChunkAddr { x: 0, z: 0 },
        );
        assert_eq!(update.new_state, 12);
        assert_eq!(update.slot, 4);
        assert_eq!(update.item, 40);
        assert_eq!(update.count_before, 64);
        assert_eq!(update.count_after, 63);
    }

    #[test]
    fn chunk_of_block_handles_negatives() {
        assert_eq!(chunk_of_block(0, 0), ChunkAddr { x: 0, z: 0 });
        assert_eq!(chunk_of_block(15, 15), ChunkAddr { x: 0, z: 0 });
        assert_eq!(chunk_of_block(16, -1), ChunkAddr { x: 1, z: -1 });
        assert_eq!(chunk_of_block(-16, -16), ChunkAddr { x: -1, z: -1 });
    }

    #[test]
    fn remote_place_accepts_fresh_rejects_duplicate() {
        let update = sample_update(12);
        let fresh = apply_remote_place(&update, 7, 64);
        assert!(fresh.accepted);
        assert_eq!(
            fresh.undo,
            BlockUndo {
                old_state: 7,
                count_before: 64
            }
        );
        let stale = apply_remote_place(&update, 12, 63);
        assert!(!stale.accepted);
        assert_eq!(stale.undo.count_before, 63);
    }

    #[test]
    fn seq_clock_starts_at_zero_and_increments_per_player() {
        let mut clock = PlaceSeqClock::new();
        let other = gid(1, 3);
        let id = gid(1, 2);
        assert_eq!(clock.issue(id), PlayerSeq(0));
        assert_eq!(clock.issue(id), PlayerSeq(1));
        assert_eq!(clock.issue(other), PlayerSeq(0));
        assert_eq!(clock.issue(id), PlayerSeq(2));
    }

    #[test]
    fn count_after_saturates_and_passes_through() {
        assert_eq!(place_count_after(64, true), 63);
        assert_eq!(place_count_after(0, true), 0);
        assert_eq!(place_count_after(64, false), 64);
    }

    #[test]
    fn atomic_stack_rejects_empty_consume() {
        assert_eq!(try_place_count_after(0, true), None);
        assert_eq!(try_place_count_after(1, true), Some(0));
        assert_eq!(try_place_count_after(64, true), Some(63));
        assert_eq!(try_place_count_after(0, false), Some(0));
        assert_eq!(try_place_count_after(64, false), Some(64));
    }

    #[test]
    fn count_before_inverts_consume() {
        assert_eq!(place_count_before(63, true), 64);
        assert_eq!(place_count_before(0, true), 1);
        assert_eq!(place_count_before(64, false), 64);
        assert_eq!(place_count_before(0, false), 0);
    }

    #[test]
    fn stale_and_duplicate_gates_split() {
        assert!(is_place_stale(7, 9));
        assert!(!is_place_stale(7, 7));
        assert!(is_place_duplicate(12, 12));
        assert!(!is_place_duplicate(12, 7));
    }

    #[test]
    fn expected_state_accepts_and_counts() {
        let metrics = PlaceMetrics::new();
        let decision = apply_remote_place_expected(&sample_update(12), 7, 7, 64, &metrics);
        assert_eq!(
            decision,
            RemotePlaceDecision::Accept {
                undo: make_place_undo(7, 64)
            }
        );
        assert_eq!(metrics.accepted(), 1);
        assert_eq!(metrics.rejected_stale(), 0);
        assert_eq!(metrics.rejected_duplicate(), 0);
    }

    #[test]
    fn expected_state_mismatch_rejects_stale_and_counts() {
        let metrics = PlaceMetrics::new();
        let decision = apply_remote_place_expected(&sample_update(12), 7, 9, 64, &metrics);
        assert_eq!(
            decision,
            RemotePlaceDecision::RejectStale {
                expected: 7,
                current: 9
            }
        );
        assert_eq!(metrics.accepted(), 0);
        assert_eq!(metrics.rejected_stale(), 1);
        assert_eq!(metrics.rejected_duplicate(), 0);
    }

    #[test]
    fn matching_new_state_rejects_duplicate_and_counts() {
        let metrics = PlaceMetrics::new();
        let decision = apply_remote_place_expected(&sample_update(12), 12, 12, 63, &metrics);
        assert_eq!(
            decision,
            RemotePlaceDecision::RejectDuplicate { state: 12 }
        );
        assert_eq!(metrics.accepted(), 0);
        assert_eq!(metrics.rejected_stale(), 0);
        assert_eq!(metrics.rejected_duplicate(), 1);
    }
}
