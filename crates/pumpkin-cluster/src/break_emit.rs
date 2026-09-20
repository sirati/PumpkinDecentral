//! Break-block emit: capture intent, mint undo, judge remote applies.
//!
//! Pipeline: [`capture_break`] packs the authoritative intent including the
//! `expected_old_state` snapshot, [`make_undo`] mints the compensation for a
//! rejected local write, and [`apply_remote_break`] judges a remote intent
//! against current ground truth.
//!
//! Verdict contract (`drops skipped`):
//! - [`RemoteBreakDecision::Accept`] means the caller performs the block break
//!   with loot drops skipped (`SKIP_DROPS`) and keeps the returned [`BlockUndo`].
//! - [`RemoteBreakDecision::RejectStale`] means expectation mismatched ground
//!   truth, so the caller drops the packet without mutating the world; the
//!   `expected`/`current` pair is the evidence for logs and metrics.
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{BlockPos, BlockUndo, BreakBlockUpdate, ChunkAddr};
use crate::time::TickStamp;

const SEQ_STRIPES: usize = 256;

static BREAK_SEQ: LazyLock<Box<[AtomicU16]>> =
    LazyLock::new(|| (0..SEQ_STRIPES).map(|_| AtomicU16::new(0)).collect());

fn stripe_for(gid: GlobalPlayerId) -> usize {
    let mixed = gid
        .server
        .0
        .rotate_left(5)
        ^ gid.player.0.wrapping_mul(0x9E37);
    (mixed as usize) % SEQ_STRIPES
}

pub fn next_break_seq(gid: GlobalPlayerId) -> PlayerSeq {
    let stripe = stripe_for(gid);
    PlayerSeq(BREAK_SEQ[stripe].fetch_add(1, Ordering::Relaxed))
}

#[must_use]
pub fn chunk_of_block(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr {
        x: x.div_euclid(16),
        z: z.div_euclid(16),
    }
}

/// Sentinel undo count: breaks never carry an inventory delta.
pub const BREAK_UNDO_NO_INVENTORY_DELTA: u8 = u8::MAX;

/// Packs the authoritative break intent, freezing the `expected_old_state`
/// snapshot the remote verdict later compares against.
#[must_use]
pub const fn capture_break(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    expected_old_state: u16,
    chunk: ChunkAddr,
) -> BreakBlockUpdate {
    BreakBlockUpdate {
        gid,
        seq,
        tick,
        pos,
        expected_old_state,
        chunk,
    }
}

/// Mints the compensation for `old_state` with no inventory delta attached.
#[must_use]
pub const fn make_undo(old_state: u16) -> BlockUndo {
    BlockUndo {
        old_state,
        count_before: BREAK_UNDO_NO_INVENTORY_DELTA,
    }
}

/// Expected-old-state verdict: accept carries the undo, stale carries the
/// evidence and must be dropped without mutating the world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteBreakDecision {
    Accept {
        undo: BlockUndo,
    },
    RejectStale {
        expected: u16,
        current: u16,
    },
}

/// Atomic accept/reject counters for the remote break verdict.
#[derive(Debug, Default)]
pub struct BreakMetrics {
    accepted: AtomicU64,
    rejected_stale: AtomicU64,
}

impl BreakMetrics {
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

/// Judges a remote break against `current_state`: match accepts with an undo
/// for the expected state, mismatch rejects as stale with both states as
/// evidence. Rejection mutates nothing except the stale counter; the caller
/// owns the drops-skipped break versus drop-packet split.
#[must_use]
pub fn apply_remote_break(
    current_state: u16,
    update: &BreakBlockUpdate,
    metrics: &BreakMetrics,
) -> RemoteBreakDecision {
    if current_state == update.expected_old_state {
        metrics.accepted.fetch_add(1, Ordering::Relaxed);
        RemoteBreakDecision::Accept {
            undo: make_undo(update.expected_old_state),
        }
    } else {
        metrics.rejected_stale.fetch_add(1, Ordering::Relaxed);
        RemoteBreakDecision::RejectStale {
            expected: update.expected_old_state,
            current: current_state,
        }
    }
}

/// Per-player sequence clock handing out `0, 1, 2, ...` wraps per player.
#[derive(Debug, Default)]
pub struct BreakSeqClock {
    next: HashMap<GlobalPlayerId, u16>,
}

impl BreakSeqClock {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    const GID: GlobalPlayerId = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));

    fn sample_update(expected_old_state: u16) -> BreakBlockUpdate {
        capture_break(
            GID,
            PlayerSeq(7),
            TickStamp(9),
            BlockPos { x: 1, y: 2, z: 3 },
            expected_old_state,
            ChunkAddr { x: 0, z: 0 },
        )
    }

    #[test]
    fn capture_packs_every_field() {
        let update = sample_update(12);
        assert_eq!(update.gid, GID);
        assert_eq!(update.seq, PlayerSeq(7));
        assert_eq!(update.tick, TickStamp(9));
        assert_eq!(update.pos, BlockPos { x: 1, y: 2, z: 3 });
        assert_eq!(update.expected_old_state, 12);
        assert_eq!(update.chunk, ChunkAddr { x: 0, z: 0 });
    }

    #[test]
    fn undo_marks_no_inventory_delta() {
        let undo = make_undo(41);
        assert_eq!(undo.old_state, 41);
        assert_eq!(undo.count_before, BREAK_UNDO_NO_INVENTORY_DELTA);
    }

    #[test]
    fn matching_state_accepts_and_counts() {
        let metrics = BreakMetrics::new();
        let decision = apply_remote_break(12, &sample_update(12), &metrics);
        assert_eq!(
            decision,
            RemoteBreakDecision::Accept {
                undo: make_undo(12)
            }
        );
        assert_eq!(metrics.accepted(), 1);
        assert_eq!(metrics.rejected_stale(), 0);
    }

    #[test]
    fn stale_state_rejects_restores_nothing_and_counts() {
        let metrics = BreakMetrics::new();
        let decision = apply_remote_break(77, &sample_update(12), &metrics);
        assert_eq!(
            decision,
            RemoteBreakDecision::RejectStale {
                expected: 12,
                current: 77
            }
        );
        assert_eq!(metrics.accepted(), 0);
        assert_eq!(metrics.rejected_stale(), 1);
    }

    #[test]
    fn seq_clock_starts_at_zero_and_increments_per_player() {
        let mut clock = BreakSeqClock::new();
        let other = GlobalPlayerId::new(ServerId(1), PlayerSlot(3));
        assert_eq!(clock.issue(GID), PlayerSeq(0));
        assert_eq!(clock.issue(GID), PlayerSeq(1));
        assert_eq!(clock.issue(other), PlayerSeq(0));
        assert_eq!(clock.issue(GID), PlayerSeq(2));
    }
}
