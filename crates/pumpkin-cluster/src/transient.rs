use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use crate::banks::Bank;
use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{BlockPos, BreakAnimUpdate, EatAbortUpdate, EatStartUpdate};
use crate::time::TickStamp;

pub const BREAK_ANIM_STOP_STAGE: u8 = 255;

thread_local! {
    static NEXT_SEQ: RefCell<HashMap<GlobalPlayerId, u16>> =
        RefCell::new(HashMap::new());
    static TRANSIENT_BANK: RefCell<Bank> = RefCell::new(Bank::new());
    static TRANSIENT_STATS: Cell<TransientCounters> =
        Cell::new(TransientCounters::new());
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransientCounters {
    pub emitted: u64,
    pub skipped_no_gid: u64,
    pub dropped: u64,
}

impl TransientCounters {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            emitted: 0,
            skipped_no_gid: 0,
            dropped: 0,
        }
    }
}

#[must_use]
pub const fn capture_eat_start(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    slot: u8,
    item: u16,
    count: u8,
) -> EatStartUpdate {
    EatStartUpdate {
        gid,
        seq,
        tick,
        slot,
        item,
        count,
    }
}

#[must_use]
pub const fn eat_inv_precondition_met(
    update: &EatStartUpdate,
    actual_item: u16,
    actual_count: u8,
) -> bool {
    crate::inventory::inv_precondition_met(
        actual_item,
        actual_count,
        update.item,
        update.count,
    )
}

#[must_use]
pub const fn capture_eat_abort(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
) -> EatAbortUpdate {
    EatAbortUpdate { gid, seq, tick }
}

#[must_use]
pub const fn capture_break_anim(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
    stage: u8,
) -> BreakAnimUpdate {
    BreakAnimUpdate {
        gid,
        seq,
        tick,
        pos,
        stage,
    }
}

#[must_use]
pub const fn capture_break_anim_stop(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    pos: BlockPos,
) -> BreakAnimUpdate {
    capture_break_anim(gid, seq, tick, pos, BREAK_ANIM_STOP_STAGE)
}

#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub const fn tick_from_counter(counter: i32) -> TickStamp {
    TickStamp(counter as u16)
}

#[must_use]
pub fn next_seq(gid: GlobalPlayerId) -> PlayerSeq {
    NEXT_SEQ.with(|counters| {
        let mut counters = counters.borrow_mut();
        let seq = counters.get(&gid).copied().unwrap_or(0);
        counters.insert(gid, seq.wrapping_add(1));
        PlayerSeq(seq)
    })
}

pub fn with_transient_bank<R>(apply: impl FnOnce(&mut Bank) -> R) -> R {
    TRANSIENT_BANK.with(|bank| apply(&mut bank.borrow_mut()))
}

#[must_use]
pub fn transient_bank_is_empty() -> bool {
    TRANSIENT_BANK.with(|bank| bank.borrow().is_empty())
}

#[must_use]
pub fn drain_transient_bank() -> Bank {
    TRANSIENT_BANK.with(|bank| std::mem::take(&mut *bank.borrow_mut()))
}

#[must_use]
pub fn transient_counters() -> TransientCounters {
    TRANSIENT_STATS.with(|stats| stats.get())
}

pub fn note_transient_dropped(count: u64) {
    TRANSIENT_STATS.with(|stats| {
        let mut current = stats.get();
        current.dropped = current.dropped.saturating_add(count);
        stats.set(current);
    });
}

fn push_transient(
    gid: Option<GlobalPlayerId>,
    push: impl FnOnce(&mut Bank, GlobalPlayerId, PlayerSeq),
) {
    let Some(gid) = gid else {
        TRANSIENT_STATS.with(|stats| {
            let mut current = stats.get();
            current.skipped_no_gid = current.skipped_no_gid.saturating_add(1);
            stats.set(current);
        });
        return;
    };
    let seq = next_seq(gid);
    TRANSIENT_BANK.with(|bank| push(&mut bank.borrow_mut(), gid, seq));
    TRANSIENT_STATS.with(|stats| {
        let mut current = stats.get();
        current.emitted = current.emitted.saturating_add(1);
        stats.set(current);
    });
}

pub fn emit_eat_start(
    gid: Option<GlobalPlayerId>,
    tick: TickStamp,
    slot: u8,
    item: u16,
    count: u8,
) {
    push_transient(gid, |bank, gid, seq| {
        bank.eat_start.push(capture_eat_start(gid, seq, tick, slot, item, count));
    });
}

pub fn emit_eat_abort(gid: Option<GlobalPlayerId>, tick: TickStamp) {
    push_transient(gid, |bank, gid, seq| {
        bank.eat_abort.push(capture_eat_abort(gid, seq, tick));
    });
}

pub fn emit_break_anim(
    gid: Option<GlobalPlayerId>,
    tick: TickStamp,
    pos: BlockPos,
    stage: u8,
) {
    push_transient(gid, |bank, gid, seq| {
        bank
            .break_anim
            .push(capture_break_anim(gid, seq, tick, pos, stage));
    });
}

pub fn emit_break_anim_stop(
    gid: Option<GlobalPlayerId>,
    tick: TickStamp,
    pos: BlockPos,
) {
    emit_break_anim(gid, tick, pos, BREAK_ANIM_STOP_STAGE);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteEating {
    pub eating: bool,
    pub slot: u8,
    pub started: TickStamp,
    pub last_seq: Option<PlayerSeq>,
}

impl RemoteEating {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            eating: false,
            slot: 0,
            started: TickStamp(0),
            last_seq: None,
        }
    }

    #[must_use]
    pub const fn is_eating(&self) -> bool {
        self.eating
    }

    const fn is_fresh(&self, seq: PlayerSeq) -> bool {
        match self.last_seq {
            None => true,
            Some(seen) => seq.is_newer_than(seen),
        }
    }

    pub fn apply_start(&mut self, update: &EatStartUpdate) -> bool {
        if !self.is_fresh(update.seq) {
            return false;
        }
        self.eating = true;
        self.slot = update.slot;
        self.started = update.tick;
        self.last_seq = Some(update.seq);
        true
    }

    pub fn apply_abort(&mut self, update: &EatAbortUpdate) -> bool {
        if !self.is_fresh(update.seq) {
            return false;
        }
        self.eating = false;
        self.last_seq = Some(update.seq);
        true
    }
}

#[must_use]
pub const fn eat_finished(
    start: TickStamp,
    now: TickStamp,
    use_duration_ticks: u16,
) -> bool {
    now.distance_since(start) >= use_duration_ticks
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakAnimAction {
    Stage {
        pos: BlockPos,
        stage: u8,
    },
    Stop {
        pos: BlockPos,
    },
}

#[derive(Debug, Default)]
pub struct RemoteBreakAnims {
    pub anims: HashMap<GlobalPlayerId, (BlockPos, u8)>,
    pub last_seq: HashMap<GlobalPlayerId, PlayerSeq>,
}

impl RemoteBreakAnims {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_break_anim(&mut self, update: &BreakAnimUpdate) -> Option<BreakAnimAction> {
        let fresh = match self.last_seq.get(&update.gid) {
            None => true,
            Some(seen) => update.seq.is_newer_than(*seen),
        };
        if !fresh {
            return None;
        }
        self.last_seq.insert(update.gid, update.seq);
        if update.stage == BREAK_ANIM_STOP_STAGE {
            self.anims.remove(&update.gid);
            Some(BreakAnimAction::Stop { pos: update.pos })
        } else {
            self.anims
                .insert(update.gid, (update.pos, update.stage));
            Some(BreakAnimAction::Stage {
                pos: update.pos,
                stage: update.stage,
            })
        }
    }

    #[must_use]
    pub fn stage(&self, gid: &GlobalPlayerId) -> Option<(BlockPos, u8)> {
        self.anims.get(gid).copied()
    }

    pub fn remove(&mut self, gid: &GlobalPlayerId) -> Option<(BlockPos, u8)> {
        self.last_seq.remove(gid);
        self.anims.remove(gid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.anims.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.anims.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn sample_gid() -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(3), PlayerSlot(7))
    }

    fn sample_pos() -> BlockPos {
        BlockPos { x: 1, y: 2, z: 3 }
    }

    fn reset_transient_state() {
        NEXT_SEQ.with(|counters| counters.borrow_mut().clear());
        TRANSIENT_BANK.with(|bank| bank.borrow_mut().clear());
        TRANSIENT_STATS.with(|stats| stats.set(TransientCounters::new()));
    }

    #[test]
    fn stop_stage_is_255() {
        assert_eq!(BREAK_ANIM_STOP_STAGE, 255);
    }

    #[test]
    fn captures_match_wire_structs() {
        reset_transient_state();
        let gid = sample_gid();
        let seq = PlayerSeq(9);
        let tick = TickStamp(11);
        let pos = sample_pos();
        assert_eq!(
            capture_eat_start(gid, seq, tick, 2, 40, 3),
            EatStartUpdate {
                gid,
                seq,
                tick,
                slot: 2,
                item: 40,
                count: 3
            }
        );
        assert_eq!(
            capture_eat_abort(gid, seq, tick),
            EatAbortUpdate { gid, seq, tick }
        );
        assert_eq!(
            capture_break_anim(gid, seq, tick, pos, 4),
            BreakAnimUpdate {
                gid,
                seq,
                tick,
                pos,
                stage: 4
            }
        );
        assert_eq!(
            capture_break_anim_stop(gid, seq, tick, pos),
            BreakAnimUpdate {
                gid,
                seq,
                tick,
                pos,
                stage: BREAK_ANIM_STOP_STAGE
            }
        );
    }

    #[test]
    fn seq_starts_at_zero_and_advances_per_gid() {
        reset_transient_state();
        let first = sample_gid();
        let second = GlobalPlayerId::new(ServerId(1), PlayerSlot(8));
        assert_eq!(next_seq(first), PlayerSeq(0));
        assert_eq!(next_seq(first), PlayerSeq(1));
        assert_eq!(next_seq(second), PlayerSeq(0));
        assert_eq!(next_seq(first), PlayerSeq(2));
    }

    #[test]
    fn seq_wraps_around() {
        reset_transient_state();
        let gid = sample_gid();
        for _ in 0..u16::MAX {
            let _ = next_seq(gid);
        }
        assert_eq!(next_seq(gid), PlayerSeq(u16::MAX));
        assert_eq!(next_seq(gid), PlayerSeq(0));
    }

    #[test]
    fn tick_from_counter_wraps() {
        reset_transient_state();
        assert_eq!(tick_from_counter(0), TickStamp(0));
        assert_eq!(tick_from_counter(20), TickStamp(20));
        assert_eq!(tick_from_counter(65_536), TickStamp(0));
        assert_eq!(tick_from_counter(-1), TickStamp(u16::MAX));
    }

    #[test]
    fn emit_pushes_updates_with_seqs() {
        reset_transient_state();
        let gid = sample_gid();
        let tick = TickStamp(11);
        let pos = sample_pos();
        emit_eat_start(Some(gid), tick, 5, 40, 3);
        emit_eat_abort(Some(gid), tick);
        emit_break_anim(Some(gid), tick, pos, 2);
        emit_break_anim_stop(Some(gid), tick, pos);
        assert!(!transient_bank_is_empty());
        let bank = drain_transient_bank();
        assert_eq!(bank.eat_start.len(), 1);
        assert_eq!(bank.eat_abort.len(), 1);
        assert_eq!(bank.break_anim.len(), 2);
        assert_eq!(bank.eat_start[0].seq, PlayerSeq(0));
        assert_eq!(bank.eat_abort[0].seq, PlayerSeq(1));
        assert_eq!(bank.break_anim[0].seq, PlayerSeq(2));
        assert_eq!(bank.break_anim[1].seq, PlayerSeq(3));
        assert_eq!(bank.eat_start[0].slot, 5);
        assert_eq!(bank.eat_start[0].item, 40);
        assert_eq!(bank.eat_start[0].count, 3);
        assert_eq!(bank.break_anim[1].stage, BREAK_ANIM_STOP_STAGE);
        assert!(transient_bank_is_empty());
        let counters = transient_counters();
        assert_eq!(counters.emitted, 4);
        assert_eq!(counters.skipped_no_gid, 0);
    }

    #[test]
    fn emit_without_gid_skips_and_counts() {
        reset_transient_state();
        let tick = TickStamp(5);
        emit_eat_start(None, tick, 0, 0, 0);
        emit_eat_abort(None, tick);
        emit_break_anim(None, tick, sample_pos(), 1);
        assert!(transient_bank_is_empty());
        let counters = transient_counters();
        assert_eq!(counters.emitted, 0);
        assert_eq!(counters.skipped_no_gid, 3);
    }

    #[test]
    fn dropped_counter_accumulates() {
        reset_transient_state();
        note_transient_dropped(2);
        note_transient_dropped(3);
        assert_eq!(transient_counters().dropped, 5);
    }

    #[test]
    fn with_bank_sees_pending_updates() {
        reset_transient_state();
        emit_eat_abort(sample_gid_opt(), TickStamp(1));
        with_transient_bank(|bank| {
            assert_eq!(bank.eat_abort.len(), 1);
        });
    }

    fn sample_gid_opt() -> Option<GlobalPlayerId> {
        Some(sample_gid())
    }

    #[test]
    fn eat_finished_uses_elapsed_ticks() {
        assert!(eat_finished(TickStamp(10), TickStamp(42), 32));
        assert!(!eat_finished(TickStamp(10), TickStamp(41), 32));
        assert!(eat_finished(TickStamp(10), TickStamp(42), 1));
    }

    #[test]
    fn eat_finished_wraps() {
        assert!(eat_finished(TickStamp(u16::MAX), TickStamp(1), 2));
        assert!(!eat_finished(TickStamp(u16::MAX), TickStamp(1), 3));
    }

    #[test]
    fn remote_eating_tracks_start_and_abort() {
        let mut eater = RemoteEating::new();
        assert!(!eater.is_eating());
        let start = EatStartUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(4),
            tick: TickStamp(20),
            slot: 7,
            item: 40,
            count: 3,
        };
        assert!(eater.apply_start(&start));
        assert!(eater.is_eating());
        assert_eq!(eater.slot, 7);
        assert_eq!(eater.started, TickStamp(20));
        assert!(eat_finished(eater.started, TickStamp(52), 32));
        let abort = EatAbortUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(5),
            tick: TickStamp(21),
        };
        assert!(eater.apply_abort(&abort));
        assert!(!eater.is_eating());
    }

    #[test]
    fn remote_eating_abort_is_idempotent() {
        let mut eater = RemoteEating::new();
        let abort = EatAbortUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(0),
            tick: TickStamp(3),
        };
        assert!(eater.apply_abort(&abort));
        assert!(!eater.is_eating());
        assert_eq!(eater.last_seq, Some(PlayerSeq(0)));
    }

    #[test]
    fn remote_eating_drops_stale_edges() {
        let mut eater = RemoteEating::new();
        let start = EatStartUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(9),
            tick: TickStamp(30),
            slot: 1,
            item: 40,
            count: 3,
        };
        assert!(eater.apply_start(&start));
        let stale_abort = EatAbortUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(8),
            tick: TickStamp(31),
        };
        assert!(!eater.apply_abort(&stale_abort));
        assert!(eater.is_eating());
        let duplicate_start = EatStartUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(9),
            tick: TickStamp(32),
            slot: 2,
            item: 40,
            count: 3,
        };
        assert!(!eater.apply_start(&duplicate_start));
        assert_eq!(eater.slot, 1);
    }

    #[test]
    fn remote_break_anims_track_stage_and_stop() {
        let gid = sample_gid();
        let pos = sample_pos();
        let mut table = RemoteBreakAnims::new();
        assert!(table.is_empty());
        let edge = BreakAnimUpdate {
            gid,
            seq: PlayerSeq(1),
            tick: TickStamp(10),
            pos,
            stage: 3,
        };
        assert_eq!(
            table.apply_break_anim(&edge),
            Some(BreakAnimAction::Stage { pos, stage: 3 })
        );
        assert_eq!(table.stage(&gid), Some((pos, 3)));
        assert_eq!(table.len(), 1);
        let stop = BreakAnimUpdate {
            gid,
            seq: PlayerSeq(2),
            tick: TickStamp(11),
            pos,
            stage: BREAK_ANIM_STOP_STAGE,
        };
        assert_eq!(
            table.apply_break_anim(&stop),
            Some(BreakAnimAction::Stop { pos })
        );
        assert_eq!(table.stage(&gid), None);
        assert!(table.is_empty());
    }

    #[test]
    fn remote_break_anims_drop_stale_and_forget() {
        let gid = sample_gid();
        let pos = sample_pos();
        let mut table = RemoteBreakAnims::new();
        let edge = BreakAnimUpdate {
            gid,
            seq: PlayerSeq(7),
            tick: TickStamp(10),
            pos,
            stage: 1,
        };
        assert!(table.apply_break_anim(&edge).is_some());
        let stale = BreakAnimUpdate {
            gid,
            seq: PlayerSeq(6),
            tick: TickStamp(11),
            pos,
            stage: 5,
        };
        assert_eq!(table.apply_break_anim(&stale), None);
        assert_eq!(table.stage(&gid), Some((pos, 1)));
        assert_eq!(table.remove(&gid), Some((pos, 1)));
        assert!(table.is_empty());
    }
}
