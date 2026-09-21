use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use crate::banks::Bank;
use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{
    ArmorUpdate, BlockingUpdate, HeldUpdate, SkinLayersUpdate, SneakUpdate, SprintUpdate,
    SwingUpdate,
};
use crate::time::TickStamp;

thread_local! {
    static NEXT_SEQ: RefCell<HashMap<GlobalPlayerId, u16>> =
        RefCell::new(HashMap::new());
    static VISUAL_BANK: RefCell<Bank> = RefCell::new(Bank::new());
    static VISUAL_STATS: Cell<VisualCounters> = Cell::new(VisualCounters::new());
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VisualCounters {
    pub emitted: u64,
    pub skipped_no_gid: u64,
    pub dropped: u64,
}

impl VisualCounters {
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
pub const fn capture_armor(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    slot: u8,
    item: u16,
) -> ArmorUpdate {
    ArmorUpdate {
        gid,
        seq,
        tick,
        slot,
        item,
    }
}

#[must_use]
pub const fn capture_held(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    slot: u8,
) -> HeldUpdate {
    HeldUpdate {
        gid,
        seq,
        tick,
        slot,
    }
}

#[must_use]
pub const fn capture_sneak(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    active: bool,
) -> SneakUpdate {
    SneakUpdate {
        gid,
        seq,
        tick,
        active,
    }
}

#[must_use]
pub const fn capture_sprint(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    active: bool,
) -> SprintUpdate {
    SprintUpdate {
        gid,
        seq,
        tick,
        active,
    }
}

#[must_use]
pub const fn capture_blocking(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    active: bool,
) -> BlockingUpdate {
    BlockingUpdate {
        gid,
        seq,
        tick,
        active,
    }
}

#[must_use]
pub const fn capture_swing(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    hand: u8,
) -> SwingUpdate {
    SwingUpdate {
        gid,
        seq,
        tick,
        hand,
    }
}

#[must_use]
pub const fn capture_skin(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    mask: u8,
) -> SkinLayersUpdate {
    SkinLayersUpdate {
        gid,
        seq,
        tick,
        mask,
    }
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

#[must_use]
pub fn with_visual_bank<R>(apply: impl FnOnce(&mut Bank) -> R) -> R {
    VISUAL_BANK.with(|bank| apply(&mut bank.borrow_mut()))
}

#[must_use]
pub fn visual_bank_is_empty() -> bool {
    VISUAL_BANK.with(|bank| bank.borrow().is_empty())
}

#[must_use]
pub fn drain_visual_bank() -> Bank {
    VISUAL_BANK.with(|bank| std::mem::take(&mut *bank.borrow_mut()))
}

#[must_use]
pub fn visual_counters() -> VisualCounters {
    VISUAL_STATS.with(|stats| stats.get())
}

pub fn note_visual_dropped(count: u64) {
    VISUAL_STATS.with(|stats| {
        let mut current = stats.get();
        current.dropped = current.dropped.saturating_add(count);
        stats.set(current);
    });
}

fn push_visual(
    gid: Option<GlobalPlayerId>,
    push: impl FnOnce(&mut Bank, GlobalPlayerId, PlayerSeq),
) {
    let Some(gid) = gid else {
        VISUAL_STATS.with(|stats| {
            let mut current = stats.get();
            current.skipped_no_gid = current.skipped_no_gid.saturating_add(1);
            stats.set(current);
        });
        return;
    };
    let seq = next_seq(gid);
    VISUAL_BANK.with(|bank| push(&mut bank.borrow_mut(), gid, seq));
    VISUAL_STATS.with(|stats| {
        let mut current = stats.get();
        current.emitted = current.emitted.saturating_add(1);
        stats.set(current);
    });
}

pub fn emit_armor(gid: Option<GlobalPlayerId>, tick: TickStamp, slot: u8, item: u16) {
    push_visual(gid, |bank, gid, seq| {
        bank.armor.push(capture_armor(gid, seq, tick, slot, item));
    });
}

pub fn emit_held(gid: Option<GlobalPlayerId>, tick: TickStamp, slot: u8) {
    push_visual(gid, |bank, gid, seq| {
        bank.held.push(capture_held(gid, seq, tick, slot));
    });
}

pub fn emit_sneak(gid: Option<GlobalPlayerId>, tick: TickStamp, active: bool) {
    push_visual(gid, |bank, gid, seq| {
        bank.sneak.push(capture_sneak(gid, seq, tick, active));
    });
}

pub fn emit_sprint(gid: Option<GlobalPlayerId>, tick: TickStamp, active: bool) {
    push_visual(gid, |bank, gid, seq| {
        bank.sprint.push(capture_sprint(gid, seq, tick, active));
    });
}

pub fn emit_blocking(gid: Option<GlobalPlayerId>, tick: TickStamp, active: bool) {
    push_visual(gid, |bank, gid, seq| {
        bank.blocking.push(capture_blocking(gid, seq, tick, active));
    });
}

pub fn emit_swing(gid: Option<GlobalPlayerId>, tick: TickStamp, hand: u8) {
    push_visual(gid, |bank, gid, seq| {
        bank.swing.push(capture_swing(gid, seq, tick, hand));
    });
}

pub fn emit_skin(gid: Option<GlobalPlayerId>, tick: TickStamp, mask: u8) {
    push_visual(gid, |bank, gid, seq| {
        bank.skin.push(capture_skin(gid, seq, tick, mask));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn reset_visual_state() {
        NEXT_SEQ.with(|counters| counters.borrow_mut().clear());
        VISUAL_BANK.with(|bank| bank.borrow_mut().clear());
        VISUAL_STATS.with(|stats| stats.set(VisualCounters::new()));
    }

    fn sample_gid() -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(1), PlayerSlot(7))
    }

    #[test]
    fn capture_ctors_echo_params() {
        reset_visual_state();
        let gid = sample_gid();
        let seq = PlayerSeq(9);
        let tick = TickStamp(3);
        assert_eq!(
            capture_armor(gid, seq, tick, 4, 512),
            ArmorUpdate {
                gid,
                seq,
                tick,
                slot: 4,
                item: 512
            }
        );
        assert_eq!(
            capture_held(gid, seq, tick, 2),
            HeldUpdate {
                gid,
                seq,
                tick,
                slot: 2
            }
        );
        assert_eq!(
            capture_sneak(gid, seq, tick, true),
            SneakUpdate {
                gid,
                seq,
                tick,
                active: true
            }
        );
        assert_eq!(
            capture_sprint(gid, seq, tick, false),
            SprintUpdate {
                gid,
                seq,
                tick,
                active: false
            }
        );
        assert_eq!(
            capture_blocking(gid, seq, tick, true),
            BlockingUpdate {
                gid,
                seq,
                tick,
                active: true
            }
        );
        assert_eq!(
            capture_swing(gid, seq, tick, 1),
            SwingUpdate {
                gid,
                seq,
                tick,
                hand: 1
            }
        );
        assert_eq!(
            capture_skin(gid, seq, tick, 0x7F),
            SkinLayersUpdate {
                gid,
                seq,
                tick,
                mask: 0x7F
            }
        );
    }

    #[test]
    fn seq_starts_at_zero_and_advances_per_gid() {
        reset_visual_state();
        let first = sample_gid();
        let second = GlobalPlayerId::new(ServerId(1), PlayerSlot(8));
        assert_eq!(next_seq(first), PlayerSeq(0));
        assert_eq!(next_seq(first), PlayerSeq(1));
        assert_eq!(next_seq(second), PlayerSeq(0));
        assert_eq!(next_seq(first), PlayerSeq(2));
    }

    #[test]
    fn seq_wraps_around() {
        reset_visual_state();
        let gid = sample_gid();
        for _ in 0..u16::MAX {
            let _ = next_seq(gid);
        }
        assert_eq!(next_seq(gid), PlayerSeq(u16::MAX));
        assert_eq!(next_seq(gid), PlayerSeq(0));
    }

    #[test]
    fn tick_from_counter_wraps() {
        reset_visual_state();
        assert_eq!(tick_from_counter(0), TickStamp(0));
        assert_eq!(tick_from_counter(20), TickStamp(20));
        assert_eq!(tick_from_counter(65_536), TickStamp(0));
        assert_eq!(tick_from_counter(-1), TickStamp(u16::MAX));
    }

    #[test]
    fn emit_pushes_updates_with_seqs() {
        reset_visual_state();
        let gid = sample_gid();
        let tick = TickStamp(11);
        emit_sprint(Some(gid), tick, true);
        emit_swing(Some(gid), tick, 0);
        assert!(!visual_bank_is_empty());
        let bank = drain_visual_bank();
        assert_eq!(bank.sprint.len(), 1);
        assert_eq!(bank.swing.len(), 1);
        assert_eq!(bank.sprint[0].seq, PlayerSeq(0));
        assert_eq!(bank.swing[0].seq, PlayerSeq(1));
        assert_eq!(bank.sprint[0].active, true);
        assert!(visual_bank_is_empty());
        let counters = visual_counters();
        assert_eq!(counters.emitted, 2);
        assert_eq!(counters.skipped_no_gid, 0);
    }

    #[test]
    fn emit_without_gid_skips_and_counts() {
        reset_visual_state();
        emit_sneak(None, TickStamp(5), true);
        emit_armor(None, TickStamp(5), 1, 2);
        assert!(visual_bank_is_empty());
        let counters = visual_counters();
        assert_eq!(counters.emitted, 0);
        assert_eq!(counters.skipped_no_gid, 2);
    }

    #[test]
    fn dropped_counter_accumulates() {
        reset_visual_state();
        note_visual_dropped(2);
        note_visual_dropped(3);
        assert_eq!(visual_counters().dropped, 5);
    }
}
