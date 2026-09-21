use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};

use serde::{Deserialize, Serialize};

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::time::TickStamp;

pub const INV_MAIN: u8 = 0;
pub const INV_ARMOR: u8 = 1;
pub const INV_OFFHAND: u8 = 2;
pub const INV_CURSOR: u8 = 3;
pub const INV_MAX_STACK: u8 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InvLoc {
    pub inv: u8,
    pub slot: u16,
}

impl InvLoc {
    #[must_use]
    pub const fn new(inv: u8, slot: u16) -> Self {
        Self { inv, slot }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvOpKind {
    Move,
    MoveHalf,
    Split,
    Swap,
    Drop,
    Pickup,
    Consume,
    Set,
}

impl InvOpKind {
    #[must_use]
    pub const fn reads_source(self) -> bool {
        match self {
            Self::Move | Self::MoveHalf | Self::Split | Self::Swap | Self::Drop | Self::Consume => true,
            Self::Pickup | Self::Set => false,
        }
    }

    #[must_use]
    pub const fn writes_destination(self) -> bool {
        match self {
            Self::Move | Self::MoveHalf | Self::Split | Self::Swap | Self::Pickup | Self::Set => true,
            Self::Drop | Self::Consume => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryOp {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub kind: InvOpKind,
    pub src: InvLoc,
    pub dst: InvLoc,
    pub item: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

impl InventoryOp {
    #[must_use]
    pub const fn stream_kind() -> crate::protocol::StreamKind {
        crate::protocol::StreamKind::PlayerWorld
    }
}

pub trait InvCells {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)>;
    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvVerdict {
    Applied,
    Rejected,
}

#[must_use]
pub const fn inv_precondition_met(
    actual_item: u16,
    actual_count: u8,
    expected_item: u16,
    expected_count: u8,
) -> bool {
    actual_item == expected_item && actual_count == expected_count
}

#[must_use]
pub const fn inv_stack_has(actual_item: u16, actual_count: u8, expected_item: u16, needed: u8) -> bool {
    actual_item == expected_item && actual_count >= needed
}

fn dst_accepts(dst_item: Option<(u16, u8)>, item: u16, count: u8) -> bool {
    match dst_item {
        None => true,
        Some((0, _)) => true,
        Some((existing, _)) if count == 0 => existing == item,
        Some((existing, have)) => {
            existing == item && have.saturating_add(count) <= INV_MAX_STACK
        }
    }
}

fn take_from(cells: &mut impl InvCells, loc: InvLoc, item: u16, count: u8) -> bool {
    let Some((have_item, have_count)) = cells.cell(loc) else {
        return false;
    };
    if !inv_stack_has(have_item, have_count, item, count) {
        return false;
    }
    let left = have_count.saturating_sub(count);
    if left == 0 {
        cells.set_cell(loc, 0, 0)
    } else {
        cells.set_cell(loc, item, left)
    }
}

fn give_to(cells: &mut impl InvCells, loc: InvLoc, item: u16, count: u8) -> bool {
    if count == 0 {
        return cells.set_cell(loc, 0, 0);
    }
    let current = cells.cell(loc);
    if !dst_accepts(current, item, count) {
        return false;
    }
    match current {
        None | Some((0, _)) => cells.set_cell(loc, item, count),
        Some((_, have)) => cells.set_cell(loc, item, have.saturating_add(count)),
    }
}

pub fn replay(cells: &mut impl InvCells, op: &InventoryOp) -> InvVerdict {
    match op.kind {
        InvOpKind::Move | InvOpKind::Consume | InvOpKind::Drop => {
            if op.count == 0 {
                return InvVerdict::Rejected;
            }
            if op.kind != InvOpKind::Drop && op.kind != InvOpKind::Consume {
                let dst = cells.cell(op.dst);
                if !dst_accepts(dst, op.item, op.count) {
                    return InvVerdict::Rejected;
                }
            }
            if !take_from(cells, op.src, op.item, op.count) {
                return InvVerdict::Rejected;
            }
            if op.kind == InvOpKind::Move {
                if !give_to(cells, op.dst, op.item, op.count) {
                    let _ = give_to(cells, op.src, op.item, op.count);
                    return InvVerdict::Rejected;
                }
            }
            InvVerdict::Applied
        }
        InvOpKind::MoveHalf => {
            let Some((have_item, have_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            if have_count < 2 || have_item != op.item {
                return InvVerdict::Rejected;
            }
            let half = have_count.saturating_div(2);
            if op.count != half || half == 0 {
                return InvVerdict::Rejected;
            }
            let dst = cells.cell(op.dst);
            if !dst_accepts(dst, op.item, half) {
                return InvVerdict::Rejected;
            }
            if !take_from(cells, op.src, op.item, half) {
                return InvVerdict::Rejected;
            }
            if !give_to(cells, op.dst, op.item, half) {
                let _ = give_to(cells, op.src, op.item, half);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Split => {
            if op.count != 1 {
                return InvVerdict::Rejected;
            }
            let Some((have_item, have_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            if have_item != op.item || have_count < 2 {
                return InvVerdict::Rejected;
            }
            match cells.cell(op.dst) {
                None | Some((0, _)) => {},
                _ => return InvVerdict::Rejected,
            }
            if !take_from(cells, op.src, op.item, 1) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.dst, op.item, 1) {
                let _ = give_to(cells, op.src, op.item, 1);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Swap => {
            if op.src == op.dst {
                return InvVerdict::Rejected;
            }
            let Some((src_item, src_count)) = cells.cell(op.src) else {
                return InvVerdict::Rejected;
            };
            let Some((dst_item, dst_count)) = cells.cell(op.dst) else {
                return InvVerdict::Rejected;
            };
            if src_count == 0 || dst_count == 0 {
                return InvVerdict::Rejected;
            }
            if !inv_precondition_met(src_item, src_count, op.item, op.count) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.src, dst_item, dst_count) {
                return InvVerdict::Rejected;
            }
            if !cells.set_cell(op.dst, src_item, src_count) {
                let _ = cells.set_cell(op.src, src_item, src_count);
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Pickup => {
            if op.count == 0 {
                return InvVerdict::Rejected;
            }
            if give_to(cells, op.dst, op.item, op.count) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
        InvOpKind::Set => {
            if cells.set_cell(op.dst, op.item, op.count) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct MemCells {
    cells: HashMap<InvLoc, (u16, u8)>,
}

impl MemCells {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&mut self, loc: InvLoc, item: u16, count: u8) {
        if count == 0 || item == 0 {
            self.cells.remove(&loc);
        } else {
            self.cells.insert(loc, (item, count));
        }
    }

    #[must_use]
    pub fn total(&self, item: u16) -> u32 {
        self.cells
            .values()
            .filter(|(id, _)| *id == item)
            .map(|(_, count)| u32::from(*count))
            .sum()
    }
}

impl InvCells for MemCells {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)> {
        self.cells.get(&loc).copied()
    }

    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool {
        if count == 0 || item == 0 {
            self.cells.remove(&loc);
        } else {
            self.cells.insert(loc, (item, count));
        }
        true
    }
}

#[derive(Debug, Default)]
pub struct InvLedger {
    cells: HashMap<(GlobalPlayerId, u8, u16), (u16, u8)>,
}

impl InvLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, gid: GlobalPlayerId, loc: InvLoc) -> Option<(u16, u8)> {
        self.cells.get(&(gid, loc.inv, loc.slot)).copied()
    }

    pub fn record(&mut self, gid: GlobalPlayerId, loc: InvLoc, item: u16, count: u8) {
        if count == 0 || item == 0 {
            self.cells.remove(&(gid, loc.inv, loc.slot));
        } else {
            self.cells.insert((gid, loc.inv, loc.slot), (item, count));
        }
    }

    pub fn apply_op(&mut self, op: &InventoryOp) -> InvVerdict {
        let mut view = LedgerView { ledger: self, gid: op.gid };
        let verdict = replay(&mut view, op);
        verdict
    }

    pub fn check_place(
        &mut self,
        gid: GlobalPlayerId,
        loc: InvLoc,
        item: u16,
        count_before: u8,
        count_after: u8,
    ) -> InvVerdict {
        match self.get(gid, loc) {
            None => {
                self.record(gid, loc, item, count_after);
                InvVerdict::Applied
            }
            Some((have_item, have_count)) => {
                if !inv_precondition_met(have_item, have_count, item, count_before) {
                    return InvVerdict::Rejected;
                }
                self.record(gid, loc, item, count_after);
                InvVerdict::Applied
            }
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

struct LedgerView<'ledger> {
    ledger: &'ledger mut InvLedger,
    gid: GlobalPlayerId,
}

impl InvCells for LedgerView<'_> {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)> {
        self.ledger.get(self.gid, loc)
    }

    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool {
        self.ledger.record(self.gid, loc, item, count);
        true
    }
}

static INV_SEQ: AtomicU16 = AtomicU16::new(0);

pub fn next_inv_seq(_gid: GlobalPlayerId) -> PlayerSeq {
    PlayerSeq(INV_SEQ.fetch_add(1, Ordering::Relaxed))
}

#[must_use]
pub fn capture_inv_op(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    kind: InvOpKind,
    src: InvLoc,
    dst: InvLoc,
    item: u16,
    count: u8,
) -> InventoryOp {
    InventoryOp {
        gid,
        seq,
        tick,
        kind,
        src,
        dst,
        item,
        count,
        nbt: Vec::new(),
    }
}

#[must_use]
pub fn capture_inv_set(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    dst: InvLoc,
    item: u16,
    count: u8,
    nbt: Vec<u8>,
) -> InventoryOp {
    InventoryOp {
        gid,
        seq,
        tick,
        kind: InvOpKind::Set,
        src: dst,
        dst,
        item,
        count,
        nbt,
    }
}

pub fn sort_inv_ops_for_tick(tick: TickStamp, ops: &mut [InventoryOp]) {
    ops.sort_by(|left, right| {
        crate::order::order_players(crate::protocol::INV_OP_SEED, tick, left.gid, right.gid)
            .then_with(|| left.seq.0.cmp(&right.seq.0))
            .then_with(|| {
                (left.src.inv, left.src.slot, left.dst.inv, left.dst.slot).cmp(&(
                    right.src.inv,
                    right.src.slot,
                    right.dst.inv,
                    right.dst.slot,
                ))
            })
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn loc(slot: u16) -> InvLoc {
        InvLoc::new(INV_MAIN, slot)
    }

    fn op(kind: InvOpKind, src: u16, dst: u16, item: u16, count: u8) -> InventoryOp {
        capture_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            kind,
            loc(src),
            loc(dst),
            item,
            count,
        )
    }

    #[test]
    fn move_replays_source_to_destination() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 10)), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(0)), Some((5, 22)));
        assert_eq!(cells.cell(loc(1)), Some((5, 10)));
    }

    #[test]
    fn move_rejects_on_source_mismatch_without_mutation() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        cells.seed(loc(1), 5, 4);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Move, 0, 1, 9, 10)), InvVerdict::Rejected);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 64)), InvVerdict::Rejected);
        assert_eq!(cells.cell(loc(0)), Some((5, 32)));
        assert_eq!(cells.cell(loc(1)), Some((5, 4)));
        assert_eq!(cells.total(5), 36);
    }

    #[test]
    fn move_rejects_when_destination_holds_other_item() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        cells.seed(loc(1), 7, 2);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Move, 0, 1, 5, 4)), InvVerdict::Rejected);
        assert_eq!(cells.cell(loc(0)), Some((5, 32)));
        assert_eq!(cells.cell(loc(1)), Some((7, 2)));
    }

    #[test]
    fn move_half_requires_exact_half() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 33);
        assert_eq!(replay(&mut cells, &op(InvOpKind::MoveHalf, 0, 1, 5, 16)), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(1)), Some((5, 16)));
        assert_eq!(replay(&mut cells, &op(InvOpKind::MoveHalf, 0, 2, 5, 9)), InvVerdict::Rejected);
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(2)), None);
    }

    #[test]
    fn split_moves_single_item_to_empty_slot() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Split, 0, 1, 5, 1)), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(0)), Some((5, 7)));
        assert_eq!(cells.cell(loc(1)), Some((5, 1)));
    }

    #[test]
    fn split_rejects_into_occupied_slot() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        cells.seed(loc(1), 5, 1);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Split, 0, 1, 5, 1)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 9);
    }

    #[test]
    fn swap_exchanges_stacks_and_validates_source() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        cells.seed(loc(1), 7, 3);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Swap, 0, 1, 5, 8)), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
        assert_eq!(cells.cell(loc(1)), Some((5, 8)));
        assert_eq!(replay(&mut cells, &op(InvOpKind::Swap, 0, 1, 9, 3)), InvVerdict::Rejected);
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
    }

    #[test]
    fn consume_and_drop_never_duplicate_on_conflict() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 4);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Consume, 0, 0, 5, 1)), InvVerdict::Applied);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Consume, 0, 0, 5, 4)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 3);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Drop, 0, 0, 9, 1)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 3);
    }

    #[test]
    fn pickup_stacks_only_onto_same_item_with_room() {
        let mut cells = MemCells::new();
        cells.seed(loc(1), 5, 60);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Pickup, 0, 1, 5, 4)), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(1)), Some((5, 64)));
        assert_eq!(replay(&mut cells, &op(InvOpKind::Pickup, 0, 1, 5, 1)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 64);
    }

    #[test]
    fn set_overwrites_without_duplication() {
        let mut cells = MemCells::new();
        cells.seed(loc(3), 5, 64);
        let set = capture_inv_set(gid(1, 1), PlayerSeq(1), TickStamp(7), loc(3), 7, 2, Vec::new());
        assert_eq!(replay(&mut cells, &set), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(3)), Some((7, 2)));
        assert_eq!(cells.total(5), 0);
    }

    #[test]
    fn ledger_rejects_stale_place_precondition() {
        let mut ledger = InvLedger::new();
        let at = InvLoc::new(INV_MAIN, 2);
        assert_eq!(ledger.check_place(gid(1, 1), at, 40, 64, 63), InvVerdict::Applied);
        assert_eq!(ledger.check_place(gid(1, 1), at, 40, 64, 63), InvVerdict::Rejected);
        assert_eq!(ledger.check_place(gid(1, 1), at, 40, 63, 62), InvVerdict::Applied);
        assert_eq!(ledger.get(gid(1, 1), at), Some((40, 62)));
    }

    #[test]
    fn ledger_replay_then_conflicting_replay_rejects() {
        let mut ledger = InvLedger::new();
        let first = op(InvOpKind::Move, 0, 1, 5, 10);
        ledger.record(gid(1, 1), loc(0), 5, 32);
        assert_eq!(ledger.apply_op(&first), InvVerdict::Applied);
        let conflict = op(InvOpKind::Move, 0, 2, 5, 30);
        assert_eq!(ledger.apply_op(&conflict), InvVerdict::Rejected);
        assert_eq!(ledger.get(gid(1, 1), loc(2)), None);
        let total: u32 = [0, 1]
            .iter()
            .filter_map(|slot| ledger.get(gid(1, 1), loc(*slot)))
            .map(|(_, count)| u32::from(count))
            .sum();
        assert_eq!(total, 32);
    }

    #[test]
    fn inv_ops_sort_deterministically() {
        let tick = TickStamp(11);
        let mut first = op(InvOpKind::Move, 0, 1, 5, 2);
        first.gid = gid(2, 1);
        let mut second = op(InvOpKind::Move, 0, 1, 5, 2);
        second.gid = gid(1, 1);
        let mut arrival = [first.clone(), second.clone()];
        sort_inv_ops_for_tick(tick, &mut arrival);
        let mut other = [second, first];
        sort_inv_ops_for_tick(tick, &mut other);
        assert_eq!(arrival, other);
    }

    #[test]
    fn precondition_rule_is_exact_match() {
        assert!(inv_precondition_met(40, 63, 40, 63));
        assert!(!inv_precondition_met(40, 62, 40, 63));
        assert!(!inv_precondition_met(41, 63, 40, 63));
        assert!(inv_stack_has(40, 63, 40, 1));
        assert!(!inv_stack_has(40, 0, 40, 1));
    }
}
