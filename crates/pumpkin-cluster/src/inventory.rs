use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};

use serde::{Deserialize, Serialize};

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::EntityRef;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryStack {
    pub item: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryPreconditions {
    pub source_before: InventoryStack,
    pub destination_before: InventoryStack,
}

impl InventoryStack {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            item: 0,
            count: 0,
            nbt: Vec::new(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.item == 0 || self.count == 0
    }

    #[must_use]
    pub fn normalized(mut self) -> Self {
        if self.is_empty() {
            self.item = 0;
            self.count = 0;
            self.nbt.clear();
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemPickup {
    pub entity: EntityRef,
    pub source_before: InventoryStack,
    pub source_after: InventoryStack,
    pub destination_before: InventoryStack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemPickupUndo {
    pub entity: EntityRef,
    pub source_before: InventoryStack,
    pub destination_before: InventoryStack,
}

impl ItemPickup {
    #[must_use]
    pub fn undo(&self) -> ItemPickupUndo {
        ItemPickupUndo {
            entity: self.entity,
            source_before: self.source_before.clone(),
            destination_before: self.destination_before.clone(),
        }
    }

    #[must_use]
    pub fn transfer_count(&self) -> Option<u8> {
        self.source_before
            .count
            .checked_sub(self.source_after.count)
    }

    #[must_use]
    pub fn is_consistent_with(&self, op: &InventoryOp) -> bool {
        let Some(transferred) = self.transfer_count() else {
            return false;
        };
        self.source_before == self.source_before.clone().normalized()
            && self.source_after == self.source_after.clone().normalized()
            && self.destination_before == self.destination_before.clone().normalized()
            && !self.source_before.is_empty()
            && self.source_before.item == op.item
            && (self.destination_before.is_empty()
                || (self.destination_before.item == self.source_before.item
                    && self.destination_before.nbt == self.source_before.nbt))
            && (self.source_after.is_empty()
                || (self.source_after.item == self.source_before.item
                    && self.source_after.nbt == self.source_before.nbt))
            && transferred == op.count
            && op.nbt == self.source_before.nbt
    }
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
    Grant,
    Remove,
}

impl InvOpKind {
    #[must_use]
    pub const fn reads_source(self) -> bool {
        match self {
            Self::Move | Self::MoveHalf | Self::Split | Self::Swap | Self::Drop | Self::Consume | Self::Remove => true,
            Self::Pickup | Self::Set | Self::Grant => false,
        }
    }

    #[must_use]
    pub const fn writes_destination(self) -> bool {
        match self {
            Self::Move | Self::MoveHalf | Self::Split | Self::Swap | Self::Pickup | Self::Set | Self::Grant => true,
            Self::Drop | Self::Consume | Self::Remove => false,
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
    pub preconditions: Option<InventoryPreconditions>,
    pub pickup: Option<ItemPickup>,
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

    fn stack(&self, loc: InvLoc) -> Option<InventoryStack> {
        self.cell(loc).map(|(item, count)| InventoryStack {
            item,
            count,
            nbt: Vec::new(),
        })
    }

    fn set_stack(&mut self, loc: InvLoc, stack: InventoryStack) -> bool {
        let stack = stack.normalized();
        self.set_cell(loc, stack.item, stack.count)
    }
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

fn stack_matches(left: &InventoryStack, right: &InventoryStack) -> bool {
    left == &right.clone().normalized()
}

fn dst_accepts(dst: &InventoryStack, incoming: &InventoryStack, count: u8) -> bool {
    dst.is_empty()
        || (dst.item == incoming.item
            && dst.nbt == incoming.nbt
            && dst.count.saturating_add(count) <= INV_MAX_STACK)
}

fn take_from(
    cells: &mut impl InvCells,
    loc: InvLoc,
    expected: &InventoryStack,
    count: u8,
) -> bool {
    let Some(have) = cells.stack(loc) else {
        return false;
    };
    if !stack_matches(&have, expected) || count == 0 || count > have.count {
        return false;
    }
    let mut left = have;
    left.count = left.count.saturating_sub(count);
    cells.set_stack(loc, left.normalized())
}

fn give_to(
    cells: &mut impl InvCells,
    loc: InvLoc,
    expected: &InventoryStack,
    incoming: &InventoryStack,
    count: u8,
) -> bool {
    let Some(current) = cells.stack(loc) else {
        return false;
    };
    if count == 0 || !stack_matches(&current, expected) || !dst_accepts(&current, incoming, count) {
        return false;
    }
    let mut next = incoming.clone();
    next.count = current.count.saturating_add(count);
    cells.set_stack(loc, next)
}

pub fn replay(cells: &mut impl InvCells, op: &InventoryOp) -> InvVerdict {
    let Some(preconditions) = op.preconditions.as_ref() else {
        return InvVerdict::Rejected;
    };
    let source = &preconditions.source_before;
    let destination = &preconditions.destination_before;
    if source != &source.clone().normalized()
        || destination != &destination.clone().normalized()
    {
        return InvVerdict::Rejected;
    }
    if op.kind != InvOpKind::Pickup && op.kind != InvOpKind::Grant && op.kind != InvOpKind::Remove
        && !semantic_op_is_consistent(op)
    {
        return InvVerdict::Rejected;
    }
    match op.kind {
        InvOpKind::Move | InvOpKind::Consume | InvOpKind::Drop => {
            if op.count == 0
                || source.is_empty()
                || source.item != op.item
                || source.nbt != op.nbt
            {
                return InvVerdict::Rejected;
            }
            if op.kind == InvOpKind::Move {
                if !give_to(cells, op.dst, destination, source, op.count) {
                    return InvVerdict::Rejected;
                }
                if !take_from(cells, op.src, source, op.count) {
                    let _ = cells.set_stack(op.dst, destination.clone());
                    return InvVerdict::Rejected;
                }
            } else if !take_from(cells, op.src, source, op.count) {
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::MoveHalf => {
            if source.count < 2 || source.item != op.item || source.nbt != op.nbt {
                return InvVerdict::Rejected;
            }
            let half = source.count.saturating_div(2);
            if op.count != half || half == 0 {
                return InvVerdict::Rejected;
            }
            if !give_to(cells, op.dst, destination, source, half) {
                return InvVerdict::Rejected;
            }
            if !take_from(cells, op.src, source, half) {
                let _ = cells.set_stack(op.dst, destination.clone());
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Split => {
            if op.count != 1 || source.item != op.item || source.nbt != op.nbt || source.count < 2 {
                return InvVerdict::Rejected;
            }
            if !destination.is_empty() || !give_to(cells, op.dst, destination, source, 1) {
                return InvVerdict::Rejected;
            }
            if !take_from(cells, op.src, source, 1) {
                let _ = cells.set_stack(op.dst, destination.clone());
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Swap => {
            if op.src == op.dst {
                return InvVerdict::Rejected;
            }
            if source.is_empty()
                || destination.is_empty()
                || source.item != op.item
                || source.count != op.count
                || source.nbt != op.nbt
            {
                return InvVerdict::Rejected;
            }
            if cells.stack(op.src).as_ref() != Some(source)
                || cells.stack(op.dst).as_ref() != Some(destination)
            {
                return InvVerdict::Rejected;
            }
            if !cells.set_stack(op.src, destination.clone()) {
                return InvVerdict::Rejected;
            }
            if !cells.set_stack(op.dst, source.clone()) {
                let _ = cells.set_stack(op.src, source.clone());
                return InvVerdict::Rejected;
            }
            if cells.stack(op.src).as_ref() != Some(destination)
                || cells.stack(op.dst).as_ref() != Some(source)
            {
                let _ = cells.set_stack(op.src, source.clone());
                let _ = cells.set_stack(op.dst, destination.clone());
                return InvVerdict::Rejected;
            }
            InvVerdict::Applied
        }
        InvOpKind::Pickup => {
            let Some(pickup) = op.pickup.as_ref() else {
                return InvVerdict::Rejected;
            };
            if op.count == 0
                || !pickup.is_consistent_with(op)
                || pickup.destination_before != *destination
            {
                return InvVerdict::Rejected;
            }
            if give_to(cells, op.dst, destination, &pickup.source_before, op.count) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
        InvOpKind::Set => InvVerdict::Rejected,
        InvOpKind::Grant => {
            let granted = InventoryStack {
                item: op.item,
                count: op.count,
                nbt: op.nbt.clone(),
            }
            .normalized();
            if op.src != op.dst
                || !source.is_empty()
                || granted.is_empty()
                || cells.stack(op.dst).as_ref() != Some(destination)
            {
                return InvVerdict::Rejected;
            }
            if cells.set_stack(op.dst, granted) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
        InvOpKind::Remove => {
            if op.src != op.dst
                || source.is_empty()
                || destination != source
                || source.item != op.item
                || source.count != op.count
                || source.nbt != op.nbt
                || cells.stack(op.src).as_ref() != Some(source)
            {
                return InvVerdict::Rejected;
            }
            if cells.set_stack(op.src, InventoryStack::empty()) {
                InvVerdict::Applied
            } else {
                InvVerdict::Rejected
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct MemCells {
    cells: HashMap<InvLoc, InventoryStack>,
}

impl MemCells {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&mut self, loc: InvLoc, item: u16, count: u8) {
        self.seed_stack(
            loc,
            InventoryStack {
                item,
                count,
                nbt: Vec::new(),
            },
        );
    }

    pub fn seed_stack(&mut self, loc: InvLoc, stack: InventoryStack) {
        let stack = stack.normalized();
        if stack.is_empty() {
            self.cells.remove(&loc);
        } else {
            self.cells.insert(loc, stack);
        }
    }

    #[must_use]
    pub fn total(&self, item: u16) -> u32 {
        self.cells
            .values()
            .filter(|stack| stack.item == item)
            .map(|stack| u32::from(stack.count))
            .sum()
    }
}

impl InvCells for MemCells {
    fn cell(&self, loc: InvLoc) -> Option<(u16, u8)> {
        self.cells.get(&loc).map(|stack| (stack.item, stack.count))
    }

    fn set_cell(&mut self, loc: InvLoc, item: u16, count: u8) -> bool {
        self.set_stack(
            loc,
            InventoryStack {
                item,
                count,
                nbt: Vec::new(),
            },
        )
    }

    fn stack(&self, loc: InvLoc) -> Option<InventoryStack> {
        self.cells.get(&loc).cloned().or_else(|| Some(InventoryStack::empty()))
    }

    fn set_stack(&mut self, loc: InvLoc, stack: InventoryStack) -> bool {
        self.seed_stack(loc, stack);
        true
    }
}

#[derive(Debug, Default)]
pub struct InvLedger {
    cells: HashMap<(GlobalPlayerId, u8, u16), InventoryStack>,
}

impl InvLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, gid: GlobalPlayerId, loc: InvLoc) -> Option<(u16, u8)> {
        self.get_stack(gid, loc)
            .map(|stack| (stack.item, stack.count))
    }

    #[must_use]
    pub fn get_stack(&self, gid: GlobalPlayerId, loc: InvLoc) -> Option<InventoryStack> {
        self.cells.get(&(gid, loc.inv, loc.slot)).cloned()
    }

    pub fn record(&mut self, gid: GlobalPlayerId, loc: InvLoc, item: u16, count: u8) {
        self.record_stack(
            gid,
            loc,
            InventoryStack {
                item,
                count,
                nbt: Vec::new(),
            },
        );
    }

    pub fn record_stack(&mut self, gid: GlobalPlayerId, loc: InvLoc, stack: InventoryStack) {
        let stack = stack.normalized();
        if stack.is_empty() {
            self.cells.remove(&(gid, loc.inv, loc.slot));
        } else {
            self.cells.insert((gid, loc.inv, loc.slot), stack);
        }
    }

    pub fn apply_op(&mut self, op: &InventoryOp) -> InvVerdict {
        let mut view = LedgerView { ledger: self, gid: op.gid };
        let verdict = replay(&mut view, op);
        verdict
    }

    pub fn with_cells<T>(
        &mut self,
        gid: GlobalPlayerId,
        apply: impl FnOnce(&mut dyn InvCells) -> T,
    ) -> T {
        let mut view = LedgerView { ledger: self, gid };
        apply(&mut view)
    }

    pub fn check_place(
        &mut self,
        gid: GlobalPlayerId,
        loc: InvLoc,
        before: InventoryStack,
        after: InventoryStack,
    ) -> InvVerdict {
        let before = before.normalized();
        let after = after.normalized();
        if before.is_empty()
            || (!after.is_empty()
                && (after.item != before.item || after.nbt != before.nbt))
            || after.count > before.count
            || self.get_stack(gid, loc).as_ref() != Some(&before)
        {
            return InvVerdict::Rejected;
        }
        self.record_stack(gid, loc, after);
        InvVerdict::Applied
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

    fn stack(&self, loc: InvLoc) -> Option<InventoryStack> {
        self.ledger
            .get_stack(self.gid, loc)
            .or_else(|| Some(InventoryStack::empty()))
    }

    fn set_stack(&mut self, loc: InvLoc, stack: InventoryStack) -> bool {
        self.ledger.record_stack(self.gid, loc, stack);
        true
    }
}

static INV_SEQ: AtomicU16 = AtomicU16::new(0);

pub fn next_inv_seq(_gid: GlobalPlayerId) -> PlayerSeq {
    PlayerSeq(INV_SEQ.fetch_add(1, Ordering::Relaxed))
}

#[must_use]
pub fn capture_semantic_inv_op(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    kind: InvOpKind,
    src: InvLoc,
    dst: InvLoc,
    source_before: InventoryStack,
    destination_before: InventoryStack,
    count: u8,
) -> Option<InventoryOp> {
    let source_before = source_before.normalized();
    let destination_before = destination_before.normalized();
    let op = InventoryOp {
        gid,
        seq,
        tick,
        kind,
        src,
        dst,
        item: source_before.item,
        count,
        nbt: source_before.nbt.clone(),
        preconditions: Some(InventoryPreconditions {
            source_before,
            destination_before,
        }),
        pickup: None,
    };
    semantic_op_is_consistent(&op).then_some(op)
}

fn semantic_op_is_consistent(op: &InventoryOp) -> bool {
    let Some(preconditions) = op.preconditions.as_ref() else {
        return false;
    };
    let source = &preconditions.source_before;
    let destination = &preconditions.destination_before;
    match op.kind {
        InvOpKind::Move | InvOpKind::Drop | InvOpKind::Consume => {
            !source.is_empty()
                && source.item == op.item
                && source.nbt == op.nbt
                && op.count != 0
                && op.count <= source.count
                && (op.kind != InvOpKind::Move || dst_accepts(destination, source, op.count))
        }
        InvOpKind::MoveHalf => {
            !source.is_empty()
                && source.item == op.item
                && source.nbt == op.nbt
                && source.count >= 2
                && op.count == source.count.saturating_div(2)
                && dst_accepts(destination, source, op.count)
        }
        InvOpKind::Split => {
            !source.is_empty()
                && source.item == op.item
                && source.nbt == op.nbt
                && source.count >= 2
                && op.count == 1
                && destination.is_empty()
        }
        InvOpKind::Swap => {
            op.src != op.dst
                && !source.is_empty()
                && !destination.is_empty()
                && source.item == op.item
                && source.count == op.count
                && source.nbt == op.nbt
        }
        InvOpKind::Pickup | InvOpKind::Set | InvOpKind::Grant | InvOpKind::Remove => false,
    }
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
        preconditions: None,
        pickup: None,
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
        preconditions: None,
        pickup: None,
    }
}

#[must_use]
pub fn capture_privileged_grant(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    dst: InvLoc,
    destination_before: InventoryStack,
    granted: InventoryStack,
) -> Option<InventoryOp> {
    let destination_before = destination_before.normalized();
    let granted = granted.normalized();
    if granted.is_empty() {
        return None;
    }
    Some(InventoryOp {
        gid,
        seq,
        tick,
        kind: InvOpKind::Grant,
        src: dst,
        dst,
        item: granted.item,
        count: granted.count,
        nbt: granted.nbt,
        preconditions: Some(InventoryPreconditions {
            source_before: InventoryStack::empty(),
            destination_before,
        }),
        pickup: None,
    })
}

#[must_use]
pub fn capture_privileged_remove(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    src: InvLoc,
    source_before: InventoryStack,
) -> Option<InventoryOp> {
    let source_before = source_before.normalized();
    if source_before.is_empty() {
        return None;
    }
    Some(InventoryOp {
        gid,
        seq,
        tick,
        kind: InvOpKind::Remove,
        src,
        dst: src,
        item: source_before.item,
        count: source_before.count,
        nbt: source_before.nbt.clone(),
        preconditions: Some(InventoryPreconditions {
            destination_before: source_before.clone(),
            source_before,
        }),
        pickup: None,
    })
}

#[must_use]
pub fn capture_item_pickup(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    dst: InvLoc,
    entity: EntityRef,
    source_before: InventoryStack,
    source_after: InventoryStack,
    destination_before: InventoryStack,
) -> Option<InventoryOp> {
    let source_before = source_before.normalized();
    let source_after = source_after.normalized();
    let destination_before = destination_before.normalized();
    let count = source_before.count.checked_sub(source_after.count)?;
    if count == 0 {
        return None;
    }
    let op = InventoryOp {
        gid,
        seq,
        tick,
        kind: InvOpKind::Pickup,
        src: dst,
        dst,
        item: source_before.item,
        count,
        nbt: source_before.nbt.clone(),
        preconditions: Some(InventoryPreconditions {
            source_before: InventoryStack::empty(),
            destination_before: destination_before.clone(),
        }),
        pickup: Some(ItemPickup {
            entity,
            source_before,
            source_after,
            destination_before,
        }),
    };
    op.pickup
        .as_ref()
        .is_some_and(|pickup| {
            pickup.is_consistent_with(&op)
                && op.preconditions.as_ref().is_some_and(|preconditions| {
                    preconditions.source_before.is_empty()
                        && preconditions.destination_before == pickup.destination_before
                })
        })
        .then_some(op)
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

    fn semantic(
        kind: InvOpKind,
        src: u16,
        dst: u16,
        source_item: u16,
        source_count: u8,
        destination_item: u16,
        destination_count: u8,
        count: u8,
    ) -> InventoryOp {
        capture_semantic_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            kind,
            loc(src),
            loc(dst),
            InventoryStack {
                item: source_item,
                count: source_count,
                nbt: Vec::new(),
            },
            InventoryStack {
                item: destination_item,
                count: destination_count,
                nbt: Vec::new(),
            },
            count,
        )
        .expect("semantic operation")
    }

    #[test]
    fn move_replays_source_to_destination() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 32);
        assert_eq!(
            replay(
                &mut cells,
                &semantic(InvOpKind::Move, 0, 1, 5, 32, 0, 0, 10)
            ),
            InvVerdict::Applied
        );
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
    fn move_requires_and_preserves_exact_stack_nbt() {
        let mut cells = MemCells::new();
        let source = InventoryStack {
            item: 5,
            count: 12,
            nbt: vec![3, 4],
        };
        let destination = InventoryStack {
            item: 5,
            count: 10,
            nbt: vec![3, 4],
        };
        cells.seed_stack(loc(0), source.clone());
        cells.seed_stack(loc(1), destination.clone());
        let op = capture_semantic_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            InvOpKind::Move,
            loc(0),
            loc(1),
            source.clone(),
            destination.clone(),
            2,
        )
        .expect("nbt move");
        assert_eq!(replay(&mut cells, &op), InvVerdict::Applied);
        assert_eq!(
            cells.stack(loc(0)),
            Some(InventoryStack {
                item: 5,
                count: 10,
                nbt: vec![3, 4],
            })
        );
        assert_eq!(
            cells.stack(loc(1)),
            Some(InventoryStack {
                item: 5,
                count: 12,
                nbt: vec![3, 4],
            })
        );
        assert!(capture_semantic_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            InvOpKind::Move,
            loc(0),
            loc(1),
            source,
            InventoryStack {
                item: 5,
                count: 10,
                nbt: vec![9],
            },
            2,
        )
        .is_none());
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
        assert_eq!(
            replay(
                &mut cells,
                &semantic(InvOpKind::MoveHalf, 0, 1, 5, 33, 0, 0, 16)
            ),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(1)), Some((5, 16)));
        assert!(capture_semantic_inv_op(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            InvOpKind::MoveHalf,
            loc(0),
            loc(2),
            InventoryStack { item: 5, count: 17, nbt: Vec::new() },
            InventoryStack::empty(),
            9,
        ).is_none());
        assert_eq!(cells.cell(loc(0)), Some((5, 17)));
        assert_eq!(cells.cell(loc(2)), None);
    }

    #[test]
    fn split_moves_single_item_to_empty_slot() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 8);
        assert_eq!(
            replay(
                &mut cells,
                &semantic(InvOpKind::Split, 0, 1, 5, 8, 0, 0, 1)
            ),
            InvVerdict::Applied
        );
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
        assert_eq!(
            replay(
                &mut cells,
                &semantic(InvOpKind::Swap, 0, 1, 5, 8, 7, 3, 8)
            ),
            InvVerdict::Applied
        );
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
        assert_eq!(cells.cell(loc(1)), Some((5, 8)));
        assert_eq!(replay(&mut cells, &op(InvOpKind::Swap, 0, 1, 9, 3)), InvVerdict::Rejected);
        assert_eq!(cells.cell(loc(0)), Some((7, 3)));
    }

    #[test]
    fn consume_and_drop_never_duplicate_on_conflict() {
        let mut cells = MemCells::new();
        cells.seed(loc(0), 5, 4);
        assert_eq!(
            replay(
                &mut cells,
                &semantic(InvOpKind::Consume, 0, 0, 5, 4, 5, 4, 1)
            ),
            InvVerdict::Applied
        );
        assert_eq!(replay(&mut cells, &op(InvOpKind::Consume, 0, 0, 5, 4)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 3);
        assert_eq!(replay(&mut cells, &op(InvOpKind::Drop, 0, 0, 9, 1)), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 3);
    }

    #[test]
    fn pickup_stacks_only_onto_same_item_with_room() {
        let mut cells = MemCells::new();
        cells.seed(loc(1), 5, 60);
        let pickup = capture_item_pickup(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            loc(1),
            EntityRef {
                origin: ServerId(2),
                owner: ServerId(2),
                local_id: 9,
                chunk: crate::protocol::ChunkAddr { x: 0, z: 0 },
            },
            InventoryStack {
                item: 5,
                count: 4,
                nbt: Vec::new(),
            },
            InventoryStack::empty(),
            InventoryStack {
                item: 5,
                count: 60,
                nbt: Vec::new(),
            },
        )
        .expect("valid pickup");
        assert_eq!(replay(&mut cells, &pickup), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(1)), Some((5, 64)));
        assert_eq!(replay(&mut cells, &pickup), InvVerdict::Rejected);
        assert_eq!(cells.total(5), 64);
    }

    #[test]
    fn pickup_keeps_exact_undo_for_entity_and_destination() {
        let entity = EntityRef {
            origin: ServerId(3),
            owner: ServerId(3),
            local_id: 12,
            chunk: crate::protocol::ChunkAddr { x: 4, z: 5 },
        };
        let source_before = InventoryStack {
            item: 5,
            count: 12,
            nbt: vec![1, 2],
        };
        let destination_before = InventoryStack {
            item: 5,
            count: 20,
            nbt: vec![1, 2],
        };
        let pickup = capture_item_pickup(
            gid(1, 1),
            PlayerSeq(2),
            TickStamp(9),
            loc(1),
            entity,
            source_before.clone(),
            InventoryStack {
                item: 5,
                count: 8,
                nbt: vec![1, 2],
            },
            destination_before.clone(),
        )
        .expect("valid pickup");
        let undo = pickup.pickup.as_ref().expect("pickup data").undo();
        assert_eq!(undo.entity, entity);
        assert_eq!(undo.source_before, source_before);
        assert_eq!(undo.destination_before, destination_before);
        assert_eq!(pickup.count, 4);
    }

    #[test]
    fn pickup_rejects_source_identity_changes() {
        assert!(capture_item_pickup(
            gid(1, 1),
            PlayerSeq(0),
            TickStamp(7),
            loc(1),
            EntityRef {
                origin: ServerId(2),
                owner: ServerId(2),
                local_id: 9,
                chunk: crate::protocol::ChunkAddr { x: 0, z: 0 },
            },
            InventoryStack {
                item: 5,
                count: 4,
                nbt: vec![1],
            },
            InventoryStack {
                item: 5,
                count: 2,
                nbt: vec![2],
            },
            InventoryStack::empty(),
        )
        .is_none());
    }

    #[test]
    fn pickup_codec_preserves_undo_and_preconditions() {
        let pickup = capture_item_pickup(
            gid(1, 1),
            PlayerSeq(3),
            TickStamp(10),
            loc(2),
            EntityRef {
                origin: ServerId(4),
                owner: ServerId(4),
                local_id: 14,
                chunk: crate::protocol::ChunkAddr { x: 6, z: 7 },
            },
            InventoryStack {
                item: 5,
                count: 3,
                nbt: vec![9],
            },
            InventoryStack::empty(),
            InventoryStack::empty(),
        )
        .expect("valid pickup");
        let bytes = postcard::to_allocvec(&pickup).expect("pickup encodes");
        let decoded: InventoryOp = postcard::from_bytes(&bytes).expect("pickup decodes");
        assert_eq!(decoded, pickup);
        assert_eq!(
            decoded.pickup.as_ref().expect("pickup data").undo(),
            pickup.pickup.as_ref().expect("pickup data").undo()
        );
    }

    #[test]
    fn set_overwrites_without_duplication() {
        let mut cells = MemCells::new();
        cells.seed(loc(3), 5, 64);
        let set = capture_inv_set(gid(1, 1), PlayerSeq(1), TickStamp(7), loc(3), 7, 2, Vec::new());
        assert_eq!(replay(&mut cells, &set), InvVerdict::Rejected);
        let grant = capture_privileged_grant(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(7),
            loc(3),
            InventoryStack { item: 5, count: 64, nbt: Vec::new() },
            InventoryStack { item: 7, count: 2, nbt: Vec::new() },
        )
        .expect("grant");
        assert_eq!(replay(&mut cells, &grant), InvVerdict::Applied);
        assert_eq!(cells.cell(loc(3)), Some((7, 2)));
        assert_eq!(cells.total(5), 0);
    }

    #[test]
    fn privileged_remove_requires_the_exact_full_stack() {
        let mut cells = MemCells::new();
        let stack = InventoryStack {
            item: 5,
            count: 2,
            nbt: vec![6],
        };
        cells.seed_stack(loc(3), stack.clone());
        let remove = capture_privileged_remove(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(7),
            loc(3),
            stack,
        )
        .expect("remove");
        assert_eq!(replay(&mut cells, &remove), InvVerdict::Applied);
        assert_eq!(cells.stack(loc(3)), Some(InventoryStack::empty()));
    }

    #[test]
    fn ledger_rejects_stale_place_precondition() {
        let mut ledger = InvLedger::new();
        let at = InvLoc::new(INV_MAIN, 2);
        let before = InventoryStack { item: 40, count: 64, nbt: vec![2] };
        ledger.record_stack(gid(1, 1), at, before.clone());
        assert_eq!(
            ledger.check_place(
                gid(1, 1),
                at,
                before.clone(),
                InventoryStack { item: 40, count: 63, nbt: vec![2] },
            ),
            InvVerdict::Applied
        );
        assert_eq!(
            ledger.check_place(
                gid(1, 1),
                at,
                before,
                InventoryStack { item: 40, count: 63, nbt: vec![2] },
            ),
            InvVerdict::Rejected
        );
        assert_eq!(
            ledger.check_place(
                gid(1, 1),
                at,
                InventoryStack { item: 40, count: 63, nbt: vec![2] },
                InventoryStack { item: 40, count: 62, nbt: vec![2] },
            ),
            InvVerdict::Applied
        );
        assert_eq!(ledger.get(gid(1, 1), at), Some((40, 62)));
        let final_slot = InvLoc::new(INV_MAIN, 3);
        let final_stack = InventoryStack { item: 40, count: 1, nbt: vec![2] };
        ledger.record_stack(gid(1, 1), final_slot, final_stack.clone());
        assert_eq!(
            ledger.check_place(
                gid(1, 1),
                final_slot,
                final_stack,
                InventoryStack::empty(),
            ),
            InvVerdict::Applied
        );
        assert_eq!(ledger.get_stack(gid(1, 1), final_slot), None);
    }

    #[test]
    fn ledger_replay_then_conflicting_replay_rejects() {
        let mut ledger = InvLedger::new();
        let first = semantic(InvOpKind::Move, 0, 1, 5, 32, 0, 0, 10);
        ledger.record(gid(1, 1), loc(0), 5, 32);
        assert_eq!(ledger.apply_op(&first), InvVerdict::Applied);
        let conflict = semantic(InvOpKind::Move, 0, 2, 5, 30, 0, 0, 30);
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
