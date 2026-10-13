use std::collections::BTreeSet;

use crate::inventory::{
    InventoryCell, InventoryGesture, InventoryGestureError, InventorySlotRef,
    InventoryTransferCount,
};

pub struct ClaimedSlotChange {
    pub slot: InventorySlotRef,
    pub before: InventoryCell,
    pub after: InventoryCell,
    pub limit: u8,
}

pub fn decode_slot_changes(
    changes: &[ClaimedSlotChange],
) -> Result<Vec<InventoryGesture>, InventoryGestureError> {
    if changes.len() > crate::inventory::INVENTORY_SERVICE_MAX_SLOTS {
        return Err(InventoryGestureError::InvalidCount);
    }
    let mut slots = BTreeSet::new();
    for change in changes {
        if !slots.insert(change.slot)
            || change.before.loc != change.slot.loc
            || change.after.loc != change.slot.loc
            || change.limit == 0
            || change.limit > 99
            || change.before.count > 99
            || change.after.count > change.limit
            || change.before.nbt.len() > crate::inventory::INVENTORY_MAX_NBT_BYTES
            || change.after.nbt.len() > crate::inventory::INVENTORY_MAX_NBT_BYTES
            || (change.before.count == 0) != (change.before.item == 0)
            || (change.after.count == 0) != (change.after.item == 0)
            || change.before.is_empty() && !change.before.nbt.is_empty()
            || change.after.is_empty() && !change.after.nbt.is_empty()
        {
            return Err(InventoryGestureError::InvalidStack);
        }
    }
    let changed = changes
        .iter()
        .filter(|change| change.before != change.after)
        .collect::<Vec<_>>();
    let same = |left: &InventoryCell, right: &InventoryCell| {
        left.item == right.item && left.count == right.count && left.nbt == right.nbt
    };
    let guard = |change: &ClaimedSlotChange| InventoryGesture::ExpectIdentity {
        slot: change.slot,
        item: change.before.item,
        components: change.before.nbt.clone(),
    };
    if changed.len() == 2
        && same(&changed[0].before, &changed[1].after)
        && same(&changed[1].before, &changed[0].after)
        && changed[0].before.item != changed[1].before.item
    {
        let mut gestures = changed
            .iter()
            .filter(|change| !change.before.is_empty())
            .map(|change| guard(change))
            .collect::<Vec<_>>();
        gestures.push(InventoryGesture::Swap {
            first: changed[0].slot,
            second: changed[1].slot,
            first_limit: changed[0].limit,
            second_limit: changed[1].limit,
        });
        return Ok(gestures);
    }
    let mut sources = Vec::new();
    let mut destinations = Vec::new();
    for change in changed {
        if !change.before.is_empty()
            && !change.after.is_empty()
            && (change.before.item != change.after.item || change.before.nbt != change.after.nbt)
        {
            return Err(InventoryGestureError::IncompatibleStack);
        }
        if change.before.count > change.after.count {
            sources.push((change, change.before.count - change.after.count));
        } else if change.after.count > change.before.count {
            destinations.push((change, change.after.count - change.before.count));
        }
    }
    let mut gestures = Vec::new();
    for (source, amount) in sources {
        gestures.push(guard(source));
        let candidates = destinations
            .iter()
            .enumerate()
            .filter(|(_, (destination, remaining))| {
                *remaining > 0
                    && destination.after.item == source.before.item
                    && destination.after.nbt == source.before.nbt
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let mut remaining = amount;
        for index in candidates.iter().copied() {
            let (destination, available) = &mut destinations[index];
            let moved = remaining.min(*available);
            let count = if candidates.len() == 1 && moved == amount && amount == source.before.count
            {
                InventoryTransferCount::All
            } else if candidates.len() == 1
                && moved == amount
                && amount == source.before.count.div_ceil(2)
            {
                InventoryTransferCount::Half
            } else {
                InventoryTransferCount::Exact(moved)
            };
            gestures.push(InventoryGesture::TransferBy {
                source: source.slot,
                destination: destination.slot,
                count,
                destination_limit: destination.limit,
            });
            remaining -= moved;
            *available -= moved;
            if remaining == 0 {
                break;
            }
        }
        if remaining != 0 {
            return Err(InventoryGestureError::InvalidCount);
        }
    }
    if destinations.iter().any(|(_, remaining)| *remaining != 0) {
        return Err(InventoryGestureError::InvalidCount);
    }
    Ok(gestures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::inventory::{
        InvLoc, InventoryStateSnapshot, InventoryTarget, reduce_inventory_gestures,
    };

    fn change(slot: u16, before: u8, after: u8) -> ClaimedSlotChange {
        let loc = InvLoc::new(crate::inventory::INV_MAIN, slot);
        let cell = |count| {
            if count == 0 {
                InventoryCell::empty(loc)
            } else {
                InventoryCell {
                    loc,
                    item: 7,
                    count,
                    nbt: vec![4],
                }
            }
        };
        ClaimedSlotChange {
            slot: InventorySlotRef::new(
                InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(2))),
                loc,
            ),
            before: cell(before),
            after: cell(after),
            limit: 64,
        }
    }

    #[test]
    fn decoded_half_transfer_rebases_after_pickup_rollback() {
        let changes = [change(0, 13, 6), change(1, 0, 7)];
        let gestures = decode_slot_changes(&changes).unwrap();
        let mut source = changes[0].before.clone();
        source.count = 5;
        let reduced = reduce_inventory_gestures(
            &[InventoryStateSnapshot {
                target: changes[0].slot.target,
                revision: 0,
                cells: vec![source, changes[1].before.clone()],
            }],
            &gestures,
        )
        .unwrap();
        assert_eq!(
            reduced
                .mutation
                .operations
                .iter()
                .find(|operation| operation.after.loc.slot == 0)
                .unwrap()
                .after
                .count,
            2
        );
        assert_eq!(
            reduced
                .mutation
                .operations
                .iter()
                .find(|operation| operation.after.loc.slot == 1)
                .unwrap()
                .after
                .count,
            3
        );
    }

    #[test]
    fn component_sized_stack_transfer_keeps_all_ninety_nine_items() {
        let mut source = change(0, 99, 0);
        let mut destination = change(1, 0, 99);
        source.limit = 99;
        destination.limit = 99;
        let gestures = decode_slot_changes(&[source, destination]).unwrap();
        let source = change(0, 99, 0);
        let destination = change(1, 0, 99);
        let reduced = reduce_inventory_gestures(
            &[InventoryStateSnapshot {
                target: source.slot.target,
                revision: 0,
                cells: vec![source.before, destination.before],
            }],
            &gestures,
        )
        .unwrap();
        assert_eq!(
            reduced
                .mutation
                .operations
                .iter()
                .find(|operation| operation.after.loc.slot == 0)
                .unwrap()
                .after
                .count,
            0
        );
        assert_eq!(
            reduced
                .mutation
                .operations
                .iter()
                .find(|operation| operation.after.loc.slot == 1)
                .unwrap()
                .after
                .count,
            99
        );
    }

    #[test]
    fn decoded_transfer_cannot_copy_items_or_change_components() {
        assert!(decode_slot_changes(&[change(0, 3, 2), change(1, 0, 2)]).is_err());
        let changes = [change(0, 3, 0), change(1, 0, 3)];
        let gestures = decode_slot_changes(&changes).unwrap();
        let mut source = changes[0].before.clone();
        source.nbt = vec![5];
        assert!(
            reduce_inventory_gestures(
                &[InventoryStateSnapshot {
                    target: changes[0].slot.target,
                    revision: 0,
                    cells: vec![source, changes[1].before.clone()]
                }],
                &gestures
            )
            .is_err()
        );
        assert!(decode_slot_changes(&[change(0, 3, 2), change(0, 3, 2)]).is_err());
    }
}
