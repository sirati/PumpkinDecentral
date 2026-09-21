use crate::identity::GlobalPlayerId;

pub const INVSEE_MAX_SLOTS: usize = 48;
pub const INVSEE_MAX_NAME_CHARS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvseeSlot {
    pub index: u16,
    pub item_id: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

impl InvseeSlot {
    #[must_use]
    pub fn new(index: u16, item_id: u16, count: u8, nbt: Vec<u8>) -> Self {
        Self {
            index,
            item_id,
            count,
            nbt,
        }
    }

    #[must_use]
    pub const fn is_empty_slot(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventorySnapshot {
    pub target: GlobalPlayerId,
    pub owner_name: String,
    pub selected: u8,
    pub slots: Vec<InvseeSlot>,
}

impl InventorySnapshot {
    #[must_use]
    pub fn new(
        target: GlobalPlayerId,
        owner_name: String,
        selected: u8,
        mut slots: Vec<InvseeSlot>,
    ) -> Self {
        let capped_name: String = owner_name.chars().take(INVSEE_MAX_NAME_CHARS).collect();
        slots.truncate(INVSEE_MAX_SLOTS);
        Self {
            target,
            owner_name: capped_name,
            selected,
            slots,
        }
    }

    #[must_use]
    pub fn slot(&self, index: u16) -> Option<&InvseeSlot> {
        self.slots.iter().find(|slot| slot.index == index)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}
