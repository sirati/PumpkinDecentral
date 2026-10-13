use serde::{Deserialize, Serialize};

use crate::canonical_action::{Cell, ChunkPart, InventoryId, Target, Value};
use crate::inventory::{INV_MAIN, INVENTORY_MAX_NBT_BYTES, InvLoc};
use crate::protocol::{ChunkKey, GlobalEntityId};

pub const INV_EQUIPMENT: u8 = 5;
pub const ENTITY_EQUIPMENT_SLOTS: u16 = 8;
pub const ENTITY_STORAGE_MAX_SLOTS: u16 = 27;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityInventoryAdmission {
    pub imported_revision: u64,
    pub cells: Vec<Cell>,
}

impl EntityInventoryAdmission {
    pub fn valid(&self) -> bool {
        let mut locations = std::collections::BTreeSet::new();
        self.cells.len() <= usize::from(ENTITY_EQUIPMENT_SLOTS + ENTITY_STORAGE_MAX_SLOTS)
            && self.cells.iter().all(|cell| {
                let Value::Inventory(item) = &cell.value else {
                    return false;
                };
                cell.revision == 0
                    && locations.insert(item.loc)
                    && matches!(
                        item.loc,
                        InvLoc {
                            inv: INV_EQUIPMENT,
                            slot: 0..=7
                        } | InvLoc {
                            inv: INV_MAIN,
                            slot: 0..=26
                        }
                    )
                    && item.count <= crate::inventory::INV_MAX_STACK
                    && item.nbt.len() <= INVENTORY_MAX_NBT_BYTES
                    && ((item.item == 0 && item.count == 0 && item.nbt.is_empty())
                        || (item.item != 0 && item.count != 0))
            })
    }

    pub fn entries(
        &self,
        chunk: ChunkKey,
        entity: GlobalEntityId,
    ) -> impl Iterator<Item = (Target, &Cell)> {
        self.cells.iter().filter_map(move |cell| {
            let Value::Inventory(item) = &cell.value else {
                return None;
            };
            Some((
                Target::Chunk {
                    key: chunk,
                    part: ChunkPart::Entity(entity),
                    inventory: Some(InventoryId::Entity {
                        inv: item.loc.inv,
                        slot: item.loc.slot,
                    }),
                },
                cell,
            ))
        })
    }
}
