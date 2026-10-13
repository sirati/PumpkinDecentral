use std::collections::BTreeSet;

use pumpkin_cluster::canonical_action::{Cell, ChunkPart, InventoryId, Target, Value};
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::inventory::{INV_MAIN, InvLoc, InventoryApplyError, InventoryCell, InventorySlotRef, InventoryStateSnapshot, InventoryTarget};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};

#[derive(Clone, PartialEq)]
pub(super) struct BlockInventoryRootImage {
    pub position: BlockPos,
    pub state: u16,
    pub block_entity: Cell,
}

#[derive(Clone, PartialEq)]
pub(super) struct CanonicalBlockInventoryRoot {
    pub chunk: ChunkKey,
    pub image: BlockInventoryRootImage,
    pub slots: u16,
    pub deferred_loot: bool,
    field: &'static str,
}

impl CanonicalBlockInventoryRoot {
    pub(super) fn from_image(chunk: ChunkKey, image: BlockInventoryRootImage) -> Result<Option<Self>, InventoryApplyError> {
        let Value::BlockEntity(bytes) = &image.block_entity.value else { return Err(InventoryApplyError::Rejected); };
        if image.position.x.div_euclid(16) != chunk.x() || image.position.z.div_euclid(16) != chunk.z()
            || !pumpkin_world::level::cluster_block_entity_document_type_matches(pumpkin_data::BlockStateId::new(image.state).ok_or(InventoryApplyError::Rejected)?, bytes) {
            return Err(InventoryApplyError::Rejected);
        }
        let state = pumpkin_data::BlockStateId::new(image.state).ok_or(InventoryApplyError::Rejected)?;
        let name = state.to_block().name.strip_prefix("minecraft:").unwrap_or(state.to_block().name);
        let (slots, field) = match name {
            "chest" | "trapped_chest" | "barrel" => (27, "Items"),
            name if name.ends_with("shulker_box") => (27, "Items"),
            "hopper" | "brewing_stand" => (5, "Items"),
            "dispenser" | "dropper" | "crafter" => (9, "Items"),
            "furnace" | "blast_furnace" | "smoker" => (3, "Items"),
            "campfire" | "soul_campfire" => (4, "Items"),
            "chiseled_bookshelf" => (6, "Items"),
            "beacon" => (1, ""),
            "lectern" => (1, "Book"),
            "jukebox" => (1, "RecordItem"),
            "decorated_pot" | "suspicious_sand" | "suspicious_gravel" => (1, "item"),
            _ => return Ok(None),
        };
        let document = Self::document(bytes)?;
        let deferred_loot = document.root_tag.get_string("LootTable").is_some();
        Ok(Some(Self { chunk, image, slots, deferred_loot, field }))
    }

    fn document(bytes: &[u8]) -> Result<pumpkin_nbt::Nbt, InventoryApplyError> {
        let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(bytes));
        pumpkin_nbt::Nbt::read_unnamed(&mut reader).map_err(|_| InventoryApplyError::Rejected)
    }

    pub(super) fn inventory_target(&self) -> InventoryTarget {
        InventoryTarget::Block { chunk: self.chunk, position: self.image.position }
    }

    pub(super) fn bindings(&self) -> Vec<InventorySlotBinding> {
        (0..self.slots).map(|slot| {
            let loc = InvLoc::new(INV_MAIN, slot);
            InventorySlotBinding { slot: InventorySlotRef::new(self.inventory_target(), loc),
                target: Target::Chunk { key: self.chunk, part: ChunkPart::Block(self.image.position),
                    inventory: Some(InventoryId::Block { inv: loc.inv, slot: loc.slot }) }, max_stack: if self.field.is_empty() { 1 } else { 99 } }
        }).collect()
    }

    pub(super) fn validate_snapshot(&self, snapshot: &InventoryStateSnapshot) -> Result<(), InventoryApplyError> {
        if snapshot.target != self.inventory_target() || snapshot.cells.len() != usize::from(self.slots)
            || snapshot.cells.iter().map(|item| item.loc).collect::<BTreeSet<_>>()
                != (0..self.slots).map(|slot| InvLoc::new(INV_MAIN, slot)).collect()
            || self.field.is_empty() && snapshot.cells.iter().any(|item| !item.is_empty() && item.count > 1)
            || self.deferred_loot && snapshot.cells.iter().any(|item| !item.is_empty()) {
            return Err(InventoryApplyError::Rejected);
        }
        Ok(())
    }

    pub(super) fn persisted_snapshot(&self) -> Result<InventoryStateSnapshot, InventoryApplyError> {
        let Value::BlockEntity(bytes) = &self.image.block_entity.value else { return Err(InventoryApplyError::Rejected); };
        let document = Self::document(bytes)?;
        let mut cells = (0..self.slots).map(|slot| InventoryCell::empty(InvLoc::new(INV_MAIN, slot))).collect::<Vec<_>>();
        let mut occupied = BTreeSet::new();
        let mut insert = |slot: u16, compound: &pumpkin_nbt::compound::NbtCompound| -> Result<(), InventoryApplyError> {
            if slot >= self.slots || !occupied.insert(slot) || self.deferred_loot { return Err(InventoryApplyError::Rejected); }
            let count = compound.get_int("count").ok_or(InventoryApplyError::Rejected)?;
            if !(1..=99).contains(&count) { return Err(InventoryApplyError::Rejected); }
            let stack = pumpkin_data::item_stack::ItemStack::read_item_stack(compound).ok_or(InventoryApplyError::Rejected)?;
            if stack.is_empty() || stack.item_count != count as u8 || stack.item_count > stack.get_max_stack_size() { return Err(InventoryApplyError::Rejected); }
            cells[usize::from(slot)] = super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, slot), &stack)?;
            Ok(())
        };
        if self.field == "Items" {
            if let Some(items) = document.root_tag.get_list(self.field) {
                for tag in items {
                    let compound = tag.extract_compound().ok_or(InventoryApplyError::Rejected)?;
                    let slot = compound.get_byte("Slot").filter(|slot| *slot >= 0).ok_or(InventoryApplyError::Rejected)? as u16;
                    insert(slot, compound)?;
                }
            }
        } else if !self.field.is_empty() && let Some(item) = document.root_tag.get_compound(self.field) { insert(0, item)?; }
        let snapshot = InventoryStateSnapshot { target: self.inventory_target(), revision: self.image.block_entity.revision, cells };
        self.validate_snapshot(&snapshot)?;
        Ok(snapshot)
    }
}

pub(super) fn validate_layout(block: Target, state: u16, block_entity: &Cell,
    bindings: &[InventorySlotBinding],
) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let Target::Chunk { key: chunk, part: ChunkPart::Block(position), inventory: None } = block else {
        return Err(ApplyFailure::InvalidAction);
    };
    let root = CanonicalBlockInventoryRoot::from_image(chunk, BlockInventoryRootImage {
        position, state, block_entity: block_entity.clone(),
    }).map_err(|_| ApplyFailure::InvalidAction)?;
    let Some(root) = root else { return if bindings.is_empty() { Ok(()) } else { Err(ApplyFailure::InvalidAction) }; };
    if root.deferred_loot || bindings.len() != usize::from(root.slots) { return Err(ApplyFailure::InvalidAction); }
    let expected = root.bindings().into_iter().map(|binding| (binding.target, binding.slot)).collect::<BTreeSet<_>>();
    let actual = bindings.iter().map(|binding| (binding.target, binding.slot)).collect::<BTreeSet<_>>();
    if bindings.iter().any(|binding| !binding.is_valid()) || expected != actual { return Err(ApplyFailure::InvalidAction); }
    Ok(())
}
