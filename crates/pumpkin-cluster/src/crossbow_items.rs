use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ActionId, ActionState, ApplyFailure, Cell, Target};
use crate::canonical_inventory::InventorySlotBinding;
use crate::inventory::InventoryCell;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossbowItems {
    pub loading: bool,
    pub weapon: InventorySlotBinding,
    pub ammunition: Option<InventorySlotBinding>,
    pub game_mode: Option<Target>,
}

impl CrossbowItems {
    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.weapon.target).chain(self.ammunition.iter().map(|binding| binding.target)).chain(self.game_mode)
    }

    pub fn valid(&self) -> bool {
        self.weapon.is_valid() && (self.loading || self.ammunition.is_none()) && self.ammunition.as_ref().is_none_or(|ammunition|
            ammunition.is_valid() && ammunition.target != self.weapon.target
                && ammunition.slot.target == self.weapon.slot.target)
    }

    pub fn derive<S: ActionState>(&self, context: &S, id: ActionId, tick: TickStamp, values: &BTreeMap<Target, Cell>)
        -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        if !self.valid() { return Err(ApplyFailure::InvalidAction); }
        let writes = context.crossbow_items(self, id, tick, values)?;
        if writes.is_empty() || writes.keys().any(|target| *target != self.weapon.target
            && self.ammunition.as_ref().is_none_or(|binding| binding.target != *target)) {
            return Err(ApplyFailure::InvalidAction);
        }
        for (target, item) in &writes {
            let binding = if *target == self.weapon.target { &self.weapon } else {
                self.ammunition.as_ref().ok_or(ApplyFailure::InvalidAction)?
            };
            if item.loc != binding.slot.loc || item.count > binding.max_stack
                || !item.is_empty() && item.count > context.inventory_limit(item, *target)? {
                return Err(ApplyFailure::Inventory(*target));
            }
        }
        Ok(writes)
    }
}
