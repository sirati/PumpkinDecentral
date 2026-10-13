use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ApplyFailure, Cell, Target, Value};
use crate::canonical_inventory::InventorySlotBinding;
use crate::inventory::InventoryCell;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMenuProposal {
    pub session: crate::inventory::InventoryScreenSession,
    pub sync_id: u8,
    pub window_kind: u16,
    pub actor_chunk: crate::protocol::ChunkKey,
    pub kind: crate::inventory::InventoryMenuKind,
    pub companion_gestures: Option<Vec<crate::inventory::InventoryGesture>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipeIngredientFact {
    pub binding: InventorySlotBinding,
    pub item: u16,
    pub components: Vec<u8>,
    pub count: u8,
    pub remainder: Option<InventoryCell>,
    pub remainder_stack_limit: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipeInventoryIntent {
    pub recipe: String,
    pub catalogue: u64,
    pub grid: Vec<InventorySlotBinding>,
    pub ingredients: Vec<RecipeIngredientFact>,
    pub output: InventoryCell,
    pub output_stack_limit: u8,
    pub output_destinations: Vec<InventorySlotBinding>,
    pub remainder_destinations: Vec<InventorySlotBinding>,
    pub preview: Option<InventorySlotBinding>,
    pub repetitions: u8,
}

impl RecipeInventoryIntent {
    pub fn capture_action<S: crate::canonical_action::ActionState>(
        self,
        owners: &S,
        capture: crate::canonical_inventory::InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, ApplyFailure> {
        use crate::canonical_action::{
            Action, ActionClass, ActionKind, Condition, Effect, Expected, SemanticIntent,
            Stakeholder, TruthView, UndoProcedure,
        };
        let targets = self.targets();
        let mut values = BTreeMap::new();
        let mut conditions = capture.guards;
        for target in &targets {
            let cell = owners
                .read(TruthView::Local, *target)
                .ok_or(ApplyFailure::Inventory(*target))?;
            conditions.push(Condition {
                target: *target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(*target, cell);
        }
        self.derive(&values)?;
        let stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(targets.iter().map(|target| target.stakeholder()))
            .chain(capture.actor_chunk.map(Stakeholder::Chunk))
            .chain(
                capture
                    .player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        conditions.sort_by_key(|condition| condition.target);
        conditions.dedup();
        let action = Action {
            edit_authority: None,
            id: capture.id,
            tick: capture.tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: capture.actor_chunk,
            player_leases: capture.player_leases,
            player_chunks: capture.player_chunks,
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects: vec![Effect::CraftItems {
                intent: self.clone(),
            }],
            counterfactual_conditions: targets.clone(),
            counterfactual_effects: targets,
            semantic_intent: Some(SemanticIntent::RecipeInventory(self)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Inventory,
            presentation: Vec::new(),
            client_origin: capture.client_origin,
        };
        action.validate()?;
        Ok(action)
    }

    pub fn bindings(&self) -> Vec<InventorySlotBinding> {
        let mut by_target = BTreeMap::new();
        for binding in self
            .grid
            .iter()
            .chain(&self.output_destinations)
            .chain(&self.remainder_destinations)
            .chain(self.preview.iter())
        {
            by_target
                .entry(binding.target)
                .or_insert_with(|| binding.clone());
        }
        by_target.into_values().collect()
    }

    pub fn targets(&self) -> Vec<Target> {
        self.bindings()
            .into_iter()
            .map(|binding| binding.target)
            .collect()
    }

    pub fn valid(&self) -> bool {
        if self.recipe.is_empty()
            || self.recipe.len() > 256
            || self.catalogue == 0
            || self.grid.is_empty()
            || self.grid.len() > 9
            || self.ingredients.is_empty()
            || self.ingredients.len() > self.grid.len()
            || self.output.is_empty()
            || self.output_stack_limit == 0
            || self.output_stack_limit > 99
            || self.output.count > self.output_stack_limit
            || self.output.nbt.len() > crate::inventory::INVENTORY_MAX_NBT_BYTES
            || self.repetitions == 0
            || self.repetitions > 64
            || self.output_destinations.is_empty()
            || self.output_destinations.len() > 128
            || self.remainder_destinations.len() > 128
        {
            return false;
        }
        let grid = self
            .grid
            .iter()
            .map(|binding| binding.target)
            .collect::<BTreeSet<_>>();
        let ingredient_targets = self
            .ingredients
            .iter()
            .map(|ingredient| ingredient.binding.target)
            .collect::<BTreeSet<_>>();
        let mut bindings = BTreeMap::new();
        let consistent_bindings = self
            .grid
            .iter()
            .chain(&self.output_destinations)
            .chain(&self.remainder_destinations)
            .chain(self.preview.iter())
            .all(|binding| {
                binding.is_valid()
                    && bindings
                        .insert(binding.target, binding.clone())
                        .is_none_or(|previous| previous == *binding)
            });
        consistent_bindings
            && grid.len() == self.grid.len()
            && ingredient_targets.len() == self.ingredients.len()
            && ingredient_targets.is_subset(&grid)
            && self.bindings().iter().all(InventorySlotBinding::is_valid)
            && self.ingredients.iter().all(|ingredient| {
                self.grid
                    .iter()
                    .any(|binding| *binding == ingredient.binding)
                    && ingredient.count > 0
                    && ingredient.item != 0
                    && ingredient.components.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES
                    && ingredient.remainder.as_ref().is_none_or(|remainder| {
                        !remainder.is_empty()
                            && ingredient.remainder_stack_limit > 0
                            && ingredient.remainder_stack_limit <= 99
                            && remainder.count <= ingredient.remainder_stack_limit
                            && remainder.nbt.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES
                    })
            })
            && self.output_destinations.iter().all(|binding| {
                !grid.contains(&binding.target)
                    && self
                        .preview
                        .as_ref()
                        .is_none_or(|preview| preview.target != binding.target)
            })
            && self
                .preview
                .as_ref()
                .is_none_or(|preview| !grid.contains(&preview.target))
    }

    pub fn derive(
        &self,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut cells = BTreeMap::new();
        for binding in self.bindings() {
            let Some(Cell {
                value: Value::Inventory(cell),
                ..
            }) = values.get(&binding.target)
            else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            if cell.loc != binding.slot.loc {
                return Err(ApplyFailure::Inventory(binding.target));
            }
            cells.insert(binding.target, cell.clone());
        }
        let ingredients = self
            .ingredients
            .iter()
            .map(|ingredient| (ingredient.binding.target, ingredient))
            .collect::<BTreeMap<_, _>>();
        for _ in 0..self.repetitions {
            for binding in &self.grid {
                let current = &cells[&binding.target];
                if let Some(ingredient) = ingredients.get(&binding.target) {
                    if current.item != ingredient.item
                        || current.nbt != ingredient.components
                        || current.count < ingredient.count
                    {
                        return Err(ApplyFailure::Inventory(binding.target));
                    }
                } else if !current.is_empty() {
                    return Err(ApplyFailure::Inventory(binding.target));
                }
            }
            for ingredient in &self.ingredients {
                let current = cells
                    .get_mut(&ingredient.binding.target)
                    .ok_or(ApplyFailure::InvalidAction)?;
                current.count -= ingredient.count;
                if current.count == 0 {
                    *current = InventoryCell::empty(current.loc);
                }
            }
            insert(&mut cells, &self.output, &self.output_destinations, self.output_stack_limit)?;
            for ingredient in &self.ingredients {
                if let Some(remainder) = &ingredient.remainder {
                    let destinations = std::iter::once(ingredient.binding.clone())
                        .chain(self.remainder_destinations.iter().cloned())
                        .collect::<Vec<_>>();
                    let mut remainder = remainder.clone();
                    remainder.count = remainder
                        .count
                        .checked_mul(ingredient.count)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    if cells[&ingredient.binding.target].is_empty() {
                        let count = remainder.count.min(ingredient.binding.max_stack).min(ingredient.remainder_stack_limit);
                        let mut restored = remainder.clone();
                        restored.loc = ingredient.binding.slot.loc;
                        restored.count = count;
                        cells.insert(ingredient.binding.target, restored);
                        remainder.count -= count;
                    }
                    if remainder.count > 0 {
                        insert(&mut cells, &remainder, &destinations, ingredient.remainder_stack_limit)?;
                    }
                }
            }
        }
        if let Some(preview) = &self.preview {
            let still_matches = self.grid.iter().all(|binding| {
                let current = &cells[&binding.target];
                ingredients
                    .get(&binding.target)
                    .map_or(current.is_empty(), |ingredient| {
                        current.item == ingredient.item
                            && current.nbt == ingredient.components
                            && current.count >= ingredient.count
                    })
            });
            let mut output = if still_matches {
                self.output.clone()
            } else {
                InventoryCell::empty(preview.slot.loc)
            };
            output.loc = preview.slot.loc;
            cells.insert(preview.target, output);
        }
        Ok(cells
            .into_iter()
            .filter(|(target, after)| {
                matches!(values.get(target),
            Some(Cell { value: Value::Inventory(before), .. }) if before != after)
            })
            .collect())
    }
}

fn insert(
    cells: &mut BTreeMap<Target, InventoryCell>,
    item: &InventoryCell,
    destinations: &[InventorySlotBinding],
    item_limit: u8,
) -> Result<(), ApplyFailure> {
    let mut remaining = item.count;
    for empty in [false, true] {
        let mut visited = BTreeSet::new();
        for binding in destinations {
            if !visited.insert(binding.target) {
                continue;
            }
            let current = cells
                .get(&binding.target)
                .ok_or(ApplyFailure::Inventory(binding.target))?;
            if current.is_empty() != empty
                || !current.is_empty() && (current.item != item.item || current.nbt != item.nbt)
            {
                continue;
            }
            let amount = remaining.min(binding.max_stack.min(item_limit).saturating_sub(current.count));
            if amount == 0 {
                continue;
            }
            let mut after = item.clone();
            after.loc = current.loc;
            after.count = current.count + amount;
            cells.insert(binding.target, after);
            remaining -= amount;
            if remaining == 0 {
                return Ok(());
            }
        }
    }
    Err(ApplyFailure::Inventory(
        destinations
            .first()
            .ok_or(ApplyFailure::InvalidAction)?
            .target,
    ))
}
