use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
use crate::canonical_action::{Action, ActionClass, ActionKind, ActionState, ApplyFailure, Cell, ChunkPart, Condition, Effect, Expected, LivePlayerLease, SemanticIntent, Stakeholder, Target, TruthView, UndoProcedure, Value};
use crate::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
use crate::canonical_menu::{MenuSessionState, MenuSource};
use crate::inventory::InventoryCell;
use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MenuInventoryTransformKind { Loom, Smithing, Stonecutter, Cartography }

impl MenuInventoryTransformKind {
    pub fn window_kind(self) -> u16 { match self { Self::Loom => 18, Self::Smithing => 21, Self::Stonecutter => 24, Self::Cartography => 23 } }
    pub fn inputs(self) -> usize { match self { Self::Loom | Self::Smithing => 3, Self::Stonecutter => 1, Self::Cartography => 2 } }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MenuInventoryTransformOutput {
    pub consumed: Vec<u8>,
    pub output: InventoryCell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuInventoryTransformIntent {
    pub lease: LivePlayerLease,
    pub menu_target: Target,
    pub menu: MenuSessionState,
    pub viewer: crate::item_contact::ContactObservation,
    pub kind: MenuInventoryTransformKind,
    pub inputs: Vec<InventorySlotBinding>,
    pub result: InventorySlotBinding,
    pub destinations: Vec<InventorySlotBinding>,
    pub repetitions: u8,
}

impl MenuInventoryTransformIntent {
    pub fn property_target(&self, property: P) -> Target {
        Target::Chunk { key: self.viewer.chunk, part: ChunkPart::LivingProperty { actor: self.viewer.actor(), property }, inventory: None }
    }

    pub fn source_targets(&self) -> Vec<Target> {
        match &self.menu.source {
            MenuSource::Block { chunk, position, .. } => vec![Target::Chunk { key: *chunk, part: ChunkPart::Block(*position), inventory: None }],
            MenuSource::Blocks(blocks) => blocks.iter().map(|block| Target::Chunk { key: block.chunk, part: ChunkPart::Block(block.position), inventory: None }).collect(),
            _ => Vec::new(),
        }
    }

    pub fn bindings(&self) -> Vec<InventorySlotBinding> {
        self.inputs.iter().chain(std::iter::once(&self.result)).chain(&self.destinations).cloned().collect()
    }

    pub fn targets(&self) -> Vec<Target> {
        let mut targets = self.bindings().into_iter().map(|binding| binding.target)
            .chain([self.menu_target, self.viewer.target(), self.property_target(P::Health), self.property_target(P::GameMode)])
            .chain(self.source_targets()).collect::<Vec<_>>();
        targets.sort_unstable(); targets.dedup(); targets
    }

    pub fn valid(&self) -> bool {
        let actor = crate::actor::ActorId::Player(self.lease.gid);
        let witness = crate::canonical_menu::MenuSessionIntent { lease: self.lease, target: self.menu_target,
            operation: crate::canonical_menu::MenuSessionOperation::Open { state: self.menu.clone(), viewer: self.viewer.clone() }, virtual_inventory: None };
        witness.valid() && self.viewer.actor() == actor && self.menu.window_kind == self.kind.window_kind()
            && matches!(self.menu.source, MenuSource::Block { .. } | MenuSource::Blocks(_))
            && self.inputs.len() == self.kind.inputs() && self.repetitions > 0 && self.repetitions <= 64
            && !self.destinations.is_empty() && self.destinations.len() <= 36
            && self.bindings().iter().all(InventorySlotBinding::is_valid)
            && self.bindings().iter().map(|binding| binding.target).collect::<BTreeSet<_>>().len() == self.bindings().len()
            && self.inputs.iter().chain(std::iter::once(&self.result)).enumerate().all(|(index, binding)|
                matches!(binding.slot.target, crate::inventory::InventoryTarget::LeasedVirtual { key, player, incarnation, session, kind, .. }
                    if key == self.lease.key.0 && player == self.lease.gid && incarnation == self.lease.incarnation
                        && session == self.menu.session && kind == self.kind.window_kind() + 1)
                    && binding.slot.loc == crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, index as u16))
            && self.destinations.iter().all(|binding| matches!(binding.slot.target,
                crate::inventory::InventoryTarget::LeasedPlayer { key, player, incarnation }
                    if key == self.lease.key.0 && player == self.lease.gid && incarnation == self.lease.incarnation)
                && (binding.slot.loc.inv == crate::inventory::INV_MAIN && binding.slot.loc.slot < 36
                    || binding.slot.loc.inv == crate::inventory::INV_CURSOR && binding.slot.loc.slot == 0))
    }

    pub fn derive<S: ActionState>(&self, state: &S, values: &BTreeMap<Target, Cell>) -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        if !self.valid() || !crate::canonical_menu::contact_matches(&self.viewer, values.get(&self.viewer.target()))
            || !matches!(values.get(&self.menu_target), Some(Cell { value: Value::LivingProperty(V::MenuSession(menu)), .. }) if menu == &self.menu)
            || !matches!(values.get(&self.property_target(P::Health)), Some(Cell { value: Value::LivingProperty(V::Unsigned(health)), .. }) if *health > 0)
            || !matches!(values.get(&self.property_target(P::GameMode)), Some(Cell { value: Value::LivingProperty(V::Unsigned(mode)), .. }) if *mode < 3) {
            return Err(ApplyFailure::InvalidAction);
        }
        let blocks = match &self.menu.source {
            MenuSource::Block { chunk, position, state } => vec![crate::canonical_menu::MenuBlockContact { chunk: *chunk, position: *position, state: *state }],
            MenuSource::Blocks(blocks) => blocks.clone(), _ => return Err(ApplyFailure::InvalidAction),
        };
        for block in blocks {
            let target = Target::Chunk { key: block.chunk, part: ChunkPart::Block(block.position), inventory: None };
            if !matches!(values.get(&target), Some(Cell { value: Value::Block(actual), .. }) if *actual == block.state) {
                return Err(ApplyFailure::Condition(target));
            }
        }
        let mut cells = self.bindings().into_iter().map(|binding| match values.get(&binding.target) {
            Some(Cell { value: Value::Inventory(cell), .. }) if cell.loc == binding.slot.loc => Ok((binding.target, cell.clone())),
            _ => Err(ApplyFailure::Inventory(binding.target)),
        }).collect::<Result<BTreeMap<_, _>, _>>()?;
        for _ in 0..self.repetitions {
            let inputs = self.inputs.iter().map(|binding| cells[&binding.target].clone()).collect::<Vec<_>>();
            let transformed = state.menu_inventory_transform(self.kind, &self.menu.selection, &inputs)?;
            if transformed.consumed.len() != inputs.len() || transformed.output.is_empty()
                || transformed.consumed.iter().all(|count| *count == 0) { return Err(ApplyFailure::InvalidAction); }
            for (binding, count) in self.inputs.iter().zip(&transformed.consumed) {
                let cell = cells.get_mut(&binding.target).ok_or(ApplyFailure::Inventory(binding.target))?;
                cell.count = cell.count.checked_sub(*count).ok_or(ApplyFailure::Inventory(binding.target))?;
                if cell.count == 0 { *cell = InventoryCell::empty(cell.loc); }
            }
            let mut remaining = transformed.output;
            for empty in [false, true] {
                for destination in &self.destinations {
                    let current = &cells[&destination.target];
                    if current.is_empty() != empty || !current.is_empty() && (current.item != remaining.item || current.nbt != remaining.nbt) { continue; }
                    let limit = destination.max_stack.min(state.inventory_limit(&remaining, destination.target)?);
                    let count = remaining.count.min(limit.saturating_sub(current.count));
                    if count == 0 { continue; }
                    let mut after = remaining.clone(); after.loc = current.loc; after.count = current.count + count;
                    cells.insert(destination.target, after); remaining.count -= count;
                    if remaining.count == 0 { break; }
                }
                if remaining.count == 0 { break; }
            }
            if remaining.count != 0 { return Err(ApplyFailure::Inventory(self.result.target)); }
        }
        cells.insert(self.result.target, InventoryCell::empty(self.result.slot.loc));
        cells.retain(|target, cell| !matches!(values.get(target), Some(Cell { value: Value::Inventory(before), .. }) if before == cell));
        Ok(cells)
    }

    pub fn capture_action<S: ActionState>(&self, state: &S, capture: InventoryActionCapture) -> Result<Action, ApplyFailure> {
        if capture.id.actor != self.viewer.actor() || capture.id.incarnation != self.lease.incarnation
            || !capture.player_leases.contains(&self.lease) { return Err(ApplyFailure::InvalidAction); }
        let targets = self.targets();
        let values = targets.iter().map(|target| state.read(TruthView::Local, *target).map(|cell| (*target, cell))
            .ok_or(ApplyFailure::Condition(*target))).collect::<Result<BTreeMap<_, _>, _>>()?;
        self.derive(state, &values)?;
        let mut conditions = capture.guards;
        for target in &targets {
            let expected = if *target == self.viewer.target() { Expected::SpatialEpochEquals(self.viewer.epoch) }
                else if *target == self.property_target(P::Health) { Expected::LivingUnsignedAtLeast(1) }
                else { Expected::Exact(values[target].clone()) };
            conditions.push(Condition { target: *target, expected });
        }
        let stakeholders = targets.iter().map(|target| target.stakeholder()).chain(conditions.iter().map(|condition| condition.target.stakeholder()))
            .chain(capture.actor_chunk.map(Stakeholder::Chunk)).chain(capture.player_chunks.iter().map(|(_, chunk)| Stakeholder::Chunk(*chunk)))
            .collect::<BTreeSet<_>>().into_iter().collect();
        let action = Action { id: capture.id, tick: capture.tick, class: ActionClass::Replayable, chunk_transition: false,
            actor_chunk: capture.actor_chunk, player_leases: capture.player_leases, player_chunks: capture.player_chunks,
            stakeholders, dependencies: Vec::new(), conditions, effects: vec![Effect::TransformMenuInventory { intent: self.clone() }],
            counterfactual_conditions: targets, counterfactual_effects: self.bindings().into_iter().map(|binding| binding.target).collect(),
            semantic_intent: Some(SemanticIntent::MenuInventoryTransform(self.clone())), factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets], kind: ActionKind::Inventory, presentation: Vec::new(),
            client_origin: capture.client_origin, edit_authority: None };
        action.validate()?; Ok(action)
    }
}
