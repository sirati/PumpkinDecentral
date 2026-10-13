use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
use crate::canonical_action::{Action, ActionId, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Effect, Expected, Target, Value};
use crate::canonical_inventory::InventorySlotBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockInventoryTickKind { Hopper, Furnace, BlastFurnace, Smoker, BrewingStand }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockInventoryTickRoot {
    pub block: Target,
    pub state: u16,
    pub bindings: Vec<InventorySlotBinding>,
}

impl BlockInventoryTickRoot {
    pub fn block_entity(&self) -> Target {
        match self.block { Target::Chunk { key, part: ChunkPart::Block(position), inventory: None } =>
            Target::Chunk { key, part: ChunkPart::BlockEntity(position), inventory: None }, _ => self.block }
    }
    pub fn valid(&self) -> bool {
        matches!(self.block, Target::Chunk { part: ChunkPart::Block(_), inventory: None, .. })
            && !self.bindings.is_empty() && self.bindings.len() <= 54
            && self.bindings.iter().all(InventorySlotBinding::is_valid)
            && self.bindings.iter().map(|binding| binding.target).collect::<BTreeSet<_>>().len() == self.bindings.len()
            && self.bindings.iter().all(|binding| matches!((self.block, binding.target),
                (Target::Chunk { key, part: ChunkPart::Block(position), .. }, Target::Chunk { key: actual,
                    part: ChunkPart::Block(actual_position), inventory: Some(crate::canonical_action::InventoryId::Block { .. }) })
                    if key == actual && position == actual_position))
    }
    pub fn targets(&self) -> Vec<Target> {
        [self.block, self.block_entity()].into_iter().chain(self.bindings.iter().map(|binding| binding.target)).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockInventoryTickIntent {
    pub source: ActionRef,
    pub source_slot: u64,
    pub producer: crate::realm_completion::PeerGeneration,
    pub kind: BlockInventoryTickKind,
    pub root: BlockInventoryTickRoot,
    pub neighbors: Vec<BlockInventoryTickRoot>,
    pub contacts: Vec<Target>,
    pub occupancy: Vec<crate::actor_occupancy::ActorOccupancyWitness>,
    pub loose_items: Vec<Target>,
    pub remainder: Option<crate::entity_body::EntityBody>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockInventoryTickDerivation {
    pub writes: BTreeMap<Target, Option<Value>>,
}

impl BlockInventoryTickIntent {
    pub fn birth_bodies(&self) -> impl Iterator<Item = &crate::entity_body::EntityBody> { self.remainder.iter() }
    pub fn remainder_target(&self) -> Option<Target> {
        self.remainder.as_ref().map(|body| Target::Chunk { key: crate::protocol::ChunkKey::new(body.spatial.world,
            (f64::from_bits(body.spatial.position[0]).floor() as i32).div_euclid(16),
            (f64::from_bits(body.spatial.position[2]).floor() as i32).div_euclid(16)), part: ChunkPart::Entity(body.id), inventory: None })
    }
    pub fn targets(&self) -> Vec<Target> {
        let mut targets = std::iter::once(&self.root).chain(&self.neighbors).flat_map(BlockInventoryTickRoot::targets)
            .chain(self.contacts.iter().copied()).chain(self.loose_items.iter().copied()).chain(self.remainder_target())
            .chain(self.occupancy.iter().map(|witness| Target::Chunk { key: witness.key, part: ChunkPart::ActorOccupancy, inventory: None }))
            .collect::<Vec<_>>(); targets.sort_unstable(); targets.dedup(); targets
    }
    pub fn effect_targets(&self) -> Vec<Target> {
        std::iter::once(&self.root).chain(&self.neighbors).flat_map(BlockInventoryTickRoot::targets)
            .chain(self.loose_items.iter().copied()).chain(self.remainder_target()).collect()
    }
    pub fn valid(&self) -> bool {
        self.root.valid() && self.neighbors.len() <= 6 && self.neighbors.iter().all(BlockInventoryTickRoot::valid)
            && self.contacts.len() <= 6 && self.contacts.iter().all(|target| matches!(target, Target::Chunk { part: ChunkPart::Block(_), inventory: None, .. }))
            && self.loose_items.len() <= 64 && self.loose_items.iter().all(|target| matches!(target, Target::Chunk { part: ChunkPart::Entity(_), inventory: None, .. }))
            && self.source.id.actor == crate::actor::ActorId::Peer(self.producer.server)
            && self.source.id.incarnation == self.producer.generation && self.producer.generation != 0
            && self.source.id.family == 8 && self.source_slot as u16 == self.source.tick.0
            && self.occupancy.len() <= 4 && self.occupancy.iter().all(crate::actor_occupancy::ActorOccupancyWitness::valid)
            && (self.kind == BlockInventoryTickKind::Hopper || self.neighbors.is_empty() && self.loose_items.is_empty() && self.occupancy.is_empty())
            && (self.kind == BlockInventoryTickKind::BrewingStand || self.remainder.is_none())
            && self.remainder.as_ref().is_none_or(|body| body.valid_at(self.source.tick) && body.birth.action == self.source
                && body.birth.source == self.producer && body.owner == self.producer
                && matches!(&body.payload, crate::entity_body::EntityPayload::Item(_)))
    }
    pub fn guards_valid(&self, action: &Action) -> bool {
        self.valid() && action.id == self.source.id && action.tick == self.source.tick
            && self.targets().iter().all(|target| action.conditions.iter().any(|condition| condition.target == *target))
            && self.remainder_target().is_none_or(|target| action.conditions.iter().any(|condition| condition.target == target && condition.expected == Expected::Missing))
            && std::iter::once(&self.root).chain(&self.neighbors).all(|root| action.conditions.iter().any(|condition|
                condition.target == root.block && matches!(&condition.expected, Expected::Exact(Cell { value: Value::Block(state), .. }) if *state == root.state)))
    }
    pub fn derive<S: ActionState>(&self, state: &S, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
        self.derive_writes(state, values)?;
        Ok(vec![Effect::TickBlockInventory { intent: self.clone() }])
    }
    pub fn derive_writes<S: ActionState>(&self, state: &S, values: &BTreeMap<Target, Cell>) -> Result<BlockInventoryTickDerivation, ApplyFailure> {
        if !self.valid() { return Err(ApplyFailure::InvalidAction); }
        let Target::Chunk { key, .. } = self.root.block else { return Err(ApplyFailure::InvalidAction); };
        if state.scheduled_executor(key)? != self.producer { return Err(ApplyFailure::InvalidAction); }
        for root in std::iter::once(&self.root).chain(&self.neighbors) {
            if !matches!(values.get(&root.block), Some(Cell { value: Value::Block(actual), .. }) if *actual == root.state) {
                return Err(ApplyFailure::Condition(root.block));
            }
            state.validate_block_inventory_layout(root.block, root.state, values.get(&root.block_entity()).ok_or(ApplyFailure::Condition(root.block_entity()))?, &root.bindings)?;
        }
        if let Some(target) = self.remainder_target() && values.contains_key(&target) { return Err(ApplyFailure::Condition(target)); }
        for witness in &self.occupancy {
            let target = Target::Chunk { key: witness.key, part: ChunkPart::ActorOccupancy, inventory: None };
            if !matches!(values.get(&target), Some(Cell { revision, value: Value::ActorOccupancy(root) })
                if *revision == witness.revision && root.is_complete() && root.entries.len() == witness.poses.len()
                    && root.entries.iter().zip(&witness.poses).all(|(entry, pose)| entry == &pose.entry)) {
                return Err(ApplyFailure::Condition(target));
            }
        }
        let derived = state.derive_block_inventory_tick(self, values)?;
        let allowed = self.effect_targets().into_iter().collect::<BTreeSet<_>>();
        if derived.writes.keys().any(|target| !allowed.contains(target)) { return Err(ApplyFailure::InvalidAction); }
        for (target, after) in &derived.writes {
            if let Some(binding) = std::iter::once(&self.root).chain(&self.neighbors).flat_map(|root| &root.bindings).find(|binding| binding.target == *target) {
                let Some(Value::Inventory(cell)) = after else { return Err(ApplyFailure::Inventory(*target)); };
                if cell.loc != binding.slot.loc || (cell.item == 0) != (cell.count == 0)
                    || cell.nbt.len() > crate::inventory::INVENTORY_MAX_NBT_BYTES || cell.is_empty() && !cell.nbt.is_empty()
                    || !cell.is_empty() && cell.count > binding.max_stack.min(state.inventory_limit(cell, *target)?) {
                    return Err(ApplyFailure::Inventory(*target));
                }
            } else if self.loose_items.contains(target) {
                let Some(Cell { value: Value::Entity(before), .. }) = values.get(target) else { return Err(ApplyFailure::Condition(*target)); };
                let crate::entity_body::EntityPayload::Item(previous) = &before.payload else { return Err(ApplyFailure::InvalidAction); };
                if let Some(value) = after {
                    let Value::Entity(body) = value else { return Err(ApplyFailure::Type(*target)); };
                    let crate::entity_body::EntityPayload::Item(item) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
                    if item.count == 0 || item.count > previous.count { return Err(ApplyFailure::InvalidAction); }
                    let mut expected = before.clone();
                    let crate::entity_body::EntityPayload::Item(expected_item) = &mut expected.payload else { return Err(ApplyFailure::InvalidAction); };
                    expected_item.count = item.count;
                    if body != &expected { return Err(ApplyFailure::InvalidAction); }
                }
            } else if Some(*target) == self.remainder_target() {
                let Some(Value::Entity(body)) = after else { return Err(ApplyFailure::Type(*target)); };
                let crate::entity_body::EntityPayload::Item(item) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
                let cell = crate::inventory::InventoryCell { loc: crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, 0),
                    item: item.item, count: item.count, nbt: item.components.clone() };
                if state.dropped_item_body(self.remainder.as_ref().ok_or(ApplyFailure::InvalidAction)?, &cell)? != **body {
                    return Err(ApplyFailure::InvalidAction);
                }
            } else if std::iter::once(&self.root).chain(&self.neighbors).any(|root| root.block == *target) {
                if !matches!(after, Some(Value::Block(_))) { return Err(ApplyFailure::Type(*target)); }
            } else if !matches!(after, Some(Value::BlockEntity(_))) { return Err(ApplyFailure::Type(*target)); }
        }
        Ok(derived)
    }
}
