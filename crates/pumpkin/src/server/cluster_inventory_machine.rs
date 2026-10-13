use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::action_command::ActionFootprint;
use pumpkin_cluster::action_identity::{MachineActionId, MachineKind};
use pumpkin_cluster::canonical_action::{ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Effect, InventoryId, Target, Value};
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::inventory::{InvLoc, InventoryCell, InventorySlotRef, InventoryTarget, INV_MAIN};

fn root(identity: MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<(Target, Target, pumpkin_data::BlockStateId, Vec<InventorySlotBinding>, Vec<InventoryCell>), ApplyFailure> {
    let block = Target::Chunk { key: identity.chunk(), part: ChunkPart::Block(identity.position), inventory: None };
    let Some(Value::Block(state)) = values.get(&block).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(block)); };
    let state = pumpkin_data::BlockStateId::new(*state).ok_or(ApplyFailure::Type(block))?;
    if identity.kind != MachineKind::Furnace || !matches!(state.to_block().name, "furnace" | "blast_furnace" | "smoker") {
        return Err(ApplyFailure::Condition(block));
    }
    let entity = Target::Chunk { key: identity.chunk(), part: ChunkPart::BlockEntity(identity.position), inventory: None };
    let mut bindings = Vec::new();
    let mut cells = Vec::new();
    for slot in 0..3 {
        let loc = InvLoc::new(INV_MAIN, slot);
        let target = Target::Chunk { key: identity.chunk(), part: ChunkPart::Block(identity.position), inventory: Some(InventoryId::Block { inv: INV_MAIN, slot }) };
        let Some(Value::Inventory(cell)) = values.get(&target).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(target)); };
        if cell.loc != loc { return Err(ApplyFailure::Inventory(target)); }
        bindings.push(InventorySlotBinding { slot: InventorySlotRef::new(InventoryTarget::Block { chunk: identity.chunk(), position: identity.position }, loc), target, max_stack: 99 });
        cells.push(cell.clone());
    }
    Ok((block, entity, state, bindings, cells))
}

fn transition<S: ActionState>(state: &S, identity: MachineActionId, values: &BTreeMap<Target, Cell>)
    -> Result<(Target, Target, Vec<InventorySlotBinding>, Option<super::cluster_inventory_block_tick::BlockTickImage>), ApplyFailure> {
    let (block, entity, block_state, bindings, cells) = root(identity, values)?;
    let be = values.get(&entity).ok_or(ApplyFailure::Condition(entity))?;
    let Value::BlockEntity(bytes) = &be.value else { return Err(ApplyFailure::Type(entity)); };
    state.validate_block_inventory_layout(block, block_state.as_u16(), be, &bindings)?;
    let after = super::cluster_inventory_block_tick::derive_furnace_event(block_state, bytes, &cells, identity.source_slot)?;
    Ok((block, entity, bindings, after))
}

pub(super) fn footprint<S: ActionState>(server: &super::Server, state: &S, identity: MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<ActionFootprint, ApplyFailure> {
    let (block, entity, bindings, after) = transition(state, identity, values)?;
    let mut reads = BTreeSet::from([block, entity]);
    reads.extend(bindings.iter().map(|binding| binding.target));
    let mut writes = BTreeSet::new();
    if let Some(after) = &after {
        writes.insert(entity);
        if values[&block].value != Value::Block(after.state.as_u16()) { writes.insert(block); }
        for (binding, cell) in bindings.iter().zip(&after.cells) {
            if values[&binding.target].value != Value::Inventory(cell.clone()) { writes.insert(binding.target); }
        }
    }
    let mut counterfactual_reads = reads.clone();
    let mut counterfactual_writes = reads.clone();
    let plans = super::cluster_world_callback_light::location_light_plans(server, pumpkin_cluster::world_command::BlockLocation { world: identity.world, position: identity.position })?;
    for plan in plans {
        let sections = plan.section_targets()?;
        let light = plan.light_targets()?;
        counterfactual_reads.extend(sections.iter().copied());
        counterfactual_reads.extend(light.iter().copied());
        counterfactual_writes.extend(light.iter().copied());
        if writes.contains(&block) { reads.extend(sections); reads.extend(light.iter().copied()); }
        if writes.contains(&block) { writes.extend(light); }
    }
    Ok(ActionFootprint { reads, writes, counterfactual_reads, counterfactual_writes })
}

pub(super) fn derive<S: ActionState>(server: &super::Server, state: &S, identity: MachineActionId, values: &BTreeMap<Target, Cell>) -> Result<Vec<AppliedWrite>, ApplyFailure> {
    let (block, entity, bindings, after) = transition(state, identity, values)?;
    let Some(after) = after else { return Ok(Vec::new()); };
    let mut output = BTreeMap::from([(entity, Value::BlockEntity(after.block_entity))]);
    for (binding, cell) in bindings.iter().zip(after.cells) { output.insert(binding.target, Value::Inventory(cell)); }
    if values[&block].value != Value::Block(after.state.as_u16()) {
        output.insert(block, Value::Block(after.state.as_u16()));
        let plans = super::cluster_world_callback_light::location_light_plans(server, pumpkin_cluster::world_command::BlockLocation { world: identity.world, position: identity.position })?;
        let mut projected = BTreeMap::new();
        for plan in &plans {
            for target in plan.section_targets()?.into_iter().chain(plan.light_targets()?) {
                if let std::collections::btree_map::Entry::Vacant(entry) = projected.entry(target) {
                    entry.insert(values.get(&target).ok_or(ApplyFailure::Condition(target))?.clone());
                }
            }
        }
        projected.insert(block, Cell { revision: values[&block].revision, value: Value::Block(after.state.as_u16()) });
        let section = Target::Chunk { key: identity.chunk(), part: ChunkPart::BlockSection(identity.position.y.div_euclid(16)), inventory: None };
        let Some(Cell { value: Value::BlockSection(blocks), .. }) = projected.get_mut(&section) else { return Err(ApplyFailure::Condition(section)); };
        let position = identity.position;
        let index = ((position.y.rem_euclid(16) * 16 + position.z.rem_euclid(16)) * 16 + position.x.rem_euclid(16)) as usize;
        *blocks.get_mut(index).ok_or(ApplyFailure::Type(section))? = after.state.as_u16();
        for plan in plans {
            for effect in state.derive_world_light(plan, &projected)? {
                let Effect::Put { target, value } = effect else { return Err(ApplyFailure::InvalidAction); };
                output.insert(target, value);
            }
        }
    }
    output.into_iter().filter_map(|(target, value)| {
        let before = match values.get(&target) { Some(before) => before.clone(), None => return Some(Err(ApplyFailure::Condition(target))) };
        (before.value != value).then(|| before.revision.checked_add(1).ok_or(ApplyFailure::Overflow(target)).map(|revision|
            AppliedWrite { target, before: Some(before.clone()), after: Some(Cell { revision, value }) }))
    }).collect()
}
