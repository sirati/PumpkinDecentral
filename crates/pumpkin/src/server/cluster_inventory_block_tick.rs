use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::inventory::{InventoryCell, InvLoc, INV_MAIN};
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;

pub(super) struct BlockTickImage {
    pub state: pumpkin_data::BlockStateId,
    pub block_entity: Vec<u8>,
    pub cells: Vec<InventoryCell>,
}

fn document(bytes: &[u8]) -> Result<pumpkin_nbt::Nbt, ApplyFailure> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let document = pumpkin_nbt::Nbt::read_unnamed(&mut reader).map_err(|_| ApplyFailure::InvalidAction)?;
    if cursor.position() != bytes.len() as u64 || document.clone().write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)? != bytes {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(document)
}

pub(super) fn initialize_genesis_tick_anchor(bytes: &[u8], source_slot: u64) -> Result<Vec<u8>, ApplyFailure> {
    let mut data = document(bytes)?;
    match data.root_tag.get_long("PumpkinInventoryTick") {
        Some(value) if value >= 0 => return Ok(bytes.to_vec()),
        Some(_) => return Err(ApplyFailure::InvalidAction),
        None if data.root_tag.get("PumpkinInventoryTick").is_some() => return Err(ApplyFailure::InvalidAction),
        None => {}
    }
    data.root_tag.put_long("PumpkinInventoryTick", i64::try_from(source_slot).map_err(|_| ApplyFailure::InvalidAction)?);
    Ok(data.write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)?.to_vec())
}

fn stacks(cells: &[InventoryCell], count: usize) -> Result<Vec<ItemStack>, ApplyFailure> {
    if cells.len() != count || cells.iter().enumerate().any(|(slot, cell)| cell.loc != InvLoc::new(INV_MAIN, slot as u16)) {
        return Err(ApplyFailure::InvalidAction);
    }
    cells.iter().map(super::cluster_invsee::item_stack_from_inventory_cell).collect::<Result<Vec<_>, _>>().map_err(|_| ApplyFailure::InvalidAction)
}

fn image(state: pumpkin_data::BlockStateId, document: pumpkin_nbt::Nbt, stacks: &[ItemStack]) -> Result<BlockTickImage, ApplyFailure> {
    Ok(BlockTickImage { state, block_entity: document.write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)?.to_vec(),
        cells: stacks.iter().enumerate().map(|(slot, stack)| super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, slot as u16), stack)).collect::<Result<Vec<_>, _>>().map_err(|_| ApplyFailure::InvalidAction)? })
}

fn elapsed_slots(bytes: &[u8], slot: u64) -> Result<std::ops::RangeInclusive<u64>, ApplyFailure> {
    let previous = document(bytes)?.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    if slot.saturating_sub(previous) > 65_536 { return Err(ApplyFailure::InvalidAction); }
    Ok(previous.saturating_add(1)..=slot)
}

pub(super) fn derive_furnace_tick(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64)
    -> Result<BlockTickImage, ApplyFailure> {
    let mut current = BlockTickImage { state, block_entity: bytes.to_vec(), cells: cells.to_vec() };
    for elapsed in elapsed_slots(bytes, slot)? {
        current = derive_furnace_step(current.state, &current.block_entity, &current.cells, elapsed)?;
    }
    Ok(current)
}

pub(super) fn derive_furnace_event(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64)
    -> Result<Option<BlockTickImage>, ApplyFailure> {
    let after = derive_furnace_tick(state, bytes, cells, slot)?;
    if after.state == state && after.cells == cells
        && document(bytes)?.root_tag.get_compound("RecipesUsed") == document(&after.block_entity)?.root_tag.get_compound("RecipesUsed") {
        return Ok(None);
    }
    Ok(Some(after))
}

pub(super) fn furnace_next_due(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], current_slot: u64)
    -> Result<Option<u64>, ApplyFailure> {
    let before = document(bytes)?;
    let anchor = before.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    if anchor > current_slot { return Err(ApplyFailure::InvalidAction); }
    let first = anchor.checked_add(1).ok_or(ApplyFailure::InvalidAction)?;
    let preview = derive_furnace_step(state, bytes, cells, first)?;
    if preview.state != state || preview.cells != cells { return Ok(Some(first)); }
    let data = document(&preview.block_entity)?;
    let remaining = u64::from(data.root_tag.get_short("lit_time_remaining").unwrap_or(0) as u16);
    if remaining == 0 { return Ok(None); }
    let spent = u64::from(data.root_tag.get_short("cooking_time_spent").unwrap_or(0) as u16);
    let total = u64::from(data.root_tag.get_short("cooking_total_time").unwrap_or(0) as u16);
    let delta = if spent > 0 && total > spent { remaining.min(total - spent) } else { remaining };
    Ok(Some(first.checked_add(delta).ok_or(ApplyFailure::InvalidAction)?))
}

pub(super) enum ProcessingInputCut {
    Ready { block_entity: Vec<u8> },
    Overdue { source_slot: u64 },
}

pub(super) fn furnace_input_cut(state: pumpkin_data::BlockStateId, bytes: &[u8], old_cells: &[InventoryCell], input_slot: u64)
    -> Result<ProcessingInputCut, ApplyFailure> {
    if let Some(source_slot) = furnace_next_due(state, bytes, old_cells, input_slot)? && source_slot < input_slot {
        return Ok(ProcessingInputCut::Overdue { source_slot });
    }
    let anchor = document(bytes)?.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    let cut = derive_furnace_tick(state, bytes, old_cells, input_slot.saturating_sub(1).max(anchor))?;
    if cut.state != state || cut.cells != old_cells { return Err(ApplyFailure::InvalidAction); }
    Ok(ProcessingInputCut::Ready { block_entity: cut.block_entity })
}

fn derive_furnace_step(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64)
    -> Result<BlockTickImage, ApplyFailure> {
    use pumpkin_data::block_properties::{BlockProperties, FurnaceLikeProperties};
    use pumpkin_data::recipes::CookingRecipeKind;
    let kind = match state.to_block().name { "furnace" => CookingRecipeKind::Smelting,
        "blast_furnace" => CookingRecipeKind::Blasting, "smoker" => CookingRecipeKind::Smoking, _ => return Err(ApplyFailure::InvalidAction) };
    let mut document = document(bytes)?;
    if document.root_tag.get_long("PumpkinInventoryTick").is_some_and(|previous| previous >= 0 && previous as u64 >= slot) {
        return image(state, document, &stacks(cells, 3)?);
    }
    let mut stacks = stacks(cells, 3)?;
    let recipe = (!stacks[0].is_empty()).then(|| pumpkin_data::recipes::get_cooking_recipe_with_ingredient(stacks[0].item, kind.clone())).flatten();
    let output = recipe.and_then(|recipe| Item::from_registry_key(recipe.result.id.strip_prefix("minecraft:").unwrap_or(recipe.result.id))
        .map(|item| ItemStack::new(recipe.result.count, item)));
    let can_cook = output.as_ref().is_some_and(|output| stacks[2].is_empty()
        || stacks[2].are_items_and_components_equal(output)
            && stacks[2].item_count.checked_add(output.item_count).is_some_and(|count| count <= output.get_max_stack_size()));
    let read = |name: &str| document.root_tag.get_short(name).map_or(0, |value| value as u16);
    let mut lit = read("lit_time_remaining").saturating_sub(1);
    let mut fuel_total = read("lit_total_time");
    let mut spent = read("cooking_time_spent");
    let mut total = recipe.map_or(read("cooking_total_time"), |recipe| u16::try_from(recipe.cookingtime).unwrap_or(u16::MAX));
    if total == 0 && recipe.is_some() { total = 200; }
    if let Some(recipe) = recipe {
        if document.root_tag.get_string("PumpkinCookingRecipe").is_some_and(|previous| previous != recipe.recipe_id) { spent = 0; }
        document.root_tag.put_string("PumpkinCookingRecipe", recipe.recipe_id.to_owned());
    }
    if lit == 0 && can_cook && !stacks[1].is_empty() {
        let base = pumpkin_data::fuels::get_item_burn_ticks(stacks[1].item.id).unwrap_or(0);
        lit = if matches!(kind, CookingRecipeKind::Blasting | CookingRecipeKind::Smoking) { (base / 2).max(u16::from(base > 0)) } else { base };
        if lit > 0 {
            fuel_total = lit;
            let fuel_item = stacks[1].item.id;
            stacks[1].decrement(1);
            if stacks[1].is_empty() && let Some(item) = pumpkin_data::recipe_remainder::get_recipe_remainder_id(fuel_item).and_then(Item::from_id) {
                stacks[1] = ItemStack::new(1, item);
            }
        }
    }
    if lit > 0 && can_cook {
        spent = spent.saturating_add(1);
        if spent >= total {
            let recipe = recipe.ok_or(ApplyFailure::InvalidAction)?;
            let output = output.ok_or(ApplyFailure::InvalidAction)?;
            if stacks[2].is_empty() { stacks[2] = output; } else { stacks[2].increment(output.item_count); }
            if stacks[0].item.id == Item::WET_SPONGE.id && stacks[1].item.id == Item::BUCKET.id { stacks[1] = ItemStack::new(1, &Item::WATER_BUCKET); }
            stacks[0].decrement(1); spent = 0;
            let mut used = document.root_tag.get_compound("RecipesUsed").cloned().unwrap_or_default();
            let count = used.get_int(recipe.recipe_id).unwrap_or(0).checked_add(1).ok_or(ApplyFailure::InvalidAction)?;
            used.put_int(recipe.recipe_id, count);
            document.root_tag.put("RecipesUsed", pumpkin_nbt::tag::NbtTag::Compound(used));
        }
    } else if lit > 0 { spent = 0; }
    else { spent = spent.saturating_sub(2).min(total); }
    document.root_tag.put_short("lit_time_remaining", lit as i16);
    document.root_tag.put_short("lit_total_time", fuel_total as i16);
    document.root_tag.put_short("cooking_time_spent", spent as i16);
    document.root_tag.put_short("cooking_total_time", total as i16);
    document.root_tag.put_long("PumpkinInventoryTick", i64::try_from(slot).map_err(|_| ApplyFailure::InvalidAction)?);
    let mut properties = FurnaceLikeProperties::from_state_id(state); properties.lit = lit > 0;
    image(properties.to_state_id(state.to_block()), document, &stacks)
}

pub(super) struct BrewingTickImage {
    pub image: BlockTickImage,
    pub remainder: Option<InventoryCell>,
}

fn non_hopper_tick_changes(kind: pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickKind,
    state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64) -> Result<bool, ApplyFailure> {
    let before = document(bytes)?;
    Ok(if kind == pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickKind::BrewingStand {
        let after = derive_brewing_tick(state, bytes, cells, slot)?;
        let data = document(&after.image.block_entity)?;
        after.image.state != state || after.image.cells != cells || after.remainder.is_some()
            || before.root_tag.get_byte("Fuel").unwrap_or(0) != data.root_tag.get_byte("Fuel").unwrap_or(0)
    } else {
        let after = derive_furnace_tick(state, bytes, cells, slot)?;
        let data = document(&after.block_entity)?;
        after.state != state || after.cells != cells || before.root_tag.get_compound("RecipesUsed") != data.root_tag.get_compound("RecipesUsed")
    })
}

pub(super) fn derive_brewing_tick(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64)
    -> Result<BrewingTickImage, ApplyFailure> {
    let mut current = BrewingTickImage { image: BlockTickImage { state, block_entity: bytes.to_vec(), cells: cells.to_vec() }, remainder: None };
    for elapsed in elapsed_slots(bytes, slot)? {
        let next = derive_brewing_step(current.image.state, &current.image.block_entity, &current.image.cells, elapsed)?;
        if current.remainder.is_some() && next.remainder.is_some() { return Err(ApplyFailure::InvalidAction); }
        let remainder = next.remainder.or(current.remainder);
        current = BrewingTickImage { image: next.image, remainder };
    }
    Ok(current)
}

pub(super) fn brewing_next_due(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], current_slot: u64)
    -> Result<Option<u64>, ApplyFailure> {
    let before = document(bytes)?;
    let anchor = before.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    if anchor > current_slot { return Err(ApplyFailure::InvalidAction); }
    let first = anchor.checked_add(1).ok_or(ApplyFailure::InvalidAction)?;
    let preview = derive_brewing_step(state, bytes, cells, first)?;
    let data = document(&preview.image.block_entity)?;
    if preview.image.state != state || preview.image.cells != cells || preview.remainder.is_some()
        || before.root_tag.get_byte("Fuel").unwrap_or(0) != data.root_tag.get_byte("Fuel").unwrap_or(0) {
        return Ok(Some(first));
    }
    let time = u64::from(data.root_tag.get_short("BrewTime").unwrap_or(0) as u16);
    if time > 0 { return Ok(Some(first.checked_add(time).ok_or(ApplyFailure::InvalidAction)?)); }
    let second = first.checked_add(1).ok_or(ApplyFailure::InvalidAction)?;
    let restarted = derive_brewing_step(preview.image.state, &preview.image.block_entity, &preview.image.cells, second)?;
    let restarted_data = document(&restarted.image.block_entity)?;
    Ok((restarted.image.state != preview.image.state || restarted.image.cells != preview.image.cells || restarted.remainder.is_some()
        || data.root_tag.get_byte("Fuel").unwrap_or(0) != restarted_data.root_tag.get_byte("Fuel").unwrap_or(0)).then_some(second))
}

pub(super) fn brewing_input_cut(state: pumpkin_data::BlockStateId, bytes: &[u8], old_cells: &[InventoryCell], input_slot: u64)
    -> Result<ProcessingInputCut, ApplyFailure> {
    if let Some(source_slot) = brewing_next_due(state, bytes, old_cells, input_slot)? && source_slot < input_slot {
        return Ok(ProcessingInputCut::Overdue { source_slot });
    }
    let before = document(bytes)?;
    let anchor = before.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    let cut = derive_brewing_tick(state, bytes, old_cells, input_slot.saturating_sub(1).max(anchor))?;
    if cut.image.state != state || cut.image.cells != old_cells || cut.remainder.is_some()
        || document(&cut.image.block_entity)?.root_tag.get_byte("Fuel").unwrap_or(0) != before.root_tag.get_byte("Fuel").unwrap_or(0) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(ProcessingInputCut::Ready { block_entity: cut.image.block_entity })
}

fn derive_brewing_step(state: pumpkin_data::BlockStateId, bytes: &[u8], cells: &[InventoryCell], slot: u64)
    -> Result<BrewingTickImage, ApplyFailure> {
    use pumpkin_data::data_component_impl::PotionContentsImpl;
    if state.to_block().name != "brewing_stand" { return Err(ApplyFailure::InvalidAction); }
    let mut document = document(bytes)?;
    let mut stacks = stacks(cells, 5)?;
    if document.root_tag.get_long("PumpkinInventoryTick").is_some_and(|previous| previous >= 0 && previous as u64 >= slot) {
        return Ok(BrewingTickImage { image: image(state, document, &stacks)?, remainder: None });
    }
    let mut fuel = i32::from(document.root_tag.get_byte("Fuel").unwrap_or(0)).clamp(0, 20);
    let mut time = i32::from(document.root_tag.get_short("BrewTime").unwrap_or(0)).clamp(0, 400);
    if fuel == 0 && !stacks[4].is_empty() && stacks[4].item.id == Item::BLAZE_POWDER.id { stacks[4].decrement(1); fuel = 20; }
    let ingredient = &stacks[3];
    let transformed = stacks.iter().take(3).map(|stack| {
        if stack.is_empty() || ingredient.is_empty() { return None; }
        let potion = stack.get_data_component::<PotionContentsImpl>()?.potion_id?;
        let recipe = pumpkin_data::potion_brewing::BREWING_RECIPES.iter().find(|recipe|
            recipe.from_item().id == stack.item.id && recipe.ingredient().id == ingredient.item.id
                && i32::from(recipe.from_potion().id) == potion)?;
        let mut contents = stack.get_data_component::<PotionContentsImpl>()?.clone();
        contents.potion_id = Some(i32::from(recipe.to_potion().id));
        let mut result = stack.clone(); result.item = recipe.to_item(); result.item_count = 1; result.set_data_component(contents);
        Some(result)
    }).collect::<Vec<_>>();
    let can_brew = transformed.iter().any(Option::is_some);
    let signature = if ingredient.is_empty() { String::new() } else {
        format!("{:016x}", xxhash_rust::xxh64::xxh64(&cells[3].nbt, u64::from(ingredient.item.id)))
    };
    let same_ingredient = document.root_tag.get_string("PumpkinBrewingIngredient").is_none_or(|previous| previous == signature);
    let mut remainder = None;
    if time > 0 {
        time -= 1;
        if !can_brew || !same_ingredient { time = 0; }
        else if time == 0 {
            for (index, result) in transformed.into_iter().enumerate() { if let Some(result) = result { stacks[index] = result; } }
            let item = stacks[3].item.id;
            stacks[3].decrement(1);
            if let Some(item) = pumpkin_data::recipe_remainder::get_recipe_remainder_id(item).and_then(Item::from_id) {
                let stack = ItemStack::new(1, item);
                if stacks[3].is_empty() { stacks[3] = stack; }
                else { remainder = Some(super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), &stack).map_err(|_| ApplyFailure::InvalidAction)?); }
            }
        }
    } else if can_brew && fuel > 0 { fuel -= 1; time = 400; }
    document.root_tag.put_byte("Fuel", fuel as i8);
    document.root_tag.put_short("BrewTime", time as i16);
    document.root_tag.put_string("PumpkinBrewingIngredient", signature);
    document.root_tag.put_long("PumpkinInventoryTick", i64::try_from(slot).map_err(|_| ApplyFailure::InvalidAction)?);
    use pumpkin_data::block_properties::{BlockProperties, BrewingStandLikeProperties};
    let mut properties = BrewingStandLikeProperties::from_state_id(state);
    properties.has_bottle_0 = !stacks[0].is_empty(); properties.has_bottle_1 = !stacks[1].is_empty(); properties.has_bottle_2 = !stacks[2].is_empty();
    Ok(BrewingTickImage { image: image(properties.to_state_id(state.to_block()), document, &stacks)?, remainder })
}

#[derive(Clone)]
pub(super) struct FrozenBlockEntityTick {
    snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    intent: pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
}

impl FrozenBlockEntityTick {
    pub(super) fn targets(&self) -> Vec<pumpkin_cluster::canonical_action::Target> { self.intent.targets() }
    pub(super) fn chunks(&self) -> Vec<pumpkin_cluster::protocol::ChunkKey> {
        self.targets().into_iter().filter_map(|target| match target {
            pumpkin_cluster::canonical_action::Target::Chunk { key, .. } => Some(key), _ => None,
        }).collect::<std::collections::BTreeSet<_>>().into_iter().collect()
    }
    pub(super) fn birth_count(&self) -> usize { usize::from(self.intent.kind == pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickKind::BrewingStand) }
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&self.intent)
    }
    pub(super) fn bind(&self, reference: pumpkin_cluster::canonical_action::ActionRef,
        producer: pumpkin_cluster::realm_completion::PeerGeneration,
        identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity]) -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
        self.bind_with_view(&*self.snapshot, reference, producer, identities)
    }
    pub(super) fn bind_with_view<S: pumpkin_cluster::canonical_action::ActionState>(&self, view: &S,
        reference: pumpkin_cluster::canonical_action::ActionRef, producer: pumpkin_cluster::realm_completion::PeerGeneration,
        identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity]) -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{Action, ActionClass, ActionKind, ActionState, Cell, ChunkPart, Condition, Expected, SemanticIntent, Stakeholder, Target, TruthView, UndoProcedure, Value};
        if identities.len() != self.birth_count() || reference.tick != self.intent.source.tick { return Err(ApplyFailure::InvalidAction); }
        let mut intent = self.intent.clone(); intent.source = reference; intent.producer = producer;
        if let Some(identity) = identities.first() {
            let Target::Chunk { key, part: ChunkPart::Block(position), .. } = intent.root.block else { return Err(ApplyFailure::InvalidAction); };
            let spatial = pumpkin_cluster::entity_body::EntitySpatial { world: key.world,
                position: [(f64::from(position.x) + 0.5).to_bits(), (f64::from(position.y) + 1.0).to_bits(), (f64::from(position.z) + 0.5).to_bits()],
                velocity_micro: [0; 3], yaw: 0_f32.to_bits(), pitch: 0_f32.to_bits(), on_ground: false };
            let placeholder = super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), &ItemStack::new(1, &Item::GLASS_BOTTLE))
                .map_err(|_| ApplyFailure::InvalidAction)?;
            intent.remainder = Some(super::cluster_item_birth::reserved_item_body(reference, identity, producer, spatial, &placeholder, 40)
                .map_err(|_| ApplyFailure::InvalidAction)?);
        }
        bind_tick_action(view, intent)
    }
}

fn bind_tick_action<S: pumpkin_cluster::canonical_action::ActionState>(view: &S,
    intent: pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent)
    -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{Action, ActionClass, ActionKind, Condition, Expected, SemanticIntent, Stakeholder, Target, TruthView, UndoProcedure};
        let reference = intent.source;
        let targets = intent.targets();
        let values = targets.iter().filter(|target| Some(**target) != intent.remainder_target()).map(|target|
            view.read(TruthView::Local, *target).map(|cell| (*target, cell)).ok_or(ApplyFailure::Condition(*target)))
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let effects = intent.derive(view, &values)?;
        let conditions = targets.iter().map(|target| Condition { target: *target, expected: if Some(*target) == intent.remainder_target() {
            Expected::Missing } else { Expected::Exact(values[target].clone()) } }).collect();
        let action = Action { id: reference.id, tick: reference.tick, class: ActionClass::Replayable, chunk_transition: false,
            actor_chunk: match intent.root.block { Target::Chunk { key, .. } => Some(key), _ => None }, player_leases: Vec::new(), player_chunks: Vec::new(),
            stakeholders: targets.iter().map(|target| target.stakeholder()).collect::<std::collections::BTreeSet<Stakeholder>>().into_iter().collect(),
            dependencies: Vec::new(), conditions, effects, counterfactual_conditions: targets, counterfactual_effects: intent.effect_targets(),
            semantic_intent: Some(SemanticIntent::World(pumpkin_cluster::world_action::WorldReplayIntent {
                steps: vec![pumpkin_cluster::world_action::WorldReplayStep::BlockInventoryTick(intent)] })), factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets], kind: ActionKind::World, presentation: Vec::new(),
            client_origin: pumpkin_cluster::canonical_action::ClientOrigin::None, edit_authority: None };
        action.validate()?; Ok(action)
}

pub(super) fn capture_block_inventory_root<S: pumpkin_cluster::canonical_action::ActionState>(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot, view: &S,
    chunk: pumpkin_cluster::protocol::ChunkKey, position: pumpkin_cluster::protocol::BlockPos)
    -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{Cell, ChunkPart, Target, TruthView, Value};
    let block = Target::Chunk { key: chunk, part: ChunkPart::Block(position), inventory: None };
    let Some(Cell { value: Value::Block(state), .. }) = view.read(TruthView::Local, block) else { return Err(ApplyFailure::Condition(block)); };
    let entity_target = Target::Chunk { key: chunk, part: ChunkPart::BlockEntity(position), inventory: None };
    let entity = view.read(TruthView::Local, entity_target).ok_or(ApplyFailure::Condition(entity_target))?;
    let descriptor = super::cluster_inventory_block_roots::CanonicalBlockInventoryRoot::from_image(chunk,
        super::cluster_inventory_block_roots::BlockInventoryRootImage { position, state, block_entity: entity.clone() })
        .map_err(|_| ApplyFailure::Inventory(block))?.ok_or(ApplyFailure::Inventory(block))?;
    if descriptor.deferred_loot { return Err(ApplyFailure::Inventory(block)); }
    let bindings = descriptor.bindings();
    for binding in &bindings {
        if snapshot.captured_inventory_slot(binding.target) != Some((binding.slot.target, binding.slot.loc)) {
            return Err(ApplyFailure::Inventory(binding.target));
        }
    }
    let root = pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot { block, state, bindings };
    view.validate_block_inventory_layout(root.block, state, &entity, &root.bindings)?;
    Ok(root)
}

pub(super) fn capture_block_entity_tick(snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    chunk: pumpkin_cluster::protocol::ChunkKey, position: pumpkin_cluster::protocol::BlockPos,
    tick: pumpkin_cluster::time::TickStamp, source_slot: u64) -> Result<Option<FrozenBlockEntityTick>, ApplyFailure> {
    capture_block_entity_tick_with_view(snapshot.clone(), &*snapshot, chunk, position, tick, source_slot)
}

pub(super) fn capture_block_entity_tick_with_view<S: pumpkin_cluster::canonical_action::ActionState>(
    snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>, view: &S,
    chunk: pumpkin_cluster::protocol::ChunkKey, position: pumpkin_cluster::protocol::BlockPos,
    tick: pumpkin_cluster::time::TickStamp, source_slot: u64) -> Result<Option<FrozenBlockEntityTick>, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionId, ActionRef, ActionState, Cell, ChunkPart, Target, TruthView, Value};
    use pumpkin_cluster::canonical_block_inventory_tick::{BlockInventoryTickIntent, BlockInventoryTickKind as K, BlockInventoryTickRoot};
    use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
    use pumpkin_cluster::inventory::{InventorySlotRef, InventoryTarget};
    if snapshot.original_tick() != tick || snapshot.original_source_slot().is_some_and(|slot| slot != source_slot) || source_slot as u16 != tick.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let block = Target::Chunk { key: chunk, part: ChunkPart::Block(position), inventory: None };
    let Some(Cell { value: Value::Block(state), .. }) = view.read(TruthView::Local, block) else { return Err(ApplyFailure::Condition(block)); };
    let state_id = pumpkin_data::BlockStateId::new(state).ok_or(ApplyFailure::InvalidAction)?;
    let kind = match state_id.to_block().name { "hopper" => K::Hopper, "furnace" => K::Furnace,
        "blast_furnace" => K::BlastFurnace, "smoker" => K::Smoker, "brewing_stand" => K::BrewingStand, _ => return Ok(None) };
    let root_for = |key, position, state| -> Result<BlockInventoryTickRoot, ApplyFailure> {
        let root = capture_block_inventory_root(&snapshot, view, key, position)?;
        if root.state != state { return Err(ApplyFailure::Condition(root.block)); }
        Ok(root)
    };
    let root = root_for(chunk, position, state)?;
    if kind != K::Hopper {
        let original = root.targets().into_iter().map(|target| view.read(TruthView::Local, target)
            .map(|cell| (target, cell)).ok_or(ApplyFailure::Condition(target))).collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let cells = root_cells(&root, &original)?;
        let bytes = root_bytes(&root, &original)?;
        let changed = non_hopper_tick_changes(kind, state_id, bytes, &cells, source_slot)?;
        if !changed { return Ok(None); }
    }
    let mut neighbors = Vec::new(); let mut contacts = Vec::new(); let mut occupancy = Vec::new(); let mut loose_items = Vec::new();
    if kind == K::Hopper {
        use pumpkin_data::block_properties::{BlockProperties, FacingHopper, HopperLikeProperties};
        let properties = HopperLikeProperties::from_state_id(state_id);
        let offset = match properties.facing { FacingHopper::Down => [0,-1,0], FacingHopper::North => [0,0,-1], FacingHopper::South => [0,0,1],
            FacingHopper::West => [-1,0,0], FacingHopper::East => [1,0,0] };
        for offset in [offset, [0,1,0]] {
            let adjacent = pumpkin_cluster::protocol::BlockPos { x: position.x + offset[0], y: position.y + offset[1], z: position.z + offset[2] };
            let key = pumpkin_cluster::protocol::ChunkKey::new(chunk.world, adjacent.x.div_euclid(16), adjacent.z.div_euclid(16));
            let target = Target::Chunk { key, part: ChunkPart::Block(adjacent), inventory: None };
            let Some(Cell { value: Value::Block(state), .. }) = view.read(TruthView::Local, target) else { return Err(ApplyFailure::Condition(target)); };
            contacts.push(target);
            let state_id = pumpkin_data::BlockStateId::new(state).ok_or(ApplyFailure::InvalidAction)?;
            if super::cluster_inventory_menu::block_inventory_policy(state_id).is_some() {
                neighbors.push(root_for(key, adjacent, state)?);
            }
        }
        let bounds = [[f64::from(position.x), f64::from(position.y) + 0.6875, f64::from(position.z)],
            [f64::from(position.x) + 1.0, f64::from(position.y) + 2.0, f64::from(position.z) + 1.0]];
        let collision = pumpkin_cluster::actor_occupancy::CollisionBox { bounds: bounds.map(|corner| corner.map(f64::to_bits)) };
        for x in ((bounds[0][0] - 0.125).floor() as i32).div_euclid(16)..=((bounds[1][0] + 0.125).floor() as i32).div_euclid(16) {
            for z in ((bounds[0][2] - 0.125).floor() as i32).div_euclid(16)..=((bounds[1][2] + 0.125).floor() as i32).div_euclid(16) {
                let mut witness = snapshot.capture_actor_occupancy(pumpkin_cluster::protocol::ChunkKey::new(chunk.world, x, z), vec![collision.clone()])?;
                let target = Target::Chunk { key: witness.key, part: ChunkPart::ActorOccupancy, inventory: None };
                let Some(Cell { revision, value: Value::ActorOccupancy(root) }) = view.read(TruthView::Local, target) else { return Err(ApplyFailure::Condition(target)); };
                if !root.is_complete() || root.entries.iter().any(|entry| !witness.poses.iter().any(|pose| &pose.entry == entry)) { return Err(ApplyFailure::Condition(target)); }
                witness.poses.retain(|pose| root.entries.iter().any(|entry| entry == &pose.entry)); witness.revision = revision;
                occupancy.push(witness);
            }
        }
        for (key, body) in snapshot.captured_entity_bodies() {
            let target = Target::Chunk { key, part: ChunkPart::Entity(body.id), inventory: None };
            let Some(Cell { value: Value::Entity(body), .. }) = view.read(TruthView::Local, target) else { continue; };
            if !matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Item(_)) { continue; }
            let Some(pose) = occupancy.iter().flat_map(|witness| &witness.poses).find(|pose| pose.entry.actor == pumpkin_cluster::actor::ActorId::Entity(body.id)) else { continue; };
            if (0..3).all(|axis| f64::from_bits(pose.position[axis]) + f64::from_bits(pose.entry.shape.offsets[1][axis]) > bounds[0][axis]
                && f64::from_bits(pose.position[axis]) + f64::from_bits(pose.entry.shape.offsets[0][axis]) < bounds[1][axis]) {
                loose_items.push(Target::Chunk { key, part: ChunkPart::Entity(body.id), inventory: None });
            }
        }
        loose_items.sort_unstable();
        if loose_items.len() > 64 { return Err(ApplyFailure::ResourceLimit); }
    }
    let producer = view.scheduled_executor(chunk)?;
    let intent = BlockInventoryTickIntent { source: ActionRef { tick, id: ActionId { actor: pumpkin_cluster::actor::ActorId::Peer(producer.server),
        incarnation: producer.generation, family: 8, sequence: 0 } }, source_slot, producer, kind, root, neighbors, contacts, occupancy, loose_items, remainder: None };
    if kind == K::Hopper {
        let values = intent.targets().into_iter().filter_map(|target| view.read(TruthView::Local, target).map(|cell| (target, cell))).collect();
        if derive_hopper_tick(view, &intent, &values)?.writes.is_empty() { return Ok(None); }
    }
    Ok(Some(FrozenBlockEntityTick { snapshot, intent }))
}

fn root_cells(root: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot,
    values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
    -> Result<Vec<InventoryCell>, ApplyFailure> {
    use pumpkin_cluster::canonical_action::Value;
    (0..root.bindings.len()).map(|index| {
        let binding = root.bindings.iter().find(|binding| binding.slot.loc == InvLoc::new(INV_MAIN, index as u16)).ok_or(ApplyFailure::InvalidAction)?;
        match values.get(&binding.target).map(|cell| &cell.value) {
            Some(Value::Inventory(cell)) if cell.loc == binding.slot.loc => Ok(cell.clone()), _ => Err(ApplyFailure::Inventory(binding.target)),
        }
    }).collect()
}

fn root_bytes<'a>(root: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot,
    values: &'a std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>) -> Result<&'a [u8], ApplyFailure> {
    match values.get(&root.block_entity()).map(|cell| &cell.value) {
        Some(pumpkin_cluster::canonical_action::Value::BlockEntity(bytes)) => Ok(bytes), _ => Err(ApplyFailure::Condition(root.block_entity())),
    }
}

fn insert_cell(destination: &mut [InventoryCell], source: &InventoryCell, slots: &[(u16, u8)], count: u8) -> Result<u8, ApplyFailure> {
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source).map_err(|_| ApplyFailure::InvalidAction)?;
    let mut remaining = count;
    for empty in [false, true] {
        for (slot, limit) in slots {
            let cell = destination.get_mut(usize::from(*slot)).ok_or(ApplyFailure::InvalidAction)?;
            if cell.is_empty() != empty || !cell.is_empty() && (cell.item != source.item || cell.nbt != source.nbt) { continue; }
            let amount = remaining.min(stack.get_max_stack_size().min(*limit).saturating_sub(cell.count));
            if amount == 0 { continue; }
            let mut after = source.clone(); after.loc = cell.loc; after.count = cell.count + amount; *cell = after;
            remaining -= amount; if remaining == 0 { return Ok(count); }
        }
    }
    Ok(count - remaining)
}

fn insertion_slots(state: pumpkin_data::BlockStateId, bytes: &[u8], item: &InventoryCell, from_above: bool, size: usize)
    -> Result<Vec<(u16, u8)>, ApplyFailure> {
    use pumpkin_data::tag::Taggable;
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(item).map_err(|_| ApplyFailure::InvalidAction)?;
    let name = state.to_block().name;
    let slots: Vec<u16> = match name {
        "beacon" => Vec::new(),
        "furnace" | "blast_furnace" | "smoker" => if from_above { vec![0] }
            else if pumpkin_data::fuels::is_fuel(item.item) || item.item == Item::BUCKET.id { vec![1] } else { Vec::new() },
        "brewing_stand" => if from_above {
            if pumpkin_data::potion_brewing::BREWING_RECIPES.iter().any(|recipe| recipe.ingredient().id == item.item) { vec![3] } else { Vec::new() }
        } else if item.item == Item::BLAZE_POWDER.id { vec![4] }
        else if [Item::POTION.id, Item::SPLASH_POTION.id, Item::LINGERING_POTION.id, Item::GLASS_BOTTLE.id].contains(&item.item) { vec![0,1,2] } else { Vec::new() },
        "crafter" => {
            let document = document(bytes)?;
            let disabled = document.root_tag.get_int_array("disabled_slots").unwrap_or(&[]);
            (0..size as u16).filter(|slot| !disabled.contains(&i32::from(*slot))).collect()
        }
        "chiseled_bookshelf" => if stack.item.has_tag(&pumpkin_data::tag::Item::MINECRAFT_BOOKSHELF_BOOKS) { (0..size as u16).collect() } else { Vec::new() },
        "jukebox" => if stack.item.has_tag(&pumpkin_data::tag::Item::C_MUSIC_DISCS) { vec![0] } else { Vec::new() },
        name if name.ends_with("shulker_box") && stack.item.registry_key.ends_with("shulker_box") => Vec::new(),
        _ => (0..size as u16).collect(),
    };
    Ok(slots.into_iter().map(|slot| (slot, if name == "brewing_stand" && slot < 3
        || matches!(name, "chiseled_bookshelf" | "jukebox") { 1 } else { 99 })).collect())
}

pub(super) struct HopperTransition<'a> {
    pub source_slot: u64,
    pub root: &'a pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot,
    pub neighbors: &'a [pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot],
    pub contacts: &'a [pumpkin_cluster::canonical_action::Target],
    pub loose_items: &'a [pumpkin_cluster::canonical_action::Target],
}

fn derive_hopper_tick<S: pumpkin_cluster::canonical_action::ActionState>(state: &S,
    intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
    values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
    -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
    derive_hopper_transition(state, HopperTransition { source_slot: intent.source_slot, root: &intent.root,
        neighbors: &intent.neighbors, contacts: &intent.contacts, loose_items: &intent.loose_items }, values)
}

pub(super) fn derive_hopper_transition<S: pumpkin_cluster::canonical_action::ActionState>(state: &S,
    intent: HopperTransition<'_>,
    values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
    -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
    use pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation;
    use pumpkin_data::block_properties::{BlockProperties, FacingHopper, HopperLikeProperties};
    let Target::Chunk { key, part: ChunkPart::Block(position), .. } = intent.root.block else { return Err(ApplyFailure::InvalidAction); };
    let block_state = pumpkin_data::BlockStateId::new(intent.root.state).ok_or(ApplyFailure::InvalidAction)?;
    if block_state.to_block().name != "hopper" { return Err(ApplyFailure::InvalidAction); }
    let properties = HopperLikeProperties::from_state_id(block_state);
    let mut root_document = document(root_bytes(&intent.root, values)?)?;
    let mut writes = std::collections::BTreeMap::new();
    if root_document.root_tag.get_long("PumpkinInventoryTick").is_some_and(|previous| previous >= 0 && previous as u64 >= intent.source_slot) {
        return Ok(BlockInventoryTickDerivation { writes });
    }
    let mut own = root_cells(&intent.root, values)?;
    let mut neighbors = intent.neighbors.iter().map(|root| root_cells(root, values)).collect::<Result<Vec<_>, _>>()?;
    let previous = root_document.root_tag.get_long("PumpkinInventoryTick").filter(|value| *value >= 0)
        .map(|value| value as u64).ok_or(ApplyFailure::InvalidAction)?;
    let elapsed = i32::try_from(intent.source_slot.saturating_sub(previous)).unwrap_or(i32::MAX);
    let mut cooldown = root_document.root_tag.get_int("TransferCooldown").unwrap_or(0).saturating_sub(elapsed).max(0);
    let offset = match properties.facing { FacingHopper::Down => [0,-1,0], FacingHopper::North => [0,0,-1], FacingHopper::South => [0,0,1],
        FacingHopper::West => [-1,0,0], FacingHopper::East => [1,0,0] };
    let output_position = pumpkin_cluster::protocol::BlockPos { x: position.x + offset[0], y: position.y + offset[1], z: position.z + offset[2] };
    let above_position = pumpkin_cluster::protocol::BlockPos { x: position.x, y: position.y + 1, z: position.z };
    let mut moved = false;
    if cooldown == 0 && properties.enabled {
        if let Some(index) = intent.neighbors.iter().position(|root| matches!(root.block, Target::Chunk { part: ChunkPart::Block(actual), .. } if actual == output_position)) {
            let root = &intent.neighbors[index];
            let destination_state = pumpkin_data::BlockStateId::new(root.state).ok_or(ApplyFailure::InvalidAction)?;
            let was_empty = neighbors[index].iter().all(InventoryCell::is_empty);
            for input in &mut own {
                if input.is_empty() { continue; }
                let slots = insertion_slots(destination_state, root_bytes(root, values)?, input, offset[1] == -1, neighbors[index].len())?;
                let count = insert_cell(&mut neighbors[index], input, &slots, 1)?;
                if count == 0 { continue; }
                input.count -= count; if input.count == 0 { *input = InventoryCell::empty(input.loc); }
                moved = true;
                if was_empty && destination_state.to_block().name == "hopper" {
                    let mut destination = document(root_bytes(root, values)?)?;
                    let previous = destination.root_tag.get_long("PumpkinInventoryTick").unwrap_or(-1);
                    let existing = destination.root_tag.get_int("TransferCooldown").unwrap_or(0);
                    destination.root_tag.put_int("TransferCooldown", existing.max(if previous >= 0 && previous as u64 >= intent.source_slot { 7 } else { 8 }));
                    destination.root_tag.put_long("PumpkinInventoryTick", i64::try_from(intent.source_slot).map_err(|_| ApplyFailure::InvalidAction)?);
                    writes.insert(root.block_entity(), Some(Value::BlockEntity(destination.write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)?.to_vec())));
                }
                break;
            }
        }
        if let Some(index) = intent.neighbors.iter().position(|root| matches!(root.block, Target::Chunk { part: ChunkPart::Block(actual), .. } if actual == above_position)) {
            let root = &intent.neighbors[index];
            let above_state = pumpkin_data::BlockStateId::new(root.state).ok_or(ApplyFailure::InvalidAction)?;
            let policy = super::cluster_inventory_menu::block_inventory_policy(above_state).ok_or(ApplyFailure::InvalidAction)?;
            for slot in &policy.bottom_slots {
                let input = neighbors[index].get_mut(usize::from(*slot)).ok_or(ApplyFailure::InvalidAction)?;
                if input.is_empty() || !policy.can_extract_bottom(*slot, input) { continue; }
                let count = insert_cell(&mut own, input, &[(0,99),(1,99),(2,99),(3,99),(4,99)], 1)?;
                if count == 0 { continue; }
                input.count -= count; if input.count == 0 { *input = InventoryCell::empty(input.loc); }
                moved = true; break;
            }
        } else {
            let above_target = intent.contacts.iter().find(|target| matches!(target, Target::Chunk { part: ChunkPart::Block(actual), .. } if *actual == above_position)).ok_or(ApplyFailure::InvalidAction)?;
            let Some(Value::Block(above_state)) = values.get(above_target).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(*above_target)); };
            if super::cluster_inventory_menu::block_inventory_policy(pumpkin_data::BlockStateId::new(*above_state).ok_or(ApplyFailure::InvalidAction)?).is_none() {
                for target in intent.loose_items {
                    let Some(Value::Entity(body)) = values.get(target).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(*target)); };
                    let pumpkin_cluster::entity_body::EntityPayload::Item(item) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
                    let cell = InventoryCell { loc: InvLoc::new(INV_MAIN, 0), item: item.item, count: item.count, nbt: item.components.clone() };
                    let count = insert_cell(&mut own, &cell, &[(0,99),(1,99),(2,99),(3,99),(4,99)], item.count)?;
                    if count == 0 { continue; }
                    if count == item.count { writes.insert(*target, None); }
                    else { let mut after = body.clone(); let pumpkin_cluster::entity_body::EntityPayload::Item(item) = &mut after.payload else { return Err(ApplyFailure::InvalidAction); }; item.count -= count; writes.insert(*target, Some(Value::Entity((after).into()))); }
                    moved = true; break;
                }
            }
        }
        if moved { cooldown = 8; }
    }
    if !moved { return Ok(BlockInventoryTickDerivation { writes }); }
    root_document.root_tag.put_int("TransferCooldown", cooldown);
    root_document.root_tag.put_long("PumpkinInventoryTick", i64::try_from(intent.source_slot).map_err(|_| ApplyFailure::InvalidAction)?);
    writes.insert(intent.root.block_entity(), Some(Value::BlockEntity(root_document.write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)?.to_vec())));
    for (root, cells) in std::iter::once((intent.root, own)).chain(intent.neighbors.iter().zip(neighbors)) {
        for (index, cell) in cells.into_iter().enumerate() {
            let binding = root.bindings.iter().find(|binding| binding.slot.loc.slot == index as u16).ok_or(ApplyFailure::InvalidAction)?;
            if values.get(&binding.target).map(|before| &before.value) != Some(&Value::Inventory(cell.clone())) { writes.insert(binding.target, Some(Value::Inventory(cell))); }
        }
    }
    let _ = (state, key);
    Ok(BlockInventoryTickDerivation { writes })
}

pub(super) fn derive<S: pumpkin_cluster::canonical_action::ActionState>(state: &S,
    intent: &pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickIntent,
    values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
    -> Result<pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickDerivation, ApplyFailure> {
    use pumpkin_cluster::canonical_action::Value;
    use pumpkin_cluster::canonical_block_inventory_tick::{BlockInventoryTickDerivation, BlockInventoryTickKind as K};
    if intent.kind == K::Hopper { return derive_hopper_tick(state, intent, values); }
    let cells = root_cells(&intent.root, values)?;
    let bytes = root_bytes(&intent.root, values)?;
    let block_state = pumpkin_data::BlockStateId::new(intent.root.state).ok_or(ApplyFailure::InvalidAction)?;
    let (image, remainder) = if intent.kind == K::BrewingStand {
        let result = derive_brewing_tick(block_state, bytes, &cells, intent.source_slot)?; (result.image, result.remainder)
    } else { (derive_furnace_tick(block_state, bytes, &cells, intent.source_slot)?, None) };
    let mut writes = std::collections::BTreeMap::new();
    writes.insert(intent.root.block, Some(Value::Block(image.state.as_u16())));
    writes.insert(intent.root.block_entity(), Some(Value::BlockEntity(image.block_entity)));
    for (index, cell) in image.cells.into_iter().enumerate() {
        let binding = intent.root.bindings.iter().find(|binding| binding.slot.loc.slot == index as u16).ok_or(ApplyFailure::InvalidAction)?;
        if values.get(&binding.target).map(|before| &before.value) != Some(&Value::Inventory(cell.clone())) { writes.insert(binding.target, Some(Value::Inventory(cell))); }
    }
    if let Some(remainder) = remainder {
        let template = intent.remainder.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        writes.insert(intent.remainder_target().ok_or(ApplyFailure::InvalidAction)?, Some(Value::Entity((state.dropped_item_body(template, &remainder)?).into())));
    }
    writes.retain(|target, after| values.get(target).map(|before| &before.value) != after.as_ref());
    Ok(BlockInventoryTickDerivation { writes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickKind as K;

    fn bytes(mut data: pumpkin_nbt::compound::NbtCompound) -> Vec<u8> {
        data.put_long("PumpkinInventoryTick", 99);
        pumpkin_nbt::Nbt::new(String::new(), data).write_unnamed_canonical().unwrap().to_vec()
    }
    fn cells(items: &[ItemStack]) -> Vec<InventoryCell> {
        items.iter().enumerate().map(|(slot, stack)| super::super::cluster_invsee::inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, slot as u16), stack).unwrap()).collect()
    }

    #[test]
    fn idle_cooking_and_brewing_do_not_reserve_tick_actions() {
        let empty = bytes(pumpkin_nbt::compound::NbtCompound::new());
        assert!(!non_hopper_tick_changes(K::Furnace, pumpkin_data::Block::FURNACE.default_state.id,
            &empty, &cells(&[ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone()]), 100).unwrap());
        assert!(!non_hopper_tick_changes(K::BrewingStand, pumpkin_data::Block::BREWING_STAND.default_state.id,
            &empty, &cells(&vec![ItemStack::EMPTY.clone(); 5]), 100).unwrap());
    }

    #[test]
    fn genesis_anchor_is_required_and_preserved() {
        let raw = pumpkin_nbt::Nbt::new(String::new(), pumpkin_nbt::compound::NbtCompound::new())
            .write_unnamed_canonical().unwrap().to_vec();
        let input = cells(&[ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone()]);
        assert!(derive_furnace_tick(pumpkin_data::Block::FURNACE.default_state.id, &raw, &input, 100).is_err());
        let anchored = initialize_genesis_tick_anchor(&raw, 99).unwrap();
        assert_eq!(initialize_genesis_tick_anchor(&anchored, 200).unwrap(), anchored);
        assert_eq!(document(&anchored).unwrap().root_tag.get_long("PumpkinInventoryTick"), Some(99));
        assert!(!non_hopper_tick_changes(K::Furnace, pumpkin_data::Block::FURNACE.default_state.id, &anchored, &input, 100).unwrap());
    }

    #[test]
    fn furnace_consumes_fuel_once_and_smelts_after_the_recipe_duration() {
        let empty = bytes(pumpkin_nbt::compound::NbtCompound::new());
        let input = cells(&[ItemStack::new(1, &Item::IRON_ORE), ItemStack::new(1, &Item::COAL), ItemStack::EMPTY.clone()]);
        let mut after = derive_furnace_tick(pumpkin_data::Block::FURNACE.default_state.id, &empty, &input, 100).unwrap();
        assert!(after.cells[1].is_empty());
        assert_eq!(after.cells[0].count, 1);
        assert!(after.cells[2].is_empty());
        let repeated = derive_furnace_tick(after.state, &after.block_entity, &after.cells, 100).unwrap();
        assert_eq!(repeated.cells, after.cells);
        assert_eq!(repeated.block_entity, after.block_entity);
        for slot in 101..299 {
            assert!(!non_hopper_tick_changes(K::Furnace, after.state, &after.block_entity, &after.cells, slot).unwrap());
        }
        assert!(non_hopper_tick_changes(K::Furnace, after.state, &after.block_entity, &after.cells, 299).unwrap());
        after = derive_furnace_tick(after.state, &after.block_entity, &after.cells, 299).unwrap();
        assert!(after.cells[0].is_empty());
        assert_eq!((after.cells[2].item, after.cells[2].count), (Item::IRON_INGOT.id, 1));
        let document = document(&after.block_entity).unwrap();
        assert_eq!(document.root_tag.get_compound("RecipesUsed").unwrap().get_int("minecraft:iron_ingot_from_smelting_iron_ore"), Some(1));
    }

    #[test]
    fn later_activation_moves_the_first_smelting_event_by_exactly_two_ticks() {
        let input = cells(&[ItemStack::new(1, &Item::IRON_ORE), ItemStack::new(1, &Item::COAL), ItemStack::EMPTY.clone()]);
        let empty_cells = cells(&vec![ItemStack::EMPTY.clone(); 3]);
        let initial = bytes(pumpkin_nbt::compound::NbtCompound::new());
        let state = pumpkin_data::Block::FURNACE.default_state.id;
        let ProcessingInputCut::Ready { block_entity: early_bytes } = furnace_input_cut(state, &initial, &empty_cells, 100).unwrap() else { panic!() };
        let ProcessingInputCut::Ready { block_entity: late_bytes } = furnace_input_cut(state, &initial, &empty_cells, 102).unwrap() else { panic!() };
        let early = derive_furnace_tick(state, &early_bytes, &input, 100).unwrap();
        let late = derive_furnace_tick(pumpkin_data::Block::FURNACE.default_state.id, &late_bytes, &input, 102).unwrap();
        assert_eq!(furnace_next_due(early.state, &early.block_entity, &early.cells, 100).unwrap(), Some(299));
        assert_eq!(furnace_next_due(late.state, &late.block_entity, &late.cells, 102).unwrap(), Some(301));
        assert!(non_hopper_tick_changes(K::Furnace, early.state, &early.block_entity, &early.cells, 299).unwrap());
        assert!(!non_hopper_tick_changes(K::Furnace, late.state, &late.block_entity, &late.cells, 299).unwrap());
        assert!(!non_hopper_tick_changes(K::Furnace, late.state, &late.block_entity, &late.cells, 300).unwrap());
        let completed = derive_furnace_tick(late.state, &late.block_entity, &late.cells, 301).unwrap();
        assert_eq!((completed.cells[2].item, completed.cells[2].count), (Item::IRON_INGOT.id, 1));
    }

    #[test]
    fn input_cut_defers_overdue_events_and_does_not_cook_new_items_in_old_elapsed_time() {
        let input = cells(&[ItemStack::new(1, &Item::IRON_ORE), ItemStack::new(1, &Item::COAL), ItemStack::EMPTY.clone()]);
        let state = pumpkin_data::Block::FURNACE.default_state.id;
        let initial = bytes(pumpkin_nbt::compound::NbtCompound::new());
        assert!(matches!(furnace_input_cut(state, &initial, &input, 102).unwrap(), ProcessingInputCut::Overdue { source_slot: 100 }));
        let started = derive_furnace_tick(state, &initial, &input, 100).unwrap();
        let mut old_cells = started.cells.clone(); old_cells[0] = InventoryCell::empty(old_cells[0].loc);
        let ProcessingInputCut::Ready { block_entity } = furnace_input_cut(started.state, &started.block_entity, &old_cells, 300).unwrap() else { panic!() };
        old_cells[0] = input[0].clone();
        let resumed = derive_furnace_tick(started.state, &block_entity, &old_cells, 300).unwrap();
        assert!(resumed.cells[2].is_empty());
        assert_eq!(furnace_next_due(resumed.state, &resumed.block_entity, &resumed.cells, 300).unwrap(), Some(499));
    }

    #[test]
    fn dragon_breath_preserves_potion_components_and_returns_exact_bottle_overflow() {
        use pumpkin_data::data_component_impl::PotionContentsImpl;
        let mut data = pumpkin_nbt::compound::NbtCompound::new(); data.put_short("BrewTime", 1); data.put_byte("Fuel", 19);
        let mut potion = ItemStack::new(1, &Item::SPLASH_POTION);
        potion.set_data_component(PotionContentsImpl { potion_id: Some(pumpkin_data::potion::Potion::AWKWARD.id as i32),
            custom_color: Some(0x123456), custom_effects: Vec::new(), custom_name: Some("original".to_owned()) });
        let input = cells(&[potion, ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone(), ItemStack::new(2, &Item::DRAGON_BREATH), ItemStack::EMPTY.clone()]);
        let after = derive_brewing_tick(pumpkin_data::Block::BREWING_STAND.default_state.id, &bytes(data), &input, 100).unwrap();
        assert_eq!((after.image.cells[0].item, after.image.cells[0].count), (Item::LINGERING_POTION.id, 1));
        let potion = super::super::cluster_invsee::item_stack_from_inventory_cell(&after.image.cells[0]).unwrap();
        let contents = potion.get_data_component::<PotionContentsImpl>().unwrap();
        assert_eq!(contents.custom_color, Some(0x123456));
        assert_eq!(contents.custom_name.as_deref(), Some("original"));
        assert_eq!(after.image.cells[3].count, 1);
        assert_eq!(after.remainder, Some(cells(&[ItemStack::new(1, &Item::GLASS_BOTTLE)]).remove(0)));
    }

    #[test]
    fn brewing_input_cut_shifts_completion_and_preserves_earlier_event_deferral() {
        use pumpkin_data::data_component_impl::PotionContentsImpl;
        let mut potion = ItemStack::new(1, &Item::POTION);
        potion.set_data_component(PotionContentsImpl { potion_id: Some(pumpkin_data::potion::Potion::WATER.id as i32),
            custom_color: None, custom_effects: Vec::new(), custom_name: None });
        let input = cells(&[potion, ItemStack::EMPTY.clone(), ItemStack::EMPTY.clone(), ItemStack::new(1, &Item::NETHER_WART), ItemStack::new(1, &Item::BLAZE_POWDER)]);
        let empty = cells(&vec![ItemStack::EMPTY.clone(); 5]);
        let initial = bytes(pumpkin_nbt::compound::NbtCompound::new());
        let state = pumpkin_data::Block::BREWING_STAND.default_state.id;
        assert!(matches!(brewing_input_cut(state, &initial, &input, 102).unwrap(), ProcessingInputCut::Overdue { source_slot: 100 }));
        let ProcessingInputCut::Ready { block_entity: early_cut } = brewing_input_cut(state, &initial, &empty, 100).unwrap() else { panic!() };
        let ProcessingInputCut::Ready { block_entity: late_cut } = brewing_input_cut(state, &initial, &empty, 102).unwrap() else { panic!() };
        let early = derive_brewing_tick(state, &early_cut, &input, 100).unwrap();
        let late = derive_brewing_tick(state, &late_cut, &input, 102).unwrap();
        assert_eq!(brewing_next_due(early.image.state, &early.image.block_entity, &early.image.cells, 100).unwrap(), Some(500));
        assert_eq!(brewing_next_due(late.image.state, &late.image.block_entity, &late.image.cells, 102).unwrap(), Some(502));
        assert!(!non_hopper_tick_changes(K::BrewingStand, late.image.state, &late.image.block_entity, &late.image.cells, 500).unwrap());
        let completed = derive_brewing_tick(late.image.state, &late.image.block_entity, &late.image.cells, 502).unwrap();
        assert!(completed.image.cells[3].is_empty());
        let potion = super::super::cluster_invsee::item_stack_from_inventory_cell(&completed.image.cells[0]).unwrap();
        assert_eq!(potion.get_data_component::<PotionContentsImpl>().unwrap().potion_id, Some(pumpkin_data::potion::Potion::AWKWARD.id as i32));
    }
}

#[cfg(test)]
mod draft_tests {
    use super::*;
    use std::collections::BTreeMap;
    use pumpkin_cluster::canonical_action::{ActionContext, ActionId, ActionRef, ActionState, AppliedWrite, Cell, ChunkPart, InventoryId, Target, TruthView, Value};
    use pumpkin_cluster::canonical_block_inventory_tick::{BlockInventoryTickDerivation, BlockInventoryTickIntent, BlockInventoryTickKind, BlockInventoryTickRoot};
    use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
    use pumpkin_cluster::inventory::{InventorySlotRef, InventoryTarget};
    use pumpkin_cluster::protocol::{BlockPos, ChunkKey};

    struct RegistryState { cells: BTreeMap<Target, Cell>, producer: pumpkin_cluster::realm_completion::PeerGeneration }
    impl ActionState for RegistryState {
        fn read(&self, _: TruthView, target: Target) -> Option<Cell> { self.cells.get(&target).cloned() }
        fn commit(&mut self, _: ActionContext, _: TruthView, writes: &[AppliedWrite]) -> Result<(), ApplyFailure> {
            for write in writes { if let Some(cell) = &write.after { self.cells.insert(write.target, cell.clone()); } else { self.cells.remove(&write.target); } }
            Ok(())
        }
        fn scheduled_executor(&self, _: ChunkKey) -> Result<pumpkin_cluster::realm_completion::PeerGeneration, ApplyFailure> { Ok(self.producer) }
        fn validate_block_inventory_layout(&self, block: Target, state: u16, entity: &Cell, bindings: &[InventorySlotBinding]) -> Result<(), ApplyFailure> {
            super::super::cluster_inventory_block_roots::validate_layout(block, state, entity, bindings)
        }
        fn inventory_limit(&self, cell: &InventoryCell, _: Target) -> Result<u8, ApplyFailure> {
            super::super::cluster_invsee::item_stack_from_inventory_cell(cell).map(|stack| stack.get_max_stack_size()).map_err(|_| ApplyFailure::InvalidAction)
        }
        fn derive_block_inventory_tick(&self, intent: &BlockInventoryTickIntent, values: &BTreeMap<Target, Cell>) -> Result<BlockInventoryTickDerivation, ApplyFailure> {
            derive(self, intent, values)
        }
    }

    fn root(key: ChunkKey, position: BlockPos, state: u16) -> BlockInventoryTickRoot {
        BlockInventoryTickRoot { block: Target::Chunk { key, part: ChunkPart::Block(position), inventory: None }, state,
            bindings: (0..5).map(|slot| InventorySlotBinding { slot: InventorySlotRef::new(InventoryTarget::Block { chunk: key, position }, InvLoc::new(INV_MAIN, slot)),
                target: Target::Chunk { key, part: ChunkPart::Block(position), inventory: Some(InventoryId::Block { inv: INV_MAIN, slot }) }, max_stack: 99 }).collect() }
    }
    fn install(state: &mut RegistryState, root: &BlockInventoryTickRoot, item: Option<&'static Item>) {
        let Target::Chunk { part: ChunkPart::Block(position), .. } = root.block else { panic!() };
        let mut data = pumpkin_nbt::compound::NbtCompound::new(); data.put_string("id", "minecraft:hopper".to_owned());
        data.put_int("x", position.x); data.put_int("y", position.y); data.put_int("z", position.z); data.put_int("TransferCooldown", 0);
        data.put_long("PumpkinInventoryTick", 99);
        let bytes = pumpkin_nbt::Nbt::new(String::new(), data).write_unnamed_canonical().unwrap().to_vec();
        state.cells.insert(root.block, Cell { revision: 0, value: Value::Block(root.state) });
        state.cells.insert(root.block_entity(), Cell { revision: 0, value: Value::BlockEntity(bytes) });
        for (index, binding) in root.bindings.iter().enumerate() {
            let stack = item.filter(|_| index == 0).map_or_else(|| ItemStack::EMPTY.clone(), |item| ItemStack::new(1, item));
            let cell = super::super::cluster_invsee::inventory_cell_from_item_stack(binding.slot.loc, &stack).unwrap();
            state.cells.insert(binding.target, Cell { revision: 0, value: Value::Inventory(cell) });
        }
    }
    fn intent(root: BlockInventoryTickRoot, neighbors: Vec<BlockInventoryTickRoot>, contacts: Vec<Target>, state: &RegistryState, sequence: u16) -> BlockInventoryTickIntent {
        BlockInventoryTickIntent { source: ActionRef { tick: pumpkin_cluster::time::TickStamp(100), id: ActionId {
            actor: pumpkin_cluster::actor::ActorId::Peer(state.producer.server), incarnation: state.producer.generation, family: 8, sequence } },
            source_slot: 100, producer: state.producer, kind: BlockInventoryTickKind::Hopper, root, neighbors, contacts, occupancy: Vec::new(), loose_items: Vec::new(), remainder: None }
    }

    #[test]
    fn later_hopper_source_uses_prior_peer_draft_inventory_and_receiver_cooldown() {
        use pumpkin_data::block_properties::{BlockProperties, FacingHopper, HopperLikeProperties};
        let key = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 0), 0, 0);
        let mut properties = HopperLikeProperties::from_state_id(pumpkin_data::Block::HOPPER.default_state.id);
        properties.facing = FacingHopper::East; properties.enabled = true;
        let block_state = properties.to_state_id(&pumpkin_data::Block::HOPPER).as_u16();
        let first = root(key, BlockPos { x: 4, y: 64, z: 4 }, block_state);
        let second = root(key, BlockPos { x: 5, y: 64, z: 4 }, block_state);
        let mut state = RegistryState { cells: BTreeMap::new(), producer: pumpkin_cluster::realm_completion::PeerGeneration {
            server: pumpkin_cluster::identity::ServerId(1), generation: 1 } };
        install(&mut state, &first, Some(&Item::COAL)); install(&mut state, &second, None);
        let block = |position| Target::Chunk { key, part: ChunkPart::Block(position), inventory: None };
        let above_first = block(BlockPos { x: 4, y: 65, z: 4 });
        let above_second = block(BlockPos { x: 5, y: 65, z: 4 });
        let output_second = block(BlockPos { x: 6, y: 64, z: 4 });
        for target in [above_first, above_second, output_second] { state.cells.insert(target, Cell { revision: 0, value: Value::Block(pumpkin_data::Block::AIR.default_state.id.as_u16()) }); }
        let first_intent = intent(first.clone(), vec![second.clone()], vec![second.block, above_first], &state, 0);
        let second_intent = intent(second.clone(), Vec::new(), vec![output_second, above_second], &state, 1);
        let stale_second = bind_tick_action(&state, second_intent.clone()).unwrap();
        let mut draft = pumpkin_cluster::actor_action_scope::ActorActionDraft::new(first_intent.source.id.actor, 1, first_intent.source.tick, 2, 64);
        draft.issue(&state, bind_tick_action(&state, first_intent).unwrap()).unwrap();
        assert!(draft.issue(&state, stale_second).is_err());
        let view = draft.view(&state);
        let values = second_intent.targets().into_iter().filter_map(|target| view.read(TruthView::Local, target).map(|cell| (target, cell))).collect();
        assert!(derive_hopper_tick(&view, &second_intent, &values).unwrap().writes.is_empty());
        let Value::Inventory(item) = view.read(TruthView::Local, second.bindings[0].target).unwrap().value else { panic!() };
        assert_eq!((item.item, item.count), (Item::COAL.id, 1));
        let Value::BlockEntity(bytes) = view.read(TruthView::Local, second.block_entity()).unwrap().value else { panic!() };
        assert_eq!(document(&bytes).unwrap().root_tag.get_int("TransferCooldown"), Some(8));
        assert_eq!(document(&bytes).unwrap().root_tag.get_long("PumpkinInventoryTick"), Some(100));
        let Value::Inventory(original) = state.read(TruthView::Local, second.bindings[0].target).unwrap().value else { panic!() };
        assert!(original.is_empty());
    }
}
