use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::inventory::{
    InventoryApplyError, InventoryCell, InventorySlotRef,
    InventoryStateSnapshot, InventoryTarget,
};
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_inventory::crafting::crafting_screen_handler::{
    CraftingRecipeIdentity, match_crafting_catalogue,
};
use pumpkin_protocol::codec::recipe::DynamicRecipe;

pub(super) struct TrustedCraftingPlan {
    pub recipe: CraftingRecipeIdentity,
    pub output: InventoryCell,
}

pub(super) struct FrozenRecipeMetadata {
    intent: pumpkin_cluster::canonical_recipe::RecipeInventoryIntent,
    recipes: std::sync::Arc<Vec<DynamicRecipe>>,
    companion_count: Option<u8>,
    quick_move: bool,
    menu: pumpkin_cluster::canonical_action::Target,
    session: u64,
    window_kind: u16,
}

impl FrozenRecipeMetadata {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), pumpkin_cluster::canonical_action::ApplyFailure> {
        inventory_metadata_resource_usage(&(&self.intent, self.companion_count, self.quick_move, self.menu, self.session, self.window_kind))
    }
    pub(super) fn bindings(
        &self,
    ) -> Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding> {
        self.intent.bindings()
    }

    pub(super) fn targets(&self) -> Vec<pumpkin_cluster::canonical_action::Target> {
        let mut targets = self.intent.targets();
        targets.push(self.menu);
        targets
    }

    pub(super) fn materialize<S: pumpkin_cluster::canonical_action::ActionState>(
        &self,
        view: &S,
        mut capture: pumpkin_cluster::canonical_inventory::InventoryActionCapture,
    ) -> Result<
        pumpkin_cluster::canonical_action::Action,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        use pumpkin_cluster::canonical_action::{ApplyFailure, TruthView, Value};
        use pumpkin_cluster::canonical_recipe::RecipeIngredientFact;
        let menu = view.read(TruthView::Local, self.menu).ok_or(ApplyFailure::Condition(self.menu))?;
        if !matches!(&menu.value, Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(state))
            if state.session == self.session && (state.session == 0 || state.window_kind == self.window_kind)) {
            return Err(ApplyFailure::Condition(self.menu));
        }
        capture.guards.push(pumpkin_cluster::canonical_action::Condition { target: self.menu,
            expected: pumpkin_cluster::canonical_action::Expected::Exact(menu) });
        let mut snapshots = BTreeMap::new();
        let mut values = BTreeMap::new();
        for binding in self.bindings() {
            let cell = view
                .read(TruthView::Local, binding.target)
                .ok_or(ApplyFailure::Inventory(binding.target))?;
            values.insert(binding.target, cell.clone());
            let Value::Inventory(cell) = cell.value else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            snapshots
                .entry(binding.slot.target)
                .or_insert_with(|| InventoryStateSnapshot {
                    target: binding.slot.target,
                    revision: 0,
                    cells: Vec::new(),
                })
                .cells
                .push(cell);
        }
        let snapshots = snapshots.into_values().collect::<Vec<_>>();
        let width = if self.intent.grid.len() == 4 { 2 } else { 3 };
        let grid = self
            .intent
            .grid
            .iter()
            .map(|binding| binding.slot)
            .collect::<Vec<_>>();
        let storage = self
            .intent
            .remainder_destinations
            .iter()
            .map(|binding| binding.slot)
            .collect::<Vec<_>>();
        let destination = &self.intent.output_destinations[0];
        let plan = prepare_crafting_take(
            &snapshots,
            &grid,
            width,
            width,
            destination.slot,
            destination.max_stack,
            &storage,
            &self.recipes,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
        if self.companion_count.is_some_and(|count| {
            plan.output.count.checked_mul(self.intent.repetitions) != Some(count)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut intent = self.intent.clone();
        intent.output = plan.output;
        intent.output_stack_limit = super::cluster_invsee::item_stack_from_inventory_cell(&intent.output)
            .map_err(|_| ApplyFailure::InvalidAction)?.get_max_stack_size();
        intent.recipe = format!("{:?}", plan.recipe);
        intent.ingredients.clear();
        for binding in &intent.grid {
            let cell = snapshots
                .iter()
                .find(|snapshot| snapshot.target == binding.slot.target)
                .and_then(|snapshot| {
                    snapshot
                        .cells
                        .iter()
                        .find(|cell| cell.loc == binding.slot.loc)
                })
                .ok_or(ApplyFailure::Inventory(binding.target))?;
            if cell.is_empty() {
                continue;
            }
            let remainder = pumpkin_data::recipe_remainder::get_recipe_remainder_id(cell.item)
                .map(|id| {
                    Item::from_id(id)
                        .ok_or(InventoryApplyError::Rejected)
                        .and_then(|item| {
                            super::cluster_invsee::inventory_cell_from_item_stack(
                                binding.slot.loc,
                                &ItemStack::new(1, item),
                            )
                        })
                })
                .transpose()
                .map_err(|_| ApplyFailure::InvalidAction)?;
            let remainder_stack_limit = remainder.as_ref().map(|cell|
                super::cluster_invsee::item_stack_from_inventory_cell(cell)
                    .map(|stack| stack.get_max_stack_size()))
                .transpose().map_err(|_| ApplyFailure::InvalidAction)?.unwrap_or(0);
            intent.ingredients.push(RecipeIngredientFact {
                binding: binding.clone(),
                item: cell.item,
                components: cell.nbt.clone(),
                count: 1,
                remainder,
                remainder_stack_limit,
            });
        }
        let mut signature = intent.recipe.as_bytes().to_vec();
        signature.extend_from_slice(&intent.output.item.to_le_bytes());
        signature.push(intent.output.count);
        signature.extend_from_slice(&intent.output.nbt);
        for ingredient in &intent.ingredients {
            signature.extend_from_slice(&ingredient.item.to_le_bytes());
            signature.extend_from_slice(&(ingredient.components.len() as u32).to_le_bytes());
            signature.extend_from_slice(&ingredient.components);
        }
        intent.catalogue = xxhash_rust::xxh64::xxh64(&signature, 0).max(1);
        if self.quick_move {
            let maximum = intent.repetitions;
            intent.repetitions = 1;
            intent.derive(&values)?;
            for repetitions in 2..=maximum {
                intent.repetitions = repetitions;
                if intent.derive(&values).is_err() {
                    intent.repetitions -= 1;
                    break;
                }
            }
        }
        intent.capture_action(view, capture)
    }
}

pub(super) fn capture_recipe_proposal(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
) -> Result<
    (
        FrozenRecipeMetadata,
        pumpkin_cluster::canonical_inventory::InventoryActionCapture,
    ),
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure, ClaimedAction};
    use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
    use pumpkin_cluster::canonical_recipe::RecipeInventoryIntent;
    use pumpkin_cluster::inventory::{
        INV_CURSOR, INV_MAIN, InvLoc, InventoryGesture, InventoryMenuKind,
    };
    let ClaimedAction::InventoryMenu(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation
        || proposal.id.family != 2
        || claim.session.actor != lease.gid
        || proposal.screen_generation != claim.session.session
        || !original.validate_live_player_lease(lease)
        || original.player_relation_participant(lease).is_none_or(|actor| actor.chunk != claim.actor_chunk)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let screen = original.inventory_source_screen(claim.session)
        .ok_or(ApplyFailure::InvalidAction)?;
    if screen.sync_id != claim.sync_id || screen.window_kind != claim.window_kind {
        return Err(ApplyFailure::InvalidAction);
    }
    let repetitions = match claim.kind {
        InventoryMenuKind::TakeDisplayedResult { repetitions } => {
            u8::try_from(repetitions).map_err(|_| ApplyFailure::InvalidAction)?
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let width = if claim.session.session == 0 {
        2
    } else if claim.window_kind == pumpkin_data::screen::WindowType::Crafting as u16 {
        3
    } else {
        return Err(ApplyFailure::InvalidAction);
    };
    let entry = |index| {
        screen
            .slots
            .iter()
            .find(|slot| slot.screen_slot == index)
            .ok_or(ApplyFailure::InvalidAction)
    };
    let preview = entry(0)?.slot;
    let grid = (1..=width * width)
        .map(|index| entry(index as u16).map(|slot| slot.slot))
        .collect::<Result<Vec<_>, _>>()?;
    let player = super::cluster_world_inventory_actions::current_inventory_address_from_snapshot(original, InventoryTarget::Player(lease.gid))
        .ok_or(ApplyFailure::InvalidAction)?;
    let cursor = InventorySlotRef::new(player, InvLoc::new(INV_CURSOR, 0));
    let storage = (0..36)
        .map(|slot| InventorySlotRef::new(player, InvLoc::new(INV_MAIN, slot)))
        .collect::<Vec<_>>();
    let mut quick_move = false;
    let destinations = match claim.companion_gestures.as_deref() {
        None | Some([]) => vec![(cursor, 99)],
        Some(
            [
                InventoryGesture::Transfer {
                    source,
                    destination,
                    ..
                },
            ],
        ) if *source == preview => vec![(*destination, 99)],
        Some([InventoryGesture::QuickMove { source, destinations }]) if *source == preview => {
            quick_move = true;
            destinations.clone()
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if destinations.is_empty() || destinations.len() > 36
        || destinations.iter().any(|(destination, limit)|
            (*destination != cursor && !storage.contains(destination)) || *limit == 0)
        || destinations.iter().map(|(slot, _)| slot).collect::<BTreeSet<_>>().len() != destinations.len() {
        return Err(ApplyFailure::InvalidAction);
    }
    let destination = destinations[0].0;
    let recipes = original.inventory_source_recipes();
    let binding = |slot: InventorySlotRef| -> Result<InventorySlotBinding, ApplyFailure> {
        Ok(InventorySlotBinding {
            slot,
            target: super::cluster_world_inventory_actions::binding_target_from_snapshot(
                original,
                Some(&screen),
                slot,
            )
            .ok_or(ApplyFailure::InvalidAction)?,
            max_stack: 99,
        })
    };
    let grid_bindings = grid
        .iter()
        .copied()
        .map(binding)
        .collect::<Result<Vec<_>, _>>()?;
    let intent = RecipeInventoryIntent {
        recipe: String::new(),
        catalogue: 0,
        grid: grid_bindings,
        ingredients: Vec::new(),
        output: InventoryCell::empty(destination.loc),
        output_stack_limit: 0,
        output_destinations: destinations.into_iter().map(|(slot, _)| binding(slot)).collect::<Result<Vec<_>, _>>()?,
        remainder_destinations: storage
            .into_iter()
            .map(binding)
            .collect::<Result<Vec<_>, _>>()?,
        preview: Some(binding(preview)?),
        repetitions,
    };
    let companion_count = match claim.companion_gestures.as_deref() {
        Some(
            [
                InventoryGesture::Transfer {
                    count,
                    destination_limit,
                    ..
                },
            ],
        ) if *destination_limit > 0 => Some(*count),
        None | Some([]) => None,
        Some([InventoryGesture::QuickMove { .. }]) => None,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    Ok((
        FrozenRecipeMetadata {
            intent,
            recipes,
            companion_count,
            quick_move,
            menu: pumpkin_cluster::canonical_action::Target::Chunk { key: claim.actor_chunk,
                part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                    actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
                    property: pumpkin_cluster::living_property::LivingProperty::MenuSession,
                }, inventory: None },
            session: claim.session.session,
            window_kind: claim.window_kind,
        },
        InventoryActionCapture {
            edit_authority: None,
            id: proposal.id,
            tick: proposal.tick,
            actor_chunk: Some(claim.actor_chunk),
            player_leases: vec![lease],
            player_chunks: vec![(lease.key, claim.actor_chunk)],
            guards: Vec::new(),
            client_origin: proposal.client_origin,
        },
    ))
}

pub(super) fn prepare_crafting_take(
    snapshots: &[InventoryStateSnapshot],
    grid: &[InventorySlotRef],
    width: usize,
    height: usize,
    destination: InventorySlotRef,
    destination_limit: u8,
    remainder_destinations: &[InventorySlotRef],
    recipes: &[DynamicRecipe],
) -> Result<TrustedCraftingPlan, InventoryApplyError> {
    if grid.len() != width * height
        || grid.is_empty()
        || grid.len() > 9
        || grid.iter().copied().collect::<BTreeSet<_>>().len() != grid.len()
        || grid.contains(&destination)
        || destination_limit == 0
    {
        return Err(InventoryApplyError::Rejected);
    }
    super::cluster_invsee::validate_imported_inventory_snapshots(snapshots)?;
    let mut original = BTreeMap::new();
    let mut revisions = BTreeMap::new();
    for snapshot in snapshots {
        if revisions
            .insert(snapshot.target, snapshot.revision)
            .is_some()
        {
            return Err(InventoryApplyError::Rejected);
        }
        for cell in &snapshot.cells {
            if original
                .insert(
                    InventorySlotRef::new(snapshot.target, cell.loc),
                    cell.clone(),
                )
                .is_some()
            {
                return Err(InventoryApplyError::Rejected);
            }
        }
    }
    for slot in grid
        .iter()
        .chain(std::iter::once(&destination))
        .chain(remainder_destinations)
    {
        if !revisions.contains_key(&slot.target) {
            return Err(InventoryApplyError::TargetUnavailable);
        }
        original
            .entry(*slot)
            .or_insert_with(|| InventoryCell::empty(slot.loc));
    }
    let stacks = grid
        .iter()
        .map(|slot| super::cluster_invsee::item_stack_from_inventory_cell(&original[slot]))
        .collect::<Result<Vec<_>, _>>()?;
    let matched = match_crafting_catalogue(width, height, &stacks, recipes)
        .ok_or(InventoryApplyError::Rejected)?;
    if let CraftingRecipeIdentity::Vanilla(index) = matched.recipe
        && !matches!(
            pumpkin_data::recipes::RECIPES_CRAFTING[index],
            pumpkin_data::recipes::CraftingRecipeTypes::CraftingShaped { .. }
                | pumpkin_data::recipes::CraftingRecipeTypes::CraftingShapeless { .. }
        )
    {
        return Err(InventoryApplyError::Rejected);
    }
    let item = Item::from_registry_key(
        matched
            .result
            .item_id
            .strip_prefix("minecraft:")
            .unwrap_or(&matched.result.item_id),
    )
    .ok_or(InventoryApplyError::Rejected)?;
    if matched.result.count == 0 {
        return Err(InventoryApplyError::Rejected);
    }
    let output = super::cluster_invsee::inventory_cell_from_item_stack(
        destination.loc,
        &ItemStack::new(matched.result.count, item),
    )?;
    Ok(TrustedCraftingPlan { recipe: matched.recipe, output })
}

pub(super) fn refresh_crafting_preview(
    server: &super::Server,
    screen: &mut pumpkin_cluster::inventory::InventoryScreenSnapshot,
) {
    let width = if screen.session.session == 0 { 2 }
        else if screen.window_kind == pumpkin_data::screen::WindowType::Crafting as u16 { 3 }
        else { return; };
    let Some(result_loc) = screen.slots.iter().find(|slot| slot.screen_slot == 0)
        .map(|slot| slot.cell.loc) else { return; };
    let stacks = (1..=width * width).map(|index| screen.slots.iter()
        .find(|slot| usize::from(slot.screen_slot) == index)
        .ok_or(InventoryApplyError::Rejected)
        .and_then(|slot| super::cluster_invsee::item_stack_from_inventory_cell(&slot.cell)))
        .collect::<Result<Vec<_>, _>>();
    let preview = stacks.ok().and_then(|stacks| {
        let recipes = server.recipe_manager.cluster_dynamic_recipes_snapshot();
        let matched = match_crafting_catalogue(width, width, &stacks, &recipes)?;
        if let CraftingRecipeIdentity::Vanilla(index) = matched.recipe
            && !matches!(pumpkin_data::recipes::RECIPES_CRAFTING[index],
                pumpkin_data::recipes::CraftingRecipeTypes::CraftingShaped { .. }
                | pumpkin_data::recipes::CraftingRecipeTypes::CraftingShapeless { .. }) {
            return None;
        }
        let key = matched.result.item_id.strip_prefix("minecraft:").unwrap_or(&matched.result.item_id);
        let item = Item::from_registry_key(key)?;
        if matched.result.count == 0 { return None; }
        super::cluster_invsee::inventory_cell_from_item_stack(result_loc,
            &ItemStack::new(matched.result.count, item)).ok()
    }).unwrap_or_else(|| InventoryCell::empty(result_loc));
    if let Some(result) = screen.slots.iter_mut().find(|slot| slot.screen_slot == 0) {
        result.cell = preview;
    }
}

pub(super) fn capture_menu_session_proposal(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
) -> Result<(pumpkin_cluster::canonical_menu::MenuSessionIntent,
    pumpkin_cluster::canonical_inventory::InventoryActionCapture), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure, ChunkPart, ClaimedAction, Target};
    let (actor_chunk, mut operation) = match &proposal.claim {
        ClaimedAction::InventoryMenuSession(claim) => (claim.actor_chunk, claim.operation.clone()),
        ClaimedAction::InventoryMenu(claim) => match claim.kind {
            pumpkin_cluster::inventory::InventoryMenuKind::ContainerButton(pumpkin_cluster::inventory::InventoryContainerButton::StonecutterRecipe { recipe }) => (claim.actor_chunk,
                pumpkin_cluster::canonical_menu::MenuSessionOperation::Select { session: claim.session.session,
                    selection: pumpkin_cluster::canonical_menu::MenuSelection::Recipe(recipe) }),
            pumpkin_cluster::inventory::InventoryMenuKind::ContainerButton(pumpkin_cluster::inventory::InventoryContainerButton::LoomPattern { pattern }) => (claim.actor_chunk,
                pumpkin_cluster::canonical_menu::MenuSessionOperation::Select { session: claim.session.session,
                    selection: pumpkin_cluster::canonical_menu::MenuSelection::LoomPattern(pattern) }),
            pumpkin_cluster::inventory::InventoryMenuKind::SelectTrade { offer } => (claim.actor_chunk,
                pumpkin_cluster::canonical_menu::MenuSessionOperation::Select { session: claim.session.session,
                    selection: pumpkin_cluster::canonical_menu::MenuSelection::TradeOffer(offer) }),
            _ => return Err(ApplyFailure::InvalidAction),
        },
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if let ClaimedAction::InventoryMenu(claim) = &proposal.claim {
        let screen = original.inventory_source_screen(claim.session).ok_or(ApplyFailure::InvalidAction)?;
        if claim.session.actor != lease.gid || proposal.screen_generation != claim.session.session
            || screen.sync_id != claim.sync_id || screen.window_kind != claim.window_kind { return Err(ApplyFailure::InvalidAction); }
    }
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation || proposal.id.family != 2
        || !original.validate_live_player_lease(lease)
        || original.player_relation_participant(lease).is_none_or(|actor| actor.chunk != actor_chunk) {
        return Err(ApplyFailure::InvalidAction);
    }
    if let pumpkin_cluster::canonical_menu::MenuSessionOperation::Open { viewer, .. } = &mut operation {
        *viewer = capture_menu_viewer(original, lease, actor_chunk, proposal.tick)?;
    }
    let intent = pumpkin_cluster::canonical_menu::MenuSessionIntent {
        lease,
        virtual_inventory: match &proposal.claim { ClaimedAction::InventoryMenuSession(claim) => claim.virtual_inventory.clone(), _ => None },
        target: Target::Chunk { key: actor_chunk, part: ChunkPart::LivingProperty {
            actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            property: pumpkin_cluster::living_property::LivingProperty::MenuSession,
        }, inventory: None },
        operation,
    };
    if !intent.valid() { return Err(ApplyFailure::InvalidAction); }
    Ok((intent, pumpkin_cluster::canonical_inventory::InventoryActionCapture {
        id: proposal.id, tick: proposal.tick, actor_chunk: Some(actor_chunk),
        player_leases: vec![lease], player_chunks: vec![(lease.key, actor_chunk)],
        guards: Vec::new(), client_origin: proposal.client_origin, edit_authority: None,
    }))
}

pub(super) fn capture_menu_viewer(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    chunk: pumpkin_cluster::protocol::ChunkKey,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<pumpkin_cluster::item_contact::ContactObservation, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value};
    let actor = original.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    if actor.chunk != chunk { return Err(ApplyFailure::InvalidAction); }
    let body = original.read(TruthView::Local, Target::Chunk { key: chunk,
        part: ChunkPart::Player(lease.gid), inventory: None }).ok_or(ApplyFailure::InvalidAction)?;
    let Value::Living(living) = body.value else { return Err(ApplyFailure::InvalidAction); };
    let scale = f64::from(super::cluster_living_metadata::player_scale(&living));
    let avatar = living.avatar.ok_or(ApplyFailure::InvalidAction)?;
    let pose = if avatar.stance.crawling || avatar.stance.swimming || avatar.stance.gliding {
        pumpkin_data::entity::EntityPose::Swimming
    } else if avatar.stance.sneaking { pumpkin_data::entity::EntityPose::Crouching }
        else { pumpkin_data::entity::EntityPose::Standing };
    let dimensions = crate::entity::Entity::get_entity_dimensions(pose);
    let generation = original.peer_incarnations().get(&lease.gid.server)
        .copied().ok_or(ApplyFailure::InvalidAction)?;
    pumpkin_cluster::item_contact::ContactObservation::player(lease,
        pumpkin_cluster::realm_completion::PeerGeneration { server: lease.gid.server, generation },
        avatar.spatial_epoch, chunk, actor.frame.effective,
        f64::from(dimensions.width) * scale * 0.5, f64::from(dimensions.height) * scale, tick)
        .ok_or(ApplyFailure::InvalidAction)
}

pub(crate) fn entity_storage_menu_screen(
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    session: u64,
    sync_id: u8,
    viewer: pumpkin_cluster::item_contact::ContactObservation,
    entity: pumpkin_cluster::item_contact::ContactObservation,
    kind: u16,
    viewer_inventory: &InventoryStateSnapshot,
    inventory: &InventoryStateSnapshot,
    slots: u16,
    title: pumpkin_util::text::TextComponent,
) -> Result<(pumpkin_cluster::inventory::InventoryScreenSnapshot,
    super::cluster_invsee::ClusterScreenPresentation,
    pumpkin_cluster::canonical_menu::MenuSessionIntent), InventoryApplyError> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::canonical_menu::{MenuSelection, MenuSessionIntent, MenuSessionOperation, MenuSessionState, MenuSource};
    use pumpkin_cluster::inventory::{INV_CURSOR, INV_MAIN, InvLoc, InventoryScreenSession, InventoryScreenSlot, InventoryScreenSnapshot};
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } = &entity.identity
        else { return Err(InventoryApplyError::Rejected); };
    if inventory.target != InventoryTarget::Entity((*id).into())
        || !matches!(viewer_inventory.target, InventoryTarget::LeasedPlayer { key, player, incarnation }
            if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation) {
        return Err(InventoryApplyError::Rejected);
    }
    let window_type = match slots {
        5 => pumpkin_data::screen::WindowType::Hopper,
        9 => pumpkin_data::screen::WindowType::Generic9x1,
        18 => pumpkin_data::screen::WindowType::Generic9x2,
        27 => pumpkin_data::screen::WindowType::Generic9x3,
        36 => pumpkin_data::screen::WindowType::Generic9x4,
        45 => pumpkin_data::screen::WindowType::Generic9x5,
        54 => pumpkin_data::screen::WindowType::Generic9x6,
        _ => return Err(InventoryApplyError::Rejected),
    };
    super::cluster_invsee::validate_imported_inventory_snapshots(&[viewer_inventory.clone(), inventory.clone()])?;
    let menu = MenuSessionState { session, window_kind: window_type as u16,
        source: MenuSource::Entity { contact: entity.clone(), kind }, selection: MenuSelection::None, ..Default::default() };
    let intent = MenuSessionIntent { lease, virtual_inventory: None, target: Target::Chunk { key: viewer.chunk,
        part: ChunkPart::LivingProperty { actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None },
        operation: MenuSessionOperation::Open { state: menu, viewer: viewer.clone() } };
    if !intent.valid() { return Err(InventoryApplyError::Rejected); }
    let mut screen_slots = Vec::with_capacity(usize::from(slots) + 36);
    let mut stacks = Vec::with_capacity(usize::from(slots) + 36);
    for screen_slot in 0..slots + 36 {
        let (snapshot, location) = if screen_slot < slots {
            (inventory, InvLoc::new(INV_MAIN, screen_slot))
        } else {
            let index = screen_slot - slots;
            (viewer_inventory, InvLoc::new(INV_MAIN, if index < 27 { index + 9 } else { index - 27 }))
        };
        let cell = snapshot.cells.iter().find(|cell| cell.loc == location).cloned()
            .unwrap_or_else(|| InventoryCell::empty(location));
        stacks.push(super::cluster_invsee::item_stack_from_inventory_cell(&cell)?);
        screen_slots.push(InventoryScreenSlot { screen_slot,
            slot: InventorySlotRef::new(snapshot.target, location), cell, limit: 99 });
    }
    let cursor_loc = InvLoc::new(INV_CURSOR, 0);
    let cursor = viewer_inventory.cells.iter().find(|cell| cell.loc == cursor_loc).cloned()
        .unwrap_or_else(|| InventoryCell::empty(cursor_loc));
    let mut affected = vec![viewer.chunk, entity.chunk];
    affected.sort_unstable();
    affected.dedup();
    Ok((InventoryScreenSnapshot { session: InventoryScreenSession { actor: lease.gid, session },
        sync_id, state_id: 0, window_kind: window_type as u16, container_slots: slots, slots: screen_slots,
        cursor, affected, preview: None, preview_incarnation: None, preview_lease: None, preview_authority: None },
        super::cluster_invsee::ClusterScreenPresentation { title, window_type, stacks }, intent))
}

pub(crate) fn block_menu_open(
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    session: u64,
    sync_id: u8,
    viewer: pumpkin_cluster::item_contact::ContactObservation,
    viewer_inventory: &InventoryStateSnapshot,
    descriptor: super::cluster_invsee::BlockMenuDescriptor,
) -> Result<(pumpkin_cluster::inventory::InventoryScreenSnapshot,
    super::cluster_invsee::ClusterScreenPresentation,
    pumpkin_cluster::canonical_menu::MenuSessionIntent), InventoryApplyError> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::canonical_menu::{MenuBlockContact, MenuSelection, MenuSessionIntent, MenuSessionOperation, MenuSessionState, MenuSource};
    if !matches!(viewer_inventory.target, InventoryTarget::LeasedPlayer { key, player, incarnation }
        if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation) {
        return Err(InventoryApplyError::Rejected);
    }
    let blocks = std::iter::once(&descriptor.first).chain(descriptor.second.iter()).map(|leg| {
        let InventoryTarget::Block { chunk, position } = leg.target else { return Err(InventoryApplyError::Rejected); };
        Ok(MenuBlockContact { chunk, position, state: leg.block_state.as_u16() })
    }).collect::<Result<Vec<_>, InventoryApplyError>>()?;
    let (screen, presentation) = super::cluster_invsee::block_menu_screen(
        pumpkin_cluster::inventory::InventoryScreenSession { actor: lease.gid, session },
        sync_id, viewer.chunk, viewer_inventory, descriptor)?;
    let state = MenuSessionState { session, window_kind: screen.window_kind,
        source: MenuSource::Blocks(blocks), selection: MenuSelection::None, ..Default::default() };
    let intent = MenuSessionIntent { lease, virtual_inventory: None, target: Target::Chunk { key: viewer.chunk,
        part: ChunkPart::LivingProperty { actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None },
        operation: MenuSessionOperation::Open { state, viewer } };
    if !intent.valid() { return Err(InventoryApplyError::Rejected); }
    Ok((screen, presentation, intent))
}

pub(super) fn capture_block_menu<S: pumpkin_cluster::canonical_action::ActionState>(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &S,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    chunk: pumpkin_cluster::protocol::ChunkKey,
    position: pumpkin_cluster::protocol::BlockPos,
    state: pumpkin_data::BlockStateId,
) -> Result<(InventoryStateSnapshot, super::cluster_invsee::BlockMenuDescriptor), InventoryApplyError> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target, TruthView, Value};
    use pumpkin_cluster::inventory::{INV_MAIN, InvLoc};
    use pumpkin_data::block_properties::{BlockProperties, ChestLikeProperties, ChestType};
    use super::cluster_invsee::{BlockMenuDescriptor, BlockMenuKind, BlockMenuLeg};
    let block = state.to_block();
    let chest = block == &pumpkin_data::Block::CHEST || block == &pumpkin_data::Block::TRAPPED_CHEST;
    let (size, kind, title) = if chest { (27, BlockMenuKind::GenericStorage { rows: 3 }, "Chest") }
        else if block == &pumpkin_data::Block::BARREL { (27, BlockMenuKind::GenericStorage { rows: 3 }, "Barrel") }
        else if block == &pumpkin_data::Block::HOPPER { (5, BlockMenuKind::Hopper, "Item Hopper") }
        else { return Err(InventoryApplyError::Rejected); };
    let target = Target::Chunk { key: chunk, part: ChunkPart::Block(position), inventory: None };
    let root = view.read(TruthView::Local, target).ok_or(InventoryApplyError::TargetUnavailable)?;
    if root.value != Value::Block(state.as_u16()) { return Err(InventoryApplyError::Rejected); }
    let capture_snapshot = |target: InventoryTarget, slots: Option<u16>| {
        let address = super::cluster_world_inventory_actions::current_inventory_address_from_snapshot(original, target)
            .ok_or(InventoryApplyError::TargetUnavailable)?;
        let bindings = original.captured_inventory_targets().filter(|(_, inventory, _)| *inventory == address)
            .map(|(target, _, loc)| (loc, target)).collect::<std::collections::BTreeMap<_, _>>();
        if bindings.is_empty() { return Err(InventoryApplyError::TargetUnavailable); }
        let mut snapshot = InventoryStateSnapshot { target: address, revision: 0, cells: Vec::new() };
        let locations = if let Some(slots) = slots { (0..slots).map(|slot| InvLoc::new(INV_MAIN, slot)).collect::<Vec<_>>() }
            else { bindings.keys().copied().collect() };
        snapshot.cells = locations.into_iter().map(|loc| {
            let target = *bindings.get(&loc).ok_or(InventoryApplyError::TargetUnavailable)?;
            match view.read(TruthView::Local, target) {
                Some(pumpkin_cluster::canonical_action::Cell { revision, value: Value::Inventory(cell) }) if cell.loc == loc => {
                    snapshot.revision = snapshot.revision.max(revision);
                    Ok(cell)
                },
                _ => Err(InventoryApplyError::TargetUnavailable),
            }
        }).collect::<Result<Vec<_>, InventoryApplyError>>()?;
        Ok::<_, InventoryApplyError>(snapshot)
    };
    let viewer = capture_snapshot(InventoryTarget::LeasedPlayer { key: lease.key.0,
        player: lease.gid, incarnation: lease.incarnation }, None)?;
    let first = BlockMenuLeg { target: InventoryTarget::Block { chunk, position }, chunk,
        block_state: state, block_revision: root.revision,
        inventory: capture_snapshot(InventoryTarget::Block { chunk, position }, Some(size))? };
    let mut descriptor = BlockMenuDescriptor { first, second: None, kind,
        title: pumpkin_util::text::TextComponent::text(title) };
    if chest {
        let properties = ChestLikeProperties::from_state_id(state);
        let direction = match properties.r#type {
            ChestType::Single => return Ok((viewer, descriptor)),
            ChestType::Left => properties.facing.rotate_clockwise(),
            ChestType::Right => properties.facing.rotate_counter_clockwise(),
        };
        let offset = direction.to_offset();
        let other_position = pumpkin_cluster::protocol::BlockPos { x: position.x + offset.x,
            y: position.y + offset.y, z: position.z + offset.z };
        let other_chunk = pumpkin_cluster::protocol::ChunkKey::new(chunk.world,
            other_position.x.div_euclid(16), other_position.z.div_euclid(16));
        let target = Target::Chunk { key: other_chunk, part: ChunkPart::Block(other_position), inventory: None };
        let other = view.read(TruthView::Local, target).ok_or(InventoryApplyError::TargetUnavailable)?;
        let Value::Block(id) = other.value else { return Err(InventoryApplyError::Rejected); };
        let other_state = pumpkin_data::BlockStateId::new(id).ok_or(InventoryApplyError::Rejected)?;
        if other_state.to_block() != block { return Err(InventoryApplyError::Rejected); }
        let other_properties = ChestLikeProperties::from_state_id(other_state);
        if other_properties.facing != properties.facing || other_properties.r#type != match properties.r#type {
            ChestType::Left => ChestType::Right, ChestType::Right => ChestType::Left, ChestType::Single => ChestType::Single,
        } { return Err(InventoryApplyError::Rejected); }
        let other_leg = BlockMenuLeg { target: InventoryTarget::Block { chunk: other_chunk, position: other_position },
            chunk: other_chunk, block_state: other_state, block_revision: other.revision,
            inventory: capture_snapshot(InventoryTarget::Block { chunk: other_chunk, position: other_position }, Some(27))? };
        if properties.r#type == ChestType::Right { descriptor.second = Some(other_leg); }
        else { let original = std::mem::replace(&mut descriptor.first, other_leg); descriptor.second = Some(original); }
        descriptor.kind = BlockMenuKind::GenericStorage { rows: 6 };
    }
    Ok((viewer, descriptor))
}

#[derive(Clone, Copy)]
pub(crate) enum BlockInventoryExtraction { All, Furnace, Brewing, Unsupported }

pub(crate) struct BlockInventoryPolicy {
    pub slots: u16,
    pub bottom_slots: Vec<u16>,
    pub extraction: BlockInventoryExtraction,
}

impl BlockInventoryPolicy {
    pub(crate) fn can_extract_bottom(&self, slot: u16, cell: &InventoryCell) -> bool {
        if !self.bottom_slots.contains(&slot) || cell.is_empty() { return false; }
        match self.extraction {
            BlockInventoryExtraction::All => true,
            BlockInventoryExtraction::Furnace => slot == 2 || slot == 1
                && [Item::BUCKET.id, Item::WATER_BUCKET.id].contains(&cell.item),
            BlockInventoryExtraction::Brewing => slot <= 2 || slot == 3 && cell.item == Item::GLASS_BOTTLE.id,
            BlockInventoryExtraction::Unsupported => false,
        }
    }
}

pub(crate) fn block_inventory_policy(state: pumpkin_data::BlockStateId) -> Option<BlockInventoryPolicy> {
    let (slots, extraction) = match state.to_block().name {
        "chest" | "trapped_chest" | "barrel" => (27, BlockInventoryExtraction::All),
        "hopper" => (5, BlockInventoryExtraction::All),
        "dispenser" | "dropper" | "crafter" => (9, BlockInventoryExtraction::All),
        "furnace" | "blast_furnace" | "smoker" => (3, BlockInventoryExtraction::Furnace),
        "brewing_stand" => (5, BlockInventoryExtraction::Brewing),
        "chiseled_bookshelf" => (6, BlockInventoryExtraction::All),
        "decorated_pot" | "jukebox" => (1, BlockInventoryExtraction::All),
        "composter" | "beacon" => (1, BlockInventoryExtraction::Unsupported),
        name if name == "shulker_box" || name.ends_with("_shulker_box") => (27, BlockInventoryExtraction::All),
        _ => return None,
    };
    let bottom_slots = match extraction {
        BlockInventoryExtraction::Furnace => vec![2, 1],
        BlockInventoryExtraction::Brewing => vec![0, 1, 2, 3],
        BlockInventoryExtraction::Unsupported => Vec::new(),
        BlockInventoryExtraction::All => (0..slots).collect(),
    };
    Some(BlockInventoryPolicy { slots, bottom_slots, extraction })
}

pub(crate) fn derive_menu_inventory_transform(
    kind: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind,
    selection: &pumpkin_cluster::canonical_menu::MenuSelection,
    inputs: &[InventoryCell],
) -> Result<pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformOutput, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_cluster::canonical_menu::MenuSelection;
    use pumpkin_cluster::canonical_menu_transform::{MenuInventoryTransformKind as K, MenuInventoryTransformOutput};
    use pumpkin_data::data_component_impl::{BannerPatternLayer, BannerPatternsImpl, MapPostProcessingImpl, TrimImpl};
    use pumpkin_nbt::tag::NbtTag;
    if inputs.len() != kind.inputs() { return Err(ApplyFailure::InvalidAction); }
    let stacks = inputs.iter().map(super::cluster_invsee::item_stack_from_inventory_cell)
        .collect::<Result<Vec<_>, _>>().map_err(|_| ApplyFailure::InvalidAction)?;
    let (result, consumed) = match kind {
        K::Loom => {
            use pumpkin_inventory::loom_screen_handler::{get_dye_color, get_selectable_patterns, is_banner_item};
            let MenuSelection::LoomPattern(index) = selection else { return Err(ApplyFailure::InvalidAction); };
            if !is_banner_item(stacks[0].item) { return Err(ApplyFailure::InvalidAction); }
            let color = get_dye_color(&stacks[1]).ok_or(ApplyFailure::InvalidAction)?;
            let pattern = get_selectable_patterns(&stacks[2]).get(usize::from(*index)).cloned().ok_or(ApplyFailure::InvalidAction)?;
            let mut layers = stacks[0].get_data_component::<BannerPatternsImpl>().cloned().unwrap_or_default();
            if layers.layers.len() >= 6 { return Err(ApplyFailure::InvalidAction); }
            layers.layers.push(BannerPatternLayer { pattern, color });
            let mut result = stacks[0].clone(); result.item_count = 1; result.set_data_component(layers);
            (result, vec![1, 1, 0])
        }
        K::Smithing => {
            use pumpkin_data::recipes::{get_smithing_transform_recipe, get_smithing_trim_recipe, get_trim_material_for_item};
            if stacks.iter().any(ItemStack::is_empty) { return Err(ApplyFailure::InvalidAction); }
            let mut result = stacks[1].clone();
            if let Some(recipe) = get_smithing_transform_recipe(stacks[0].item, stacks[1].item, stacks[2].item) {
                result.item = Item::from_registry_key(recipe.result.id.strip_prefix("minecraft:").unwrap_or(recipe.result.id)).ok_or(ApplyFailure::InvalidAction)?;
                result.item_count = recipe.result.count;
            } else if let Some(recipe) = get_smithing_trim_recipe(stacks[0].item, stacks[1].item, stacks[2].item) {
                let material = get_trim_material_for_item(stacks[2].item).ok_or(ApplyFailure::InvalidAction)?;
                if result.get_data_component::<TrimImpl>().is_some_and(|trim|
                    matches!((&trim.material, &trim.pattern), (NbtTag::String(old_material), NbtTag::String(old_pattern))
                        if old_material.as_ref() == material && old_pattern.as_ref() == recipe.pattern)) { return Err(ApplyFailure::InvalidAction); }
                result.item_count = 1;
                result.set_data_component(TrimImpl { material: NbtTag::String(material.into()), pattern: NbtTag::String(recipe.pattern.into()) });
            } else { return Err(ApplyFailure::InvalidAction); }
            (result, vec![1, 1, 1])
        }
        K::Stonecutter => {
            let MenuSelection::Recipe(index) = selection else { return Err(ApplyFailure::InvalidAction); };
            if stacks[0].is_empty() { return Err(ApplyFailure::InvalidAction); }
            let recipe = pumpkin_data::recipes::RECIPES_STONECUTTING.iter().filter(|recipe| recipe.ingredient.match_item(stacks[0].item))
                .nth(*index as usize).ok_or(ApplyFailure::InvalidAction)?;
            let item = Item::from_registry_key(recipe.result.id.strip_prefix("minecraft:").unwrap_or(recipe.result.id)).ok_or(ApplyFailure::InvalidAction)?;
            (ItemStack::new(recipe.result.count, item), vec![1])
        }
        K::Cartography => {
            if stacks.iter().any(ItemStack::is_empty) || !pumpkin_inventory::cartography_table_screen_handler::is_map_item(&stacks[0]) {
                return Err(ApplyFailure::InvalidAction);
            }
            let mut result = stacks[0].clone(); result.item_count = 1;
            if stacks[1].item.id == Item::PAPER.id { result.set_data_component(MapPostProcessingImpl::SCALE); }
            else if stacks[1].item.id == Item::GLASS_PANE.id { result.set_data_component(MapPostProcessingImpl::LOCK); }
            else if stacks[1].item.id == Item::MAP.id { result.item_count = 2; }
            else { return Err(ApplyFailure::InvalidAction); }
            (result, vec![1, 1])
        }
    };
    if result.is_empty() || result.item_count > result.get_max_stack_size() { return Err(ApplyFailure::InvalidAction); }
    Ok(MenuInventoryTransformOutput { consumed, output: super::cluster_invsee::inventory_cell_from_item_stack(
        pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, kind.inputs() as u16), &result)
        .map_err(|_| ApplyFailure::InvalidAction)? })
}

pub(super) struct FrozenMenuTransform {
    pub intent: pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformIntent,
    quick_move: bool,
    companion_count: Option<u8>,
}

impl FrozenMenuTransform {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), pumpkin_cluster::canonical_action::ApplyFailure> {
        inventory_metadata_resource_usage(&(&self.intent, self.quick_move, self.companion_count))
    }
    pub(super) fn materialize<S: pumpkin_cluster::canonical_action::ActionState>(&self, state: &S,
        capture: pumpkin_cluster::canonical_inventory::InventoryActionCapture)
        -> Result<pumpkin_cluster::canonical_action::Action, pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ApplyFailure, Cell, TruthView, Value};
        let mut intent = self.intent.clone();
        let Some(Cell { value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(menu)), .. }) =
            state.read(TruthView::Local, intent.menu_target) else { return Err(ApplyFailure::Condition(intent.menu_target)); };
        if menu.session != intent.menu.session || menu.window_kind != intent.menu.window_kind || menu.source != intent.menu.source {
            return Err(ApplyFailure::InvalidAction);
        }
        intent.menu = menu;
        let values = intent.targets().into_iter().map(|target| state.read(TruthView::Local, target)
            .map(|cell| (target, cell)).ok_or(ApplyFailure::Condition(target))).collect::<Result<BTreeMap<_, _>, _>>()?;
        if self.quick_move {
            intent.repetitions = (1..=intent.repetitions).rev().find(|count| {
                let mut candidate = intent.clone(); candidate.repetitions = *count; candidate.derive(state, &values).is_ok()
            }).ok_or(ApplyFailure::InvalidAction)?;
        }
        if let Some(count) = self.companion_count {
            let inputs = intent.inputs.iter().map(|binding| match &values[&binding.target].value {
                Value::Inventory(cell) => Ok(cell.clone()), _ => Err(ApplyFailure::Inventory(binding.target)),
            }).collect::<Result<Vec<_>, _>>()?;
            let output = state.menu_inventory_transform(intent.kind, &intent.menu.selection, &inputs)?;
            if output.output.count.checked_mul(intent.repetitions) != Some(count) { return Err(ApplyFailure::InvalidAction); }
        }
        intent.capture_action(state, capture)
    }
}

pub(super) fn menu_transform_kind(window: u16) -> Option<pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind> {
    use pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformKind as K;
    [K::Loom, K::Smithing, K::Stonecutter, K::Cartography].into_iter().find(|kind| kind.window_kind() == window)
}

pub(super) fn capture_menu_transform(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
) -> Result<(FrozenMenuTransform, pumpkin_cluster::canonical_inventory::InventoryActionCapture), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure, ClaimedAction, ChunkPart, Target, TruthView, Value};
    use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
    use pumpkin_cluster::inventory::{InventoryGesture, InventoryMenuKind, InvLoc, INV_CURSOR, INV_MAIN};
    let ClaimedAction::InventoryMenu(claim) = &proposal.claim else { return Err(ApplyFailure::InvalidAction); };
    let kind = menu_transform_kind(claim.window_kind).ok_or(ApplyFailure::InvalidAction)?;
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid) || proposal.id.incarnation != lease.incarnation
        || proposal.id.family != 2 || claim.session.actor != lease.gid || proposal.screen_generation != claim.session.session
        || !original.validate_live_player_lease(lease) { return Err(ApplyFailure::InvalidAction); }
    let repetitions = match claim.kind { InventoryMenuKind::TakeDisplayedResult { repetitions } =>
        u8::try_from(repetitions).map_err(|_| ApplyFailure::InvalidAction)?, _ => return Err(ApplyFailure::InvalidAction) };
    let menu_target = Target::Chunk { key: claim.actor_chunk, part: ChunkPart::LivingProperty {
        actor: pumpkin_cluster::actor::ActorId::Player(lease.gid), property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None };
    let Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(menu)) =
        original.read(TruthView::Local, menu_target).ok_or(ApplyFailure::Condition(menu_target))?.value else { return Err(ApplyFailure::Type(menu_target)); };
    if menu.session != claim.session.session || menu.window_kind != claim.window_kind { return Err(ApplyFailure::InvalidAction); }
    let screen = original.inventory_source_screen(claim.session).ok_or(ApplyFailure::InvalidAction)?;
    if screen.sync_id != claim.sync_id || screen.window_kind != claim.window_kind { return Err(ApplyFailure::InvalidAction); }
    let entry = |index| screen.slots.iter().find(|slot| slot.screen_slot == index).map(|slot| slot.slot).ok_or(ApplyFailure::InvalidAction);
    let result = entry(kind.inputs() as u16)?;
    let player = super::cluster_world_inventory_actions::current_inventory_address_from_snapshot(original, InventoryTarget::Player(lease.gid)).ok_or(ApplyFailure::InvalidAction)?;
    let cursor = InventorySlotRef::new(player, InvLoc::new(INV_CURSOR, 0));
    let storage = (0..36).map(|slot| InventorySlotRef::new(player, InvLoc::new(INV_MAIN, slot))).collect::<Vec<_>>();
    let mut quick_move = false;
    let mut companion_count = None;
    let destinations = match claim.companion_gestures.as_deref() {
        None | Some([]) => vec![(cursor, 99)],
        Some([InventoryGesture::Transfer { source, destination, count, destination_limit }]) if *source == result => {
            companion_count = Some(*count); vec![(*destination, *destination_limit)]
        }
        Some([InventoryGesture::QuickMove { source, destinations }]) if *source == result => { quick_move = true; destinations.clone() }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if destinations.is_empty() || destinations.len() > 36 || destinations.iter().any(|(slot, limit)|
        *limit == 0 || *slot != cursor && !storage.contains(slot)) { return Err(ApplyFailure::InvalidAction); }
    let binding = |slot: InventorySlotRef, max_stack| -> Result<InventorySlotBinding, ApplyFailure> { Ok(InventorySlotBinding {
        slot, target: super::cluster_world_inventory_actions::binding_target_from_snapshot(original, Some(&screen), slot).ok_or(ApplyFailure::InvalidAction)?, max_stack }) };
    let intent = pumpkin_cluster::canonical_menu_transform::MenuInventoryTransformIntent { lease, menu_target, menu,
        viewer: capture_menu_viewer(original, lease, claim.actor_chunk, proposal.tick)?, kind,
        inputs: (0..kind.inputs()).map(|index| entry(index as u16).and_then(|slot| binding(slot, 99))).collect::<Result<Vec<_>, _>>()?,
        result: binding(result, 99)?, destinations: destinations.into_iter().map(|(slot, limit)| binding(slot, limit)).collect::<Result<Vec<_>, _>>()?, repetitions };
    if !intent.valid() { return Err(ApplyFailure::InvalidAction); }
    Ok((FrozenMenuTransform { intent, quick_move, companion_count }, InventoryActionCapture { id: proposal.id, tick: proposal.tick,
        actor_chunk: Some(claim.actor_chunk), player_leases: vec![lease], player_chunks: vec![(lease.key, claim.actor_chunk)], guards: Vec::new(), client_origin: proposal.client_origin, edit_authority: None }))
}

pub(super) fn refresh_menu_transform_preview(server: &super::Server, screen: &mut pumpkin_cluster::inventory::InventoryScreenSnapshot) {
    let Some(kind) = menu_transform_kind(screen.window_kind) else { return; };
    let result_index = kind.inputs() as u16;
    let Some(location) = screen.slots.iter().find(|slot| slot.screen_slot == result_index).map(|slot| slot.cell.loc) else { return; };
    let states = server.cluster_actor_living.load();
    let preview = states.get(&pumpkin_cluster::actor::ActorId::Player(screen.session.actor)).filter(|living|
        living.menu.session == screen.session.session && living.menu.window_kind == screen.window_kind).and_then(|living| {
        let inputs = (0..kind.inputs()).map(|index| screen.slots.iter().find(|slot| usize::from(slot.screen_slot) == index)
            .map(|slot| slot.cell.clone())).collect::<Option<Vec<_>>>()?;
        derive_menu_inventory_transform(kind, &living.menu.selection, &inputs).ok().map(|result| result.output)
    }).unwrap_or_else(|| InventoryCell::empty(location));
    if let Some(result) = screen.slots.iter_mut().find(|slot| slot.screen_slot == result_index) { result.cell = preview; }
}

pub(crate) fn virtual_block_menu_layout(state: pumpkin_data::BlockStateId) -> Option<pumpkin_cluster::canonical_menu::MenuVirtualInventoryLayout> {
    use pumpkin_cluster::canonical_menu::MenuVirtualInventoryLayout as L;
    Some(match state.to_block().name { "crafting_table" => L::Crafting, "loom" => L::Loom,
        "smithing_table" => L::Smithing, "stonecutter" => L::Stonecutter, "cartography_table" => L::Cartography,
        _ => return None })
}

pub(crate) fn virtual_block_menu_open(
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    session: u64,
    sync_id: u8,
    viewer: pumpkin_cluster::item_contact::ContactObservation,
    block: pumpkin_cluster::canonical_menu::MenuBlockContact,
    viewer_inventory: &InventoryStateSnapshot,
) -> Result<(pumpkin_cluster::inventory::InventoryScreenSnapshot, super::cluster_invsee::ClusterScreenPresentation,
    pumpkin_cluster::canonical_menu::MenuSessionIntent), InventoryApplyError> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::canonical_menu::{MenuSelection, MenuSessionIntent, MenuSessionOperation, MenuSessionState, MenuSource, MenuVirtualInventoryAdmission, MenuVirtualInventoryLayout as L};
    use pumpkin_cluster::inventory::{InventoryScreenSession, InventoryScreenSlot, InventoryScreenSnapshot, InvLoc, INV_MAIN, INV_CURSOR};
    let layout = virtual_block_menu_layout(pumpkin_data::BlockStateId::new(block.state).ok_or(InventoryApplyError::Rejected)?)
        .ok_or(InventoryApplyError::Rejected)?;
    if !matches!(viewer_inventory.target, InventoryTarget::LeasedPlayer { key, player, incarnation }
        if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation) { return Err(InventoryApplyError::Rejected); }
    super::cluster_invsee::validate_imported_inventory_snapshots(&[viewer_inventory.clone()])?;
    let (window_type, title) = match layout { L::Crafting => (pumpkin_data::screen::WindowType::Crafting, "Crafting"),
        L::Loom => (pumpkin_data::screen::WindowType::Loom, "Loom"), L::Smithing => (pumpkin_data::screen::WindowType::Smithing, "Upgrade Gear"),
        L::Stonecutter => (pumpkin_data::screen::WindowType::Stonecutter, "Stonecutter"), L::Cartography => (pumpkin_data::screen::WindowType::CartographyTable, "Cartography Table"),
        _ => return Err(InventoryApplyError::Rejected) };
    let admission = MenuVirtualInventoryAdmission { lease, session, layout: layout.clone() };
    let virtual_inventory = admission.snapshot();
    let slots = layout.slots();
    let state = MenuSessionState { session, window_kind: layout.window_kind(), source: MenuSource::Blocks(vec![block.clone()]),
        selection: MenuSelection::None, ..Default::default() };
    let intent = MenuSessionIntent { lease, target: Target::Chunk { key: viewer.chunk, part: ChunkPart::LivingProperty {
        actor: pumpkin_cluster::actor::ActorId::Player(lease.gid), property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None },
        operation: MenuSessionOperation::Open { state, viewer: viewer.clone() }, virtual_inventory: Some(layout) };
    if !intent.valid() { return Err(InventoryApplyError::Rejected); }
    let mut screen_slots = Vec::with_capacity(usize::from(slots) + 36);
    let mut stacks = Vec::with_capacity(usize::from(slots) + 36);
    for screen_slot in 0..slots + 36 {
        let (snapshot, location) = if screen_slot < slots { (&virtual_inventory, InvLoc::new(INV_MAIN, screen_slot)) }
            else { let index = screen_slot - slots; (viewer_inventory, InvLoc::new(INV_MAIN, if index < 27 { index + 9 } else { index - 27 })) };
        let cell = snapshot.cells.iter().find(|cell| cell.loc == location).cloned().unwrap_or_else(|| InventoryCell::empty(location));
        stacks.push(super::cluster_invsee::item_stack_from_inventory_cell(&cell)?);
        screen_slots.push(InventoryScreenSlot { screen_slot, slot: InventorySlotRef::new(snapshot.target, location), cell, limit: 99 });
    }
    let location = InvLoc::new(INV_CURSOR, 0);
    let cursor = viewer_inventory.cells.iter().find(|cell| cell.loc == location).cloned().unwrap_or_else(|| InventoryCell::empty(location));
    let mut affected = vec![viewer.chunk, block.chunk]; affected.sort_unstable(); affected.dedup();
    Ok((InventoryScreenSnapshot { session: InventoryScreenSession { actor: lease.gid, session }, sync_id, state_id: 0,
        window_kind: window_type as u16, container_slots: slots, slots: screen_slots, cursor, affected,
        preview: None, preview_incarnation: None, preview_lease: None, preview_authority: None },
        super::cluster_invsee::ClusterScreenPresentation { title: pumpkin_util::text::TextComponent::text(title), window_type, stacks }, intent))
}

pub(super) fn inventory_metadata_resource_usage<T: serde::Serialize>(metadata: &T)
    -> Result<(usize, usize), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    Ok((pumpkin_cluster::logical_records::count(metadata)?, postcard::experimental::serialized_size(metadata).map_err(|_| ApplyFailure::ResourceLimit)?))
}

pub(super) fn derive_beacon_selection(
    bytes: &[u8],
    payment: &InventoryCell,
    primary: Option<u16>,
    secondary: Option<u16>,
) -> Result<(Vec<u8>, InventoryCell), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_data::tag::Taggable;
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(payment).map_err(|_| ApplyFailure::InvalidAction)?;
    if payment.count != 1 || !stack.item.has_tag(&pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS) {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let mut data = pumpkin_nbt::Nbt::read_unnamed(&mut reader).map_err(|_| ApplyFailure::InvalidAction)?;
    if cursor.position() != bytes.len() as u64 || data.clone().write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)? != bytes {
        return Err(ApplyFailure::InvalidAction);
    }
    let levels = data.root_tag.get_int("Levels").ok_or(ApplyFailure::InvalidAction)?;
    if !(0..=4).contains(&levels) || primary.is_some_and(|effect| !matches!((effect, levels),
        (1 | 3, 1..=4) | (11 | 8, 2..=4) | (5, 3..=4)))
        || secondary.is_some_and(|effect| levels != 4 || effect != 10 && Some(effect) != primary) {
        return Err(ApplyFailure::InvalidAction);
    }
    for (field, effect) in [("primary_effect", primary), ("secondary_effect", secondary)] {
        if let Some(effect) = effect { data.root_tag.put_int(field, i32::from(effect)); }
        else { data.root_tag.child_tags.remove(field); }
    }
    Ok((data.write_unnamed_canonical().map_err(|_| ApplyFailure::InvalidAction)?.to_vec(), InventoryCell::empty(payment.loc)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::inventory::{InvLoc, INV_MAIN};

    #[test]
    fn beacon_selection_consumes_one_payment_and_enforces_actual_level() {
        let mut data = pumpkin_nbt::compound::NbtCompound::new();
        data.put_int("Levels", 1);
        let before = pumpkin_nbt::Nbt::new(String::new(), data).write_unnamed_canonical().unwrap().to_vec();
        let payment = super::super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), &ItemStack::new(1, &Item::IRON_INGOT)).unwrap();
        let (bytes, after) = derive_beacon_selection(&before, &payment, Some(1), None).unwrap();
        assert!(after.is_empty());
        assert_ne!(before, bytes);
        assert!(derive_beacon_selection(&before, &payment, Some(5), None).is_err());
        assert!(derive_beacon_selection(&before, &payment, Some(1), Some(10)).is_err());
        let invalid = super::super::cluster_invsee::inventory_cell_from_item_stack(payment.loc, &ItemStack::new(1, &Item::DIRT)).unwrap();
        assert!(derive_beacon_selection(&before, &invalid, Some(1), None).is_err());
    }
}
