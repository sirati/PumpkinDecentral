use pumpkin_cluster::canonical_merchant::{MerchantOfferState, MerchantOffersState, MerchantScheme};
use pumpkin_cluster::inventory::{InventoryApplyError, InvLoc, INV_MAIN};
use pumpkin_data::item_stack::ItemStack;
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_nbt::tag::NbtTag;

pub(crate) fn merchant_offers_from_nbt(root: &NbtCompound, scheme: MerchantScheme)
    -> Result<Option<MerchantOffersState>, InventoryApplyError> {
    let Some(raw_offers) = root.get("Offers") else { return Ok(None); };
    let NbtTag::Compound(recipes) = raw_offers else { return Err(InventoryApplyError::Rejected); };
    if recipes.child_tags.keys().any(|key| key.as_ref() != "Recipes") { return Err(InventoryApplyError::Rejected); }
    let entries = recipes.get_list("Recipes").ok_or(InventoryApplyError::Rejected)?;
    if entries.len() > 256 { return Err(InventoryApplyError::Rejected); }
    let mut offers = Vec::with_capacity(entries.len());
    for entry in entries {
        let NbtTag::Compound(entry) = entry else { return Err(InventoryApplyError::Rejected); };
        if entry.child_tags.keys().any(|key| !matches!(key.as_ref(), "buy" | "buyB" | "sell" | "uses" | "maxUses" | "xp"
            | "rewardExp" | "specialPrice" | "priceMultiplier" | "demand")) { return Err(InventoryApplyError::Rejected); }
        if !matches!(entry.get_byte("rewardExp"), Some(0 | 1)) { return Err(InventoryApplyError::Rejected); }
        let read = |name| {
            let data = entry.get_compound(name).ok_or(InventoryApplyError::Rejected)?;
            if data.child_tags.keys().any(|key| !matches!(key.as_ref(), "id" | "count" | "components"))
                || !data.get_int("count").is_some_and(|count| (0..=99).contains(&count))
                || data.get("components").is_some_and(|value| !matches!(value, NbtTag::Compound(_))) {
                return Err(InventoryApplyError::Rejected);
            }
            let stack = ItemStack::read_item_stack(data).ok_or(InventoryApplyError::Rejected)?;
            if stack.item_count > stack.get_max_stack_size() { return Err(InventoryApplyError::Rejected); }
            Ok(stack)
        };
        let cost_a = read("buy")?;
        let output = read("sell")?;
        let cost_b = entry.get("buyB").map(|_| read("buyB")).transpose()?.filter(|stack| !stack.is_empty());
        let unsigned = |name| entry.get_int(name).and_then(|value| u32::try_from(value).ok())
            .ok_or(InventoryApplyError::Rejected);
        offers.push(MerchantOfferState {
            cost_a: super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), &cost_a)?,
            cost_b: cost_b.map(|stack| super::cluster_invsee::inventory_cell_from_item_stack(
                InvLoc::new(INV_MAIN, 1), &stack)).transpose()?,
            output: super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 2), &output)?,
            cost_a_limit: cost_a.get_max_stack_size(), uses: unsigned("uses")?,
            max_uses: unsigned("maxUses")?, experience: unsigned("xp")?,
            reward_experience: entry.get_bool("rewardExp").ok_or(InventoryApplyError::Rejected)?,
            special_price: entry.get_int("specialPrice").ok_or(InventoryApplyError::Rejected)?,
            price_multiplier: entry.get_float("priceMultiplier").ok_or(InventoryApplyError::Rejected)?.to_bits(),
            demand: entry.get_int("demand").ok_or(InventoryApplyError::Rejected)?,
        });
    }
    let level = match root.get("VillagerData") {
        None => 1,
        Some(NbtTag::Compound(data)) => match data.get("level") { None => 1, Some(NbtTag::Int(level)) => *level,
            _ => return Err(InventoryApplyError::Rejected) },
        _ => return Err(InventoryApplyError::Rejected),
    };
    let experience = match root.get("Xp") { None => 0, Some(NbtTag::Int(value)) => *value,
        _ => return Err(InventoryApplyError::Rejected) };
    let state = MerchantOffersState { version: 1, scheme,
        level: u8::try_from(level).map_err(|_| InventoryApplyError::Rejected)?,
        experience: u32::try_from(experience)
            .map_err(|_| InventoryApplyError::Rejected)?, offers };
    if !state.valid() { return Err(InventoryApplyError::Rejected); }
    Ok(Some(state))
}

pub(crate) fn merchant_offers_to_nbt(
    root: &mut NbtCompound,
    state: &MerchantOffersState,
) -> Result<(), InventoryApplyError> {
    if !state.valid() { return Err(InventoryApplyError::Rejected); }
    let mut entries = Vec::with_capacity(state.offers.len());
    for offer in &state.offers {
        let mut entry = NbtCompound::new();
        for (name, cell) in [("buy", Some(&offer.cost_a)), ("buyB", offer.cost_b.as_ref()), ("sell", Some(&offer.output))] {
            let mut stack = NbtCompound::new();
            if let Some(cell) = cell {
                super::cluster_invsee::item_stack_from_inventory_cell(cell)?.write_item_stack(&mut stack);
            } else {
                ItemStack::EMPTY.write_item_stack(&mut stack);
            }
            entry.put_compound(name, stack);
        }
        entry.put_int("uses", offer.uses as i32);
        entry.put_int("maxUses", offer.max_uses as i32);
        entry.put_int("xp", offer.experience as i32);
        entry.put_bool("rewardExp", offer.reward_experience);
        entry.put_int("specialPrice", offer.special_price);
        entry.put_float("priceMultiplier", f32::from_bits(offer.price_multiplier));
        entry.put_int("demand", offer.demand);
        entries.push(NbtTag::Compound(entry));
    }
    let mut offers = root.get_compound("Offers").cloned().unwrap_or_default();
    offers.put_list("Recipes", entries);
    let mut villager = root.get_compound("VillagerData").cloned().unwrap_or_default();
    villager.put_int("level", i32::from(state.level));
    root.put_compound("Offers", offers);
    if state.scheme == MerchantScheme::Villager { root.put_compound("VillagerData", villager); }
    root.put_int("Xp", state.experience as i32);
    Ok(())
}

pub(super) struct FrozenMerchantMetadata {
    pub intent: pumpkin_cluster::canonical_merchant::MerchantTradeIntent,
    pub capture: pumpkin_cluster::canonical_inventory::InventoryActionCapture,
    primary: pumpkin_cluster::realm_completion::PeerGeneration,
    spatial: pumpkin_cluster::entity_body::EntitySpatial,
    claimed_offer: Option<u32>,
    companion_count: Option<u8>,
    quick_move: bool,
}

impl FrozenMerchantMetadata {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), pumpkin_cluster::canonical_action::ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(&self.intent, self.primary, &self.spatial,
            self.claimed_offer, self.companion_count, self.quick_move,
            (self.capture.id, self.capture.tick, self.capture.actor_chunk, &self.capture.player_leases, &self.capture.player_chunks,
                &self.capture.guards, &self.capture.client_origin, &self.capture.edit_authority)))
    }
    pub(super) fn bind_identity(&mut self, id: pumpkin_cluster::canonical_action::ActionId,
        allocator: &mut pumpkin_cluster::entity_origin::EntityOriginAllocator)
        -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, ChunkPart, Target};
        self.capture.id = id;
        if self.intent.repetitions == 0 { self.intent.rewards.clear(); return Ok(()); }
        let reference = ActionRef { tick: self.capture.tick, id };
        let identities = allocator.reserve(allocator.source(), reference, usize::from(self.intent.repetitions))
            .map_err(|_| ApplyFailure::Capacity)?;
        self.intent.rewards = identities.iter().map(|identity| {
            let template = super::cluster_item_birth::reserved_experience_body(reference, identity,
                self.primary, self.spatial.clone(), 3).map_err(|_| ApplyFailure::InvalidAction)?;
            Ok(pumpkin_cluster::canonical_merchant::MerchantTradeReward {
                target: Target::Chunk { key: self.intent.menu.merchant.chunk,
                    part: ChunkPart::Entity(identity.id), inventory: None }, template })
        }).collect::<Result<Vec<_>, ApplyFailure>>()?;
        Ok(())
    }

    pub(super) fn materialize<S: pumpkin_cluster::canonical_action::ActionState>(&self, view: &S)
        -> Result<pumpkin_cluster::canonical_action::Action, pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, TruthView, Value};
        use pumpkin_cluster::living_property::LivingPropertyValue;
        let mut intent = self.intent.clone();
        let menu = view.read(TruthView::Local, intent.menu_target).ok_or(ApplyFailure::Condition(intent.menu_target))?;
        let Value::LivingProperty(LivingPropertyValue::MenuSession(menu)) = menu.value
            else { return Err(ApplyFailure::Type(intent.menu_target)); };
        let offer = if self.intent.operation == pumpkin_cluster::canonical_merchant::MerchantOperation::SelectOffer {
            self.claimed_offer.ok_or(ApplyFailure::InvalidAction)?
        } else {
            let pumpkin_cluster::canonical_menu::MenuSelection::TradeOffer(offer) = menu.selection
                else { return Err(ApplyFailure::InvalidAction); };
            offer
        };
        if self.claimed_offer.is_some_and(|claimed| claimed != offer) { return Err(ApplyFailure::InvalidAction); }
        intent.menu.selected_offer = offer;
        let offers_target = intent.menu.offers_target();
        let offers = view.read(TruthView::Local, offers_target).ok_or(ApplyFailure::Condition(offers_target))?;
        let Value::LivingProperty(LivingPropertyValue::MerchantOffers(Some(offers))) = offers.value
            else { return Err(ApplyFailure::Type(offers_target)); };
        let offer = offers.offers.get(offer as usize).ok_or(ApplyFailure::InvalidAction)?;
        intent.output_stack_limit = super::cluster_invsee::item_stack_from_inventory_cell(&offer.output)
            .map_err(|_| ApplyFailure::InvalidAction)?.get_max_stack_size();
        let reference = ActionRef { tick: self.capture.tick, id: self.capture.id };
        if self.quick_move {
            let values = intent.targets().into_iter().filter_map(|target|
                view.read(TruthView::Local, target).map(|cell| (target, cell))).collect();
            let maximum = intent.repetitions;
            let rewards = intent.rewards.clone();
            intent.repetitions = 1;
            intent.rewards.truncate(1);
            intent.derive(view, reference, &values)?;
            for repetitions in 2..=maximum {
                intent.repetitions = repetitions;
                intent.rewards = rewards[..usize::from(repetitions)].to_vec();
                if intent.derive(view, reference, &values).is_err() {
                    intent.repetitions -= 1;
                    intent.rewards.truncate(usize::from(intent.repetitions));
                    break;
                }
            }
        }
        if self.companion_count.is_some_and(|count| u16::from(offer.output.count)
            .checked_mul(intent.repetitions) != Some(u16::from(count))) { return Err(ApplyFailure::InvalidAction); }
        intent.capture_action(view, self.capture.clone())
    }
}

pub(super) fn capture_merchant_proposal(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    executors: &std::collections::BTreeMap<pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::realm_completion::PeerGeneration>,
) -> Result<FrozenMerchantMetadata, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure, ChunkPart, ClaimedAction, Target, TruthView, Value};
    use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
    use pumpkin_cluster::canonical_merchant::{MerchantMenuWitness, MerchantTradeIntent, MerchantOperation};
    use pumpkin_cluster::inventory::{INV_CURSOR, InventoryGesture, InventoryMenuKind, InventorySlotRef, InventoryTarget};
    use pumpkin_cluster::living_property::LivingPropertyValue;
    let ClaimedAction::InventoryMenu(claim) = &proposal.claim else { return Err(ApplyFailure::InvalidAction); };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation || proposal.id.family != 2
        || claim.session.actor != lease.gid || proposal.screen_generation != claim.session.session
        || claim.window_kind != pumpkin_data::screen::WindowType::Merchant as u16
        || !original.validate_live_player_lease(lease) { return Err(ApplyFailure::InvalidAction); }
    let menu_target = Target::Chunk { key: claim.actor_chunk, part: ChunkPart::LivingProperty {
        actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
        property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None };
    let menu = original.read(TruthView::Local, menu_target).ok_or(ApplyFailure::Condition(menu_target))?;
    let Value::LivingProperty(LivingPropertyValue::MenuSession(menu)) = menu.value else { return Err(ApplyFailure::Type(menu_target)); };
    if menu.session != claim.session.session { return Err(ApplyFailure::InvalidAction); }
    let pumpkin_cluster::canonical_menu::MenuSource::Merchant { contact, kind } = menu.source
        else { return Err(ApplyFailure::InvalidAction); };
    if kind != pumpkin_data::entity::EntityType::VILLAGER.id && kind != pumpkin_data::entity::EntityType::WANDERING_TRADER.id {
        return Err(ApplyFailure::InvalidAction);
    }
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } = contact.identity.clone()
        else { return Err(ApplyFailure::InvalidAction); };
    let merchant_chunk = original.captured_entity_bodies().find_map(|(chunk, body)| (body.id == id).then_some(chunk)).ok_or(ApplyFailure::Condition(contact.target()))?;
    let merchant_target = Target::Chunk { key: merchant_chunk,
        part: ChunkPart::Entity(id), inventory: None };
    let body = original.read(TruthView::Local, merchant_target).ok_or(ApplyFailure::Condition(merchant_target))?;
    let Value::Entity(body) = body.value else { return Err(ApplyFailure::Type(contact.target())); };
    if body.kind != kind || !matches!(&contact.identity, pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth }
        if *id == body.id && *birth == body.birth) { return Err(ApplyFailure::InvalidAction); }
    let shape = original.actor_collision_shape(merchant_target, &Value::Entity((body.clone()).into()))?;
    let half_width = (f64::from_bits(shape.offsets[1][0]) - f64::from_bits(shape.offsets[0][0])) * 0.5;
    let height = f64::from_bits(shape.offsets[1][1]) - f64::from_bits(shape.offsets[0][1]);
    let merchant = pumpkin_cluster::item_contact::ContactObservation::entity(&body, merchant_chunk, &body.spatial,
        half_width, height, proposal.tick,
        pumpkin_cluster::identity::PlayerSeq(0)).ok_or(ApplyFailure::InvalidAction)?;
    let viewer = super::cluster_inventory_menu::capture_menu_viewer(original, lease, claim.actor_chunk, proposal.tick)?;
    let (claimed_offer, repetitions, operation) = match claim.kind {
        InventoryMenuKind::SelectTrade { offer } => (Some(offer), 0, MerchantOperation::SelectOffer),
        InventoryMenuKind::ExecuteTrade { offer, repetitions } => (Some(offer), repetitions, MerchantOperation::Execute),
        InventoryMenuKind::TakeDisplayedResult { repetitions } => (None, repetitions, MerchantOperation::Execute),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if operation == MerchantOperation::Execute && repetitions == 0 || repetitions > 64 { return Err(ApplyFailure::InvalidAction); }
    let screen = original.inventory_source_screen(claim.session).ok_or(ApplyFailure::InvalidAction)?;
    if screen.sync_id != claim.sync_id || screen.window_kind != claim.window_kind { return Err(ApplyFailure::InvalidAction); }
    let entry = |index| screen.slots.iter().find(|slot| slot.screen_slot == index)
        .map(|slot| slot.slot).ok_or(ApplyFailure::InvalidAction);
    let cost_a = entry(0)?;
    let cost_b = entry(1)?;
    let result = entry(2)?;
    let player = super::cluster_world_inventory_actions::current_inventory_address_from_snapshot(original, InventoryTarget::Player(lease.gid)).ok_or(ApplyFailure::InvalidAction)?;
    let cursor = InventorySlotRef::new(player, InvLoc::new(INV_CURSOR, 0));
    let storage = (9..36).chain(0..9).map(|slot| InventorySlotRef::new(player, InvLoc::new(INV_MAIN, slot))).collect::<Vec<_>>();
    let mut quick_move = false;
    let mut companion_count = None;
    if operation == MerchantOperation::SelectOffer && claim.companion_gestures.as_ref().is_some_and(|gestures| !gestures.is_empty()) {
        return Err(ApplyFailure::InvalidAction);
    }
    let destinations = match claim.companion_gestures.as_deref() {
        None | Some([]) if operation == MerchantOperation::SelectOffer => storage.clone(),
        None | Some([]) => vec![cursor],
        Some([InventoryGesture::Transfer { source, destination, count, destination_limit }])
            if *source == result && *destination_limit > 0 => { companion_count = Some(*count); vec![*destination] }
        Some([InventoryGesture::QuickMove { source, destinations }]) if *source == result => {
            quick_move = true;
            destinations.iter().map(|(slot, _)| *slot).collect()
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if destinations.is_empty() || destinations.len() > 36 || destinations.iter().any(|slot| *slot != cursor && !storage.contains(slot))
        || destinations.iter().copied().collect::<std::collections::BTreeSet<_>>().len() != destinations.len() {
        return Err(ApplyFailure::InvalidAction);
    }
    let binding = |slot| Ok(InventorySlotBinding { slot,
        target: super::cluster_world_inventory_actions::binding_target_from_snapshot(original, Some(&screen), slot)
            .ok_or(ApplyFailure::InvalidAction)?, max_stack: 99 });
    let intent = MerchantTradeIntent { operation, menu: MerchantMenuWitness { session: claim.session, lease, viewer,
        merchant, merchant_kind: kind, selected_offer: 0 }, menu_target,
        cost_a: binding(cost_a)?, cost_b: binding(cost_b)?, result: binding(result)?,
        output_destinations: destinations.into_iter().map(binding).collect::<Result<Vec<_>, ApplyFailure>>()?,
        output_stack_limit: 1, repetitions, rewards: Vec::new() };
    let primary = executors.get(&intent.menu.merchant.chunk).copied().ok_or(ApplyFailure::InvalidAction)?;
    let spatial = pumpkin_cluster::entity_body::EntitySpatial { world: body.spatial.world,
        position: body.spatial.position, velocity_micro: [0; 3], yaw: body.spatial.yaw,
        pitch: body.spatial.pitch, on_ground: body.spatial.on_ground };
    Ok(FrozenMerchantMetadata { intent, primary, spatial, claimed_offer, companion_count, quick_move,
        capture: InventoryActionCapture { id: proposal.id, tick: proposal.tick, actor_chunk: Some(claim.actor_chunk),
            player_leases: vec![lease], player_chunks: vec![(lease.key, claim.actor_chunk)], guards: Vec::new(),
            client_origin: proposal.client_origin, edit_authority: None } })
}

pub(crate) fn merchant_presentation(
    merchant: pumpkin_cluster::protocol::GlobalEntityId,
    state: &MerchantOffersState,
) -> Result<pumpkin_cluster::inventory::InventoryMerchantPresentation, InventoryApplyError> {
    use pumpkin_cluster::inventory::{InventoryMerchantPresentation, InventoryTradeOfferPresentation};
    if !state.valid() { return Err(InventoryApplyError::Rejected); }
    Ok(InventoryMerchantPresentation { merchant, tier: state.level, experience: state.experience,
        regular_villager: state.scheme == MerchantScheme::Villager,
        can_restock: state.scheme == MerchantScheme::Villager, last_trader: None, last_trader_name: None,
        new_trading_ui: true, economy_trade: true,
        offers: state.offers.iter().map(|offer| InventoryTradeOfferPresentation {
            first_cost: offer.cost_a.clone(), second_cost: offer.cost_b.clone(), result: offer.output.clone(),
            uses: offer.uses, max_uses: offer.max_uses, reward_experience: offer.reward_experience,
            merchant_experience: offer.experience, special_price: offer.special_price,
            demand: offer.demand, price_multiplier_bits: offer.price_multiplier,
        }).collect() })
}

pub(super) fn refresh_merchant_preview(
    screen: &mut pumpkin_cluster::inventory::InventoryScreenSnapshot,
    offers: &MerchantOffersState,
    selected: u32,
) {
    let Some(result_loc) = screen.slots.iter().find(|slot| slot.screen_slot == 2).map(|slot| slot.cell.loc) else { return; };
    let output = offers.offers.get(selected as usize).and_then(|offer| {
        if offer.uses >= offer.max_uses { return None; }
        let first = offer.adjusted_cost_a()?;
        for (index, required) in [(0, Some(&first)), (1, offer.cost_b.as_ref())] {
            let actual = &screen.slots.iter().find(|slot| slot.screen_slot == index)?.cell;
            if let Some(required) = required {
                if actual.item != required.item || actual.nbt != required.nbt || actual.count < required.count { return None; }
            } else if !actual.is_empty() { return None; }
        }
        let mut output = offer.output.clone();
        output.loc = result_loc;
        Some(output)
    }).unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(result_loc));
    if let Some(result) = screen.slots.iter_mut().find(|slot| slot.screen_slot == 2) { result.cell = output; }
}

pub(crate) fn merchant_menu_screen(
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    session: u64,
    sync_id: u8,
    viewer: pumpkin_cluster::item_contact::ContactObservation,
    merchant: pumpkin_cluster::item_contact::ContactObservation,
    kind: u16,
    viewer_inventory: &pumpkin_cluster::inventory::InventoryStateSnapshot,
    inputs: &pumpkin_cluster::inventory::InventoryStateSnapshot,
    offers: &MerchantOffersState,
) -> Result<(pumpkin_cluster::inventory::InventoryScreenSnapshot,
    super::cluster_invsee::ClusterScreenPresentation,
    pumpkin_cluster::canonical_menu::MenuSessionIntent,
    pumpkin_cluster::inventory::InventoryMerchantPresentation), InventoryApplyError> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::canonical_menu::{MenuSelection, MenuSessionIntent, MenuSessionOperation, MenuSessionState, MenuSource};
    use pumpkin_cluster::inventory::{INV_CURSOR, InventoryCell, InventoryScreenSession, InventoryScreenSlot,
        InventoryScreenSnapshot, InventorySlotRef, InventoryTarget};
    if !matches!(viewer_inventory.target, InventoryTarget::LeasedPlayer { key, player, incarnation }
        if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation)
        || !matches!(inputs.target, InventoryTarget::LeasedVirtual { key, player, incarnation, session: actual, kind, .. }
            if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation && actual == session
                && kind == pumpkin_data::screen::WindowType::Merchant as u16 + 1)
        || offers.offers.is_empty()
        || inputs.cells.iter().any(|cell| cell.loc.inv != INV_MAIN || cell.loc.slot >= 3)
        || !(kind == pumpkin_data::entity::EntityType::VILLAGER.id && offers.scheme == MerchantScheme::Villager
            || kind == pumpkin_data::entity::EntityType::WANDERING_TRADER.id && offers.scheme == MerchantScheme::WanderingTrader) {
        return Err(InventoryApplyError::Rejected);
    }
    super::cluster_invsee::validate_imported_inventory_snapshots(&[viewer_inventory.clone(), inputs.clone()])?;
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } = merchant.identity.clone()
        else { return Err(InventoryApplyError::Rejected); };
    let window_type = pumpkin_data::screen::WindowType::Merchant;
    let state = MenuSessionState { session, window_kind: window_type as u16,
        source: MenuSource::Merchant { contact: merchant.clone(), kind },
        selection: MenuSelection::TradeOffer(0), ..Default::default() };
    let intent = MenuSessionIntent { lease, virtual_inventory: Some(pumpkin_cluster::canonical_menu::MenuVirtualInventoryLayout::Merchant), target: Target::Chunk { key: viewer.chunk,
        part: ChunkPart::LivingProperty { actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None },
        operation: MenuSessionOperation::Open { state, viewer: viewer.clone() } };
    if !intent.valid() { return Err(InventoryApplyError::Rejected); }
    let mut slots = Vec::with_capacity(39);
    for index in 0..39 {
        let (snapshot, location) = if index < 3 { (inputs, InvLoc::new(INV_MAIN, index)) }
            else { let offset = index - 3; (viewer_inventory, InvLoc::new(INV_MAIN, if offset < 27 { offset + 9 } else { offset - 27 })) };
        let cell = snapshot.cells.iter().find(|cell| cell.loc == location).cloned().unwrap_or_else(|| InventoryCell::empty(location));
        slots.push(InventoryScreenSlot { screen_slot: index, slot: InventorySlotRef::new(snapshot.target, location), cell, limit: 99 });
    }
    let cursor_loc = InvLoc::new(INV_CURSOR, 0);
    let cursor = viewer_inventory.cells.iter().find(|cell| cell.loc == cursor_loc).cloned().unwrap_or_else(|| InventoryCell::empty(cursor_loc));
    let mut affected = vec![viewer.chunk, merchant.chunk];
    affected.sort_unstable(); affected.dedup();
    let mut screen = InventoryScreenSnapshot { session: InventoryScreenSession { actor: lease.gid, session },
        sync_id, state_id: 0, window_kind: window_type as u16, container_slots: 3, slots, cursor, affected,
        preview: None, preview_incarnation: None, preview_lease: None, preview_authority: None };
    refresh_merchant_preview(&mut screen, offers, 0);
    let stacks = screen.slots.iter().map(|slot| super::cluster_invsee::item_stack_from_inventory_cell(&slot.cell))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((screen, super::cluster_invsee::ClusterScreenPresentation { title: pumpkin_util::text::TextComponent::text(
        if offers.scheme == MerchantScheme::Villager { "Villager" } else { "Wandering Trader" }), window_type, stacks },
        intent, merchant_presentation(id, offers)?))
}

pub(crate) fn initial_merchant_offers(
    scheme: MerchantScheme,
    villager: Option<(pumpkin_data::villager::VillagerProfession, pumpkin_data::villager::VillagerType, u8)>,
    uuid: uuid::Uuid,
) -> Result<Option<MerchantOffersState>, InventoryApplyError> {
    use pumpkin_data::villager::{VillagerProfession, VillagerTradeModifier};
    use rand::{RngExt, SeedableRng, seq::IndexedRandom};
    use crate::entity::passive::villager::{apply_potion, apply_random_dye, apply_random_stew_effect,
        enchanted_book_offer_items, enchant_trade_item};
    let mut rng = rand::rngs::StdRng::seed_from_u64(xxhash_rust::xxh64::xxh64(uuid.as_bytes(), 0x4d45524348414e54));
    let mut generated = Vec::new();
    let level = if scheme == MerchantScheme::WanderingTrader {
        use pumpkin_data::villager::{TRADES_WANDERING_TRADER_BUYING, TRADES_WANDERING_TRADER_COMMON, TRADES_WANDERING_TRADER_UNCOMMON};
        for (pool, count) in [(TRADES_WANDERING_TRADER_BUYING, 2), (TRADES_WANDERING_TRADER_UNCOMMON, 2), (TRADES_WANDERING_TRADER_COMMON, 5)] {
            crate::entity::passive::wandering_trader::add_offers_from_trade_set(&mut generated, pool, count, &mut rng);
        }
        1
    } else {
        let Some((profession, biome, level)) = villager else { return Ok(None); };
        if matches!(profession, VillagerProfession::None | VillagerProfession::Nitwit) { return Ok(None); }
        if !(1..=5).contains(&level) { return Err(InventoryApplyError::Rejected); }
        let pool = profession.trade_set(i32::from(level)).ok_or(InventoryApplyError::Rejected)?;
        let mut remaining = pool.trades.iter().collect::<Vec<_>>();
        while generated.len() < usize::try_from(pool.amount).map_err(|_| InventoryApplyError::Rejected)? && !remaining.is_empty() {
            let index = rng.random_range(0..remaining.len());
            let trade = remaining.remove(index);
            if !trade.allowed_types.is_empty() && !trade.allowed_types.contains(&biome) { continue; }
            let mut cost_a = ItemStack::new(trade.wants.count as u8, trade.wants.item);
            let mut output = ItemStack::new(trade.gives.count as u8, trade.gives.item);
            let mut cost_b = trade.wants_b.as_ref().map(|item| ItemStack::new(item.count as u8, item.item));
            match trade.modifier {
                VillagerTradeModifier::None => {},
                VillagerTradeModifier::EnchantRandomly => {
                    let Some(items) = enchanted_book_offer_items(&mut rng) else { continue; };
                    (cost_a, output, cost_b) = items;
                }
                VillagerTradeModifier::EnchantWithLevels { min, max } => {
                    let Some((enchanted, extra)) = enchant_trade_item(&mut rng, trade.gives.item, min, max) else { continue; };
                    output = enchanted;
                    let count = i32::from(cost_a.item_count).saturating_add(extra).clamp(0, i32::from(cost_a.get_max_stack_size()));
                    if count == 0 { continue; }
                    cost_a.set_count(count as u8);
                }
                VillagerTradeModifier::ExplorationMap { .. } => return Err(InventoryApplyError::TargetUnavailable),
                VillagerTradeModifier::RandomDyes => apply_random_dye(&mut rng, &mut output),
                VillagerTradeModifier::RandomPotion => {
                    let Some(potion) = pumpkin_data::tag::Potion::MINECRAFT_TRADEABLE.0.choose(&mut rng) else { continue; };
                    apply_potion(&mut output, potion);
                }
                VillagerTradeModifier::SuspiciousStew => apply_random_stew_effect(&mut rng, &mut output),
                VillagerTradeModifier::Potion(potion) => apply_potion(&mut output, potion),
            }
            use std::borrow::Cow;
            use pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer;
            generated.push(pumpkin_protocol::java::client::play::MerchantOffer {
                base_cost_a: ItemStackSerializer(Cow::Owned(cost_a)), output: ItemStackSerializer(Cow::Owned(output)),
                cost_b: cost_b.map(|stack| ItemStackSerializer(Cow::Owned(stack))), reward_exp: true,
                uses: 0, max_uses: trade.max_uses, xp: trade.xp, special_price: 0,
                price_multiplier: trade.price_multiplier, demand: 0,
            });
        }
        level
    };
    let offers = generated.into_iter().map(|offer| {
        let cost_a = &*offer.base_cost_a.0;
        Ok(MerchantOfferState {
            cost_a: super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), cost_a)?,
            cost_b: offer.cost_b.map(|stack| super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 1), &stack.0)).transpose()?,
            output: super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 2), &offer.output.0)?,
            cost_a_limit: cost_a.get_max_stack_size(), uses: 0,
            max_uses: u32::try_from(offer.max_uses).map_err(|_| InventoryApplyError::Rejected)?,
            experience: u32::try_from(offer.xp).map_err(|_| InventoryApplyError::Rejected)?, reward_experience: true,
            special_price: 0, price_multiplier: offer.price_multiplier.to_bits(), demand: 0,
        })
    }).collect::<Result<Vec<_>, InventoryApplyError>>()?;
    let state = MerchantOffersState { version: 1, scheme, level, experience: 0, offers };
    if !state.valid() { return Err(InventoryApplyError::Rejected); }
    Ok(Some(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (NbtCompound, MerchantOffersState) {
        let state = initial_merchant_offers(MerchantScheme::WanderingTrader, None, uuid::Uuid::from_u128(17)).unwrap().unwrap();
        let mut root = NbtCompound::new();
        merchant_offers_to_nbt(&mut root, &state).unwrap();
        (root, state)
    }

    #[test]
    fn merchant_genesis_round_trip_retains_actual_generated_offers() {
        let (root, expected) = fixture();
        assert_eq!(merchant_offers_from_nbt(&root, MerchantScheme::WanderingTrader).unwrap(), Some(expected));
    }

    #[test]
    fn merchant_genesis_rejects_unknown_and_malformed_optional_offer_fields() {
        let (root, _) = fixture();
        for malformed in [NbtTag::Int(1), NbtTag::Compound(NbtCompound::new())] {
            let mut modified = root.clone();
            let NbtTag::Compound(offers) = modified.child_tags.get_mut("Offers").unwrap() else { panic!() };
            let NbtTag::List(entries) = offers.child_tags.get_mut("Recipes").unwrap() else { panic!() };
            let NbtTag::Compound(first) = &mut entries[0] else { panic!() };
            first.put("buyB", malformed);
            assert!(merchant_offers_from_nbt(&modified, MerchantScheme::WanderingTrader).is_err());
        }
        let mut modified = root.clone();
        let NbtTag::Compound(offers) = modified.child_tags.get_mut("Offers").unwrap() else { panic!() };
        let NbtTag::List(entries) = offers.child_tags.get_mut("Recipes").unwrap() else { panic!() };
        let NbtTag::Compound(first) = &mut entries[0] else { panic!() };
        first.put_int("unrepresented", 1);
        assert!(merchant_offers_from_nbt(&modified, MerchantScheme::WanderingTrader).is_err());
        let mut modified = root;
        let NbtTag::Compound(offers) = modified.child_tags.get_mut("Offers").unwrap() else { panic!() };
        offers.put_int("unrepresented", 1);
        assert!(merchant_offers_from_nbt(&modified, MerchantScheme::WanderingTrader).is_err());
    }
}
