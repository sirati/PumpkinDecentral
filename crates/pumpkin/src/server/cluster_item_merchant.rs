use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
use pumpkin_cluster::canonical_menu::{MenuSessionOperation, MenuVirtualInventoryAdmission, MenuVirtualInventoryLayout};
use pumpkin_cluster::canonical_merchant::MerchantOffersState;
use pumpkin_cluster::inventory::{InventorySlotRef, InventoryStateSnapshot, InventoryTarget};
use pumpkin_cluster::item_action::{ActorItemInteraction, ItemActionKind};
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::time::TickStamp;

pub(super) struct FrozenMerchantOpen {
    lease: LivePlayerLease,
    viewer: ContactObservation,
    merchant: ContactObservation,
    kind: u16,
    offers_target: Target,
    offers: MerchantOffersState,
    viewer_inventory: InventoryStateSnapshot,
    bindings: Vec<InventorySlotBinding>,
    full_source_slot: u64,
    bound: Option<(ActionRef, MenuVirtualInventoryAdmission)>,
}

impl FrozenMerchantOpen {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(self.lease, &self.viewer, &self.merchant, self.kind, self.offers_target, &self.offers, &self.viewer_inventory, &self.bindings, self.full_source_slot, &self.bound))
    }

    pub(super) fn capture(state: &impl ActionState, proposal: &ActionProposal,
        metadata: &super::cluster_item_use::ItemUseSourceMetadata) -> Result<Option<Self>, ApplyFailure> {
        let ClaimedAction::ItemUse(claim) = &proposal.claim else { return Ok(None); };
        if !matches!(claim.intent.kind, ItemActionKind::UseOnActor { interaction: ActorItemInteraction::Interact, .. }) {
            return Ok(None);
        }
        let merchant = claim.target_observation.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let Some(Cell { value: Value::Entity(body), .. }) = state.read(TruthView::Local, merchant.target()) else {
            return Ok(None);
        };
        if body.kind != pumpkin_data::entity::EntityType::VILLAGER.id
            && body.kind != pumpkin_data::entity::EntityType::WANDERING_TRADER.id { return Ok(None); }
        let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
        let viewer = metadata.source_observation.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let Some(Cell { value: Value::Living(living), .. }) = state.read(TruthView::Local, viewer.target()) else {
            return Err(ApplyFailure::Condition(viewer.target()));
        };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        if avatar.game_mode == 3 || living.combat.health_milli == 0 { return Err(ApplyFailure::InvalidAction); }
        let (_, offers_target) = super::cluster_owned_mob_initial::merchant_open_target(state, merchant, body.kind)?;
        let Some(Cell { value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MerchantOffers(Some(offers))), .. })
            = state.read(TruthView::Local, offers_target) else { return Err(ApplyFailure::Type(offers_target)); };
        let inventory_target = InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation };
        let mut bindings = Vec::new();
        let mut cells = Vec::new();
        for loc in pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots() {
            let target = Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: loc.inv, slot: loc.slot }) };
            let Some(Cell { value: Value::Inventory(cell), .. }) = state.read(TruthView::Local, target) else {
                return Err(ApplyFailure::Inventory(target));
            };
            bindings.push(InventorySlotBinding { slot: InventorySlotRef::new(inventory_target, loc), target,
                max_stack: if loc.inv == pumpkin_cluster::inventory::INV_ARMOR || loc.inv == pumpkin_cluster::inventory::INV_SELECTED { 1 } else { 99 } });
            cells.push(cell);
        }
        Ok(Some(Self { lease, viewer: viewer.clone(), merchant: merchant.clone(), kind: body.kind,
            offers_target, offers, viewer_inventory: InventoryStateSnapshot { target: inventory_target, revision: 0, cells }, bindings,
            full_source_slot: super::cluster_world_rules::slot_for_tick(proposal.tick).ok_or(ApplyFailure::Capacity)?, bound: None }))
    }

    pub(super) fn bindings(&self) -> &[InventorySlotBinding] { &self.bindings }

    pub(super) fn full_source_slot(&self) -> u64 { self.full_source_slot }

    pub(super) fn targets(&self) -> Vec<Target> {
        let mut targets = vec![self.viewer.target(), self.merchant.target(), self.offers_target];
        targets.extend(self.bindings.iter().map(|binding| binding.target));
        for property in [pumpkin_cluster::living_property::LivingProperty::MenuSession,
            pumpkin_cluster::living_property::LivingProperty::Health, pumpkin_cluster::living_property::LivingProperty::GameMode] {
            targets.push(Target::Chunk { key: self.viewer.chunk, part: ChunkPart::LivingProperty {
                actor: pumpkin_cluster::actor::ActorId::Player(self.lease.gid), property }, inventory: None });
        }
        if let Some(admission) = self.admission() { targets.extend(admission.entries().into_iter().map(|(target, _)| target)); }
        targets.sort_unstable(); targets.dedup(); targets
    }

    pub(super) fn bind(&mut self, id: ActionId, tick: TickStamp) -> Result<(), ApplyFailure> {
        let reference = ActionRef { id, tick };
        if id.actor != self.viewer.actor() || id.incarnation != self.lease.incarnation || tick != self.viewer.simulation_tick {
            return Err(ApplyFailure::InvalidAction);
        }
        let identity = pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(reference, self.full_source_slot)
            .ok_or(ApplyFailure::InvalidAction)?;
        let admission = MenuVirtualInventoryAdmission { lease: self.lease, session: identity.session, layout: MenuVirtualInventoryLayout::Merchant };
        if self.bound.as_ref().is_some_and(|prior| prior != &(reference, admission.clone())) { return Err(ApplyFailure::InvalidAction); }
        self.bound = Some((reference, admission));
        Ok(())
    }

    pub(super) fn admission(&self) -> Option<MenuVirtualInventoryAdmission> { self.bound.as_ref().map(|(_, admission)| admission.clone()) }

    fn screen(&self, reference: ActionRef) -> Result<(pumpkin_cluster::inventory::InventoryScreenSnapshot,
        super::cluster_invsee::ClusterScreenPresentation, pumpkin_cluster::canonical_menu::MenuSessionIntent,
        pumpkin_cluster::inventory::InventoryMerchantPresentation), ApplyFailure> {
        let Some((bound, admission)) = &self.bound else { return Err(ApplyFailure::InvalidAction); };
        if *bound != reference { return Err(ApplyFailure::InvalidAction); }
        let identity = pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(reference, self.full_source_slot)
            .ok_or(ApplyFailure::InvalidAction)?;
        super::cluster_inventory_merchant::merchant_menu_screen(self.lease, identity.session, identity.sync_id,
            self.viewer.clone(), self.merchant.clone(), self.kind, &self.viewer_inventory, &admission.snapshot(), &self.offers)
            .map_err(|_| ApplyFailure::InvalidAction)
    }

    pub(super) fn materialize(&self, state: &impl ActionState, id: ActionId, tick: TickStamp, client_origin: ClientOrigin)
        -> Result<Action, ApplyFailure> {
        let (_, _, intent, _) = self.screen(ActionRef { id, tick })?;
        let mut action = intent.capture_action(state, InventoryActionCapture { edit_authority: None, id, tick,
            actor_chunk: Some(self.viewer.chunk), player_leases: vec![self.lease], player_chunks: vec![(self.lease.key, self.viewer.chunk)],
            guards: Vec::new(), client_origin })?;
        for property in [pumpkin_cluster::living_property::LivingProperty::Health, pumpkin_cluster::living_property::LivingProperty::GameMode] {
            let target = Target::Chunk { key: self.viewer.chunk, part: ChunkPart::LivingProperty { actor: self.viewer.actor(), property }, inventory: None };
            let expected = if property == pumpkin_cluster::living_property::LivingProperty::Health { Expected::LivingUnsignedAtLeast(1) }
                else { Expected::Exact(state.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?) };
            action.conditions.push(Condition { target, expected }); action.counterfactual_conditions.push(target);
        }
        action.validate()?;
        Ok(action)
    }

    pub(super) fn presentation(&self, reference: ActionRef) -> Result<super::cluster_invsee::CanonicalInventoryOpenPresentation, ApplyFailure> {
        let (screen, presentation, intent, merchant) = self.screen(reference)?;
        let MenuSessionOperation::Open { state, .. } = intent.operation else { return Err(ApplyFailure::InvalidAction); };
        Ok(super::cluster_invsee::CanonicalInventoryOpenPresentation { reference, lease: self.lease,
            screen, source: state.source, menu: None, presentation: Some(presentation), merchant: Some(merchant) })
    }
}
