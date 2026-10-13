use pumpkin_cluster::canonical_action::{
    Action, ActionProposal, ActionRef, ActionState, ApplyFailure, ChunkPart, ClaimedAction, Target,
    TruthView, Value,
};
use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
use pumpkin_cluster::canonical_inventory_close::{InventoryCloseDrop, InventoryCloseIntent};
use pumpkin_cluster::entity_origin::EntityOriginAllocator;
use pumpkin_cluster::inventory::{INV_CURSOR, INV_MAIN, InvLoc, InventorySlotRef, InventoryTarget};

pub(super) struct FrozenCloseMetadata {
    pub intent: InventoryCloseIntent,
    pub capture: InventoryActionCapture,
    pub notification: Option<CanonicalCloseNotification>,
    primary: pumpkin_cluster::realm_completion::PeerGeneration,
    spatial: pumpkin_cluster::entity_body::EntitySpatial,
    placeholder: pumpkin_cluster::inventory::InventoryCell,
}

pub(super) struct CanonicalCloseNotification {
    pub permit: tokio::sync::mpsc::OwnedPermit<super::cluster_invsee::InvseeCommand>,
    pub player: std::sync::Arc<crate::entity::player::Player>,
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub session: pumpkin_cluster::inventory::InventoryScreenSession,
    pub window_kind: u16,
}

impl FrozenCloseMetadata {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(&self.intent, self.primary, &self.spatial, &self.placeholder,
            self.capture.id, self.capture.tick, self.capture.actor_chunk, &self.capture.player_leases, &self.capture.player_chunks,
            &self.capture.guards, &self.capture.client_origin, &self.capture.edit_authority))
    }
    pub(super) fn bind_identity(
        &mut self,
        id: pumpkin_cluster::canonical_action::ActionId,
        allocator: &mut EntityOriginAllocator,
    ) -> Result<(), ApplyFailure> {
        if id.actor != pumpkin_cluster::actor::ActorId::Player(self.intent.lease.gid)
            || id.incarnation != self.intent.lease.incarnation
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.capture.id = id;
        let reference = ActionRef {
            tick: self.capture.tick,
            id,
        };
        let reservations = allocator
            .reserve(allocator.source(), reference, self.intent.sources.len())
            .map_err(|_| ApplyFailure::Capacity)?;
        let chunk = self
            .capture
            .actor_chunk
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut overflow = Vec::new();
        for (slot, identity) in self.intent.sources.iter().zip(reservations.iter()) {
            let body = super::cluster_item_birth::reserved_item_body(
                reference,
                identity,
                self.primary,
                self.spatial.clone(),
                &self.placeholder,
                40,
            )
            .map_err(|_| ApplyFailure::InvalidAction)?;
            overflow.push(InventoryCloseDrop {
                source: slot.target,
                target: Target::Chunk {
                    key: chunk,
                    part: ChunkPart::Entity(identity.id),
                    inventory: None,
                },
                template: body,
            });
        }
        self.intent.overflow = overflow;
        Ok(())
    }
    pub(super) fn materialize<S: ActionState>(&self, view: &S) -> Result<Action, ApplyFailure> {
        self.intent.capture_action(view, self.capture.clone())
    }
}

pub(super) fn capture_close_proposal(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    proposal: &ActionProposal,
    executors: &std::collections::BTreeMap<pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::realm_completion::PeerGeneration>,
) -> Result<FrozenCloseMetadata, ApplyFailure> {
    let ClaimedAction::InventoryClose(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation
        || proposal.id.family != 2
        || !original.validate_live_player_lease(lease)
        || claim.token == 0
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let menu = Target::Chunk { key: claim.actor_chunk, part: ChunkPart::LivingProperty {
        actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
        property: pumpkin_cluster::living_property::LivingProperty::MenuSession,
    }, inventory: None };
    let current_menu = original.read(TruthView::Local, menu).ok_or(ApplyFailure::Condition(menu))?;
    let Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(current_menu)) = current_menu.value
        else { return Err(ApplyFailure::Type(menu)); };
    let active_session = pumpkin_cluster::inventory::InventoryScreenSession { actor: lease.gid, session: current_menu.session };
    let session = claim.session.unwrap_or(active_session);
    let closed = current_menu.closed_sessions.iter().find(|closed| closed.session == session.session);
    let acknowledged = session != active_session && closed.is_some();
    if session.actor != lease.gid || session != active_session && !acknowledged {
        return Err(ApplyFailure::InvalidAction);
    }
    let screen = original.inventory_source_screen(if acknowledged { active_session } else { session })
        .ok_or(ApplyFailure::InvalidAction)?;
    let window_kind = if acknowledged { closed.unwrap().window_kind } else { screen.window_kind };
    let notification_player = super::cluster_invsee::find_inventory_player(state.server, lease.gid)
        .ok_or(ApplyFailure::InvalidAction)?;
    if super::cluster_world_owner::player_action_lease(state.server, &notification_player)
        != Some(lease)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let notification_permit = super::cluster_invsee::reserve_inventory_close_notification(
        state.server,
        lease,
        session,
        window_kind,
    )
    .map_err(|_| ApplyFailure::Capacity)?;
    let actor = original.player_projection(lease.gid)
        .ok_or(ApplyFailure::InvalidAction)?;
    if actor.chunk != claim.actor_chunk {
        return Err(ApplyFailure::InvalidAction);
    }
    let player = super::cluster_world_inventory_actions::current_inventory_address_from_snapshot(original, InventoryTarget::Player(lease.gid))
        .ok_or(ApplyFailure::InvalidAction)?;
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
    let mut sources = vec![binding(InventorySlotRef::new(
        player,
        InvLoc::new(INV_CURSOR, 0),
    ))?];
    let mut results = Vec::new();
    for slot in &screen.slots {
        if slot.screen_slot >= screen.container_slots || !slot.slot.target.is_screen_virtual() {
            continue;
        }
        if super::cluster_world_inventory::close_result_slot(
            screen.window_kind,
            slot.screen_slot,
            session.session == 0,
        ) {
            results.push(binding(slot.slot)?);
        } else {
            sources.push(binding(slot.slot)?);
        }
    }
    let destinations = (0..36)
        .map(|slot| binding(InventorySlotRef::new(player, InvLoc::new(INV_MAIN, slot))))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = executors.get(&claim.actor_chunk).copied()
        .filter(|generation| generation.generation != 0)
        .ok_or(ApplyFailure::InvalidAction)?;
    let frame = actor.frame.effective;
    let spatial = pumpkin_cluster::entity_body::EntitySpatial {
        world: actor.world,
        position: frame.position.map(f64::to_bits),
        velocity_micro: [0; 3],
        yaw: frame.yaw.to_bits(),
        pitch: frame.pitch.to_bits(),
        on_ground: frame.on_ground,
    };
    let placeholder = super::cluster_invsee::inventory_cell_from_item_stack(
        InvLoc::new(INV_MAIN, 0),
        &pumpkin_data::item_stack::ItemStack::new(1, &pumpkin_data::item::Item::STONE),
    )
    .map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(FrozenCloseMetadata {
        primary,
        spatial,
        placeholder,
        notification: Some(CanonicalCloseNotification {
            permit: notification_permit,
            player: notification_player,
            lease,
            session,
            window_kind,
        }),
        intent: InventoryCloseIntent {
            lease,
            session,
            token: claim.token,
            departure: claim.session.is_none(),
            menu,
            window_kind,
            sources,
            destinations,
            results,
            overflow: Vec::new(),
        },
        capture: InventoryActionCapture {
            edit_authority: None,
            id: proposal.id,
            tick: proposal.tick,
            actor_chunk: Some(claim.actor_chunk),
            player_leases: vec![lease],
            player_chunks: vec![(lease.key, claim.actor_chunk)],
            guards: Vec::new(),
            client_origin: proposal.client_origin,
        },
    })
}

#[derive(Clone)]
pub(super) struct FrozenDeathClose {
    pub intent: InventoryCloseIntent,
    primary: pumpkin_cluster::realm_completion::PeerGeneration,
    spatial: pumpkin_cluster::entity_body::EntitySpatial,
    placeholder: pumpkin_cluster::inventory::InventoryCell,
    notification: std::sync::Arc<crossbeam::queue::ArrayQueue<CanonicalCloseNotification>>,
    reference: Option<ActionRef>,
}

impl FrozenDeathClose {
    pub(super) fn birth_count(&self) -> usize { self.intent.sources.len() }

    pub(super) fn bind_identities(
        &mut self,
        reference: ActionRef,
        identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    ) -> Result<(), ApplyFailure> {
        if identities.len() != self.birth_count()
            || reference.id.actor != pumpkin_cluster::actor::ActorId::Player(self.intent.lease.gid)
            || reference.id.incarnation != self.intent.lease.incarnation { return Err(ApplyFailure::InvalidAction); }
        self.intent.token = (1_u64 << 63) | (u64::from(reference.id.family) << 48)
            | (u64::from(reference.id.sequence) << 16) | u64::from(reference.tick.0);
        self.intent.overflow = self.intent.sources.iter().zip(identities).map(|(slot, identity)| {
            let template = super::cluster_item_birth::reserved_item_body(reference, identity,
                self.primary, self.spatial.clone(), &self.placeholder, 40)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            let chunk = pumpkin_cluster::protocol::ChunkKey::new(template.spatial.world,
                (f64::from_bits(template.spatial.position[0]).floor() as i32).div_euclid(16),
                (f64::from_bits(template.spatial.position[2]).floor() as i32).div_euclid(16));
            Ok(InventoryCloseDrop { source: slot.target, target: Target::Chunk {
                key: chunk, part: ChunkPart::Entity(identity.id), inventory: None }, template })
        }).collect::<Result<Vec<_>, ApplyFailure>>()?;
        self.reference = Some(reference);
        Ok(())
    }

    pub(super) fn take_notification(&self, reference: ActionRef) -> Option<CanonicalCloseNotification> {
        (reference.id.actor == pumpkin_cluster::actor::ActorId::Player(self.intent.lease.gid)
            && reference.id.incarnation == self.intent.lease.incarnation
            && self.reference.is_none_or(|bound| bound == reference))
            .then(|| self.notification.pop()).flatten()
    }
}

pub(super) fn capture_death_close_from_snapshot(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    chunk: pumpkin_cluster::protocol::ChunkKey,
    tick: pumpkin_cluster::time::TickStamp,
    executors: &std::collections::BTreeMap<pumpkin_cluster::protocol::ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration>,
) -> Result<FrozenDeathClose, ApplyFailure> {
    use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};
    if original.original_tick() != tick || !original.validate_live_player_lease(lease) { return Err(ApplyFailure::InvalidAction); }
    let menu_target = Target::Chunk { key: chunk, part: ChunkPart::LivingProperty {
        actor: pumpkin_cluster::actor::ActorId::Player(lease.gid), property: LivingProperty::MenuSession }, inventory: None };
    let Some(pumpkin_cluster::canonical_action::Cell { value: Value::LivingProperty(LivingPropertyValue::MenuSession(menu)), .. }) =
        original.read(TruthView::Local, menu_target) else { return Err(ApplyFailure::Condition(menu_target)); };
    let body_target = Target::Chunk { key: chunk, part: ChunkPart::Player(lease.gid), inventory: None };
    let Some(pumpkin_cluster::canonical_action::Cell { value: Value::Living(living), .. }) = original.read(TruthView::Local, body_target)
        else { return Err(ApplyFailure::Condition(body_target)); };
    let retained = menu.last_close.zip(living.combat.timing.death_cause).and_then(|(reference, cause)|
        state.inventory.retained_death_close(lease, reference, cause));
    let binding = |target, inventory, location| InventorySlotBinding {
        target, slot: InventorySlotRef::new(inventory, location), max_stack: 99,
    };
    let own = |inventory| matches!(inventory, InventoryTarget::LeasedPlayer { key, player, incarnation }
        if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation);
    let cursor = original.captured_inventory_targets().find_map(|(target, inventory, location)|
        (own(inventory) && location == InvLoc::new(INV_CURSOR, 0)).then_some(binding(target, inventory, location)))
        .ok_or(ApplyFailure::InvalidAction)?;
    let mut destinations = original.captured_inventory_targets().filter_map(|(target, inventory, location)|
        (own(inventory) && location.inv == INV_MAIN && location.slot < 36).then_some(binding(target, inventory, location))).collect::<Vec<_>>();
    destinations.sort_by_key(|binding| binding.slot.loc.slot);
    if destinations.len() != 36 { return Err(ApplyFailure::InvalidAction); }
    let mut intent = if let Some(mut retained) = retained {
        retained.menu = menu_target;
        for binding in retained.bindings() {
            if !original.available(TruthView::Local, binding.target) { return Err(ApplyFailure::Condition(binding.target)); }
        }
        retained
    } else {
        let result_slot = match menu.window_kind { 0 | 12 => Some(0), 8 | 15 | 19 | 23 => Some(2), 18 | 21 => Some(3), 24 => Some(1), _ => None };
        let mut sources = vec![cursor]; let mut results = Vec::new();
        let mut virtual_slots = 0;
        for (target, inventory, location) in original.captured_inventory_targets() {
            if matches!(inventory, InventoryTarget::LeasedVirtual { key, player, incarnation, session, kind, .. }
                if key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation
                    && session == menu.session && kind == menu.window_kind + 1) {
                if location.inv != INV_MAIN { return Err(ApplyFailure::InvalidAction); }
                virtual_slots += 1;
                let binding = binding(target, inventory, location);
                if Some(location.slot) == result_slot { results.push(binding); } else { sources.push(binding); }
            }
        }
        let expected_virtual = match menu.window_kind { 0 => Some(5), 12 => Some(10), 8 | 15 | 19 | 23 => Some(3),
            18 | 21 => Some(4), 24 | 13 => Some(2), 9 => Some(1), _ => None };
        if expected_virtual.is_some_and(|expected| virtual_slots != expected) { return Err(ApplyFailure::Condition(menu_target)); }
        InventoryCloseIntent { lease, session: pumpkin_cluster::inventory::InventoryScreenSession { actor: lease.gid, session: menu.session },
            token: 1, departure: false, menu: menu_target, window_kind: menu.window_kind, sources, destinations, results, overflow: Vec::new() }
    };
    intent.departure = false; intent.overflow.clear();
    let projection = original.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    if projection.chunk != chunk || projection.world != chunk.world { return Err(ApplyFailure::InvalidAction); }
    let frame = projection.frame.effective;
    let spatial = pumpkin_cluster::entity_body::EntitySpatial { world: chunk.world, position: frame.position.map(f64::to_bits),
        velocity_micro: [0; 3], yaw: frame.yaw.to_bits(), pitch: frame.pitch.to_bits(), on_ground: frame.on_ground };
    let drop_chunk = pumpkin_cluster::protocol::ChunkKey::new(chunk.world,
        (frame.position[0].floor() as i32).div_euclid(16), (frame.position[2].floor() as i32).div_euclid(16));
    let primary = executors.get(&drop_chunk).copied().ok_or(ApplyFailure::InvalidAction)?;
    let player = super::cluster_invsee::find_inventory_player(state.server, lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    if super::cluster_world_owner::player_action_lease(state.server, &player) != Some(lease) { return Err(ApplyFailure::InvalidAction); }
    let permit = super::cluster_invsee::reserve_inventory_close_notification(state.server, lease, intent.session, intent.window_kind)
        .map_err(|_| ApplyFailure::Capacity)?;
    let notification = std::sync::Arc::new(crossbeam::queue::ArrayQueue::new(1));
    notification.push(CanonicalCloseNotification { permit, player, lease, session: intent.session, window_kind: intent.window_kind })
        .map_err(|_| ApplyFailure::Capacity)?;
    let placeholder = super::cluster_invsee::inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0),
        &pumpkin_data::item_stack::ItemStack::new(1, &pumpkin_data::item::Item::STONE)).map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(FrozenDeathClose { intent, primary, spatial, placeholder, notification, reference: None })
}
