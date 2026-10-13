use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{
    Action, ActionClass, ActionKind, ActionProposal, ActionState, ApplyFailure, ClaimedAction,
    ClientOrigin, Condition, Expected, SemanticIntent, Target, TruthView, UndoProcedure, Value,
};
use pumpkin_cluster::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
use pumpkin_cluster::item_action::ItemActionKind;
use pumpkin_cluster::item_use::{ItemUseIntent, ItemUseOperation};
use pumpkin_cluster::time::TickStamp;

pub(super) type CapturedItemUseSource = (
    pumpkin_cluster::canonical_action::LivePlayerLease,
    InventorySlotBinding,
    pumpkin_cluster::canonical_action::Cell,
    Condition,
    ItemUseSourceMetadata,
);

#[derive(Clone)]
pub(super) struct ItemUseSourceMetadata {
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    source_target: pumpkin_cluster::inventory::InventoryTarget,
    pub source_observation: Option<pumpkin_cluster::item_contact::ContactObservation>,
    pub launch: Option<pumpkin_cluster::item_use::ItemLaunchReservation>,
}

impl ItemUseSourceMetadata {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(self.lease, self.source_target, &self.source_observation, &self.launch))
    }
}

pub(super) fn capture_source_metadata(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &ActionProposal,
) -> Result<ItemUseSourceMetadata, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if claim.intent.actor != proposal.id.actor
        || proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || claim.owner_install_generation == 0
        || original.player_install_generation(lease) != Some(claim.owner_install_generation)
        || claim.effective_frame.effective.actor != lease.gid
        || original.inventory_source_lease(lease.gid) != Some(lease)
        || !matches!(
            claim.intent.source.loc.inv,
            pumpkin_cluster::inventory::INV_MAIN | pumpkin_cluster::inventory::INV_OFFHAND
        )
    {
        return Err(ApplyFailure::InvalidAction);
    }
    if claim.intent.source.loc.inv == pumpkin_cluster::inventory::INV_OFFHAND
        && claim.intent.source.loc.slot != 0
        || claim.intent.source.loc.inv == pumpkin_cluster::inventory::INV_MAIN
            && claim.intent.source.loc.slot >= 9
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = Target::Player { key: lease.key, lease: Some(lease), inventory: Some(
        pumpkin_cluster::canonical_action::InventoryId::Player { inv: claim.intent.source.loc.inv, slot: claim.intent.source.loc.slot }) };
    let leased_root = pumpkin_cluster::inventory::InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation };
    if claim.intent.source.target != leased_root && claim.intent.source.target != pumpkin_cluster::inventory::InventoryTarget::Player(lease.gid) {
        return Err(ApplyFailure::Inventory(target));
    }
    let source_target = original.captured_inventory_targets().find_map(|(actual, root, loc)|
        (actual == target && loc == claim.intent.source.loc && (root == leased_root
            || root == pumpkin_cluster::inventory::InventoryTarget::Player(lease.gid))).then_some(root))
        .ok_or(ApplyFailure::Inventory(target))?;
    Ok(ItemUseSourceMetadata {
        lease,
        source_target,
        source_observation: None,
        launch: None,
    })
}

pub(super) fn capture_launch_metadata(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot, proposal: &ActionProposal,
    metadata: &mut ItemUseSourceMetadata, source: pumpkin_cluster::realm_completion::PeerGeneration,
) -> Result<(), ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else { return Err(ApplyFailure::InvalidAction); };
    if source.server != metadata.lease.gid.server { return Err(ApplyFailure::InvalidAction); }
    let (association, owner) = original.player_association(metadata.lease).ok_or(ApplyFailure::InvalidAction)?;
    if association != claim.intent.actor_chunk || owner != source { return Err(ApplyFailure::InvalidAction); }
    let projection = original.player_projection(metadata.lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    let target = Target::Chunk { key: association, part: pumpkin_cluster::canonical_action::ChunkPart::Player(metadata.lease.gid), inventory: None };
    let Some(pumpkin_cluster::canonical_action::Cell { value: Value::Living(living), .. }) = original.read(TruthView::Local, target)
        else { return Err(ApplyFailure::Type(target)); };
    let avatar = living.avatar.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if !living.belongs_to(pumpkin_cluster::actor::ActorId::Player(metadata.lease.gid)) || avatar.world != association.world { return Err(ApplyFailure::InvalidAction); }
    let pose = if projection.crawling { pumpkin_data::entity::EntityPose::Swimming }
        else if projection.sneaking { pumpkin_data::entity::EntityPose::Crouching }
        else { super::cluster_living_metadata::player_pose(&living) };
    let dimensions = super::cluster_living_metadata::player_dimensions(&living, pose);
    let height = f64::from(dimensions.height);
    let half_width = f64::from(dimensions.width) / 2.0;
    let eye_height = f64::from(dimensions.eye_height);
    let effective = projection.frame.effective;
    let shooter = pumpkin_cluster::item_contact::ContactObservation::player(metadata.lease, source,
        avatar.spatial_epoch, association, effective, half_width, height, proposal.tick)
        .ok_or(ApplyFailure::InvalidAction)?;
    metadata.source_observation = Some(shooter.clone());
    if !matches!(claim.intent.kind, ItemActionKind::Use { .. } | ItemActionKind::Release { .. }) { return Ok(()); }
    let mut ammunition = Vec::with_capacity(37);
    for (inv, slots) in [(pumpkin_cluster::inventory::INV_MAIN, 36_u16), (pumpkin_cluster::inventory::INV_OFFHAND, 1_u16)] {
        for slot in 0..slots {
            ammunition.push(InventorySlotBinding { slot: pumpkin_cluster::inventory::InventorySlotRef::new(metadata.source_target,
                pumpkin_cluster::inventory::InvLoc::new(inv, slot)), max_stack: 99,
                target: Target::Player { key: metadata.lease.key, lease: Some(metadata.lease),
                    inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player { inv, slot }) } });
        }
    }
    let position = effective.position;
    let spawn_chunk = pumpkin_cluster::protocol::ChunkKey::new(claim.intent.actor_chunk.world,
        (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    let launch = pumpkin_cluster::item_use::ItemLaunchReservation { identities: Vec::new(), shooter,
        eye_height: eye_height.to_bits(), yaw: effective.yaw.to_bits(), pitch: effective.pitch.to_bits(),
        velocity_micro: effective.velocity.map(|value| (value * 1_000_000.0).round() as i64),
        on_ground: effective.on_ground, spawn_chunk, ammunition };
    metadata.launch = Some(launch);
    Ok(())
}

pub(super) fn bind_launch_identity(metadata: &mut ItemUseSourceMetadata,
    id: pumpkin_cluster::canonical_action::ActionId, tick: TickStamp,
    allocator: &mut pumpkin_cluster::entity_origin::EntityOriginAllocator) -> Result<(), ApplyFailure> {
    let Some(launch) = metadata.launch.as_mut() else { return Ok(()); };
    if launch.shooter.actor() != id.actor || launch.shooter.simulation_tick != tick
        || launch.shooter.owner != allocator.source() { return Err(ApplyFailure::InvalidAction); }
    launch.identities = allocator.reserve(launch.shooter.owner,
        pumpkin_cluster::canonical_action::ActionRef { tick, id }, 3)
        .map_err(|_| ApplyFailure::Capacity)?.to_vec();
    if !launch.valid() { return Err(ApplyFailure::InvalidAction); }
    Ok(())
}

pub(super) fn normalize_source<S: ActionState>(
    owners: &S,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &ActionProposal,
) -> Result<CapturedItemUseSource, ApplyFailure> {
    normalize_captured_source(
        owners,
        proposal,
        &capture_source_metadata(original, proposal)?,
    )
}

pub(super) fn normalize_captured_source<S: ActionState>(
    owners: &S,
    proposal: &ActionProposal,
    metadata: &ItemUseSourceMetadata,
) -> Result<CapturedItemUseSource, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = metadata.lease;
    let selector = Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player {
            inv: pumpkin_cluster::inventory::INV_SELECTED,
            slot: 0,
        }),
    };
    let selected = owners
        .read(TruthView::Local, selector)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let Value::Inventory(selected_item) = &selected.value else {
        return Err(ApplyFailure::Inventory(selector));
    };
    if selected_item.count != 1
        || !selected_item.nbt.is_empty()
        || !(1..=9).contains(&selected_item.item)
    {
        return Err(ApplyFailure::Inventory(selector));
    }
    let mut slot = claim.intent.source;
    slot.target = metadata.source_target;
    if slot.loc.inv == pumpkin_cluster::inventory::INV_MAIN {
        slot.loc.slot = selected_item.item - 1;
    }
    let source = Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player {
            inv: slot.loc.inv,
            slot: slot.loc.slot,
        }),
    };
    let cell = owners
        .read(TruthView::Local, source)
        .ok_or(ApplyFailure::Inventory(source))?;
    let Value::Inventory(item) = &cell.value else {
        return Err(ApplyFailure::Inventory(source));
    };
    let binding = InventorySlotBinding {
        slot,
        target: source,
        max_stack: owners.inventory_limit(item, source)?,
    };
    Ok((
        lease,
        binding,
        cell,
        Condition {
            target: selector,
            expected: Expected::Exact(selected),
        },
        metadata.clone(),
    ))
}

pub(super) fn materialize<S: ActionState>(
    server: &std::sync::Arc<super::Server>,
    owners: &S,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &ActionProposal,
    tick: TickStamp,
) -> Result<Action, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let captured = normalize_source(owners, original, proposal)?;
    if matches!(claim.intent.kind, ItemActionKind::UseOnBlock { .. }) {
        return super::cluster_world_callbacks::materialize_use_on_block(
            server, owners, inventory, original, proposal, tick,
        );
    }
    materialize_captured(owners, proposal, tick, captured)
}

pub(super) fn materialize_captured<S: ActionState>(
    owners: &S,
    proposal: &ActionProposal,
    tick: TickStamp,
    captured: CapturedItemUseSource,
) -> Result<Action, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    if let Some(action) = super::cluster_vehicle_interaction::materialize(owners, proposal, tick, &captured)? {
        return Ok(action);
    }
    if let Some(action) = super::cluster_object_interaction::materialize(owners, proposal, tick, &captured)? {
        return Ok(action);
    }
    let (lease, binding, source_cell, selected_guard, metadata) = captured;
    let source = binding.target;
    let Value::Inventory(item) = &source_cell.value else {
        return Err(ApplyFailure::Inventory(source));
    };
    let equipment = InventoryEquipmentBinding::player(
        lease,
        claim.intent.actor_chunk,
        owners.inventory_equipment_attribute_ids(),
    );
    let (destinations, _, _) =
        super::cluster_world_inventory_actions::pickup_destinations(owners, lease, item)?;
    let operation = match claim.intent.kind {
        ItemActionKind::Use { .. } => ItemUseOperation::Start,
        ItemActionKind::Release { .. } => ItemUseOperation::Release,
        ItemActionKind::Complete => ItemUseOperation::Complete,
        ItemActionKind::UseOnActor { .. } => ItemUseOperation::Complete,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if item.count == 0 {
        return Err(ApplyFailure::Inventory(source));
    }
    if claim.intent.client_claim.item_in_hand.as_ref().is_some_and(|held| held.item != item.item || held.count != item.count || held.nbt != item.nbt)
        || claim.intent.client_claim.hotbar_slot.is_some_and(|slot| binding.slot.loc.inv == pumpkin_cluster::inventory::INV_MAIN
            && u16::from(slot) != binding.slot.loc.slot)
        || claim.intent.client_claim.clicked_block_state.is_some()
        || claim.intent.client_claim.changes.iter().any(|change| !matches!(change.target,
            pumpkin_cluster::item_action::ClaimedInventoryTarget::Container(slot) if slot == binding.slot)
            || change.before.item != item.item || change.before.count != item.count || change.before.nbt != item.nbt
            || change.after != change.before)
        || claim.intent.client_claim.changes.len() > 1 {
        return Err(ApplyFailure::Inventory(source));
    }
    let remainder_destinations = destinations
        .into_iter()
        .filter(|destination| {
            destination.target != source && destination.target != equipment.offhand
        })
        .filter_map(|destination| match destination.target {
            Target::Player {
                inventory:
                    Some(pumpkin_cluster::canonical_action::InventoryId::Player { inv, slot }),
                ..
            } => Some(InventorySlotBinding {
                target: destination.target,
                max_stack: 99,
                slot: pumpkin_cluster::inventory::InventorySlotRef::new(
                    claim.intent.source.target,
                    pumpkin_cluster::inventory::InvLoc::new(inv, slot),
                ),
            }),
            _ => None,
        })
        .collect();
    let intent = ItemUseIntent {
        vehicle_source: if matches!(claim.intent.kind, ItemActionKind::UseOnActor { .. }) {
            Some(metadata.source_observation.clone().ok_or(ApplyFailure::InvalidAction)?)
        } else { None },
        vehicle: if matches!(claim.intent.kind, ItemActionKind::UseOnActor { .. }) {
            Some(claim.target_observation.clone().ok_or(ApplyFailure::InvalidAction)?)
        } else { None },
        launch: metadata.launch,
        living: equipment.living,
        source: binding,
        operation,
        max_health_attribute: u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id),
        used_statistic: Some((
            pumpkin_data::statistic::StatisticCategory::Used as u16,
            u32::from(item.item),
        )),
        blocking_delay_ticks: if matches!(
            proposal.client_origin,
            ClientOrigin::BedrockLegacyTransaction(_) | ClientOrigin::BedrockStackRequest { .. }
        ) {
            0
        } else {
            5
        },
        remainder_destinations,
        equipment: vec![equipment],
    };
    let mut values = BTreeMap::new();
    for target in intent.targets() {
        match owners.read(TruthView::Local, target) {
            Some(cell) => { values.insert(target, cell); }
            None if intent.launch.as_ref().is_some_and(|launch| launch.identities.iter().any(|identity|
                target == (Target::Chunk { key: launch.spawn_chunk,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Entity(identity.id), inventory: None }))) => {}
            None => return Err(ApplyFailure::Condition(target)),
        }
    }
    let effects = intent.derive(owners, proposal.id, tick, &values)?;
    let mut presentation = Vec::new();
    if operation == ItemUseOperation::Complete && intent.vehicle.is_none() {
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
            .map_err(|_| ApplyFailure::Inventory(source))?;
        let consumable = stack
            .get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>()
            .ok_or(ApplyFailure::InvalidAction)?;
        let position = claim.effective_frame.effective.position;
        let position = pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32,
            y: position[1].floor() as i32,
            z: position[2].floor() as i32,
        };
        for effect in consumable.effects.iter() {
            if let pumpkin_data::data_component_impl::ConsumeEffect::PlaySound(sound) = effect {
                let event = match sound {
                    pumpkin_data::data_component_impl::IdOr::Id(sound) => {
                        pumpkin_cluster::world_presentation::WorldPresentationIntent::Sound {
                            chunk: claim.intent.actor_chunk,
                            position,
                            sound: *sound as u16,
                            category: pumpkin_data::sound::SoundCategory::Players as u8,
                            volume_milli: 1000,
                            pitch_milli: 1000,
                        }
                    }
                    pumpkin_data::data_component_impl::IdOr::Value(sound) => {
                        let range_milli = sound
                            .range
                            .map(|range| {
                                if !range.is_finite() || !(0.001..=128.0).contains(&range) {
                                    return Err(ApplyFailure::InvalidAction);
                                }
                                Ok((range * 1000.0).round() as u32)
                            })
                            .transpose()?;
                        pumpkin_cluster::world_presentation::WorldPresentationIntent::CustomSound {
                            chunk: claim.intent.actor_chunk,
                            position,
                            name: sound.sound_name.clone(),
                            range_milli,
                            category: pumpkin_data::sound::SoundCategory::Players as u8,
                            volume_milli: 1000,
                            pitch_milli: 1000,
                        }
                    }
                };
                if !event.valid() {
                    return Err(ApplyFailure::InvalidAction);
                }
                presentation.push(event);
            }
        }
    }
    let health_target =
        intent.property_target(pumpkin_cluster::living_property::LivingProperty::Health);
    let mut conditions = values
        .iter()
        .map(|(target, cell)| Condition {
            target: *target,
            expected: if *target == source {
                Expected::InventoryItem {
                    item: item.item,
                    nbt: item.nbt.clone(),
                    minimum: 1,
                }
            } else if *target == health_target {
                Expected::LivingUnsignedAtLeast(1)
            } else {
                Expected::Exact(cell.clone())
            },
        })
        .collect::<Vec<_>>();
    conditions.push(selected_guard);
    if let Some(vehicle) = &intent.vehicle {
        let source_observation = metadata.source_observation.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let ItemActionKind::UseOnActor { target, target_chunk, .. } = claim.intent.kind else { return Err(ApplyFailure::InvalidAction); };
        let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &vehicle.identity else { return Err(ApplyFailure::InvalidAction); };
        if target != pumpkin_cluster::actor::ActorId::Entity(*id) || target_chunk != vehicle.chunk
            || source_observation.identity != pumpkin_cluster::item_contact::ContactIdentity::Player(lease)
            || !source_observation.valid() || !vehicle.valid() || source_observation.world != vehicle.world
            || source_observation.position.iter().zip(vehicle.position).map(|(a,b)|
                (f64::from_bits(*a)-f64::from_bits(b)).powi(2)).sum::<f64>() > 36.0 {
            return Err(ApplyFailure::InvalidAction);
        }
        conditions.retain(|condition| condition.target != vehicle.target());
        conditions.push(Condition { target: vehicle.target(), expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: vehicle.owner } });
        conditions.push(Condition { target: vehicle.target(), expected: Expected::SpatialEpochEquals(vehicle.epoch) });
        conditions.push(Condition { target: source_observation.target(), expected: Expected::SpatialEpochEquals(source_observation.epoch) });
    }
    if let Some(launch) = &intent.launch {
        conditions.retain(|condition| condition.target != launch.shooter.target());
        conditions.push(Condition { target: launch.shooter.target(), expected: Expected::SpatialEpochEquals(launch.shooter.epoch) });
        for identity in &launch.identities {
            let target = Target::Chunk { key: launch.spawn_chunk,
                part: pumpkin_cluster::canonical_action::ChunkPart::Entity(identity.id), inventory: None };
            if values.contains_key(&target) { return Err(ApplyFailure::Condition(target)); }
            conditions.push(Condition { target, expected: Expected::Missing });
        }
    }
    let footprint = intent.targets().collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>();
    let mut stakeholders = footprint
        .iter()
        .map(|target| target.stakeholder())
        .collect::<Vec<_>>();
    stakeholders.sort_unstable();
    stakeholders.dedup();
    let action = Action {
        edit_authority: None,
        id: proposal.id,
        tick,
        class: ActionClass::Replayable,
        kind: ActionKind::Item,
        chunk_transition: false,
        presentation,
        actor_chunk: Some(claim.intent.actor_chunk),
        player_leases: vec![lease],
        player_chunks: vec![(lease.key, claim.intent.actor_chunk)],
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: footprint.clone(),
        counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::ItemUse(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: proposal.client_origin,
    };
    action.validate()?;
    Ok(action)
}

pub(super) fn reject(server: &super::Server, proposal: &ActionProposal) {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return;
    };
    let pumpkin_cluster::actor::ActorId::Player(gid) = proposal.id.actor else {
        return;
    };
    super::cluster_inventory_protocol::publish_item_action_origin(
        server,
        gid,
        &claim.intent.kind,
        super::cluster_inventory_protocol::canonical_item_origin(proposal.client_origin),
        false,
    );
}

pub(super) fn complete(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    observations: &[(
        pumpkin_cluster::canonical_action::LivePlayerLease,
        super::cluster_world_actor::ActorSpatialState,
        pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::canonical_action::SpatialEpoch,
    )],
    tick: TickStamp,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    drafts: &mut std::collections::HashMap<
        (pumpkin_cluster::actor::ActorId, u64, TickStamp),
        pumpkin_cluster::actor_action_scope::ActorActionDraft,
    >,
    capacity: usize,
) -> Vec<Action> {
    let mut output = Vec::new();
    for (lease, _, _, _) in observations {
        let Some(state) = original.player_projection(lease.gid) else { continue; };
        let Some((association, _)) = original.player_association(*lease) else { continue; };
        let chunk = &association;
        if output.len() == capacity {
            break;
        }
        let key = (
            pumpkin_cluster::actor::ActorId::Player(lease.gid),
            lease.incarnation,
            tick,
        );
        let draft = drafts.entry(key).or_insert_with(|| {
            pumpkin_cluster::actor_action_scope::ActorActionDraft::new(
                key.0, key.1, tick, 4096, 16384,
            )
        });
        let view = draft.view(original);
        let target = Target::Chunk {
            key: *chunk,
            part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                actor: key.0,
                property: pumpkin_cluster::living_property::LivingProperty::ItemUse,
            },
            inventory: None,
        };
        let Some(pumpkin_cluster::canonical_action::Cell {
            value:
                Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::ItemUse(
                    Some(active),
                )),
            ..
        }) = view.read(TruthView::Local, target)
        else {
            continue;
        };
        if active.mode != pumpkin_cluster::item_action::ItemUseMode::Consume
            || u32::from(tick.distance_since(active.started)) < active.duration_ticks
        {
            continue;
        }
        let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else {
            continue;
        };
        let Some(id) = reservation.id(0, 10) else {
            clock.release_unused(reservation);
            continue;
        };
        let proposal = ActionProposal {
            command: None,
            id,
            tick,
            lease: Some(*lease),
            client_origin: ClientOrigin::None,
            screen_generation: 0,
            claimed_targets: Vec::new(),
            claim: ClaimedAction::ItemUse(pumpkin_cluster::item_use::ItemUseClaim {
                target_observation: None,
                owner_install_generation: original
                    .player_install_generation(*lease)
                    .unwrap_or(0),
                effective_frame: state.frame,
                eye_height: 0.0,
                intent: pumpkin_cluster::item_action::ItemActionIntent {
                    actor: key.0,
                    tick,
                    source: active.source,
                    expected_inventory_revision: 0,
                    expected_item: active.item,
                    actor_chunk: *chunk,
                    affected: vec![*chunk],
                    kind: ItemActionKind::Complete,
                    client_claim: pumpkin_cluster::item_action::ItemActionClientClaim::default(),
                },
            }),
        };
        let Ok(captured) = normalize_source(&view, original, &proposal) else {
            clock.release_unused(reservation);
            continue;
        };
        let Ok(action) = materialize_captured(&view, &proposal, tick, captured) else {
            clock.release_unused(reservation);
            continue;
        };
        if draft.issue(original, action).is_ok() {
            output.push(draft.actions().last().unwrap().clone());
        } else { clock.release_unused(reservation); }
    }
    output
}

pub(super) fn placement_consumption(
    source: &InventorySlotBinding,
    viewer: &pumpkin_cluster::item_contact::ContactObservation,
    values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
) -> Result<Option<pumpkin_cluster::canonical_action::Effect>, ApplyFailure> {
    let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = viewer.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    if !viewer.valid() || !source.is_valid()
        || !matches!(source.target, Target::Player { key, lease: Some(bound), inventory: Some(_) } if key == lease.key && bound == lease)
        || !matches!(source.slot.loc.inv, pumpkin_cluster::inventory::INV_MAIN | pumpkin_cluster::inventory::INV_OFFHAND) {
        return Err(ApplyFailure::InvalidAction);
    }
    let Some(pumpkin_cluster::canonical_action::Cell { value: Value::Inventory(item), .. }) = values.get(&source.target) else {
        return Err(ApplyFailure::Inventory(source.target));
    };
    if item.is_empty() || item.loc != source.slot.loc { return Err(ApplyFailure::Inventory(source.target)); }
    let mode = Target::Chunk { key: viewer.chunk, part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
        actor: viewer.actor(), property: pumpkin_cluster::living_property::LivingProperty::GameMode }, inventory: None };
    match values.get(&mode).map(|cell| &cell.value) {
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1))) => Ok(None),
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(0 | 2))) => {
            Ok(Some(pumpkin_cluster::canonical_action::Effect::TransformItems {
                intent: pumpkin_cluster::canonical_inventory::ItemTransformIntent { source: source.clone(), item: item.item,
                    components: item.nbt.clone(), operation: pumpkin_cluster::canonical_inventory::ItemTransformOperation::Consume { count: 1 },
                    remainder_destinations: Vec::new() },
            }))
        }
        _ => Err(ApplyFailure::Condition(mode)),
    }
}
