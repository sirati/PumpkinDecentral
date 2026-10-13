use std::collections::BTreeSet;
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{
    Action, ActionProposal, ActionState, ApplyFailure, ClaimedAction, LivePlayerLease, Target,
    TruthView,
};
use pumpkin_cluster::canonical_inventory::{
    InventoryActionCapture, InventoryGestureReplayIntent, InventorySlotBinding,
};
use pumpkin_cluster::inventory::{InvLoc, InventorySlotRef, InventoryTarget};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::protocol::ChunkKey;

enum CapturedProposal {
    MobAdmission(super::cluster_owned_mob_admission::FrozenMobAdmission),
    Wake(pumpkin_cluster::sleep_action::FrozenWake),
    Dismount(pumpkin_cluster::vehicle_dismount::FrozenVehicleDismount),
    Creative(
        pumpkin_cluster::creative_inventory::CreativeInventoryIntent,
        InventoryActionCapture,
    ),
    Inventory(InventoryGestureReplayIntent, InventoryActionCapture),
    Recipe(
        super::cluster_inventory_menu::FrozenRecipeMetadata,
        InventoryActionCapture,
    ),
    Close(super::cluster_inventory_close::FrozenCloseMetadata),
    Merchant(super::cluster_inventory_merchant::FrozenMerchantMetadata),
    MenuSession(pumpkin_cluster::canonical_menu::MenuSessionIntent, InventoryActionCapture),
    MenuTransform(super::cluster_inventory_menu::FrozenMenuTransform, InventoryActionCapture),
    Combat(super::cluster_world_combat_actions::AttackMaterialization),
    Respawn {
        intent: pumpkin_cluster::respawn_action::SemanticRespawn,
        death: Option<FrozenRespawnDeath>,
    },
    ItemUse(super::cluster_item_use::ItemUseSourceMetadata),
    EntityPlacement(Box<super::cluster_entity_placement::FrozenEntityPlacement>),
    MerchantOpen(super::cluster_item_merchant::FrozenMerchantOpen),
    VehicleStorage(super::cluster_vehicle_storage::FrozenVehicleStorage),
    ActorMount(super::cluster_actor_mount::FrozenActorMount),
    ItemDrop(super::cluster_item_drop::FrozenDropMetadata),
    PlayerCommand(super::cluster_world_owner::FrozenPlayerCommandMaterialization),
    EntityCommand(super::cluster_entity_command::FrozenEntityCommand),
    PlayerAdministration(super::cluster_world_owner::PlayerAdministrationMaterialization),
}

struct FrozenRespawnDeath {
    identity: pumpkin_cluster::combat_action::LivingTickIdentity,
    spatial: pumpkin_cluster::entity_body::EntitySpatial,
    cause: pumpkin_cluster::canonical_action::ActionRef,
    sources: Vec<InventorySlotBinding>,
    equipment: Vec<pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding>,
    rules: Option<(pumpkin_cluster::world_rules::WorldRulesWitness, bool)>,
    close: super::cluster_inventory_close::FrozenDeathClose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SourceCaptureFailure {
    IntrinsicResourceLimit,
    RetainedCapacity,
    Invalid(ApplyFailure),
}

impl From<ApplyFailure> for SourceCaptureFailure {
    fn from(reason: ApplyFailure) -> Self {
        match reason {
            ApplyFailure::ResourceLimit => Self::IntrinsicResourceLimit,
            ApplyFailure::Capacity | ApplyFailure::MissingStakeholder(_) => Self::RetainedCapacity,
            reason => Self::Invalid(reason),
        }
    }
}

pub(super) struct FrozenProposalWork {
    pub proposal: ActionProposal,
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    draft: ActorActionDraft,
    metadata: CapturedProposal,
    targets: Vec<Target>,
    pub tick: TickStamp,
    output_ids: Vec<pumpkin_cluster::canonical_action::ActionId>,
    identities_bound: bool,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    prepared_outputs: Option<Vec<Action>>,
    open_source: Option<Arc<super::cluster_invsee::InventoryOpenSource>>,
    open_frame: Option<super::cluster_invsee::CanonicalInventoryOpenPresentation>,
    open_permit: Option<tokio::sync::mpsc::OwnedPermit<super::cluster_invsee::LocalInventoryLifecycle>>,
}

fn player_bindings(
    state: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    lease: LivePlayerLease,
) -> Result<Vec<InventorySlotBinding>, ApplyFailure> {
    use pumpkin_cluster::inventory::{INV_ARMOR, INV_CURSOR, INV_MAIN, INV_OFFHAND, INV_SELECTED};
    let mut bindings = Vec::with_capacity(43);
    for (inv, count) in [
        (INV_MAIN, 36),
        (INV_ARMOR, 4),
        (INV_OFFHAND, 1),
        (INV_CURSOR, 1),
        (INV_SELECTED, 1),
    ] {
        for slot in 0..count {
            let slot = InventorySlotRef::new(
                InventoryTarget::LeasedPlayer {
                    key: lease.key.0,
                    player: lease.gid,
                    incarnation: lease.incarnation,
                },
                InvLoc::new(inv, slot),
            );
            let target = state.captured_inventory_targets().find_map(|(target, owner, location)| {
                (owner == slot.target && location == slot.loc).then_some(target)
            }).ok_or(ApplyFailure::InvalidAction)?;
            bindings.push(InventorySlotBinding {
                slot,
                target,
                max_stack: if inv == INV_ARMOR || inv == INV_SELECTED {
                    1
                } else {
                    99
                },
            });
        }
    }
    Ok(bindings)
}

impl FrozenProposalWork {
    pub(super) fn supports(proposal: &ActionProposal) -> bool {
        matches!(
            &proposal.claim,
            ClaimedAction::EntityAdmissionRequest(_)
                | ClaimedAction::Inventory(_)
                | ClaimedAction::InventoryMenu(_)
                | ClaimedAction::InventoryClose(_)
                | ClaimedAction::InventoryMenuSession(_)
                | ClaimedAction::Combat(_)
                | ClaimedAction::Respawn(_)
                | ClaimedAction::Wake(_)
                | ClaimedAction::Dismount(_)
                | ClaimedAction::PlayerCommand
                | ClaimedAction::PlayerAdministration(_)
        ) || super::cluster_entity_placement::supports(proposal) || matches!(&proposal.claim, ClaimedAction::ItemUse(claim) if !matches!(claim.intent.kind, pumpkin_cluster::item_action::ItemActionKind::UseOnBlock { .. }))
    }

    pub(super) fn capture_static(
        server: Arc<super::Server>,
        state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        draft: &ActorActionDraft,
        proposal: ActionProposal,
        tick: TickStamp,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        executors: &std::collections::BTreeMap<
            pumpkin_cluster::protocol::ChunkKey,
            pumpkin_cluster::realm_completion::PeerGeneration,
        >,
    ) -> Result<Self, ApplyFailure> {
        Self::capture_static_impl(server, state, draft, proposal, tick, source, executors, None)
    }

    pub(super) fn capture_static_from_snapshot(
        original: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        server: Arc<super::Server>,
        state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        draft: &ActorActionDraft,
        proposal: ActionProposal,
        tick: TickStamp,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        executors: &std::collections::BTreeMap<
            pumpkin_cluster::protocol::ChunkKey,
            pumpkin_cluster::realm_completion::PeerGeneration,
        >,
    ) -> Result<Self, ApplyFailure> {
        Self::capture_static_impl(server, state, draft, proposal, tick, source, executors, Some(original))
    }

    fn capture_static_impl(
        server: Arc<super::Server>,
        state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
        draft: &ActorActionDraft,
        proposal: ActionProposal,
        tick: TickStamp,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        executors: &std::collections::BTreeMap<
            pumpkin_cluster::protocol::ChunkKey,
            pumpkin_cluster::realm_completion::PeerGeneration,
        >,
        original: Option<Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>>,
    ) -> Result<Self, ApplyFailure> {
        if proposal.tick != tick {
            return Err(ApplyFailure::InvalidAction);
        }
        let original = match original {
            Some(original) if original.original_tick() == tick => original,
            Some(_) => return Err(ApplyFailure::InvalidAction),
            None => super::cluster_world_callback_snapshot::CallbackTickSnapshot::capture_actor_phase(server.clone(), state, tick)?,
        };
        let producer = source;
        let view = draft.view(original.as_ref());
        let mut scope = BTreeSet::new();
        let mut chunks = BTreeSet::new();
        let mut leases = Vec::new();
        let mut bindings = Vec::new();
        let mut metadata = match &proposal.claim {
            ClaimedAction::EntityAdmissionRequest(_) => {
                let captured = super::cluster_owned_mob_admission::FrozenMobAdmission::capture(
                    &server, state, &proposal, source, executors)?;
                chunks.insert(captured.chunk());
                chunks.insert(captured.actor_chunk());
                if let Some(lease) = captured.lease() {
                    leases.push(lease);
                    scope.insert(Target::Chunk { key: captured.actor_chunk(),
                        part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid), inventory: None });
                }
                CapturedProposal::MobAdmission(captured)
            }

            ClaimedAction::Dismount(claim) => {
                let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
                if original.player_install_generation(lease) != Some(claim.owner_install_generation) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let chunk = proposal.claimed_targets.iter().find_map(|target| match target {
                    Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid), inventory: None }
                        if *gid == lease.gid => Some(*key), _ => None,
                }).ok_or(ApplyFailure::InvalidAction)?;
                let frozen = pumpkin_cluster::vehicle_dismount::FrozenVehicleDismount::capture(&view, lease, chunk, claim)?;
                for target in frozen.targets() {
                    if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                    scope.insert(target);
                }
                leases.push(lease);
                CapturedProposal::Dismount(frozen)
            }
            ClaimedAction::Wake(claim) => {
                let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
                if original.player_install_generation(lease) != Some(claim.owner_install_generation) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let chunk = proposal.claimed_targets.iter().find_map(|target| match target {
                    Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid), inventory: None }
                        if *gid == lease.gid => Some(*key), _ => None,
                }).ok_or(ApplyFailure::InvalidAction)?;
                let frozen = pumpkin_cluster::sleep_action::FrozenWake::capture(&view, lease, chunk, claim, pumpkin_cluster::canonical_action::ActionRef { id: proposal.id, tick: proposal.tick })?;
                for target in frozen.targets() {
                    if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                    scope.insert(target);
                }
                leases.push(lease);
                CapturedProposal::Wake(frozen)
            }
            ClaimedAction::Respawn(claim) => {
                let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
                if proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
                    || proposal.id.incarnation != lease.incarnation
                    || original.player_install_generation(lease)
                        != Some(claim.owner_install_generation)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let source = proposal
                    .claimed_targets
                    .iter()
                    .find_map(|target| match target {
                        Target::Chunk {
                            key,
                            part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                            inventory: None,
                        } if *gid == lease.gid => Some(*key),
                        _ => None,
                    })
                    .ok_or(ApplyFailure::InvalidAction)?;
                let target = Target::Chunk {
                    key: source,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid),
                    inventory: None,
                };
                let Some(pumpkin_cluster::canonical_action::Cell {
                    value: pumpkin_cluster::canonical_action::Value::Living(dead),
                    ..
                }) = view.read(TruthView::Local, target)
                else {
                    return Err(ApplyFailure::Condition(target));
                };
                if dead.combat.health_milli != 0
                    || dead.combat.timing.death_cause != claim.death_cause
                    || dead
                        .avatar
                        .as_ref()
                        .is_none_or(|avatar| avatar.spatial_epoch != claim.source_epoch)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let detach = match dead.avatar.as_ref().and_then(|avatar| avatar.mount.as_ref()) {
                    Some(mount @ pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. }) => {
                        let vehicle_chunk = original.original_store().entity_body_chunk(false, *id).ok_or(ApplyFailure::InvalidAction)?;
                        let vehicle_target = Target::Chunk { key: vehicle_chunk, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(*id), inventory: None };
                        let Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(body), .. }) = view.read(TruthView::Local, vehicle_target) else { return Err(ApplyFailure::Condition(vehicle_target)); };
                        Some(pumpkin_cluster::actor_relation::capture_player_detach(&view, lease, source, claim.source_epoch, mount, vehicle_chunk, &body)?)
                    }
                    Some(_) => return Err(ApplyFailure::InvalidAction),
                    None => None,
                };
                let death = {
                    let all_bindings = player_bindings(original.as_ref(), lease)?;
                    let sources = all_bindings.iter().filter(|binding| binding.slot.loc.inv < pumpkin_cluster::inventory::INV_SELECTED).cloned().collect();
                    bindings.extend(all_bindings);
                    let close = super::cluster_inventory_close::capture_death_close_from_snapshot(original.as_ref(), state, lease, source, tick, executors)?;
                    scope.extend(close.intent.targets());
                    bindings.extend(close.intent.bindings());
                    let effective = original.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?.frame.effective;
                    let spatial = pumpkin_cluster::entity_body::EntitySpatial { world: source.world,
                        position: effective.position.map(f64::to_bits), velocity_micro: [0;3],
                        yaw: effective.yaw.to_bits(), pitch: effective.pitch.to_bits(), on_ground: effective.on_ground };
                    let drop_chunk = pumpkin_cluster::protocol::ChunkKey::new(spatial.world,
                        (f64::from_bits(spatial.position[0]).floor() as i32).div_euclid(16),
                        (f64::from_bits(spatial.position[2]).floor() as i32).div_euclid(16));
                    chunks.insert(drop_chunk);
                    Some(FrozenRespawnDeath {
                        identity: pumpkin_cluster::combat_action::LivingTickIdentity { identity: pumpkin_cluster::item_contact::ContactIdentity::Player(lease),
                            owner: producer, epoch: claim.source_epoch, chunk: source, simulation_tick: tick },
                        spatial, cause: dead.combat.timing.death_cause.ok_or(ApplyFailure::InvalidAction)?, sources,
                        equipment: vec![pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(lease, source, view.inventory_equipment_attribute_ids())],
                        rules: None, close,
                    })
                };
                let intent = pumpkin_cluster::respawn_action::SemanticRespawn {
                    lease, source, preferred: claim.world_plan,
                    fallback: claim.fallback_plan.ok_or(ApplyFailure::InvalidAction)?,
                    avatar: claim.avatar.clone(), max_health_attribute: claim.max_health_attribute,
                    detach,
                    death_inventory: None,
                    light_plans: super::cluster_world_respawn::respawn_light_plans(&server, claim.world_plan)?,
                };
                for target in intent.targets()? {
                    if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                    scope.insert(target);
                }
                leases.push(lease);
                CapturedProposal::Respawn { intent, death }

            }
            ClaimedAction::InventoryMenuSession(_) => {
                let (intent, capture) = super::cluster_inventory_menu::capture_menu_session_proposal(original.as_ref(), state, &proposal)?;
                scope.extend(intent.targets());
                leases.extend(capture.player_leases.iter().copied());
                CapturedProposal::MenuSession(intent, capture)
            }
            ClaimedAction::InventoryClose(_) => {
                let metadata = super::cluster_inventory_close::capture_close_proposal(
                    original.as_ref(), state, &proposal, executors,
                )?;
                scope.extend(metadata.intent.targets());
                leases.push(metadata.intent.lease);
                bindings.extend(metadata.intent.bindings());
                CapturedProposal::Close(metadata)
            }
            ClaimedAction::InventoryMenu(claim) if matches!(claim.kind,
                pumpkin_cluster::inventory::InventoryMenuKind::ContainerButton(pumpkin_cluster::inventory::InventoryContainerButton::StonecutterRecipe { .. }
                    | pumpkin_cluster::inventory::InventoryContainerButton::LoomPattern { .. })) => {
                let (intent, capture) = super::cluster_inventory_menu::capture_menu_session_proposal(original.as_ref(), state, &proposal)?;
                scope.extend(intent.targets()); leases.extend(capture.player_leases.iter().copied());
                CapturedProposal::MenuSession(intent, capture)
            }
            ClaimedAction::InventoryMenu(claim) if super::cluster_inventory_menu::menu_transform_kind(claim.window_kind).is_some() => {
                let (metadata, capture) = super::cluster_inventory_menu::capture_menu_transform(original.as_ref(), state, &proposal)?;
                scope.extend(metadata.intent.targets()); leases.extend(capture.player_leases.iter().copied());
                bindings.extend(metadata.intent.bindings()); CapturedProposal::MenuTransform(metadata, capture)
            }
            ClaimedAction::InventoryMenu(claim) if claim.window_kind == pumpkin_data::screen::WindowType::Merchant as u16 => {
                let metadata = super::cluster_inventory_merchant::capture_merchant_proposal(original.as_ref(), state, &proposal, executors)?;
                scope.extend(metadata.intent.targets());
                leases.push(metadata.intent.menu.lease);
                bindings.extend(metadata.intent.bindings());
                CapturedProposal::Merchant(metadata)
            }
            ClaimedAction::InventoryMenu(_) => {
                let (intent, capture) =
                    super::cluster_inventory_menu::capture_recipe_proposal(original.as_ref(), state, &proposal)?;
                scope.extend(intent.targets());
                scope.extend(capture.guards.iter().map(|guard| guard.target));
                leases.extend(capture.player_leases.iter().copied());
                bindings.extend(intent.bindings());
                CapturedProposal::Recipe(intent, capture)
            }
            ClaimedAction::Inventory(_) if super::cluster_item_drop::selected_drop(&proposal) => {
                let captured =
                    super::cluster_item_drop::capture_static(original.as_ref(), &proposal, source, executors)?;
                scope.extend(captured.targets());
                chunks.insert(captured.spawn);
                leases.push(captured.lease);
                bindings.extend(captured.candidates.iter().cloned());
                CapturedProposal::ItemDrop(captured)
            }
            ClaimedAction::Inventory(_) => {
                let (intent, capture) =
                    super::cluster_world_inventory_actions::capture_inventory_proposal(
                        original.as_ref(), state, &proposal, tick,
                    )?;
                scope.extend(intent.targets());
                scope.extend(capture.guards.iter().map(|guard| guard.target));
                leases.extend(capture.player_leases.iter().copied());
                bindings.extend(intent.bindings.iter().cloned());
                let mode = Target::Chunk {
                    key: proposal
                        .lease
                        .and_then(|lease| {
                            state.actors.get(lease.gid).map(|actor| actor.local.chunk)
                        })
                        .ok_or(ApplyFailure::InvalidAction)?,
                    part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                        actor: proposal.id.actor,
                        property: pumpkin_cluster::living_property::LivingProperty::GameMode,
                    },
                    inventory: None,
                };
                if let Some(creative) =
                    pumpkin_cluster::creative_inventory::CreativeInventoryIntent::from_gestures(
                        &intent, mode,
                    )
                {
                    scope.extend(creative.targets());
                    CapturedProposal::Creative(creative, capture)
                } else {
                    CapturedProposal::Inventory(intent, capture)
                }
            }
            ClaimedAction::Combat(_) => {
                let metadata = super::cluster_world_combat_actions::capture_proposal(
                    state, original.as_ref(), &view, &proposal, tick,
                )?;
                for contact in [&metadata.contact.attacker, &metadata.contact.victim]
                    .into_iter()
                    .chain(metadata.sweep_victims.iter().map(|victim| &victim.contact))
                {
                    scope.insert(contact.target());
                    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
                        &contact.identity
                    {
                        leases.push(*lease);
                    }
                }
                for lease in &leases {
                    bindings.extend(player_bindings(original.as_ref(), *lease)?);
                }
                CapturedProposal::Combat(metadata)
            }
            ClaimedAction::ItemUse(claim)
                if !matches!(
                    claim.intent.kind,
                    pumpkin_cluster::item_action::ItemActionKind::UseOnBlock { .. }
                ) || super::cluster_entity_placement::supports(&proposal) =>
            {
                let mut captured =
                    super::cluster_item_use::capture_source_metadata(original.as_ref(), &proposal)?;
                super::cluster_item_use::capture_launch_metadata(
                    original.as_ref(),
                    &proposal,
                    &mut captured,
                    source,
                )?;
                if let Some(launch) = &captured.launch {
                    chunks.insert(launch.spawn_chunk);
                }
                chunks.insert(claim.intent.actor_chunk);
                if let Some(observed) = &claim.target_observation {
                    chunks.insert(observed.chunk);
                    scope.insert(observed.target());
                }
                let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
                scope.insert(Target::Chunk {
                    key: claim.intent.actor_chunk,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid),
                    inventory: None,
                });
                for property in [pumpkin_cluster::living_property::LivingProperty::Health,
                    pumpkin_cluster::living_property::LivingProperty::GameMode,
                    pumpkin_cluster::living_property::LivingProperty::PlayerStance] {
                    scope.insert(Target::Chunk { key: claim.intent.actor_chunk,
                        part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                            actor: ActorId::Player(lease.gid), property }, inventory: None });
                }
                leases.push(lease);
                let placement = if super::cluster_entity_placement::supports(&proposal) {
                    super::cluster_entity_placement::FrozenEntityPlacement::capture(&view, (&original), &proposal, &captured)?
                } else { None };
                if let Some(placement) = placement {
                    for target in placement.targets() {
                        if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                        scope.insert(target);
                    }
                    bindings = player_bindings(original.as_ref(), lease)?;
                    CapturedProposal::EntityPlacement(Box::new(placement))
                } else if let Some(merchant) = super::cluster_item_merchant::FrozenMerchantOpen::capture
(&view, &proposal, &captured)? {
                    for target in merchant.targets() {
                        if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                        scope.insert(target);
                    }
                    bindings = merchant.bindings().to_vec();
                    CapturedProposal::MerchantOpen(merchant)
                } else if let Some(storage) = super::cluster_vehicle_storage::FrozenVehicleStorage::capture(&view, &proposal, &captured)? {
                    for target in storage.targets() {
                        if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                        scope.insert(target);
                    }
                    bindings = storage.bindings().to_vec();
                    CapturedProposal::VehicleStorage(storage)
                } else if let Some(mount) = super::cluster_actor_mount::FrozenActorMount::capture(&view, &proposal, &captured)? {
                    for target in mount.targets() {
                        if let Target::Chunk { key, .. } = target { chunks.insert(*key); }
                        scope.insert(*target);
                    }
                    bindings = player_bindings(original.as_ref(), lease)?;
                    bindings.extend(mount.bindings());
                    CapturedProposal::ActorMount(mount)
                } else {
                    bindings = player_bindings(original.as_ref(), lease)?;
                    CapturedProposal::ItemUse(captured)
                }
            }
            ClaimedAction::PlayerCommand
                if matches!(
                    proposal.id.actor,
                    pumpkin_cluster::actor::ActorId::Entity(_)
                ) =>
            {
                let captured = super::cluster_entity_command::freeze(&server, original.as_ref(), &view, &proposal)?;
                if let Some(detach) = &captured.detach { leases.extend(detach.leases.iter().copied()); }
                let targets = captured.targets();
                for target in &targets {
                    if let Target::Chunk { key, .. } = target {
                        chunks.insert(*key);
                    }
                }
                scope.extend(targets);
                CapturedProposal::EntityCommand(captured)
            }
            ClaimedAction::PlayerCommand => {
                let mut captured = super::cluster_world_owner::freeze_player_command(
                    &server,
                    state.actors,
                    state.living,
                    state.inventory,
                    &proposal,
                    tick,
                )
                .map_err(|_| ApplyFailure::InvalidAction)?;
                scope.insert(Target::Chunk {
                    key: captured.actor.association,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(
                        captured.actor.lease.gid,
                    ),
                    inventory: None,
                });
                leases.push(captured.actor.lease);
                if let Some(contact) = &captured.contact {
                    scope.insert(contact.target());
                    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
                        &contact.identity
                    {
                        leases.push(*lease);
                    }
                }
                let request = &proposal
                    .command
                    .as_ref()
                    .ok_or(ApplyFailure::InvalidAction)?
                    .2;
                if matches!(request.mutation, pumpkin_cluster::player_command_control::PlayerCommandMutation::GameMode { mode: 3, .. }) {
                    let target = Target::Chunk { key: captured.actor.association,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Player(captured.actor.lease.gid), inventory: None };
                    let Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Living(body), .. }) = view.read(TruthView::Local, target)
                        else { return Err(ApplyFailure::Condition(target)); };
                    let avatar = body.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
                    if avatar.spatial_epoch != captured.actor.epoch { return Err(ApplyFailure::InvalidAction); }
                    if let Some(mount) = &avatar.mount {
                        let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } = mount else { return Err(ApplyFailure::InvalidAction); };
                        let vehicle_chunk = state.blocks.entity_body_chunk(false, *id).ok_or(ApplyFailure::Capacity)?;
                        let vehicle_target = Target::Chunk { key: vehicle_chunk, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(*id), inventory: None };
                        let Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(vehicle), .. }) = view.read(TruthView::Local, vehicle_target)
                            else { return Err(ApplyFailure::Condition(vehicle_target)); };
                        let detach = pumpkin_cluster::actor_relation::capture_player_detach(&view, captured.actor.lease,
                            captured.actor.association, captured.actor.epoch, mount, vehicle_chunk, &vehicle)?;
                        for target in detach.conditions.iter().map(|condition| condition.target).chain(detach.effects.iter().flat_map(pumpkin_cluster::canonical_action::Effect::targets)) {
                            if let Target::Chunk { key, .. } = target { chunks.insert(key); }
                            scope.insert(target);
                        }
                        chunks.extend(detach.chunks.iter().copied());
                        captured.detach = Some(Box::new(detach));
                    }
                }
                if let pumpkin_cluster::player_command_control::PlayerCommandMutation::Teleport(
                    destination,
                ) = &request.mutation
                {
                    let destination = match destination {
                        pumpkin_cluster::player_command_control::TeleportDestination::Absolute { world, position, .. } =>
                            pumpkin_cluster::protocol::ChunkKey::new(*world, (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16)),
                        pumpkin_cluster::player_command_control::TeleportDestination::Player { .. } => captured.contact.as_ref().ok_or(ApplyFailure::InvalidAction)?.chunk,
                    };
                    scope.insert(Target::Chunk {
                        key: destination,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Player(
                            captured.actor.lease.gid,
                        ),
                        inventory: None,
                    });
                }
                CapturedProposal::PlayerCommand(captured)
            }
            ClaimedAction::PlayerAdministration(update) => {
                let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
                let actor = state
                    .actors
                    .input_projection(lease.gid)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let living = state
                    .living
                    .get(pumpkin_cluster::actor::ActorId::Player(lease.gid))
                    .ok_or(ApplyFailure::InvalidAction)?;
                let avatar = living
                    .local
                    .avatar
                    .as_ref()
                    .ok_or(ApplyFailure::InvalidAction)?;
                let captured = super::cluster_world_owner::PlayerAdministrationMaterialization {
                    lease,
                    association: actor.chunk,
                    effective: actor.frame,
                    epoch: avatar.spatial_epoch,
                    id: proposal.id,
                };
                scope.insert(Target::Chunk {
                    key: captured.association,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid),
                    inventory: None,
                });
                leases.push(lease);
                scope.extend(update.affected.iter().map(|key| Target::Chunk {
                    key: *key,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid),
                    inventory: None,
                }));
                CapturedProposal::PlayerAdministration(captured)
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        scope.extend(bindings.iter().map(|binding| binding.target));
        leases.sort_unstable();
        leases.dedup();
        let targets = scope.into_iter().collect::<Vec<_>>();
        let snapshot = original.with_scope_in_chunks(state, &chunks, &targets, &leases, &bindings)?.with_scheduled_executors(executors);
        let snapshot = if let CapturedProposal::MenuSession(intent, _) = &metadata
            && let Some(admission) = intent.admission() {
            snapshot.with_reserved_menu_inventory(state, &admission)?
        } else { snapshot };
        if let CapturedProposal::Respawn { death: Some(death), .. } = &mut metadata {
            let (rules, policy) = snapshot.world_rules_at(death.identity.chunk.world, tick).ok_or(ApplyFailure::Capacity)?;
            death.rules = Some((rules, policy.keep_inventory));
        }
        snapshot.resource_usage()?;
        Ok(Self {
            proposal,
            snapshot,
            draft: draft.clone(),
            metadata,
            targets,
            tick,
            output_ids: Vec::new(),
            identities_bound: false,
            source,
            prepared_outputs: None,
            open_source: None,
            open_frame: None,
            open_permit: None,
        })
    }

    pub(super) fn output_count(&self) -> Result<u32, ApplyFailure> {
        match &self.metadata {
            CapturedProposal::Combat(metadata) => {
                metadata.output_count().ok_or(ApplyFailure::Capacity)
            }
            _ => Ok(1),
        }
    }

    pub(super) fn attach_menu_open_source(&mut self, open: Arc<super::cluster_invsee::InventoryOpenSource>) -> Result<(), ApplyFailure> {
        let CapturedProposal::MenuSession(intent, capture) = &mut self.metadata else { return Err(ApplyFailure::InvalidAction); };
        let pumpkin_cluster::canonical_menu::MenuSessionOperation::Open { state, .. } = &intent.operation else { return Err(ApplyFailure::InvalidAction); };
        if self.identities_bound || self.open_source.is_some() || state.session != open.screen.session.session
            || state.window_kind != open.screen.window_kind || state.source != open.source || intent.virtual_inventory.is_some()
            || open.screen.session.actor != intent.lease.gid {
            return Err(ApplyFailure::InvalidAction);
        }
        if let pumpkin_cluster::canonical_menu::MenuSource::RemotePlayer { lease } = &open.source {
            capture.edit_authority = Some(super::cluster_world_inventory_actions::capture_inventory_view_authority(
                self.snapshot.as_ref(), &self.proposal, &open.screen, *lease)?);
        }
        self.open_source = Some(open);
        Ok(())
    }

    pub(super) fn bind_ids(
        &mut self,
        ids: Vec<pumpkin_cluster::canonical_action::ActionId>,
        allocator: &mut pumpkin_cluster::entity_origin::EntityOriginAllocator,
    ) -> Result<(), ApplyFailure> {
        self.allocate_ids(ids)?;
        if self.identities_bound {
            return Ok(());
        }
        match &mut self.metadata {
            CapturedProposal::Respawn { intent, death: Some(death) } => {
                let reference = pumpkin_cluster::canonical_action::ActionRef { id: self.output_ids[0], tick: self.tick };
                let count = death.sources.len().checked_add(death.close.birth_count()).ok_or(ApplyFailure::Capacity)?;
                let identities = allocator.reserve(self.source, reference, count).map_err(|_| ApplyFailure::Capacity)?;
                let (drops, closing) = identities.split_at(death.sources.len());
                death.close.bind_identities(reference, closing)?;
                let chunk = pumpkin_cluster::protocol::ChunkKey::new(death.spatial.world,
                    (f64::from_bits(death.spatial.position[0]).floor() as i32).div_euclid(16),
                    (f64::from_bits(death.spatial.position[2]).floor() as i32).div_euclid(16));
                let primary = self.snapshot.scheduled_executor(chunk)?;
                let (rules, keep_inventory) = death.rules.ok_or(ApplyFailure::InvalidAction)?;
                let mut fate = super::cluster_item_birth::death_inventory_intent(reference, death.identity.clone(), death.spatial.clone(),
                    death.cause, rules, keep_inventory, death.sources.clone(), death.equipment.clone(), None, drops, primary)?;
                fate.close = Some(death.close.intent.clone());
                intent.death_inventory = Some(fate);
            }
            CapturedProposal::MobAdmission(metadata) => metadata.bind(self.output_ids[0], self.tick, allocator, self.snapshot.as_ref())?,
            CapturedProposal::EntityPlacement(metadata) => metadata.bind(self.output_ids[0], self.tick, allocator)?,
            CapturedProposal::MerchantOpen(metadata) => metadata.bind(self.output_ids[0], self.tick)?,
            CapturedProposal::Creative(_, capture)
            | CapturedProposal::Inventory(_, capture)
            | CapturedProposal::Recipe(_, capture)
            | CapturedProposal::MenuTransform(_, capture)
            | CapturedProposal::MenuSession(_, capture) => capture.id = self.output_ids[0],
            CapturedProposal::Combat(metadata) => metadata.id = self.output_ids[0],
            CapturedProposal::Close(metadata) => {
                metadata.bind_identity(self.output_ids[0], allocator)?
            }
            CapturedProposal::Merchant(metadata) => {
                metadata.bind_identity(self.output_ids[0], allocator)?
            }
            CapturedProposal::ItemUse(metadata) => super::cluster_item_use::bind_launch_identity(
                metadata,
                self.output_ids[0],
                self.tick,
                allocator,
            )?,
            CapturedProposal::ItemDrop(metadata) => {
                metadata.bind_identity(self.output_ids[0], self.tick, allocator)?
            }
            CapturedProposal::PlayerCommand(metadata) => metadata.actor.id = self.output_ids[0],
            CapturedProposal::EntityCommand(metadata) => metadata.id = self.output_ids[0],
            CapturedProposal::PlayerAdministration(metadata) => metadata.id = self.output_ids[0],
            _ => {}
        }
        if let CapturedProposal::MerchantOpen(metadata) = &self.metadata {
            let admission = metadata.admission().ok_or(ApplyFailure::InvalidAction)?;
            self.snapshot = self.snapshot.clone().with_reserved_generated_menu_inventory(&admission,
                pumpkin_cluster::canonical_action::ActionRef { id: self.output_ids[0], tick: self.tick }, metadata.full_source_slot())?;
            self.targets.extend(metadata.targets());
            self.targets.sort_unstable();
            self.targets.dedup();
        }
        if let CapturedProposal::EntityPlacement(metadata) = &self.metadata {
            let admission = metadata.admission()?;
            self.snapshot = self.snapshot.clone().with_reserved_admissions(allocator,
                pumpkin_cluster::canonical_action::ActionRef { id: self.output_ids[0], tick: self.tick },
                self.source, std::slice::from_ref(&admission))?;
            self.targets.extend(metadata.targets()); self.targets.sort_unstable(); self.targets.dedup();
        }
        if let CapturedProposal::MobAdmission(metadata) = &self.metadata {

            let admission = metadata.admission().ok_or(ApplyFailure::InvalidAction)?;
            self.snapshot = self.snapshot.clone().with_reserved_admissions(allocator,
                pumpkin_cluster::canonical_action::ActionRef { id: self.proposal.id, tick: self.tick },
                self.source, std::slice::from_ref(admission))?;
            self.targets.extend(admission.targets());
            self.targets.sort_unstable();
            self.targets.dedup();
        }
        let births = match &self.metadata {
            CapturedProposal::Respawn { intent, .. } => intent.death_inventory.iter().flat_map(|death|
                death.drops.iter().map(|drop| match drop.destination { Target::Chunk { key, .. } => (key, drop.template.id), _ => unreachable!() })
                    .chain(death.close.iter().flat_map(|close| close.overflow.iter().map(|drop| match drop.target { Target::Chunk { key, .. } => (key, drop.template.id), _ => unreachable!() }))))
                .collect(),
            CapturedProposal::Merchant(metadata) => metadata.intent.rewards.iter().map(|reward| {
                match reward.target { Target::Chunk { key, .. } => (key, reward.template.id), _ => unreachable!() }
            }).collect::<Vec<_>>(),
            CapturedProposal::Close(metadata) => metadata
                .intent
                .overflow
                .iter()
                .map(|drop| match drop.target {
                    Target::Chunk { key, .. } => (key, drop.template.id),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
            CapturedProposal::ItemUse(metadata) => metadata
                .launch
                .as_ref()
                .map(|launch| {
                    launch
                        .identities
                        .iter()
                        .map(|identity| (launch.spawn_chunk, identity.id))
                        .collect()
                })
                .unwrap_or_default(),
            CapturedProposal::ItemDrop(metadata) => metadata
                .identity
                .as_ref()
                .map(|identity| vec![(metadata.spawn, identity.id)])
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        if !births.is_empty() {
            self.snapshot = self.snapshot.clone().with_reserved_births(
                allocator,
                pumpkin_cluster::canonical_action::ActionRef {
                    id: self.proposal.id,
                    tick: self.tick,
                },
                self.source,
                &births,
            )?;
            self.targets
                .extend(births.into_iter().map(|(key, entity)| Target::Chunk {
                    key,
                    part: pumpkin_cluster::canonical_action::ChunkPart::Entity(entity),
                    inventory: None,
                }));
            self.targets.sort_unstable();
            self.targets.dedup();
        }
        if let Some(open) = &self.open_source {
            let CapturedProposal::MenuSession(intent, capture) = &mut self.metadata else { return Err(ApplyFailure::InvalidAction); };
            let pumpkin_cluster::canonical_menu::MenuSessionOperation::Open { state, .. } = &mut intent.operation else { return Err(ApplyFailure::InvalidAction); };
            let reference = pumpkin_cluster::canonical_action::ActionRef { id: self.output_ids[0], tick: self.tick };
            let fullslot = self.snapshot.original_source_slot().ok_or(ApplyFailure::Capacity)?;
            let identity = pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(reference, fullslot).ok_or(ApplyFailure::InvalidAction)?;
            let mut screen = open.screen.clone();
            screen.session.session = identity.session;
            screen.sync_id = identity.sync_id;
            state.session = identity.session;
            if let Some(authority) = &mut capture.edit_authority { authority.screen_session = identity.session; }
            self.open_frame = Some(super::cluster_invsee::CanonicalInventoryOpenPresentation {
                reference, lease: intent.lease, screen, source: open.source.clone(), menu: open.menu.clone(),
                presentation: open.presentation.clone(), merchant: None,
            });
        }
        self.identities_bound = true;
        Ok(())
    }

    pub(super) fn allocate_ids(
        &mut self,
        ids: Vec<pumpkin_cluster::canonical_action::ActionId>,
    ) -> Result<(), ApplyFailure> {
        if (!self.output_ids.is_empty() && self.output_ids != ids)
            || ids.len() != self.output_count()? as usize
            || ids.iter().any(|id| {
                id.actor != self.proposal.id.actor
                    || id.incarnation != self.proposal.id.incarnation
                    || id.family != self.proposal.id.family
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.proposal.id = ids[0];
        self.output_ids = ids;
        Ok(())
    }

    pub(super) fn allocated_ids(&self) -> &[pumpkin_cluster::canonical_action::ActionId] {
        &self.output_ids
    }

    pub(super) fn materialize(&self) -> Result<Action, ApplyFailure> {
        self.materialize_view(self.draft.view(self.snapshot.as_ref()))
    }

    fn materialize_view(
        &self,
        view: pumpkin_cluster::actor_action_scope::ActorActionDraftView<
            '_,
            super::cluster_world_callback_snapshot::CallbackTickSnapshot,
        >,
    ) -> Result<Action, ApplyFailure> {
        if let Some(target) = self
            .targets
            .iter()
            .find(|&&target| !view.available(TruthView::Local, target))
        {
            return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
        }
        match &self.metadata {
            CapturedProposal::MobAdmission(metadata) => metadata.materialize(),
            CapturedProposal::Wake(frozen) => frozen.materialize(self.proposal.id, self.tick),
            CapturedProposal::Dismount(frozen) => frozen.materialize(self.proposal.id, self.tick),
            CapturedProposal::Creative(intent, capture) => {
                intent.clone().capture(&view, capture.clone())
            }
            CapturedProposal::Inventory(intent, capture) => {
                intent.clone().capture_action(&view, capture.clone())
            }
            CapturedProposal::Recipe(intent, capture) => intent.materialize(&view, capture.clone()),
            CapturedProposal::MenuTransform(metadata, capture) => metadata.materialize(&view, capture.clone()),
            CapturedProposal::Close(metadata) => metadata.materialize(&view),
            CapturedProposal::Merchant(metadata) => metadata.materialize(&view),
            CapturedProposal::MenuSession(intent, capture) => intent.capture_action(&view, capture.clone()),
            CapturedProposal::Combat(metadata) => {
                super::cluster_world_combat_actions::materialize_attack(&view, metadata.clone())
            }
            CapturedProposal::Respawn { intent, .. } => intent.clone().capture_action(&view, self.proposal.id, self.tick),
            CapturedProposal::ItemUse(metadata) => {
                let source = super::cluster_item_use::normalize_captured_source(
                    &view,
                    &self.proposal,
                    metadata,
                )?;
                super::cluster_item_use::materialize_captured(
                    &view,
                    &self.proposal,
                    self.tick,
                    source,
                )
            }
            CapturedProposal::EntityPlacement(metadata) => metadata.materialize(&view, self.proposal.id, self.tick, self.proposal.client_origin),
            CapturedProposal::MerchantOpen(metadata) => metadata.materialize(&view, self.proposal.id, self.tick, self.proposal.client_origin),
            CapturedProposal::VehicleStorage(metadata) => metadata.materialize(&view, self.proposal.id, self.tick, self.proposal.client_origin),
            CapturedProposal::ActorMount(metadata) => metadata.materialize(&view, &self.proposal, self.tick),
            CapturedProposal::ItemDrop(metadata) => metadata.materialize(&view, &self.proposal),
            CapturedProposal::EntityCommand(metadata) => metadata.materialize(&view, &self.proposal),
            CapturedProposal::PlayerCommand(metadata) => {
                let mut actor = metadata.actor;
                actor.association = self
                    .draft
                    .player_association(actor.lease.gid, actor.association)
                    .ok_or(ApplyFailure::InvalidAction)?;
                super::cluster_world_owner::materialize_player_command(
                    &view,
                    actor,
                    metadata.expected.clone(),
                    metadata.contact.clone(),
                    metadata.detach.as_deref().cloned(),
                    &self.proposal,
                    self.tick,
                    |chunk| {
                        view.available(
                            TruthView::Local,
                            Target::Chunk {
                                key: chunk,
                                part: pumpkin_cluster::canonical_action::ChunkPart::Player(
                                    metadata.actor.lease.gid,
                                ),
                                inventory: None,
                            },
                        )
                    },
                )?
                .map_err(|chunk| {
                    ApplyFailure::MissingStakeholder(
                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
                    )
                })
            }
            CapturedProposal::PlayerAdministration(metadata) => {
                let ClaimedAction::PlayerAdministration(original) = &self.proposal.claim else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let mut update = original.clone();
                update.tick = self.tick;
                update.seq = pumpkin_cluster::identity::PlayerSeq(metadata.id.sequence);
                let mut actor = *metadata;
                actor.association = self
                    .draft
                    .player_association(actor.lease.gid, actor.association)
                    .ok_or(ApplyFailure::InvalidAction)?;
                super::cluster_world_owner::materialize_player_administration(
                    &view,
                    actor,
                    &self.proposal,
                    &update,
                    self.tick,
                    |chunk| {
                        view.available(
                            TruthView::Local,
                            Target::Chunk {
                                key: chunk,
                                part: pumpkin_cluster::canonical_action::ChunkPart::Player(
                                    metadata.lease.gid,
                                ),
                                inventory: None,
                            },
                        )
                    },
                )?
                .map_err(|chunk| {
                    ApplyFailure::MissingStakeholder(
                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
                    )
                })
            }
        }
    }

    pub(super) fn reserve_open_presentation(&mut self, server: &super::Server) -> Result<(), ApplyFailure> {
        if self.open_frame.is_none() { self.open_frame = self.vehicle_open_presentation()?; }
        if self.open_frame.is_some() && self.open_permit.is_none() {
            self.open_permit = Some(super::cluster_invsee::reserve_canonical_open_presentation(server)
                .map_err(|_| ApplyFailure::Capacity)?);
        }
        Ok(())
    }

    pub(super) fn publish_open_presentation(&mut self) {
        if let Some(open) = self.open_frame.take() {
            self.open_permit.take().expect("reserved canonical menu presentation")
                .send(super::cluster_invsee::LocalInventoryLifecycle::CanonicalOpenPresentation(open));
        }
    }

    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        fn usage<T: serde::Serialize>(value: &T) -> Result<(usize, usize), ApplyFailure> {
            Ok((pumpkin_cluster::logical_records::count(value)?, postcard::experimental::serialized_size(value).map_err(|_| ApplyFailure::InvalidAction)?))
        }
        fn capture_usage(capture: &InventoryActionCapture) -> Result<(usize, usize), ApplyFailure> {
            usage(&(&capture.edit_authority, capture.id, capture.tick, capture.actor_chunk, &capture.player_leases, &capture.player_chunks, &capture.guards, capture.client_origin))
        }
        let (mut records, mut bytes) = usage(&(&self.proposal, &self.targets, &self.output_ids))?;
        let mut add = |next: (usize, usize)| -> Result<(), ApplyFailure> {
            records = records.checked_add(next.0).ok_or(ApplyFailure::ResourceLimit)?;
            bytes = bytes.checked_add(next.1).ok_or(ApplyFailure::ResourceLimit)?;
            Ok(())
        };
        add(self.draft.resource_usage()?)?;
        if let Some((_, _, request)) = &self.proposal.command { add(usage(request)?)?; }
        match &self.metadata {
            CapturedProposal::Creative(intent, capture) => { add(usage(intent)?)?; add(capture_usage(capture)?)?; }
            CapturedProposal::Inventory(intent, capture) => { add(usage(intent)?)?; add(capture_usage(capture)?)?; }
            CapturedProposal::MenuSession(intent, capture) => { add(usage(intent)?)?; add(capture_usage(capture)?)?; }
            CapturedProposal::Recipe(metadata, capture) => { add(metadata.captured_resource_usage()?)?; add(capture_usage(capture)?)?; }
            CapturedProposal::MenuTransform(metadata, capture) => { add(metadata.captured_resource_usage()?)?; add(capture_usage(capture)?)?; }
            CapturedProposal::Close(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::Merchant(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::Combat(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::ItemUse(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::EntityPlacement(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::ItemDrop(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::MerchantOpen(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::VehicleStorage(metadata) => add(metadata.captured_resource_usage()?)?,
            CapturedProposal::ActorMount(metadata) => add(metadata.captured_resource_usage()?)?,
            _ => {}
        }
        pumpkin_cluster::canonical_action::check_logical_budget(records, bytes)?;
        Ok((records, bytes))
    }

    pub(super) fn historical_footprint(&self) -> BTreeSet<ChunkKey> {
        self.targets.iter().filter_map(|target| match target { Target::Chunk { key, .. } => Some(*key), _ => None }).collect()
    }

    pub(super) fn enrich_original_chunks(&mut self, cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut]) -> Result<(), ApplyFailure> {
        self.with_historical_chunks(cuts)
    }

    pub(super) fn source_readiness(&self) -> Result<(), ApplyFailure> {
        Ok(())
    }

    pub(super) fn with_historical_chunks(&mut self, cuts: &[super::cluster_actor_local_cut::ActorLocalChunkCut]) -> Result<(), ApplyFailure> {
        if self.identities_bound || !self.output_ids.is_empty() || self.prepared_outputs.is_some() {
            return Err(ApplyFailure::InvalidAction);
        }
        self.snapshot = self.snapshot.with_historical_chunks(cuts)?;
        Ok(())
    }

    pub(super) fn original_snapshot(&self) -> Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot> {
        self.snapshot.clone()
    }

    pub(super) fn take_close_notification(
        &mut self,
    ) -> Option<super::cluster_inventory_close::CanonicalCloseNotification> {
        match &mut self.metadata {
            CapturedProposal::Close(metadata) => metadata.notification.take(),
            CapturedProposal::Respawn { death: Some(death), .. } => death.close.take_notification(pumpkin_cluster::canonical_action::ActionRef { id: self.output_ids[0], tick: self.tick }),
            _ => None,
        }
    }

    pub(super) fn draft(&self) -> &ActorActionDraft {
        &self.draft
    }

    pub(super) fn is_prepared(&self) -> bool {
        self.prepared_outputs.is_some()
    }

    pub(super) fn prepare_outputs(
        &mut self,
        mut prepare: impl FnMut(
            &Action,
            &pumpkin_cluster::actor_action_scope::ActorActionDraftView<
                '_,
                super::cluster_world_callback_snapshot::CallbackTickSnapshot,
            >,
        ) -> Result<(), ApplyFailure>,
    ) -> Result<Vec<Action>, ApplyFailure> {
        if let Some(actions) = &self.prepared_outputs {
            return Ok(actions.clone());
        }
        if !self.identities_bound || self.output_ids.len() != self.output_count()? as usize {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut draft = self.draft.clone();
        draft.reserve_source_resources(self.output_ids.len(), self.targets.len()).map_err(|_| ApplyFailure::InvalidAction)?;
        let mut actions = Vec::with_capacity(self.output_ids.len());
        for index in 0..self.output_ids.len() {
            let view = draft.view(self.snapshot.as_ref());
            let action = match &self.metadata {
                CapturedProposal::Combat(metadata) => {
                    super::cluster_world_combat_actions::output_at(
                        &view,
                        metadata.clone(),
                        &self.output_ids,
                        index,
                    )?
                }
                _ if index == 0 => self.materialize_view(view)?,
                _ => return Err(ApplyFailure::InvalidAction),
            };
            action.validate_resource_budget()?;
            prepare(&action, &draft.view(self.snapshot.as_ref()))?;
            draft
                .issue(self.snapshot.as_ref(), action.clone())
                .map_err(|_| ApplyFailure::InvalidAction)?;
            actions.push(action);
        }
        self.draft = draft;
        self.prepared_outputs = Some(actions.clone());
        Ok(actions)
    }

    pub(super) fn targets(&self) -> &[Target] {
        &self.targets
    }

    pub(super) fn replace_draft(&mut self, draft: ActorActionDraft) {
        self.draft = draft;
    }

    pub(super) fn view(
        &self,
    ) -> pumpkin_cluster::actor_action_scope::ActorActionDraftView<
        '_,
        super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    > {
        self.draft.view(self.snapshot.as_ref())
    }

    pub(super) fn issue(
        &mut self,
        action: Action,
    ) -> Result<(), (pumpkin_cluster::actor_action_scope::ScopeFailure, Action)> {
        self.draft.issue(self.snapshot.as_ref(), action)
    }

    pub(super) fn expand(
        &mut self,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        targets: &[Target],
    ) -> Result<(), ApplyFailure> {
        self.snapshot = self.snapshot.expand_history(graphs, self.tick, targets)?;
        Ok(())
    }
}

impl FrozenProposalWork {
    pub(super) fn vehicle_open_presentation(&self)
        -> Result<Option<super::cluster_invsee::CanonicalInventoryOpenPresentation>, ApplyFailure> {
        match &self.metadata {
            CapturedProposal::MerchantOpen(metadata) => metadata.presentation(
                pumpkin_cluster::canonical_action::ActionRef { id: self.proposal.id, tick: self.tick }).map(Some),
            CapturedProposal::VehicleStorage(metadata) => metadata.presentation(
                pumpkin_cluster::canonical_action::ActionRef { id: self.proposal.id, tick: self.tick }).map(Some),
            _ => Ok(None),
        }
    }
}
