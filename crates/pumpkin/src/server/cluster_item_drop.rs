use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{Action, ActionId, ActionProposal, ActionState, ApplyFailure, Cell, ChunkPart, ClaimedAction, InventoryId, LivePlayerLease, Target, TruthView, Value};
use pumpkin_cluster::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
use pumpkin_cluster::entity_body::EntitySpatial;
use pumpkin_cluster::entity_origin::{EntityOriginAllocator, EntityOriginIdentity};
use pumpkin_cluster::inventory::{InvLoc, InventoryGesture, InventorySlotRef, InventoryTarget, InventoryTransferCount, INV_MAIN, INV_SELECTED};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

#[derive(Clone)]
pub(super) struct FrozenDropMetadata {
    pub lease: LivePlayerLease,
    pub selector: Target,
    pub candidates: Vec<InventorySlotBinding>,
    pub equipment: InventoryEquipmentBinding,
    pub association: ChunkKey,
    pub spawn: ChunkKey,
    pub spatial: EntitySpatial,
    pub primary: PeerGeneration,
    pub source: PeerGeneration,
    pub count: InventoryTransferCount,
    pub identity: Option<EntityOriginIdentity>,
}

impl FrozenDropMetadata {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(self.lease, self.selector, &self.candidates, &self.equipment, self.association, self.spawn, &self.spatial, self.primary, self.source, &self.count, &self.identity))
    }
}

pub(super) fn selected_drop(proposal: &ActionProposal) -> bool {
    matches!(&proposal.claim, ClaimedAction::Inventory(claim)
        if matches!(claim.gestures.as_slice(), [InventoryGesture::DropSelectedHand { .. }]))
}

pub(super) fn capture_static(original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    proposal: &ActionProposal, source: PeerGeneration, executors: &BTreeMap<ChunkKey, PeerGeneration>)
    -> Result<FrozenDropMetadata, ApplyFailure> {
    let ClaimedAction::Inventory(claim) = &proposal.claim else { return Err(ApplyFailure::InvalidAction); };
    let [InventoryGesture::DropSelectedHand { player, count }] = claim.gestures.as_slice() else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    let expected = InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation };
    if (*player != expected && *player != InventoryTarget::Player(lease.gid))
        || proposal.id.actor != pumpkin_cluster::actor::ActorId::Player(lease.gid)
        || source.server != lease.gid.server || source.generation == 0
        || !matches!(count, InventoryTransferCount::All | InventoryTransferCount::Exact(1..=99)) {
        return Err(ApplyFailure::InvalidAction);
    }
    if original.inventory_source_lease(lease.gid) != Some(lease) { return Err(ApplyFailure::InvalidAction); }
    let (association, issuer) = original.player_association(lease).ok_or(ApplyFailure::InvalidAction)?;
    if issuer != source { return Err(ApplyFailure::InvalidAction); }
    for (property, is_health) in [(pumpkin_cluster::living_property::LivingProperty::Health, true), (pumpkin_cluster::living_property::LivingProperty::GameMode, false)] {
        let target = Target::Chunk { key: association, part: ChunkPart::LivingProperty { actor: proposal.id.actor, property }, inventory: None };
        let Some(Cell { value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(value)), .. }) = original.read(TruthView::Local, target) else { return Err(ApplyFailure::Condition(target)); };
        if (is_health && value == 0) || (!is_health && value > 2) { return Err(ApplyFailure::Condition(target)); }
    }
    let actor = original.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    let body = Target::Chunk { key: association, part: ChunkPart::Player(lease.gid), inventory: None };
    let Some(Cell { value: Value::Living(living), .. }) = original.read(TruthView::Local, body) else { return Err(ApplyFailure::Type(body)); };
    let pose = if actor.crawling { pumpkin_data::entity::EntityPose::Swimming }
        else if actor.sneaking { pumpkin_data::entity::EntityPose::Crouching }
        else { super::cluster_living_metadata::player_pose(&living) };
    let eye_height = super::cluster_living_metadata::player_dimensions(&living, pose).eye_height;
    let mut position = actor.frame.effective.position;
    position[1] += f64::from(eye_height) - 0.3;
    let spawn = ChunkKey::new(association.world, (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    let primary = executors.get(&spawn).copied().ok_or(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(spawn)))?;
    if original.scheduled_executor(spawn)? != primary { return Err(ApplyFailure::InvalidAction); }
    let yaw = f64::from(actor.frame.effective.yaw).to_radians();
    let pitch = f64::from(actor.frame.effective.pitch).to_radians();
    let target = |slot| Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: INV_MAIN, slot }) };
    let candidates = (0..9).map(|slot| InventorySlotBinding { slot: InventorySlotRef::new(expected, InvLoc::new(INV_MAIN, slot)), target: target(slot), max_stack: 99 }).collect();
    Ok(FrozenDropMetadata { lease, selector: Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: INV_SELECTED, slot: 0 }) },
        candidates, equipment: InventoryEquipmentBinding::player(lease, association, original.inventory_equipment_attribute_ids()),
        association: association, spawn, spatial: EntitySpatial { world: spawn.world, position: position.map(f64::to_bits),
            velocity_micro: [(-yaw.sin() * pitch.cos() * 300_000.0) as i64, (-pitch.sin() * 300_000.0 + 100_000.0) as i64, (yaw.cos() * pitch.cos() * 300_000.0) as i64],
            yaw: actor.frame.effective.yaw.to_bits(), pitch: actor.frame.effective.pitch.to_bits(), on_ground: false },
        primary, source, count: count.clone(), identity: None })
}

impl FrozenDropMetadata {
    fn property(&self, property: pumpkin_cluster::living_property::LivingProperty) -> Target {
        Target::Chunk { key: self.association, part: ChunkPart::LivingProperty { actor: pumpkin_cluster::actor::ActorId::Player(self.lease.gid), property }, inventory: None }
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        [self.property(pumpkin_cluster::living_property::LivingProperty::Health), self.property(pumpkin_cluster::living_property::LivingProperty::GameMode), self.selector].into_iter().chain(self.candidates.iter().map(|binding| binding.target))
            .chain(self.equipment.targets()).chain(self.identity.iter().map(|identity| Target::Chunk { key: self.spawn, part: ChunkPart::Entity(identity.id), inventory: None }))
    }

    pub fn bind_identity(&mut self, id: ActionId, tick: TickStamp, allocator: &mut EntityOriginAllocator) -> Result<(), ApplyFailure> {
        self.identity = allocator.reserve(self.source, pumpkin_cluster::canonical_action::ActionRef { tick, id }, 1)
            .map_err(|_| ApplyFailure::Capacity)?.first().cloned();
        Ok(())
    }

    pub fn materialize<S: ActionState>(&self, view: &S, proposal: &ActionProposal) -> Result<Action, ApplyFailure> {
        let health = self.property(pumpkin_cluster::living_property::LivingProperty::Health);
        let mode = self.property(pumpkin_cluster::living_property::LivingProperty::GameMode);
        if !matches!(view.read(TruthView::Local, health), Some(Cell { value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1..)), .. })) { return Err(ApplyFailure::Condition(health)); }
        let mode_cell = view.read(TruthView::Local, mode).ok_or(ApplyFailure::Condition(mode))?;
        if !matches!(mode_cell.value, Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(0..=2))) { return Err(ApplyFailure::Condition(mode)); }
        let Some(Cell { value: Value::Inventory(selected), .. }) = view.read(TruthView::Local, self.selector) else {
            return Err(ApplyFailure::Inventory(self.selector));
        };
        if !(1..=9).contains(&selected.item) { return Err(ApplyFailure::Inventory(self.selector)); }
        let binding = self.candidates[usize::from(selected.item - 1)].clone();
        let Some(Cell { value: Value::Inventory(source), .. }) = view.read(TruthView::Local, binding.target) else {
            return Err(ApplyFailure::Inventory(binding.target));
        };
        let identity = self.identity.as_ref().ok_or(ApplyFailure::Capacity)?;
        let body = super::cluster_item_birth::reserved_item_body(pumpkin_cluster::canonical_action::ActionRef { tick: proposal.tick, id: proposal.id },
            identity, self.primary, self.spatial.clone(), &source, 40).map_err(|_| ApplyFailure::InvalidAction)?;
        let mut action = pumpkin_cluster::item_drop::capture(view, proposal.id, proposal.tick, self.association, Some(self.lease),
            pumpkin_cluster::item_drop::DropItemIntent { selected: Some(self.selector), candidates: self.candidates.clone(), source: binding,
                count: self.count.clone(), destination: Target::Chunk { key: self.spawn, part: ChunkPart::Entity(body.id), inventory: None }, body,
                equipment: vec![self.equipment.clone()] }, proposal.client_origin.clone())?;
        action.conditions.push(pumpkin_cluster::canonical_action::Condition { target: health, expected: pumpkin_cluster::canonical_action::Expected::LivingUnsignedAtLeast(1) });
        action.conditions.push(pumpkin_cluster::canonical_action::Condition { target: mode, expected: pumpkin_cluster::canonical_action::Expected::Exact(mode_cell) });
        action.counterfactual_conditions.extend([health, mode]);
        action.counterfactual_conditions.sort_unstable();
        action.counterfactual_conditions.dedup();
        action.validate()?;
        Ok(action)
    }
}
