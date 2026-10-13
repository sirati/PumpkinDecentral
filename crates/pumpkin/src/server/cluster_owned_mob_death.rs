use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::actor_relation::{ActorRelationParticipant, OwnerDetachPlan, capture_owner_detach_from_participants};
use pumpkin_cluster::canonical_action::{Action, ActionId, ActionState, ApplyFailure, ChunkPart, ClientOrigin, InventoryId, Target, TruthView, Value};
use pumpkin_cluster::canonical_entity_retirement::{EntityInventoryDisposition, EntityRetirementIntent, LivingDeathRetirement};
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::entity_body::EntityPayload;
use pumpkin_cluster::inventory::{InventorySlotRef, InventoryTarget, INV_MAX_STACK};
use pumpkin_cluster::item_contact::ContactObservation;

pub(super) struct FrozenMobRetirement {
    intent: EntityRetirementIntent,
    detach: OwnerDetachPlan,
}

impl FrozenMobRetirement {
    pub(super) fn capture_identity(
        snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
        draft: &ActorActionDraft,
        identity: &pumpkin_cluster::combat_action::LivingTickIdentity,
        _contacts: &[ContactObservation],
    ) -> Result<Option<Self>, ApplyFailure> {
        if !identity.valid() { return Err(ApplyFailure::InvalidAction); }
        let view = draft.view(snapshot);
        let Some(cell) = view.read(TruthView::Local, identity.target()) else {
            return Ok(None);
        };
        let Value::Entity(body) = cell.value else { return Ok(None); };
        let EntityPayload::Living(living) = &body.payload else { return Ok(None); };
        if living.combat.health_milli != 0 || !living.combat.timing.death_inventory_processed
            || living.combat.timing.death_ticks < 20 { return Ok(None); }
        let cause = living.combat.timing.death_cause.ok_or(ApplyFailure::InvalidAction)?;
        if body.owner != identity.owner || body.spatial_epoch != identity.epoch
            || !matches!(&identity.identity, pumpkin_cluster::item_contact::ContactIdentity::Entity {id,birth}
                if *id == body.id && *birth == body.birth) { return Err(ApplyFailure::InvalidAction); }
        let detach = capture_entity_death_detach(snapshot, &view, identity)?;
        let inventory = view.entity_inventory_layout(&body)?.into_iter().map(|loc| InventorySlotBinding {
            slot: InventorySlotRef::new(InventoryTarget::Entity(body.id.into()), loc),
            target: Target::Chunk { key: identity.chunk, part: ChunkPart::Entity(body.id),
                inventory: Some(InventoryId::Entity { inv: loc.inv, slot: loc.slot }) },
            max_stack: INV_MAX_STACK,
        }).collect();
        let intent = EntityRetirementIntent { target: identity.target(), birth: body.birth,
            owner: body.owner, kind: body.kind, inventory, disposition: EntityInventoryDisposition::Destroy,
            living_death: Some(LivingDeathRetirement { death_cause: cause, epoch: identity.epoch }) };
        Ok(Some(Self { intent, detach }))
    }

    pub(super) fn materialize(&self,
        view: &impl ActionState, id: ActionId, tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<Action, ApplyFailure> {
        self.intent.capture_action_with_detach(view, id, tick, ClientOrigin::None, Some(self.detach.clone()))
    }
}

impl FrozenMobRetirement {
    pub(super) fn capture(
        snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
        draft: &ActorActionDraft,
        contact: &ContactObservation,
        contacts: &[ContactObservation],
    ) -> Result<Option<Self>, ApplyFailure> {
        Self::capture_identity(snapshot, draft,
            &pumpkin_cluster::combat_action::LivingTickIdentity::from_contact(contact), contacts)
    }
}

#[cfg(test)]
#[path = "cluster_owned_mob_death_tests.rs"]
mod tests;

pub(super) fn capture_entity_death_detach(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &impl ActionState,
    identity: &pumpkin_cluster::combat_action::LivingTickIdentity,
) -> Result<OwnerDetachPlan, ApplyFailure> {
    if !identity.valid() { return Err(ApplyFailure::InvalidAction); }
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &identity.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    let target = identity.target();
    let Some(cell) = view.read(TruthView::Local, target) else { return Err(ApplyFailure::Condition(target)); };
    let Value::Entity(body) = cell.value else { return Err(ApplyFailure::Type(target)); };
    if body.id != *id || body.birth != *birth || body.owner != identity.owner || body.spatial_epoch != identity.epoch
        || !matches!(&body.payload, EntityPayload::Living(living) if living.combat.health_milli == 0 && living.combat.timing.death_cause.is_some()) {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut participants = Vec::new();
    for related in body.mount.iter().chain(body.passengers.values()) {
        if participants.iter().any(|known: &ActorRelationParticipant| &known.identity == related) { continue; }
        let participant = match related {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =>
                snapshot.player_relation_participant(*lease),
            pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } => {
                let store = snapshot.original_store();
                store.entity_body_chunk(false, *id).and_then(|chunk| {
                    let cell = store.entity_body(false, chunk, *id)?;
                    let Value::Entity(other) = &cell.value else { return None; };
                    (other.birth == *birth).then(|| ActorRelationParticipant::from_entity(other, chunk)).flatten()
                })
            }
        }.ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))?;
        participants.push(participant);
    }
    capture_owner_detach_from_participants(view, &body, identity.chunk, &participants)
        .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))
}

pub(super) struct MobDeathLootFacts {
    pub items: Vec<crate::world::loot::LootItemFact>,
    pub experience: u32,
}

pub(super) fn death_loot_facts(
    body: &pumpkin_cluster::entity_body::EntityBody,
    cause: pumpkin_cluster::canonical_action::ActionRef,
    seed: u64,
    enabled: bool,
    params: &crate::world::loot::LootContextParameters,
    output_capacity: usize,
) -> Result<MobDeathLootFacts, ApplyFailure> {
    let kind = pumpkin_data::entity::EntityType::from_raw(body.kind).filter(|kind| kind.mob)
        .ok_or(ApplyFailure::InvalidAction)?;
    let EntityPayload::Living(living) = &body.payload else { return Err(ApplyFailure::InvalidAction); };
    let mob = living.mob.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if living.combat.health_milli != 0 || living.combat.timing.death_cause != Some(cause)
        || params.this_entity != Some(kind) || output_capacity > 4096 {
        return Err(ApplyFailure::InvalidAction);
    }
    if !enabled { return Ok(MobDeathLootFacts { items: Vec::new(), experience: 0 }); }
    let key = format!("minecraft:entities/{}", kind.resource_name);
    let items = pumpkin_data::loot_table::get_loot_table(&key).map_or_else(Vec::new, |table|
        crate::world::loot::generate_loot_facts_with_context(table, seed as i64, params));
    let items = crate::world::loot::partition_loot_facts(&items, output_capacity)
        .ok_or(ApplyFailure::InvalidAction)?;
    let experience = if params.killed_by_player == Some(true) && mob.reproduction.growth_age >= 0 {
        kind.experience_reward
    } else { 0 };
    Ok(MobDeathLootFacts { items, experience })
}
