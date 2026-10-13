use pumpkin_cluster::{
    action_command::{ActorReference, Hand, ItemCommandSource},
    actor_occupancy::ActorOccupancyCut,
    canonical_action::{
        ActionProposal, ActionState, ApplyFailure, ClaimedAction, TruthView, Value,
    },
    entity_body::EntityPayload,
    inventory::{INV_MAIN, INV_OFFHAND},
    item_action::{ActorItemInteraction, ItemActionKind},
    item_contact::{ContactIdentity, ContactObservation},
    object_command::{ObjectInteractionAttributes, ObjectInteractionRegistry, UseObject},
    object_state::ObjectKind,
};

pub(super) struct Registry;

impl ObjectInteractionRegistry for Registry {
    fn object_interaction_attributes(&self) -> Result<ObjectInteractionAttributes, ApplyFailure> {
        Ok(ObjectInteractionAttributes {
            scale: u16::from(pumpkin_data::attributes::Attributes::SCALE.id),
            reach: u16::from(pumpkin_data::attributes::Attributes::ENTITY_INTERACTION_RANGE.id),
        })
    }
    fn object_interaction_eye_height(
        &self,
        living: &pumpkin_cluster::item_action::ActorLivingState,
    ) -> Result<f64, ApplyFailure> {
        Ok(f64::from(
            super::cluster_living_metadata::player_eye_height(living),
        ))
    }
}

fn reference(contact: &ContactObservation) -> ActorReference {
    match &contact.identity {
        ContactIdentity::Player(lease) => ActorReference::Player {
            lease: *lease,
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        },
        ContactIdentity::Entity { id, birth } => ActorReference::Entity {
            id: *id,
            birth: birth.clone(),
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        },
    }
}

fn cut(contact: &ContactObservation) -> ActorOccupancyCut {
    ActorOccupancyCut {
        position: contact.position,
        sample_tick: contact.simulation_tick,
        simulation_sequence: contact.observed_sequence,
    }
}

pub(super) fn prepare(
    state: &impl ActionState,
    proposal: &ActionProposal,
    captured: &super::cluster_item_use::CapturedItemUseSource,
) -> Result<Option<(UseObject, ItemCommandSource)>, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Ok(None);
    };
    let ItemActionKind::UseOnActor {
        target,
        target_chunk,
        interaction: ActorItemInteraction::Interact,
        ..
    } = &claim.intent.kind
    else {
        return Ok(None);
    };
    let observed = claim
        .target_observation
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    if observed.actor() != *target
        || observed.chunk != *target_chunk
        || !observed.valid()
        || observed.simulation_tick.is_after(proposal.tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let body = state
        .read(TruthView::Local, observed.target())
        .ok_or(ApplyFailure::Condition(observed.target()))?;
    let Value::Entity(body) = &body.value else {
        return Ok(None);
    };
    let EntityPayload::Object(object) = &body.payload else {
        return Ok(None);
    };
    if !matches!(
        object.kind,
        ObjectKind::ItemFrame(_) | ObjectKind::Interaction(_)
    ) {
        return Ok(None);
    }
    let ContactIdentity::Entity { id, birth } = &observed.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    if body.id != *id
        || body.birth != *birth
        || body.owner != observed.owner
        || body.spatial_epoch != observed.epoch
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let (lease, binding, _, _, metadata) = captured;
    let source = metadata
        .source_observation
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    if source.identity != ContactIdentity::Player(*lease)
        || proposal.lease != Some(*lease)
        || source.actor() != proposal.id.actor
        || source.chunk != claim.intent.actor_chunk
        || !source.valid()
        || source.simulation_tick.is_after(proposal.tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let hand = match binding.slot.loc.inv {
        INV_MAIN => Hand::Main,
        INV_OFFHAND => Hand::Off,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    Ok(Some((
        UseObject {
            target: reference(observed),
            cut: cut(observed),
        },
        ItemCommandSource {
            actor: reference(source),
            hand,
            cut: cut(source),
        },
    )))
}
