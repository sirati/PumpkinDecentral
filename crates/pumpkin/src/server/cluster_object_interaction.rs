use pumpkin_cluster::{
    canonical_action::{
        Action, ActionProposal, ActionState, ApplyFailure, ChunkPart, ClaimedAction, Target,
        TruthView, Value,
    },
    entity_body::EntityPayload,
    item_action::ItemActionKind,
    object_action::{ObjectIntent, ObjectOperation},
    object_state::ObjectKind,
    time::TickStamp,
};

pub(super) fn materialize(
    state: &impl ActionState,
    proposal: &ActionProposal,
    tick: TickStamp,
    captured: &super::cluster_item_use::CapturedItemUseSource,
) -> Result<Option<Action>, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let ItemActionKind::UseOnActor {
        target,
        target_chunk,
        interaction,
        ..
    } = claim.intent.kind
    else {
        return Ok(None);
    };
    let observed = claim
        .target_observation
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    if observed.actor() != target
        || observed.chunk != target_chunk
        || observed.simulation_tick.is_after(tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let body_cell = state
        .read(TruthView::Local, observed.target())
        .ok_or(ApplyFailure::Condition(observed.target()))?;
    let Value::Entity(body) = body_cell.value else {
        return Ok(None);
    };
    let EntityPayload::Object(object) = &body.payload else {
        return Ok(None);
    };
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } = &observed.identity
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    if &body.birth != birth || body.owner != observed.owner || body.spatial_epoch != observed.epoch
    {
        return Err(ApplyFailure::Condition(observed.target()));
    }
    let operation = match (&object.kind, interaction) {
        (
            ObjectKind::ItemFrame(_),
            pumpkin_cluster::item_action::ActorItemInteraction::Interact,
        ) => ObjectOperation::RotateFrame,
        (
            ObjectKind::Interaction(_),
            pumpkin_cluster::item_action::ActorItemInteraction::Interact,
        ) => ObjectOperation::RecordInteraction,
        (
            ObjectKind::Interaction(_),
            pumpkin_cluster::item_action::ActorItemInteraction::Attack,
        ) => ObjectOperation::RecordAttack,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let (lease, _, _, selected_guard, metadata) = captured;
    let mut source = metadata
        .source_observation
        .clone()
        .ok_or(ApplyFailure::InvalidAction)?;
    if source.actor() != proposal.id.actor
        || source.chunk != claim.intent.actor_chunk
        || source.simulation_tick.is_after(tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let source_body = Target::Chunk {
        key: source.chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let living_cell = state
        .read(TruthView::Local, source_body)
        .ok_or(ApplyFailure::Condition(source_body))?;
    let living = pumpkin_cluster::living_property::state(&living_cell.value)
        .ok_or(ApplyFailure::Type(source_body))?;
    if living
        .avatar
        .as_ref()
        .is_none_or(|avatar| avatar.game_mode == 3)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let attribute = u16::from(pumpkin_data::attributes::Attributes::ENTITY_INTERACTION_RANGE.id);
    let base = living
        .combat
        .effective_attributes
        .get(&attribute)
        .copied()
        .map(f64::from_bits)
        .ok_or(ApplyFailure::InvalidAction)?;
    let reach = living.combat.equipment.attribute(attribute, base);
    let bounds =
        super::cluster_object_metadata::bounds(&body).ok_or(ApplyFailure::InvalidAction)?;
    source.simulation_tick = tick;
    let mut target = observed.clone();
    target.simulation_tick = tick;
    ObjectIntent {
        source,
        target,
        operation,
        eye_height: claim.eye_height.to_bits(),
        reach: reach.to_bits(),
        reach_attribute: attribute,
        bounds_minimum: [
            bounds.min.x.to_bits(),
            bounds.min.y.to_bits(),
            bounds.min.z.to_bits(),
        ],
        bounds_maximum: [
            bounds.max.x.to_bits(),
            bounds.max.y.to_bits(),
            bounds.max.z.to_bits(),
        ],
    }
    .capture(
        state,
        pumpkin_cluster::canonical_inventory::InventoryActionCapture {
            edit_authority: None,
            id: proposal.id,
            tick,
            actor_chunk: Some(claim.intent.actor_chunk),
            player_leases: vec![*lease],
            player_chunks: vec![(lease.key, claim.intent.actor_chunk)],
            guards: vec![selected_guard.clone()],
            client_origin: proposal.client_origin,
        },
    )
    .map(Some)
}
