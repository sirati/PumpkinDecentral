use pumpkin_cluster::action_issue::ActionIssueClock;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::Action;
use pumpkin_cluster::entity_body::{DroppedItemChange, DroppedItemCondition, EntityPayload};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

pub(crate) fn capture_frame(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    source: PeerGeneration,
    tick: TickStamp,
    ready: impl Fn(
        pumpkin_cluster::protocol::ChunkKey,
        &pumpkin_cluster::entity_body::EntityBody,
    ) -> bool,
) -> Option<
    pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<(
        pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::entity_body::EntityBody,
    )>,
> {
    let mut jobs = snapshot
        .captured_entity_bodies()
        .filter_map(|(chunk, body)| {
            let last = match &body.payload {
                EntityPayload::Item(item) => item.last_lifetime_tick,
                EntityPayload::Experience(orb) if body.owner == source => orb.last_lifetime_tick,
                _ => return None,
            };
            (ready(chunk, body)
                && (last.is_some() || !body.birth.action.tick.is_after(tick))
                && last.is_none_or(|last| tick.is_after(last)))
            .then_some((chunk, body.clone()))
        })
        .collect::<Vec<_>>();
    jobs.sort_unstable_by_key(|(_, body)| body.id);
    pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame::new(source, tick, jobs)
}

pub(crate) fn emit_frame(
    frame: &pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<(
        pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::entity_body::EntityBody,
    )>,
    ids: &mut ActionIssueClock,
    capacity: usize,
) -> Result<Vec<Action>, pumpkin_cluster::item_lifecycle::ItemLifecycleFailure> {
    let jobs = &frame.remaining()[..frame.remaining().len().min(capacity)];
    if jobs.iter().any(|(_, body)| ids.available(
        ActorId::Entity(body.id), body.id.generation, frame.tick,
    ) == 0) {
        return Ok(Vec::new());
    }
    let mut reservations = Vec::with_capacity(jobs.len());
    for (_, body) in jobs {
        let Some(reservation) = ids.reserve(
            ActorId::Entity(body.id),
            u64::from(body.id.generation),
            frame.tick,
            1,
        ) else {
            for reservation in reservations.into_iter().rev() {
                ids.release_unused(reservation);
            }
            return Ok(Vec::new());
        };
        reservations.push(reservation);
    }
    let mut actions = Vec::with_capacity(jobs.len());
    for ((chunk, body), reservation) in jobs.iter().zip(&reservations) {
        let id = reservation
            .id(0, 2)
            .ok_or(pumpkin_cluster::item_lifecycle::ItemLifecycleFailure::InvalidChange)?;
        let result = if matches!(body.payload, EntityPayload::Experience(_)) {
            pumpkin_cluster::experience_orb::lifetime(
                body,
                *chunk,
                frame.source,
                frame.tick,
                id.sequence,
            )
            .map_err(|_| pumpkin_cluster::item_lifecycle::ItemLifecycleFailure::InvalidChange)
        } else {
            let EntityPayload::Item(item) = &body.payload else {
                unreachable!();
            };
            let change = if item.matches(DroppedItemCondition::Expired) {
                DroppedItemChange::Expire
            } else {
                DroppedItemChange::AdvanceLifetime
            };
            pumpkin_cluster::item_lifecycle::lifecycle_action(
                body,
                *chunk,
                frame.source,
                frame.source,
                frame.tick,
                id.sequence,
                change,
            )
        };
        match result {
            Ok(action) => actions.push(action),
            Err(reason) => {
                for reservation in reservations.into_iter().rev() {
                    ids.release_unused(reservation);
                }
                return Err(reason);
            }
        }
    }
    Ok(actions)
}
