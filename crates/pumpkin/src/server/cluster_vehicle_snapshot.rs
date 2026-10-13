use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::entity_body::{EntityBirth, EntityPayload, EntitySpatial, VehicleKind};
use pumpkin_cluster::item_contact::ContactIdentity;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

#[derive(Clone)]
pub(crate) struct VehicleView {
    pub birth: EntityBirth,
    pub owner: PeerGeneration,
    pub kind: VehicleKind,
    pub chunk: ChunkKey,
    pub spatial: EntitySpatial,
    pub tick: TickStamp,
    pub passengers: BTreeMap<u8, ContactIdentity>,
    pub controller: Option<pumpkin_cluster::vehicle_control::VehicleControllerRecord>,
}

#[derive(Default)]
pub(crate) struct VehicleViews {
    pub vehicles: BTreeMap<GlobalEntityId, VehicleView>,
}

pub(super) fn publish(
    server: &super::Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
    controllers: &pumpkin_cluster::vehicle_control::VehicleControllerBank,
    _tick: TickStamp,
) {
    let mut vehicles = BTreeMap::new();
    for (chunk, cell) in blocks.local_entity_bodies() {
        let pumpkin_cluster::canonical_action::Value::Entity(body) = &cell.value else {
            continue;
        };
        let EntityPayload::Vehicle(state) = &body.payload else {
            continue;
        };
        let sample = spatial
            .get(body.id)
            .filter(|sample| sample.matches_body(body));
        let controller = controllers
            .get(body.id)
            .filter(|record| record.birth == body.birth && record.owner == body.owner)
            .cloned();
        vehicles.insert(
            body.id,
            VehicleView {
                birth: body.birth.clone(),
                owner: body.owner,
                kind: state.kind,
                chunk,
                spatial: spatial.spatial(body).clone(),
                tick: sample.map_or(
                    state.simulation_tick.unwrap_or(body.birth.action.tick),
                    |sample| sample.tick,
                ),
                passengers: body.passengers.clone(),
                controller,
            },
        );
    }
    server
        .cluster_vehicle_views
        .store(Arc::new(VehicleViews { vehicles }));
}

pub(super) fn original_observation(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    identity: &ContactIdentity,
    tick: TickStamp,
) -> Result<
    pumpkin_cluster::vehicle_command::VehicleObservation,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use super::cluster_world_callback_snapshot::OriginalReliableVelocity;
    use pumpkin_cluster::canonical_action::{
        ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value,
    };
    let ContactIdentity::Entity { id, birth } = identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    let motion = original.original_motion(identity, tick)?;
    let target = motion.contact.target();
    let cell = original
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?;
    let Value::Entity(body) = &cell.value else {
        return Err(ApplyFailure::Type(target));
    };
    if body.id != *id || body.birth != *birth || !matches!(body.payload, EntityPayload::Vehicle(_))
    {
        return Err(ApplyFailure::Condition(target));
    }
    let spatial = original
        .entity_effective_spatial(*id)
        .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))?;
    let OriginalReliableVelocity::EntityBody { velocity, revision } = motion.canonical_velocity
    else {
        return Err(ApplyFailure::Type(target));
    };
    let Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(actual),
        inventory: None,
    } = target
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    if actual != *id || spatial.world != chunk.world {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(pumpkin_cluster::vehicle_command::VehicleObservation {
        actor: pumpkin_cluster::action_command::ActorReference::Entity {
            id: *id,
            birth: birth.clone(),
            chunk,
            epoch: motion.contact.epoch,
            owner: motion.contact.owner,
        },
        position: motion.contact.position,
        yaw: spatial.yaw,
        on_ground: spatial.on_ground,
        sequence: motion.contact.observed_sequence,
        velocity: pumpkin_cluster::vehicle_command::EntityVelocityCut {
            reliable: velocity,
            body_revision: revision,
            effective: motion.velocity_micro,
        },
    })
}
