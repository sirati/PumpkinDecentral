use std::collections::HashMap;
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::Value;
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::{EntityPose, EntityType};

#[derive(Debug, Clone)]
pub(crate) struct CombatClientTarget {
    pub tags: Arc<std::collections::BTreeSet<String>>,
    pub contact: ContactObservation,
    pub kind: u16,
    pub game_mode: Option<u8>,
    pub yaw: u32,
    pub on_ground: bool,
}

pub(super) fn entity_identity(
    server: &super::Server,
    client_entity_id: i32,
) -> Option<(
    pumpkin_cluster::protocol::GlobalEntityId,
    pumpkin_cluster::entity_body::EntityBirth,
    PeerGeneration,
)> {
    let targets = server.cluster_combat_client_targets.load();
    let target = targets.get(&client_entity_id)?;
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } =
        &target.contact.identity
    else {
        return None;
    };
    (target.contact.valid()
        && server
            .cluster_peer_incarnations
            .load()
            .get(&target.contact.owner.server)
            == Some(&target.contact.owner.generation))
    .then(|| (*id, birth.clone(), target.contact.owner))
}

pub(super) fn publish(
    server: &super::Server,
    actors: &super::cluster_world_actor::OwnedActors,
    living: &super::cluster_world_actor::OwnedLivingActors,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
    ids: &super::cluster_entity_projection::OwnerEntityClientIds,
    tick: TickStamp,
) {
    let generations = server.cluster_peer_incarnations.load();
    let mut targets = HashMap::new();
    for (gid, spatial, association, epoch) in actors.player_observations(living) {
        let Some(generation) = generations.get(&gid.server) else {
            continue;
        };
        let source = PeerGeneration {
            server: gid.server,
            generation: *generation,
        };
        let Some((lease, client_id)) = actors.player_client_identity(gid, source) else {
            continue;
        };
        let sleeping = living
            .get(ActorId::Player(gid))
            .and_then(|state| state.local.avatar.as_ref())
            .is_some_and(|avatar| avatar.presentation.sleeping.is_some());
        let pose = if sleeping {
            EntityPose::Sleeping
        } else if spatial.gliding {
            EntityPose::FallFlying
        } else if spatial.crawling || spatial.swimming {
            EntityPose::Swimming
        } else if spatial.sneaking {
            EntityPose::Crouching
        } else {
            EntityPose::Standing
        };
        let Some(state) = living.get(ActorId::Player(gid)) else { continue; };
        let dimensions = super::cluster_living_metadata::player_dimensions(&state.local, pose);
        if let Some(contact) = ContactObservation::player(
            lease,
            source,
            epoch,
            association,
            spatial.frame.effective,
            f64::from(dimensions.width) * 0.5,
            f64::from(dimensions.height),
            tick,
        ) {
            targets.insert(
                client_id,
                CombatClientTarget {
                    tags: Arc::new(living.get(ActorId::Player(gid)).map(|state| state.local.tags.clone()).unwrap_or_default()),
                    contact,
                    kind: EntityType::PLAYER.id,
                    game_mode: Some(spatial.game_mode),
                    yaw: spatial.frame.effective.yaw.to_bits(),
                    on_ground: spatial.frame.effective.on_ground,
                },
            );
        }
    }
    for (chunk, cell) in blocks.local_entity_bodies() {
        let Value::Entity(body) = &cell.value else {
            continue;
        };
        let Some(kind) = EntityType::from_raw(body.kind) else {
            continue;
        };
        let Some(client_id) = ids.client_id(body) else {
            continue;
        };
        let projected = spatial.project(body);
        if let Some(contact) = ContactObservation::entity(
            body,
            chunk,
            &projected.spatial,
            f64::from(kind.dimension[0]) * 0.5,
            f64::from(kind.dimension[1]),
            tick,
            spatial
                .get(body.id)
                .filter(|sample| sample.birth == body.birth && sample.epoch == body.spatial_epoch)
                .map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| {
                    sample.sequence
                }),
        ) {
            targets.insert(
                client_id,
                CombatClientTarget {
                    tags: Arc::new(body.tags.clone()),
                    contact,
                    kind: body.kind,
                    game_mode: None,
                    yaw: projected.spatial.yaw,
                    on_ground: projected.spatial.on_ground,
                },
            );
        }
    }
    server
        .cluster_combat_client_targets
        .store(Arc::new(targets));
}
