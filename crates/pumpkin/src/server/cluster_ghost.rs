use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::PlayerReplicaSnapshot;
use pumpkin_cluster::wire::MovementBatch;
use tokio::sync::mpsc;

use super::Server;
use super::cluster_entity_apply::forward_entity_pos_datagram;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram};
use crate::entity::EntityBase;
use crate::entity::cluster_player_ghost::RemotePlayerReplica;

static GHOST_APPLIED: AtomicU64 = AtomicU64::new(0);
static GHOST_DATAGRAMS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn remote_player_replica(
    server: &Server,
    gid: GlobalPlayerId,
) -> Option<Arc<RemotePlayerReplica>> {
    server.cluster_remote_players.load().get(&gid).cloned()
}

pub(crate) fn remote_player_client_entity_id(
    server: &Server,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
) -> Option<i32> {
    let replicas = server.cluster_remote_players.load();
    let frame = &replicas.get(&lease.gid)?.frame;
    (frame.lease == lease && frame.source == source).then_some(frame.client_entity_id)
}

pub fn remote_player_gid(entity: &dyn EntityBase) -> Option<GlobalPlayerId> {
    entity
        .cast_any()
        .downcast_ref::<RemotePlayerReplica>()
        .map(|replica| replica.frame.lease.gid)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoundaryApplyOutcome {
    Applied,
    AwaitingClientBinding,
    StaleIncarnation,
    UnauthorizedSource,
    InvalidSpatialState,
    AwaitingPresence,
    AwaitingActivePresence,
    AwaitingWorld,
    AwaitingChunk,
}

pub fn apply_remote_player_boundary(
    server: &Arc<Server>,
    snapshot: PlayerReplicaSnapshot,
) -> BoundaryApplyOutcome {
    let update = snapshot.movement;
    if update.incarnation == 0 {
        return BoundaryApplyOutcome::StaleIncarnation;
    }
    if super::cluster_world_actor::OwnedActors::validate_remote(update, snapshot.visual).is_err() {
        return BoundaryApplyOutcome::InvalidSpatialState;
    }
    let Some(generation) = server
        .cluster_peer_incarnations
        .load()
        .get(&update.gid.server)
        .copied()
    else {
        return BoundaryApplyOutcome::AwaitingPresence;
    };
    let Some(replica) = remote_player_replica(server, update.gid) else {
        return BoundaryApplyOutcome::AwaitingClientBinding;
    };
    let frame = &replica.frame;
    if frame.lease.incarnation > update.incarnation {
        return BoundaryApplyOutcome::StaleIncarnation;
    }
    if frame.lease.incarnation != update.incarnation || frame.source.generation != generation {
        return BoundaryApplyOutcome::AwaitingClientBinding;
    }
    if frame
        .living
        .avatar
        .as_ref()
        .is_none_or(|avatar| avatar.spatial_epoch != update.spatial_epoch)
    {
        return BoundaryApplyOutcome::AwaitingClientBinding;
    }
    if !super::cluster_remote_player_presentation::submit_movement(
        server,
        super::cluster_remote_player_presentation::RemotePlayerMovement {
            source: frame.source,
            update,
        },
    ) {
        return BoundaryApplyOutcome::AwaitingClientBinding;
    }
    BoundaryApplyOutcome::Applied
}

pub fn ghost_applied() -> u64 {
    GHOST_APPLIED.load(Ordering::Relaxed)
}
pub(super) fn record_movement_applied() {
    GHOST_APPLIED.fetch_add(1, Ordering::Relaxed);
}
pub fn ghost_datagrams() -> u64 {
    GHOST_DATAGRAMS.load(Ordering::Relaxed)
}

fn carries_entity_pos(bytes: &[u8]) -> bool {
    matches!(postcard::take_from_bytes::<u32>(bytes), Ok((magic, _)) if magic == ENTITY_POS_MAGIC)
}

pub fn apply_entity_pos_datagram(_server: &Arc<Server>, peer: ServerId, bytes: &[u8]) -> usize {
    let Ok(datagram) = postcard::from_bytes::<EntityPosDatagram>(bytes) else {
        return 0;
    };
    if !datagram.is_consistent() || datagram.is_empty() {
        return 0;
    }
    let delivered = datagram.len();
    if forward_entity_pos_datagram(peer, datagram.updates) {
        delivered
    } else {
        0
    }
}

fn apply_movement_batch(server: &Arc<Server>, peer: ServerId, datagram: MovementBatch) -> usize {
    let Some(generation) = server.cluster_peer_incarnations.load().get(&peer).copied() else {
        return 0;
    };
    let source = pumpkin_cluster::realm_completion::PeerGeneration {
        server: peer,
        generation,
    };
    let mut submitted = 0;
    for update in datagram
        .updates()
        .filter(|update| update.gid.server == peer && update.incarnation != 0)
    {
        if super::cluster_remote_player_presentation::submit_movement(
            server,
            super::cluster_remote_player_presentation::RemotePlayerMovement { source, update },
        ) {
            submitted += 1;
        }
    }
    submitted
}

pub fn spawn_ghost_apply(
    server: &Arc<Server>,
    _local: ServerId,
    mut datagram_in_rx: mpsc::Receiver<(ServerId, u64, Vec<u8>)>,
) {
    let server = server.clone();
    server.clone().spawn_task(async move {
        let mut item_decode_failures = HashMap::<
            (ServerId, pumpkin_cluster::item_spatial_wire::DecodeFailure),
            std::time::Instant,
        >::new();
        let mut movement_decode_failures = HashMap::<ServerId, std::time::Instant>::new();
        let mut movement_route_failures = HashMap::<ServerId, std::time::Instant>::new();
        while let Some((peer, generation, bytes)) = datagram_in_rx.recv().await {
            if server.cluster_peer_generations.load().get(&peer) != Some(&generation) {
                continue;
            }
            if bytes.starts_with(b"PCI1") {
                if let Err(reason) =
                    super::cluster_item_physics::route_datagram(&server, peer, generation, bytes)
                    && item_decode_failures
                        .get(&(peer, reason))
                        .is_none_or(|last| last.elapsed().as_secs() >= 60)
                {
                    tracing::warn!(
                        peer = peer.0,
                        ?reason,
                        "cluster item spatial datagram rejected"
                    );
                    item_decode_failures.insert((peer, reason), std::time::Instant::now());
                }
                continue;
            }
            GHOST_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            if carries_entity_pos(&bytes) {
                apply_entity_pos_datagram(&server, peer, &bytes);
                continue;
            }
            match MovementBatch::decode(bytes.into()) {
                Ok(batch) => {
                    let offered = batch
                        .updates()
                        .filter(|update| update.gid.server == peer && update.incarnation != 0)
                        .count();
                    let submitted = apply_movement_batch(&server, peer, batch);
                    if submitted < offered
                        && movement_route_failures
                            .get(&peer)
                            .is_none_or(|last| last.elapsed().as_secs() >= 300)
                    {
                        tracing::warn!(
                            peer = peer.0,
                            stream = "player-movement",
                            record_count = offered - submitted,
                            reason = "owned-route-backpressure",
                            "cluster movement datagram rejected"
                        );
                        movement_route_failures.insert(peer, std::time::Instant::now());
                    }
                }
                Err(reason) => {
                    if movement_decode_failures
                        .get(&peer)
                        .is_none_or(|last| last.elapsed().as_secs() >= 300)
                    {
                        tracing::warn!(
                            peer = peer.0,
                            stream = "player-movement",
                            record_count = 0,
                            ?reason,
                            "cluster movement datagram rejected"
                        );
                        movement_decode_failures.insert(peer, std::time::Instant::now());
                    }
                }
            }
        }
    });
}
