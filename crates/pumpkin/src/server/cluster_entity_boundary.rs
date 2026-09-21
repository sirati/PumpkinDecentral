use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use pumpkin_cluster::entities::{
    EntityBoundaryHandoff, EntityHandoff, EntityOrigin, EntitySpawnState,
    decode_entity_boundary_handoff, encode_entity_boundary_handoff,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_nbt::Nbt;
use pumpkin_nbt::deserializer::NbtReadHelperJava;
use pumpkin_util::math::get_section_cord;
use pumpkin_util::math::vector2::Vector2;
use tokio::sync::mpsc;
use tracing::warn;

use super::Server;
use super::cluster_entity_apply::{BoundaryMaterialization, queue_boundary_materialization};
use crate::entity::EntityBase;

struct BoundaryOutbox {
    local: ServerId,
    primary: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
}

#[derive(Clone)]
struct FrozenBoundary {
    handoff: EntityHandoff,
    state: FrozenState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FrozenState {
    Frozen,
    Prepared,
    Rejected,
}

enum BoundaryInput {
    Freeze(EntityHandoff),
    Inbound(InboundParcel),
    PrimaryChunkFull(pumpkin_cluster::protocol::ChunkAddr),
    Persisted {
        handoff: EntityHandoff,
        accepted: bool,
    },
}

struct PrimaryPersistenceRequest {
    handoff: EntityHandoff,
}

#[derive(Default)]
struct BoundaryRegistry {
    frozen: HashMap<pumpkin_cluster::protocol::EntityRef, FrozenBoundary>,
    primary_pending: HashMap<pumpkin_cluster::protocol::ChunkAddr, Vec<EntityHandoff>>,
}

impl BoundaryRegistry {
    fn freeze(&mut self, handoff: EntityHandoff) -> bool {
        let reference = handoff.spawn.entity;
        if self.frozen.contains_key(&reference) {
            return false;
        }
        self.frozen.insert(
            reference,
            FrozenBoundary {
                handoff,
                state: FrozenState::Frozen,
            },
        );
        true
    }

    fn prepared(&mut self, handoff: &EntityHandoff) -> bool {
        let Some(entry) = self.frozen.get_mut(&handoff.spawn.entity) else {
            return false;
        };
        if entry.handoff == *handoff && entry.state == FrozenState::Frozen {
            entry.state = FrozenState::Prepared;
            return true;
        }
        false
    }

    fn confirm(&mut self, origin: EntityOrigin, previous_owner: ServerId, successor: ServerId) -> Option<EntityHandoff> {
        let reference = self
            .frozen
            .iter()
            .find_map(|(reference, entry)| {
                (entry.handoff.origin == origin
                    && entry.handoff.previous_owner == previous_owner
                    && entry.handoff.successor == successor
                    && entry.state == FrozenState::Prepared)
                    .then_some(*reference)
            })?;
        self.frozen.remove(&reference).map(|entry| entry.handoff)
    }

    fn reject(&mut self, origin: EntityOrigin, previous_owner: ServerId, successor: ServerId) -> bool {
        let Some(entry) = self.frozen.values_mut().find(|entry| {
            entry.handoff.origin == origin
                && entry.handoff.previous_owner == previous_owner
                && entry.handoff.successor == successor
                && entry.state == FrozenState::Prepared
        }) else {
            return false;
        };
        entry.state = FrozenState::Rejected;
        true
    }

    fn restore_prepared(&mut self, handoff: EntityHandoff) {
        self.frozen.insert(
            handoff.spawn.entity,
            FrozenBoundary {
                handoff,
                state: FrozenState::Prepared,
            },
        );
    }

    fn queue_primary_persistence(&mut self, handoff: EntityHandoff) {
        self.primary_pending
            .entry(handoff.spawn.entity.chunk)
            .or_default()
            .push(handoff);
    }

    fn take_primary_persistence(
        &mut self,
        chunk: pumpkin_cluster::protocol::ChunkAddr,
    ) -> Vec<EntityHandoff> {
        self.primary_pending.remove(&chunk).unwrap_or_default()
    }
}

static BOUNDARY_OUTBOX: OnceLock<BoundaryOutbox> = OnceLock::new();
static BOUNDARY_BRIDGE: OnceLock<mpsc::Sender<BoundaryInput>> = OnceLock::new();
static FROZEN: std::sync::LazyLock<ArcSwap<HashSet<pumpkin_cluster::protocol::EntityRef>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(HashSet::new()));
static LAST_WARNING_MILLIS: AtomicU64 = AtomicU64::new(0);

fn warn_boundary(reason: &str, handoff: &EntityHandoff) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0);
    let last = LAST_WARNING_MILLIS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000
        || LAST_WARNING_MILLIS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    warn!(
        reason,
        origin_server = handoff.origin.server.0,
        origin_entity = handoff.origin.local_id,
        owner = handoff.previous_owner.0,
        successor = handoff.successor.0,
        chunk_x = handoff.spawn.entity.chunk.x,
        chunk_z = handoff.spawn.entity.chunk.z,
        "cluster boundary entity handoff retained"
    );
}

fn frozen_insert(reference: pumpkin_cluster::protocol::EntityRef) {
    FROZEN.rcu(|current| {
        let mut next = current.as_ref().clone();
        next.insert(reference);
        Arc::new(next)
    });
}

fn frozen_remove(reference: pumpkin_cluster::protocol::EntityRef) {
    FROZEN.rcu(|current| {
        let mut next = current.as_ref().clone();
        next.remove(&reference);
        Arc::new(next)
    });
}

pub fn is_frozen(reference: pumpkin_cluster::protocol::EntityRef) -> bool {
    FROZEN.load().contains(&reference)
}

pub fn install_boundary_outbox(
    local: ServerId,
    primary: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = BOUNDARY_OUTBOX.set(BoundaryOutbox {
        local,
        primary,
        outbound,
    });
}

pub fn notify_primary_chunk_full(chunk: pumpkin_cluster::protocol::ChunkAddr) {
    let Some(bridge) = BOUNDARY_BRIDGE.get() else {
        return;
    };
    let _ = bridge.try_send(BoundaryInput::PrimaryChunkFull(chunk));
}

fn exact_holder_or_primary(server: &Server, chunk: pumpkin_cluster::protocol::ChunkAddr) -> Option<ServerId> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let holder = super::cluster::chunk_holders(chunk)
        .into_iter()
        .filter(|peer| {
            *peer != local.0 && *peer != server.advanced_config.cluster.primary_server_id
        })
        .find(|peer| super::cluster::cluster_peer_admitted(*peer))
        .map(ServerId);
    holder.or(Some(ServerId(server.advanced_config.cluster.primary_server_id)))
}

pub fn freeze_on_unloaded_crossing(server: &Server, entity: &dyn EntityBase) {
    if !server.advanced_config.cluster.enabled || entity.get_player().is_some() {
        return;
    }
    let Some(outbox) = BOUNDARY_OUTBOX.get() else {
        return;
    };
    if outbox.local != ServerId(server.advanced_config.cluster.server_id) {
        return;
    }
    let inner = entity.get_entity();
    if inner.cluster_owner.load(Ordering::Relaxed) != outbox.local.0 {
        return;
    }
    let position = inner.pos.load();
    let chunk = pumpkin_cluster::protocol::ChunkAddr {
        x: get_section_cord(position.x.floor() as i32),
        z: get_section_cord(position.z.floor() as i32),
    };
    let world = inner.world.load();
    let local_chunk = Vector2::new(chunk.x, chunk.z);
    if world.level.is_cluster_full(&local_chunk) {
        return;
    }
    let Some(successor) = exact_holder_or_primary(server, chunk) else {
        return;
    };
    let Some(handoff) = super::cluster_entity_emit::boundary_handoff(server, entity, successor)
    else {
        return;
    };
    let Some(bridge) = BOUNDARY_BRIDGE.get() else {
        warn_boundary("actor-unavailable", &handoff);
        return;
    };
    match bridge.try_send(BoundaryInput::Freeze(handoff.clone())) {
        Ok(()) => frozen_insert(handoff.spawn.entity),
        Err(_) => {
            warn_boundary("actor-full", &handoff);
        }
    }
}

fn send_frame(outbox: &BoundaryOutbox, peer: ServerId, frame: EntityBoundaryHandoff) -> bool {
    let Ok(bytes) = encode_entity_boundary_handoff(&frame) else {
        return false;
    };
    outbox
        .outbound
        .try_send(OutboundParcel {
            peer,
            header: StreamHeader::new(pumpkin_cluster::protocol::StreamKind::Control, None),
            bytes,
        })
        .is_ok()
}

fn send_reject(outbox: &BoundaryOutbox, handoff: &EntityHandoff) {
    if !send_frame(
        outbox,
        handoff.previous_owner,
        EntityBoundaryHandoff::Reject {
            origin: handoff.origin,
            previous_owner: handoff.previous_owner,
            successor: handoff.successor,
        },
    ) {
        warn_boundary("reject-send-failed", handoff);
    }
}

fn send_confirm(outbox: &BoundaryOutbox, handoff: &EntityHandoff) {
    if !send_frame(
        outbox,
        handoff.previous_owner,
        EntityBoundaryHandoff::Confirm {
            origin: handoff.origin,
            previous_owner: handoff.previous_owner,
            successor: handoff.successor,
        },
    ) {
        warn_boundary("confirm-send-failed", handoff);
    }
}

fn persisted_nbt(handoff: &EntityHandoff) -> Option<pumpkin_nbt::NbtCompound> {
    let bytes = match &handoff.spawn.state {
        EntitySpawnState::Entity { nbt } => nbt,
        EntitySpawnState::ItemDrop { entity_nbt, .. } => entity_nbt,
        EntitySpawnState::Player(_) => return None,
    };
    let mut cursor = Cursor::new(bytes.as_slice());
    let mut reader = NbtReadHelperJava::new(&mut cursor);
    Nbt::read_unnamed(&mut reader).ok().map(|nbt| nbt.root_tag)
}

async fn persist_primary(
    server: &Arc<Server>,
    handoff: EntityHandoff,
) -> bool {
    let local_primary = ServerId(server.advanced_config.cluster.primary_server_id);
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
        || !(handoff.is_consistent()
            || (handoff.previous_owner == local_primary && handoff.successor == local_primary))
        || matches!(&handoff.spawn.state, EntitySpawnState::Player(_))
    {
        false
    } else if let Some(world) = server.worlds.load().first().cloned() {
        let pos = Vector2::new(handoff.spawn.entity.chunk.x, handoff.spawn.entity.chunk.z);
        let saved = if world.level.is_cluster_full(&pos) {
            if let Some(nbt) = persisted_nbt(&handoff) {
                world.level.persist_cluster_entity_nbt(pos, nbt).await
            } else {
                false
            }
        } else {
            false
        };
        world.level.unpin_cluster_chunk(&pos);
        world.level.should_unload.store(true, Ordering::Release);
        saved
    } else {
        false
    }
}

async fn primary_persistence_actor(
    server: Arc<Server>,
    mut jobs: mpsc::Receiver<PrimaryPersistenceRequest>,
    events: mpsc::Sender<BoundaryInput>,
) {
    while let Some(job) = jobs.recv().await {
        let accepted = persist_primary(&server, job.handoff.clone()).await;
        let _ = events.try_send(BoundaryInput::Persisted {
            handoff: job.handoff,
            accepted,
        });
    }
}

fn queue_primary_persistence_job(
    jobs: &mpsc::Sender<PrimaryPersistenceRequest>,
    events: &mpsc::Sender<BoundaryInput>,
    handoff: EntityHandoff,
) {
    if jobs
        .try_send(PrimaryPersistenceRequest {
            handoff: handoff.clone(),
        })
        .is_err()
    {
        let _ = events.try_send(BoundaryInput::Persisted {
            handoff,
            accepted: false,
        });
    }
}

fn process_prepare(
    server: &Arc<Server>,
    registry: &mut BoundaryRegistry,
    outbox: &BoundaryOutbox,
    persistence_jobs: &mpsc::Sender<PrimaryPersistenceRequest>,
    persistence_events: &mpsc::Sender<BoundaryInput>,
    materialization_events: &mpsc::Sender<BoundaryMaterialization>,
    peer: ServerId,
    handoff: EntityHandoff,
) {
    if peer != handoff.previous_owner
        || !handoff.is_consistent()
        || handoff.successor != outbox.local
        || matches!(&handoff.spawn.state, EntitySpawnState::Player(_))
    {
        warn_boundary("invalid-prepare", &handoff);
        return;
    }
    let chunk = Vector2::new(handoff.spawn.entity.chunk.x, handoff.spawn.entity.chunk.z);
    let local_full = server
        .worlds
        .load()
        .iter()
        .any(|world| world.level.is_cluster_full(&chunk));
    if outbox.local == outbox.primary {
        schedule_primary_persistence(
            server,
            registry,
            persistence_jobs,
            persistence_events,
            handoff,
        );
    } else if local_full {
        if !queue_boundary_materialization(handoff.clone(), materialization_events.clone()) {
            send_reject(outbox, &handoff);
            warn_boundary("materialize-queue-full", &handoff);
        }
    } else {
        send_reject(outbox, &handoff);
        warn_boundary("not-full-holder", &handoff);
    }
}

fn process_inbound(
    server: &Arc<Server>,
    registry: &mut BoundaryRegistry,
    outbox: &BoundaryOutbox,
    persistence_jobs: &mpsc::Sender<PrimaryPersistenceRequest>,
    persistence_events: &mpsc::Sender<BoundaryInput>,
    materialization_events: &mpsc::Sender<BoundaryMaterialization>,
    parcel: InboundParcel,
) {
    let Ok(frame) = decode_entity_boundary_handoff(&parcel.bytes) else {
        return;
    };
    match frame {
        EntityBoundaryHandoff::Prepare(handoff) => {
            process_prepare(
                server,
                registry,
                outbox,
                persistence_jobs,
                persistence_events,
                materialization_events,
                parcel.peer,
                handoff,
            );
        }
        EntityBoundaryHandoff::Confirm {
            origin,
            previous_owner,
            successor,
        } => {
            if parcel.peer != successor || previous_owner != outbox.local {
                return;
            }
            let Some(handoff) = registry.confirm(origin, previous_owner, successor) else {
                return;
            };
            frozen_remove(handoff.spawn.entity);
            let removed = super::cluster_entity_apply::remove_transferred_entities(
                server,
                outbox.local,
                std::slice::from_ref(&handoff),
            );
            if removed != 1 {
                frozen_insert(handoff.spawn.entity);
                registry.restore_prepared(handoff.clone());
                warn_boundary("source-removal-failed", &handoff);
            }
        }
        EntityBoundaryHandoff::Reject {
            origin,
            previous_owner,
            successor,
        } => {
            if parcel.peer == successor
                && previous_owner == outbox.local
                && !registry.reject(origin, previous_owner, successor)
            {
                return;
            }
        }
    }
}

fn schedule_primary_persistence(
    server: &Arc<Server>,
    registry: &mut BoundaryRegistry,
    persistence_jobs: &mpsc::Sender<PrimaryPersistenceRequest>,
    persistence_events: &mpsc::Sender<BoundaryInput>,
    handoff: EntityHandoff,
) {
    let pos = Vector2::new(handoff.spawn.entity.chunk.x, handoff.spawn.entity.chunk.z);
    let Some(world) = server.worlds.load().first().cloned() else {
        let _ = persistence_events.try_send(BoundaryInput::Persisted {
            handoff,
            accepted: false,
        });
        return;
    };
    world.level.pin_cluster_chunk(pos);
    if world.level.is_cluster_full(&pos) {
        queue_primary_persistence_job(persistence_jobs, persistence_events, handoff);
        return;
    }
    registry.queue_primary_persistence(handoff.clone());
    super::cluster::request_primary_chunk_load(server, handoff.spawn.entity.chunk);
}

async fn boundary_actor(server: Arc<Server>, mut inbound: mpsc::Receiver<InboundParcel>) {
    let (input_tx, mut input_rx) = mpsc::channel(256);
    let (materialization_tx, mut materialization_rx) = mpsc::channel(256);
    let (persistence_tx, persistence_rx) = mpsc::channel(256);
    server.spawn_task(primary_persistence_actor(
        Arc::clone(&server),
        persistence_rx,
        input_tx.clone(),
    ));
    let _ = BOUNDARY_BRIDGE.set(input_tx.clone());
    let mut registry = BoundaryRegistry::default();
    loop {
        tokio::select! {
            parcel = inbound.recv() => {
                let Some(parcel) = parcel else { break };
                if input_tx.try_send(BoundaryInput::Inbound(parcel)).is_err() {
                    warn!("cluster boundary entity inbound queue full");
                }
            }
            input = input_rx.recv() => {
                let Some(input) = input else { break };
                let Some(outbox) = BOUNDARY_OUTBOX.get() else { continue };
                match input {
                    BoundaryInput::Freeze(handoff) => {
                        if !registry.freeze(handoff.clone()) {
                            continue;
                        }
                        if handoff.successor == outbox.local && outbox.local == outbox.primary {
                            schedule_primary_persistence(
                                &server,
                                &mut registry,
                                &persistence_tx,
                                &input_tx,
                                handoff,
                            );
                            continue;
                        }
                        if !send_frame(outbox, handoff.successor, EntityBoundaryHandoff::Prepare(handoff.clone())) {
                            registry.frozen.remove(&handoff.spawn.entity);
                            frozen_remove(handoff.spawn.entity);
                            warn_boundary("prepare-send-failed", &handoff);
                            continue;
                        }
                        if !registry.prepared(&handoff) {
                            warn_boundary("prepare-state-lost", &handoff);
                        }
                    }
                    BoundaryInput::Inbound(parcel) => {
                        process_inbound(
                            &server,
                            &mut registry,
                            outbox,
                            &persistence_tx,
                            &input_tx,
                            &materialization_tx,
                            parcel,
                        );
                    }
                    BoundaryInput::PrimaryChunkFull(chunk) => {
                        for handoff in registry.take_primary_persistence(chunk) {
                            queue_primary_persistence_job(&persistence_tx, &input_tx, handoff);
                        }
                    }
                    BoundaryInput::Persisted { handoff, accepted } => {
                        if accepted
                            && handoff.previous_owner == outbox.local
                            && handoff.successor == outbox.local
                        {
                            let removed = super::cluster_entity_apply::remove_primary_persisted_boundary_entity(
                                &server,
                                outbox.local,
                                handoff.spawn.entity,
                            );
                            if removed {
                                registry.frozen.remove(&handoff.spawn.entity);
                                frozen_remove(handoff.spawn.entity);
                            } else {
                                warn_boundary("primary-source-removal-failed", &handoff);
                            }
                        } else if accepted {
                            send_confirm(outbox, &handoff);
                        } else {
                            send_reject(outbox, &handoff);
                            warn_boundary("primary-persist-rejected", &handoff);
                        }
                    }
                }
            }
            materialized = materialization_rx.recv() => {
                let Some(result) = materialized else { break };
                let Some(outbox) = BOUNDARY_OUTBOX.get() else { continue };
                if result.accepted {
                    send_confirm(outbox, &result.handoff);
                } else {
                    send_reject(outbox, &result.handoff);
                    warn_boundary("materialize-rejected", &result.handoff);
                }
            }
        }
    }
}

pub fn spawn_boundary_actor(server: &Arc<Server>, inbound: mpsc::Receiver<InboundParcel>) {
    server.spawn_task(boundary_actor(Arc::clone(server), inbound));
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::entities::{EntityOrigin, EntitySpawn};
    use pumpkin_cluster::protocol::{ChunkAddr, EntityRef};
    use pumpkin_cluster::time::TickStamp;

    fn handoff() -> EntityHandoff {
        EntityHandoff {
            origin: EntityOrigin { server: ServerId(1), local_id: 7 },
            previous_owner: ServerId(1),
            successor: ServerId(2),
            spawn: EntitySpawn {
                entity: EntityRef {
                    origin: ServerId(1),
                    owner: ServerId(1),
                    local_id: 7,
                    chunk: ChunkAddr { x: 4, z: 9 },
                },
                tick: TickStamp(5),
                kind: 1,
                pos: [1.0, 2.0, 3.0],
                yaw: 0.0,
                pitch: 0.0,
                state: EntitySpawnState::Entity { nbt: Vec::new() },
            },
            velocity: [0.0, 0.0, 0.0],
        }
    }

    #[test]
    fn confirmation_removes_only_the_matching_prepared_entity() {
        let handoff = handoff();
        let mut registry = BoundaryRegistry::default();
        assert!(registry.freeze(handoff.clone()));
        assert!(registry.prepared(&handoff));
        assert!(registry
            .confirm(handoff.origin, handoff.previous_owner, ServerId(3))
            .is_none());
        assert_eq!(
            registry.confirm(handoff.origin, handoff.previous_owner, handoff.successor),
            Some(handoff),
        );
    }

    #[test]
    fn rejection_preserves_the_frozen_source_without_retrying() {
        let handoff = handoff();
        let mut registry = BoundaryRegistry::default();
        assert!(registry.freeze(handoff.clone()));
        assert!(registry.prepared(&handoff));
        assert!(registry.reject(handoff.origin, handoff.previous_owner, handoff.successor));
        assert!(registry
            .confirm(handoff.origin, handoff.previous_owner, handoff.successor)
            .is_none());
        assert!(!registry.freeze(handoff));
    }

    #[test]
    fn primary_persistence_releases_only_the_loaded_chunk() {
        let first = handoff();
        let mut second = handoff();
        second.spawn.entity.chunk = ChunkAddr { x: 5, z: 9 };
        let mut registry = BoundaryRegistry::default();
        registry.queue_primary_persistence(first.clone());
        registry.queue_primary_persistence(second.clone());
        assert_eq!(
            registry.take_primary_persistence(first.spawn.entity.chunk),
            vec![first]
        );
        assert_eq!(
            registry.take_primary_persistence(second.spawn.entity.chunk),
            vec![second]
        );
    }
}
