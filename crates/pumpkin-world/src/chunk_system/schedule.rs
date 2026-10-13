use super::channel::LevelChange;
use super::chunk_holder::ChunkHolder;
use super::chunk_state::{Chunk, InitialChunkPalettes, StagedChunkEnum};
use super::dag::{DAG, EdgeKey, Node, NodeKey};
use super::generation_cache::{Cache, SurfaceBiomeNeighborhood};
use super::worker_logic::{RecvChunk, io_read_work, io_write_work};
use super::{
    ChunkLevel, ChunkListener, ChunkLoading, ChunkPos, HashMapType, HashSetType, IOLock,
    LevelChannel,
};
use crate::chunk::io::Dirtiable;
use crate::level::{
    ClusterSnapshotPublication, FreshChunkAdmission, Level, LoadedChunkChange,
    PersistedChunkAdmission, SyncChunk,
};
use dashmap::DashMap;
use pumpkin_config::lighting::LightingEngineConfig;
use pumpkin_util::math::vector2::Vector2;
use slotmap::Key;
use std::cmp::{Ordering, max};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::mem::swap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

pub(crate) struct TaskHeapNode(i8, NodeKey);
impl PartialEq for TaskHeapNode {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl TaskHeapNode {
    #[cfg(test)]
    pub(crate) const fn node_key(&self) -> NodeKey {
        self.1
    }
}
impl Eq for TaskHeapNode {}
impl PartialOrd for TaskHeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TaskHeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

pub struct GenerationSchedule {
    queue: BinaryHeap<TaskHeapNode>,
    parked_full: BinaryHeap<TaskHeapNode>,
    graph: DAG,

    last_level: ChunkLevel,
    player_level: ChunkLevel,
    owner_demands: HashMapType<ChunkPos, HashSetType<u64>>,
    peer_request_retention: HashSetType<ChunkPos>,
    owner_ready_routes: HashMapType<
        ChunkPos,
        HashMapType<u64, tokio::sync::mpsc::Sender<crate::level::OwnerChunkReady>>,
    >,
    pending_owner_ready: VecDeque<(ChunkPos, u64, u64)>,
    publication_generations: HashMapType<ChunkPos, u64>,
    next_publication_generation: u64,
    owner_demand_rx: crossbeam::channel::Receiver<crate::level::OwnerChunkDemand>,
    deferred_primary_io: HashMapType<ChunkPos, Chunk>,
    last_high_priority: Vec<ChunkPos>,
    send_level: Arc<LevelChannel>,

    public_chunk_map: Arc<DashMap<Vector2<i32>, SyncChunk>>,
    loaded_chunk_changes: Arc<crossbeam::queue::SegQueue<LoadedChunkChange>>,
    chunk_map: HashMap<ChunkPos, ChunkHolder>,
    unload_chunks: HashSetType<ChunkPos>,

    /// Tasks that are graph-ready (`in_degree` == 0) but cannot yet run because
    /// one or more of their required neighbor chunks haven't been delivered yet.
    /// Parked here and re-queued by `check_waiting_tasks()` as chunk data arrives.
    waiting_for_chunks: HashSetType<NodeKey>,

    cluster_fetches: HashMapType<ChunkPos, Instant>,
    cluster_level: Option<Arc<Level>>,
    pending_fresh: HashMapType<ChunkPos, PendingFresh>,
    pending_persisted: HashMapType<ChunkPos, PendingPersisted>,
    pending_persisted_decisions: VecDeque<crate::level::PersistedChunkDecision>,
    next_fresh_generation: u64,
    fresh_wake_tx: crossbeam::channel::Sender<()>,
    fresh_wake_rx: crossbeam::channel::Receiver<()>,
    cluster_publications: crossbeam::channel::Receiver<ClusterSnapshotPublication>,
    peer_install_claims: crossbeam::channel::Receiver<crate::level::PeerChunkInstallClaim>,
    pending_claim_results: VecDeque<(
        tokio::sync::mpsc::Sender<crate::level::PeerChunkInstallClaimResult>,
        crate::level::PeerChunkInstallClaimResult,
    )>,

    io_lock: IOLock,
    running_task_count: u16,
    max_in_flight: u16,
    queue_dirty: bool,
    recv_chunk: crossbeam::channel::Receiver<(ChunkPos, RecvChunk)>,
    io_read: tokio::sync::mpsc::Sender<Vec<ChunkPos>>,
    io_write: tokio::sync::mpsc::Sender<Vec<(ChunkPos, Chunk)>>,
    send_chunk: crossbeam::channel::Sender<(ChunkPos, RecvChunk)>,
    listener: Arc<ChunkListener>,
    lighting_config: LightingEngineConfig,
    last_unload: std::time::Instant,
    generation_pool: Option<Arc<rayon::ThreadPool>>,
}

struct PendingFresh {
    chunk: SyncChunk,
    image: std::sync::Arc<[u8]>,
    palettes: Option<InitialChunkPalettes>,
    generation: u64,
    response: Option<tokio::sync::oneshot::Receiver<bool>>,
    abort: bool,
}

struct PendingPersisted {
    original: SyncChunk,
    approved: Option<SyncChunk>,
    generation: u64,
    response: Option<tokio::sync::oneshot::Receiver<Option<SyncChunk>>>,
    submitted: bool,
    rejected: bool,
    terminal_rejection: bool,
    peer_claimed: bool,
    unexpected_arrival_reported: bool,
}

fn publish_persisted_reservation(
    reservations: &arc_swap::ArcSwap<
        rustc_hash::FxHashMap<ChunkPos, crate::level::PersistedChunkReservation>,
    >,
    pos: ChunkPos,
    reservation: Option<crate::level::PersistedChunkReservation>,
) {
    let mut next = (**reservations.load()).clone();
    if let Some(reservation) = reservation {
        next.insert(pos, reservation);
    } else {
        next.remove(&pos);
    }
    reservations.store(Arc::new(next));
}

impl GenerationSchedule {
    fn flush_peer_claim_results(&mut self) {
        while let Some((respond, result)) = self.pending_claim_results.pop_front() {
            match respond.try_send(result) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(result)) => {
                    self.pending_claim_results.push_front((respond, result));
                    break;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(result)) => {
                    if let Some(pending) = self.pending_persisted.get_mut(&result.position)
                        && pending.generation == result.generation
                    {
                        pending.peer_claimed = false;
                    }
                }
            }
        }
    }

    fn poll_peer_install_claims(&mut self) {
        self.flush_peer_claim_results();
        let mut drained = false;
        if let Some(level) = self.cluster_level.as_ref() {
            for _ in 0..1_024 {
                let Some((position, generation)) = level.cluster_peer_install_releases.pop() else {
                    break;
                };
                drained = true;
                if let Some(pending) = self.pending_persisted.get_mut(&position)
                    && pending.generation == generation
                {
                    pending.peer_claimed = false;
                }
            }
        }
        for _ in 0..256 {
            if self.pending_claim_results.len() >= 256 {
                break;
            }
            let Ok(claim) = self.peer_install_claims.try_recv() else {
                break;
            };
            drained = true;
            self.process_peer_install_claim(claim);
        }
        self.flush_peer_claim_results();
        if drained
            && let Some(level) = self.cluster_level.as_ref()
            && let Some(route) = level.cluster_peer_claim_capacity.load_full()
        {
            let _ = route.try_send(());
        }
    }

    fn process_peer_install_claim(&mut self, claim: crate::level::PeerChunkInstallClaim) {
        match claim {
            crate::level::PeerChunkInstallClaim::Claim {
                position,
                generation,
                respond,
            } => {
                let private = self.pending_persisted.get(&position).is_some_and(|pending| {
                        self.chunk_map.get(&position).is_some_and(|holder| {
                            !holder.public && matches!(holder.chunk.as_ref(), Some(Chunk::Level(chunk)) if Arc::ptr_eq(chunk, &pending.original))
                        })
                    });
                let recovery = self.cluster_level.as_ref().is_some_and(|level| {
                    level
                        .cluster_publication_reservations
                        .load()
                        .get(&position)
                        .is_some_and(|reservation| {
                            reservation.generation == generation
                                && reservation.state
                                    == crate::level::PersistedChunkReservationState::Recovery
                        })
                });
                let allowed = self
                    .pending_persisted
                    .get_mut(&position)
                    .is_some_and(|pending| {
                        if !private
                            || !recovery
                            || pending.generation != generation
                            || !pending.rejected
                            || !pending.terminal_rejection
                            || pending.peer_claimed
                        {
                            return false;
                        }
                        pending.peer_claimed = true;
                        true
                    });
                if let Some(level) = self.cluster_level.as_ref() {
                    self.pending_claim_results.push_back((
                        respond,
                        crate::level::PeerChunkInstallClaimResult {
                            world_key: level.cluster_world_key,
                            dimension: level.cluster_dimension,
                            position,
                            generation,
                            allowed,
                        },
                    ));
                }
            }
            crate::level::PeerChunkInstallClaim::Release {
                position,
                generation,
            } => {
                if let Some(pending) = self.pending_persisted.get_mut(&position)
                    && pending.generation == generation
                {
                    pending.peer_claimed = false;
                }
            }
        }
        self.flush_peer_claim_results();
    }

    fn flush_persisted_decisions(&mut self) {
        let Some(level) = self.cluster_level.as_ref() else {
            return;
        };
        let Some(route) = level.cluster_persisted_decision.load_full() else {
            return;
        };
        while let Some(decision) = self.pending_persisted_decisions.pop_front() {
            match route.try_send(decision) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(decision)) => {
                    self.pending_persisted_decisions.push_front(decision);
                    break;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
    }

    fn apply_cluster_publication(&mut self, publication: ClusterSnapshotPublication) {
        let pos = publication.position;
        let snapshot = publication.snapshot;
        if crate::level::is_cluster_secondary() {
            self.secondary_cluster_arrive(pos, snapshot.clone());
        } else if self.pending_persisted.get(&pos).is_some_and(|pending| {
            publication.reservation_generation == Some(pending.generation)
                && pending.rejected
                && pending.peer_claimed
        }) && self.cluster_level.as_ref().is_some_and(|level| {
            level
                .cluster_publication_reservations
                .load()
                .get(&pos)
                .is_some_and(|reservation| {
                    reservation.generation == publication.reservation_generation.unwrap_or_default()
                        && reservation.state
                            == crate::level::PersistedChunkReservationState::Recovery
                })
        }) {
            let private = self.pending_persisted.get(&pos).is_some_and(|pending| {
                self.chunk_map.get(&pos).is_some_and(|holder| {
                    !holder.public
                        && matches!(holder.chunk.as_ref(), Some(Chunk::Level(chunk)) if Arc::ptr_eq(chunk, &pending.original))
                })
            });
            if private {
                self.pending_persisted.remove(&pos);
                if let Some(level) = self.cluster_level.as_ref() {
                    publish_persisted_reservation(
                        &level.cluster_publication_reservations,
                        pos,
                        None,
                    );
                }
                self.cluster_fetches.remove(&pos);
                if let Some(level) = self.cluster_level.as_ref() {
                    level.clear_cluster_fetch_wanted(&pos);
                }
                if let Some(mut holder) = self.chunk_map.remove(&pos) {
                    let stage = StagedChunkEnum::Full;
                    self.drop_satisfied_tasks(&mut holder, stage);
                    holder.current_stage = stage;
                    if !holder.occupied.is_null() && self.graph.nodes.contains_key(holder.occupied)
                    {
                        self.drop_node(holder.occupied);
                    }
                    holder.occupied = NodeKey::null();
                    holder.chunk = Some(Chunk::Level(snapshot.clone()));
                    holder.public = true;
                    self.publish_chunk(pos, snapshot.clone());
                    self.listener.process_new_chunk(pos, &snapshot);
                    self.chunk_map.insert(pos, holder);
                    self.check_waiting_tasks(false);
                }
            }
        }
        let published = self
            .public_chunk_map
            .get(&pos)
            .is_some_and(|current| Arc::ptr_eq(current.value(), &snapshot));
        let _ = publication.respond.send(published);
    }

    fn poll_cluster_publications(&mut self) {
        for _ in 0..256 {
            let Ok(publication) = self.cluster_publications.try_recv() else {
                break;
            };
            self.apply_cluster_publication(publication);
        }
    }

    fn poll_persisted_admissions(&mut self) {
        self.flush_persisted_decisions();
        let Some(level) = self.cluster_level.as_ref() else {
            return;
        };
        let Some(route) = level.persisted_chunk_admission.load_full() else {
            return;
        };
        let world_key = level.cluster_world_key;
        let dimension = level.cluster_dimension;
        let publication_reservations = level.cluster_publication_reservations.clone();
        for pos in self.pending_persisted.keys().copied().collect::<Vec<_>>() {
            if self.pending_persisted_decisions.len() >= 4_096 {
                break;
            }
            let Some(mut pending) = self.pending_persisted.remove(&pos) else {
                continue;
            };
            if pending.rejected {
                self.pending_persisted.insert(pos, pending);
                continue;
            }
            if !pending.submitted {
                let (respond, response) = tokio::sync::oneshot::channel();
                match route.try_send(PersistedChunkAdmission::Prepare {
                    world_key,
                    dimension,
                    position: pos,
                    generation: pending.generation,
                    original: pending.original.clone(),
                    respond,
                    wake: self.fresh_wake_tx.clone(),
                }) {
                    Ok(()) => {
                        pending.response = Some(response);
                        pending.submitted = true;
                    }
                    Err(_) => {
                        self.pending_persisted.insert(pos, pending);
                        continue;
                    }
                }
            }
            if let Some(response) = pending.response.as_mut() {
                match response.try_recv() {
                    Ok(Some(approved)) => {
                        pending.approved = Some(approved);
                        pending.response = None;
                        if let Some(mut reservation) =
                            publication_reservations.load().get(&pos).copied()
                            && reservation.generation == pending.generation
                        {
                            reservation.state =
                                crate::level::PersistedChunkReservationState::Approved;
                            publish_persisted_reservation(
                                &publication_reservations,
                                pos,
                                Some(reservation),
                            );
                        }
                        self.pending_persisted_decisions.push_back(
                            crate::level::PersistedChunkDecision {
                                world_key,
                                dimension,
                                position: pos,
                                generation: pending.generation,
                                recovery: false,
                            },
                        );
                    }
                    Ok(None) => {
                        pending.rejected = true;
                        pending.terminal_rejection = true;
                        if let Some(mut reservation) =
                            publication_reservations.load().get(&pos).copied()
                            && reservation.generation == pending.generation
                        {
                            reservation.state =
                                crate::level::PersistedChunkReservationState::Recovery;
                            publish_persisted_reservation(
                                &publication_reservations,
                                pos,
                                Some(reservation),
                            );
                        }
                        self.pending_persisted_decisions.push_back(
                            crate::level::PersistedChunkDecision {
                                world_key,
                                dimension,
                                position: pos,
                                generation: pending.generation,
                                recovery: true,
                            },
                        );
                        error!(
                            x = pos.x,
                            z = pos.y,
                            generation = pending.generation,
                            "cluster persisted chunk admission rejected"
                        );
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        pending.rejected = true;
                        if let Some(mut reservation) =
                            publication_reservations.load().get(&pos).copied()
                            && reservation.generation == pending.generation
                        {
                            reservation.state = crate::level::PersistedChunkReservationState::Lost;
                            publish_persisted_reservation(
                                &publication_reservations,
                                pos,
                                Some(reservation),
                            );
                        }
                        self.pending_persisted_decisions.push_back(
                            crate::level::PersistedChunkDecision {
                                world_key,
                                dimension,
                                position: pos,
                                generation: pending.generation,
                                recovery: false,
                            },
                        );
                        error!(
                            x = pos.x,
                            z = pos.y,
                            generation = pending.generation,
                            "cluster persisted chunk admission outcome was lost"
                        );
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                }
            }
            if let Some(approved) = pending.approved.as_ref() {
                let same_private_chunk = self.chunk_map.get(&pos).is_some_and(|holder| {
                    !holder.public
                        && matches!(holder.chunk.as_ref(), Some(Chunk::Level(chunk)) if Arc::ptr_eq(chunk, &pending.original))
                });
                if !same_private_chunk {
                    pending.rejected = true;
                    error!(
                        x = pos.x,
                        z = pos.y,
                        generation = pending.generation,
                        "cluster persisted chunk changed during admission"
                    );
                } else if let Ok(permit) = route.try_reserve() {
                    let approved = approved.clone();
                    if let Some(holder) = self.chunk_map.get_mut(&pos) {
                        holder.chunk = Some(Chunk::Level(approved.clone()));
                        holder.public = true;
                    }
                    self.publish_chunk(pos, approved.clone());
                    permit.send(PersistedChunkAdmission::Published {
                        world_key,
                        dimension,
                        position: pos,
                        generation: pending.generation,
                    });
                    self.listener.process_new_chunk(pos, &approved);
                    publish_persisted_reservation(&publication_reservations, pos, None);
                    self.check_waiting_tasks(crate::level::is_cluster_secondary());
                    continue;
                }
            }
            self.pending_persisted.insert(pos, pending);
        }
        self.flush_persisted_decisions();
    }

    fn resume_parked_full(&mut self) {
        let available = 128usize.saturating_sub(
            self.pending_fresh.len()
                + self.pending_persisted.len()
                + usize::from(self.running_task_count),
        );
        for _ in 0..available {
            let Some(mut task) = self.parked_full.pop() else {
                break;
            };
            if let Some(node) = self.graph.nodes.get(task.1) {
                task.0 = Self::calc_priority(
                    &self.last_level,
                    &self.last_high_priority,
                    node.pos,
                    node.stage,
                );
                self.queue.push(task);
            }
        }
    }

    fn poll_fresh_admissions(&mut self) {
        let Some(level) = self.cluster_level.as_ref() else {
            return;
        };
        let Some(route) = level.fresh_chunk_admission.load_full() else {
            return;
        };
        let world_key = level.cluster_world_key;
        let dimension = level.cluster_dimension;
        for pos in self.pending_fresh.keys().copied().collect::<Vec<_>>() {
            let Some(mut pending) = self.pending_fresh.remove(&pos) else {
                continue;
            };
            if pending.abort {
                if route
                    .try_send(FreshChunkAdmission::Aborted {
                        world_key,
                        dimension,
                        position: pos,
                        generation: pending.generation,
                    })
                    .is_err()
                {
                    self.pending_fresh.insert(pos, pending);
                }
                continue;
            }
            if pending.response.is_none() && pending.palettes.is_some() {
                let palettes = pending.palettes.take().expect("checked above");
                let (respond, response) = tokio::sync::oneshot::channel();
                match route.try_send(FreshChunkAdmission::Prepare {
                    world_key,
                    dimension,
                    position: pos,
                    generation: pending.generation,
                    image: pending.image.clone(),
                    palettes,
                    respond,
                    wake: self.fresh_wake_tx.clone(),
                }) {
                    Ok(()) => pending.response = Some(response),
                    Err(tokio::sync::mpsc::error::TrySendError::Full(
                        FreshChunkAdmission::Prepare { palettes, .. },
                    ))
                    | Err(tokio::sync::mpsc::error::TrySendError::Closed(
                        FreshChunkAdmission::Prepare { palettes, .. },
                    )) => pending.palettes = Some(palettes),
                    Err(_) => unreachable!(),
                }
            }
            if let Some(response) = pending.response.as_mut() {
                match response.try_recv() {
                    Ok(true) => pending.response = None,
                    Ok(false) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        error!(
                            x = pos.x,
                            z = pos.y,
                            generation = pending.generation,
                            "cluster fresh chunk admission rejected"
                        );
                        continue;
                    }
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                }
            }
            if pending.response.is_none() && pending.palettes.is_none() {
                let same_private_chunk = self.chunk_map.get(&pos).is_some_and(|holder| {
                    !holder.public
                        && matches!(
                            holder.chunk.as_ref(),
                            Some(Chunk::Level(chunk)) if Arc::ptr_eq(chunk, &pending.chunk)
                        )
                });
                if !same_private_chunk {
                    error!(
                        x = pos.x,
                        z = pos.y,
                        generation = pending.generation,
                        "cluster fresh chunk changed during admission"
                    );
                    pending.abort = true;
                    self.pending_fresh.insert(pos, pending);
                    continue;
                }
                let Ok(permit) = route.try_reserve() else {
                    self.pending_fresh.insert(pos, pending);
                    continue;
                };
                self.publish_chunk(pos, pending.chunk.clone());
                if let Some(holder) = self.chunk_map.get_mut(&pos) {
                    holder.public = true;
                    if holder.target_stage == StagedChunkEnum::None
                        && holder.dependency_stage == StagedChunkEnum::None
                    {
                        self.unload_chunks.insert(pos);
                    }
                }
                permit.send(FreshChunkAdmission::Published {
                    world_key,
                    dimension,
                    position: pos,
                    generation: pending.generation,
                });
                self.listener.process_new_chunk(pos, &pending.chunk);
                self.check_waiting_tasks(crate::level::is_cluster_secondary());
                continue;
            }
            self.pending_fresh.insert(pos, pending);
        }
        self.resume_parked_full();
    }

    fn publish_chunk(&mut self, pos: ChunkPos, chunk: SyncChunk) -> Option<SyncChunk> {
        let previous = self.public_chunk_map.insert(pos, chunk);
        if let Some(level) = self
            .cluster_level
            .as_ref()
            .filter(|level| level.cluster_genesis_required)
        {
            level.install_primary_chunk_fence(pos);
        }
        self.next_publication_generation = self.next_publication_generation.wrapping_add(1).max(1);
        let generation = self.next_publication_generation;
        self.publication_generations.insert(pos, generation);
        if let Some(routes) = self.owner_ready_routes.get(&pos) {
            self.pending_owner_ready
                .extend(routes.keys().copied().map(|lease| (pos, lease, generation)));
        }
        if previous.is_none() {
            self.loaded_chunk_changes
                .push(LoadedChunkChange::Loaded(pos));
        }
        previous
    }

    fn unpublish_chunk(&mut self, pos: ChunkPos) -> Option<SyncChunk> {
        let removed = self.public_chunk_map.remove(&pos).map(|(_, chunk)| chunk);
        if let Some(level) = self
            .cluster_level
            .as_ref()
            .filter(|level| level.cluster_genesis_required)
        {
            level.retire_primary_chunk_fence(pos);
        }
        self.publication_generations.remove(&pos);
        if removed.is_some() {
            self.loaded_chunk_changes
                .push(LoadedChunkChange::Unloaded(pos));
        }
        removed
    }

    fn flush_owner_ready(&mut self) {
        let count = self.pending_owner_ready.len();
        for _ in 0..count {
            let Some((position, lease, generation)) = self.pending_owner_ready.pop_front() else {
                break;
            };
            if self.publication_generations.get(&position) != Some(&generation)
                || !self
                    .owner_demands
                    .get(&position)
                    .is_some_and(|leases| leases.contains(&lease))
            {
                continue;
            }
            let Some(route) = self
                .owner_ready_routes
                .get(&position)
                .and_then(|routes| routes.get(&lease))
            else {
                continue;
            };
            match route.try_send(crate::level::OwnerChunkReady {
                world_key: self
                    .cluster_level
                    .as_ref()
                    .map_or(0, |level| level.cluster_world_key),
                dimension: self
                    .cluster_level
                    .as_ref()
                    .map_or(0, |level| level.cluster_dimension),
                position,
                lease,
                publication_generation: generation,
            }) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.pending_owner_ready
                        .push_back((position, lease, generation));
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    if let Some(routes) = self.owner_ready_routes.get_mut(&position) {
                        routes.remove(&lease);
                        if routes.is_empty() {
                            self.owner_ready_routes.remove(&position);
                        }
                    }
                }
            }
        }
    }

    pub fn create(
        io_read_thread_count: usize,
        level: Arc<Level>,
        level_channel: Arc<LevelChannel>,
        listener: Arc<ChunkListener>,
        thread_tracker: &mut Vec<thread::JoinHandle<()>>,
    ) {
        let (send_chunk, recv_chunk) = crossbeam::channel::unbounded();
        let (fresh_wake_tx, fresh_wake_rx) = crossbeam::channel::bounded(1);
        let (cluster_publication_tx, cluster_publications) = crossbeam::channel::bounded(256);
        let (peer_install_claim_tx, peer_install_claims) = crossbeam::channel::bounded(256);
        let (owner_demand_tx, owner_demand_rx) = crossbeam::channel::bounded(1_024);
        level
            .cluster_snapshot_publication
            .store(Some(Arc::new(cluster_publication_tx)));
        level
            .cluster_peer_install_claim
            .store(Some(Arc::new(peer_install_claim_tx)));
        level
            .cluster_owner_chunk_demand
            .store(Some(Arc::new(owner_demand_tx)));
        level
            .fresh_chunk_wake
            .store(Some(Arc::new(fresh_wake_tx.clone())));

        let (send_read_io, recv_read_io) = tokio::sync::mpsc::channel(io_read_thread_count + 5);
        let recv_read_io = Arc::new(tokio::sync::Mutex::new(recv_read_io));

        let (send_write_io, recv_write_io) = tokio::sync::mpsc::channel(500);

        let io_lock = Arc::new((
            Mutex::new(HashMapType::default()),
            tokio::sync::Notify::new(),
        ));

        for _ in 0..io_read_thread_count {
            level.chunk_system_tasks.spawn(io_read_work(
                recv_read_io.clone(),
                send_chunk.clone(),
                level.clone(),
                io_lock.clone(),
            ));
        }

        level.chunk_system_tasks.spawn(io_write_work(
            recv_write_io,
            level.clone(),
            io_lock.clone(),
        ));

        let cpus = thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let gen_threads = (cpus / 2).clamp(2, 16);
        let generation_pool = if crate::level::is_cluster_secondary() {
            info!(
                "Cluster secondary scheduler starting in no-generate mode: local chunk generation is disabled, chunks must arrive from the holding peer"
            );
            None
        } else {
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(gen_threads)
                    .thread_name(|i| format!("ChunkGen-{i}"))
                    .build()
                    .expect("Failed to build Chunk Generation ThreadPool"),
            ))
        };
        let max_in_flight = (gen_threads * 2) as u16;

        let level_sched = level;
        let lighting_config = level_sched.lighting_config;
        let handle = thread::Builder::new()
            .name("Schedule".to_string())
            .spawn(move || {
                let scheduler = Self {
                    queue: BinaryHeap::new(),
                    parked_full: BinaryHeap::new(),
                    graph: DAG::default(),
                    last_level: ChunkLevel::default(),
                    player_level: ChunkLevel::default(),
                    owner_demands: HashMapType::default(),
                    peer_request_retention: HashSetType::default(),
                    owner_ready_routes: HashMapType::default(),
                    pending_owner_ready: VecDeque::new(),
                    publication_generations: HashMapType::default(),
                    next_publication_generation: 1,
                    owner_demand_rx,
                    deferred_primary_io: HashMapType::default(),
                    last_high_priority: Vec::new(),
                    send_level: level_channel,
                    public_chunk_map: level_sched.loaded_chunks.clone(),
                    loaded_chunk_changes: level_sched.loaded_chunk_changes.clone(),
                    unload_chunks: HashSetType::default(),
                    waiting_for_chunks: HashSetType::default(),
                    cluster_fetches: HashMapType::default(),
                    cluster_level: Some(level_sched.clone()),
                    pending_fresh: HashMapType::default(),
                    pending_persisted: HashMapType::default(),
                    pending_persisted_decisions: VecDeque::new(),
                    next_fresh_generation: 1,
                    fresh_wake_tx,
                    fresh_wake_rx,
                    cluster_publications,
                    peer_install_claims,
                    pending_claim_results: VecDeque::new(),
                    io_lock,
                    running_task_count: 0,
                    max_in_flight,
                    queue_dirty: false,
                    recv_chunk,
                    io_read: send_read_io,
                    io_write: send_write_io,
                    send_chunk,
                    listener,
                    chunk_map: HashMap::default(),
                    lighting_config,
                    last_unload: std::time::Instant::now(),
                    generation_pool,
                };
                scheduler.work(&level_sched);
            })
            .expect("Failed to spawn Scheduler Thread");

        thread_tracker.push(handle);
    }

    fn apply_lighting_override(&self, chunk: &SyncChunk) {
        match self.lighting_config {
            LightingEngineConfig::Full => {
                let mut engine = chunk
                    .light_engine
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for section in &mut engine.block_light {
                    section.fill(15);
                }
                for section in &mut engine.sky_light {
                    section.fill(15);
                }
                chunk.dirty.store(true, Relaxed);
            }
            LightingEngineConfig::Dark => {
                let mut engine = chunk
                    .light_engine
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for section in &mut engine.block_light {
                    section.fill(0);
                }
                for section in &mut engine.sky_light {
                    section.fill(0);
                }
                chunk.dirty.store(true, Relaxed);
            }
            LightingEngineConfig::Default => {}
        }
    }

    fn calc_priority(
        last_level: &ChunkLevel,
        last_high_priority: &[ChunkPos],
        pos: ChunkPos,
        stage: StagedChunkEnum,
    ) -> i8 {
        let base_level = *last_level.get(&pos).unwrap_or(&ChunkLoading::MAX_LEVEL);
        if base_level == ChunkLoading::MAX_LEVEL {
            return 127;
        }
        if last_high_priority.is_empty() {
            return base_level + (stage as i8);
        }
        let mut min_dst = i32::MAX;
        for i in last_high_priority {
            let dst = max((i.x - pos.x).abs(), (i.y - pos.y).abs());
            min_dst = min_dst.min(dst);
            if dst <= StagedChunkEnum::FULL_RADIUS
                && stage <= StagedChunkEnum::FULL_DEPENDENCIES[dst as usize]
            {
                return base_level + (stage as i8) - 100 + (dst as i8);
            }
        }
        base_level + (stage as i8) + (min_dst.min(60) as i8)
    }

    fn sort_queue(&mut self) {
        let mut tasks: Vec<_> = self.queue.drain().collect();
        for i in &mut tasks {
            if let Some(node) = self.graph.nodes.get(i.1) {
                i.0 = Self::calc_priority(
                    &self.last_level,
                    &self.last_high_priority,
                    node.pos,
                    node.stage,
                );
            }
        }
        self.queue = BinaryHeap::from(tasks);
        let mut parked: Vec<_> = self.parked_full.drain().collect();
        for task in &mut parked {
            if let Some(node) = self.graph.nodes.get(task.1) {
                task.0 = Self::calc_priority(
                    &self.last_level,
                    &self.last_high_priority,
                    node.pos,
                    node.stage,
                );
            }
        }
        self.parked_full = BinaryHeap::from(parked);
    }

    fn resort_work_secondary(
        &mut self,
        new_data: (Option<LevelChange>, Option<Vec<ChunkPos>>),
    ) -> bool {
        if new_data.0.is_none() && new_data.1.is_none() {
            return false;
        }
        if let Some(high_priority) = new_data.1 {
            self.last_high_priority = high_priority;
        }
        let Some(new_level) = new_data.0 else {
            return true;
        };
        for (pos, (_old_stage, new_stage)) in new_level.0 {
            if new_stage == StagedChunkEnum::None {
                self.cluster_fetches.remove(&pos);
                if let Some(mut holder) = self.chunk_map.remove(&pos) {
                    holder.target_stage = StagedChunkEnum::None;
                    holder.dependency_stage = StagedChunkEnum::None;
                    self.chunk_map.insert(pos, holder);
                }
                self.unload_chunks.insert(pos);
                continue;
            }
            self.unload_chunks.remove(&pos);
            if let Some(mut holder) = self.chunk_map.remove(&pos) {
                let held = holder.public || matches!(holder.chunk, Some(Chunk::Level(_)));
                if held {
                    holder.target_stage = new_stage;
                    holder.dependency_stage = StagedChunkEnum::None;
                    self.chunk_map.insert(pos, holder);
                }
            }
            self.secondary_request_fetch(pos);
        }
        self.last_level = new_level.1;
        true
    }

    fn secondary_cluster_arrive(&mut self, pos: ChunkPos, snapshot: SyncChunk) {
        let mut holder = self.chunk_map.remove(&pos).unwrap_or_default();
        holder.target_stage = StagedChunkEnum::level_to_stage(
            *self
                .last_level
                .get(&pos)
                .unwrap_or(&ChunkLoading::MAX_LEVEL),
        );
        holder.dependency_stage = StagedChunkEnum::None;
        let held = holder.public
            || matches!(holder.chunk, Some(Chunk::Level(_)))
            || self.public_chunk_map.contains_key(&pos);
        if held {
            self.chunk_map.insert(pos, holder);
            return;
        }
        let stage = StagedChunkEnum::Full;
        self.drop_satisfied_tasks(&mut holder, stage);
        holder.current_stage = stage;
        holder.occupied = NodeKey::null();
        self.apply_lighting_override(&snapshot);
        self.publish_chunk(pos, snapshot.clone());
        holder.public = true;
        trace!(
            "Notifying players: chunk {:?} fetched from holding peer (Full status)",
            pos
        );
        self.listener.process_new_chunk(pos, &snapshot);
        holder.chunk = Some(Chunk::Level(snapshot));
        self.chunk_map.insert(pos, holder);
    }

    fn secondary_request_fetch(&mut self, pos: ChunkPos) {
        if self.cluster_fetches.contains_key(&pos) {
            return;
        }
        let Some(level) = &self.cluster_level else {
            return;
        };
        level.prefetch_cluster_chunk(pos);
        if level.try_dispatch_cluster_fetch(pos) {
            self.cluster_fetches.insert(pos, Instant::now());
        }
    }

    fn poll_cluster_fetches(&mut self) {
        let Some(fetch_level) = self.cluster_level.clone() else {
            return;
        };
        let secondary = crate::level::is_cluster_secondary();
        if !secondary && !fetch_level.cluster_genesis_required {
            return;
        }
        let positions: Vec<_> = if secondary {
            fetch_level
                .cluster_snapshots
                .iter()
                .map(|entry| *entry.key())
                .collect()
        } else {
            Vec::new()
        };
        let mut completed: Vec<(ChunkPos, SyncChunk)> = Vec::with_capacity(positions.len());
        for pos in positions {
            if let Some((_, snapshot)) = fetch_level.cluster_snapshots.remove(&pos) {
                self.cluster_fetches.remove(&pos);
                completed.push((pos, snapshot));
            }
        }
        for (pos, snapshot) in completed {
            fetch_level.cluster_ingest_snapshot(pos, &snapshot);
            self.receive_chunk(pos, RecvChunk::Cluster((pos, snapshot)));
        }
        let wanted: Vec<_> = fetch_level
            .cluster_fetch_wanted
            .iter()
            .map(|entry| *entry.key())
            .filter(|pos| secondary || self.pending_persisted.contains_key(pos))
            .collect();
        for pos in wanted {
            if self.cluster_fetches.contains_key(&pos) {
                continue;
            }
            if fetch_level.try_dispatch_cluster_fetch(pos) {
                self.cluster_fetches.insert(pos, Instant::now());
            }
        }
        if secondary {
            self.secondary_discard_timed_out_partials();
        }
    }

    fn secondary_discard_timed_out_partials(&mut self) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .cluster_fetches
            .iter()
            .filter_map(|(pos, started)| {
                (now.duration_since(*started) >= Duration::from_secs(30)).then_some(*pos)
            })
            .collect();
        for pos in expired {
            let Some(mut holder) = self.chunk_map.remove(&pos) else {
                self.cluster_fetches.remove(&pos);
                continue;
            };
            if holder.public
                || holder.current_stage == StagedChunkEnum::Full
                || matches!(holder.chunk, Some(Chunk::Level(_)))
                || self.public_chunk_map.contains_key(&pos)
            {
                self.chunk_map.insert(pos, holder);
                self.cluster_fetches.remove(&pos);
                continue;
            }
            if !holder.occupied.is_null()
                || holder.tasks.iter().any(|task| {
                    self.graph
                        .nodes
                        .get(*task)
                        .is_some_and(|node| node.in_flight)
                })
            {
                self.chunk_map.insert(pos, holder);
                continue;
            }
            for task in holder.tasks {
                if !task.is_null() {
                    self.waiting_for_chunks.remove(&task);
                    self.drop_node(task);
                }
            }
            self.graph.drop_edge_chain(holder.occupied_by);
            holder.occupied_by = EdgeKey::null();
            self.cluster_fetches.remove(&pos);
            if let Some(level) = &self.cluster_level {
                level.clear_cluster_fetch_wanted(&pos);
            }
            error!(
                chunk_x = pos.x,
                chunk_z = pos.y,
                "cluster chunk load timed out; discarded partial holder"
            );
        }
    }

    /// TODO: will remove at some point
    pub(crate) fn restore_ready_tasks(
        graph: &mut DAG,
        queue: &mut BinaryHeap<TaskHeapNode>,
        chunk_map: &HashMap<ChunkPos, ChunkHolder>,
        last_level: &ChunkLevel,
        last_high_priority: &[ChunkPos],
        _waiting_for_chunks: &HashSetType<NodeKey>,
        secondary: bool,
    ) -> usize {
        if secondary {
            return 0;
        }
        debug_assert!(queue.is_empty());

        let mut to_drop = Vec::new();
        let mut ready = Vec::new();

        for (key, node) in &graph.nodes {
            if node.stage == StagedChunkEnum::None || node.in_flight {
                continue;
            }

            let Some(holder) = chunk_map.get(&node.pos) else {
                to_drop.push(key);
                continue;
            };

            let effective_target = holder.target_stage.max(holder.dependency_stage);
            if holder.current_stage >= node.stage
                || holder.tasks[node.stage as usize] != key
                || node.stage > effective_target
            {
                to_drop.push(key);
                continue;
            }

            if node.in_degree == 0 && !node.in_queue {
                ready.push((key, node.pos, node.stage));
            }
        }

        for key in to_drop {
            let Some(old) = graph.nodes.remove(key) else {
                continue;
            };
            let mut edge = old.edge;
            while !edge.is_null() {
                let Some(cur) = graph.edges.remove(edge) else {
                    break;
                };
                if let Some(target_node) = graph.nodes.get_mut(cur.to)
                    && target_node.in_degree > 0
                {
                    target_node.in_degree -= 1;
                    if target_node.in_degree == 0 && !target_node.in_queue && !target_node.in_flight
                    {
                        target_node.in_queue = true;
                        queue.push(TaskHeapNode(
                            Self::calc_priority(
                                last_level,
                                last_high_priority,
                                target_node.pos,
                                target_node.stage,
                            ),
                            cur.to,
                        ));
                    }
                }
                edge = cur.next;
            }
        }

        for (key, pos, stage) in ready {
            if let Some(node) = graph.nodes.get_mut(key)
                && node.in_degree == 0
                && !node.in_queue
            {
                node.in_queue = true;
                queue.push(TaskHeapNode(
                    Self::calc_priority(last_level, last_high_priority, pos, stage),
                    key,
                ));
            }
        }

        queue.len()
    }

    /// Ensure that the dependency chain for `req_stage` exists on `holder` (for chunk at
    /// `chunk_pos`) and wire it to depend on `dependency_task`.
    ///
    /// Bumps `holder.dependency_stage` (NOT `target_stage`) to at least `req_stage` so
    /// that neighbor chunks pulled in as generation dependencies are not discarded before
    /// their dependency is satisfied. `target_stage` is left alone so the level-change
    /// bookkeeping invariant (`old_stage == holder.target_stage`) is never violated.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn ensure_dependency_chain(
        graph: &mut DAG,
        queue: &mut BinaryHeap<TaskHeapNode>,
        last_level: &ChunkLevel,
        last_high_priority: &[ChunkPos],
        dependency_task: NodeKey,
        chunk_pos: ChunkPos,
        holder: &mut ChunkHolder,
        req_stage: StagedChunkEnum,
        secondary: bool,
    ) -> [bool; StagedChunkEnum::COUNT] {
        if secondary {
            return [false; StagedChunkEnum::COUNT];
        }
        // Insert occupied_by edge head
        holder.occupied_by = graph.edges.insert(crate::chunk_system::dag::Edge::new(
            dependency_task,
            holder.occupied_by,
        ));

        if !holder.occupied.is_null() {
            graph.add_edge(holder.occupied, dependency_task);
        }

        // Bump dependency_stage so this chunk's IO/generation tasks are scheduled and
        // kept alive even if target_stage is None (outside player view radius).
        // We deliberately do NOT touch target_stage — that field is owned by resort_work
        // and must match the level-change bookkeeping or the debug_assert will fire.
        if holder.dependency_stage < req_stage {
            holder.dependency_stage = req_stage;
        }

        // Effective target is the max of what the player wants and what dependencies need.
        let effective_target = holder.target_stage.max(holder.dependency_stage);
        let mut newly_created = [false; StagedChunkEnum::COUNT];

        // Create any missing tasks from current_stage+1 up to effective_target.
        // We do this even when current_stage >= req_stage, because dependency_stage may
        // require tasks beyond req_stage that haven't been created yet.
        if holder.current_stage < effective_target {
            let empty = StagedChunkEnum::Empty as usize;
            let start = (holder.current_stage as usize + 1).max(empty);
            let end = effective_target as u8 as usize;

            for (i, flag) in newly_created[start..=end].iter_mut().enumerate() {
                let stage_i = start + i;
                if holder.tasks[stage_i].is_null() {
                    let new_node = graph
                        .nodes
                        .insert(Node::new(chunk_pos, StagedChunkEnum::from(stage_i as u8)));
                    holder.tasks[stage_i] = new_node;
                    *flag = true;
                    if !holder.occupied.is_null() {
                        graph.add_edge(holder.occupied, new_node);
                    }
                }
            }

            for stage_i in start..=end {
                if !newly_created[stage_i] {
                    continue;
                }
                let cur = holder.tasks[stage_i];

                if stage_i > empty {
                    let prev = holder.tasks[stage_i - 1];
                    if !prev.is_null() {
                        graph.add_edge(prev, cur);
                    }
                }
                if stage_i < end {
                    let next = holder.tasks[stage_i + 1];
                    if !next.is_null() && !newly_created[stage_i + 1] {
                        graph.add_edge(cur, next);
                    }
                }
            }

            // Queue the entry task (lowest unblocked stage)
            let entry_task = holder.tasks[start];
            if !entry_task.is_null()
                && let Some(n) = graph.nodes.get_mut(entry_task)
                && n.in_degree == 0
                && !n.in_queue
            {
                n.in_queue = true;
                queue.push(TaskHeapNode(
                    Self::calc_priority(
                        last_level,
                        last_high_priority,
                        chunk_pos,
                        StagedChunkEnum::from(start as u8),
                    ),
                    entry_task,
                ));
            }
        }

        // If req_stage is already satisfied, dependency_task doesn't need to wait —
        // it was only blocked on `occupied` (handled above) and the stage itself is done.
        // Do NOT add an edge here: tasks[req_stage] is null (completed and dropped).
        if holder.current_stage >= req_stage {
            return newly_created;
        }

        // Wire req_stage task → dependency_task so dependency_task can't run until
        // this chunk reaches req_stage. tasks[req_stage] is guaranteed non-null here:
        // effective_target >= req_stage (we just set dependency_stage = req_stage) and
        // current_stage < req_stage, so the task was created in the loop above (or
        // already existed).
        let req_end = req_stage as u8 as usize;
        let ano_task = holder.tasks[req_end];
        debug_assert!(
            !ano_task.is_null(),
            "holder.tasks[req_stage] must not be null before adding edge"
        );
        graph.add_edge(ano_task, dependency_task);

        newly_created
    }

    /// Check if any tasks parked in `waiting_for_chunks` now have all their neighbor
    /// chunk data available, and re-queue them if so.
    /// Must be called after every `receive_chunk` call.
    fn check_waiting_tasks(&mut self, secondary: bool) {
        if secondary {
            return;
        }
        if self.waiting_for_chunks.is_empty() {
            return;
        }

        let mut now_ready: Vec<NodeKey> = Vec::new();
        let mut stale: Vec<NodeKey> = Vec::new();

        self.waiting_for_chunks.retain(|&node_key| {
            let Some(node) = self.graph.nodes.get(node_key) else {
                return false; // node was dropped, discard silently
            };

            let Some(holder) = self.chunk_map.get(&node.pos) else {
                stale.push(node_key);
                return false;
            };

            let effective_target = holder.target_stage.max(holder.dependency_stage);
            if holder.current_stage >= node.stage
                || holder.tasks[node.stage as usize] != node_key
                || node.stage > effective_target
            {
                stale.push(node_key);
                return false;
            }

            let read_radius = node.stage.get_read_radius();
            let pos = node.pos;
            let all_ready = (-read_radius..=read_radius).all(|dx| {
                (-read_radius..=read_radius).all(|dy| {
                    self.chunk_map
                        .get(&pos.add_raw(dx, dy))
                        .is_some_and(|h| h.chunk.is_some())
                })
            });
            if all_ready {
                if node.in_degree == 0 {
                    now_ready.push(node_key);
                    false
                } else {
                    true
                }
            } else {
                true
            }
        });

        for node_key in stale {
            self.drop_node(node_key);
        }

        for node_key in now_ready {
            if let Some(n) = self.graph.nodes.get_mut(node_key)
                && n.in_degree == 0
                && !n.in_queue
            {
                n.in_queue = true;
                let priority =
                    Self::calc_priority(&self.last_level, &self.last_high_priority, n.pos, n.stage);
                self.queue.push(TaskHeapNode(priority, node_key));
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    fn resort_work(&mut self, new_data: (Option<LevelChange>, Option<Vec<ChunkPos>>)) -> bool {
        if crate::level::is_cluster_secondary() {
            return self.resort_work_secondary(new_data);
        }
        if new_data.0.is_none() && new_data.1.is_none() {
            return false;
        }
        if let Some(high_priority) = new_data.1 {
            self.last_high_priority = high_priority;
            self.queue_dirty = true;
        }
        let Some(new_level) = new_data.0 else {
            return true;
        };
        let mut worklist: std::collections::VecDeque<(ChunkPos, StagedChunkEnum, NodeKey)> =
            std::collections::VecDeque::new();

        for (pos, (old_stage, new_stage)) in new_level.0 {
            debug_assert_ne!(old_stage, new_stage);
            debug_assert_eq!(
                new_stage,
                StagedChunkEnum::level_to_stage(
                    *new_level.1.get(&pos).unwrap_or(&ChunkLoading::MAX_LEVEL)
                )
            );
            let mut holder = self.chunk_map.remove(&pos).unwrap_or_default();
            debug_assert_eq!(holder.target_stage, old_stage);
            holder.target_stage = new_stage;

            // Effective target is what we actually need to schedule tasks up to.
            let effective_old = old_stage.max(holder.dependency_stage);
            let effective_new = new_stage.max(holder.dependency_stage);

            if effective_old > effective_new {
                for i in (effective_new.max(holder.current_stage) as usize + 1)
                    ..=(effective_old as usize)
                {
                    let task = &mut holder.tasks[i];
                    if !task.is_null() {
                        let is_in_flight = self.graph.nodes.get(*task).is_some_and(|n| n.in_flight);
                        if !is_in_flight {
                            self.waiting_for_chunks.remove(task);
                            self.drop_node(*task);
                            *task = NodeKey::null();
                        }
                    }
                }
                if new_stage == StagedChunkEnum::None {
                    if holder.dependency_stage != StagedChunkEnum::None {
                        let has_valid_task = self.graph.prune_edge_chain(&mut holder.occupied_by);
                        if !has_valid_task {
                            holder.dependency_stage = StagedChunkEnum::None;
                        }
                    }
                    if holder.dependency_stage == StagedChunkEnum::None {
                        self.unload_chunks.insert(pos);
                    }
                }
            } else {
                if old_stage == StagedChunkEnum::None {
                    self.unload_chunks.remove(&pos);
                    if holder.current_stage == StagedChunkEnum::Full
                        && !holder.public
                        && !(self
                            .cluster_level
                            .as_ref()
                            .is_some_and(|level| level.cluster_genesis_required)
                            && matches!(holder.chunk, Some(Chunk::Level(_))))
                    {
                        holder.public = true;
                        match holder.chunk.as_ref().expect("chunk exists") {
                            Chunk::Level(chunk) => {
                                self.apply_lighting_override(chunk);
                                self.publish_chunk(pos, chunk.clone());
                                self.listener.process_new_chunk(pos, chunk);
                            }
                            Chunk::Proto(_) => panic!(),
                        }
                    }
                }
                for i in (effective_old.max(holder.current_stage) as u8 + 1)..=(effective_new as u8)
                {
                    let task = &mut holder.tasks[i as usize];
                    if task.is_null() {
                        *task = self.graph.nodes.insert(Node::new(pos, i.into()));
                        if !holder.occupied.is_null() {
                            self.graph.add_edge(holder.occupied, *task);
                        }
                    }
                    let task = *task;
                    if i > 1 {
                        let stage = StagedChunkEnum::from(i);
                        let dependency = stage.get_direct_dependencies();
                        let radius = stage.get_direct_radius();
                        self.unload_chunks.remove(&pos);
                        let req_stage = dependency[0];
                        let newly_created = Self::ensure_dependency_chain(
                            &mut self.graph,
                            &mut self.queue,
                            &self.last_level,
                            &self.last_high_priority,
                            task,
                            pos,
                            &mut holder,
                            req_stage,
                            crate::level::is_cluster_secondary(),
                        );
                        for (stage_i, &created) in newly_created.iter().enumerate() {
                            if created && stage_i > 1 {
                                let stage = StagedChunkEnum::from(stage_i as u8);
                                let dependency = stage.get_direct_dependencies();
                                let radius = stage.get_direct_radius();
                                let cur_task = holder.tasks[stage_i];
                                for r in 1..=(radius as u8) {
                                    let neighbor_req = dependency[r as usize];
                                    for &(ndx, ndz) in
                                        pumpkin_data::chunk_view_lut::get_chebyshev_ring(r)
                                    {
                                        let neighbor_pos = pos.add_raw(ndx as i32, ndz as i32);
                                        worklist.push_back((neighbor_pos, neighbor_req, cur_task));
                                    }
                                }
                            }
                        }

                        for r in 1..=(radius as u8) {
                            let req_stage = dependency[r as usize];
                            for &(dx, dz) in pumpkin_data::chunk_view_lut::get_chebyshev_ring(r) {
                                let new_pos = pos.add_raw(dx as i32, dz as i32);
                                self.unload_chunks.remove(&new_pos);
                                worklist.push_back((new_pos, req_stage, task));
                            }
                        }
                    }
                    let node = self.graph.nodes.get_mut(task).expect("node exists");
                    if node.in_degree == 0 && !node.in_queue {
                        node.in_queue = true;
                        self.queue.push(TaskHeapNode(0, task));
                    }
                }
            }
            self.chunk_map.insert(pos, holder);
        }

        while let Some((pos, req_stage, dep_task)) = worklist.pop_front() {
            self.unload_chunks.remove(&pos);
            let mut holder = self.chunk_map.remove(&pos).unwrap_or_default();
            let newly_created = Self::ensure_dependency_chain(
                &mut self.graph,
                &mut self.queue,
                &new_level.1,
                &self.last_high_priority,
                dep_task,
                pos,
                &mut holder,
                req_stage,
                crate::level::is_cluster_secondary(),
            );
            for (stage_i, &created) in newly_created.iter().enumerate() {
                if created && stage_i > 1 {
                    let stage = StagedChunkEnum::from(stage_i as u8);
                    let dependency = stage.get_direct_dependencies();
                    let radius = stage.get_direct_radius();
                    let cur_task = holder.tasks[stage_i];
                    for r in 1..=(radius as u8) {
                        let neighbor_req = dependency[r as usize];
                        for &(dx, dz) in pumpkin_data::chunk_view_lut::get_chebyshev_ring(r) {
                            let neighbor_pos = pos.add_raw(dx as i32, dz as i32);
                            worklist.push_back((neighbor_pos, neighbor_req, cur_task));
                        }
                    }
                }
            }
            self.chunk_map.insert(pos, holder);
        }

        self.last_level = new_level.1;
        self.queue_dirty = true;
        true
    }

    fn advance_owner_demands_and_levels(&mut self) -> bool {
        let (player_change, priority) = self.send_level.get();
        let mut changed = HashSetType::<ChunkPos>::default();
        if let Some((changes, player_level)) = player_change {
            changed.extend(changes.keys().copied());
            self.player_level = player_level;
        }
        for _ in 0..1_024 {
            let Ok(demand) = self.owner_demand_rx.try_recv() else {
                break;
            };
            match demand {
                crate::level::OwnerChunkDemand::RetainPeerRequest { position } => {
                    if self.peer_request_retention.insert(position) {
                        changed.insert(position);
                    }
                }
                crate::level::OwnerChunkDemand::ReleasePeerRequest { position } => {
                    if self.peer_request_retention.remove(&position) {
                        changed.insert(position);
                    }
                }
                crate::level::OwnerChunkDemand::Acquire {
                    position,
                    lease,
                    ready,
                } => {
                    if let Some(ready) = ready {
                        self.owner_ready_routes
                            .entry(position)
                            .or_default()
                            .insert(lease, ready);
                        if let Some(generation) =
                            self.publication_generations.get(&position).copied()
                        {
                            self.pending_owner_ready
                                .push_back((position, lease, generation));
                        }
                    }
                    if self
                        .owner_demands
                        .entry(position)
                        .or_default()
                        .insert(lease)
                    {
                        changed.insert(position);
                    }
                }
                crate::level::OwnerChunkDemand::Release { position, lease } => {
                    if let Some(routes) = self.owner_ready_routes.get_mut(&position) {
                        routes.remove(&lease);
                        if routes.is_empty() {
                            self.owner_ready_routes.remove(&position);
                        }
                    }
                    if let Some(leases) = self.owner_demands.get_mut(&position)
                        && leases.remove(&lease)
                    {
                        changed.insert(position);
                        if leases.is_empty() {
                            self.owner_demands.remove(&position);
                        }
                    }
                }
            }
        }
        self.flush_owner_ready();
        if changed.is_empty() {
            return self.resort_work((None, priority));
        }
        let mut combined = self.player_level.clone();
        for position in self
            .owner_demands
            .keys()
            .copied()
            .chain(self.peer_request_retention.iter().copied())
        {
            combined
                .entry(position)
                .and_modify(|level| *level = (*level).min(43))
                .or_insert(43);
        }
        let mut changes = HashMapType::default();
        for position in changed {
            let old = StagedChunkEnum::level_to_stage(
                *self
                    .last_level
                    .get(&position)
                    .unwrap_or(&ChunkLoading::MAX_LEVEL),
            );
            let new = StagedChunkEnum::level_to_stage(
                *combined.get(&position).unwrap_or(&ChunkLoading::MAX_LEVEL),
            );
            if old != new {
                changes.insert(position, (old, new));
            }
        }
        if changes.is_empty() {
            self.last_level = combined;
            return self.resort_work((None, priority));
        }
        self.resort_work((Some((changes, combined)), priority))
    }

    fn recompute_dependency_stages(&mut self) {
        let mut required: HashMapType<ChunkPos, StagedChunkEnum> = HashMapType::default();
        let mut worklist: Vec<(ChunkPos, StagedChunkEnum)> = self
            .chunk_map
            .iter()
            .filter(|(_, holder)| holder.target_stage != StagedChunkEnum::None)
            .map(|(pos, holder)| (*pos, holder.target_stage))
            .collect();

        while let Some((pos, req)) = worklist.pop() {
            let entry = required.entry(pos).or_insert(StagedChunkEnum::None);
            if *entry >= req {
                continue;
            }
            let start = *entry as u8 + 1;
            *entry = req;

            // Only expand the stages that were not already accounted for.
            for i in start..=(req as u8) {
                let stage = StagedChunkEnum::from(i);
                let radius = stage.get_direct_radius();
                if radius == 0 {
                    continue;
                }
                let dependencies = stage.get_direct_dependencies();
                for r in 1..=(radius as u8) {
                    let req = dependencies[r as usize];
                    for &(dx, dz) in pumpkin_data::chunk_view_lut::get_chebyshev_ring(r) {
                        let neighbor = pos.add_raw(dx as i32, dz as i32);
                        worklist.push((neighbor, req));
                    }
                }
            }
        }

        let mut nodes_to_drop = Vec::new();
        let mut newly_unused = Vec::new();
        for (pos, holder) in &mut self.chunk_map {
            let new_dependency = required.get(pos).copied().unwrap_or(StagedChunkEnum::None);
            if new_dependency >= holder.dependency_stage {
                continue;
            }
            holder.dependency_stage = new_dependency;

            let effective_target = holder.target_stage.max(new_dependency);
            for i in (effective_target as usize + 1)..StagedChunkEnum::COUNT {
                let task = holder.tasks[i];
                if !task.is_null() {
                    nodes_to_drop.push((*pos, i, task));
                }
            }
            if effective_target == StagedChunkEnum::None {
                newly_unused.push(*pos);
            }
        }
        self.unload_chunks.extend(newly_unused);

        for (pos, index, task) in nodes_to_drop {
            if self
                .graph
                .nodes
                .get(task)
                .is_some_and(|node| node.in_flight)
            {
                continue;
            }
            self.waiting_for_chunks.remove(&task);
            self.drop_node(task);
            if let Some(holder) = self.chunk_map.get_mut(&pos) {
                holder.tasks[index] = NodeKey::null();
            }
        }

        self.purge_dropped_queue_entries();
    }

    /// Drop heap entries whose node has been cancelled. They are skipped when popped,
    /// but a saturated queue is never drained, so without this the heap keeps every
    /// cancelled task of every chunk the player has flown past and `sort_queue` gets
    /// slower on each level change.
    fn purge_dropped_queue_entries(&mut self) {
        if self.queue.is_empty() {
            return;
        }
        let graph = &self.graph;
        let tasks: Vec<_> = self
            .queue
            .drain()
            .filter(|task| graph.nodes.contains_key(task.1))
            .collect();
        self.queue = BinaryHeap::from(tasks);
        self.parked_full
            .retain(|task| graph.nodes.contains_key(task.1));
    }

    fn garbage_collect_dependencies(&mut self) {
        self.recompute_dependency_stages();

        // Garbage collect stranded dependencies and empty holders
        let mut stranded = Vec::new();
        let mut empty_holders = Vec::new();

        for (pos, holder) in &self.chunk_map {
            if holder.target_stage == StagedChunkEnum::None {
                if holder.dependency_stage != StagedChunkEnum::None {
                    stranded.push(*pos);
                } else if holder.current_stage == StagedChunkEnum::None
                    && holder.chunk.is_none()
                    && holder.occupied.is_null()
                    && holder.tasks.iter().all(Key::is_null)
                    && !holder.public
                {
                    empty_holders.push(*pos);
                }
            }
        }

        for pos in stranded {
            let holder = self.chunk_map.get_mut(&pos).expect("holder exists");
            if !holder.occupied.is_null() && self.graph.nodes.contains_key(holder.occupied) {
                continue;
            }

            let has_valid_task = self.graph.prune_edge_chain(&mut holder.occupied_by);
            if !has_valid_task {
                holder.dependency_stage = StagedChunkEnum::None;
                self.unload_chunks.insert(pos);
            }
        }

        for pos in empty_holders {
            if let Some(mut holder) = self.chunk_map.remove(&pos) {
                self.graph.drop_edge_chain(holder.occupied_by);
                holder.occupied_by = EdgeKey::null();
            }
        }
    }

    fn process_unload_queue(&mut self) {
        if self.unload_chunks.is_empty() {
            return;
        }
        let mut unload_chunks = HashSetType::default();
        swap(&mut unload_chunks, &mut self.unload_chunks);
        let mut chunks = Vec::with_capacity(unload_chunks.len());
        for pos in unload_chunks {
            let Some(mut holder) = self.chunk_map.remove(&pos) else {
                continue;
            };
            if self.cluster_level.as_ref().is_some_and(|level| {
                level.cluster_genesis_required
                    && level.cluster_unsettled_primary_chunks.load().contains(&pos)
            }) {
                self.chunk_map.insert(pos, holder);
                self.unload_chunks.insert(pos);
                continue;
            }
            if self.owner_demands.contains_key(&pos) || self.peer_request_retention.contains(&pos) {
                self.chunk_map.insert(pos, holder);
                continue;
            }
            if self.pending_fresh.contains_key(&pos) {
                self.chunk_map.insert(pos, holder);
                continue;
            }
            if let Some(pending) = self.pending_persisted.get(&pos)
                && holder.target_stage == StagedChunkEnum::None
                && holder.dependency_stage == StagedChunkEnum::None
            {
                let recovery_active = self.cluster_fetches.contains_key(&pos)
                    || self
                        .cluster_level
                        .as_ref()
                        .is_some_and(|level| level.cluster_fetch_wanted.contains(&pos));
                let can_abort = (!pending.submitted || pending.terminal_rejection)
                    && !recovery_active
                    && !pending.peer_claimed;
                if can_abort {
                    self.pending_persisted.remove(&pos);
                    if let Some(level) = self.cluster_level.as_ref() {
                        publish_persisted_reservation(
                            &level.cluster_publication_reservations,
                            pos,
                            None,
                        );
                    }
                }
            }
            if self.pending_persisted.contains_key(&pos) {
                let retiring = holder.target_stage == StagedChunkEnum::None
                    && holder.dependency_stage == StagedChunkEnum::None;
                self.chunk_map.insert(pos, holder);
                if retiring {
                    self.unload_chunks.insert(pos);
                }
                continue;
            }
            if self
                .cluster_level
                .as_ref()
                .is_some_and(|level| level.is_cluster_pinned(&pos))
            {
                self.chunk_map.insert(pos, holder);
                continue;
            }
            if holder.target_stage != StagedChunkEnum::None
                || holder.dependency_stage != StagedChunkEnum::None
            {
                self.chunk_map.insert(pos, holder);
                continue;
            }
            if !holder.occupied.is_null() {
                self.chunk_map.insert(pos, holder);
                self.unload_chunks.insert(pos);
                continue;
            }
            if holder.public
                && let Some(level) = self
                    .cluster_level
                    .as_ref()
                    .filter(|level| level.cluster_genesis_required)
                && !level.try_begin_primary_chunk_unload(pos)
            {
                self.chunk_map.insert(pos, holder);
                self.unload_chunks.insert(pos);
                continue;
            }

            for task in holder.tasks {
                if !task.is_null() {
                    let is_in_flight = self.graph.nodes.get(task).is_some_and(|n| n.in_flight);
                    if !is_in_flight {
                        self.waiting_for_chunks.remove(&task);
                        self.drop_node(task);
                    }
                }
            }

            self.graph.drop_edge_chain(holder.occupied_by);
            holder.occupied_by = EdgeKey::null();

            if holder.public {
                self.unpublish_chunk(pos);
                holder.public = false;
            }

            if let Some(tmp) = holder.chunk {
                match tmp {
                    Chunk::Level(chunk) => {
                        // Save chunk to disk if dirty
                        if chunk.is_dirty() {
                            chunks.push((pos, Chunk::Level(chunk)));
                        }
                    }
                    Chunk::Proto(proto) => {
                        if !matches!(proto.stage, StagedChunkEnum::Empty | StagedChunkEnum::None) {
                            chunks.push((pos, Chunk::Proto(proto)));
                        }
                    }
                }
            }
        }
        if chunks.is_empty() {
            return;
        }
        if crate::level::is_cluster_secondary() {
            return;
        }
        let mut data = self
            .io_lock
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (pos, _chunk) in &chunks {
            *data.entry(*pos).or_insert(0) += 1;
        }
        drop(data);
        if let Err(e) = self.io_write.blocking_send(chunks) {
            error!(
                "Failed to send chunks to io write thread during save (may have shut down): {:?}",
                e
            );
        }
    }

    fn save_all_chunk(&mut self, save_proto_chunk: bool) {
        if crate::level::is_cluster_secondary() {
            return;
        }
        let mut chunks = Vec::with_capacity(self.chunk_map.len());

        for (pos, holder) in &mut self.chunk_map {
            if self.pending_fresh.contains_key(pos) || self.pending_persisted.contains_key(pos) {
                continue;
            }
            if let Some(chunk) = &holder.chunk {
                let should_save = match chunk {
                    Chunk::Level(sync_chunk) => sync_chunk.is_dirty(),
                    Chunk::Proto(proto) => {
                        save_proto_chunk
                            && !matches!(
                                proto.stage,
                                crate::chunk_system::chunk_state::StagedChunkEnum::Empty
                                    | crate::chunk_system::chunk_state::StagedChunkEnum::None
                            )
                    }
                };

                if should_save {
                    let chunk_to_save = match chunk {
                        Chunk::Level(sync_chunk) => Chunk::Level(sync_chunk.clone()),
                        Chunk::Proto(_) => holder.chunk.take().expect("proto chunk exists"),
                    };
                    chunks.push((*pos, chunk_to_save));
                }
            }
        }

        if chunks.is_empty() {
            return;
        }

        info!(
            "Saving {} chunks (collected from {} holders)...",
            chunks.len(),
            self.chunk_map.len()
        );

        let mut data = self
            .io_lock
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (pos, _) in &chunks {
            *data.entry(*pos).or_insert(0) += 1;
        }
        drop(data);

        if let Err(e) = self.io_write.blocking_send(chunks) {
            error!("Failed to send chunks to io write thread: {:?}", e);
        }
    }

    fn drop_node(&mut self, node: NodeKey) {
        let Some(old) = self.graph.nodes.remove(node) else {
            return;
        };
        let mut edge = old.edge;
        while !edge.is_null() {
            let cur = self.graph.edges.remove(edge).expect("edge exists");
            if let Some(node) = self.graph.nodes.get_mut(cur.to) {
                debug_assert!(node.in_degree >= 1);
                node.in_degree -= 1;
                if node.in_degree == 0 && !node.in_queue {
                    self.queue.push(TaskHeapNode(
                        Self::calc_priority(
                            &self.last_level,
                            &self.last_high_priority,
                            node.pos,
                            node.stage,
                        ),
                        cur.to,
                    ));
                    node.in_queue = true;
                }
            }
            edge = cur.next;
        }
    }

    fn drop_satisfied_tasks(&mut self, holder: &mut ChunkHolder, stage: StagedChunkEnum) {
        // A neighboring generation cache may already have advanced this holder past the
        // returning task's stage. Drop every scheduled task satisfied by the returned data,
        // including that task's stale in-flight node.
        for task_idx in (StagedChunkEnum::None as usize + 1)..=(stage as usize) {
            if !holder.tasks[task_idx].is_null() {
                self.waiting_for_chunks.remove(&holder.tasks[task_idx]);
                self.drop_node(holder.tasks[task_idx]);
                holder.tasks[task_idx] = NodeKey::null();
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    fn receive_chunk(&mut self, pos: ChunkPos, data: RecvChunk) {
        let counted_task = !matches!(data, RecvChunk::Cluster(_));
        if let Some(pending) = self.pending_persisted.get_mut(&pos) {
            if !pending.unexpected_arrival_reported {
                pending.unexpected_arrival_reported = true;
                error!(
                    x = pos.x,
                    z = pos.y,
                    generation = pending.generation,
                    "cluster persisted admission reserved this chunk against replacement"
                );
            }
            if counted_task {
                self.running_task_count = self.running_task_count.saturating_sub(1);
            }
            return;
        }
        match data {
            RecvChunk::IO(chunk) => {
                let mut holder = self.chunk_map.remove(&pos).expect("holder exists");
                if holder.public
                    && let Some(level) = self
                        .cluster_level
                        .as_ref()
                        .filter(|level| level.cluster_genesis_required)
                {
                    if matches!(chunk, Chunk::Level(_)) {
                        // A stale disk image may never replace an already public,
                        // owner-admitted primary image.
                        if !holder.occupied.is_null()
                            && self.graph.nodes.contains_key(holder.occupied)
                        {
                            self.drop_node(holder.occupied);
                        }
                        holder.occupied = NodeKey::null();
                        holder.current_stage = StagedChunkEnum::Full;
                        self.chunk_map.insert(pos, holder);
                        self.running_task_count = self.running_task_count.saturating_sub(1);
                        return;
                    }
                    if !level.try_begin_primary_chunk_unload(pos) {
                        self.deferred_primary_io.insert(pos, chunk);
                        self.chunk_map.insert(pos, holder);
                        self.running_task_count = self.running_task_count.saturating_sub(1);
                        return;
                    }
                }
                let mut private = false;
                if holder.chunk.is_some() {
                    warn!(
                        "receive_chunk(IO): holder already has chunk at {:?}; replacing",
                        pos
                    );
                }
                debug_assert_eq!(holder.current_stage, StagedChunkEnum::None);

                let stage = StagedChunkEnum::from(chunk.get_stage_id());
                self.drop_satisfied_tasks(&mut holder, stage);
                holder.current_stage = stage;
                debug_assert!(self.graph.nodes.contains_key(holder.occupied));
                self.drop_node(holder.occupied);
                holder.occupied = NodeKey::null();

                match &chunk {
                    Chunk::Level(data) => {
                        if self
                            .cluster_level
                            .as_ref()
                            .is_some_and(|level| level.cluster_genesis_required)
                        {
                            private = true;
                            self.next_fresh_generation = self.next_fresh_generation.wrapping_add(1);
                            self.pending_persisted.insert(
                                pos,
                                PendingPersisted {
                                    original: data.clone(),
                                    approved: None,
                                    generation: self.next_fresh_generation,
                                    response: None,
                                    submitted: false,
                                    rejected: false,
                                    terminal_rejection: false,
                                    peer_claimed: false,
                                    unexpected_arrival_reported: false,
                                },
                            );
                            if let Some(level) = self.cluster_level.as_ref() {
                                publish_persisted_reservation(
                                    &level.cluster_publication_reservations,
                                    pos,
                                    Some(crate::level::PersistedChunkReservation {
                                        generation: self.next_fresh_generation,
                                        state:
                                            crate::level::PersistedChunkReservationState::Pending,
                                    }),
                                );
                            }
                        } else {
                            self.apply_lighting_override(data);
                            let result = self.publish_chunk(pos, data.clone());
                            if result.is_some() {
                                trace!(
                                    "receive_chunk(IO): replacing existing public chunk at {:?}",
                                    pos
                                );
                            }
                            holder.public = true;
                            trace!(
                                "Notifying players: chunk {:?} loaded from disk (Full status)",
                                pos
                            );
                            self.listener.process_new_chunk(pos, data);
                        }
                    }
                    Chunk::Proto(_) => {
                        if holder.public {
                            debug!(
                                "Chunk {:?} downgraded to Proto for relighting, marking as non-public",
                                pos
                            );
                            self.unpublish_chunk(pos);
                            holder.public = false;
                        }
                    }
                }
                holder.chunk = Some(chunk);
                self.chunk_map.insert(pos, holder);

                // A new chunk arrived — unblock any waiting generation tasks
                if !private {
                    self.check_waiting_tasks(crate::level::is_cluster_secondary());
                }
            }
            RecvChunk::Cluster((cluster_pos, snapshot)) => {
                if crate::level::is_cluster_secondary() {
                    self.secondary_cluster_arrive(cluster_pos, snapshot);
                    return;
                }
                let Some(mut holder) = self.chunk_map.remove(&cluster_pos) else {
                    return;
                };
                let already_held = holder.public
                    || matches!(holder.chunk, Some(Chunk::Level(_)))
                    || self.public_chunk_map.contains_key(&cluster_pos);
                if already_held {
                    trace!(
                        "receive_chunk(Cluster): discarding snapshot for already held chunk at {:?}",
                        cluster_pos
                    );
                    self.chunk_map.insert(cluster_pos, holder);
                    return;
                }
                let stage = StagedChunkEnum::Full;
                self.drop_satisfied_tasks(&mut holder, stage);
                holder.current_stage = stage;
                if !holder.occupied.is_null() && self.graph.nodes.contains_key(holder.occupied) {
                    self.drop_node(holder.occupied);
                }
                holder.occupied = NodeKey::null();
                self.apply_lighting_override(&snapshot);
                self.publish_chunk(cluster_pos, snapshot.clone());
                holder.public = true;
                trace!(
                    "Notifying players: chunk {:?} fetched from holding peer (Full status)",
                    cluster_pos
                );
                self.listener.process_new_chunk(cluster_pos, &snapshot);
                holder.chunk = Some(Chunk::Level(snapshot));
                self.chunk_map.insert(cluster_pos, holder);
                self.check_waiting_tasks(crate::level::is_cluster_secondary());
                return;
            }
            RecvChunk::Generation(data) => {
                let mut initial_palettes = data.initial_palettes;
                let mut dx = 0;
                let mut dy = 0;
                for chunk in data.chunks {
                    let new_pos = ChunkPos::new(data.x + dx, data.z + dy);
                    dy += 1;
                    if dy == data.size {
                        dy = 0;
                        dx += 1;
                    }
                    if self.pending_persisted.contains_key(&new_pos) {
                        error!(
                            x = new_pos.x,
                            z = new_pos.y,
                            "cluster persisted admission reserved a generated neighbor against replacement"
                        );
                        continue;
                    }
                    match chunk {
                        Chunk::Level(chunk) => {
                            let mut holder =
                                self.chunk_map.remove(&new_pos).expect("holder exists");
                            let stage = StagedChunkEnum::Full;
                            if new_pos == pos {
                                if holder.current_stage != StagedChunkEnum::Spawn {
                                    warn!(
                                        "receive_chunk(Level): holder at {:?} for pos {:?} expected {:?}; aligning",
                                        holder.current_stage,
                                        new_pos,
                                        StagedChunkEnum::Spawn
                                    );
                                    holder.current_stage = StagedChunkEnum::Spawn;
                                }
                                self.drop_satisfied_tasks(&mut holder, stage);
                                if self.graph.nodes.contains_key(holder.occupied) {
                                    self.drop_node(holder.occupied);
                                }
                                holder.current_stage = stage;

                                let was_public = holder.public;
                                if self
                                    .cluster_level
                                    .as_ref()
                                    .is_some_and(|level| level.cluster_genesis_required)
                                    && was_public
                                {
                                    error!(
                                        x = new_pos.x,
                                        z = new_pos.y,
                                        "cluster primary rejected regeneration of a published chunk"
                                    );
                                } else if let Some(palettes) = initial_palettes.take() {
                                    let generation = self.next_fresh_generation;
                                    self.next_fresh_generation = self
                                        .next_fresh_generation
                                        .checked_add(1)
                                        .expect("fresh chunk generation exhausted");
                                    holder.chunk = Some(Chunk::Level(chunk.clone()));
                                    self.pending_fresh.insert(
                                        new_pos,
                                        PendingFresh {
                                            image: std::sync::Arc::from(
                                                crate::level::cluster_encode_snapshot(&chunk),
                                            ),
                                            chunk,
                                            palettes: Some(palettes),
                                            generation,
                                            response: None,
                                            abort: false,
                                        },
                                    );
                                } else if self
                                    .cluster_level
                                    .as_ref()
                                    .is_some_and(|level| level.cluster_genesis_required)
                                {
                                    error!(
                                        x = new_pos.x,
                                        z = new_pos.y,
                                        "cluster primary generated a chunk without a genesis palette"
                                    );
                                    holder.chunk = Some(Chunk::Level(chunk));
                                } else if was_public {
                                    let public_chunk = chunk.clone();
                                    self.publish_chunk(new_pos, public_chunk);
                                    info!(
                                        "Notifying players: regenerated chunk at {:?} (was already public)",
                                        new_pos
                                    );
                                    self.listener.process_new_chunk(new_pos, &chunk);
                                    holder.chunk = Some(Chunk::Level(chunk));
                                } else {
                                    let public_chunk = chunk.clone();
                                    holder.chunk = Some(Chunk::Level(chunk));
                                    let result = self.publish_chunk(new_pos, public_chunk);
                                    holder.public = true;
                                    if result.is_some() {
                                        warn!(
                                            "public_chunk_map.insert returned existing chunk for {new_pos:?}"
                                        );
                                    }
                                    if let Some(pc) = self.public_chunk_map.get(&new_pos) {
                                        trace!(
                                            "Notifying players: new chunk at {:?} (generation complete)",
                                            new_pos
                                        );
                                        self.listener.process_new_chunk(new_pos, &pc);
                                    } else {
                                        error!(
                                            "CRITICAL: Failed to retrieve chunk {:?} from public_chunk_map immediately after insert!",
                                            new_pos
                                        );
                                    }
                                }
                            } else {
                                self.drop_satisfied_tasks(&mut holder, stage);
                                holder.current_stage = stage;
                                holder.chunk = Some(Chunk::Level(chunk));
                            }

                            if !holder.occupied.is_null()
                                && self.graph.nodes.contains_key(holder.occupied)
                            {
                                self.drop_node(holder.occupied);
                            }
                            holder.occupied = NodeKey::null();

                            self.chunk_map.insert(new_pos, holder);
                        }
                        Chunk::Proto(chunk) => {
                            let mut holder =
                                self.chunk_map.remove(&new_pos).expect("holder exists");

                            let stage = StagedChunkEnum::from(chunk.stage_id());
                            self.drop_satisfied_tasks(&mut holder, stage);

                            if new_pos == pos {
                                debug_assert_ne!(holder.current_stage, StagedChunkEnum::None);
                                if self.graph.nodes.contains_key(holder.occupied) {
                                    self.drop_node(holder.occupied);
                                }
                                holder.current_stage = stage;
                            } else {
                                if holder.current_stage < stage {
                                    holder.current_stage = stage;
                                }
                                if !holder.occupied.is_null()
                                    && self.graph.nodes.contains_key(holder.occupied)
                                {
                                    self.drop_node(holder.occupied);
                                }
                            }

                            holder.occupied = NodeKey::null();
                            holder.chunk = Some(Chunk::Proto(chunk));
                            self.chunk_map.insert(new_pos, holder);
                        }
                    }
                }

                // Neighbor chunks returned to holders — unblock waiting tasks
                self.check_waiting_tasks(crate::level::is_cluster_secondary());
            }
            RecvChunk::GenerationFailure {
                pos: fail_pos,
                stage,
                error,
            } => {
                error!(
                    "Received generation failure notification for chunk {:?} at stage {:?}: {}",
                    fail_pos, stage, error
                );

                if let Some(mut holder) = self.chunk_map.remove(&pos) {
                    let target_stage = holder.target_stage;

                    if !holder.occupied.is_null() {
                        if self.graph.nodes.contains_key(holder.occupied) {
                            self.drop_node(holder.occupied);
                        }
                        holder.occupied = NodeKey::null();
                    }

                    for i in 0..holder.tasks.len() {
                        if !holder.tasks[i].is_null() {
                            self.waiting_for_chunks.remove(&holder.tasks[i]);
                            self.drop_node(holder.tasks[i]);
                            holder.tasks[i] = NodeKey::null();
                        }
                    }

                    holder.current_stage = StagedChunkEnum::None;
                    holder.dependency_stage = StagedChunkEnum::None;
                    holder.chunk = None;

                    if crate::level::is_cluster_secondary() {
                        self.chunk_map.insert(pos, holder);
                        self.secondary_request_fetch(pos);
                        warn!(
                            "Chunk {:?} reset to None on cluster secondary; waiting for holder snapshot (target: {:?})",
                            pos, target_stage
                        );
                    } else {
                        for i in (StagedChunkEnum::None as usize + 1)..=(target_stage as usize) {
                            let stage_enum = StagedChunkEnum::from(i as u8);
                            let task_node = Node::new(pos, stage_enum);
                            holder.tasks[i] = self.graph.nodes.insert(task_node);

                            if i > (StagedChunkEnum::None as usize + 1) {
                                self.graph.add_edge(holder.tasks[i - 1], holder.tasks[i]);
                            }
                        }

                        if target_stage > StagedChunkEnum::None {
                            let first_task = holder.tasks[StagedChunkEnum::None as usize + 1];
                            if let Some(node) = self.graph.nodes.get_mut(first_task) {
                                node.in_queue = true;
                            }
                            self.queue.push(TaskHeapNode(
                                Self::calc_priority(
                                    &self.last_level,
                                    &self.last_high_priority,
                                    pos,
                                    StagedChunkEnum::from(1),
                                ) - 50,
                                first_task,
                            ));
                        }

                        self.chunk_map.insert(pos, holder);

                        warn!(
                            "Chunk {:?} reset to None and re-queued for regeneration (target: {:?})",
                            pos, target_stage
                        );
                    }
                } else {
                    error!("Failed to find holder for failed chunk {:?}", pos);
                }
            }
        }
        if counted_task {
            self.running_task_count -= 1;
        }
    }

    fn retry_deferred_primary_io(&mut self) {
        let positions = self.deferred_primary_io.keys().copied().collect::<Vec<_>>();
        for pos in positions {
            let Some(holder) = self.chunk_map.get(&pos) else {
                self.deferred_primary_io.remove(&pos);
                continue;
            };
            if holder.public
                && self.cluster_level.as_ref().is_some_and(|level| {
                    level
                        .cluster_primary_chunk_fences
                        .load()
                        .get(&pos)
                        .is_none_or(|fence| fence.load(std::sync::atomic::Ordering::Acquire) != 0)
                })
            {
                continue;
            }
            if let Some(chunk) = self.deferred_primary_io.remove(&pos) {
                self.running_task_count += 1;
                self.receive_chunk(pos, RecvChunk::IO(chunk));
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    fn work(mut self, level: &Arc<Level>) {
        debug!(
            "schedule thread start id: {:?} name: {}",
            thread::current().id(),
            thread::current().name().unwrap_or("unknown")
        );
        loop {
            self.retry_deferred_primary_io();
            self.poll_fresh_admissions();
            self.poll_persisted_admissions();
            self.poll_peer_install_claims();
            self.poll_cluster_publications();
            self.flush_owner_ready();
            if level.should_unload.swap(false, Relaxed) {
                self.garbage_collect_dependencies();
                self.process_unload_queue();
            }
            if level.should_save.swap(false, Relaxed) {
                self.save_all_chunk(false);
            }
            if level.shut_down_chunk_system.load(Relaxed) {
                info!("Saving chunks before shutdown...");
                self.garbage_collect_dependencies();
                self.process_unload_queue();
                self.save_all_chunk(true);
                break;
            }

            // 1. Get latest world state (player moves, etc)
            if self.advance_owner_demands_and_levels() {
                self.garbage_collect_dependencies();
            }

            self.poll_cluster_fetches();

            // Process unload queue periodically (every 1 second) to batch writes together
            // and act as a brief memory cache if a player walks back into the chunk.
            // This must run even when the queue is empty: `garbage_collect_dependencies`
            // is what puts stale dependency holders into the queue in the first place.
            if self.last_unload.elapsed() >= std::time::Duration::from_secs(1) {
                self.garbage_collect_dependencies();
                self.process_unload_queue();
                self.last_unload = std::time::Instant::now();
            }

            // 2. Process all pending chunk results from workers
            while let Ok((pos, data)) = self.recv_chunk.try_recv() {
                self.receive_chunk(pos, data);
            }
            self.poll_fresh_admissions();
            self.poll_persisted_admissions();
            self.poll_peer_install_claims();
            self.poll_cluster_publications();
            self.flush_owner_ready();

            // 3. Re-sort if world state changed or new tasks added
            if self.queue_dirty {
                self.sort_queue();
                self.queue_dirty = false;
            }

            // 4. Process ready tasks in the queue (up to max_in_flight)
            let mut io_batch = Vec::with_capacity(16);
            'out2: while let Some(task) = self.queue.pop() {
                if level.shut_down_chunk_system.load(Relaxed) {
                    self.queue.push(task);
                    info!("Shutdown detected during task processing, saving chunks...");
                    self.save_all_chunk(true);
                    break 'out2;
                }

                if self.running_task_count >= self.max_in_flight {
                    self.queue.push(task);
                    break 'out2;
                }
                if self.graph.nodes.get(task.1).is_some_and(|node| {
                    node.stage == StagedChunkEnum::Full
                        || (node.stage == StagedChunkEnum::Empty && level.cluster_genesis_required)
                }) && self.pending_fresh.len()
                    + self.pending_persisted.len()
                    + usize::from(self.running_task_count)
                    >= 128
                {
                    self.parked_full.push(task);
                    continue;
                }

                // Briefly check for high-priority results or world changes to avoid stalling
                while let Ok((pos, data)) = self.recv_chunk.try_recv() {
                    self.receive_chunk(pos, data);
                    if self.advance_owner_demands_and_levels() {
                        // If world state changed, we MUST re-sort before continuing
                        self.garbage_collect_dependencies();
                        self.queue.push(task);
                        self.queue_dirty = true;
                        break 'out2;
                    }
                }

                if let Some(node) = self.graph.nodes.get_mut(task.1) {
                    node.in_queue = false;
                    if node.in_degree != 0 {
                        continue;
                    }
                    node.in_flight = true;
                    let node = node.clone();

                    // A chunk can be advanced as part of a neighboring task's write cache.
                    // In that case its queued node may survive even though the returned
                    // ProtoChunk has already reached this stage. Dispatching the stale node
                    // would run the same stage twice and trip ProtoChunk's stage invariant.
                    let actual_stage = self
                        .chunk_map
                        .get(&node.pos)
                        .and_then(|holder| holder.chunk.as_ref())
                        .map(Chunk::get_stage_id);
                    if actual_stage.is_some_and(|stage| stage >= node.stage as u8) {
                        if let Some(holder) = self.chunk_map.get_mut(&node.pos) {
                            holder.current_stage = holder
                                .current_stage
                                .max(StagedChunkEnum::from(actual_stage.expect("checked above")));
                            let task_slot = &mut holder.tasks[node.stage as usize];
                            if *task_slot == task.1 {
                                *task_slot = NodeKey::null();
                            }
                        }
                        self.waiting_for_chunks.remove(&task.1);
                        self.drop_node(task.1);
                        continue;
                    }

                    // Cancel/drop task if chunk is out of range or no longer needed by any target/dependency
                    let effective_target = self
                        .chunk_map
                        .get(&node.pos)
                        .map_or(StagedChunkEnum::None, |h| {
                            h.target_stage.max(h.dependency_stage)
                        });

                    if node.stage > effective_target {
                        if let Some(holder) = self.chunk_map.get_mut(&node.pos) {
                            let task_slot = &mut holder.tasks[node.stage as usize];
                            if *task_slot == task.1 {
                                *task_slot = NodeKey::null();
                            }
                        }
                        self.waiting_for_chunks.remove(&task.1);
                        self.drop_node(task.1);
                        continue;
                    }

                    if crate::level::is_cluster_secondary() {
                        self.waiting_for_chunks.remove(&task.1);
                        self.drop_node(task.1);
                        if let Some(holder) = self.chunk_map.get_mut(&node.pos) {
                            let slot = &mut holder.tasks[node.stage as usize];
                            if *slot == task.1 {
                                *slot = NodeKey::null();
                            }
                        }
                        self.secondary_request_fetch(node.pos);
                        continue;
                    }

                    if node.stage == StagedChunkEnum::Empty {
                        self.running_task_count += 1;
                        let holder = self.chunk_map.get_mut(&node.pos).expect("holder exists");
                        debug_assert!(holder.occupied.is_null());
                        debug_assert_eq!(holder.current_stage, StagedChunkEnum::None);
                        let occupy = self.graph.nodes.insert(Node::new(
                            ChunkPos::new(i32::MAX, i32::MAX),
                            StagedChunkEnum::None,
                        ));
                        let effective_target = holder.target_stage.max(holder.dependency_stage);
                        for i in (holder.current_stage as usize + 1)..=(effective_target as usize) {
                            self.graph.add_edge(occupy, holder.tasks[i]);
                        }
                        holder.occupied = occupy;

                        io_batch.push(node.pos);
                        if io_batch.len() >= 16
                            && self
                                .io_read
                                .blocking_send(std::mem::take(&mut io_batch))
                                .is_err()
                        {
                            info!("IO read thread closed, saving remaining chunks...");
                            self.save_all_chunk(true);
                            break 'out2;
                        }
                    } else {
                        // Send any pending IO batch before starting generation
                        if !io_batch.is_empty()
                            && self
                                .io_read
                                .blocking_send(std::mem::take(&mut io_batch))
                                .is_err()
                        {
                            info!("IO read thread closed, saving remaining chunks...");
                            self.save_all_chunk(true);
                            break 'out2;
                        }

                        let write_radius = node.stage.get_write_radius();
                        let read_radius = node.stage.get_read_radius();

                        // Pre-validate that every chunk in the read area has its data present
                        // before we snapshot neighbors or swap the write area out.
                        //
                        // The dependency graph ensures predecessor *tasks* are complete, but
                        // there is a brief window between a task completing on a generation
                        // thread and its chunk data being placed back into the holder. Any
                        // stage whose write area overlaps with a currently-running task will
                        // see chunk==None in that window. We park here and let
                        // check_waiting_tasks() re-queue once all data has arrived.
                        {
                            let all_ready = (-read_radius..=read_radius).all(|dx| {
                                (-read_radius..=read_radius).all(|dy| {
                                    self.chunk_map
                                        .get(&node.pos.add_raw(dx, dy))
                                        .is_some_and(|h| h.chunk.is_some())
                                })
                            });

                            if !all_ready {
                                if let Some(n) = self.graph.nodes.get_mut(task.1) {
                                    n.in_queue = false;
                                    n.in_flight = false;
                                }
                                self.waiting_for_chunks.insert(task.1);
                                // Close the TOCTOU window: the chunk we're waiting for may
                                // have arrived in the recv_chunk drain that happened earlier
                                // in this same loop iteration, before this task was parked.
                                // If so, check_waiting_tasks() will immediately re-queue it
                                // so it isn't stranded with running_task_count==0.
                                self.check_waiting_tasks(crate::level::is_cluster_secondary());
                                continue;
                            }
                        }

                        let mut cache = Cache::new(
                            node.pos.x - write_radius,
                            node.pos.y - write_radius,
                            write_radius << 1 | 1,
                        );
                        cache.capture_seed = self
                            .cluster_level
                            .as_ref()
                            .is_some_and(|level| level.cluster_genesis_required);

                        if node.stage == StagedChunkEnum::Surface {
                            let mut neighborhood =
                                SurfaceBiomeNeighborhood::new(node.pos.x, node.pos.y);
                            for dx in -1..=1 {
                                for dz in -1..=1 {
                                    let holder = self
                                        .chunk_map
                                        .get(&node.pos.add_raw(dx, dz))
                                        .expect("surface biome dependency holder exists");
                                    let chunk = holder
                                        .chunk
                                        .as_ref()
                                        .expect("surface biome dependency is available");
                                    assert!(
                                        neighborhood.push_chunk(chunk),
                                        "surface biome dependency has an incomplete palette"
                                    );
                                }
                            }
                            cache.set_surface_biomes(neighborhood);
                        }

                        let occupy = self.graph.nodes.insert(Node::new(
                            ChunkPos::new(i32::MAX, i32::MAX),
                            StagedChunkEnum::None,
                        ));

                        for dx in -write_radius..=write_radius {
                            for dy in -write_radius..=write_radius {
                                let new_pos = node.pos.add_raw(dx, dy);
                                let holder =
                                    self.chunk_map.get_mut(&new_pos).expect("holder exists");
                                let mut tmp = None;
                                swap(&mut tmp, &mut holder.chunk);
                                let Some(tmp) = tmp else {
                                    panic!(
                                        "Missing chunk for position {:?} while processing generation task for {:?} stage {:?}",
                                        new_pos, node.pos, node.stage
                                    )
                                };
                                match tmp {
                                    Chunk::Level(chunk) => {
                                        cache.chunks.push(Chunk::Level(chunk));
                                    }
                                    Chunk::Proto(chunk) => {
                                        cache.chunks.push(Chunk::Proto(chunk));
                                    }
                                }

                                debug_assert!(holder.occupied.is_null());

                                let mut cur_edge = holder.occupied_by;
                                let mut prev_edge = EdgeKey::null();
                                let mut change_head = None;
                                while !cur_edge.is_null() {
                                    let edge = self.graph.edges.get(cur_edge).expect("edge exists");
                                    if self.graph.nodes.contains_key(edge.to) {
                                        prev_edge = cur_edge;
                                        cur_edge = edge.next;
                                        self.graph.add_edge(occupy, edge.to);
                                    } else {
                                        let next = edge.next;
                                        self.graph.edges.remove(cur_edge);
                                        cur_edge = next;
                                        if prev_edge.is_null() {
                                            change_head = Some(next);
                                        } else {
                                            self.graph
                                                .edges
                                                .get_mut(prev_edge)
                                                .expect("edge exists")
                                                .next = next;
                                        }
                                    }
                                }
                                if let Some(next) = change_head {
                                    holder.occupied_by = next;
                                }

                                holder.occupied = occupy;
                            }
                        }

                        self.running_task_count += 1;
                        let pos = node.pos;
                        let stage = node.stage;
                        let send_chunk = self.send_chunk.clone();
                        let level = level.clone();

                        self.generation_pool
                            .clone()
                            .expect("Chunk generation pool is missing: the secondary dispatch gate must park generation tasks before reaching the pool")
                            .spawn(move || {
                                let result = crate::chunk_system::worker_logic::run_generation(
                                    pos, cache, stage, &level,
                                );
                                let _ = send_chunk.send((pos, result));
                            });
                    }
                }
            }

            // Flush any remaining IO batch
            if !io_batch.is_empty()
                && self
                    .io_read
                    .blocking_send(std::mem::take(&mut io_batch))
                    .is_err()
            {
                info!("IO read thread closed, saving remaining chunks...");
                self.save_all_chunk(true);
            }

            // 5. Wait for work or results
            if self.queue.is_empty() {
                if self.running_task_count > 0 {
                    match self.recv_chunk.recv_timeout(Duration::from_millis(5)) {
                        Ok((pos, data)) => {
                            self.receive_chunk(pos, data);
                            if self.advance_owner_demands_and_levels() {
                                self.garbage_collect_dependencies();
                            }
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                            // Periodically check LevelChannel for new requests
                            if self.advance_owner_demands_and_levels() {
                                self.garbage_collect_dependencies();
                            }
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                    }
                } else if !self.pending_fresh.is_empty()
                    || !self.pending_persisted.is_empty()
                    || !self.parked_full.is_empty()
                {
                    crossbeam::select! {
                        recv(self.recv_chunk) -> received => {
                            match received {
                                Ok((pos, data)) => self.receive_chunk(pos, data),
                                Err(_) => break,
                            }
                        }
                        recv(self.fresh_wake_rx) -> _ => {}
                        recv(self.cluster_publications) -> publication => {
                            if let Ok(publication) = publication {
                                self.apply_cluster_publication(publication);
                            }
                        }
                        default(Duration::from_millis(5)) => {}
                    }
                    if self.advance_owner_demands_and_levels() {
                        self.garbage_collect_dependencies();
                    }
                    if !crate::level::is_cluster_secondary() {
                        self.check_waiting_tasks(false);
                        let _ = Self::restore_ready_tasks(
                            &mut self.graph,
                            &mut self.queue,
                            &self.chunk_map,
                            &self.last_level,
                            &self.last_high_priority,
                            &self.waiting_for_chunks,
                            false,
                        );
                    }
                } else {
                    // No tasks in flight, check for any unblocked waiting tasks or stranded ready tasks
                    if !crate::level::is_cluster_secondary() {
                        self.check_waiting_tasks(crate::level::is_cluster_secondary());
                        let restored = Self::restore_ready_tasks(
                            &mut self.graph,
                            &mut self.queue,
                            &self.chunk_map,
                            &self.last_level,
                            &self.last_high_priority,
                            &self.waiting_for_chunks,
                            crate::level::is_cluster_secondary(),
                        );
                        if restored > 0 || !self.queue.is_empty() {
                            debug!(
                                "Restored {restored} stranded ready chunk tasks to generation queue"
                            );
                            if self.queue_dirty {
                                self.sort_queue();
                                self.queue_dirty = false;
                            }
                            continue;
                        }
                    }
                    debug_assert!(self.debug_check());
                    debug_assert_eq!(self.running_task_count, 0);
                    // Both player-level and owner-demand changes are sampled on
                    // this bounded idle wake; the latter cannot wake LevelChannel.
                    let _ = self.fresh_wake_rx.recv_timeout(Duration::from_millis(5));
                    if self.advance_owner_demands_and_levels() {
                        self.garbage_collect_dependencies();
                    }
                }
                if self.queue_dirty {
                    self.sort_queue();
                    self.queue_dirty = false;
                }
            } else if self.running_task_count >= self.max_in_flight {
                // Queue has tasks, but we are at maximum in-flight capacity.
                // Wait for an in-flight worker to finish instead of busy-spinning.
                match self.recv_chunk.recv_timeout(Duration::from_millis(5)) {
                    Ok((pos, data)) => {
                        self.receive_chunk(pos, data);
                        if self.advance_owner_demands_and_levels() {
                            self.garbage_collect_dependencies();
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        if self.advance_owner_demands_and_levels() {
                            self.garbage_collect_dependencies();
                        }
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                }
                if self.queue_dirty {
                    self.sort_queue();
                    self.queue_dirty = false;
                }
            }
        }
        info!(
            "schedule: waiting for {} generation tasks to finish",
            self.running_task_count
        );
        let mut wait_iterations = 0;
        let max_wait_iterations = 100; // 5 seconds max wait
        while self.running_task_count > 0 && wait_iterations < max_wait_iterations {
            if let Ok((pos, data)) = self.recv_chunk.try_recv() {
                self.receive_chunk(pos, data);
                wait_iterations = 0;
            } else {
                wait_iterations += 1;
                if wait_iterations % 20 == 0 {
                    warn!(
                        "Still waiting for {} tasks to complete (waited {}ms)",
                        self.running_task_count,
                        wait_iterations * 50
                    );
                }
                thread::sleep(Duration::from_millis(50));
            }
        }

        if self.running_task_count > 0 {
            warn!(
                "Cancelling {} in-flight generation tasks",
                self.running_task_count
            );
            let mut nodes_to_drop = Vec::new();

            for holder in self.chunk_map.values_mut() {
                for task in &mut holder.tasks {
                    if !task.is_null() {
                        self.waiting_for_chunks.remove(task);
                        nodes_to_drop.push(*task);
                        *task = NodeKey::null();
                    }
                }

                if !holder.occupied.is_null()
                    && let Some(node) = self.graph.nodes.get(holder.occupied)
                    && node.pos.x == i32::MAX
                    && node.pos.y == i32::MAX
                {
                    nodes_to_drop.push(holder.occupied);
                    holder.occupied = NodeKey::null();
                }

                self.graph.drop_edge_chain(holder.occupied_by);
                holder.occupied_by = EdgeKey::null();
            }

            for node_key in nodes_to_drop {
                self.drop_node(node_key);
            }

            self.running_task_count = 0;
        }

        drop(self.io_write);

        let unreleased_count = self.graph.nodes.len();
        if unreleased_count > 0 {
            warn!(
                "Cleaning up {} unreleased nodes from incomplete tasks",
                unreleased_count
            );
        }
        self.graph.edges.clear();
    }

    fn debug_check(&self) -> bool {
        if !self.graph.nodes.is_empty() {
            for (key, value) in &self.graph.nodes {
                error!("unrelease node {key:?}: {value:?}");
            }
            panic!("nodes count error");
        }
        for (pos, holder) in &self.chunk_map {
            for i in &holder.tasks {
                debug_assert!(i.is_null());
            }
            debug_assert_eq!(
                holder.target_stage,
                StagedChunkEnum::level_to_stage(
                    *self.last_level.get(pos).unwrap_or(&ChunkLoading::MAX_LEVEL)
                )
            );
            let effective = holder.target_stage.max(holder.dependency_stage);
            debug_assert!(holder.current_stage >= effective);
            debug_assert!(holder.occupied.is_null());
            if holder.current_stage != StagedChunkEnum::None {
                debug_assert_eq!(
                    holder.chunk.as_ref().expect("chunk exists").get_stage_id(),
                    holder.current_stage as u8
                );
            }
        }
        true
    }
}

#[cfg(test)]
mod secondary_park_regression {
    use super::*;

    use pumpkin_config::world::LevelConfig;
    use pumpkin_data::dimension::Dimension;
    use tempfile::TempDir;

    static SECONDARY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct SecondaryTestMode {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl SecondaryTestMode {
        fn enable() -> Self {
            let lock = SECONDARY_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            crate::level::set_cluster_secondary(true);
            Self { _lock: lock }
        }
    }

    impl Drop for SecondaryTestMode {
        fn drop(&mut self) {
            crate::level::set_cluster_secondary(false);
        }
    }

    fn secondary_test_schedule() -> GenerationSchedule {
        let (send_chunk, recv_chunk) = crossbeam::channel::unbounded();
        let (io_read_tx, _io_read_rx) = tokio::sync::mpsc::channel(8);
        let (io_write_tx, _io_write_rx) = tokio::sync::mpsc::channel(8);
        let (fresh_wake_tx, fresh_wake_rx) = crossbeam::channel::bounded(1);
        let (_cluster_publication_tx, cluster_publications) = crossbeam::channel::bounded(8);
        let (_peer_install_claim_tx, peer_install_claims) = crossbeam::channel::bounded(8);
        let (_owner_demand_tx, owner_demand_rx) = crossbeam::channel::bounded(8);
        GenerationSchedule {
            queue: BinaryHeap::new(),
            parked_full: BinaryHeap::new(),
            graph: DAG::default(),
            last_level: ChunkLevel::default(),
            player_level: ChunkLevel::default(),
            owner_demands: HashMapType::default(),
            peer_request_retention: HashSetType::default(),
            owner_ready_routes: HashMapType::default(),
            pending_owner_ready: VecDeque::new(),
            publication_generations: HashMapType::default(),
            next_publication_generation: 1,
            owner_demand_rx,
            deferred_primary_io: HashMapType::default(),
            last_high_priority: Vec::new(),
            send_level: Arc::new(LevelChannel::new()),
            public_chunk_map: Arc::new(DashMap::new()),
            loaded_chunk_changes: Arc::new(crossbeam::queue::SegQueue::new()),
            chunk_map: HashMap::new(),
            unload_chunks: HashSetType::default(),
            waiting_for_chunks: HashSetType::default(),
            io_lock: Arc::new((
                Mutex::new(HashMapType::default()),
                tokio::sync::Notify::new(),
            )),
            running_task_count: 0,
            max_in_flight: 2,
            queue_dirty: false,
            recv_chunk,
            io_read: io_read_tx,
            io_write: io_write_tx,
            send_chunk,
            listener: Arc::new(ChunkListener::new()),
            lighting_config: LightingEngineConfig::Default,
            last_unload: std::time::Instant::now(),
            generation_pool: None,
            cluster_fetches: HashMapType::default(),
            cluster_level: None,
            pending_fresh: HashMapType::default(),
            pending_persisted: HashMapType::default(),
            pending_persisted_decisions: VecDeque::new(),
            next_fresh_generation: 1,
            fresh_wake_tx,
            fresh_wake_rx,
            cluster_publications,
            peer_install_claims,
            pending_claim_results: VecDeque::new(),
        }
    }

    #[test]
    fn peer_request_retention_is_independent_of_action_leases() {
        let _mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let (sender, receiver) = crossbeam::channel::bounded(8);
        schedule.owner_demand_rx = receiver;
        let position = ChunkPos::new(8, -3);
        sender
            .send(crate::level::OwnerChunkDemand::RetainPeerRequest { position })
            .unwrap();
        sender
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 11,
                ready: None,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        sender
            .send(crate::level::OwnerChunkDemand::ReleasePeerRequest { position })
            .unwrap();
        assert!(!schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&43));
        assert!(!schedule.peer_request_retention.contains(&position));
        sender
            .send(crate::level::OwnerChunkDemand::Release {
                position,
                lease: 11,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert!(!schedule.last_level.contains_key(&position));
        sender
            .send(crate::level::OwnerChunkDemand::RetainPeerRequest { position })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        sender
            .send(crate::level::OwnerChunkDemand::Release {
                position,
                lease: 11,
            })
            .unwrap();
        assert!(!schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&43));
    }

    #[test]
    fn owner_demand_retains_full_stage_until_last_lease_releases() {
        let _mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let (sender, receiver) = crossbeam::channel::bounded(8);
        schedule.owner_demand_rx = receiver;
        let position = ChunkPos::new(8, -3);
        sender
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 11,
                ready: None,
            })
            .unwrap();
        sender
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 12,
                ready: None,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&43));
        assert_eq!(schedule.owner_demands[&position].len(), 2);

        sender
            .send(crate::level::OwnerChunkDemand::Release {
                position,
                lease: 11,
            })
            .unwrap();
        assert!(!schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&43));

        sender
            .send(crate::level::OwnerChunkDemand::Release {
                position,
                lease: 12,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert!(!schedule.owner_demands.contains_key(&position));
        assert!(!schedule.last_level.contains_key(&position));
    }

    #[test]
    fn owner_demand_ready_is_generation_qualified_and_retained_until_delivered() {
        let _mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let (demand_tx, demand_rx) = crossbeam::channel::bounded(2);
        schedule.owner_demand_rx = demand_rx;
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(1);
        let position = ChunkPos::new(13, -6);
        schedule.publication_generations.insert(position, 27);
        schedule.public_chunk_map.insert(
            position,
            crate::chunk::ChunkData::empty_sync(position.x, position.y),
        );
        ready_tx
            .try_send(crate::level::OwnerChunkReady {
                world_key: 0,
                dimension: 0,
                position,
                lease: 99,
                publication_generation: 1,
            })
            .unwrap();
        demand_tx
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 7,
                ready: Some(ready_tx),
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.pending_owner_ready.len(), 1);
        assert_eq!(ready_rx.try_recv().unwrap().lease, 99);
        schedule.flush_owner_ready();
        let ready = ready_rx.try_recv().unwrap();
        assert_eq!(
            (ready.position, ready.lease, ready.publication_generation),
            (position, 7, 27)
        );

        schedule.pending_owner_ready.push_back((position, 7, 27));
        schedule.publication_generations.insert(position, 28);
        schedule.flush_owner_ready();
        assert!(ready_rx.try_recv().is_err());

        schedule.pending_owner_ready.push_back((position, 7, 27));
        demand_tx
            .send(crate::level::OwnerChunkDemand::Release { position, lease: 7 })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert!(ready_rx.try_recv().is_err());
        assert!(schedule.pending_owner_ready.is_empty());
    }

    #[tokio::test]
    async fn unsettled_primary_chunk_cannot_unload_until_owner_releases_it() {
        let _mode = SecondaryTestMode::enable();
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let position = ChunkPos::new(3, 5);
        level
            .cluster_unsettled_primary_chunks
            .store(Arc::new([position].into_iter().collect()));
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level.clone());
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        let chunk = crate::chunk::ChunkData::empty_sync(position.x, position.y);
        schedule.publish_chunk(position, chunk.clone());
        holder.public = true;
        holder.chunk = Some(Chunk::Level(chunk));
        schedule.chunk_map.insert(position, holder);
        assert!(level.claim_primary_chunk_for_actions(position));
        schedule.unload_chunks.insert(position);
        schedule.process_unload_queue();
        assert!(schedule.chunk_map.contains_key(&position));
        assert!(schedule.unload_chunks.contains(&position));

        level
            .cluster_unsettled_primary_chunks
            .store(Arc::new(Default::default()));
        schedule.process_unload_queue();
        assert!(schedule.chunk_map.contains_key(&position));
        level.release_primary_chunk_action_claim(position);
        schedule.process_unload_queue();
        assert!(!schedule.chunk_map.contains_key(&position));
    }

    #[tokio::test]
    async fn primary_prepublication_claim_blocks_first_unload() {
        let _mode = SecondaryTestMode::enable();
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let position = ChunkPos::new(31, 7);
        level.install_primary_chunk_fence(position);
        assert!(level.claim_primary_chunk_for_actions(position));
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level.clone());
        let chunk = crate::chunk::ChunkData::empty_sync(position.x, position.y);
        schedule.publish_chunk(position, chunk.clone());
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        holder.public = true;
        holder.chunk = Some(Chunk::Level(chunk));
        schedule.chunk_map.insert(position, holder);
        schedule.unload_chunks.insert(position);
        schedule.process_unload_queue();
        assert!(schedule.chunk_map.contains_key(&position));
        assert!(schedule.unload_chunks.contains(&position));
        let stale_reader = level.cluster_primary_chunk_fences.load_full();
        let stale_token = stale_reader.get(&position).unwrap().clone();
        level.release_primary_chunk_action_claim(position);
        schedule.process_unload_queue();
        assert!(!schedule.chunk_map.contains_key(&position));
        assert_eq!(stale_token.load(std::sync::atomic::Ordering::Acquire), 2);
        assert!(
            !level
                .cluster_primary_chunk_fences
                .load()
                .contains_key(&position)
        );
    }

    #[tokio::test]
    async fn primary_proto_downgrade_waits_for_owner_action_claim() {
        let _mode = SecondaryTestMode::enable();
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let position = ChunkPos::new(7, -2);
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level.clone());
        let public = crate::chunk::ChunkData::empty_sync(position.x, position.y);
        schedule.publish_chunk(position, public.clone());
        let occupied = schedule
            .graph
            .nodes
            .insert(Node::new(position, StagedChunkEnum::Empty));
        let mut holder = ChunkHolder::default();
        holder.public = true;
        holder.occupied = occupied;
        holder.chunk = Some(Chunk::Level(public));
        schedule.chunk_map.insert(position, holder);
        schedule.running_task_count = 1;
        assert!(level.claim_primary_chunk_for_actions(position));
        let proto = crate::generation::proto_chunk::ProtoChunk::new(
            position.x,
            position.y,
            &level.world_gen(),
        );
        schedule.receive_chunk(position, RecvChunk::IO(Chunk::Proto(Box::new(proto))));
        assert!(schedule.public_chunk_map.contains_key(&position));
        assert!(schedule.deferred_primary_io.contains_key(&position));
        assert_eq!(schedule.running_task_count, 0);

        level.release_primary_chunk_action_claim(position);
        schedule.retry_deferred_primary_io();
        assert!(!schedule.public_chunk_map.contains_key(&position));
        assert!(!schedule.deferred_primary_io.contains_key(&position));
    }

    #[test]
    fn owner_demand_creates_primary_generation_target() {
        let _guard = SECONDARY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::level::set_cluster_secondary(false);
        let mut schedule = secondary_test_schedule();
        let (sender, receiver) = crossbeam::channel::bounded(1);
        schedule.owner_demand_rx = receiver;
        let position = ChunkPos::new(4, 7);
        sender
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 71,
                ready: None,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(
            schedule.chunk_map[&position].target_stage,
            StagedChunkEnum::Full
        );
        assert!(schedule.graph.nodes.len() > 0);
    }

    #[test]
    fn owner_release_restores_player_stage() {
        let _mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let position = ChunkPos::new(-6, 2);
        let mut player_level = ChunkLevel::default();
        player_level.insert(position, 44);
        let mut changes = HashMapType::default();
        changes.insert(position, (StagedChunkEnum::None, StagedChunkEnum::Spawn));
        schedule.send_level.set_level((changes, player_level));
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(
            schedule
                .chunk_map
                .get(&position)
                .map(|holder| holder.target_stage),
            None
        );
        assert_eq!(schedule.last_level.get(&position), Some(&44));

        let (sender, receiver) = crossbeam::channel::bounded(2);
        schedule.owner_demand_rx = receiver;
        sender
            .send(crate::level::OwnerChunkDemand::Acquire {
                position,
                lease: 9,
                ready: None,
            })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&43));
        sender
            .send(crate::level::OwnerChunkDemand::Release { position, lease: 9 })
            .unwrap();
        assert!(schedule.advance_owner_demands_and_levels());
        assert_eq!(schedule.last_level.get(&position), Some(&44));
    }

    #[tokio::test]
    async fn persisted_chunk_requires_matching_owner_publication() {
        let _guard = SECONDARY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        crate::level::set_cluster_secondary(false);
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let (admission_tx, mut admission_rx) = tokio::sync::mpsc::channel(8);
        level
            .persisted_chunk_admission
            .store(Some(Arc::new(admission_tx)));
        let (decision_tx, mut decision_rx) = tokio::sync::mpsc::channel(8);
        level
            .cluster_persisted_decision
            .store(Some(Arc::new(decision_tx)));
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level);
        let pos = ChunkPos::new(9, -4);
        let original = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let approved = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        holder.chunk = Some(Chunk::Level(original.clone()));
        schedule.chunk_map.insert(pos, holder);
        schedule.pending_persisted.insert(
            pos,
            PendingPersisted {
                original,
                approved: None,
                generation: 17,
                response: None,
                submitted: false,
                rejected: false,
                terminal_rejection: false,
                peer_claimed: false,
                unexpected_arrival_reported: false,
            },
        );

        schedule.poll_persisted_admissions();
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        let PersistedChunkAdmission::Prepare {
            generation,
            respond,
            ..
        } = admission_rx.try_recv().unwrap()
        else {
            panic!("expected persisted prepare");
        };
        assert_eq!(generation, 17);
        schedule.receive_chunk(
            pos,
            RecvChunk::IO(Chunk::Level(crate::chunk::ChunkData::empty_sync(
                pos.x, pos.y,
            ))),
        );
        assert!(schedule.chunk_map.get(&pos).is_some_and(|holder| {
            matches!(holder.chunk.as_ref(), Some(Chunk::Level(chunk)) if Arc::ptr_eq(chunk, &schedule.pending_persisted[&pos].original))
        }));
        schedule.receive_chunk(
            pos,
            RecvChunk::Cluster((pos, crate::chunk::ChunkData::empty_sync(pos.x, pos.y))),
        );
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        assert!(schedule.pending_persisted.contains_key(&pos));

        let rejected_pos = ChunkPos::new(10, -4);
        let rejected_chunk = crate::chunk::ChunkData::empty_sync(rejected_pos.x, rejected_pos.y);
        let mut rejected_holder = ChunkHolder::default();
        rejected_holder.current_stage = StagedChunkEnum::Full;
        rejected_holder.chunk = Some(Chunk::Level(rejected_chunk.clone()));
        schedule.chunk_map.insert(rejected_pos, rejected_holder);
        schedule.pending_persisted.insert(
            rejected_pos,
            PendingPersisted {
                original: rejected_chunk,
                approved: None,
                generation: 18,
                response: None,
                submitted: false,
                rejected: false,
                terminal_rejection: false,
                peer_claimed: false,
                unexpected_arrival_reported: false,
            },
        );
        schedule.poll_persisted_admissions();
        let PersistedChunkAdmission::Prepare {
            respond: rejected_response,
            ..
        } = admission_rx.try_recv().unwrap()
        else {
            panic!("expected persisted prepare");
        };
        assert!(rejected_response.send(None).is_ok());
        schedule.poll_persisted_admissions();
        let rejected = decision_rx.try_recv().unwrap();
        assert_eq!(rejected.generation, 18);
        assert!(rejected.recovery);
        assert!(!schedule.public_chunk_map.contains_key(&rejected_pos));
        schedule.unload_chunks.insert(rejected_pos);
        schedule.process_unload_queue();
        assert!(!schedule.pending_persisted.contains_key(&rejected_pos));

        let held_pos = ChunkPos::new(12, -4);
        let held_chunk = crate::chunk::ChunkData::empty_sync(held_pos.x, held_pos.y);
        let mut held_holder = ChunkHolder::default();
        held_holder.current_stage = StagedChunkEnum::Full;
        held_holder.chunk = Some(Chunk::Level(held_chunk.clone()));
        schedule.chunk_map.insert(held_pos, held_holder);
        schedule.pending_persisted.insert(
            held_pos,
            PendingPersisted {
                original: held_chunk,
                approved: None,
                generation: 20,
                response: None,
                submitted: true,
                rejected: true,
                terminal_rejection: true,
                peer_claimed: false,
                unexpected_arrival_reported: false,
            },
        );
        schedule.cluster_fetches.insert(held_pos, Instant::now());
        schedule.unload_chunks.insert(held_pos);
        schedule.process_unload_queue();
        assert!(schedule.pending_persisted.contains_key(&held_pos));
        assert!(!schedule.public_chunk_map.contains_key(&held_pos));

        let recovered_pos = ChunkPos::new(11, -4);
        let stale = crate::chunk::ChunkData::empty_sync(recovered_pos.x, recovered_pos.y);
        let recovered = crate::chunk::ChunkData::empty_sync(recovered_pos.x, recovered_pos.y);
        let mut recovered_holder = ChunkHolder::default();
        recovered_holder.current_stage = StagedChunkEnum::Full;
        recovered_holder.chunk = Some(Chunk::Level(stale.clone()));
        schedule.chunk_map.insert(recovered_pos, recovered_holder);
        schedule.pending_persisted.insert(
            recovered_pos,
            PendingPersisted {
                original: stale,
                approved: None,
                generation: 19,
                response: None,
                submitted: false,
                rejected: true,
                terminal_rejection: true,
                peer_claimed: false,
                unexpected_arrival_reported: false,
            },
        );
        publish_persisted_reservation(
            &schedule
                .cluster_level
                .as_ref()
                .unwrap()
                .cluster_publication_reservations,
            recovered_pos,
            Some(crate::level::PersistedChunkReservation {
                generation: 19,
                state: crate::level::PersistedChunkReservationState::Recovery,
            }),
        );
        let (old_claim_tx, mut old_claim_rx) = tokio::sync::mpsc::channel(1);
        schedule.process_peer_install_claim(crate::level::PeerChunkInstallClaim::Claim {
            position: recovered_pos,
            generation: 18,
            respond: old_claim_tx,
        });
        assert!(!old_claim_rx.try_recv().unwrap().allowed);
        let (claim_result_tx, mut claim_result_rx) = tokio::sync::mpsc::channel(1);
        schedule.process_peer_install_claim(crate::level::PeerChunkInstallClaim::Claim {
            position: recovered_pos,
            generation: 19,
            respond: claim_result_tx,
        });
        assert!(claim_result_rx.try_recv().unwrap().allowed);
        schedule.unload_chunks.insert(recovered_pos);
        schedule.process_unload_queue();
        assert!(schedule.pending_persisted.contains_key(&recovered_pos));
        assert!(!schedule.public_chunk_map.contains_key(&recovered_pos));
        let (claim_route, claim_rx) = crossbeam::channel::bounded(1);
        schedule.peer_install_claims = claim_rx;
        schedule
            .cluster_level
            .as_ref()
            .unwrap()
            .cluster_peer_install_claim
            .store(Some(Arc::new(claim_route.clone())));
        assert!(
            claim_route
                .try_send(crate::level::PeerChunkInstallClaim::Release {
                    position: ChunkPos::new(99, 99),
                    generation: 1,
                })
                .is_ok()
        );
        assert!(
            schedule
                .cluster_level
                .as_ref()
                .unwrap()
                .try_release_peer_chunk_install(recovered_pos, 19)
        );
        assert_eq!(
            schedule
                .cluster_level
                .as_ref()
                .unwrap()
                .cluster_peer_install_releases
                .len(),
            1
        );
        schedule.poll_peer_install_claims();
        assert!(!schedule.pending_persisted[&recovered_pos].peer_claimed);
        let (reclaim_tx, mut reclaim_rx) = tokio::sync::mpsc::channel(1);
        schedule.process_peer_install_claim(crate::level::PeerChunkInstallClaim::Claim {
            position: recovered_pos,
            generation: 19,
            respond: reclaim_tx,
        });
        assert!(reclaim_rx.try_recv().unwrap().allowed);
        schedule.receive_chunk(
            recovered_pos,
            RecvChunk::Cluster((recovered_pos, recovered.clone())),
        );
        assert!(!schedule.public_chunk_map.contains_key(&recovered_pos));
        assert!(schedule.pending_persisted[&recovered_pos].peer_claimed);
        let (wrong_respond, mut wrong_result) = tokio::sync::oneshot::channel();
        schedule.apply_cluster_publication(ClusterSnapshotPublication {
            position: recovered_pos,
            snapshot: recovered.clone(),
            reservation_generation: Some(18),
            respond: wrong_respond,
        });
        assert_eq!(wrong_result.try_recv(), Ok(false));
        assert!(!schedule.public_chunk_map.contains_key(&recovered_pos));
        let (published, mut published_result) = tokio::sync::oneshot::channel();
        schedule.apply_cluster_publication(ClusterSnapshotPublication {
            position: recovered_pos,
            snapshot: recovered.clone(),
            reservation_generation: Some(19),
            respond: published,
        });
        assert_eq!(published_result.try_recv(), Ok(true));
        assert!(Arc::ptr_eq(
            &schedule.public_chunk_map.get(&recovered_pos).unwrap(),
            &recovered
        ));
        assert!(!schedule.pending_persisted.contains_key(&recovered_pos));

        let (late_respond, mut late_result) = tokio::sync::oneshot::channel();
        schedule.apply_cluster_publication(ClusterSnapshotPublication {
            position: recovered_pos,
            snapshot: crate::chunk::ChunkData::empty_sync(recovered_pos.x, recovered_pos.y),
            reservation_generation: Some(19),
            respond: late_respond,
        });
        assert_eq!(late_result.try_recv(), Ok(false));
        assert!(Arc::ptr_eq(
            &schedule.public_chunk_map.get(&recovered_pos).unwrap(),
            &recovered
        ));

        assert!(respond.send(Some(approved.clone())).is_ok());
        schedule.poll_persisted_admissions();
        let approved_decision = decision_rx.try_recv().unwrap();
        assert_eq!(approved_decision.generation, 17);
        assert!(!approved_decision.recovery);
        assert!(Arc::ptr_eq(
            &schedule.public_chunk_map.get(&pos).unwrap(),
            &approved
        ));
        let PersistedChunkAdmission::Published { generation, .. } =
            admission_rx.try_recv().unwrap()
        else {
            panic!("expected persisted publication");
        };
        assert_eq!(generation, 17);
    }

    #[tokio::test]
    async fn fresh_chunk_remains_private_until_owner_admission() {
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let (admission_tx, mut admission_rx) = tokio::sync::mpsc::channel(8);
        level
            .fresh_chunk_admission
            .store(Some(Arc::new(admission_tx)));
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level);
        let pos = ChunkPos::new(4, 7);
        let chunk = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        holder.chunk = Some(Chunk::Level(chunk.clone()));
        schedule.chunk_map.insert(pos, holder);
        schedule.pending_fresh.insert(
            pos,
            PendingFresh {
                image: std::sync::Arc::from(crate::level::cluster_encode_snapshot(&chunk)),
                chunk,
                palettes: Some(InitialChunkPalettes {
                    min_y: 0,
                    blocks: Box::new([]),
                    biomes: Box::new([]),
                }),
                generation: 1,
                response: None,
                abort: false,
            },
        );
        let mut changes = HashMapType::default();
        changes.insert(pos, (StagedChunkEnum::None, StagedChunkEnum::Full));
        let mut levels = ChunkLevel::default();
        levels.insert(pos, ChunkLoading::FULL_CHUNK_LEVEL);
        assert!(schedule.resort_work((Some((changes, levels)), None)));
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        schedule.poll_fresh_admissions();
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        let FreshChunkAdmission::Prepare {
            generation,
            respond,
            ..
        } = admission_rx.try_recv().unwrap()
        else {
            panic!("fresh chunk was not prepared");
        };
        assert_eq!(generation, 1);
        schedule.chunk_map.get_mut(&pos).unwrap().target_stage = StagedChunkEnum::None;
        respond.send(true).unwrap();
        schedule.poll_fresh_admissions();
        assert!(schedule.public_chunk_map.contains_key(&pos));
        let FreshChunkAdmission::Published { generation, .. } = admission_rx.try_recv().unwrap()
        else {
            panic!("fresh chunk publication was not reported");
        };
        assert_eq!(generation, 1);
        assert!(!schedule.pending_fresh.contains_key(&pos));
        assert!(schedule.unload_chunks.contains(&pos));
    }

    #[tokio::test]
    async fn fresh_admission_does_not_publish_replaced_chunk() {
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let (admission_tx, mut admission_rx) = tokio::sync::mpsc::channel(8);
        level
            .fresh_chunk_admission
            .store(Some(Arc::new(admission_tx)));
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level);
        let pos = ChunkPos::new(4, 7);
        let original = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let replacement = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        holder.chunk = Some(Chunk::Level(original.clone()));
        schedule.chunk_map.insert(pos, holder);
        schedule.pending_fresh.insert(
            pos,
            PendingFresh {
                image: std::sync::Arc::from(crate::level::cluster_encode_snapshot(&original)),
                chunk: original,
                palettes: Some(InitialChunkPalettes {
                    min_y: 0,
                    blocks: Box::new([]),
                    biomes: Box::new([]),
                }),
                generation: 1,
                response: None,
                abort: false,
            },
        );
        schedule.poll_fresh_admissions();
        let FreshChunkAdmission::Prepare { respond, .. } = admission_rx.try_recv().unwrap() else {
            panic!("fresh chunk was not prepared");
        };
        schedule.chunk_map.get_mut(&pos).unwrap().chunk = Some(Chunk::Level(replacement));
        respond.send(true).unwrap();
        schedule.poll_fresh_admissions();
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        schedule.poll_fresh_admissions();
        let FreshChunkAdmission::Aborted { generation, .. } = admission_rx.try_recv().unwrap()
        else {
            panic!("replaced chunk did not abort its owner admission");
        };
        assert_eq!(generation, 1);
    }

    #[test]
    fn saturated_fresh_admission_parks_only_full_generation() {
        let mut schedule = secondary_test_schedule();
        let full_pos = ChunkPos::new(1, 1);
        let full = schedule
            .graph
            .nodes
            .insert(Node::new(full_pos, StagedChunkEnum::Full));
        schedule.parked_full.push(TaskHeapNode(0, full));
        let empty_pos = ChunkPos::new(2, 2);
        let empty = schedule
            .graph
            .nodes
            .insert(Node::new(empty_pos, StagedChunkEnum::Empty));
        schedule.queue.push(TaskHeapNode(0, empty));
        let chunk = crate::chunk::ChunkData::empty_sync(0, 0);
        for index in 0..128 {
            schedule.pending_fresh.insert(
                ChunkPos::new(index, 0),
                PendingFresh {
                    image: std::sync::Arc::from(crate::level::cluster_encode_snapshot(&chunk)),
                    chunk: chunk.clone(),
                    palettes: None,
                    generation: index as u64,
                    response: None,
                    abort: false,
                },
            );
        }
        schedule.resume_parked_full();
        assert_eq!(schedule.queue.pop().unwrap().1, empty);
        assert!(schedule.queue.is_empty());
        schedule.pending_fresh.remove(&ChunkPos::new(0, 0));
        schedule.resume_parked_full();
        assert_eq!(schedule.queue.pop().unwrap().1, full);
    }

    #[tokio::test]
    async fn primary_generation_without_genesis_palette_never_publishes() {
        let root = TempDir::new().unwrap();
        let level = Level::from_root_folder_with_cluster_genesis(
            &LevelConfig::default(),
            root.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
            true,
        );
        let mut schedule = secondary_test_schedule();
        schedule.cluster_level = Some(level);
        let pos = ChunkPos::new(4, 7);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Spawn;
        schedule.chunk_map.insert(pos, holder);
        schedule.running_task_count = 1;
        let mut generated = Cache::new(pos.x, pos.y, 1);
        generated
            .chunks
            .push(Chunk::Level(crate::chunk::ChunkData::empty_sync(
                pos.x, pos.y,
            )));
        schedule.receive_chunk(pos, RecvChunk::Generation(generated));
        assert!(!schedule.public_chunk_map.contains_key(&pos));
        assert!(!schedule.chunk_map.get(&pos).unwrap().public);
        assert!(!schedule.pending_fresh.contains_key(&pos));
    }

    fn parked_noise_task(schedule: &mut GenerationSchedule) -> NodeKey {
        let pos = ChunkPos::new(0, 0);
        let stage = StagedChunkEnum::Noise;
        let node_key = schedule.graph.nodes.insert(Node::new(pos, stage));
        let mut holder = ChunkHolder::default();
        holder.target_stage = stage;
        holder.dependency_stage = StagedChunkEnum::None;
        holder.current_stage = StagedChunkEnum::Empty;
        holder.chunk = Some(Chunk::Level(crate::chunk::ChunkData::empty_sync(
            pos.x, pos.y,
        )));
        holder.tasks[stage as usize] = node_key;
        schedule.chunk_map.insert(pos, holder);
        schedule.waiting_for_chunks.insert(node_key);
        node_key
    }

    #[test]
    fn secondary_parked_generation_task_never_requeued_on_idle() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let parked = parked_noise_task(&mut schedule);
        schedule.check_waiting_tasks(true);
        assert!(schedule.queue.is_empty());
        assert!(schedule.waiting_for_chunks.contains(&parked));
        assert!(
            !schedule
                .graph
                .nodes
                .get(parked)
                .expect("parked node exists")
                .in_queue
        );
        let waiting_snapshot = schedule.waiting_for_chunks.clone();
        let restored = GenerationSchedule::restore_ready_tasks(
            &mut schedule.graph,
            &mut schedule.queue,
            &schedule.chunk_map,
            &schedule.last_level,
            &schedule.last_high_priority,
            &waiting_snapshot,
            true,
        );
        assert_eq!(restored, 0);
        assert!(schedule.queue.is_empty());
        assert!(schedule.waiting_for_chunks.contains(&parked));
        assert!(
            !schedule
                .graph
                .nodes
                .get(parked)
                .expect("parked node exists")
                .in_queue
        );
    }

    #[test]
    fn secondary_resort_without_dispatcher_never_marks_fetch_in_flight() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(3, 5);
        let mut changes = HashMapType::default();
        changes.insert(pos, (StagedChunkEnum::None, StagedChunkEnum::Full));
        let mut levels = ChunkLevel::default();
        levels.insert(pos, ChunkLoading::FULL_CHUNK_LEVEL);
        assert!(schedule.resort_work((Some((changes, levels)), None)));
        assert!(schedule.graph.nodes.is_empty());
        assert!(schedule.queue.is_empty());
        assert!(schedule.waiting_for_chunks.is_empty());
        assert!(schedule.chunk_map.get(&pos).is_none());
        assert!(schedule.debug_check());
        assert!(!schedule.cluster_fetches.contains_key(&pos));
    }

    #[test]
    fn secondary_resort_unload_never_creates_schedule_nodes() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(3, 5);
        let mut holder = ChunkHolder::default();
        holder.target_stage = StagedChunkEnum::Full;
        schedule.chunk_map.insert(pos, holder);
        schedule.cluster_fetches.insert(pos, Instant::now());
        let mut changes = HashMapType::default();
        changes.insert(pos, (StagedChunkEnum::Full, StagedChunkEnum::None));
        let levels = ChunkLevel::default();
        assert!(schedule.resort_work((Some((changes, levels)), None)));
        assert!(schedule.graph.nodes.is_empty());
        assert!(schedule.queue.is_empty());
        assert!(!schedule.cluster_fetches.contains_key(&pos));
        let holder = schedule.chunk_map.get(&pos).expect("holder exists");
        assert_eq!(holder.target_stage, StagedChunkEnum::None);
        assert!(schedule.unload_chunks.contains(&pos));
    }

    #[test]
    fn secondary_ensure_dependency_chain_creates_nothing() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(1, 2);
        let dependency = schedule
            .graph
            .nodes
            .insert(Node::new(pos, StagedChunkEnum::Empty));
        let mut holder = ChunkHolder::default();
        let created = GenerationSchedule::ensure_dependency_chain(
            &mut schedule.graph,
            &mut schedule.queue,
            &schedule.last_level,
            &schedule.last_high_priority,
            dependency,
            pos,
            &mut holder,
            StagedChunkEnum::Full,
            true,
        );
        assert!(created.iter().all(|done| !done));
        assert_eq!(schedule.graph.nodes.len(), 1);
        assert!(schedule.queue.is_empty());
        assert_eq!(holder.dependency_stage, StagedChunkEnum::None);
        assert!(holder.occupied_by.is_null());
    }

    #[test]
    fn secondary_restore_ready_tasks_skips_generation_nodes() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let parked = parked_noise_task(&mut schedule);
        let occupy = schedule.graph.nodes.insert(Node::new(
            ChunkPos::new(i32::MAX, i32::MAX),
            StagedChunkEnum::None,
        ));
        schedule.graph.add_edge(occupy, parked);
        schedule
            .graph
            .nodes
            .get_mut(parked)
            .expect("parked node exists")
            .in_degree = 1;
        schedule.graph.nodes.remove(occupy);
        let waiting_snapshot = schedule.waiting_for_chunks.clone();
        let restored = GenerationSchedule::restore_ready_tasks(
            &mut schedule.graph,
            &mut schedule.queue,
            &schedule.chunk_map,
            &schedule.last_level,
            &schedule.last_high_priority,
            &waiting_snapshot,
            true,
        );
        assert!(schedule.queue.is_empty());
        assert_eq!(restored, 0);
        let parked_node = schedule
            .graph
            .nodes
            .get(parked)
            .expect("parked node exists");
        assert!(!parked_node.in_queue);
    }

    fn fetching_surface_holder(schedule: &mut GenerationSchedule) -> (ChunkPos, NodeKey) {
        let pos = ChunkPos::new(0, 0);
        let stage = StagedChunkEnum::Surface;
        let node_key = schedule.graph.nodes.insert(Node::new(pos, stage));
        schedule
            .graph
            .nodes
            .get_mut(node_key)
            .expect("fetch node exists")
            .in_flight = true;
        let mut holder = ChunkHolder::default();
        holder.target_stage = StagedChunkEnum::Full;
        holder.dependency_stage = StagedChunkEnum::None;
        holder.current_stage = StagedChunkEnum::Empty;
        holder.tasks[stage as usize] = node_key;
        schedule.chunk_map.insert(pos, holder);
        schedule.waiting_for_chunks.insert(node_key);
        (pos, node_key)
    }

    #[test]
    fn secondary_fetch_poll_completes_holder_to_public() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let (pos, fetching) = fetching_surface_holder(&mut schedule);
        schedule.cluster_fetches.insert(pos, Instant::now());
        schedule.poll_cluster_fetches();
        assert!(!schedule.cluster_fetches.is_empty());
        let holder = schedule.chunk_map.get(&pos).expect("holder exists");
        assert!(!holder.public);
        assert!(schedule.waiting_for_chunks.contains(&fetching));
        let missing = ChunkPos::new(7, 7);
        schedule.receive_chunk(
            missing,
            RecvChunk::Cluster((missing, crate::chunk::ChunkData::empty_sync(7, 7))),
        );
        let arrived = schedule.chunk_map.get(&missing).expect("holder exists");
        assert_eq!(arrived.current_stage, StagedChunkEnum::Full);
        assert_eq!(arrived.target_stage, StagedChunkEnum::None);
        assert!(arrived.public);
        assert!(schedule.queue.is_empty());
    }

    #[test]
    fn secondary_handoff_hold_then_arrive_then_unload_stays_consistent() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(3, 5);
        let mut changes = HashMapType::default();
        changes.insert(pos, (StagedChunkEnum::None, StagedChunkEnum::Full));
        let mut levels = ChunkLevel::default();
        levels.insert(pos, ChunkLoading::FULL_CHUNK_LEVEL);
        assert!(schedule.resort_work((Some((changes, levels)), None)));
        assert!(schedule.chunk_map.get(&pos).is_none());
        assert!(schedule.debug_check());
        schedule.receive_chunk(
            pos,
            RecvChunk::Cluster((pos, crate::chunk::ChunkData::empty_sync(3, 5))),
        );
        let held = schedule.chunk_map.get(&pos).expect("holder exists");
        assert_eq!(held.current_stage, StagedChunkEnum::Full);
        assert_eq!(held.target_stage, StagedChunkEnum::Full);
        assert!(schedule.debug_check());
        let mut changes = HashMapType::default();
        changes.insert(pos, (StagedChunkEnum::Full, StagedChunkEnum::None));
        let levels = ChunkLevel::default();
        assert!(schedule.resort_work((Some((changes, levels)), None)));
        assert!(schedule.debug_check());
        schedule.process_unload_queue();
        assert!(schedule.chunk_map.get(&pos).is_none());
        assert!(schedule.debug_check());
    }

    #[test]
    fn secondary_arrive_without_ticket_pins_consistent_holder() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(9, 9);
        schedule.receive_chunk(
            pos,
            RecvChunk::Cluster((pos, crate::chunk::ChunkData::empty_sync(9, 9))),
        );
        let holder = schedule.chunk_map.get(&pos).expect("holder exists");
        assert_eq!(holder.current_stage, StagedChunkEnum::Full);
        assert_eq!(holder.target_stage, StagedChunkEnum::None);
        assert!(schedule.graph.nodes.is_empty());
        assert!(schedule.queue.is_empty());
        assert!(schedule.debug_check());
    }

    #[test]
    fn secondary_fetch_without_snapshot_stays_wanted() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let (pos, fetching) = fetching_surface_holder(&mut schedule);
        schedule.cluster_fetches.insert(pos, Instant::now());
        schedule.poll_cluster_fetches();
        assert!(schedule.cluster_fetches.contains_key(&pos));
        let node = schedule
            .graph
            .nodes
            .get(fetching)
            .expect("fetch node exists");
        assert!(node.in_flight);
        assert!(!node.in_queue);
        assert!(schedule.queue.is_empty());
        schedule.poll_cluster_fetches();
        schedule.check_waiting_tasks(true);
        let restored = GenerationSchedule::restore_ready_tasks(
            &mut schedule.graph,
            &mut schedule.queue,
            &schedule.chunk_map,
            &schedule.last_level,
            &schedule.last_high_priority,
            &schedule.waiting_for_chunks,
            true,
        );
        assert_eq!(restored, 0);
        assert!(schedule.queue.is_empty());
    }

    #[test]
    fn secondary_timeout_discards_only_partial_holder() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(3, 5);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Surface;
        schedule.chunk_map.insert(pos, holder);
        schedule
            .cluster_fetches
            .insert(pos, Instant::now() - Duration::from_secs(31));
        schedule.secondary_discard_timed_out_partials();
        assert!(!schedule.cluster_fetches.contains_key(&pos));
        assert!(!schedule.chunk_map.contains_key(&pos));
    }

    #[test]
    fn secondary_timeout_keeps_full_holder() {
        let _secondary_mode = SecondaryTestMode::enable();
        let mut schedule = secondary_test_schedule();
        let pos = ChunkPos::new(3, 5);
        let snapshot = crate::chunk::ChunkData::empty_sync(pos.x, pos.y);
        let mut holder = ChunkHolder::default();
        holder.current_stage = StagedChunkEnum::Full;
        holder.chunk = Some(Chunk::Level(snapshot.clone()));
        holder.public = true;
        schedule.public_chunk_map.insert(pos, snapshot);
        schedule.chunk_map.insert(pos, holder);
        schedule
            .cluster_fetches
            .insert(pos, Instant::now() - Duration::from_secs(31));
        schedule.secondary_discard_timed_out_partials();
        assert!(!schedule.cluster_fetches.contains_key(&pos));
        assert!(schedule.chunk_map.contains_key(&pos));
        assert!(schedule.public_chunk_map.contains_key(&pos));
    }
}
