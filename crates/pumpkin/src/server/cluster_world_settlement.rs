use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::completion::{TickFinish, encode_dependency, encode_settlement};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkKey, StreamKind, TickBatch};
use pumpkin_cluster::settlement::{
    DependencyNotice, ElectionRevision, PrimaryLoad, PrimaryLoadSnapshot, SettlementCoordinator,
    SettlementId, SettlementPublication,
};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use tokio::sync::mpsc;

use super::Server;
use super::cluster_world_actions::{ReadyPromotion, WorldTickBuckets};

pub(super) struct SettlementOwner {
    local: ServerId,
    outbound: SettlementOutbox,
    coordinator: SettlementCoordinator,
    fetches: HashMap<SettlementId, Vec<pumpkin_cluster::settlement::ChunkFetch>>,
    acquired_pins: HashMap<ChunkKey, usize>,
    pin_counts: HashMap<ChunkKey, usize>,
    election_revision: u64,
    grounded: HashMap<SettlementId, BTreeSet<ChunkKey>>,
    awaiting_publication: HashMap<SettlementId, (Vec<ChunkKey>, BTreeSet<ServerId>)>,
    expired_acquired: HashSet<ChunkKey>,
}

struct PendingSettlementParcel {
    id: Option<SettlementId>,
    parcel: OutboundParcel,
}

struct SettlementOutbox {
    sender: mpsc::Sender<OutboundParcel>,
    drop_request: mpsc::Sender<ServerId>,
    pending: BTreeMap<ServerId, VecDeque<PendingSettlementParcel>>,
    quarantined: HashSet<ServerId>,
    unsent_drops: HashSet<ServerId>,
}

impl SettlementOutbox {
    fn new(sender: mpsc::Sender<OutboundParcel>, drop_request: mpsc::Sender<ServerId>) -> Self {
        Self {
            sender,
            drop_request,
            pending: BTreeMap::new(),
            quarantined: HashSet::new(),
            unsent_drops: HashSet::new(),
        }
    }

    fn queue(&mut self, peer: ServerId, bytes: Vec<u8>, id: Option<SettlementId>) {
        if self.quarantined.contains(&peer) {
            return;
        }
        let pending = self.pending.entry(peer).or_default();
        if pending.len() >= 1_200 {
            self.pending.remove(&peer);
            self.quarantined.insert(peer);
            self.unsent_drops.insert(peer);
            tracing::error!(
                peer = peer.0,
                "cluster settlement delivery capacity exhausted"
            );
            return;
        }
        pending.push_back(PendingSettlementParcel {
            id,
            parcel: OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes,
            },
        });
    }

    fn flush(&mut self) -> Vec<(SettlementId, ServerId)> {
        self.unsent_drops
            .retain(|peer| self.drop_request.try_send(*peer).is_err());
        let mut sent = Vec::new();
        for (peer, pending) in &mut self.pending {
            while let Some(parcel) = pending.front() {
                if self.sender.try_send(parcel.parcel.clone()).is_err() {
                    break;
                }
                if let Some(id) = parcel.id {
                    sent.push((id, *peer));
                }
                pending.pop_front();
            }
        }
        self.pending.retain(|_, pending| !pending.is_empty());
        sent
    }

    fn peer_dropped(&mut self, peer: ServerId) {
        self.pending.remove(&peer);
        self.quarantined.remove(&peer);
        self.unsent_drops.remove(&peer);
    }
}

impl SettlementOwner {
    pub(super) fn new(
        local: ServerId,
        outbound: mpsc::Sender<OutboundParcel>,
        drop_request: mpsc::Sender<ServerId>,
    ) -> Self {
        Self {
            local,
            outbound: SettlementOutbox::new(outbound, drop_request),
            coordinator: SettlementCoordinator::default(),
            fetches: HashMap::new(),
            acquired_pins: HashMap::new(),
            pin_counts: HashMap::new(),
            election_revision: 0,
            grounded: HashMap::new(),
            awaiting_publication: HashMap::new(),
            expired_acquired: HashSet::new(),
        }
    }

    pub(super) fn register_local(
        &mut self,
        server: &Server,
        buckets: &mut WorldTickBuckets,
        batch: &TickBatch,
    ) {
        let directory = super::cluster_datagram::holder_directory();
        for (id, chunks) in action_dependencies(batch) {
            let Ok(notice) = DependencyNotice::from_directory(id, self.local, chunks, &directory)
            else {
                continue;
            };
            buckets.block_settlement(id.tick, id.action);
            self.note_dependency(server, buckets, self.local, notice);
        }
    }

    pub(super) fn note_dependency(
        &mut self,
        server: &Server,
        buckets: &mut WorldTickBuckets,
        source: ServerId,
        notice: DependencyNotice,
    ) {
        let directory = super::cluster_datagram::holder_directory();
        let Ok(routing) = self
            .coordinator
            .note_dependency(source, notice.clone(), &directory)
        else {
            return;
        };
        buckets.block_settlement(notice.id.tick, notice.id.action);
        if server.advanced_config.cluster.role == pumpkin_config::ClusterRole::Primary {
            let held = notice
                .chunks
                .iter()
                .copied()
                .filter(|chunk| directory.sorted_holders(chunk).contains(&self.local.0))
                .collect::<Vec<_>>();
            if self
                .coordinator
                .lock_primary_chunks(notice.id, self.local, held.iter().copied())
                .is_ok()
            {
                for chunk in held {
                    self.pin(server, chunk);
                }
            }
        }
        if notice.waiter == self.local
            && let Ok(bytes) = encode_dependency(&notice)
        {
            for peer in routing.recipients.iter().copied() {
                self.outbound.queue(peer, bytes.clone(), None);
            }
        }
        if routing.complete_holder.is_some() {
            if self.coordinator.assign_complete_holder(notice.id).is_err() {
                return;
            }
            return;
        }
        let primaries = primary_peers(server, self.local);
        let loads = primary_loads(&directory, &primaries);
        self.election_revision = self.election_revision.wrapping_add(1);
        let election = PrimaryLoadSnapshot::new(
            ElectionRevision {
                term: 0,
                revision: self.election_revision,
            },
            loads
                .into_iter()
                .map(|(primary, load)| PrimaryLoad { primary, load }),
        );
        let Ok(plan) = self
            .coordinator
            .plan_primary_resolution(notice.id, election)
        else {
            return;
        };
        if plan.owner != self.local {
            return;
        }
        for fetch in &plan.fetches {
            if let Some(world) = world_for(server, fetch.chunk) {
                let position =
                    pumpkin_util::math::vector2::Vector2::new(fetch.chunk.x(), fetch.chunk.z());
                self.pin(server, fetch.chunk);
                *self.acquired_pins.entry(fetch.chunk).or_default() += 1;
                let _ = world
                    .level
                    .prefetch_cluster_chunk_from(position, fetch.source.0);
            }
        }
        self.fetches.insert(notice.id, plan.fetches);
    }

    pub(super) fn observe_settlement(
        &mut self,
        server: &Server,
        source: ServerId,
        publication: SettlementPublication,
        buckets: &mut WorldTickBuckets,
    ) {
        if let Ok(released) = self.coordinator.observe_settlement(source, &publication) {
            self.release_primary_pins(server, released);
            buckets.release_settlement(publication.id.tick, publication.id.action);
        }
    }

    pub(super) fn peer_dropped(
        &mut self,
        server: &Server,
        buckets: &mut WorldTickBuckets,
        peer: ServerId,
    ) {
        self.outbound.peer_dropped(peer);
        for (_, released) in self.awaiting_publication.iter_mut() {
            released.1.remove(&peer);
        }
        self.release_delivered_publications(server);
        let ids = self.coordinator.peer_dropped(peer);
        let directory = super::cluster_datagram::holder_directory();
        let mut primaries = primary_peers(server, self.local);
        primaries.remove(&peer);
        let loads = primary_loads(&directory, &primaries);
        for id in ids {
            self.election_revision = self.election_revision.wrapping_add(1);
            let election = PrimaryLoadSnapshot::new(
                ElectionRevision {
                    term: 0,
                    revision: self.election_revision,
                },
                loads.iter().map(|(primary, load)| PrimaryLoad {
                    primary: *primary,
                    load: *load,
                }),
            );
            let Ok(plan) = self.coordinator.plan_primary_resolution(id, election) else {
                continue;
            };
            if plan.owner != self.local {
                continue;
            }
            for fetch in &plan.fetches {
                if let Some(world) = world_for(server, fetch.chunk) {
                    let position =
                        pumpkin_util::math::vector2::Vector2::new(fetch.chunk.x(), fetch.chunk.z());
                    self.pin(server, fetch.chunk);
                    *self.acquired_pins.entry(fetch.chunk).or_default() += 1;
                    let _ = world
                        .level
                        .prefetch_cluster_chunk_from(position, fetch.source.0);
                }
            }
            self.fetches.insert(id, plan.fetches);
            buckets.block_settlement(id.tick, id.action);
        }
    }

    pub(super) fn progress(&mut self, server: &Server, buckets: &mut WorldTickBuckets) {
        for (id, peer) in self.outbound.flush() {
            if let Some((_, waiting)) = self.awaiting_publication.get_mut(&id) {
                waiting.remove(&peer);
            }
        }
        self.release_delivered_publications(server);
        let pending = self.fetches.keys().copied().collect::<Vec<_>>();
        for id in pending {
            let Some(fetches) = self.fetches.get(&id) else {
                continue;
            };
            let mut ready = true;
            for fetch in fetches {
                let loaded = world_for(server, fetch.chunk).is_some_and(|world| {
                    world
                        .level
                        .is_chunk_loaded(&pumpkin_util::math::vector2::Vector2::new(
                            fetch.chunk.x(),
                            fetch.chunk.z(),
                        ))
                });
                if loaded {
                    let _ = self.coordinator.note_acquired(id, self.local, fetch.chunk);
                } else {
                    ready = false;
                }
            }
            if ready {
                self.fetches.remove(&id);
            }
        }
        for id in self.coordinator.pending_ids() {
            if self.coordinator.owner(id) == Some(self.local)
                && !self.fetches.contains_key(&id)
                && buckets.settlement_complete_except_barrier(id.tick, id.action)
            {
                buckets.release_settlement(id.tick, id.action);
            }
        }
        for chunk in self.coordinator.expire_retention(now_millis()) {
            if self
                .awaiting_publication
                .values()
                .any(|(pins, _)| pins.contains(&chunk))
            {
                self.expired_acquired.insert(chunk);
                continue;
            }
            for _ in 0..self.acquired_pins.remove(&chunk).unwrap_or_default() {
                self.unpin(server, chunk);
            }
        }
    }

    pub(super) fn promoted(&mut self, server: &Server, promotion: &ReadyPromotion) {
        for action in &promotion.actions {
            let id = SettlementId {
                tick: promotion.tick,
                action: *action,
            };
            self.grounded
                .entry(id)
                .or_default()
                .extend(promotion.chunks.iter().copied());
            if self.coordinator.owner(id) != Some(self.local) {
                continue;
            }
            self.finish_grounded(server, id);
        }
    }

    fn finish_grounded(&mut self, server: &Server, id: SettlementId) {
        let Some(chunks) = self.grounded.get(&id).cloned() else {
            return;
        };
        for chunk in chunks {
            let _ = self.coordinator.note_grounded(id, self.local, chunk);
        }
        let Ok(outcome) = self.coordinator.settle(id, self.local, now_millis()) else {
            return;
        };
        self.grounded.remove(&id);
        let Ok(bytes) = encode_settlement(&outcome.publication) else {
            self.awaiting_publication
                .insert(id, (outcome.released_primary_chunks, outcome.recipients));
            return;
        };
        let recipients = outcome.recipients;
        for peer in &recipients {
            self.outbound.queue(*peer, bytes.clone(), Some(id));
        }
        self.awaiting_publication
            .insert(id, (outcome.released_primary_chunks, recipients));
        for (sent_id, peer) in self.outbound.flush() {
            if let Some((_, waiting)) = self.awaiting_publication.get_mut(&sent_id) {
                waiting.remove(&peer);
            }
        }
        self.release_delivered_publications(server);
    }

    fn release_delivered_publications(&mut self, server: &Server) {
        let completed = self
            .awaiting_publication
            .iter()
            .filter_map(|(id, (_, peers))| peers.is_empty().then_some(*id))
            .collect::<Vec<_>>();
        for id in completed {
            if let Some((pins, _)) = self.awaiting_publication.remove(&id) {
                self.release_primary_pins(server, pins);
            }
        }
        let ready = self
            .expired_acquired
            .iter()
            .copied()
            .filter(|chunk| {
                !self
                    .awaiting_publication
                    .values()
                    .any(|(pins, _)| pins.contains(chunk))
            })
            .collect::<Vec<_>>();
        for chunk in ready {
            self.expired_acquired.remove(&chunk);
            for _ in 0..self.acquired_pins.remove(&chunk).unwrap_or_default() {
                self.unpin(server, chunk);
            }
        }
    }

    fn release_primary_pins(&mut self, server: &Server, chunks: Vec<ChunkKey>) {
        for chunk in chunks {
            self.unpin(server, chunk);
        }
    }

    fn pin(&mut self, server: &Server, chunk: ChunkKey) {
        let count = self.pin_counts.entry(chunk).or_default();
        if *count == 0
            && let Some(world) = world_for(server, chunk)
        {
            world
                .level
                .pin_cluster_chunk(pumpkin_util::math::vector2::Vector2::new(
                    chunk.x(),
                    chunk.z(),
                ));
        }
        *count = count.saturating_add(1);
    }

    fn unpin(&mut self, server: &Server, chunk: ChunkKey) {
        let Some(count) = self.pin_counts.get_mut(&chunk) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count != 0 {
            return;
        }
        self.pin_counts.remove(&chunk);
        if let Some(world) = world_for(server, chunk) {
            world
                .level
                .unpin_cluster_chunk(&pumpkin_util::math::vector2::Vector2::new(
                    chunk.x(),
                    chunk.z(),
                ));
        }
    }
}

fn action_dependencies(batch: &TickBatch) -> BTreeMap<SettlementId, BTreeSet<ChunkKey>> {
    let mut result = BTreeMap::new();
    for entry in TickFinish::from_batch(batch).chunks {
        for action in entry.actions {
            result
                .entry(SettlementId {
                    tick: batch.tick,
                    action,
                })
                .or_insert_with(BTreeSet::new)
                .insert(entry.chunk);
        }
    }
    result
}

fn primary_peers(server: &Server, local: ServerId) -> BTreeSet<ServerId> {
    let mut peers: BTreeSet<_> = server
        .cluster_primary_peers
        .load()
        .iter()
        .copied()
        .collect();
    if server.advanced_config.cluster.role == pumpkin_config::ClusterRole::Primary {
        peers.insert(local);
    }
    peers
}

fn primary_loads(
    directory: &pumpkin_cluster::chunks::Directory,
    primaries: &BTreeSet<ServerId>,
) -> HashMap<ServerId, u64> {
    let mut loads = primaries
        .iter()
        .copied()
        .map(|peer| (peer, 0_u64))
        .collect::<HashMap<_, _>>();
    for holders in directory.holders.values() {
        for holder in holders {
            if let Some(load) = loads.get_mut(&ServerId(*holder)) {
                *load = load.saturating_add(1);
            }
        }
    }
    loads
}

fn world_for(server: &Server, chunk: ChunkKey) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == chunk.world)
        .cloned()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::completion::ActionKey;

    #[test]
    fn settlement_publication_survives_outbound_backpressure() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (drop_request, mut drops) = mpsc::channel(1);
        sender
            .try_send(OutboundParcel {
                peer: ServerId(9),
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: vec![0],
            })
            .unwrap();
        let mut outbox = SettlementOutbox::new(sender, drop_request);
        let id = SettlementId {
            tick: pumpkin_cluster::time::TickStamp(17),
            action: ActionKey {
                actor: pumpkin_cluster::actor::ActorId::Peer(ServerId(1)),
                sequence: 4,
                family: 6,
            },
        };
        outbox.queue(ServerId(2), vec![1, 2, 3], Some(id));
        assert!(outbox.flush().is_empty());
        assert!(drops.try_recv().is_err());
        assert_eq!(receiver.try_recv().unwrap().peer, ServerId(9));
        assert_eq!(outbox.flush(), vec![(id, ServerId(2))]);
        assert_eq!(receiver.try_recv().unwrap().bytes, vec![1, 2, 3]);
    }
}
