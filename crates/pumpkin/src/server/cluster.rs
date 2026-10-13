use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use pumpkin_cluster::accept::{Acceptor, decode_accept};
use pumpkin_cluster::admin_sync::{AdminControlMessage, decode_control_message};
use pumpkin_cluster::boundary::{PlayerBoundaryTracker, encode as encode_boundary};
use pumpkin_cluster::chunks::{
    AcquisitionState, ChunkAcquisition, ChunkAdvert, ChunkAnnounce, ChunkDrop, Directory,
    PingTracker, PrimaryClaims,
};
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::completion::{CompletionMessage, LocalTickBatch, decode_message};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::lifecycle::{
    EntityHandoffRequest, Lifecycle, LifecycleEffect, LifecycleInput, LifecycleState,
};
use pumpkin_cluster::membership::{
    ClusterNodeRole, JoinHandshake, decode_handshake, encode_handshake,
};
use pumpkin_cluster::mesh::{MeshConfig, PinnedPeer};
use pumpkin_cluster::ntp::{DEFAULT_NTP_SERVER, NtpConfig, NtpSync};
use pumpkin_cluster::protocol::{ChunkKey, StreamKind, WorldId};
use pumpkin_cluster::streams::{
    DemuxControl, InboundParcel, OutboundParcel, StreamHeader, StreamRegistry, demux_channels,
    run_demux,
};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::transport::{
    Transport, TransportActivationState, channel_pair, load_or_generate_keypair,
};
use pumpkin_cluster::xfer::{
    ChunkPayload, decode_announce, decode_payload, decode_request, encode_announce, encode_payload,
};
use pumpkin_config::{ClusterConfig, ClusterRole};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::level::{
    ClusterFetchRequest, ClusterLogoutGrace, OwnerChunkDemand, set_cluster_fetch_sender,
    set_cluster_has_peers, set_cluster_logout_sender, set_cluster_secondary,
    set_cluster_unwant_sender,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, trace, warn};

use super::Server;
use super::cluster_entity_apply::spawn_entity_apply;
use super::cluster_regions;
use super::cluster_visual::spawn_visual_apply;
use super::cluster_world_apply::spawn_world_apply;
use super::{cluster_chat_in, cluster_datagram};

static FUSED_BRIDGE: std::sync::OnceLock<mpsc::Sender<Vec<u8>>> = std::sync::OnceLock::new();
static FUSED_FORWARDED: AtomicU64 = AtomicU64::new(0);
static FETCHED_CHUNKS: AtomicU64 = AtomicU64::new(0);
static SERVED_CHUNKS: AtomicU64 = AtomicU64::new(0);
static NTP_UNDISCIPLINED_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static NTP_DISCIPLINE_STARTED: AtomicBool = AtomicBool::new(false);
static CLUSTER_ADMITTED: AtomicBool = AtomicBool::new(false);
static CLUSTER_LIFECYCLE_STATE: AtomicU64 = AtomicU64::new(0);
static CLUSTER_MESH_READY: AtomicBool = AtomicBool::new(false);
static CLUSTER_IS_PRIMARY: AtomicBool = AtomicBool::new(false);
static CLUSTER_PENDING_SYNC: AtomicU64 = AtomicU64::new(0);
static CLUSTER_INFLIGHT_FETCH: AtomicU64 = AtomicU64::new(0);
static CLUSTER_WANTED_CHUNKS: AtomicU64 = AtomicU64::new(0);
static CLUSTER_HANDOFF_SENT: AtomicU64 = AtomicU64::new(0);
static LIFECYCLE_IN: std::sync::OnceLock<mpsc::Sender<LifecycleInput>> = std::sync::OnceLock::new();

#[must_use]
pub fn disciplined_offset_millis() -> i64 {
    match pumpkin_cluster::ntp::shared_offset_millis() {
        Some(offset) => offset,
        _ => {
            NTP_UNDISCIPLINED_FALLBACKS.fetch_add(1, Ordering::Relaxed);
            0
        }
    }
}

#[must_use]
pub fn disciplined_tick_stamp(millis: i64) -> TickStamp {
    TickStamp::from_disciplined_millis(millis, disciplined_offset_millis())
}

#[must_use]
pub fn ntp_disciplined() -> bool {
    pumpkin_cluster::ntp::shared_offset_millis().is_some()
}

#[must_use]
pub fn ntp_undisciplined_fallbacks() -> u64 {
    NTP_UNDISCIPLINED_FALLBACKS.load(Ordering::Relaxed)
}

fn start_ntp_discipline(server: &Server, config: &ClusterConfig) {
    if NTP_DISCIPLINE_STARTED.swap(true, Ordering::Relaxed) {
        return;
    }
    let mut servers = config.ntp_servers.clone();
    if servers.is_empty() {
        servers.push(DEFAULT_NTP_SERVER.to_owned());
    }
    let max_offset_millis = config.max_offset_millis;
    let (sync, _) = NtpSync::new(NtpConfig::new(servers.clone(), max_offset_millis));
    server.spawn_task(async move {
        sync.run().await;
    });
    info!(
        servers = servers.len(),
        max_offset_millis, "cluster ntp discipline started"
    );
}

#[must_use]
pub fn fused_forwarded() -> u64 {
    FUSED_FORWARDED.load(Ordering::Relaxed)
}

#[must_use]
pub fn admin_applied() -> u64 {
    super::cluster_admin_apply::admin_mutations_applied()
}

#[must_use]
pub fn fetched_chunks() -> u64 {
    FETCHED_CHUNKS.load(Ordering::Relaxed)
}

#[must_use]
pub fn served_chunks() -> u64 {
    SERVED_CHUNKS.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_player_join_allowed() -> bool {
    if !CLUSTER_MESH_READY.load(Ordering::Relaxed) {
        return true;
    }
    if CLUSTER_IS_PRIMARY.load(Ordering::Relaxed) {
        return false;
    }
    CLUSTER_ADMITTED.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_lifecycle_state_text() -> &'static str {
    match CLUSTER_LIFECYCLE_STATE.load(Ordering::Relaxed) {
        1 => "joining",
        2 => "member",
        3 => "leaving",
        _ => "solo",
    }
}

#[must_use]
pub fn cluster_pending_sync() -> u64 {
    CLUSTER_PENDING_SYNC.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_inflight_fetch() -> u64 {
    CLUSTER_INFLIGHT_FETCH.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_wanted_chunks() -> u64 {
    CLUSTER_WANTED_CHUNKS.load(Ordering::Relaxed)
}

fn chunk_need_route_capacity(protocols: &[(bool,u32)]) -> usize {
    let namespace_capacity = usize::from(u16::MAX) + 1;
    let clients = protocols.iter().filter(|(enabled,_)| *enabled).fold(0_usize,|total,(_,limit)| {
        total.saturating_add(if *limit == 0 {namespace_capacity} else {(*limit as usize).min(namespace_capacity)})
    }).min(namespace_capacity);
    clients.saturating_mul(4).saturating_add(16384+512)
}

pub(crate) struct ChunkNeedLease {
    release: Option<tokio::sync::mpsc::OwnedPermit<ChunkNeed>>,
    lease: u64,
    chunks: Vec<ChunkKey>,
}

pub(crate) struct HistoricalChunkNeedLease {
    release: Option<mpsc::OwnedPermit<ChunkNeed>>,
    requests: Vec<pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>,
}

impl Drop for HistoricalChunkNeedLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release.send(ChunkNeed::ReleaseHistoricalSet {
                requests: std::mem::take(&mut self.requests),
            });
        }
    }
}

pub(crate) fn try_require_original_actor_chunk(
    server: &Server,
    chunk: ChunkKey,
    full_source_slot: u64,
    lease: u64,
    providers: std::collections::BTreeSet<pumpkin_cluster::realm_completion::PeerGeneration>,
) -> Option<HistoricalChunkNeedLease> {
    let provider = *providers.first()?;
    let request = pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest {chunk, full_source_slot, lease, provider};
    if !request.valid() || providers.iter().any(|provider| provider.generation == 0) { return None; }
    let route = server.cluster_chunk_need_route.load_full()?;
    let acquire = route.as_ref().clone().try_reserve_owned().ok()?;
    let release = route.as_ref().clone().try_reserve_owned().ok()?;
    acquire.send(ChunkNeed::AcquireOriginalActorChunk {request, providers});
    Some(HistoricalChunkNeedLease {release:Some(release), requests:vec![request]})
}

pub(crate) fn try_require_historical_chunks(
    server: &Server,
    requests: Vec<pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>,
) -> Option<HistoricalChunkNeedLease> {
    if requests.is_empty()
        || requests.len() > 8192
        || requests.iter().any(|request| !request.valid())
    {
        return None;
    }
    let identities = requests
        .iter()
        .map(|request| (request.chunk, request.lease))
        .collect::<std::collections::BTreeSet<_>>();
    if identities.len() != requests.len() {
        return None;
    }
    let route = server.cluster_chunk_need_route.load_full()?;
    let acquire = route.as_ref().clone().try_reserve_owned().ok()?;
    let release = route.as_ref().clone().try_reserve_owned().ok()?;
    acquire.send(ChunkNeed::AcquireHistoricalSet {
        requests: requests.clone(),
    });
    Some(HistoricalChunkNeedLease {
        release: Some(release),
        requests,
    })
}

pub(crate) enum ChunkNeed {
    AcquireOriginalActorChunk {
        request: pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest,
        providers: std::collections::BTreeSet<pumpkin_cluster::realm_completion::PeerGeneration>,
    },
    AcquireHistoricalSet {
        requests: Vec<pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>,
    },
    ReleaseHistoricalSet {
        requests: Vec<pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>,
    },
    RetainLease {
        lease: ChunkNeedLease,
        expires_millis: u64,
    },
    AcquireSet {
        chunks: Vec<ChunkKey>,
        lease: u64,
    },
    ReleaseSet {
        chunks: Vec<ChunkKey>,
        lease: u64,
    },
    Acquire {
        chunk: ChunkKey,
        lease: u64,
        ready: Option<mpsc::Sender<pumpkin_world::level::OwnerChunkReady>>,
    },
    Release {
        chunk: ChunkKey,
        lease: u64,
    },
}

pub(crate) fn try_require_lobby_chunks(
    server: &Server,
    world: &Arc<crate::world::World>,
    required: &[Vector2<i32>],
    session: u64,
) -> Option<ChunkNeedLease> {
    if session >= (1_u64 << 62) || required.len() > 8192 {
        return None;
    }
    let mut positions = required.to_vec();
    positions.sort_unstable_by_key(|position| (position.x, position.y));
    positions.dedup();
    let world_id = super::cluster_world_delta::world_id(world);
    let chunks = positions
        .into_iter()
        .map(|position| ChunkKey::new(world_id, position.x, position.y))
        .collect::<Vec<_>>();
    let route = server.cluster_chunk_need_route.load_full()?;
    let acquire = route.as_ref().clone().try_reserve_owned().ok()?;
    let release = route.as_ref().clone().try_reserve_owned().ok()?;
    let lease = session | (1_u64 << 63);
    acquire.send(ChunkNeed::AcquireSet {
        chunks: chunks.clone(),
        lease,
    });
    Some(ChunkNeedLease {
        release: Some(release),
        lease,
        chunks,
    })
}

pub(crate) fn try_require_chunk_set(
    server: &Server,
    required: &[ChunkKey],
    session: u64,
) -> Option<ChunkNeedLease> {
    if session >= (1_u64 << 62) || required.len() > 8192 {
        return None;
    }
    let mut chunks = required.to_vec();
    chunks.sort_unstable();
    chunks.dedup();
    let route = server.cluster_chunk_need_route.load_full()?;
    let acquire = route.as_ref().clone().try_reserve_owned().ok()?;
    let release = route.as_ref().clone().try_reserve_owned().ok()?;
    let lease = session | (1_u64 << 63);
    acquire.send(ChunkNeed::AcquireSet {
        chunks: chunks.clone(),
        lease,
    });
    Some(ChunkNeedLease {
        release: Some(release),
        lease,
        chunks,
    })
}

pub(crate) fn try_retain_chunk_need(
    server: &Server,
    lease: ChunkNeedLease,
    expires_millis: u64,
) -> Result<(), ChunkNeedLease> {
    let Some(route) = server.cluster_chunk_need_route.load_full() else {
        return Err(lease);
    };
    let Ok(permit) = route.try_reserve() else {
        return Err(lease);
    };
    permit.send(ChunkNeed::RetainLease {
        lease,
        expires_millis,
    });
    Ok(())
}

impl Drop for ChunkNeedLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release.send(ChunkNeed::ReleaseSet {
                chunks: std::mem::take(&mut self.chunks),
                lease: self.lease,
            });
        }
    }
}

impl ChunkNeedLease {
    #[cfg(test)]
    pub(crate) fn test_owned(
        release: mpsc::OwnedPermit<ChunkNeed>,
        lease: u64,
        chunks: Vec<ChunkKey>,
    ) -> Self {
        Self {
            release: Some(release),
            lease,
            chunks,
        }
    }

    pub(crate) fn chunks(&self) -> &[ChunkKey] {
        &self.chunks
    }
}

struct PendingChunkNeedSet {
    chunks: Vec<ChunkKey>,
    lease: u64,
    next: usize,
    acquire: bool,
}

pub fn log_cluster_shutdown_progress(server: &Server, context: &str) {
    let players = server.get_player_count();
    info!(
        context,
        players,
        owned_entities = 0,
        pending_sync_buckets = CLUSTER_PENDING_SYNC.load(Ordering::Relaxed),
        inflight_chunk_fetches = CLUSTER_INFLIGHT_FETCH.load(Ordering::Relaxed),
        handoffs_sent = CLUSTER_HANDOFF_SENT.load(Ordering::Relaxed),
        lifecycle = cluster_lifecycle_state_text(),
        admitted = CLUSTER_ADMITTED.load(Ordering::Relaxed),
        "cluster shutdown progress"
    );
}

pub fn begin_cluster_leave_drain(server: &Server) {
    let Some(input) = LIFECYCLE_IN.get() else {
        return;
    };
    if CLUSTER_IS_PRIMARY.load(Ordering::Relaxed) {
        return;
    }
    let player_count = server.get_player_count();
    let request = LifecycleInput::RequestLeave {
        player_count,
        owned: Vec::new(),
        holders: Vec::new(),
    };
    if input.try_send(request).is_err() {
        warn!(
            player_count,
            "cluster leave request dropped: lifecycle inbox full"
        );
        return;
    }
    info!(
        player_count,
        lifecycle = cluster_lifecycle_state_text(),
        "cluster leave requested"
    );
}

fn lifecycle_state_code(state: LifecycleState) -> u64 {
    match state {
        LifecycleState::Solo => 0,
        LifecycleState::Joining => 1,
        LifecycleState::Member => 2,
        LifecycleState::Leaving => 3,
    }
}

fn queue_handshake(
    pending: &mut VecDeque<OutboundParcel>,
    to: &[u16],
    message: JoinHandshake,
    local: u16,
) {
    trace!(server_id = local, destinations = ?to, message = ?message, "cluster handshake queued");
    let bytes = match encode_handshake(&message) {
        Ok(bytes) => bytes,
        Err(_) => {
            warn!(server_id = local, "cluster handshake encode failed");
            return;
        }
    };
    for peer in to {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        pending.push_back(parcel);
        assert!(
            pending.len() <= 1_024,
            "cluster handshake journal exhausted"
        );
    }
}

fn send_handoff(
    outbound: &mpsc::Sender<OutboundParcel>,
    to: &[u16],
    request: &EntityHandoffRequest,
    local: u16,
) {
    let Ok(bytes) = postcard::to_allocvec(request) else {
        warn!(server_id = local, "cluster handoff encode failed");
        return;
    };
    if to.is_empty() {
        warn!(
            server_id = local,
            leaver = request.leaver,
            entities = request.handoffs.len(),
            "cluster handoff has no targets, broadcasting to mesh"
        );
    }
    for peer in to {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        if outbound.try_send(parcel).is_err() {
            warn!(
                server_id = local,
                to = peer,
                "cluster handoff send dropped: mesh queue full"
            );
        }
    }
    CLUSTER_HANDOFF_SENT.fetch_add(1, Ordering::Relaxed);
}

fn publish_admitted_peer(server: &Arc<Server>, peer: u16) -> bool {
    let admin_seeded = super::cluster_admin_apply::publish_admin_snapshot_to_peer(server, peer);
    super::cluster_presence::publish_roster_to_peer(server, peer);
    admin_seeded
}

async fn lifecycle_effect_task(
    server: Arc<Server>,
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
    peers: Vec<ServerId>,
    mut lifecycle: Lifecycle,
    mut inputs: mpsc::Receiver<LifecycleInput>,
    peer_reset: mpsc::Sender<u16>,
    primary_peer: mpsc::Sender<u16>,
    replacement_ready: mpsc::Sender<ServerId>,
    peer_admitted: tokio::sync::watch::Sender<Arc<HashSet<ServerId>>>,
    mut peer_state: tokio::sync::watch::Receiver<
        Arc<pumpkin_cluster::transport::PeerStateSnapshot>,
    >,
    peer_drop: tokio::sync::watch::Sender<Arc<HashMap<ServerId, u64>>>,
    mut drop_complete: tokio::sync::watch::Receiver<Arc<HashMap<ServerId, u64>>>,
) {
    let fallback: Vec<u16> = peers.iter().map(|peer| peer.0).collect();
    let mut activation = TransportActivationState::default();
    let mut observed_connected = HashSet::new();
    let mut observed_drops = HashMap::new();
    let mut observed_admissions = HashMap::new();
    let mut observed_capacity = 0;
    let mut peer_drop_epochs = HashMap::new();
    let mut observed_completed = HashMap::new();
    let mut pending_peer_generations = None;
    let mut handoff_events = VecDeque::new();
    let mut pending_handshakes = VecDeque::new();
    let mut pending_admin_seeds = HashSet::new();
    let mut namespace_candidates = HashMap::new();
    let mut admin_seed_retry = tokio::time::interval(Duration::from_millis(10));
    loop {
        let effects = tokio::select! {
            permit = outbound.reserve(), if !pending_handshakes.is_empty() => {
                let Ok(permit) = permit else { break; };
                if let Some(parcel) = pending_handshakes.pop_front() {
                    permit.send(parcel);
                }
                Vec::new()
            }
            _ = admin_seed_retry.tick(), if !pending_admin_seeds.is_empty() => {
                pending_admin_seeds.retain(|peer: &ServerId| {
                    server.cluster_connected_peers.load().contains(peer)
                        && !super::cluster_admin_apply::publish_admin_snapshot_to_peer(&server, peer.0)
                });
                Vec::new()
            }
            permit = server.cluster_handoff_tx.get().expect("cluster handoff owner missing").reserve(), if !handoff_events.is_empty() => {
                let Ok(permit) = permit else {
                    break;
                };
                if let Some(command) = handoff_events.pop_front() {
                    permit.send(command);
                }
                Vec::new()
            }
            input = inputs.recv() => {
                let Some(mut input) = input else {
                    break;
                };
                if let LifecycleInput::RemoteHello { hello, pin_match } = &mut input {
                    let snapshot = server.cluster_namespaces.snapshot();
                    let admission = super::cluster_namespaces::NamespaceAdmission {
                        peer: ServerId(hello.candidate),
                        incarnation: hello.incarnation,
                        certificate_fingerprint: hello.cert_fingerprint,
                        role: hello.role,
                        protocol_version: hello.protocol_version,
                        supersedes: snapshot.admitted.get(&ServerId(hello.candidate)).map(|previous| {
                            pumpkin_cluster::realm_completion::PeerGeneration {
                                server: previous.peer,
                                generation: previous.incarnation,
                            }
                        }),
                    };
                    if hello.incarnation == 0 || server.cluster_namespaces.retained_collision(&admission).is_some() {
                        *pin_match = false;
                    } else if *pin_match {
                        namespace_candidates.insert(hello.candidate, admission);
                    }
                }
                lifecycle.apply(input)
            }
            changed = peer_state.changed() => {
                if changed.is_err() {
                    continue;
                }
                let state = Arc::clone(&peer_state.borrow_and_update());
                server.cluster_connected_peers.store(Arc::new(state.connected.clone()));
                pending_peer_generations = Some(state.drop_epoch.clone());
                server.cluster_primary_peers.rcu(|current| {
                    current
                        .intersection(&state.connected)
                        .copied()
                        .collect::<HashSet<_>>()
                });
                cluster_datagram::publish_peer_datagram_limits(state.datagram_limits.clone());
                server.cluster_datagram_limits.store(Arc::new(state.datagram_limits.clone()));
                let mut effects = Vec::new();
                let mut restarted_peers = HashSet::new();
                for (&peer, &epoch) in &state.drop_epoch {
                    let previous = observed_drops.get(&peer).copied().unwrap_or(0);
                    for _ in previous..epoch {
                        restarted_peers.insert(peer);
                        handoff_events.push_back(
                            super::cluster_handoff::HandoffCommand::PeerDropped(peer),
                        );
                        *peer_drop_epochs.entry(peer).or_default() += 1;
                        peer_drop.send_replace(Arc::new(peer_drop_epochs.clone()));
                        effects.extend(lifecycle.apply(LifecycleInput::NotePeerDisconnected { peer: peer.0 }));
                    }
                }
                for peer in &state.connected {
                    if !observed_connected.contains(peer) || restarted_peers.contains(peer) {
                        effects.extend(lifecycle.apply(LifecycleInput::NotePeerConnected { peer: peer.0 }));
                    }
                }
                for (&peer, &epoch) in &state.admission_epoch {
                    let previous = observed_admissions.get(&peer).copied().unwrap_or(0);
                    for _ in previous..epoch {
                        if activation.admission_applied(peer) {
                            if !publish_admitted_peer(&server, peer.0) {
                                pending_admin_seeds.insert(peer);
                            }
                        }
                    }
                }
                if state.replacement_capacity_epoch != observed_capacity {
                    observed_capacity = state.replacement_capacity_epoch;
                }
                observed_connected = state.connected.clone();
                observed_drops = state.drop_epoch.clone();
                observed_admissions = state.admission_epoch.clone();
                activation.flush_replacements(&replacement_ready);
                effects
            }
            changed = drop_complete.changed() => {
                if changed.is_err() {
                    continue;
                }
                let completed = Arc::clone(&drop_complete.borrow_and_update());
                let mut effects = Vec::new();
                for (&peer, &epoch) in completed.iter() {
                    let previous = observed_completed.get(&peer).copied().unwrap_or(0);
                    for _ in previous..epoch {
                        effects.extend(lifecycle.apply(LifecycleInput::CompletePeerDrop { peer: peer.0 }));
                    }
                }
                observed_completed = completed.as_ref().clone();
                effects
            }
        };
        for effect in effects {
            match effect {
                LifecycleEffect::SendHello { to, hello } => {
                    info!(
                        server_id = local.0,
                        to = to.len(),
                        "cluster join hello sent"
                    );
                    queue_handshake(
                        &mut pending_handshakes,
                        &to,
                        JoinHandshake::Hello(hello),
                        local.0,
                    );
                }
                LifecycleEffect::SendVoteRequest { to, request } => {
                    queue_handshake(
                        &mut pending_handshakes,
                        &to,
                        JoinHandshake::VoteRequest(request),
                        local.0,
                    );
                }
                LifecycleEffect::SendVote { to, vote } => {
                    queue_handshake(
                        &mut pending_handshakes,
                        &to,
                        JoinHandshake::Vote(vote),
                        local.0,
                    );
                }
                LifecycleEffect::BroadcastAdmit { to, admit } => {
                    info!(
                        server_id = local.0,
                        candidate = admit.candidate,
                        "cluster admit broadcast"
                    );
                    queue_handshake(
                        &mut pending_handshakes,
                        &to,
                        JoinHandshake::Admit(admit),
                        local.0,
                    );
                }
                LifecycleEffect::SendPeerView { to, view } => {
                    queue_handshake(
                        &mut pending_handshakes,
                        &to,
                        JoinHandshake::PeerView(view),
                        local.0,
                    );
                }
                LifecycleEffect::PeerRoleObserved { peer, role } => {
                    if role == ClusterNodeRole::Primary {
                        server.cluster_primary_peers.rcu(|current| {
                            let mut next = (**current).clone();
                            next.insert(ServerId(peer));
                            next
                        });
                        let _ = primary_peer.try_send(peer);
                        handoff_events.push_back(
                            super::cluster_handoff::HandoffCommand::PrimaryConnected(ServerId(
                                peer,
                            )),
                        );
                    }
                }
                LifecycleEffect::PeerAuthenticated { peer } => {
                    handoff_events.push_back(
                        super::cluster_handoff::HandoffCommand::PeerConnected(ServerId(peer)),
                    );
                    if let Some(admission) = namespace_candidates.get(&peer)
                        && lifecycle.peer_incarnations().get(&peer) == Some(&admission.incarnation)
                    {
                        let _ = server.cluster_namespaces.try_observe(admission.clone());
                    }
                    activation.joined(ServerId(peer));
                    activation.publish_admissions(&peer_admitted);
                }
                LifecycleEffect::EmitHandoff(request) => {
                    info!(
                        server_id = local.0,
                        leaver = request.leaver,
                        handoffs = request.handoffs.len(),
                        "cluster leave handoff emitted"
                    );
                    send_handoff(&outbound, &fallback, &request, local.0);
                }
                LifecycleEffect::Joined { candidate } => {
                    if let Some(admission) = namespace_candidates.get(&candidate)
                        && lifecycle.peer_incarnations().get(&candidate)
                            == Some(&admission.incarnation)
                    {
                        let _ = server.cluster_namespaces.try_observe(admission.clone());
                    }
                    info!(server_id = local.0, candidate, "cluster peer joined");
                    if candidate == local.0 {
                        CLUSTER_ADMITTED.store(true, Ordering::Relaxed);
                        CLUSTER_LIFECYCLE_STATE.store(2, Ordering::Relaxed);
                    } else {
                        activation.joined(ServerId(candidate));
                        activation.publish_admissions(&peer_admitted);
                    }
                }
                LifecycleEffect::Left { peer } => {
                    info!(server_id = local.0, peer, "cluster peer left");
                    activation.left(ServerId(peer));
                    activation.publish_admissions(&peer_admitted);
                    if peer == local.0 {
                        CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
                    } else {
                        super::cluster_presence::forget_remote_server(&server, peer);
                        let _ = peer_reset.try_send(peer);
                    }
                }
                LifecycleEffect::LeaveBlocked {
                    player_count,
                    owned_entities,
                } => {
                    warn!(
                        server_id = local.0,
                        player_count,
                        owned_entities,
                        "cluster leave blocked: drain players and entities first"
                    );
                }
                LifecycleEffect::VoteDenied { candidate, voter } => {
                    warn!(
                        server_id = local.0,
                        candidate, voter, "cluster join vote denied"
                    );
                }
                LifecycleEffect::VoteBlocked { candidate } => {
                    warn!(
                        server_id = local.0,
                        candidate, "cluster join vote blocked: suspect peer"
                    );
                }
                LifecycleEffect::SuspectMarked { peer } => {
                    warn!(
                        server_id = local.0,
                        peer, "cluster suspect peer marked after unclean drop"
                    );
                }
                LifecycleEffect::SuspectCleared { peer } => {
                    info!(
                        server_id = local.0,
                        peer, "cluster suspect peer cleared by operator"
                    );
                }
                LifecycleEffect::PeerDropCompleted { peer } => {
                    activation.replacement_completed(ServerId(peer));
                    activation.flush_replacements(&replacement_ready);
                }
                LifecycleEffect::StateChanged(state) => {
                    CLUSTER_LIFECYCLE_STATE.store(lifecycle_state_code(state), Ordering::Relaxed);
                    if state == LifecycleState::Member {
                        CLUSTER_ADMITTED.store(true, Ordering::Relaxed);
                    } else if state == LifecycleState::Solo || state == LifecycleState::Joining {
                        CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
                    }
                    info!(server_id = local.0, ?state, "cluster lifecycle state");
                }
            }
            activation.flush_replacements(&replacement_ready);
        }
        server.cluster_peer_incarnations.store(Arc::new(
            lifecycle
                .peer_incarnations()
                .into_iter()
                .map(|(peer, incarnation)| (ServerId(peer), incarnation))
                .collect(),
        ));
        if let Some(generations) = pending_peer_generations.take() {
            server.cluster_peer_generations.store(Arc::new(generations));
        }
    }
}

pub fn note_cluster_handshake(parcel: &InboundParcel) {
    let Some(input) = LIFECYCLE_IN.get() else {
        return;
    };
    let Ok(message) = decode_handshake(&parcel.bytes) else {
        if let Ok(request) = postcard::from_bytes::<EntityHandoffRequest>(&parcel.bytes)
            && request.leaver == parcel.peer.0
        {
            let _ = input.try_send(LifecycleInput::RemoteHandoff(request));
        }
        return;
    };
    trace!(peer = parcel.peer.0, generation = parcel.generation, message = ?message, "cluster handshake inbound");
    let next = match message {
        JoinHandshake::Hello(hello) if hello.candidate == parcel.peer.0 => {
            LifecycleInput::RemoteHello {
                hello,
                pin_match: true,
            }
        }
        JoinHandshake::Hello(_) => return,
        JoinHandshake::VoteRequest(request) if request.requested_by == parcel.peer.0 => {
            LifecycleInput::RemoteVoteRequest(request)
        }
        JoinHandshake::Vote(vote) if vote.voter == parcel.peer.0 => {
            LifecycleInput::RemoteVote(vote)
        }
        JoinHandshake::Admit(admit) if admit.admitted_by == parcel.peer.0 => {
            LifecycleInput::RemoteAdmit(admit)
        }
        JoinHandshake::PeerView(view) if view.reporter == parcel.peer.0 => {
            LifecycleInput::RemotePeerView(view)
        }
        _ => return,
    };
    if input.try_send(next).is_err() {
        warn!(
            peer = parcel.peer.0,
            "cluster handshake dropped: lifecycle inbox full"
        );
    }
}

pub fn forward_fused(receiver: mpsc::Receiver<Vec<u8>>) {
    let Some(bridge) = FUSED_BRIDGE.get() else {
        return;
    };
    let mut receiver = receiver;
    while let Ok(batch) = receiver.try_recv() {
        if bridge.try_send(batch).is_err() {
            break;
        }
    }
}

/// Forwards one encoded movement batch to the mesh without blocking.
///
/// Thin single-batch counterpart to [`forward_fused`] for producers that
/// already hold encoded [`TickBatch`](pumpkin_cluster::protocol::TickBatch)
/// bytes, such as the movement bank pump. Drops the batch when no bridge is
/// installed or the bridge is full, touching only lock-free channel
/// operations and taking no locks.
pub fn forward_movement_batch(batch: Vec<u8>) {
    let Some(bridge) = FUSED_BRIDGE.get() else {
        return;
    };
    let _ = bridge.try_send(batch);
}

const fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte.saturating_sub(b'0')),
        b'a'..=b'f' => Some(byte.saturating_sub(b'a').saturating_add(10)),
        b'A'..=b'F' => Some(byte.saturating_sub(b'A').saturating_add(10)),
        _ => None,
    }
}

fn decode_pin_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = [0_u8; 32];
    let mut i = 0_usize;
    while i < 32 {
        let hi = hex_val(bytes[i.saturating_mul(2)])?;
        let lo = hex_val(bytes[i.saturating_mul(2).saturating_add(1)])?;
        out[i] = hi.saturating_mul(16).saturating_add(lo);
        i = i.saturating_add(1);
    }
    Some(out)
}

fn mesh_from_config(config: &ClusterConfig) -> Option<MeshConfig> {
    let mut peers = Vec::with_capacity(config.peers.len());
    for peer in &config.peers {
        let Some(pin) = decode_pin_hex(&peer.pubkey_sha256_hex) else {
            warn!(
                server_id = peer.server_id,
                "cluster peer pin is not 64 hex chars, mesh disabled"
            );
            return None;
        };
        peers.push(PinnedPeer {
            server: ServerId(peer.server_id),
            addr: peer.addr.clone(),
            pubkey_sha256: pin,
        });
    }
    Some(MeshConfig {
        server: ServerId(config.server_id),
        bind_addr: config.bind_addr.clone(),
        cert_path: config.cert_path.clone(),
        key_path: config.key_path.clone(),
        peers,
    })
}

pub fn maybe_bootstrap(server: &Arc<Server>) {
    let config = server.advanced_config.cluster.clone();
    if !config.enabled {
        return;
    }
    start_ntp_discipline(server, &config);
    let is_primary = matches!(config.role, ClusterRole::Primary);
    let replayed_admin_ids = if is_primary {
        match super::cluster_admin_journal::replay(server) {
            Ok(ids) => ids,
            Err(error) => {
                error!(%error, "cluster admin journal replay failed");
                std::process::exit(1);
            }
        }
    } else {
        Vec::new()
    };
    if is_primary {
        server.cluster_admin_view.store(Arc::new(
            super::cluster_admin_apply::snapshot_from_stores(
                server,
                replayed_admin_ids.len() as u64,
            ),
        ));
    }
    let Some(mesh) = mesh_from_config(&config) else {
        error!("cluster enabled but mesh peers invalid, mesh disabled");
        return;
    };
    let local = ServerId(config.server_id);
    if let Err(error) = load_or_generate_keypair(&mesh) {
        error!("cluster keypair unavailable: {error}");
        return;
    }
    let peer_ids: Vec<ServerId> = mesh.peers.iter().map(|peer| peer.server).collect();
    let fallback: Vec<u16> = peer_ids.iter().map(|peer| peer.0).collect();
    let (transport_channels, mesh_channels) = channel_pair(4096);
    let transport = match Transport::bind(mesh, transport_channels) {
        Ok(transport) => transport,
        Err(error) => {
            if is_primary {
                error!("cluster QUIC bind failed on primary: {error}");
                std::process::exit(1);
            }
            error!("cluster QUIC bind failed on secondary: {error}");
            return;
        }
    };
    match transport.local_addr() {
        Ok(addr) => info!(
            server_id = local.0,
            addr = %addr,
            peers = peer_ids.len(),
            role = if is_primary { "primary" } else { "secondary" },
            "cluster QUIC endpoint bound"
        ),
        Err(error) => warn!("cluster endpoint bound but local addr unknown: {error}"),
    }
    let fingerprint = transport.local_cert_fingerprint();
    CLUSTER_IS_PRIMARY.store(is_primary, Ordering::Relaxed);
    CLUSTER_MESH_READY.store(true, Ordering::Relaxed);
    CLUSTER_LIFECYCLE_STATE.store(0, Ordering::Relaxed);
    CLUSTER_ADMITTED.store(peer_ids.is_empty(), Ordering::Relaxed);
    let (lifecycle_in_tx, lifecycle_in_rx) = mpsc::channel(4096);
    let lifecycle = Lifecycle::new_solo(local.0);
    server.cluster_peer_incarnations.store(Arc::new(
        lifecycle
            .peer_incarnations()
            .into_iter()
            .map(|(peer, generation)| (ServerId(peer), generation))
            .collect(),
    ));
    let namespace_snapshot = server.cluster_namespaces.snapshot();
    let local_namespace = super::cluster_namespaces::NamespaceAdmission {
        peer: local,
        incarnation: lifecycle.local_incarnation(),
        certificate_fingerprint: fingerprint,
        role: if is_primary {
            pumpkin_cluster::membership::ClusterNodeRole::Primary
        } else {
            pumpkin_cluster::membership::ClusterNodeRole::Secondary
        },
        protocol_version: pumpkin_cluster::membership::CLUSTER_PROTOCOL_VERSION,
        supersedes: namespace_snapshot.admitted.get(&local).map(|previous| {
            pumpkin_cluster::realm_completion::PeerGeneration {
                server: previous.peer,
                generation: previous.incarnation,
            }
        }),
    };
    if namespace_snapshot
        .admitted
        .get(&local)
        .is_some_and(|previous| previous.incarnation == local_namespace.incarnation)
        || server
            .cluster_namespaces
            .retained_collision(&local_namespace)
            .is_some()
    {
        error!(
            server_id = local.0,
            "cluster source namespace collides with retained identity"
        );
        return;
    }
    let _ = server.cluster_namespaces.try_observe(local_namespace);
    server
        .cluster_peer_incarnations
        .store(Arc::new(HashMap::from([(
            local,
            lifecycle.local_incarnation(),
        )])));
    let peer_list: Vec<u16> = peer_ids.iter().map(|peer| peer.0).collect();
    let _ = lifecycle_in_tx.try_send(LifecycleInput::BeginJoin {
        cert_fingerprint: fingerprint,
        peers: peer_list,
        role: if is_primary {
            ClusterNodeRole::Primary
        } else {
            ClusterNodeRole::Secondary
        },
    });
    let _ = LIFECYCLE_IN.set(lifecycle_in_tx);
    if let Some(admission_input) = LIFECYCLE_IN.get().cloned() {
        server.spawn_task(async move {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            let _ = admission_input.try_send(LifecycleInput::AdvanceAdmission {
                elapsed_millis: 10_000,
            });
        });
    }
    server.spawn_task(async move {
        transport.run().await;
    });

    let (demux_senders, demux_receivers) = demux_channels(4096);
    let (control_tx, control_rx) = mpsc::channel(64);
    for peer in &peer_ids {
        let _ = control_tx.try_send(DemuxControl::OpenShared { peer: *peer });
    }
    let inbound_rx = mesh_channels.inbound_rx;
    let peer_state_rx = mesh_channels.peer_state_rx;
    let replacement_ready_tx = mesh_channels.replacement_ready_tx;
    let peer_admitted_tx = mesh_channels.peer_admitted_tx;
    let outbound_tx = mesh_channels.outbound_tx;
    let drop_request_tx = mesh_channels.drop_request_tx;
    let generation_drop_request_tx = mesh_channels.generation_drop_request_tx;
    let datagram_in_rx = mesh_channels.datagram_in_rx;
    server
        .cluster_datagram_outlet
        .store(Some(Arc::new(mesh_channels.datagram_out_tx.clone())));
    cluster_datagram::install_datagram_outbox(peer_ids.clone(), mesh_channels.datagram_out_tx);
    super::cluster_world_delta::install_world_delta_outbox(peer_ids.clone(), outbound_tx.clone());
    super::cluster_ghost::spawn_ghost_apply(&server, local, datagram_in_rx);
    server.spawn_task(run_demux(
        inbound_rx,
        control_rx,
        demux_senders,
        StreamRegistry::new(),
    ));

    super::cluster_transient::spawn_retired_transient_drain(&server, demux_receivers.transient);

    spawn_visual_apply(&server, local, demux_receivers.visual);

    let (completion_tx, completion_rx) = mpsc::channel::<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        CompletionMessage,
    )>(1024);
    let (local_tick_tx, local_tick_rx) = mpsc::channel::<LocalTickBatch>(64);
    let (actor_drain_tx, actor_drain_rx) = mpsc::channel(256);
    let (chunk_inventory_tx, chunk_inventory_rx) = mpsc::channel(128);
    server
        .cluster_chunk_inventory_route
        .store(Some(Arc::new(chunk_inventory_tx.clone())));
    server
        .cluster_actor_drain_route
        .store(Some(Arc::new(actor_drain_tx)));
    let (peer_drop_tx, peer_drop_rx) = tokio::sync::watch::channel(Arc::new(HashMap::new()));
    let (drop_complete_tx, drop_complete_rx) =
        tokio::sync::watch::channel(Arc::new(HashMap::new()));
    spawn_world_apply(
        &server,
        demux_receivers.world,
        demux_receivers.combat,
        completion_rx,
        local_tick_rx,
        peer_drop_rx,
        drop_complete_tx,
        peer_ids.clone(),
        outbound_tx.clone(),
        drop_request_tx.clone(),
        actor_drain_rx,
        chunk_inventory_rx,
    );
    let (persisted_decision_tx, persisted_decision_rx) = mpsc::channel(4096);
    let (peer_claim_capacity_tx, peer_claim_capacity_rx) = mpsc::channel(1);
    for world in server.worlds.load().iter() {
        world
            .level
            .cluster_persisted_decision
            .store(Some(Arc::new(persisted_decision_tx.clone())));
        world
            .level
            .cluster_peer_claim_capacity
            .store(Some(Arc::new(peer_claim_capacity_tx.clone())));
    }
    if server.is_cluster_primary() {
        super::cluster_checkpoint::spawn_persisted_admission(&server);
    }

    let (peer_reset_tx, peer_reset_rx) = mpsc::channel::<u16>(64);
    let (primary_peer_tx, primary_peer_rx) = mpsc::channel::<u16>(8);
    let (fetch_tx, fetch_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_fetch_sender(fetch_tx);
    let (unwant_tx, unwant_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_unwant_sender(unwant_tx);
    let need_capacity = chunk_need_route_capacity(&[
        (server.advanced_config.networking.java.enabled,server.advanced_config.networking.java.max_players),
        (server.advanced_config.networking.bedrock.enabled,server.advanced_config.networking.bedrock.max_players),
    ]);
    let (need_tx, need_rx) = mpsc::channel::<ChunkNeed>(need_capacity);
    server
        .cluster_chunk_need_route
        .store(Some(Arc::new(need_tx)));
    let (logout_tx, logout_rx) = mpsc::channel::<ClusterLogoutGrace>(64);
    set_cluster_logout_sender(logout_tx);
    let chunk_server = server.clone();
    let chunk_outbound = outbound_tx.clone();
    server.spawn_task(chunk_task(
        chunk_server,
        local,
        fallback.clone(),
        chunk_outbound,
        demux_receivers.chunk,
        persisted_decision_rx,
        peer_claim_capacity_rx,
        fetch_rx,
        unwant_rx,
        need_rx,
        logout_rx,
        peer_reset_rx,
        primary_peer_rx,
        chunk_inventory_tx,
    ));

    spawn_entity_apply(&server, local, demux_receivers.entity);

    server.spawn_task(accept_task(
        server.clone(),
        demux_receivers.accept,
        completion_tx,
        generation_drop_request_tx,
    ));

    super::cluster_entity_apply::install_entity_outbox(
        local,
        fallback.clone(),
        outbound_tx.clone(),
    );
    let _ = server
        .cluster_global_outbound
        .configure(outbound_tx.clone());
    super::cluster_invsee::configure(server, local, outbound_tx.clone());
    super::cluster_world_rules::spawn(server);
    let (admin_tx, admin_rx) = mpsc::channel::<InboundParcel>(1024);
    let (admin_local_tx, admin_local_rx) = mpsc::channel(4096);
    let (admin_seed_tx, admin_seed_rx) = mpsc::channel(256);
    server
        .cluster_admin_local_route
        .store(Some(Arc::new(admin_local_tx)));
    server
        .cluster_admin_seed_route
        .store(Some(Arc::new(admin_seed_tx)));
    let (chat_tx, chat_rx) = mpsc::channel::<InboundParcel>(1024);
    let (hide_tx, hide_rx) = mpsc::channel::<InboundParcel>(1024);
    let invsee_tx = server.cluster_invsee_tx.clone();
    let (presence_tx, presence_rx) = mpsc::channel::<InboundParcel>(1024);
    let (world_time_tx, world_time_rx) = mpsc::channel::<InboundParcel>(1024);
    let (health_policy_tx, health_policy_rx) = mpsc::channel::<InboundParcel>(64);
    let (handoff_in_tx, handoff_in_rx) = mpsc::channel::<InboundParcel>(128);
    let (handoff_command_tx, handoff_command_rx) = mpsc::channel(128);
    let _ = server.cluster_handoff_tx.set(handoff_command_tx);
    #[cfg(feature = "cluster-e2e")]
    let (test_tx, test_rx) = mpsc::channel::<InboundParcel>(64);
    super::cluster_health_policy::spawn_health_policy(
        server,
        outbound_tx.clone(),
        health_policy_rx,
    );
    super::cluster_presence::install_presence_outbox(
        server,
        local,
        fallback.clone(),
        outbound_tx.clone(),
    );
    super::cluster_tick_pump::install_tick_pump_outbox(local_tick_tx);
    cluster_chat_in::spawn_control_fanout(
        server,
        demux_receivers.control,
        admin_tx,
        chat_tx,
        invsee_tx,
        presence_tx,
        hide_tx,
        world_time_tx,
        health_policy_tx,
        handoff_in_tx,
        #[cfg(feature = "cluster-e2e")]
        test_tx,
    );
    super::cluster_hide::spawn_hide_apply(server, hide_rx);
    super::cluster_world_time::spawn_world_time_apply(server, world_time_rx);
    server.spawn_task(super::cluster_handoff::run(
        server.clone(),
        local,
        fallback.clone(),
        outbound_tx.clone(),
        handoff_command_rx,
        handoff_in_rx,
    ));
    #[cfg(feature = "cluster-e2e")]
    super::cluster_test::spawn_test_apply(
        server,
        local,
        fallback.clone(),
        outbound_tx.clone(),
        test_rx,
    );
    cluster_chat_in::spawn_chat_delivery(server, local, chat_rx);
    super::cluster_presence::spawn_presence_apply(server, local, presence_rx);

    server
        .cluster_player_admin
        .install_transport(outbound_tx.clone());
    server.spawn_task(admin_task(
        server.clone(),
        local,
        admin_rx,
        admin_local_rx,
        admin_seed_rx,
        replayed_admin_ids,
    ));

    let (fused_tx, fused_rx) = mpsc::channel::<Vec<u8>>(8);
    let _ = FUSED_BRIDGE.set(fused_tx);
    server.spawn_task(fuse_task(
        fused_rx,
        outbound_tx.clone(),
        drop_request_tx.clone(),
        peer_ids.clone(),
    ));

    server.spawn_task(lifecycle_effect_task(
        server.clone(),
        local,
        outbound_tx.clone(),
        peer_ids.clone(),
        lifecycle,
        lifecycle_in_rx,
        peer_reset_tx,
        primary_peer_tx,
        replacement_ready_tx,
        peer_admitted_tx,
        peer_state_rx,
        peer_drop_tx,
        drop_complete_rx,
    ));

    for world in server.worlds.load().iter() {
        world.level.set_cluster_dual_enabled(!is_primary);
    }
    set_cluster_secondary(!is_primary);
    set_cluster_has_peers(!peer_ids.is_empty());
    if !is_primary && peer_ids.is_empty() {
        error!(
            server_id = local.0,
            "cluster secondary has no pinned peers, chunk loading will wait for a holder instead of generating"
        );
    }
    info!(
        server_id = local.0,
        secondary = !is_primary,
        "cluster mesh bootstrap complete"
    );

    cluster_regions::spawn_region_keeper(&server);
    #[cfg(feature = "cluster-e2e")]
    if is_primary {
        spawn_e2e_primary_pregen(&server);
    }
}

#[cfg(feature = "cluster-e2e")]
fn spawn_e2e_primary_pregen(server: &Arc<Server>) {
    let Ok(radius) = std::env::var("PUMPKIN_CLUSTER_E2E_PREGEN_RADIUS") else {
        return;
    };
    let Ok(radius) = radius.parse::<u8>() else {
        error!("cluster e2e primary pregen radius invalid");
        return;
    };
    let Some(radius) = std::num::NonZeroU8::new(radius) else {
        error!("cluster e2e primary pregen radius out of range");
        return;
    };
    if radius.get() > 32 {
        error!("cluster e2e primary pregen radius out of range");
        return;
    }
    let Some(world) = server
        .worlds
        .load()
        .iter()
        .find(|world| world.dimension.id == 0)
        .cloned()
    else {
        error!("cluster e2e primary pregen overworld unavailable");
        return;
    };
    let info = server.level_info.load();
    let center = Vector2::new(info.spawn_x.div_euclid(16), info.spawn_z.div_euclid(16));
    let positions: Vec<_> =
        pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(center, radius)
            .all_chunks_within()
            .collect();
    server.spawn_task(async move {
        let mut pending = positions.into_iter();
        let mut workers = tokio::task::JoinSet::new();
        let mut completed = Vec::new();
        loop {
            while workers.len() < 8 {
                let Some(position) = pending.next() else { break };
                let level = world.level.clone();
                workers.spawn(async move {
                    (position, level.get_or_fetch_chunk(position, Clone::clone).await)
                });
            }
            let Some(result) = workers.join_next().await else { break };
            let Ok(chunk) = result else {
                error!("cluster e2e primary pregen worker failed");
                return;
            };
            completed.push(chunk);
        }
        let count = completed.len();
        world.level.write_chunks(completed).await;
        info!(target: "cluster_e2e", event = "primary_pregen_complete", count, radius = radius.get(), center_x = center.x, center_z = center.y);
    });
}

static CHUNK_WARN_LAST_MILLIS: AtomicU64 = AtomicU64::new(0);

fn chunk_warn_cooldown_elapsed() -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0);
    let last = CHUNK_WARN_LAST_MILLIS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000 {
        return false;
    }
    CHUNK_WARN_LAST_MILLIS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

fn flush_owner_chunk_demands(
    pending: &mut VecDeque<(ChunkKey, OwnerChunkDemand)>,
    mut route_for: impl FnMut(
        ChunkKey,
    ) -> Option<(
        Arc<crossbeam::channel::Sender<OwnerChunkDemand>>,
        Option<Arc<crossbeam::channel::Sender<()>>>,
    )>,
) {
    let mut blocked_chunks = HashSet::new();
    for _ in 0..pending.len() {
        let (chunk, demand) = pending.pop_front().expect("queue length frozen");
        if blocked_chunks.contains(&chunk) {
            pending.push_back((chunk, demand));
            continue;
        }
        let Some((route, wake)) = route_for(chunk) else {
            blocked_chunks.insert(chunk);
            pending.push_back((chunk, demand));
            continue;
        };
        match route.try_send(demand) {
            Ok(()) => {
                if let Some(wake) = wake {
                    let _ = wake.try_send(());
                }
            }
            Err(crossbeam::channel::TrySendError::Full(demand))
            | Err(crossbeam::channel::TrySendError::Disconnected(demand)) => {
                blocked_chunks.insert(chunk);
                pending.push_back((chunk, demand));
            }
        }
    }
}

pub enum AcquisitionSourceInput {
    Control {
        control: pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl,
        events: mpsc::Sender<AcquisitionOwnerEvent>,
    },
    Seal {
        tick: TickStamp,
        events: mpsc::Sender<AcquisitionOwnerEvent>,
    },
}

pub enum AcquisitionOwnerEvent {
    Issued(pumpkin_cluster::acquisition_prefix::NumberedChunkAcquisition),
    ClosedPrefix(pumpkin_cluster::acquisition_prefix::AcquisitionPrefix),
    ObservedPrefix {
        authenticated: pumpkin_cluster::realm_completion::PeerGeneration,
        prefix: pumpkin_cluster::acquisition_prefix::AcquisitionPrefix,
    },
}

struct AcquisitionRequestBank {
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    providers: HashMap<ServerId, u64>,
    next_subscription: u64,
    tick: Option<TickStamp>,
    pending: VecDeque<AcquisitionSourceInput>,
    active: HashMap<ChunkKey, pumpkin_cluster::chunks::ChunkSubscriptionRequest>,
    directory_dirty: bool,
    events: mpsc::Sender<AcquisitionOwnerEvent>,
}

impl AcquisitionRequestBank {
    fn observe(&mut self, tick: TickStamp) {
        if self
            .tick
            .is_some_and(|previous| tick == previous || !tick.is_after(previous))
        {
            return;
        }
        let mut next = self
            .tick
            .map_or(tick, |previous| TickStamp(previous.0.wrapping_add(1)));
        loop {
            if self.pending.len() >= 16_384 {
                break;
            }
            self.pending.push_back(AcquisitionSourceInput::Seal {
                tick: next,
                events: self.events.clone(),
            });
            self.tick = Some(next);
            if next == tick {
                break;
            }
            next = TickStamp(next.0.wrapping_add(1));
        }
    }

    fn acquire(&mut self, directory: &mut Directory, chunk: ChunkKey, provider: ServerId) -> bool {
        let Some(generation) = self.providers.get(&provider).copied() else {
            return false;
        };
        let Some(tick) = self.tick else {
            return false;
        };
        if self.pending.len() >= 16_384
            || self.active.len() >= 8192 && !self.active.contains_key(&chunk)
        {
            return false;
        }
        let Some(next) = self.next_subscription.checked_add(1) else {
            return false;
        };
        let subscription_id = self.next_subscription;
        self.next_subscription = next;
        let request = pumpkin_cluster::chunks::ChunkSubscriptionRequest {
            chunk,
            subscriber: self.source.server.0,
            incarnation: self.source.generation,
            subscription_id,
            from_tick: TickStamp(tick.0.wrapping_add(1)),
        };
        if !directory.subscribe(pumpkin_cluster::chunks::ChunkSubscription {
            chunk,
            subscriber: request.subscriber,
            incarnation: request.incarnation,
            subscription_id,
            from_tick: request.from_tick,
        }) {
            return false;
        }
        self.directory_dirty = true;
        self.pending.push_back(AcquisitionSourceInput::Control {
            control: pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::Acquire {
                provider: pumpkin_cluster::realm_completion::PeerGeneration {
                    server: provider,
                    generation,
                },
                request,
            },
            events: self.events.clone(),
        });
        self.active.insert(chunk, request);
        true
    }

    fn retire_unwanted(&mut self, directory: &mut Directory, wanted: &HashSet<ChunkKey>) {
        let Some(tick) = self.tick else {
            return;
        };
        let retired = self
            .active
            .keys()
            .filter(|chunk| !wanted.contains(chunk))
            .copied()
            .collect::<Vec<_>>();
        for chunk in retired {
            if self.pending.len() >= 16_384 {
                break;
            }
            let previous = self.active.remove(&chunk).expect("active acquisition");
            let subscription_id = previous.subscription_id;
            directory.unsubscribe(pumpkin_cluster::chunks::ChunkUnsubscription {
                chunk,
                subscriber: previous.subscriber,
                incarnation: previous.incarnation,
                subscription_id,
            });
            self.directory_dirty = true;
            self.pending.push_back(AcquisitionSourceInput::Control {
                control: pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::Retire {
                    request: pumpkin_cluster::chunks::ChunkUnsubscription {
                        chunk,
                        subscriber: self.source.server.0,
                        incarnation: self.source.generation,
                        subscription_id,
                    },
                    tick: TickStamp(tick.0.wrapping_add(1)),
                },
                events: self.events.clone(),
            });
        }
    }
}

async fn chunk_task(
    server: Arc<Server>,
    local: ServerId,
    fallback: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut chunk_rx: mpsc::Receiver<InboundParcel>,
    mut persisted_decision_rx: mpsc::Receiver<pumpkin_world::level::PersistedChunkDecision>,
    mut peer_claim_capacity_rx: mpsc::Receiver<()>,
    mut want_rx: mpsc::Receiver<ClusterFetchRequest>,
    mut unwant_rx: mpsc::Receiver<ClusterFetchRequest>,
    mut need_rx: mpsc::Receiver<ChunkNeed>,
    mut logout_rx: mpsc::Receiver<ClusterLogoutGrace>,
    mut peer_reset_rx: mpsc::Receiver<u16>,
    mut primary_peer_rx: mpsc::Receiver<u16>,
    chunk_inventory_tx: mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
) {
    let (installed_tx, mut installed_rx) = mpsc::channel::<(ServerId, ChunkPayload)>(128);
    let (serve_done_tx, mut serve_done_rx) =
        mpsc::channel::<super::cluster_world_owner::ChunkServeExport>(16);
    let (actor_cut_tx, mut actor_cut_rx) =
        mpsc::channel::<super::cluster_world_owner::ActorCutExport>(2);
    let (actor_cut_input_tx, actor_cut_input_rx) = mpsc::channel(2);
    let mut actor_cut_route_pending = Some(actor_cut_input_rx);
    let mut expected_historical = HashMap::<
        (ChunkKey, u64),
        pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest,
    >::new();
    let mut historical_candidates = BTreeMap::<(ChunkKey,u64), std::collections::BTreeSet<pumpkin_cluster::realm_completion::PeerGeneration>>::new();
    let mut unselected_historical = BTreeMap::<(ChunkKey,u64),pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>::new();
    let mut historical_selection_cursor = None::<(ChunkKey,u64)>;
    let mut pending_actor_images = VecDeque::<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        pumpkin_cluster::actor_local_cut::ActorLocalChunkImage,
    )>::new();
    let mut serve_queue = VecDeque::<ServeRequest>::new();
    let mut targeted_chunk_requests = VecDeque::<TargetedChunkRequest>::new();
    let mut queued_chunk_data = VecDeque::<(u64, OutboundParcel)>::new();
    let mut serve_inflight = 0_usize;
    let mut directory = Directory::new();
    let (acquisition_events, mut acquisition_events_rx) = mpsc::channel(1024);
    let mut acquisition_source = AcquisitionRequestBank {
        source: pumpkin_cluster::realm_completion::PeerGeneration {
            server: local,
            generation: *server
                .cluster_peer_incarnations
                .load()
                .get(&local)
                .expect("local acquisition namespace"),
        },
        providers: HashMap::new(),
        next_subscription: 1,
        tick: None,
        pending: VecDeque::new(),
        active: HashMap::new(),
        directory_dirty: false,
        events: acquisition_events,
    };

    let mut acquisition_receipts =
        pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger::default();
    let mut pending_receipt_publication = None;
    let mut wanted: HashSet<ChunkKey> = HashSet::new();
    let mut level_wanted: HashSet<ChunkKey> = HashSet::new();
    let mut owner_needs: HashMap<ChunkKey, HashSet<u64>> = HashMap::new();
    let mut pending_owner_demands = VecDeque::<(ChunkKey, OwnerChunkDemand)>::new();
    let mut pending_need_sets = VecDeque::<PendingChunkNeedSet>::new();
    let mut pending_historical_need_sets = VecDeque::<(
        bool,
        VecDeque<pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>,
    )>::new();
    let mut retained_need_leases = Vec::<(u64, ChunkNeedLease)>::with_capacity(1024);
    let mut pending: HashMap<ChunkKey, ChunkAcquisition> = HashMap::new();
    let mut queued_requests = VecDeque::<(ChunkKey, u16, u64, OutboundParcel)>::new();
    let mut deferred_installs = VecDeque::<PreparedChunkInstall>::new();
    let mut pending_disk_installs = HashMap::<ChunkKey, PreparedChunkInstall>::new();
    let mut pending_claim_installs = HashMap::<(ChunkKey, u64), PreparedChunkInstall>::new();
    let mut pending_unclaimed_installs = HashMap::<(ChunkKey, u64), PreparedChunkInstall>::new();
    let (claim_result_tx, mut claim_result_rx) = mpsc::channel(256);
    let mut persisted_decision_open = true;
    let mut pings = PingTracker::new();
    let mut advertised: HashSet<ChunkKey> = HashSet::new();
    let mut claims = PrimaryClaims::new();
    let mut unroutable_reported: HashSet<ChunkKey> = HashSet::new();
    let mut grace: HashMap<ChunkKey, u64> = HashMap::new();
    let mut primary = None;
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    loop {
        if let Some(input) = actor_cut_route_pending.take() {
            if let Err(error) = chunk_inventory_tx.try_send(
                super::cluster_world_owner::ChunkInventoryRequest::ActorCutRoute {
                    output: actor_cut_tx.clone(),
                    input,
                },
            ) {
                if let super::cluster_world_owner::ChunkInventoryRequest::ActorCutRoute {
                    input,
                    ..
                } = error.into_inner()
                {
                    actor_cut_route_pending = Some(input);
                }
            }
        }
        acquisition_source.providers = (**server.cluster_peer_incarnations.load()).clone();
        let now = disciplined_tick_stamp(chunk_now_millis() as i64);
        if acquisition_source.pending.len() < 16_384 {
            acquisition_source.observe(now);
        }
        acquisition_source.retire_unwanted(&mut directory, &wanted);
        for _ in 0..64 {
            if acquisition_source.pending.len() >= 16384 {
                break;
            }
            let Some((acquire, mut requests)) = pending_historical_need_sets.pop_front() else {
                break;
            };
            if let Some(request) = requests.pop_front() {
                let control = if acquire {
                    pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::HistoricalCut(
                        request,
                    )
                } else {
                    let tick = acquisition_source
                        .tick
                        .map_or(request.source_tick(), |tick| {
                            TickStamp(tick.0.wrapping_add(1))
                        });
                    pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::ReleaseHistorical {
                        chunk: request.chunk, lease: request.lease, full_source_slot: request.full_source_slot, tick,
                    }
                };
                acquisition_source
                    .pending
                    .push_back(AcquisitionSourceInput::Control {
                        control,
                        events: acquisition_source.events.clone(),
                    });
            }
            if !requests.is_empty() {
                pending_historical_need_sets.push_front((acquire, requests));
            }
        }
        if pending_historical_need_sets.len() < 256 {
            let mut selected = Vec::new();
            let pending_keys = if let Some(cursor) = historical_selection_cursor {
                unselected_historical.range((std::ops::Bound::Excluded(cursor),std::ops::Bound::Unbounded))
                    .chain(unselected_historical.range(..=cursor)).take(8).map(|(key,_)| *key).collect::<Vec<_>>()
            } else { unselected_historical.keys().take(8).copied().collect::<Vec<_>>() };
            for key in &pending_keys {
                historical_selection_cursor = Some(*key);
                let Some(request) = unselected_historical.get(key) else { continue; };
                let Some(candidates) = historical_candidates.get(key) else { continue; };
                let live = server.cluster_peer_incarnations.load();
                let peers = candidates.iter().filter(|source| source.server != local && live.get(&source.server).copied() == Some(source.generation)
                    && directory.holders.get(&request.chunk).is_some_and(|holders| holders.contains(&source.server.0)))
                    .map(|source| source.server.0).collect::<Vec<_>>();
                let Some(peer) = pings.best(&peers) else { continue; };
                let Some(provider) = candidates.iter().find(|source| source.server.0 == peer) else { continue; };
                let mut request = *request;
                request.provider = *provider;
                expected_historical.insert(*key,request);
                pending_historical_need_sets.push_back((true,std::collections::VecDeque::from([request])));
                selected.push(*key);
            }
            for key in selected { unselected_historical.remove(&key); }
        }
        let mut acquisitions_processed = false;
        for source in acquisition_receipts.pending_sources() {
            acquisitions_processed |= process_chunk_acquisitions(
                &server,
                local,
                &mut directory,
                &mut acquisition_receipts,
                &mut claims,
                &mut pending_owner_demands,
                &mut serve_queue,
                source,
                &chunk_inventory_tx,
            );
        }
        process_targeted_chunk_requests(
            &server,
            local,
            &directory,
            &acquisition_receipts,
            &mut targeted_chunk_requests,
            &mut claims,
            &mut pending_owner_demands,
            &mut serve_queue,
        );
        if acquisitions_processed {
            cluster_datagram::publish_holder_directory(&directory);
            server
                .cluster_chunk_directory
                .store(Arc::new(directory.clone()));
            pending_receipt_publication = Some(acquisition_receipts.clone());
        }
        if let Some(receipts) = pending_receipt_publication.take() {
            match chunk_inventory_tx.try_send(
                super::cluster_world_owner::ChunkInventoryRequest::AcquisitionReceipts(receipts),
            ) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(
                    super::cluster_world_owner::ChunkInventoryRequest::AcquisitionReceipts(
                        receipts,
                    ),
                )) => {
                    pending_receipt_publication = Some(receipts);
                }
                Err(_) => panic!("cluster acquisition receipt owner unavailable"),
            }
        }
        if pending_receipt_publication.is_none() {
            while let Some((provider, image)) = pending_actor_images.pop_front() {
                if expected_historical.get(&(image.request.chunk, image.request.lease))
                    != Some(&image.request)
                {
                    continue;
                }
                if !acquisition_receipts
                    .active_historical()
                    .iter()
                    .any(|(source, request)| source.server == local && *request == image.request)
                {
                    pending_actor_images.push_front((provider, image));
                    break;
                }
                match actor_cut_input_tx.try_send((provider, image)) {
                    Ok(()) => {}
                    Err(error) => {
                        pending_actor_images.push_front(error.into_inner());
                        break;
                    }
                }
            }
        }
        if acquisition_source.directory_dirty {
            cluster_datagram::publish_holder_directory(&directory);
            server
                .cluster_chunk_directory
                .store(Arc::new(directory.clone()));
            acquisition_source.directory_dirty = false;
        }
        while let Some(input) = acquisition_source.pending.pop_front() {
            match chunk_inventory_tx
                .try_send(super::cluster_world_owner::ChunkInventoryRequest::Acquisition(input))
            {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(
                    super::cluster_world_owner::ChunkInventoryRequest::Acquisition(input),
                )) => {
                    acquisition_source.pending.push_front(input);
                    break;
                }
                Err(_) => panic!("cluster acquisition owner unavailable"),
            }
        }
        for _ in 0..64 {
            if pending_owner_demands.len() >= 1024 || queued_requests.len() >= 16384 {
                break;
            }
            let Some(mut need) = pending_need_sets.pop_front() else {
                break;
            };
            if let Some(chunk) = need.chunks.get(need.next).copied() {
                need.next += 1;
                if need.acquire {
                    if owner_needs.entry(chunk).or_default().insert(need.lease) {
                        pending_owner_demands.push_back((
                            chunk,
                            OwnerChunkDemand::Acquire {
                                position: Vector2::new(chunk.x(), chunk.z()),
                                lease: need.lease,
                                ready: None,
                            },
                        ));
                        wanted.insert(chunk);
                        grace.remove(&chunk);
                        request_chunk_once(
                            &mut acquisition_source,
                            &server.cluster_peer_generations.load(),
                            &mut directory,
                            &pings,
                            primary,
                            &outbound,
                            &mut pending,
                            &mut queued_requests,
                            chunk,
                            None,
                            None,
                        );
                    }
                } else if let Some(leases) = owner_needs.get_mut(&chunk) {
                    if leases.remove(&need.lease) {
                        pending_owner_demands.push_back((
                            chunk,
                            OwnerChunkDemand::Release {
                                position: Vector2::new(chunk.x(), chunk.z()),
                                lease: need.lease,
                            },
                        ));
                    }
                    if leases.is_empty() {
                        owner_needs.remove(&chunk);
                        if !level_wanted.contains(&chunk) {
                            pending_disk_installs.remove(&chunk);
                            pending_unclaimed_installs.retain(|(key, _), _| *key != chunk);
                            reset_fetch_state(
                                &server,
                                &mut wanted,
                                &mut pending,
                                &mut unroutable_reported,
                                chunk,
                                false,
                            );
                        }
                    }
                }
            }
            if need.next < need.chunks.len() {
                pending_need_sets.push_front(need);
            }
        }
        flush_queued_chunk_data(
            &outbound,
            &mut queued_chunk_data,
            &server.cluster_peer_generations.load(),
        );
        flush_owner_chunk_demands(&mut pending_owner_demands, |chunk| {
            server
                .worlds
                .load()
                .iter()
                .find(|world| {
                    world.level.cluster_world_key == chunk.world.key
                        && world.level.cluster_dimension == chunk.world.dimension
                })
                .and_then(|world| {
                    world
                        .level
                        .cluster_owner_chunk_demand
                        .load_full()
                        .map(|route| (route, world.level.fresh_chunk_wake.load_full()))
                })
        });
        tokio::select! {
            event = acquisition_events_rx.recv() => {
                let Some(event) = event else { break; };
                match event {
                    AcquisitionOwnerEvent::Issued(control) => {
                        if acquisition_receipts.admit_source(control.source).is_ok()
                            && acquisition_receipts.receive(control.source, control).is_ok()
                        {
                            process_chunk_acquisitions(&server, local, &mut directory, &mut acquisition_receipts,
                                &mut claims, &mut pending_owner_demands, &mut serve_queue, control.source, &chunk_inventory_tx);
                        }
                    }
                    AcquisitionOwnerEvent::ClosedPrefix(prefix) => {
                        let _ = acquisition_receipts.admit_source(prefix.source);
                        let _ = acquisition_receipts.announce(prefix.source, prefix);
                    }
                    AcquisitionOwnerEvent::ObservedPrefix { authenticated, prefix } => {
                        let _ = acquisition_receipts.admit_source(authenticated);
                        let _ = acquisition_receipts.announce(authenticated, prefix);
                    }
                }
                cluster_datagram::publish_holder_directory(&directory);
                server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                pending_receipt_publication = Some(acquisition_receipts.clone());
            }
            _ = std::future::ready(()), if !pending_historical_need_sets.is_empty() && acquisition_source.pending.len() < 16384 => {}
            _ = std::future::ready(()), if !pending_need_sets.is_empty() && pending_owner_demands.len() < 1024 && queued_requests.len() < 16384 => {}
            need = need_rx.recv(), if pending_owner_demands.len() < 1_024 && pending_need_sets.len() < 256 && pending_historical_need_sets.len() < 256 && acquisition_source.pending.len() < 16_000 => {
                let Some(need) = need else { break };
                match need {
                    ChunkNeed::AcquireOriginalActorChunk {request,providers} => {
                        let key = (request.chunk,request.lease);
                        if historical_candidates.len() < 8192 && request.valid() && providers.contains(&request.provider) {
                            historical_candidates.insert(key,providers);
                            unselected_historical.insert(key,request);
                        }
                    }
                    ChunkNeed::AcquireHistoricalSet { requests } => {
                        if requests.len() <= 8192 && expected_historical.len().saturating_add(requests.iter().filter(|request| !expected_historical.contains_key(&(request.chunk, request.lease))).count()) <= 8192 {
                            for request in &requests { expected_historical.insert((request.chunk, request.lease), *request); }
                            pending_historical_need_sets.push_back((true, requests.into()));
                        }
                    }
                    ChunkNeed::ReleaseHistoricalSet { requests } => {
                        if requests.len() <= 8192 {
                            for request in &requests {
                                historical_candidates.remove(&(request.chunk,request.lease));
                                unselected_historical.remove(&(request.chunk,request.lease));
                                if expected_historical.get(&(request.chunk, request.lease)).is_some_and(|active| active.full_source_slot == request.full_source_slot) { expected_historical.remove(&(request.chunk, request.lease)); }
                            }
                            pending_historical_need_sets.push_back((false, requests.into()));
                        }
                    }
                    ChunkNeed::RetainLease { lease, expires_millis } => {
                        retained_need_leases.push((expires_millis, lease));
                    }
                    ChunkNeed::AcquireSet { chunks, lease } => {
                        if chunks.len() <= 8192 {
                            pending_need_sets.push_back(PendingChunkNeedSet { chunks, lease, next: 0, acquire: true });
                        }
                    }
                    ChunkNeed::ReleaseSet { chunks, lease } => {
                        pending_need_sets.retain(|pending| pending.lease != lease);
                        if chunks.len() <= 8192 {
                            pending_need_sets.push_back(PendingChunkNeedSet { chunks, lease, next: 0, acquire: false });
                        }
                    }
                    ChunkNeed::Acquire { chunk, lease, ready } => {
                        if owner_needs.entry(chunk).or_default().insert(lease) {
                            pending_owner_demands.push_back((chunk, OwnerChunkDemand::Acquire {
                                position: Vector2::new(chunk.position.x, chunk.position.z), lease, ready,
                            }));
                            wanted.insert(chunk);
                            grace.remove(&chunk);
                            request_chunk_once(
                                &mut acquisition_source,
                                &server.cluster_peer_generations.load(),
                                &mut directory, &pings, primary, &outbound, &mut pending,
                                &mut queued_requests, chunk, None, None,
                            );
                        }
                    }
                    ChunkNeed::Release { chunk, lease } => {
                        if let Some(leases) = owner_needs.get_mut(&chunk) {
                            if leases.remove(&lease) {
                                pending_owner_demands.push_back((chunk, OwnerChunkDemand::Release {
                                    position: Vector2::new(chunk.position.x, chunk.position.z), lease,
                                }));
                            }
                            if leases.is_empty() {
                                owner_needs.remove(&chunk);
                                if !level_wanted.contains(&chunk) {
                                    pending_disk_installs.remove(&chunk);
                                    pending_unclaimed_installs.retain(|(pending, _), _| *pending != chunk);
                                    reset_fetch_state(
                                        &server, &mut wanted, &mut pending,
                                        &mut unroutable_reported, chunk, false,
                                    );
                                }
                            }
                        }
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            incoming = chunk_rx.recv(), if pending_actor_images.len() < 2 && deferred_installs.len() < 256 && pending_disk_installs.len() < 128 && pending_claim_installs.len() + pending_unclaimed_installs.len() < 256 && serve_queue.len() < 8_192 && targeted_chunk_requests.len() < 8_192 && pending_owner_demands.len() < 1_024 => {
                let Some(parcel) = incoming else { break };
                let historical_drop = if parcel.header.kind == StreamKind::ChunkAdvert
                    && server.cluster_peer_generations.load().get(&parcel.peer).copied() == Some(parcel.generation) {
                    match decode_announce(&parcel.bytes) {
                        Ok(ChunkAnnounce::Release(drop)) if drop.holder == parcel.peer.0 => Some(drop),
                        _ => None,
                    }
                } else { None };
                if let Some(drop) = historical_drop {
                    let affected = expected_historical.iter().filter(|(_,request)| request.chunk == drop.chunk && request.provider.server == parcel.peer)
                        .map(|(key,request)| (*key,*request)).collect::<Vec<_>>();
                    for (key,request) in affected {
                        if let Some(candidates) = historical_candidates.get_mut(&key) {
                            candidates.remove(&request.provider);
                            unselected_historical.insert(key,request);
                        }
                    }
                }
                handle_chunk_parcel(&mut acquisition_source, &mut acquisition_receipts, &expected_historical, &mut pending_actor_images, &mut targeted_chunk_requests, &server, local.0, &mut directory, &mut wanted, &mut pending, &mut queued_requests, &mut deferred_installs, &mut pending_disk_installs, &mut pending_claim_installs, &mut pending_unclaimed_installs, &claim_result_tx, &mut pings, &mut claims, &mut pending_owner_demands, &mut unroutable_reported, primary, &fallback, &outbound, &chunk_inventory_tx, &installed_tx, &mut serve_queue, parcel).await;
                cluster_datagram::publish_holder_directory(&directory);
                server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                pending_receipt_publication = Some(acquisition_receipts.clone());
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            decision = persisted_decision_rx.recv(), if persisted_decision_open => {
                if let Some(decision) = decision {
                    if let Some(world) = server.worlds.load().iter().find(|world| {
                        world.level.cluster_world_key == decision.world_key
                            && world.level.cluster_dimension == decision.dimension
                    })
                        && let Some(wake) = world.level.fresh_chunk_wake.load_full()
                    {
                        let _ = wake.try_send(());
                    }
                    let key = ChunkKey::new(WorldId::new(decision.dimension, decision.world_key), decision.position.x, decision.position.y);
                    if pending_disk_installs.get(&key).is_some_and(|install| install.reservation_generation == Some(decision.generation)) {
                        if let Some(install) = pending_disk_installs.remove(&key)
                            && decision.recovery
                        {
                            request_peer_install_claim(install, &claim_result_tx, &mut pending_claim_installs, &mut pending_unclaimed_installs);
                        }
                    }
                } else {
                    persisted_decision_open = false;
                }
            }
            claim_result = claim_result_rx.recv() => {
                if let Some(result) = claim_result {
                    if let Some(world) = server.worlds.load().iter().find(|world| {
                        world.level.cluster_world_key == result.world_key
                            && world.level.cluster_dimension == result.dimension
                    })
                        && let Some(wake) = world.level.fresh_chunk_wake.load_full()
                    {
                        let _ = wake.try_send(());
                    }
                    let key = ChunkKey::new(WorldId::new(result.dimension, result.world_key), result.position.x, result.position.y);
                    if let Some(install) = pending_claim_installs.remove(&(key, result.generation)) {
                        let selected = server.cluster_peer_generations.load().get(&install.source).copied() == Some(install.source_generation)
                            && wanted.contains(&key)
                            && pending.get(&key).is_some_and(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { peer, .. } if peer == install.source.0));
                        if result.allowed && selected && install.reservation_generation == Some(result.generation) {
                            deferred_installs.push_back(install);
                        } else if result.allowed {
                            let _ = install.world.level.try_release_peer_chunk_install(result.position, result.generation);
                        }
                    }
                }
            }
            capacity = peer_claim_capacity_rx.recv(), if !pending_unclaimed_installs.is_empty() => {
                if capacity.is_some() {
                    let keys: Vec<_> = pending_unclaimed_installs.keys().copied().collect();
                    for key in keys {
                        let Some(install) = pending_unclaimed_installs.remove(&key) else { continue };
                        let selected = server.cluster_peer_generations.load().get(&install.source).copied() == Some(install.source_generation)
                            && wanted.contains(&key.0)
                            && pending.get(&key.0).is_some_and(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { peer, .. } if peer == install.source.0));
                        let reservation = install.world.level.cluster_publication_reservations.load();
                        let valid = reservation.get(&Vector2::new(key.0.x(), key.0.z())).is_some_and(|entry| {
                            entry.generation == key.1 && entry.state == pumpkin_world::level::PersistedChunkReservationState::Recovery
                        });
                        if selected && valid {
                            request_peer_install_claim(install, &claim_result_tx, &mut pending_claim_installs, &mut pending_unclaimed_installs);
                        }
                    }
                }
            }
            served = actor_cut_rx.recv(), if queued_chunk_data.len() < 16 => {
                let Some(served) = served else { break; };
                if server.cluster_peer_incarnations.load().get(&served.requester.server).copied() != Some(served.requester.generation) { continue; }
                let Some(generation) = server.cluster_peer_generations.load().get(&served.requester.server).copied() else { continue; };
                let request = served.image.request;
                if !acquisition_receipts.active_historical().iter().any(|(source, active)| *source == served.requester && *active == request) { continue; }
                let Ok(bytes) = pumpkin_cluster::actor_local_cut::encode(&served.image) else { continue; };
                let parcel = OutboundParcel { peer: served.requester.server, header: StreamHeader::new(StreamKind::ChunkData, None), bytes };
                match outbound.try_send(parcel) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(parcel)) => queued_chunk_data.push_back((generation, parcel)),
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            served = serve_done_rx.recv() => {
                let Some(served) = served else { break };
                serve_inflight = serve_inflight.saturating_sub(1);
                if server.cluster_peer_generations.load().get(&served.peer).copied() != Some(served.generation) { continue; }
                if served.export.is_some() && advertised.insert(served.chunk) {
                    let advert = ChunkAdvert { holder: local.0, chunk: served.chunk };
                    directory.apply_advert(advert);
                    announce_holds(&outbound, &fallback, ChunkAnnounce::Acquire(advert));
                    cluster_datagram::publish_holder_directory(&directory);
                    server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                }
                let outcome = match served.export {
                    Some(export) => serve_chunk_export(local.0,
                        *server.cluster_peer_incarnations.load().get(&local).expect("local transfer incarnation"),
                        &outbound, served.peer, served.chunk, served.subscription, export),
                    None => ServeOutcome::Unavailable(served.chunk),
                };
                match outcome {
                    ServeOutcome::Enqueued => {}
                    ServeOutcome::Deferred(parcel) => queued_chunk_data.push_back((served.generation, parcel)),
                    ServeOutcome::Unavailable(chunk) => {
                        advertised.remove(&chunk);
                        directory.apply_drop(ChunkDrop { holder: local.0, chunk });
                        cluster_datagram::publish_holder_directory(&directory);
                        server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                        announce_holds(
                            &outbound,
                            &fallback,
                            ChunkAnnounce::Release(ChunkDrop { holder: local.0, chunk }),
                        );
                    }
                }
            }
            permit = outbound.reserve(), if !queued_requests.is_empty() => {
                let permit = permit.expect("cluster chunk request transport closed");
                if let Some((addr, peer, generation, parcel)) = queued_requests.pop_front()
                    && queued_chunk_request_current(&server.cluster_peer_generations.load(), &wanted, &pending, addr, peer, generation)
                {
                    permit.send(parcel);
                }
            }
            permit = chunk_inventory_tx.reserve(), if !deferred_installs.is_empty() => {
                let permit = permit.expect("cluster chunk inventory owner closed");
                if let Some(install) = deferred_installs.pop_front() {
                    let chunk = install.payload.chunk;
                    if server.cluster_peer_generations.load().get(&install.source).copied() == Some(install.source_generation)
                        && wanted.contains(&chunk)
                        && pending.get(&chunk).is_some_and(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { peer, .. } if peer == install.source.0))
                    {
                        permit.send(install.request);
                        spawn_installed_chunk_pipeline(&server, &chunk_inventory_tx, &installed_tx, install.source, install.source_generation, install.payload, install.chunk, install.world, install.reservation_generation, install.receive);
                    } else if let Some(generation) = install.reservation_generation {
                        let _ = install.world.level.try_release_peer_chunk_install(
                            Vector2::new(chunk.x(), chunk.z()), generation,
                        );
                    }
                }
            }
            installed = installed_rx.recv() => {
                let Some((peer, payload)) = installed else { break };
                let chunk = payload.chunk;
                complete_chunk_payload(local.0, &mut wanted, &mut pending, &mut pings, &mut advertised, &mut unroutable_reported, &fallback, &outbound, peer, payload);
                if advertised.contains(&chunk) {
                    directory.apply_advert(ChunkAdvert { holder: local.0, chunk });
                    cluster_datagram::publish_holder_directory(&directory);
                    server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                }
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            want = want_rx.recv() => {
                let Some(request) = want else { break };
                let addr = ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y);
                level_wanted.insert(addr);
                wanted.insert(addr);
                grace.remove(&addr);
                if request_chunk_once(
                    &mut acquisition_source,
                    &server.cluster_peer_generations.load(),
                    &mut directory,
                    &pings,
                    primary,
                    &outbound,
                    &mut pending,
                    &mut queued_requests,
                    addr,
                    request.preferred,
                    None,
                ) {
                    unroutable_reported.remove(&addr);
                }
                while let Ok(request) = want_rx.try_recv() {
                    let addr = ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y);
                    level_wanted.insert(addr);
                    wanted.insert(addr);
                    grace.remove(&addr);
                    if request_chunk_once(
                        &mut acquisition_source,
                        &server.cluster_peer_generations.load(),
                        &mut directory,
                        &pings,
                        primary,
                        &outbound,
                        &mut pending,
                        &mut queued_requests,
                        addr,
                        request.preferred,
                        None,
                    ) {
                        unroutable_reported.remove(&addr);
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            unwant = unwant_rx.recv() => {
                let Some(request) = unwant else { break };
                let addr = ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y);
                level_wanted.remove(&addr);
                if !owner_needs.contains_key(&addr) {
                    pending_disk_installs.remove(&addr);
                    pending_unclaimed_installs.retain(|(chunk, _), _| *chunk != addr);
                    reset_fetch_state(
                        &server, &mut wanted, &mut pending, &mut unroutable_reported, addr, false,
                    );
                }
                while let Ok(request) = unwant_rx.try_recv() {
                    let addr = ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y);
                    level_wanted.remove(&addr);
                    if !owner_needs.contains_key(&addr) {
                        pending_disk_installs.remove(&addr);
                        pending_unclaimed_installs.retain(|(chunk, _), _| *chunk != addr);
                        reset_fetch_state(
                            &server, &mut wanted, &mut pending, &mut unroutable_reported, addr, false,
                        );
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            logout = logout_rx.recv() => {
                let Some(entry) = logout else { break };
                for pos in &entry.chunks {
                    grace.insert(
                        ChunkKey::new(WorldId::new(entry.dimension, entry.world), pos.x, pos.y),
                        entry.expires_millis,
                    );
                }
            }
            reset = peer_reset_rx.recv() => {
                let Some(peer) = reset else { break };
                pending_disk_installs.retain(|_, install| install.source.0 != peer);
                pending_unclaimed_installs.retain(|_, install| install.source.0 != peer);
                queued_chunk_data.retain(|(_, parcel)| parcel.peer.0 != peer);
                queued_requests.retain(|(_, target, _, _)| *target != peer);
                serve_queue.retain(|request| request.destination.0 != peer);
                if primary == Some(peer) {
                    primary = None;
                }
                directory.drop_holder(peer);
                cluster_datagram::publish_holder_directory(&directory);
                server.cluster_chunk_directory.store(Arc::new(directory.clone()));
                let retry: Vec<ChunkKey> = pending
                    .iter_mut()
                    .filter_map(|(addr, acquisition)| acquisition.unavailable(peer).then_some(*addr))
                    .collect();
                for addr in retry {
                    if request_chunk_once(
                        &mut acquisition_source,
                        &server.cluster_peer_generations.load(),
                        &mut directory,
                        &pings,
                        primary,
                        &outbound,
                        &mut pending,
                        &mut queued_requests,
                        addr,
                        None,
                        Some(peer),
                    ) {
                        unroutable_reported.remove(&addr);
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            observed_primary = primary_peer_rx.recv() => {
                let Some(peer) = observed_primary else { break };
                if primary.is_none() || primary == Some(peer) {
                    primary = Some(peer);
                    for addr in wanted.iter().copied() {
                        if request_chunk_once(
                            &mut acquisition_source,
                            &server.cluster_peer_generations.load(),
                            &mut directory,
                            &pings,
                            primary,
                            &outbound,
                            &mut pending,
                            &mut queued_requests,
                            addr,
                            None,
                            None,
                        ) {
                            unroutable_reported.remove(&addr);
                        }
                    }
                } else if chunk_warn_cooldown_elapsed() {
                    warn!(known = primary, observed = peer, "multiple cluster primaries advertised");
                }
            }
            _ = sweep.tick() => {
                let now_millis = chunk_now_millis();
                retained_need_leases.retain(|(expires, _)| *expires > now_millis);
                release_expired_claims(local.0, &directory, &mut claims, &mut pending_owner_demands, now_millis);
                report_stuck_fetches(&mut pending, now_millis);
                report_unroutable_wants(
                    &wanted,
                    &pending,
                    &advertised,
                    &mut unroutable_reported,
                    primary.is_some(),
                );
                if !grace.is_empty() {
                    let expired: Vec<ChunkKey> = grace
                        .iter()
                        .filter_map(|(addr, deadline)| (*deadline <= now_millis).then_some(*addr))
                        .collect();
                    for addr in expired {
                        grace.remove(&addr);
                        reset_fetch_state(
                            &server,
                            &mut wanted,
                            &mut pending,
                            &mut unroutable_reported,
                            addr,
                            true,
                        );
                    }
                    CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
                }
            }
        }
        if acquisition_source.directory_dirty {
            cluster_datagram::publish_holder_directory(&directory);
            server
                .cluster_chunk_directory
                .store(Arc::new(directory.clone()));
            acquisition_source.directory_dirty = false;
        }
        CLUSTER_WANTED_CHUNKS.store(wanted.len() as u64, Ordering::Relaxed);
        while serve_inflight < 8 && queued_chunk_data.len() < 16_384 {
            let Ok(permit) = chunk_inventory_tx.try_reserve() else {
                break;
            };
            let Ok(respond) = serve_done_tx.clone().try_reserve_owned() else {
                break;
            };
            let Some(request) = serve_queue.pop_front() else {
                break;
            };
            serve_inflight += 1;
            let chunk = request.fetch.chunk;
            let export = super::cluster_world_owner::ChunkInventoryRequest::Export {
                chunk,
                respond: super::cluster_world_owner::ChunkExportReply::Serve {
                    peer: request.destination,
                    generation: request.generation,
                    chunk,
                    requested_cut: request.requested_cut,
                    subscription: request.subscription,
                    route: serve_done_tx.clone(),
                    respond: Some(respond),
                },
            };
            permit.send(export);
        }
    }
}

fn flush_queued_chunk_data(
    outbound: &mpsc::Sender<OutboundParcel>,
    queued: &mut VecDeque<(u64, OutboundParcel)>,
    generations: &HashMap<ServerId, u64>,
) {
    while let Some((generation, parcel)) = queued.pop_front() {
        if generations.get(&parcel.peer) != Some(&generation) {
            continue;
        }
        match outbound.try_send(parcel) {
            Ok(()) => {
                SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(parcel)) => {
                queued.push_front((generation, parcel));
                break;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("cluster chunk data transport closed");
            }
        }
    }
}

struct TargetedChunkRequest {
    request: pumpkin_cluster::chunks::ChunkSubscriptionRequest,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    transport_generation: u64,
}

struct ServeRequest {
    fetch: pumpkin_cluster::chunks::ChunkFetch,
    requested_cut: Option<TickStamp>,
    subscription: Option<pumpkin_cluster::chunks::ChunkSubscriptionRequest>,
    destination: ServerId,
    generation: u64,
}

enum ServeOutcome {
    Enqueued,
    Deferred(OutboundParcel),
    Unavailable(ChunkKey),
}

struct PreparedChunkInstall {
    source: ServerId,
    source_generation: u64,
    payload: ChunkPayload,
    chunk: pumpkin_world::level::SyncChunk,
    world: Arc<crate::world::World>,
    reservation_generation: Option<u64>,
    request: super::cluster_world_owner::ChunkInventoryRequest,
    receive: oneshot::Receiver<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
}

fn request_peer_install_claim(
    install: PreparedChunkInstall,
    claim_results: &mpsc::Sender<pumpkin_world::level::PeerChunkInstallClaimResult>,
    pending_claims: &mut HashMap<(ChunkKey, u64), PreparedChunkInstall>,
    pending_unclaimed: &mut HashMap<(ChunkKey, u64), PreparedChunkInstall>,
) {
    let Some(generation) = install.reservation_generation else {
        return;
    };
    let chunk = install.payload.chunk;
    if let Some(existing) = pending_claims.get_mut(&(chunk, generation)) {
        *existing = install;
        return;
    }
    if let Some(existing) = pending_unclaimed.get_mut(&(chunk, generation)) {
        *existing = install;
        return;
    }
    if install.world.level.try_claim_peer_chunk_install(
        Vector2::new(chunk.x(), chunk.z()),
        generation,
        claim_results.clone(),
    ) {
        pending_claims.insert((chunk, generation), install);
    } else {
        pending_unclaimed.insert((chunk, generation), install);
    }
}

fn serve_chunk_export(
    local: u16,
    incarnation: u64,
    outbound: &mpsc::Sender<OutboundParcel>,
    destination: ServerId,
    chunk: ChunkKey,
    subscription: Option<pumpkin_cluster::chunks::ChunkSubscriptionRequest>,
    export: super::cluster_world_owner::ChunkStateExport,
) -> ServeOutcome {
    let mut payload = export.into_payload(chunk, local);
    if let Some(request) = subscription {
        use sha2::Digest;
        let hash = sha2::Sha256::digest(&payload.snapshot).into();
        payload.subscription = Some(pumpkin_cluster::chunks::ChunkSubscriptionStart::new(
            request,
            local,
            incarnation,
            payload.snapshot_cut,
            hash,
        ));
    }
    if payload.validate_transfer_evidence().is_err() {
        if chunk_warn_cooldown_elapsed() {
            error!(
                chunk_x = chunk.x(),
                chunk_z = chunk.z(),
                "cluster chunk export lacks consistent action history"
            );
        }
        return ServeOutcome::Unavailable(chunk);
    }
    let Ok(bytes) = encode_payload(&payload) else {
        error!(
            chunk_x = chunk.x(),
            chunk_z = chunk.z(),
            "cluster chunk payload encode failed"
        );
        return ServeOutcome::Unavailable(chunk);
    };
    let parcel = OutboundParcel {
        peer: destination,
        header: StreamHeader::new(StreamKind::ChunkData, None),
        bytes,
    };
    match outbound.try_send(parcel) {
        Ok(()) => {
            SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
            trace!(
                target: "cluster_chunk",
                chunk_x = chunk.x(),
                chunk_z = chunk.z(),
                to = destination.0,
                "cluster chunk snapshot enqueued"
            );
            ServeOutcome::Enqueued
        }
        Err(mpsc::error::TrySendError::Full(value))
        | Err(mpsc::error::TrySendError::Closed(value)) => ServeOutcome::Deferred(value),
    }
}

fn reset_fetch_state(
    server: &Arc<Server>,
    wanted: &mut HashSet<ChunkKey>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    unroutable_reported: &mut HashSet<ChunkKey>,
    addr: ChunkKey,
    unpin: bool,
) {
    wanted.remove(&addr);
    pending.remove(&addr);
    unroutable_reported.remove(&addr);
    let pos = Vector2::new(addr.x(), addr.z());
    if let Some(world) = world_for_chunk(server, addr) {
        world.level.clear_cluster_fetch_wanted(&pos);
        if unpin {
            world.level.unpin_cluster_chunk(&pos);
        }
    }
}

fn chunk_now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0)
}

fn world_for_chunk(server: &Server, chunk: ChunkKey) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| {
            world.dimension.id == chunk.world.dimension
                && xxhash_rust::xxh64::xxh64(world.get_world_name().as_bytes(), 0)
                    == chunk.world.key
        })
        .cloned()
}

fn release_expired_claims(
    local: u16,
    directory: &Directory,
    claims: &mut PrimaryClaims,
    demands: &mut VecDeque<(ChunkKey, OwnerChunkDemand)>,
    now_millis: u64,
) {
    for addr in claims.take_expired_limit(
        now_millis,
        1_024_usize.saturating_sub(demands.len()).min(64),
    ) {
        if directory
            .holders_of(&addr)
            .is_some_and(|holders| holders.iter().any(|holder| *holder != local))
        {
            claims.note_requested(addr, now_millis);
            continue;
        }
        demands.push_back((
            addr,
            OwnerChunkDemand::ReleasePeerRequest {
                position: Vector2::new(addr.x(), addr.z()),
            },
        ));
    }
}

fn report_stuck_fetches(pending: &mut HashMap<ChunkKey, ChunkAcquisition>, now_millis: u64) {
    for (addr, acquisition) in pending {
        if let Some((from, waited_millis)) = acquisition.take_stuck_diagnostic(now_millis) {
            error!(
                chunk_x = addr.x(),
                chunk_z = addr.z(),
                from,
                waited_millis,
                "cluster chunk fetch stuck without chunk"
            );
        }
    }
}

fn inflight_fetches(pending: &HashMap<ChunkKey, ChunkAcquisition>) -> u64 {
    pending
        .values()
        .filter(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { .. }))
        .count() as u64
}

fn request_chunk_once(
    acquisition_source: &mut AcquisitionRequestBank,
    generations: &HashMap<ServerId, u64>,
    directory: &mut Directory,
    pings: &PingTracker,
    primary: Option<u16>,
    outbound: &mpsc::Sender<OutboundParcel>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    queued_requests: &mut VecDeque<(ChunkKey, u16, u64, OutboundParcel)>,
    addr: ChunkKey,
    preferred: Option<u16>,
    exclude: Option<u16>,
) -> bool {
    let acquisition = pending
        .entry(addr)
        .or_insert_with(|| ChunkAcquisition::new(addr));
    if let Some(peer) = exclude {
        acquisition.unavailable(peer);
    }
    let Some(fetch) = preferred
        .filter(|peer| Some(*peer) != exclude)
        .and_then(|peer| acquisition.request_from(peer, chunk_now_millis()))
        .or_else(|| {
            acquisition.request_excluding(directory, pings, primary, chunk_now_millis(), exclude)
        })
    else {
        return matches!(acquisition.state, AcquisitionState::Requested { .. });
    };
    let from = fetch.from;
    let Some(generation) = generations.get(&ServerId(from)).copied() else {
        acquisition.unavailable(from);
        return false;
    };
    if let Some(request) = acquisition_source.active.get(&addr).copied() {
        let Ok(bytes) = pumpkin_cluster::xfer::encode_subscription(
            &pumpkin_cluster::chunks::ChunkSubscriptionControl::Request(request),
        ) else {
            acquisition.unavailable(from);
            return false;
        };
        let parcel = OutboundParcel {
            peer: ServerId(from),
            header: StreamHeader::new(StreamKind::ChunkRequest, None),
            bytes,
        };
        match outbound.try_send(parcel) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(parcel)) => {
                queued_requests.push_back((addr, from, generation, parcel))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("cluster acquisition continuation transport closed")
            }
        }
        return true;
    }
    if !acquisition_source.acquire(directory, addr, ServerId(from)) {
        acquisition.unavailable(from);
        return false;
    }
    let _ = (outbound, queued_requests, generation);

    true
}

fn queued_chunk_request_current(
    generations: &HashMap<ServerId, u64>,
    wanted: &HashSet<ChunkKey>,
    pending: &HashMap<ChunkKey, ChunkAcquisition>,
    chunk: ChunkKey,
    peer: u16,
    generation: u64,
) -> bool {
    generations.get(&ServerId(peer)) == Some(&generation)
        && wanted.contains(&chunk)
        && pending.get(&chunk).is_some_and(|acquisition| {
            matches!(acquisition.state, AcquisitionState::Requested { peer: selected, .. } if selected == peer)
        })
}

fn report_unroutable_wants(
    wanted: &HashSet<ChunkKey>,
    pending: &HashMap<ChunkKey, ChunkAcquisition>,
    advertised: &HashSet<ChunkKey>,
    reported: &mut HashSet<ChunkKey>,
    primary_known: bool,
) {
    if !primary_known {
        reported.clear();
        return;
    }
    reported.retain(|addr| {
        wanted.contains(addr)
            && !advertised.contains(addr)
            && pending.get(addr).is_none_or(|acquisition| {
                matches!(acquisition.state, AcquisitionState::AwaitingRoute)
            })
    });
    let mut newly_unroutable = Vec::new();
    for addr in wanted {
        if advertised.contains(addr) {
            continue;
        }
        if pending.get(addr).is_some_and(|acquisition| {
            matches!(acquisition.state, AcquisitionState::Requested { .. })
        }) {
            continue;
        }
        if reported.insert(*addr) {
            newly_unroutable.push(*addr);
        }
    }
    if !newly_unroutable.is_empty() {
        let awaiting_route = newly_unroutable
            .iter()
            .filter(|addr| {
                pending.get(addr).is_some_and(|acquisition| {
                    matches!(acquisition.state, AcquisitionState::AwaitingRoute)
                })
            })
            .count();
        let without_acquisition = newly_unroutable
            .iter()
            .filter(|addr| !pending.contains_key(addr))
            .count();
        newly_unroutable.sort_unstable();
        error!(
            count = newly_unroutable.len(),
            wanted = wanted.len(),
            locally_advertised = advertised.len(),
            awaiting_route,
            without_acquisition,
            sample = ?newly_unroutable.iter().take(4).collect::<Vec<_>>(),
            "cluster chunks without routable holders"
        );
    }
}

fn announce_holds(
    outbound: &mpsc::Sender<OutboundParcel>,
    fallback: &[u16],
    announce: ChunkAnnounce,
) {
    let Ok(bytes) = encode_announce(&announce) else {
        return;
    };
    for peer in fallback {
        match outbound.try_send(OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::ChunkAdvert, None),
            bytes: bytes.clone(),
        }) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if chunk_warn_cooldown_elapsed() {
                    error!(peer, "cluster chunk advert queue full");
                }
            }
        }
    }
}

fn process_targeted_chunk_requests(
    server: &Server,
    local: ServerId,
    directory: &Directory,
    receipts: &pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger,
    pending: &mut VecDeque<TargetedChunkRequest>,
    claims: &mut PrimaryClaims,
    demands: &mut VecDeque<(ChunkKey, OwnerChunkDemand)>,
    serve_queue: &mut VecDeque<ServeRequest>,
) {
    let count = pending.len();
    for _ in 0..count {
        let Some(targeted) = pending.pop_front() else {
            break;
        };
        if server
            .cluster_peer_generations
            .load()
            .get(&targeted.source.server)
            != Some(&targeted.transport_generation)
            || server
                .cluster_peer_incarnations
                .load()
                .get(&targeted.source.server)
                != Some(&targeted.source.generation)
        {
            continue;
        }
        let request = targeted.request;
        if !directory.matches_subscription(request) {
            if !receipts.cut_closed(
                request.from_tick,
                &std::collections::BTreeSet::from([targeted.source]),
            ) {
                pending.push_back(targeted);
            }
            continue;
        }
        let retain =
            server.is_cluster_primary() && !claims.is_retained(&request.chunk, chunk_now_millis());
        if serve_queue.len() >= 8192 || retain && demands.len() >= 1024 {
            pending.push_front(targeted);
            break;
        }
        claims.note_requested(request.chunk, chunk_now_millis());
        if retain {
            demands.push_back((
                request.chunk,
                OwnerChunkDemand::RetainPeerRequest {
                    position: Vector2::new(request.chunk.x(), request.chunk.z()),
                },
            ));
        }
        serve_queue.push_back(ServeRequest {
            fetch: pumpkin_cluster::chunks::ChunkFetch {
                chunk: request.chunk,
                from: local.0,
            },
            requested_cut: Some(TickStamp(request.from_tick.0.wrapping_sub(1))),
            subscription: Some(request),
            destination: targeted.source.server,
            generation: targeted.transport_generation,
        });
    }
}

fn process_chunk_acquisitions(
    server: &Server,
    local: ServerId,
    directory: &mut Directory,
    receipts: &mut pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger,
    claims: &mut PrimaryClaims,
    demands: &mut VecDeque<(ChunkKey, OwnerChunkDemand)>,
    serve_queue: &mut VecDeque<ServeRequest>,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    owner: &mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
) -> bool {
    let mut processed = false;
    while let Some(numbered) = receipts.next_pending(source).copied() {
        match numbered.control {
            pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::Acquire {
                provider,
                request,
            } => {
                if directory.subscription_retired_or_superseded(request) {
                    if receipts.mark_processed(source, numbered.sequence).is_err() {
                        break;
                    }
                    processed = true;
                    continue;
                }
                let serves = provider.server == local
                    && provider.generation
                        == *server
                            .cluster_peer_incarnations
                            .load()
                            .get(&local)
                            .unwrap_or(&0)
                    && source.server != local;
                let retain = serves
                    && server.is_cluster_primary()
                    && !claims.is_retained(&request.chunk, chunk_now_millis());
                if serves && serve_queue.len() >= 8192 || retain && demands.len() >= 1024 {
                    break;
                }
                let transport_generation = if serves {
                    let Some(generation) = server
                        .cluster_peer_generations
                        .load()
                        .get(&source.server)
                        .copied()
                    else {
                        break;
                    };
                    Some(generation)
                } else {
                    None
                };
                let subscription = pumpkin_cluster::chunks::ChunkSubscription {
                    chunk: request.chunk,
                    subscriber: request.subscriber,
                    incarnation: request.incarnation,
                    subscription_id: request.subscription_id,
                    from_tick: request.from_tick,
                };
                if !directory.subscribe(subscription) {
                    break;
                }
                if let Some(generation) = transport_generation {
                    claims.note_requested(request.chunk, chunk_now_millis());
                    if retain {
                        demands.push_back((
                            request.chunk,
                            OwnerChunkDemand::RetainPeerRequest {
                                position: Vector2::new(request.chunk.x(), request.chunk.z()),
                            },
                        ));
                    }
                    serve_queue.push_back(ServeRequest {
                        fetch: pumpkin_cluster::chunks::ChunkFetch {
                            chunk: request.chunk,
                            from: local.0,
                        },
                        requested_cut: Some(TickStamp(request.from_tick.0.wrapping_sub(1))),
                        subscription: Some(request),
                        destination: source.server,
                        generation,
                    });
                }
            }
            pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::Retire {
                request,
                ..
            } => {
                directory.unsubscribe(request);
            }
            pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::HistoricalCut(_)
            | pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::ReleaseHistorical {
                ..
            } => {
                let _ = owner;
            }
        }
        if receipts.mark_processed(source, numbered.sequence).is_err() {
            break;
        }
        processed = true;
    }
    processed
}

async fn handle_chunk_parcel(
    acquisition_source: &mut AcquisitionRequestBank,
    acquisition_receipts: &mut pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger,
    expected_historical: &HashMap<
        (ChunkKey, u64),
        pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest,
    >,
    pending_actor_images: &mut VecDeque<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        pumpkin_cluster::actor_local_cut::ActorLocalChunkImage,
    )>,
    targeted_chunk_requests: &mut VecDeque<TargetedChunkRequest>,
    server: &Arc<Server>,
    local: u16,
    directory: &mut Directory,
    wanted: &mut HashSet<ChunkKey>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    queued_requests: &mut VecDeque<(ChunkKey, u16, u64, OutboundParcel)>,
    deferred_installs: &mut VecDeque<PreparedChunkInstall>,
    pending_disk_installs: &mut HashMap<ChunkKey, PreparedChunkInstall>,
    pending_claim_installs: &mut HashMap<(ChunkKey, u64), PreparedChunkInstall>,
    pending_unclaimed_installs: &mut HashMap<(ChunkKey, u64), PreparedChunkInstall>,
    claim_result_tx: &mpsc::Sender<pumpkin_world::level::PeerChunkInstallClaimResult>,
    pings: &mut PingTracker,
    claims: &mut PrimaryClaims,
    pending_owner_demands: &mut VecDeque<(ChunkKey, OwnerChunkDemand)>,
    unroutable_reported: &mut HashSet<ChunkKey>,
    primary: Option<u16>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    chunk_inventory_tx: &mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
    installed_tx: &mpsc::Sender<(ServerId, ChunkPayload)>,
    serve_queue: &mut VecDeque<ServeRequest>,
    parcel: InboundParcel,
) {
    if server
        .cluster_peer_generations
        .load()
        .get(&parcel.peer)
        .copied()
        != Some(parcel.generation)
    {
        return;
    }
    if parcel.header.kind == StreamKind::ChunkData && parcel.bytes.starts_with(b"ALC2") {
        let Some(generation) = server
            .cluster_peer_incarnations
            .load()
            .get(&parcel.peer)
            .copied()
        else {
            return;
        };
        let provider = pumpkin_cluster::realm_completion::PeerGeneration {
            server: parcel.peer,
            generation,
        };
        let Ok(image) = pumpkin_cluster::actor_local_cut::decode(&parcel.bytes, provider) else {
            return;
        };
        let requester = pumpkin_cluster::realm_completion::PeerGeneration {
            server: ServerId(local),
            generation: *server
                .cluster_peer_incarnations
                .load()
                .get(&ServerId(local))
                .unwrap_or(&0),
        };
        if image.requester != requester
            || expected_historical.get(&(image.request.chunk, image.request.lease))
                != Some(&image.request)
        {
            return;
        }
        pending_actor_images.push_back((provider, image));
        return;
    }
    if parcel.header.kind == StreamKind::ChunkAdvert {
        let Ok(announce) = decode_announce(&parcel.bytes) else {
            if chunk_warn_cooldown_elapsed() {
                warn!(
                    peer = parcel.peer.0,
                    bytes = parcel.bytes.len(),
                    "cluster chunk advert decode failed"
                );
            }
            return;
        };
        match announce {
            ChunkAnnounce::Acquire(advert) => {
                if advert.holder != parcel.peer.0 {
                    return;
                }
                directory.apply_advert(advert);
                if wanted.contains(&advert.chunk) {
                    if request_chunk_once(
                        acquisition_source,
                        &server.cluster_peer_generations.load(),
                        directory,
                        pings,
                        primary,
                        outbound,
                        pending,
                        queued_requests,
                        advert.chunk,
                        None,
                        None,
                    ) {
                        unroutable_reported.remove(&advert.chunk);
                    }
                }
            }
            ChunkAnnounce::Release(drop) => {
                if drop.holder != parcel.peer.0 {
                    return;
                }
                directory.apply_drop(drop);
                if wanted.contains(&drop.chunk)
                    && pending
                        .get_mut(&drop.chunk)
                        .is_some_and(|acquisition| acquisition.unavailable(drop.holder))
                {
                    if request_chunk_once(
                        acquisition_source,
                        &server.cluster_peer_generations.load(),
                        directory,
                        pings,
                        primary,
                        outbound,
                        pending,
                        queued_requests,
                        drop.chunk,
                        None,
                        Some(drop.holder),
                    ) {
                        unroutable_reported.remove(&drop.chunk);
                        if let Some(world) = world_for_chunk(server, drop.chunk) {
                            world.level.clear_cluster_fetch_wanted(&Vector2::new(
                                drop.chunk.x(),
                                drop.chunk.z(),
                            ));
                        }
                    }
                }
            }
        }
        return;
    }
    if parcel.header.kind == StreamKind::ChunkRequest {
        let authenticated = server
            .cluster_peer_incarnations
            .load()
            .get(&parcel.peer)
            .copied()
            .map(
                |generation| pumpkin_cluster::realm_completion::PeerGeneration {
                    server: parcel.peer,
                    generation,
                },
            );
        if parcel.bytes.starts_with(b"CAQ2") {
            if let Some(authenticated) = authenticated
                && let Ok(control) = pumpkin_cluster::acquisition_prefix::decode(&parcel.bytes)
                && acquisition_receipts.admit_source(authenticated).is_ok()
                && acquisition_receipts.receive(authenticated, control).is_ok()
            {
                process_chunk_acquisitions(
                    server,
                    ServerId(local),
                    directory,
                    acquisition_receipts,
                    claims,
                    pending_owner_demands,
                    serve_queue,
                    authenticated,
                    chunk_inventory_tx,
                );
            }
            return;
        }
        if parcel.bytes.starts_with(b"CAR2") {
            if let Some(authenticated) = authenticated
                && let Ok(roster) =
                    pumpkin_cluster::acquisition_prefix::decode_roster(&parcel.bytes)
                && roster.validate(authenticated).is_ok()
            {
                let local_generation = server
                    .cluster_peer_incarnations
                    .load()
                    .get(&ServerId(local))
                    .copied();
                let continued = roster
                    .active
                    .iter()
                    .filter_map(|numbered| {
                        let pumpkin_cluster::acquisition_prefix::ChunkAcquisitionControl::Acquire {
                            provider,
                            request,
                        } = numbered.control
                        else {
                            return None;
                        };
                        (provider.server == ServerId(local)
                            && Some(provider.generation) == local_generation
                            && authenticated.server != ServerId(local))
                        .then_some(TargetedChunkRequest {
                            request,
                            source: authenticated,
                            transport_generation: parcel.generation,
                        })
                    })
                    .collect::<Vec<_>>();
                let mut candidate_directory = directory.clone();
                let mut candidate_receipts = acquisition_receipts.clone();
                if targeted_chunk_requests
                    .len()
                    .saturating_add(continued.len())
                    <= 8192
                    && candidate_receipts.admit_source(authenticated).is_ok()
                    && candidate_directory
                        .replace_source_roster(authenticated, &roster)
                        .is_ok()
                    && candidate_receipts
                        .seed_processed_roster(authenticated, &roster)
                        .is_ok()
                {
                    *directory = candidate_directory;
                    *acquisition_receipts = candidate_receipts;
                    targeted_chunk_requests.extend(continued);
                    process_targeted_chunk_requests(
                        server,
                        ServerId(local),
                        directory,
                        acquisition_receipts,
                        targeted_chunk_requests,
                        claims,
                        pending_owner_demands,
                        serve_queue,
                    );
                    process_chunk_acquisitions(
                        server,
                        ServerId(local),
                        directory,
                        acquisition_receipts,
                        claims,
                        pending_owner_demands,
                        serve_queue,
                        authenticated,
                        chunk_inventory_tx,
                    );
                }
            }
            return;
        }
        if parcel.bytes.starts_with(b"CSB1") {
            if let Some(authenticated) = authenticated
                && let Ok(pumpkin_cluster::chunks::ChunkSubscriptionControl::Request(request)) =
                    pumpkin_cluster::xfer::decode_subscription(&parcel.bytes)
                && request.subscriber == authenticated.server.0
                && request.incarnation == authenticated.generation
                && request.subscription_id != 0
                && targeted_chunk_requests.len() < 8192
            {
                targeted_chunk_requests.push_back(TargetedChunkRequest {
                    request,
                    source: authenticated,
                    transport_generation: parcel.generation,
                });
                process_targeted_chunk_requests(
                    server,
                    ServerId(local),
                    directory,
                    acquisition_receipts,
                    targeted_chunk_requests,
                    claims,
                    pending_owner_demands,
                    serve_queue,
                );
            }
            return;
        }
        let Ok(fetch) = decode_request(&parcel.bytes) else {
            if chunk_warn_cooldown_elapsed() {
                warn!(
                    target: "cluster_chunk",
                    peer = parcel.peer.0,
                    bytes = parcel.bytes.len(),
                    "cluster chunk request decode failed"
                );
            }
            return;
        };
        trace!(
            target: "cluster_chunk",
            chunk_x = fetch.chunk.x(),
            chunk_z = fetch.chunk.z(),
            from = parcel.peer.0,
            "cluster chunk request received"
        );
        let now = chunk_now_millis();
        let retained = claims.is_retained(&fetch.chunk, now);
        claims.note_requested(fetch.chunk, now);
        if server.is_cluster_primary() && !retained {
            pending_owner_demands.push_back((
                fetch.chunk,
                OwnerChunkDemand::RetainPeerRequest {
                    position: Vector2::new(fetch.chunk.x(), fetch.chunk.z()),
                },
            ));
        }
        if serve_queue.len() >= 1_024 {
            announce_holds(
                outbound,
                fallback,
                ChunkAnnounce::Release(ChunkDrop {
                    holder: local,
                    chunk: fetch.chunk,
                }),
            );
            return;
        }
        serve_queue.push_back(ServeRequest {
            fetch,
            requested_cut: None,
            subscription: None,
            destination: parcel.peer,
            generation: parcel.generation,
        });
        return;
    }
    if parcel.header.kind != StreamKind::ChunkData {
        return;
    }
    if server
        .cluster_peer_generations
        .load()
        .get(&parcel.peer)
        .copied()
        != Some(parcel.generation)
    {
        return;
    }
    let Ok(payload) = decode_payload(&parcel.bytes) else {
        if chunk_warn_cooldown_elapsed() {
            warn!(
                peer = parcel.peer.0,
                bytes = parcel.bytes.len(),
                "cluster chunk payload decode failed"
            );
        }
        return;
    };
    if !wanted.contains(&payload.chunk)
        || !acquisition_source
            .active
            .get(&payload.chunk)
            .is_some_and(|request| {
                payload.snapshot_cut.0 == request.from_tick.0.wrapping_sub(1)
                    && payload.subscription.is_some_and(|start| {
                        start.subscription.chunk == request.chunk
                            && start.subscription.subscriber == request.subscriber
                            && start.subscription.incarnation == request.incarnation
                            && start.subscription.subscription_id == request.subscription_id
                            && start.subscription.from_tick == request.from_tick
                            && start.provider == parcel.peer.0
                            && server.cluster_peer_incarnations.load().get(&parcel.peer)
                                == Some(&start.provider_incarnation)
                    })
            })
    {
        return;
    }
    let Some(AcquisitionState::Requested {
        peer: selected,
        since_millis: _,
        ..
    }) = pending
        .get(&payload.chunk)
        .map(|acquisition| acquisition.state)
    else {
        return;
    };
    if selected != parcel.peer.0 || payload.holder != selected {
        if chunk_warn_cooldown_elapsed() {
            warn!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                selected,
                sender = parcel.peer.0,
                claimed_holder = payload.holder,
                "cluster chunk payload from unselected source"
            );
        }
        return;
    }
    if payload.validate_transfer_evidence().is_err() {
        if chunk_warn_cooldown_elapsed() {
            warn!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                peer = parcel.peer.0,
                "cluster chunk transfer evidence rejected"
            );
        }
        return;
    }
    let Some(world) = world_for_chunk(server, payload.chunk) else {
        return;
    };
    let Some((chunk, min_y, block_palettes, biome_palettes)) =
        pumpkin_world::level::cluster_decode_snapshot_with_blocks(
            payload.chunk.x(),
            payload.chunk.z(),
            &payload.snapshot,
        )
    else {
        error!(
            chunk_x = payload.chunk.x(),
            chunk_z = payload.chunk.z(),
            "cluster chunk snapshot decode failed"
        );
        return;
    };
    let blocks = super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
        min_y,
        block_palettes,
        biome_palettes,
    );
    let projected = payload.project_ground();
    let block_evidence_matches = projected.is_ok_and(|projected| {
        projected.into_iter().all(|(target, expected)| {
            let pumpkin_cluster::canonical_action::Target::Chunk {
                key,
                part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
                inventory: None,
            } = target
            else {
                return true;
            };
            matches!(
                expected,
                Some(pumpkin_cluster::canonical_action::Cell {
                    value: pumpkin_cluster::canonical_action::Value::Block(expected),
                    ..
                }) if blocks.block(key, position).is_ok_and(|actual| actual.as_u16() == expected)
            )
        })
    });
    if !block_evidence_matches {
        if chunk_warn_cooldown_elapsed() {
            warn!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                peer = parcel.peer.0,
                "cluster chunk ground differs from transferred action graph"
            );
        }
        return;
    }
    let (respond, receive) = oneshot::channel();
    let reservation = if server.is_cluster_primary() {
        world
            .level
            .cluster_publication_reservations
            .load()
            .get(&Vector2::new(payload.chunk.x(), payload.chunk.z()))
            .copied()
    } else {
        None
    };
    let reservation_generation = reservation.map(|reservation| reservation.generation);
    let install = PreparedChunkInstall {
        source: parcel.peer,
        source_generation: parcel.generation,
        request: super::cluster_world_owner::ChunkInventoryRequest::Install {
            chunk: payload.chunk,
            persisted_generation: None,
            peer_reservation_generation: reservation_generation,
            peer_source: Some((parcel.peer, parcel.generation)),
            genesis: payload.genesis.clone(),
            occupancy: payload.occupancy.clone(),
            image: std::sync::Arc::from(payload.snapshot.clone()),
            blocks,
            states: payload.inventory.clone(),
            pendings: payload.pendings.clone(),
            snapshot_cut: payload.snapshot_cut,
            graphs: payload.graphs.clone(),
            ground_anchors: payload.ground_anchors.clone(),
            completion: payload.completion.clone(),
            ground_frontier: payload.ground_frontier,
            player_inventory_frontiers: payload.player_inventory_frontiers.clone(),
            player_living: payload.player_living.clone(),
            player_avatar_absent: payload.player_avatar_absent.clone(),
            entity_bodies: payload.entity_bodies.clone(),
            entity_history: payload.entity_history.clone(),
            dependency_history: payload.dependency_history.clone(),
            item_spatial: payload.item_spatial.clone(),
            respond,
        },
        payload,
        chunk,
        world,
        reservation_generation,
        receive,
    };
    if server.is_cluster_primary()
        && reservation.is_none_or(|reservation| {
            matches!(
                reservation.state,
                pumpkin_world::level::PersistedChunkReservationState::Approved
                    | pumpkin_world::level::PersistedChunkReservationState::Lost
            )
        })
    {
        return;
    }
    if server.is_cluster_primary()
        && reservation.is_some_and(|reservation| {
            reservation.state == pumpkin_world::level::PersistedChunkReservationState::Pending
        })
    {
        pending_disk_installs.insert(install.payload.chunk, install);
        return;
    }
    if server.is_cluster_primary() {
        request_peer_install_claim(
            install,
            claim_result_tx,
            pending_claim_installs,
            pending_unclaimed_installs,
        );
        return;
    }
    if deferred_installs.is_empty() {
        let PreparedChunkInstall {
            source,
            source_generation,
            request,
            payload,
            chunk,
            world,
            reservation_generation,
            receive,
        } = install;
        match chunk_inventory_tx.try_send(request) {
            Ok(()) => {
                spawn_installed_chunk_pipeline(
                    server,
                    chunk_inventory_tx,
                    installed_tx,
                    source,
                    source_generation,
                    payload,
                    chunk,
                    world,
                    reservation_generation,
                    receive,
                );
                return;
            }
            Err(mpsc::error::TrySendError::Full(request)) => {
                deferred_installs.push_back(PreparedChunkInstall {
                    source,
                    source_generation,
                    request,
                    payload,
                    chunk,
                    world,
                    reservation_generation,
                    receive,
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("cluster chunk inventory owner closed");
            }
        }
    } else {
        deferred_installs.push_back(install);
    }
}

fn spawn_installed_chunk_pipeline(
    server: &Arc<Server>,
    chunk_inventory_tx: &mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
    installed_tx: &mpsc::Sender<(ServerId, ChunkPayload)>,
    source: ServerId,
    source_generation: u64,
    payload: ChunkPayload,
    chunk: pumpkin_world::level::SyncChunk,
    world: Arc<crate::world::World>,
    reservation_generation: Option<u64>,
    receive: oneshot::Receiver<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
) {
    let installed_tx = installed_tx.clone();
    let commit_tx = chunk_inventory_tx.clone();
    server.spawn_task(async move {
        let Ok(Ok(())) = receive.await else {
            if let Some(generation) = reservation_generation {
                let _ = world.level.try_release_peer_chunk_install(
                    Vector2::new(payload.chunk.x(), payload.chunk.z()),
                    generation,
                );
            }
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk inventory install rejected"
            );
            return;
        };
        let Ok(commit_permit) = commit_tx.try_reserve_owned() else {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk ready route saturated or closed"
            );
            return;
        };
        let Ok(installed_permit) = installed_tx.try_reserve_owned() else {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk installed-result route saturated or closed"
            );
            return;
        };
        let (publish_respond, publish_receive) = oneshot::channel();
        if !world.level.try_publish_cluster_snapshot(
            Vector2::new(payload.chunk.x(), payload.chunk.z()),
            chunk,
            reservation_generation,
            publish_respond,
        ) || !matches!(publish_receive.await, Ok(true))
        {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk scheduler publication failed"
            );
            return;
        }
        let (ready_respond, ready_receive) = oneshot::channel();
        commit_permit.send(
            super::cluster_world_owner::ChunkInventoryRequest::CommitReady {
                chunk: payload.chunk,
                identity: super::cluster_world_owner::ChunkAdmissionIdentity {
                    persisted_generation: None,
                    peer_reservation_generation: reservation_generation,
                    peer_source: Some((source, source_generation)),
                },
                respond: ready_respond,
            },
        );
        if !matches!(ready_receive.await, Ok(Ok(()))) {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk ready publication failed"
            );
            return;
        }
        installed_permit.send((source, payload));
    });
}

#[allow(clippy::too_many_arguments)]
fn complete_chunk_payload(
    local: u16,
    wanted: &mut HashSet<ChunkKey>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    pings: &mut PingTracker,
    advertised: &mut HashSet<ChunkKey>,
    unroutable_reported: &mut HashSet<ChunkKey>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    source: ServerId,
    payload: ChunkPayload,
) {
    if !wanted.contains(&payload.chunk) {
        return;
    }
    let Some(AcquisitionState::Requested {
        peer: selected,
        since_millis: requested_at,
        ..
    }) = pending.get(&payload.chunk).map(|request| request.state)
    else {
        return;
    };
    if selected != source.0 || payload.holder != selected {
        return;
    }
    pings.record(source.0, chunk_now_millis().saturating_sub(requested_at));
    wanted.remove(&payload.chunk);
    pending.remove(&payload.chunk);
    unroutable_reported.remove(&payload.chunk);
    FETCHED_CHUNKS.fetch_add(1, Ordering::Relaxed);
    if advertised.insert(payload.chunk) {
        announce_holds(
            outbound,
            fallback,
            ChunkAnnounce::Acquire(ChunkAdvert {
                holder: local,
                chunk: payload.chunk,
            }),
        );
    }
}

pub(crate) async fn save_primary_checkpoints(
    server: &Arc<Server>,
    request: u64,
    chunks: &[ChunkKey],
) -> Result<(), String> {
    if !server.is_cluster_primary() {
        return Ok(());
    }
    let route = server
        .cluster_chunk_inventory_route
        .load_full()
        .ok_or_else(|| "cluster chunk owner route is unavailable".to_owned())?;
    for &key in chunks {
        let (respond, receive) = oneshot::channel();
        route
            .try_send(super::cluster_world_owner::ChunkInventoryRequest::Export {
                chunk: key,
                respond: super::cluster_world_owner::ChunkExportReply::Checkpoint {
                    request,
                    respond: Some(respond),
                },
            })
            .map_err(|_| {
                format!(
                    "owner checkpoint export route full for {},{}",
                    key.x(),
                    key.z()
                )
            })?;
        let export = tokio::time::timeout(Duration::from_secs(5), receive)
            .await
            .map_err(|_| {
                format!(
                    "owner checkpoint export timed out for {},{}",
                    key.x(),
                    key.z()
                )
            })?
            .map_err(|_| format!("owner checkpoint export rejected {},{}", key.x(), key.z()))?;
        let frontier = export.ground_frontier.ok_or_else(|| {
            format!(
                "chunk {},{} has no proven accepted frontier",
                key.x(),
                key.z()
            )
        })?;
        let payload = export.into_payload(key, server.advanced_config.cluster.server_id);
        payload.validate_transfer_evidence().map_err(|error| {
            format!(
                "checkpoint action evidence invalid for {},{}: {error:?}",
                key.x(),
                key.z()
            )
        })?;
        let folder = world_for_chunk(server, key)
            .ok_or_else(|| format!("checkpoint world missing for {},{}", key.x(), key.z()))?
            .level
            .level_folder
            .dim_folder
            .clone();
        tokio::task::spawn_blocking(move || {
            super::cluster_checkpoint::save(
                &folder,
                &super::cluster_checkpoint::ChunkCheckpoint {
                    generation: frontier.epoch,
                    payload,
                },
            )
        })
        .await
        .map_err(|error| format!("checkpoint writer failed: {error}"))?
        .map_err(|error| format!("checkpoint write failed: {error}"))?;
    }
    Ok(())
}

async fn admin_task(
    server: Arc<Server>,
    local: ServerId,
    mut control_rx: mpsc::Receiver<InboundParcel>,
    local_rx: mpsc::Receiver<super::cluster_admin_apply::LocalAdminMutation>,
    seed_rx: mpsc::Receiver<ServerId>,
    _replayed_ids: Vec<[u8; 16]>,
) {
    super::cluster_admin_replica::spawn(&server, local, local_rx, seed_rx);
    let mut replica_pending = None;
    let mut policy_pending = None;
    let mut scoreboard_pending = None;
    let mut rules_pending = None;
    let mut gamerules_pending = None;
    let mut skin_pending = None;
    let mut command_pending = None;
    let mut command_observation_pending = None;
    let mut command_warnings = HashMap::<ServerId, std::time::Instant>::new();
    let mut command_warning_overflow = None;
    let mut flush = tokio::time::interval(Duration::from_millis(10));
    loop {
        let parcel = tokio::select! {
            parcel = control_rx.recv(), if scoreboard_pending.is_none() && policy_pending.is_none() && replica_pending.is_none() && rules_pending.is_none() && gamerules_pending.is_none() && skin_pending.is_none() && command_pending.is_none() && command_observation_pending.is_none() => parcel,
            _ = flush.tick() => {
                if let Some(parcel) = scoreboard_pending.take() {
                    scoreboard_pending = super::cluster_scoreboard::receive(&server, parcel).err();
                }
                if let Some(parcel) = policy_pending.take() {
                    policy_pending = super::cluster_access_policy::receive(&server, parcel).err();
                }
                if let Some(parcel) = replica_pending.take() {
                    replica_pending = super::cluster_admin_replica::receive(&server, parcel).err();
                }
                if let Some(parcel) = rules_pending.take() {
                    rules_pending = super::cluster_world_rules::receive(&server, parcel).err();
                }
                if let Some(parcel) = gamerules_pending.take() {
                    gamerules_pending = super::cluster_gamerules::receive(&server, parcel).err();
                }
                if let Some(seed) = skin_pending.take() {
                    match server.cluster_profile_skin_fragments.try_send(seed) {
                        Ok(()) => {},
                        Err(mpsc::error::TrySendError::Full(seed)) => skin_pending = Some(seed),
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                if let Some(command) = command_pending.take() {
                    command_pending = server.cluster_player_commands.receive(command).err();
                }
                if let Some(observation) = command_observation_pending.take() {
                    command_observation_pending = server.cluster_player_commands.retry_observation(observation).err();
                }
                continue;
            }
        };
        let Some(parcel) = parcel else {
            break;
        };
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        note_cluster_handshake(&parcel);
        if parcel.bytes.starts_with(super::cluster_scoreboard::PREFIX) {
            scoreboard_pending = super::cluster_scoreboard::receive(&server, parcel).err();
            continue;
        }
        if parcel
            .bytes
            .starts_with(super::cluster_access_policy::PREFIX)
        {
            policy_pending = super::cluster_access_policy::receive(&server, parcel).err();
            continue;
        }
        if parcel
            .bytes
            .starts_with(pumpkin_cluster::admin_replica::ADMIN_REPLICA_PREFIX)
        {
            replica_pending = super::cluster_admin_replica::receive(&server, parcel).err();
            continue;
        }
        if parcel.bytes.starts_with(super::cluster_gamerules::PREFIX) {
            gamerules_pending = super::cluster_gamerules::receive(&server, parcel).err();
            continue;
        }
        if parcel.bytes.starts_with(super::cluster_world_rules::PREFIX) {
            rules_pending = super::cluster_world_rules::receive(&server, parcel).err();
            continue;
        }
        if parcel
            .bytes
            .starts_with(super::cluster_profile_skins::PREFIX)
        {
            if let Some(seed) = super::cluster_profile_skins::decode_control(&server, &parcel) {
                match server.cluster_profile_skin_fragments.try_send(seed) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(seed)) => {
                        skin_pending = Some(seed)
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        error!("cluster profile skin owner stopped");
                        break;
                    }
                }
            }
            continue;
        }
        if let Some(batch) = pumpkin_cluster::player_command_control::decode_observer(&parcel.bytes)
        {
            let Some(incarnation) = server
                .cluster_peer_incarnations
                .load()
                .get(&parcel.peer)
                .copied()
            else {
                continue;
            };
            if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation)
            {
                continue;
            }
            let origin = pumpkin_cluster::realm_completion::PeerGeneration {
                server: parcel.peer,
                generation: incarnation,
            };
            command_observation_pending = server
                .cluster_player_commands
                .receive_observer(origin, parcel.generation, batch)
                .err();
            continue;
        }
        if let Some(request) = pumpkin_cluster::player_command_control::decode(&parcel.bytes) {
            if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation)
            {
                continue;
            }
            let origin = pumpkin_cluster::realm_completion::PeerGeneration {
                server: parcel.peer,
                generation: request.source_incarnation,
            };
            if let Err(reason) =
                super::cluster_player_commands::authorize(&server, origin, &request)
            {
                command_observation_pending = server
                    .cluster_player_commands
                    .record_input_failure(origin, request.correlation, reason)
                    .err();
            } else {
                command_pending = server
                    .cluster_player_commands
                    .receive(super::cluster_player_commands::AuthenticatedPlayerCommand {
                        origin,
                        transport_epoch: parcel.generation,
                        request,
                    })
                    .err();
            }
            continue;
        }
        if parcel
            .bytes
            .starts_with(pumpkin_cluster::player_command_control::PLAYER_COMMAND_PREFIX)
            || parcel.bytes.starts_with(b"PCObs001")
        {
            let now = std::time::Instant::now();
            if command_warnings
                .get(&parcel.peer)
                .or(command_warning_overflow.as_ref())
                .is_none_or(|last| now.duration_since(*last).as_secs() >= 300)
            {
                command_warnings.retain(|_, last| now.duration_since(*last).as_secs() < 300);
                if command_warnings.len() < 256 {
                    command_warnings.insert(parcel.peer, now);
                } else {
                    command_warning_overflow = Some(now);
                }
                warn!(
                    peer = parcel.peer.0,
                    generation = parcel.generation,
                    "invalid cluster player command control"
                );
            }
            continue;
        }
        let Ok(message) = decode_control_message(&parcel.bytes) else {
            continue;
        };
        if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation) {
            continue;
        }
        if let AdminControlMessage::Kick(request) = &message {
            if super::cluster_admin_apply::authorized_remote_kick(&server, parcel.peer, request) {
                super::cluster_admin_apply::deliver_remote_kick(&server, request);
            }
        }
    }
}

async fn accept_task(
    _server: Arc<Server>,
    mut accept_rx: mpsc::Receiver<InboundParcel>,
    completion_tx: mpsc::Sender<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        CompletionMessage,
    )>,
    drop_request: mpsc::Sender<(ServerId, u64)>,
) {
    let mut acceptor = Acceptor::new();
    let mut failed_peers = HashSet::new();
    let mut pending_drops = BTreeMap::new();
    loop {
        tokio::select! {
            permit = drop_request.reserve(), if !pending_drops.is_empty() => {
                let Ok(permit) = permit else { break };
                if let Some((peer, generation)) = pending_drops.pop_first() {
                    permit.send((peer, generation));
                }
            }
            parcel = accept_rx.recv() => {
                let Some(parcel) = parcel else {
                    break;
                };
                if parcel.header.kind != StreamKind::Accept {
                    continue;
                }
                if failed_peers.contains(&(parcel.peer, parcel.generation)) {
                    continue;
                }
                if let Ok(message) = decode_message(&parcel.bytes) {
                    match completion_tx.try_send((pumpkin_cluster::realm_completion::PeerGeneration {
                        server: parcel.peer,
                        generation: parcel.generation,
                    }, message)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            pending_drops.insert(parcel.peer, parcel.generation);
                            if failed_peers.insert((parcel.peer, parcel.generation)) {
                                error!(from = parcel.peer.0, generation = parcel.generation, "cluster completion queue full; dropping peer");
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                    continue;
                }
                match decode_accept(&parcel.bytes) {
                    Ok(batch) => {
                        trace!(
                            tick = batch.tick.0,
                            decisions = batch.len(),
                            "cluster accept batch applied"
                        );
                        acceptor.apply_accept(batch);
                        CLUSTER_PENDING_SYNC.store(
                            acceptor.pending_tick_stamps().len() as u64,
                            Ordering::Relaxed,
                        );
                    }
                    Err(_) => {
                        warn!("cluster accept batch decode failed");
                    }
                }
            }
        }
    }
}

async fn fuse_task(
    mut fused_rx: mpsc::Receiver<Vec<u8>>,
    outbound: mpsc::Sender<OutboundParcel>,
    drop_request: mpsc::Sender<ServerId>,
    peers: Vec<ServerId>,
) {
    let mut boundaries = PlayerBoundaryTracker::new();
    let mut pending_updates = VecDeque::new();
    let mut pending_boundaries = VecDeque::new();
    let mut source_closed = false;
    let mut pulse = tokio::time::interval(std::time::Duration::from_millis(10));
    loop {
        while let Some(parcel) = pending_boundaries.pop_front() {
            match outbound.try_send(parcel) {
                Ok(()) => {
                    FUSED_FORWARDED.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(parcel)) => {
                    pending_boundaries.push_front(parcel);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    for peer in &peers {
                        let _ = drop_request.try_send(*peer);
                    }
                    tracing::error!("cluster player boundary outbound closed");
                    return;
                }
            }
        }
        while pending_boundaries.len().saturating_add(peers.len()) <= 8_192 {
            let Some(update) = pending_updates.pop_front() else {
                break;
            };
            for routed in
                boundaries.route(&[update], cluster_datagram::holders_for_chunk, |movement| {
                    pumpkin_cluster::protocol::PlayerReplicaSnapshot {
                        movement,
                        visual: super::cluster_tick_pump::player_visual_snapshot(movement.gid),
                    }
                })
            {
                if !peers.contains(&routed.peer) {
                    continue;
                }
                let Ok(bytes) = encode_boundary(&routed.update) else {
                    continue;
                };
                pending_boundaries.push_back(OutboundParcel {
                    peer: routed.peer,
                    header: StreamHeader::new(StreamKind::PlayerBoundary, Some(routed.gid)),
                    bytes,
                });
            }
        }
        if source_closed && pending_updates.is_empty() && pending_boundaries.is_empty() {
            return;
        }
        tokio::select! {
            next = fused_rx.recv(), if pending_updates.is_empty() && !source_closed => {
                match next {
                    Some(batch) => {
                        match decode_batch(&batch) {
                            Ok(decoded) => {
                                pending_updates.extend(decoded.pos);
                                cluster_datagram::forward_fused_batch(&batch);
                            }
                            Err(error) => warn!(%error, "cluster fused movement batch decode failed"),
                        }
                    }
                    None => source_closed = true,
                }
            }
            _ = pulse.tick() => {}
        }
    }
}

#[cfg(test)]
mod discipline_tests {
    use super::*;
    use pumpkin_cluster::ntp::{publish_shared_offset, withdraw_shared_offset};

    #[test]
    fn unavailable_chunk_demand_does_not_block_other_worlds_or_reorder_its_lease() {
        let unavailable = ChunkKey::new(WorldId::new(0, 1), 1, 2);
        let available = ChunkKey::new(WorldId::new(0, 2), 3, 4);
        let position = |chunk: ChunkKey| Vector2::new(chunk.x(), chunk.z());
        let mut pending = VecDeque::from([
            (
                unavailable,
                OwnerChunkDemand::Acquire {
                    position: position(unavailable),
                    lease: 8,
                    ready: None,
                },
            ),
            (
                unavailable,
                OwnerChunkDemand::Release {
                    position: position(unavailable),
                    lease: 8,
                },
            ),
            (
                available,
                OwnerChunkDemand::Acquire {
                    position: position(available),
                    lease: 9,
                    ready: None,
                },
            ),
        ]);
        let (available_tx, available_rx) = crossbeam::channel::bounded(2);
        let available_tx = Arc::new(available_tx);
        flush_owner_chunk_demands(&mut pending, |chunk| {
            (chunk == available).then(|| (available_tx.clone(), None))
        });
        assert_eq!(pending.len(), 2);
        assert!(matches!(
            available_rx.try_recv(),
            Ok(OwnerChunkDemand::Acquire { lease: 9, .. })
        ));

        let (recovered_tx, recovered_rx) = crossbeam::channel::bounded(2);
        let recovered_tx = Arc::new(recovered_tx);
        flush_owner_chunk_demands(&mut pending, |chunk| {
            (chunk == unavailable).then(|| (recovered_tx.clone(), None))
        });
        assert!(pending.is_empty());
        assert!(matches!(
            recovered_rx.try_recv(),
            Ok(OwnerChunkDemand::Acquire { lease: 8, .. })
        ));
        assert!(matches!(
            recovered_rx.try_recv(),
            Ok(OwnerChunkDemand::Release { lease: 8, .. })
        ));
    }

    #[test]
    fn queued_chunk_export_cannot_cross_peer_replacement() {
        let (outbound, mut receive) = mpsc::channel(1);
        let mut queued = VecDeque::from([(
            8,
            OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader::new(StreamKind::ChunkData, None),
                bytes: vec![4, 5, 6],
            },
        )]);
        flush_queued_chunk_data(&outbound, &mut queued, &HashMap::from([(ServerId(2), 9)]));
        assert!(queued.is_empty());
        assert!(receive.try_recv().is_err());
    }

    #[test]
    fn full_chunk_data_transport_keeps_the_exact_parcel_until_capacity_returns() {
        let (outbound, mut receiver) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(1),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![1],
            })
            .unwrap();
        let mut queued = VecDeque::from([(
            9,
            OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader::new(StreamKind::ChunkData, None),
                bytes: vec![4, 5, 6],
            },
        )]);
        let generations = HashMap::from([(ServerId(2), 9)]);
        flush_queued_chunk_data(&outbound, &mut queued, &generations);
        assert_eq!(queued.len(), 1);
        assert_eq!(receiver.try_recv().unwrap().bytes, vec![1]);
        flush_queued_chunk_data(&outbound, &mut queued, &generations);
        assert!(queued.is_empty());
        let delivered = receiver.try_recv().unwrap();
        assert_eq!(delivered.peer, ServerId(2));
        assert_eq!(delivered.header.kind, StreamKind::ChunkData);
        assert_eq!(delivered.bytes, vec![4, 5, 6]);
    }

    #[test]
    fn held_chunk_does_not_report_as_unroutable_during_duplicate_want() {
        let held = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let waiting = ChunkKey::new(WorldId::new(0, 1), 1, 0);
        let wanted = HashSet::from([held, waiting]);
        let pending = HashMap::from([(waiting, ChunkAcquisition::new(waiting))]);
        let advertised = HashSet::from([held]);
        let mut reported = HashSet::new();
        report_unroutable_wants(&wanted, &pending, &advertised, &mut reported, true);
        assert_eq!(reported, HashSet::from([waiting]));
        report_unroutable_wants(&wanted, &pending, &advertised, &mut reported, true);
        assert_eq!(reported, HashSet::from([waiting]));
    }

    #[test]
    fn saturated_transport_retains_one_chunk_acquisition() {
        let generations = HashMap::from([(ServerId(1), 7)]);
        let (outbound, _receiver) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(1),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![1],
            })
            .expect("fill transport");
        let chunk = ChunkKey::new(WorldId::new(0, 7), 2, 3);
        let mut pending = HashMap::new();
        let mut queued = VecDeque::new();
        let (events, _events_rx) = mpsc::channel(8);
        let mut acquisition_source = AcquisitionRequestBank {
            source: pumpkin_cluster::realm_completion::PeerGeneration {
                server: ServerId(2),
                generation: 8,
            },
            providers: generations.clone(),
            next_subscription: 1,
            tick: Some(TickStamp(1)),
            pending: VecDeque::new(),
            active: HashMap::new(),
            directory_dirty: false,
            events,
        };
        let mut directory = Directory::new();
        let pings = PingTracker::new();
        assert!(request_chunk_once(
            &mut acquisition_source,
            &generations,
            &mut directory,
            &pings,
            Some(1),
            &outbound,
            &mut pending,
            &mut queued,
            chunk,
            None,
            None,
        ));
        assert!(matches!(
            pending.get(&chunk).map(|acquisition| acquisition.state),
            Some(AcquisitionState::Requested { peer: 1, .. })
        ));
        assert_eq!(acquisition_source.pending.len(), 1);
        assert!(request_chunk_once(
            &mut acquisition_source,
            &generations,
            &mut directory,
            &pings,
            Some(1),
            &outbound,
            &mut pending,
            &mut queued,
            chunk,
            None,
            None,
        ));
        assert_eq!(acquisition_source.pending.len(), 1);
        assert!(queued.is_empty());
        assert!(directory.matches_subscription(*acquisition_source.active.get(&chunk).unwrap()));
        let wanted = HashSet::from([chunk]);
        assert!(queued_chunk_request_current(
            &generations,
            &wanted,
            &pending,
            chunk,
            1,
            7
        ));
        assert!(!queued_chunk_request_current(
            &HashMap::from([(ServerId(1), 8)]),
            &wanted,
            &pending,
            chunk,
            1,
            7
        ));
        assert!(!queued_chunk_request_current(
            &generations,
            &HashSet::new(),
            &pending,
            chunk,
            1,
            7
        ));
        pending.get_mut(&chunk).unwrap().unavailable(1);
        assert!(!queued_chunk_request_current(
            &generations,
            &wanted,
            &pending,
            chunk,
            1,
            7
        ));
    }

    #[test]
    fn undisciplined_stamp_falls_back_and_healthy_sample_applies() {
        withdraw_shared_offset();
        let fallbacks = ntp_undisciplined_fallbacks();
        assert_eq!(disciplined_offset_millis(), 0);
        assert_eq!(ntp_undisciplined_fallbacks(), fallbacks + 1);
        assert!(!ntp_disciplined());
        publish_shared_offset(40);
        assert!(ntp_disciplined());
        assert_eq!(disciplined_offset_millis(), 40);
        assert_eq!(
            disciplined_tick_stamp(1000),
            TickStamp::from_disciplined_millis(1000, 40)
        );
        withdraw_shared_offset();
        assert!(!ntp_disciplined());
        assert_eq!(disciplined_offset_millis(), 0);
        withdraw_shared_offset();
        assert!(!ntp_disciplined());
    }
}

#[cfg(test)]
mod chunk_need_capacity_tests {
    use super::*;

    #[test]
    fn thousand_live_grace_promotion_and_retirement_owners_keep_release_reservations() {
        let capacity = chunk_need_route_capacity(&[(true,1000),(false,1000)]);
        let (sender,_receiver) = mpsc::channel::<()>(capacity);
        let mut ownership = Vec::new();
        for _ in 0..4000 { ownership.push(sender.clone().try_reserve_owned().unwrap()); }
        assert!(sender.capacity() >= 16384+512);
        drop(ownership);
        assert_eq!(sender.capacity(),capacity);
        assert_eq!(chunk_need_route_capacity(&[(true,0),(true,0)]),chunk_need_route_capacity(&[(true,65536)]));
    }
}
