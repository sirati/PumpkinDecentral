use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use pumpkin_cluster::accept::{Acceptor, decode_accept};
use pumpkin_cluster::admin_sync::{
    AdminAuthority, AdminControlMessage, AdminSnapshotAssembler, decode_control_message,
    encode_control_message,
};
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
    encode_request,
};
use pumpkin_config::{ClusterConfig, ClusterRole};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::level::{
    ClusterFetchRequest, ClusterLogoutGrace, cluster_encode_snapshot, set_cluster_fetch_sender,
    set_cluster_has_peers, set_cluster_logout_sender, set_cluster_secondary,
    set_cluster_unwant_sender,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};

use super::Server;
use super::cluster_entity_apply::spawn_entity_apply;
use super::cluster_regions;
use super::cluster_transient::spawn_transient_apply;
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
static TICK_FINISH_DROPPED: AtomicU64 = AtomicU64::new(0);
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

pub(crate) struct ChunkNeedLease {
    level: Arc<pumpkin_world::level::Level>,
    positions: Vec<Vector2<i32>>,
}

pub(crate) fn require_lobby_chunks(
    world: &Arc<crate::world::World>,
    required: &[Vector2<i32>],
) -> ChunkNeedLease {
    let mut positions = required.to_vec();
    positions.sort_unstable_by_key(|position| (position.x, position.y));
    positions.dedup();
    world.level.acquire_cluster_lobby_need(&positions);
    ChunkNeedLease {
        level: world.level.clone(),
        positions,
    }
}

impl Drop for ChunkNeedLease {
    fn drop(&mut self) {
        self.level.release_cluster_lobby_need(&self.positions);
    }
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
    server.note_cluster_health_policy_admitted(peer);
    let admin_seeded = super::cluster_admin_apply::publish_admin_snapshot_to_peer(server, peer);
    super::cluster_presence::publish_roster_to_peer(server, peer);
    if matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
        server.publish_cluster_health_policy_to(peer);
    }
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
    let mut handoff_events = VecDeque::new();
    let mut pending_handshakes = VecDeque::new();
    let mut pending_admin_seeds = HashSet::new();
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
                let Some(input) = input else {
                    break;
                };
                lifecycle.apply(input)
            }
            changed = peer_state.changed() => {
                if changed.is_err() {
                    continue;
                }
                let state = Arc::clone(&peer_state.borrow_and_update());
                server.cluster_connected_peers.store(Arc::new(state.connected.clone()));
                server.cluster_peer_generations.store(Arc::new(state.drop_epoch.clone()));
                server.cluster_primary_peers.rcu(|current| {
                    current
                        .intersection(&state.connected)
                        .copied()
                        .collect::<HashSet<_>>()
                });
                cluster_datagram::publish_peer_datagram_limits(state.datagram_limits.clone());
                let mut effects = Vec::new();
                let mut restarted_peers = HashSet::new();
                for (&peer, &epoch) in &state.drop_epoch {
                    let previous = observed_drops.get(&peer).copied().unwrap_or(0);
                    for _ in previous..epoch {
                        restarted_peers.insert(peer);
                        server.note_cluster_health_policy_disconnected(peer.0);
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
                    server.note_cluster_health_policy_connected(peer);
                    if role == ClusterNodeRole::Primary {
                        server.cluster_primary_peers.rcu(|current| {
                            let mut next = (**current).clone();
                            next.insert(ServerId(peer));
                            next
                        });
                        let _ = primary_peer.try_send(peer);
                        server.note_cluster_health_policy_primary(peer);
                        handoff_events.push_back(
                            super::cluster_handoff::HandoffCommand::PrimaryConnected(ServerId(
                                peer,
                            )),
                        );
                    }
                }
                LifecycleEffect::PeerAuthenticated { peer } => {
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
                    info!(server_id = local.0, candidate, "cluster peer joined");
                    if candidate == local.0 {
                        server.note_cluster_health_policy_local_admitted();
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
                        server.note_cluster_health_policy_disconnected(peer);
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
    }
}

pub fn note_cluster_handshake(parcel: &InboundParcel) {
    let Some(input) = LIFECYCLE_IN.get() else {
        return;
    };
    let Ok(message) = decode_handshake(&parcel.bytes) else {
        if let Ok(request) = postcard::from_bytes::<EntityHandoffRequest>(&parcel.bytes) {
            let _ = input.try_send(LifecycleInput::RemoteHandoff(request.clone()));
        }
        return;
    };
    trace!(peer = parcel.peer.0, generation = parcel.generation, message = ?message, "cluster handshake inbound");
    let next = match message {
        JoinHandshake::Hello(hello) => LifecycleInput::RemoteHello {
            hello,
            pin_match: true,
        },
        JoinHandshake::VoteRequest(request) => LifecycleInput::RemoteVoteRequest(request),
        JoinHandshake::Vote(vote) => LifecycleInput::RemoteVote(vote),
        JoinHandshake::Admit(admit) => LifecycleInput::RemoteAdmit(admit),
        JoinHandshake::PeerView(view) => LifecycleInput::RemotePeerView(view),
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
    let datagram_in_rx = mesh_channels.datagram_in_rx;
    cluster_datagram::install_datagram_outbox(peer_ids.clone(), mesh_channels.datagram_out_tx);
    super::cluster_world_delta::install_world_delta_outbox(peer_ids.clone(), outbound_tx.clone());
    super::cluster_ghost::spawn_ghost_apply(&server, local, datagram_in_rx);
    server.spawn_task(run_demux(
        inbound_rx,
        control_rx,
        demux_senders,
        StreamRegistry::new(),
    ));

    spawn_transient_apply(&server, local, demux_receivers.transient);

    spawn_visual_apply(&server, local, demux_receivers.visual);

    let (completion_tx, completion_rx) = mpsc::channel::<(ServerId, CompletionMessage)>(1024);
    let (local_tick_tx, local_tick_rx) = mpsc::channel::<LocalTickBatch>(64);
    let (actor_drain_tx, actor_drain_rx) = mpsc::channel(256);
    let (chunk_inventory_tx, chunk_inventory_rx) = mpsc::channel(128);
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

    let (peer_reset_tx, peer_reset_rx) = mpsc::channel::<u16>(64);
    let (primary_peer_tx, primary_peer_rx) = mpsc::channel::<u16>(8);
    let (fetch_tx, fetch_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_fetch_sender(fetch_tx);
    let (unwant_tx, unwant_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_unwant_sender(unwant_tx);
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
        fetch_rx,
        unwant_rx,
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
    super::cluster_world_time::install_world_time_outbox(fallback.clone(), outbound_tx.clone());
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

async fn chunk_task(
    server: Arc<Server>,
    local: ServerId,
    fallback: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut chunk_rx: mpsc::Receiver<InboundParcel>,
    mut want_rx: mpsc::Receiver<ClusterFetchRequest>,
    mut unwant_rx: mpsc::Receiver<ClusterFetchRequest>,
    mut logout_rx: mpsc::Receiver<ClusterLogoutGrace>,
    mut peer_reset_rx: mpsc::Receiver<u16>,
    mut primary_peer_rx: mpsc::Receiver<u16>,
    chunk_inventory_tx: mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
) {
    let (installed_tx, mut installed_rx) = mpsc::channel::<(ServerId, ChunkPayload)>(128);
    let (serve_done_tx, mut serve_done_rx) = mpsc::channel::<ServeOutcome>(16);
    let mut serve_queue = VecDeque::<ServeRequest>::new();
    let mut queued_chunk_data = VecDeque::<OutboundParcel>::new();
    let mut serve_inflight = 0_usize;
    let mut directory = Directory::new();
    let mut wanted: HashSet<ChunkKey> = HashSet::new();
    let mut pending: HashMap<ChunkKey, ChunkAcquisition> = HashMap::new();
    let mut queued_requests = VecDeque::<(ChunkKey, u16, OutboundParcel)>::new();
    let mut deferred_installs = VecDeque::<PreparedChunkInstall>::new();
    let mut pings = PingTracker::new();
    let mut advertised: HashSet<ChunkKey> = HashSet::new();
    let mut claims = PrimaryClaims::new();
    let mut unroutable_reported: HashSet<ChunkKey> = HashSet::new();
    let mut grace: HashMap<ChunkKey, u64> = HashMap::new();
    let mut primary = None;
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    loop {
        flush_queued_chunk_data(&outbound, &mut queued_chunk_data);
        tokio::select! {
            incoming = chunk_rx.recv(), if deferred_installs.len() < 256 && serve_queue.len() < 8_192 => {
                let Some(parcel) = incoming else { break };
                handle_chunk_parcel(&server, local.0, &mut directory, &mut wanted, &mut pending, &mut queued_requests, &mut deferred_installs, &mut pings, &mut claims, &mut unroutable_reported, primary, &fallback, &outbound, &chunk_inventory_tx, &installed_tx, &mut serve_queue, parcel).await;
                cluster_datagram::publish_holder_directory(&directory);
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            served = serve_done_rx.recv() => {
                let Some(served) = served else { break };
                serve_inflight = serve_inflight.saturating_sub(1);
                match served {
                    ServeOutcome::Enqueued => {}
                    ServeOutcome::Deferred(parcel) => queued_chunk_data.push_back(parcel),
                    ServeOutcome::Unavailable(chunk) => {
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
                if let Some((addr, peer, parcel)) = queued_requests.pop_front()
                    && wanted.contains(&addr)
                    && pending.get(&addr).is_some_and(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { peer: selected, .. } if selected == peer))
                {
                    permit.send(parcel);
                }
            }
            permit = chunk_inventory_tx.reserve(), if !deferred_installs.is_empty() => {
                let permit = permit.expect("cluster chunk inventory owner closed");
                if let Some(install) = deferred_installs.pop_front() {
                    let chunk = install.payload.chunk;
                    if wanted.contains(&chunk)
                        && pending.get(&chunk).is_some_and(|acquisition| matches!(acquisition.state, AcquisitionState::Requested { peer, .. } if peer == install.source.0))
                    {
                        permit.send(install.request);
                        spawn_installed_chunk_pipeline(&server, &chunk_inventory_tx, &installed_tx, install.source, install.payload, install.chunk, install.world, install.receive);
                    }
                }
            }
            installed = installed_rx.recv() => {
                let Some((peer, payload)) = installed else { break };
                complete_chunk_payload(local.0, &mut wanted, &mut pending, &mut pings, &mut advertised, &mut unroutable_reported, &fallback, &outbound, peer, payload);
                CLUSTER_INFLIGHT_FETCH.store(inflight_fetches(&pending), Ordering::Relaxed);
            }
            want = want_rx.recv() => {
                let Some(request) = want else { break };
                let addr = ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y);
                wanted.insert(addr);
                grace.remove(&addr);
                if request_chunk_once(
                    &directory,
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
                    wanted.insert(addr);
                    grace.remove(&addr);
                    if request_chunk_once(
                        &directory,
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
                reset_fetch_state(
                    &server,
                    &mut wanted,
                    &mut pending,
                    &mut unroutable_reported,
                    ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y),
                    false,
                );
                while let Ok(request) = unwant_rx.try_recv() {
                    reset_fetch_state(
                        &server,
                        &mut wanted,
                        &mut pending,
                        &mut unroutable_reported,
                        ChunkKey::new(WorldId::new(request.dimension, request.world), request.pos.x, request.pos.y),
                        false,
                    );
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
                queued_chunk_data.retain(|parcel| parcel.peer.0 != peer);
                if primary == Some(peer) {
                    primary = None;
                }
                directory.drop_holder(peer);
                cluster_datagram::publish_holder_directory(&directory);
                let retry: Vec<ChunkKey> = pending
                    .iter_mut()
                    .filter_map(|(addr, acquisition)| acquisition.unavailable(peer).then_some(*addr))
                    .collect();
                for addr in retry {
                    if request_chunk_once(
                        &directory,
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
                            &directory,
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
                release_expired_claims(&server, &directory, &mut claims, now_millis);
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
        CLUSTER_WANTED_CHUNKS.store(wanted.len() as u64, Ordering::Relaxed);
        while serve_inflight < 8 && queued_chunk_data.len() < 16_384 {
            let Some(request) = serve_queue.pop_front() else {
                break;
            };
            serve_inflight += 1;
            spawn_chunk_serve(
                &server,
                local.0,
                outbound.clone(),
                chunk_inventory_tx.clone(),
                serve_done_tx.clone(),
                request,
            );
        }
    }
}

fn flush_queued_chunk_data(
    outbound: &mpsc::Sender<OutboundParcel>,
    queued: &mut VecDeque<OutboundParcel>,
) {
    while let Some(parcel) = queued.pop_front() {
        match outbound.try_send(parcel) {
            Ok(()) => {
                SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(parcel)) => {
                queued.push_front(parcel);
                break;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("cluster chunk data transport closed");
            }
        }
    }
}

struct ServeRequest {
    fetch: pumpkin_cluster::chunks::ChunkFetch,
    destination: ServerId,
    known_holders: Vec<u16>,
}

enum ServeOutcome {
    Enqueued,
    Deferred(OutboundParcel),
    Unavailable(ChunkKey),
}

struct PreparedChunkInstall {
    source: ServerId,
    payload: ChunkPayload,
    chunk: pumpkin_world::level::SyncChunk,
    world: Arc<crate::world::World>,
    request: super::cluster_world_owner::ChunkInventoryRequest,
    receive: oneshot::Receiver<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
}

fn spawn_chunk_serve(
    server: &Arc<Server>,
    local: u16,
    outbound: mpsc::Sender<OutboundParcel>,
    chunk_inventory_tx: mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
    done_tx: mpsc::Sender<ServeOutcome>,
    request: ServeRequest,
) {
    let task_server = server.clone();
    server.spawn_task(async move {
        let result =
            serve_chunk_request(&task_server, local, &outbound, &chunk_inventory_tx, request).await;
        if done_tx.try_send(result).is_err() {
            panic!("cluster chunk serve result capacity reserved by worker limit");
        }
    });
}

async fn serve_chunk_request(
    server: &Arc<Server>,
    local: u16,
    outbound: &mpsc::Sender<OutboundParcel>,
    chunk_inventory_tx: &mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
    request: ServeRequest,
) -> ServeOutcome {
    let fetch = request.fetch;
    let Some((snapshot, loaded, blocks)) = snapshot_chunk(server, fetch.chunk).await else {
        return ServeOutcome::Unavailable(fetch.chunk);
    };
    trace!(
        target: "cluster_chunk",
        chunk_x = fetch.chunk.x(),
        chunk_z = fetch.chunk.z(),
        to = request.destination.0,
        loaded,
        "cluster chunk snapshot ready"
    );
    let (respond, receive) = oneshot::channel();
    if chunk_inventory_tx
        .try_send(super::cluster_world_owner::ChunkInventoryRequest::Export {
            chunk: fetch.chunk,
            blocks,
            respond,
        })
        .is_err()
    {
        error!(
            chunk_x = fetch.chunk.x(),
            chunk_z = fetch.chunk.z(),
            "cluster chunk inventory export route full"
        );
        return ServeOutcome::Unavailable(fetch.chunk);
    }
    let Ok(Ok(export)) = tokio::time::timeout(Duration::from_secs(5), receive).await else {
        error!(
            chunk_x = fetch.chunk.x(),
            chunk_z = fetch.chunk.z(),
            "cluster chunk inventory export did not complete"
        );
        return ServeOutcome::Unavailable(fetch.chunk);
    };
    let payload = ChunkPayload::new(
        fetch.chunk,
        local,
        snapshot,
        export.pendings,
        request.known_holders,
    )
    .with_inventory(export.inventory);
    let Ok(bytes) = encode_payload(&payload) else {
        error!(
            chunk_x = fetch.chunk.x(),
            chunk_z = fetch.chunk.z(),
            "cluster chunk payload encode failed"
        );
        return ServeOutcome::Unavailable(fetch.chunk);
    };
    let parcel = OutboundParcel {
        peer: request.destination,
        header: StreamHeader::new(StreamKind::ChunkData, None),
        bytes,
    };
    match outbound.try_send(parcel) {
        Ok(()) => {
            SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
            trace!(
                target: "cluster_chunk",
                chunk_x = fetch.chunk.x(),
                chunk_z = fetch.chunk.z(),
                to = request.destination.0,
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
    server: &Arc<Server>,
    directory: &Directory,
    claims: &mut PrimaryClaims,
    now_millis: u64,
) {
    for addr in claims.take_expired(now_millis) {
        if directory
            .holders_of(&addr)
            .is_some_and(|holders| !holders.is_empty())
        {
            claims.note_requested(addr, now_millis);
            continue;
        }
        let pos = Vector2::new(addr.x(), addr.z());
        if let Some(world) = world_for_chunk(server, addr) {
            world.level.unpin_cluster_chunk(&pos);
        }
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
    directory: &Directory,
    pings: &PingTracker,
    primary: Option<u16>,
    outbound: &mpsc::Sender<OutboundParcel>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    queued_requests: &mut VecDeque<(ChunkKey, u16, OutboundParcel)>,
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
    let Ok(bytes) = encode_request(&fetch) else {
        acquisition.unavailable(from);
        pending.remove(&addr);
        error!(
            chunk_x = addr.x(),
            chunk_z = addr.z(),
            from,
            "cluster chunk request encode failed"
        );
        return false;
    };
    let parcel = OutboundParcel {
        peer: ServerId(from),
        header: StreamHeader::new(StreamKind::ChunkRequest, None),
        bytes,
    };
    if queued_requests.is_empty() {
        match outbound.try_send(parcel) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(parcel)) => {
                queued_requests.push_back((addr, from, parcel));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("cluster chunk request transport closed");
            }
        }
    } else {
        queued_requests.push_back((addr, from, parcel));
    }
    assert!(
        queued_requests.len() <= 16_384,
        "cluster chunk request journal capacity exhausted"
    );
    true
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

async fn handle_chunk_parcel(
    server: &Arc<Server>,
    local: u16,
    directory: &mut Directory,
    wanted: &mut HashSet<ChunkKey>,
    pending: &mut HashMap<ChunkKey, ChunkAcquisition>,
    queued_requests: &mut VecDeque<(ChunkKey, u16, OutboundParcel)>,
    deferred_installs: &mut VecDeque<PreparedChunkInstall>,
    pings: &mut PingTracker,
    claims: &mut PrimaryClaims,
    unroutable_reported: &mut HashSet<ChunkKey>,
    primary: Option<u16>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    chunk_inventory_tx: &mpsc::Sender<super::cluster_world_owner::ChunkInventoryRequest>,
    installed_tx: &mpsc::Sender<(ServerId, ChunkPayload)>,
    serve_queue: &mut VecDeque<ServeRequest>,
    parcel: InboundParcel,
) {
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
                directory.apply_advert(advert);
                if wanted.contains(&advert.chunk) {
                    if request_chunk_once(
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
                directory.apply_drop(drop);
                if wanted.contains(&drop.chunk)
                    && pending
                        .get_mut(&drop.chunk)
                        .is_some_and(|acquisition| acquisition.unavailable(drop.holder))
                {
                    if request_chunk_once(
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
        claims.note_requested(fetch.chunk, chunk_now_millis());
        if let Some(world) = world_for_chunk(server, fetch.chunk) {
            world
                .level
                .pin_cluster_chunk(Vector2::new(fetch.chunk.x(), fetch.chunk.z()));
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
        let mut known_holders = directory.sorted_holders(&fetch.chunk);
        known_holders.push(local);
        serve_queue.push_back(ServeRequest {
            fetch,
            destination: parcel.peer,
            known_holders,
        });
        return;
    }
    if parcel.header.kind != StreamKind::ChunkData {
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
    if !wanted.contains(&payload.chunk) {
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
    let (respond, receive) = oneshot::channel();
    let install = PreparedChunkInstall {
        source: parcel.peer,
        request: super::cluster_world_owner::ChunkInventoryRequest::Install {
            chunk: payload.chunk,
            blocks: super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
                min_y,
                block_palettes,
                biome_palettes,
            ),
            states: payload.inventory.clone(),
            pendings: payload.pendings.clone(),
            respond,
        },
        payload,
        chunk,
        world,
        receive,
    };
    if deferred_installs.is_empty() {
        let PreparedChunkInstall {
            source,
            request,
            payload,
            chunk,
            world,
            receive,
        } = install;
        match chunk_inventory_tx.try_send(request) {
            Ok(()) => {
                spawn_installed_chunk_pipeline(
                    server,
                    chunk_inventory_tx,
                    installed_tx,
                    source,
                    payload,
                    chunk,
                    world,
                    receive,
                );
                return;
            }
            Err(mpsc::error::TrySendError::Full(request)) => {
                deferred_installs.push_back(PreparedChunkInstall {
                    source,
                    request,
                    payload,
                    chunk,
                    world,
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
    payload: ChunkPayload,
    chunk: pumpkin_world::level::SyncChunk,
    world: Arc<crate::world::World>,
    receive: oneshot::Receiver<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
) {
    let installed_tx = installed_tx.clone();
    let commit_tx = chunk_inventory_tx.clone();
    server.spawn_task(async move {
        let Ok(Ok(Ok(()))) = tokio::time::timeout(Duration::from_secs(5), receive).await else {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk inventory install rejected or timed out"
            );
            return;
        };
        world
            .level
            .store_cluster_snapshot(Vector2::new(payload.chunk.x(), payload.chunk.z()), &chunk);
        let (ready_respond, ready_receive) = oneshot::channel();
        let mut ready_request = super::cluster_world_owner::ChunkInventoryRequest::CommitReady {
            chunk: payload.chunk,
            respond: ready_respond,
        };
        let mut queued = false;
        for _ in 0..100 {
            match commit_tx.try_send(ready_request) {
                Ok(()) => {
                    queued = true;
                    break;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(request)) => {
                    ready_request = request;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
        if !queued {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk ready route saturated or closed"
            );
            return;
        }
        if !matches!(
            tokio::time::timeout(Duration::from_secs(5), ready_receive).await,
            Ok(Ok(Ok(())))
        ) {
            error!(
                chunk_x = payload.chunk.x(),
                chunk_z = payload.chunk.z(),
                "cluster chunk ready publication failed or timed out"
            );
            return;
        }
        let mut installed = (source, payload);
        for _ in 0..100 {
            match installed_tx.try_send(installed) {
                Ok(()) => return,
                Err(tokio::sync::mpsc::error::TrySendError::Full(value)) => {
                    installed = value;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
        error!("cluster chunk installed-result queue saturated or closed");
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

async fn snapshot_chunk(
    server: &Arc<Server>,
    addr: ChunkKey,
) -> Option<(Vec<u8>, bool, super::cluster_chunk_state::OwnedChunkBlocks)> {
    let world = world_for_chunk(server, addr)?;
    let pos = Vector2::new(addr.x(), addr.z());
    if let Some(entry) = world.level.loaded_chunks.get(&pos) {
        return Some((
            cluster_encode_snapshot(entry.value()),
            true,
            super::cluster_chunk_state::OwnedChunkBlocks::from_chunk(entry.value()),
        ));
    }
    if matches!(server.advanced_config.cluster.role, ClusterRole::Secondary) {
        return None;
    }
    world.level.get_or_fetch_chunk(pos, |_| ()).await;
    world.level.loaded_chunks.get(&pos).map(|entry| {
        (
            cluster_encode_snapshot(entry.value()),
            false,
            super::cluster_chunk_state::OwnedChunkBlocks::from_chunk(entry.value()),
        )
    })
}

async fn admin_task(
    server: Arc<Server>,
    local: ServerId,
    mut control_rx: mpsc::Receiver<InboundParcel>,
    mut local_rx: mpsc::Receiver<super::cluster_admin_apply::LocalAdminMutation>,
    mut seed_rx: mpsc::Receiver<ServerId>,
    replayed_ids: Vec<[u8; 16]>,
) {
    let mut relay_pending = VecDeque::new();
    let mut seed_pending = VecDeque::new();
    let mut admin_revision = replayed_ids.len() as u64;
    let mut pending_versioned = std::collections::BTreeMap::new();
    let mut pending_snapshot = None;
    let mut snapshot_request_pending = false;
    let mut admin_initialized = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let mut sequenced_pending: VecDeque<(
        ServerId,
        [u8; 16],
        pumpkin_cluster::admin_sync::AdminAudit,
    )> = VecDeque::new();
    let mut mutation_acks = VecDeque::new();
    let mut primary_acks_pending = VecDeque::new();
    let mut acknowledged_mutations =
        super::cluster_admin_journal::AcceptedMutationIds::from_replay(replayed_ids);
    let primary = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let (journal_tx, journal_requests) = mpsc::channel(1);
    let (journal_complete, mut journal_rx) = mpsc::channel(1);
    if primary {
        super::cluster_admin_journal::spawn(journal_requests, journal_complete);
    }
    let mut journal_inflight = false;
    let mut flush = tokio::time::interval(Duration::from_millis(10));
    loop {
        let parcel = tokio::select! {
            seed = seed_rx.recv(), if primary && seed_pending.len() < 256 => {
                if let Some(peer) = seed {
                    if !seed_pending.contains(&peer) {
                        seed_pending.push_back(peer);
                    }
                }
                continue;
            }
            local_mutation = local_rx.recv(), if primary && sequenced_pending.len() < 4096 => {
                if let Some(local_mutation) = local_mutation {
                    if !acknowledged_mutations.contains(&local_mutation.id)
                        && !sequenced_pending.iter().any(|(_, pending, _)| pending == &local_mutation.id)
                    {
                        sequenced_pending.push_back((local, local_mutation.id, local_mutation.audit));
                    }
                }
                continue;
            }
            completion = journal_rx.recv(), if journal_inflight => {
                let Some((id, result)) = completion else {
                    error!("cluster admin journal stopped");
                    break;
                };
                journal_inflight = false;
                if let Err(error) = result {
                    if super::cluster_admin_apply::admin_broadcast_warn_cooldown_elapsed() {
                        error!(%error, "cluster admin journal append failed");
                    }
                } else if let Some((peer, pending_id, audit)) = sequenced_pending.pop_front() {
                    if id != pending_id {
                        error!("cluster admin journal completion order violated");
                        break;
                    }
                    super::cluster_admin_apply::apply_sequenced_mutation_to_server(&server, &audit);
                    acknowledged_mutations.insert(id);
                    admin_revision = admin_revision.wrapping_add(1);
                    server.cluster_admin_view.store(Arc::new(
                        super::cluster_admin_apply::snapshot_with_revision(&server, admin_revision),
                    ));
                    if peer == local {
                        super::cluster_admin_apply::notify_durable_admin_ack(&server, &audit.issuer);
                    } else {
                        mutation_acks.push_back((peer, id));
                    }
                    relay_pending.push_back(AdminControlMessage::VersionedMutationWithAudit {
                        revision: admin_revision,
                        audit,
                    });
                }
                continue;
            }
            parcel = control_rx.recv(), if relay_pending.len() < 4096 && sequenced_pending.len() < 4096 && mutation_acks.len() < 4096 && primary_acks_pending.len() < 4096 => parcel,
            _ = flush.tick(), if snapshot_request_pending || !seed_pending.is_empty() || !relay_pending.is_empty() || !sequenced_pending.is_empty() || !mutation_acks.is_empty() || !primary_acks_pending.is_empty() => {
                if snapshot_request_pending {
                    let primary_peer = server.cluster_primary_peers.load().iter().copied().min_by_key(|peer| peer.0);
                    if let Some(primary_peer) = primary_peer
                        && let Ok(bytes) = encode_control_message(&AdminControlMessage::SnapshotRequest)
                        && server.cluster_global_outbound.try_event(&server, vec![primary_peer], bytes)
                    {
                        snapshot_request_pending = false;
                    }
                }
                while let Some(peer) = seed_pending.front().copied() {
                    if !server.cluster_connected_peers.load().contains(&peer)
                        || super::cluster_admin_apply::enqueue_admin_snapshot(&server, peer, admin_revision)
                    {
                        seed_pending.pop_front();
                    } else {
                        break;
                    }
                }
                if !journal_inflight && relay_pending.len() < 4096 && mutation_acks.len() < 4096 {
                    if let Some((_, id, audit)) = sequenced_pending.front() {
                        match journal_tx.try_send((*id, audit.clone())) {
                            Ok(()) => journal_inflight = true,
                            Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                error!("cluster admin journal request owner stopped");
                                break;
                            }
                        }
                    }
                }
                while let Some((peer, id)) = mutation_acks.front().copied() {
                    let Ok(bytes) = encode_control_message(&AdminControlMessage::MutationApplied { id }) else { break; };
                    if !server.cluster_global_outbound.try_event(&server, vec![peer], bytes) { break; }
                    mutation_acks.pop_front();
                }
                while let Some(id) = primary_acks_pending.front().copied() {
                    if !server.cluster_global_outbound.acknowledge_primary_event(id) { break; }
                    primary_acks_pending.pop_front();
                }
                while let Some(message) = relay_pending.front() {
                    if !super::cluster_admin_apply::relay_inbound_mutation_from_primary(&server, message) {
                        break;
                    }
                    relay_pending.pop_front();
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
        if let Some(frame) = super::cluster_player_admin::decode_lobby_intent(&parcel.bytes) {
            let result = super::cluster_player_admin::authorize_lobby_intent(
                &server,
                &frame,
                parcel.peer,
                parcel.generation,
            );
            let result = result.and_then(|()| {
                server
                    .cluster_player_admin
                    .accept_remote_intent(frame.source, frame.intent.clone())
                    .map_err(|_| {
                        super::cluster_player_admin::PlayerAdministrativeFailureReason::QueueFull
                    })
            });
            if let Err(reason) = result
                && !super::cluster_player_admin::reject_lobby_intent(
                    &server,
                    parcel.peer,
                    &frame.intent,
                    reason,
                )
            {
                tracing::error!(
                    peer = parcel.peer.0,
                    ?reason,
                    "cluster lobby administrative denial could not be delivered"
                );
            }
            continue;
        }
        if let Some(frame) = super::cluster_player_admin::decode_intent(&parcel.bytes) {
            let generation = server.cluster_peer_generations.load();
            if super::cluster_player_admin::authorized_intent_frame(
                &frame,
                parcel.peer,
                parcel.generation,
                generation.get(&parcel.peer).copied(),
                local,
                server.cluster_primary_peers.load().contains(&parcel.peer),
            ) {
                let _ = server
                    .cluster_player_admin
                    .accept_remote_intent(frame.source, frame.intent);
            }
            continue;
        }
        if let Some(frame) = super::cluster_player_admin::decode_publication(&parcel.bytes) {
            let generation = server.cluster_peer_generations.load();
            if super::cluster_player_admin::authorized_publication_frame(
                &frame,
                parcel.peer,
                parcel.generation,
                generation.get(&parcel.peer).copied(),
            ) {
                server.cluster_player_admin.publish(frame.publication);
            }
            continue;
        }
        if let Some(frame) = super::cluster_player_admin::decode_failure(&parcel.bytes) {
            let generation = server.cluster_peer_generations.load();
            if super::cluster_player_admin::authorized_failure_frame(
                &frame,
                parcel.peer,
                parcel.generation,
                generation.get(&parcel.peer).copied(),
                local,
            ) {
                server.cluster_player_admin.publish_failure(frame.failure);
            }
            continue;
        }
        let Ok(message) = decode_control_message(&parcel.bytes) else {
            debug!("cluster control parcel is not an admin message");
            continue;
        };
        let generations = server.cluster_peer_generations.load();
        let primary_peers = server.cluster_primary_peers.load();
        let authority = if matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
            AdminAuthority::Primary
        } else {
            AdminAuthority::Secondary
        };
        if !pumpkin_cluster::admin_sync::authorized_control_message(
            authority,
            local,
            parcel.peer,
            parcel.generation,
            generations.get(&parcel.peer).copied(),
            primary_peers.contains(&parcel.peer),
            &message,
        ) {
            continue;
        }
        let message = match message {
            AdminControlMessage::SnapshotPart {
                revision,
                index,
                total,
                digest,
                bytes,
            } if !primary => {
                if revision < admin_revision {
                    continue;
                }
                match AdminSnapshotAssembler::accept(
                    &mut pending_snapshot,
                    revision,
                    index,
                    total,
                    digest,
                    bytes,
                ) {
                    Ok(Some(snapshot)) => AdminControlMessage::Snapshot(snapshot),
                    Ok(None) => continue,
                    Err(error) => {
                        if super::cluster_admin_apply::admin_broadcast_warn_cooldown_elapsed() {
                            warn!(from = parcel.peer.0, %error, "invalid cluster admin snapshot part");
                        }
                        snapshot_request_pending = true;
                        continue;
                    }
                }
            }
            message => message,
        };
        if let AdminControlMessage::Kick(request) = &message
            && !super::cluster_admin_apply::authorized_remote_kick(&server, parcel.peer, request)
        {
            continue;
        }
        match &message {
            AdminControlMessage::Snapshot(snapshot) if !primary => {
                if snapshot.revision >= admin_revision {
                    if !super::cluster_admin_apply::apply_admin_snapshot(&server, snapshot) {
                        warn!(
                            from = parcel.peer.0,
                            revision = snapshot.revision,
                            "invalid cluster admin snapshot"
                        );
                        snapshot_request_pending = true;
                    } else {
                        admin_revision = snapshot.revision;
                        admin_initialized = true;
                        pending_snapshot = None;
                        pending_versioned.retain(|revision, _| *revision > admin_revision);
                        while let Some(audit) =
                            pending_versioned.remove(&admin_revision.wrapping_add(1))
                        {
                            admin_revision = admin_revision.wrapping_add(1);
                            super::cluster_admin_apply::handle_admin_message_for_server(
                                &server,
                                &AdminControlMessage::VersionedMutationWithAudit {
                                    revision: admin_revision,
                                    audit,
                                },
                            );
                            server.cluster_admin_view.store(Arc::new(
                                super::cluster_admin_apply::snapshot_with_revision(
                                    &server,
                                    admin_revision,
                                ),
                            ));
                        }
                        snapshot_request_pending = !pending_versioned.is_empty();
                    }
                }
                continue;
            }
            AdminControlMessage::SnapshotRequest if primary => {
                if seed_pending.len() < 256 && !seed_pending.contains(&parcel.peer) {
                    seed_pending.push_back(parcel.peer);
                }
                continue;
            }
            AdminControlMessage::VersionedMutationWithAudit { revision, audit } if !primary => {
                if *revision > admin_revision {
                    if admin_initialized && *revision == admin_revision.wrapping_add(1) {
                        admin_revision = *revision;
                        super::cluster_admin_apply::handle_admin_message_for_server(
                            &server, &message,
                        );
                        server.cluster_admin_view.store(Arc::new(
                            super::cluster_admin_apply::snapshot_with_revision(
                                &server,
                                admin_revision,
                            ),
                        ));
                        while let Some(next) =
                            pending_versioned.remove(&admin_revision.wrapping_add(1))
                        {
                            admin_revision = admin_revision.wrapping_add(1);
                            super::cluster_admin_apply::handle_admin_message_for_server(
                                &server,
                                &AdminControlMessage::VersionedMutationWithAudit {
                                    revision: admin_revision,
                                    audit: next,
                                },
                            );
                            server.cluster_admin_view.store(Arc::new(
                                super::cluster_admin_apply::snapshot_with_revision(
                                    &server,
                                    admin_revision,
                                ),
                            ));
                        }
                    } else if pending_versioned.len() < 4096 {
                        pending_versioned.insert(*revision, audit.clone());
                    } else {
                        pending_versioned.clear();
                        if super::cluster_admin_apply::admin_broadcast_warn_cooldown_elapsed() {
                            warn!(
                                from = parcel.peer.0,
                                "cluster admin revision backlog exceeded bound; requesting full snapshot"
                            );
                        }
                    }
                    snapshot_request_pending =
                        !pending_versioned.is_empty() || *revision > admin_revision.wrapping_add(1);
                    if pending_snapshot
                        .as_ref()
                        .is_some_and(|snapshot: &AdminSnapshotAssembler| {
                            snapshot.revision() <= admin_revision
                        })
                    {
                        pending_snapshot = None;
                    }
                }
                continue;
            }
            AdminControlMessage::MutationApplied { id } => {
                if !server
                    .cluster_global_outbound
                    .acknowledge_primary_event(*id)
                {
                    primary_acks_pending.push_back(*id);
                }
                continue;
            }
            AdminControlMessage::SequencedMutation { id, audit }
                if matches!(server.advanced_config.cluster.role, ClusterRole::Primary) =>
            {
                if acknowledged_mutations.contains(id) {
                    mutation_acks.push_back((parcel.peer, *id));
                } else if !sequenced_pending
                    .iter()
                    .any(|(_, pending, _)| pending == id)
                {
                    sequenced_pending.push_back((parcel.peer, *id, audit.clone()));
                }
                continue;
            }
            _ => {}
        }
        super::cluster_admin_apply::handle_admin_message_for_server(&server, &message);
        if matches!(
            message,
            AdminControlMessage::Mutation(_) | AdminControlMessage::MutationWithAudit(_)
        ) && matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
        {
            relay_pending.push_back(message);
        }
        while let Some(message) = relay_pending.front() {
            if !super::cluster_admin_apply::relay_inbound_mutation_from_primary(&server, message) {
                break;
            }
            relay_pending.pop_front();
        }
    }
}

async fn accept_task(
    server: Arc<Server>,
    mut accept_rx: mpsc::Receiver<InboundParcel>,
    completion_tx: mpsc::Sender<(ServerId, CompletionMessage)>,
) {
    let mut acceptor = Acceptor::new();
    loop {
        tokio::select! {
            parcel = accept_rx.recv() => {
                let Some(parcel) = parcel else {
                    break;
                };
                if parcel.header.kind != StreamKind::Accept {
                    continue;
                }
                if let Ok(message) = decode_message(&parcel.bytes) {
                    if completion_tx.try_send((parcel.peer, message)).is_err()
                        && TICK_FINISH_DROPPED.fetch_add(1, Ordering::Relaxed) == 0
                    {
                        error!(from = parcel.peer.0, "cluster completion queue full");
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
                        if let Some(handle) = server.primary_save.as_ref() {
                            let _ = handle.try_submit(batch.tick, parcel.bytes.clone());
                        }
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
    fn full_chunk_data_transport_keeps_the_exact_parcel_until_capacity_returns() {
        let (outbound, mut receiver) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(1),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![1],
            })
            .unwrap();
        let mut queued = VecDeque::from([OutboundParcel {
            peer: ServerId(2),
            header: StreamHeader::new(StreamKind::ChunkData, None),
            bytes: vec![4, 5, 6],
        }]);
        flush_queued_chunk_data(&outbound, &mut queued);
        assert_eq!(queued.len(), 1);
        assert_eq!(receiver.try_recv().unwrap().bytes, vec![1]);
        flush_queued_chunk_data(&outbound, &mut queued);
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
        let directory = Directory::new();
        let pings = PingTracker::new();
        assert!(request_chunk_once(
            &directory,
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
        assert_eq!(queued.len(), 1);
        assert!(request_chunk_once(
            &directory,
            &pings,
            Some(1),
            &outbound,
            &mut pending,
            &mut queued,
            chunk,
            None,
            None,
        ));
        assert_eq!(queued.len(), 1);
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
