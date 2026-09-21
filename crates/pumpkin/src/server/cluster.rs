use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use pumpkin_cluster::admin_sync::{
    AdminControlMessage, AdminMutation, InMemoryBanStore, InMemoryOpStore,
    apply_mutation_to_stores, decode_control_message,
    submit_admin_mutation,
};
use pumpkin_cluster::chunks::{
    ChunkAdvert, ChunkAnnounce, ChunkDrop, ChunkFetch, Directory, PingTracker, PrimaryClaims,
    fetch_stuck_since,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::lifecycle::{EntityHandoffRequest, Lifecycle, LifecycleEffect, LifecycleInput, LifecycleState, run_lifecycle};
use pumpkin_cluster::membership::{JoinHandshake, decode_handshake, encode_handshake};
use pumpkin_cluster::mesh::{MeshConfig, PinnedPeer};
use pumpkin_cluster::ntp::{DEFAULT_NTP_SERVER, NtpConfig, NtpSync};
use pumpkin_cluster::protocol::{ChunkAddr, StreamKind};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::streams::{
    DemuxControl, InboundParcel, OutboundParcel, StreamHeader, StreamRegistry, demux_channels,
    run_demux,
};
use pumpkin_cluster::transport::{Transport, channel_pair, load_or_generate_keypair};
use pumpkin_cluster::xfer::{
    ChunkPayload, PendingRef, decode_announce, decode_payload, decode_request, encode_announce,
    encode_payload, encode_request,
};
use pumpkin_config::{ClusterConfig, ClusterRole};
use pumpkin_data::chunk::ChunkStatus;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::level::{
    ClusterChunkAvailability, ClusterFetchRequest, ClusterLogoutGrace, cluster_decode_snapshot,
    cluster_encode_snapshot, set_cluster_chunk_availability_sender, set_cluster_fetch_sender,
    set_cluster_has_peers, set_cluster_logout_sender, set_cluster_secondary,
    set_cluster_unwant_sender,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::Server;
use super::cluster_regions;
use super::cluster_entity_apply::spawn_entity_apply;
use super::cluster_world_apply::spawn_world_apply;
use super::cluster_transient::spawn_transient_apply;
use super::cluster_visual::spawn_visual_apply;
use super::{cluster_chat_in, cluster_chat_out, cluster_datagram};

static FUSED_BRIDGE: std::sync::OnceLock<mpsc::Sender<Vec<u8>>> = std::sync::OnceLock::new();
static FUSED_FORWARDED: AtomicU64 = AtomicU64::new(0);
static ADMIN_APPLIED: AtomicU64 = AtomicU64::new(0);
static FETCHED_CHUNKS: AtomicU64 = AtomicU64::new(0);
static SERVED_CHUNKS: AtomicU64 = AtomicU64::new(0);
static NTP_UNDISCIPLINED_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static NTP_DISCIPLINE_STARTED: AtomicBool = AtomicBool::new(false);
static CLUSTER_ADMITTED: AtomicBool = AtomicBool::new(false);
static WORLD_TIME_BOOTSTRAPPED: AtomicBool = AtomicBool::new(false);
static CLUSTER_LIFECYCLE_STATE: AtomicU64 = AtomicU64::new(0);
static CLUSTER_ENABLED: AtomicBool = AtomicBool::new(false);
static CLUSTER_MESH_READY: AtomicBool = AtomicBool::new(false);
static CLUSTER_IS_PRIMARY: AtomicBool = AtomicBool::new(false);
static CLUSTER_PENDING_SYNC: AtomicU64 = AtomicU64::new(0);
static CLUSTER_INFLIGHT_FETCH: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DROPPED_SYNC: AtomicU64 = AtomicU64::new(0);
static CLUSTER_HANDOFF_SENT: AtomicU64 = AtomicU64::new(0);
static CHUNK_DIAGNOSTIC_LAST_MILLIS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static LIFECYCLE_IN: std::sync::OnceLock<mpsc::Sender<LifecycleInput>> = std::sync::OnceLock::new();
static CHUNK_HOLDER_DIRECTORY: std::sync::LazyLock<ArcSwap<BTreeMap<ChunkAddr, Vec<u16>>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));
static CLUSTER_MEMBERS: std::sync::LazyLock<ArcSwap<BTreeSet<u16>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeSet::new()));

#[must_use]
pub fn chunk_holders(chunk: ChunkAddr) -> Vec<u16> {
    CHUNK_HOLDER_DIRECTORY
        .load()
        .get(&chunk)
        .cloned()
        .unwrap_or_default()
}

#[must_use]
pub fn cluster_peer_admitted(peer: u16) -> bool {
    CLUSTER_MEMBERS.load().contains(&peer)
}

fn admit_cluster_peer(peer: u16) {
    CLUSTER_MEMBERS.rcu(|members| {
        let mut next = (**members).clone();
        next.insert(peer);
        Arc::new(next)
    });
}

fn remove_cluster_peer(peer: u16) {
    CLUSTER_MEMBERS.rcu(|members| {
        let mut next = (**members).clone();
        next.remove(&peer);
        Arc::new(next)
    });
}

fn publish_chunk_holders(directory: &Directory) {
    let mut snapshot = BTreeMap::new();
    for chunk in directory.holders.keys() {
        let holders: Vec<u16> = directory
            .sorted_holders(chunk)
            .into_iter()
            .filter(|peer| cluster_peer_admitted(*peer))
            .collect();
        if !holders.is_empty() {
            snapshot.insert(*chunk, holders);
        }
    }
    CHUNK_HOLDER_DIRECTORY.store(Arc::new(snapshot));
}

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
pub fn disciplined_tick_now() -> Option<TickStamp> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|age| i64::try_from(age.as_millis()).ok())?;
    let offset = pumpkin_cluster::ntp::shared_offset_millis()?;
    Some(TickStamp::from_disciplined_millis(millis, offset))
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
    let max_precision_millis = config.max_precision_millis;
    let (sync, _) = NtpSync::new(NtpConfig::new(servers.clone(), max_precision_millis));
    server.spawn_task(async move {
        sync.run().await;
    });
    info!(
        servers = servers.len(),
        max_precision_millis, "cluster ntp discipline started"
    );
}

#[must_use]
pub fn fused_forwarded() -> u64 {
    FUSED_FORWARDED.load(Ordering::Relaxed)
}

#[must_use]
pub fn admin_applied() -> u64 {
    ADMIN_APPLIED.load(Ordering::Relaxed)
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
pub fn cluster_real_world_handoff_allowed() -> bool {
    if !CLUSTER_ENABLED.load(Ordering::Relaxed) {
        return true;
    }
    if !CLUSTER_MESH_READY.load(Ordering::Relaxed) {
        return false;
    }
    if CLUSTER_IS_PRIMARY.load(Ordering::Relaxed) {
        return false;
    }
    CLUSTER_ADMITTED.load(Ordering::Acquire) && WORLD_TIME_BOOTSTRAPPED.load(Ordering::Acquire)
}

#[must_use]
pub fn cluster_lobby_connection_status() -> Option<&'static str> {
    if !CLUSTER_ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    if !CLUSTER_MESH_READY.load(Ordering::Relaxed) {
        return Some("No connection to cluster peers");
    }
    if !CLUSTER_ADMITTED.load(Ordering::Acquire) {
        return Some("No connection to all cluster peers");
    }
    if !WORLD_TIME_BOOTSTRAPPED.load(Ordering::Acquire) {
        return Some("No connection to primary");
    }
    None
}

pub fn mark_world_time_bootstrapped() {
    WORLD_TIME_BOOTSTRAPPED.store(true, Ordering::Release);
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
pub fn cluster_dropped_sync() -> u64 {
    CLUSTER_DROPPED_SYNC.load(Ordering::Relaxed)
}

pub fn log_cluster_shutdown_progress(server: &Server, context: &str) {
    let players = server.get_player_count();
    let owned_entities = super::cluster_entity_emit::locally_owned_entities(server).len();
    info!(
        context,
        players,
        owned_entities,
        pending_sync_buckets = CLUSTER_PENDING_SYNC.load(Ordering::Relaxed),
        inflight_chunk_fetches = CLUSTER_INFLIGHT_FETCH.load(Ordering::Relaxed),
        dropped_sync_buckets = CLUSTER_DROPPED_SYNC.load(Ordering::Relaxed),
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
    let owned = super::cluster_entity_emit::locally_owned_entities(server);
    let mut holders = Vec::with_capacity(owned.len());
    for entity in &owned {
        let chunk = entity.chunk();
        holders.push((chunk, chunk_holders(chunk)));
    }
    let request = LifecycleInput::RequestLeave {
        player_count,
        owned,
        holders,
    };
    if input.try_send(request).is_err() {
        warn!(player_count, "cluster leave request dropped: lifecycle inbox full");
        return;
    }
    info!(player_count, lifecycle = cluster_lifecycle_state_text(), "cluster leave requested");
}

fn lifecycle_state_code(state: LifecycleState) -> u64 {
    match state {
        LifecycleState::Solo => 0,
        LifecycleState::Joining => 1,
        LifecycleState::Member => 2,
        LifecycleState::Leaving => 3,
    }
}

fn send_handshake(outbound: &mpsc::Sender<OutboundParcel>, to: &[u16], message: JoinHandshake, local: u16) {
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
        if outbound.try_send(parcel).is_err() {
            warn!(server_id = local, to = peer, "cluster handshake send dropped: mesh queue full");
        }
    }
}

fn send_handoff(
    outbound: &mpsc::Sender<OutboundParcel>,
    members: &[u16],
    request: &EntityHandoffRequest,
    local: u16,
) -> Option<Vec<pumpkin_cluster::entities::EntityHandoff>> {
    if !request.is_consistent() {
        warn!(server_id = local, leaver = request.leaver, "cluster handoff rejected: inconsistent payload");
        return None;
    }
    if !request.handoffs.is_empty() {
        let mut sent = Vec::new();
        for successor in request.successors() {
            if !cluster_peer_admitted(successor.0) {
                warn!(server_id = local, successor = successor.0, "cluster handoff successor is not admitted");
                continue;
            }
            let scoped = request.for_successor(successor);
            let Ok(bytes) = postcard::to_allocvec(&scoped) else {
                warn!(server_id = local, successor = successor.0, "cluster handoff encode failed");
                continue;
            };
            if outbound
                .try_send(OutboundParcel {
                    peer: successor,
                    header: StreamHeader::new(StreamKind::Control, None),
                    bytes,
                })
                .is_err()
            {
                warn!(server_id = local, to = successor.0, "cluster handoff send dropped: mesh queue full");
                continue;
            }
            sent.extend(scoped.handoffs);
        }
        CLUSTER_HANDOFF_SENT.fetch_add(sent.len() as u64, Ordering::Relaxed);
        return Some(sent);
    }
    let Ok(bytes) = postcard::to_allocvec(request) else {
        warn!(server_id = local, "cluster handoff encode failed");
        return None;
    };
    for peer in members {
        if !cluster_peer_admitted(*peer) {
            continue;
        }
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        if outbound.try_send(parcel).is_err() {
            warn!(server_id = local, to = peer, "cluster handoff send dropped: mesh queue full");
            return None;
        }
    }
    CLUSTER_HANDOFF_SENT.fetch_add(1, Ordering::Relaxed);
    Some(Vec::new())
}

async fn lifecycle_effect_task(
    server: Arc<Server>,
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
    peers: Vec<ServerId>,
    membership_notice: mpsc::Sender<bool>,
    mut effects: mpsc::Receiver<LifecycleEffect>,
) {
    let fallback: Vec<u16> = peers.iter().map(|peer| peer.0).collect();
    while let Some(effect) = effects.recv().await {
        match effect {
            LifecycleEffect::SendHello { to, hello } => {
                info!(server_id = local.0, to = to.len(), "cluster join hello sent");
                send_handshake(&outbound, &to, JoinHandshake::Hello(hello), local.0);
            }
            LifecycleEffect::SendVoteRequest { to, request } => {
                send_handshake(&outbound, &to, JoinHandshake::VoteRequest(request), local.0);
            }
            LifecycleEffect::SendVote { to, vote } => {
                send_handshake(&outbound, &to, JoinHandshake::Vote(vote), local.0);
            }
            LifecycleEffect::BroadcastAdmit { to, admit } => {
                info!(server_id = local.0, candidate = admit.candidate, "cluster admit broadcast");
                send_handshake(&outbound, &to, JoinHandshake::Admit(admit), local.0);
            }
            LifecycleEffect::EmitHandoff(request) => {
                info!(
                    server_id = local.0,
                    leaver = request.leaver,
                    handoffs = request.handoffs.len(),
                    "cluster leave handoff emitted"
                );
                let Some(sent) = send_handoff(&outbound, &fallback, &request, local.0) else {
                    continue;
                };
                if !request.handoffs.is_empty() {
                    let removed = super::cluster_entity_apply::remove_transferred_entities(
                        &server,
                        local,
                        &sent,
                    );
                    if removed != sent.len() {
                        warn!(server_id = local.0, removed, expected = sent.len(), "cluster handoff source removal incomplete");
                        continue;
                    }
                    if sent.len() != request.handoffs.len() {
                        warn!(server_id = local.0, delivered = sent.len(), expected = request.handoffs.len(), "cluster handoff remains locally owned");
                        continue;
                    }
                    begin_cluster_leave_drain(server.as_ref());
                } else if let Some(input) = LIFECYCLE_IN.get() {
                    if input
                        .try_send(LifecycleInput::LeaveNoticeEnqueued)
                        .is_err()
                    {
                        warn!(server_id = local.0, "cluster leave state notice dropped: lifecycle inbox full");
                    }
                }
            }
            LifecycleEffect::Joined { candidate } => {
                info!(server_id = local.0, candidate, "cluster peer joined");
                admit_cluster_peer(candidate);
                if candidate != local.0 {
                    if CLUSTER_ADMITTED.load(Ordering::Relaxed)
                        && membership_notice.try_send(true).is_err()
                    {
                        warn!(server_id = local.0, "cluster membership peer notice dropped");
                    }
                    super::cluster_admin_apply::publish_ops_to_peer(&server, candidate);
                    super::cluster_hide::publish_hidden_to_peer(&server, candidate);
                    super::cluster_presence::publish_roster_to_peer(&server, candidate);
                    if local.0 == server.advanced_config.cluster.primary_server_id {
                        super::cluster_world_time::send_bootstrap_to_peer(
                            &server,
                            &outbound,
                            ServerId(candidate),
                        );
                    }
                }
            }
            LifecycleEffect::Left { peer } => {
                info!(server_id = local.0, peer, "cluster peer left");
                remove_cluster_peer(peer);
                if peer == local.0 {
                    CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
                } else {
                    super::cluster_presence::forget_remote_server(peer);
                }
            }
            LifecycleEffect::LeaveBlocked { player_count, owned_entities } => {
                warn!(
                    server_id = local.0,
                    player_count,
                    owned_entities,
                    "cluster leave blocked: drain players and entities first"
                );
            }
            LifecycleEffect::VoteDenied { candidate, voter } => {
                warn!(server_id = local.0, candidate, voter, "cluster join vote denied");
            }
            LifecycleEffect::VoteBlocked { candidate } => {
                warn!(server_id = local.0, candidate, "cluster join vote blocked: suspect peer");
            }
            LifecycleEffect::SuspectMarked { peer } => {
                warn!(server_id = local.0, peer, "cluster suspect peer marked after unclean drop");
            }
            LifecycleEffect::SuspectCleared { peer } => {
                info!(server_id = local.0, peer, "cluster suspect peer cleared by operator");
            }
            LifecycleEffect::StateChanged(state) => {
                CLUSTER_LIFECYCLE_STATE.store(lifecycle_state_code(state), Ordering::Relaxed);
                if state == LifecycleState::Member {
                    admit_cluster_peer(local.0);
                    for peer in &peers {
                        admit_cluster_peer(peer.0);
                    }
                    CLUSTER_ADMITTED.store(true, Ordering::Relaxed);
                    if membership_notice.try_send(true).is_err() {
                        warn!(server_id = local.0, "cluster membership admission notice dropped");
                    }
                } else {
                    CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
                    if membership_notice.try_send(false).is_err() {
                        warn!(server_id = local.0, "cluster membership withdrawal notice dropped");
                    }
                }
                info!(server_id = local.0, ?state, "cluster lifecycle state");
            }
        }
    }
}

pub fn note_cluster_handshake(parcel: &InboundParcel) {
    let Some(input) = LIFECYCLE_IN.get() else {
        return;
    };
    let Ok(message) = decode_handshake(&parcel.bytes) else {
        if let Ok(request) = postcard::from_bytes::<EntityHandoffRequest>(&parcel.bytes) {
            if request.handoffs.is_empty() {
                if request.leaver != parcel.peer.0 {
                    warn!(peer = parcel.peer.0, leaver = request.leaver, "cluster leave notice owner mismatch");
                } else if input.try_send(LifecycleInput::RemoteHandoff(request)).is_err() {
                    warn!(peer = parcel.peer.0, "cluster leave notice dropped: lifecycle inbox full");
                }
            } else {
                if !super::cluster_entity_apply::forward_entity_handoffs(
                    parcel.peer,
                    request.handoffs,
                ) {
                    warn!(peer = parcel.peer.0, "cluster entity handoff dropped");
                }
            }
        }
        return;
    };
    let authenticated = match &message {
        JoinHandshake::Hello(hello) => hello.candidate == parcel.peer.0,
        JoinHandshake::VoteRequest(request) => request.requested_by == parcel.peer.0,
        JoinHandshake::Vote(vote) => vote.voter == parcel.peer.0,
        JoinHandshake::Admit(admit) => admit.admitted_by == parcel.peer.0,
    };
    if !authenticated {
        warn!(peer = parcel.peer.0, "cluster handshake identity mismatch");
        return;
    }
    let next = match message {
        JoinHandshake::Hello(hello) => LifecycleInput::RemoteHello { hello, pin_match: true },
        JoinHandshake::VoteRequest(request) => LifecycleInput::RemoteVoteRequest(request),
        JoinHandshake::Vote(vote) => LifecycleInput::RemoteVote(vote),
        JoinHandshake::Admit(admit) => LifecycleInput::RemoteAdmit(admit),
    };
    if input.try_send(next).is_err() {
        warn!(peer = parcel.peer.0, "cluster handshake dropped: lifecycle inbox full");
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
    CLUSTER_ENABLED.store(config.enabled, Ordering::Relaxed);
    if !config.enabled {
        return;
    }
    start_ntp_discipline(server, &config);
    let is_primary = matches!(config.role, ClusterRole::Primary);
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
    let (lifecycle_in_tx, mut lifecycle_in_rx) = mpsc::channel(4096);
    let (lifecycle_out_tx, lifecycle_out_rx) = mpsc::channel::<LifecycleEffect>(4096);
    let lifecycle = Lifecycle::new_solo(local.0);
    let peer_list: Vec<u16> = peer_ids.iter().map(|peer| peer.0).collect();
    let _ = LIFECYCLE_IN.set(lifecycle_in_tx.clone());
    if lifecycle_in_tx
        .try_send(LifecycleInput::BeginJoin {
            cert_fingerprint: fingerprint,
            peers: peer_list,
        })
        .is_err()
    {
        error!(server_id = local.0, "cluster lifecycle bootstrap input dropped");
        return;
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
    let peer_ready_rx = mesh_channels.peer_ready_rx;
    let outbound_tx = mesh_channels.outbound_tx;
    let datagram_in_rx = mesh_channels.datagram_in_rx;
    cluster_datagram::install_datagram_outbox(peer_ids.clone(), mesh_channels.datagram_out_tx);
    super::cluster_world_delta::install_world_delta_outbox(local, outbound_tx.clone());
    super::cluster_world_apply::install_world_action_outbox(outbound_tx.clone());
    super::cluster_primary_persist::install_primary_tick_stream(
        &server,
        local,
        ServerId(config.primary_server_id),
        outbound_tx.clone(),
        demux_receivers.primary_tick,
    );
    super::cluster_playerdata::install_playerdata(&server, peer_ids.clone(), outbound_tx.clone());
    super::cluster_playerdata::spawn_position_datagram_demux(&server, datagram_in_rx);
    server.spawn_task(run_demux(
        inbound_rx,
        control_rx,
        demux_senders,
        StreamRegistry::new(),
    ));

    spawn_transient_apply(&server, local, demux_receivers.transient);

    spawn_visual_apply(&server, local, demux_receivers.visual);

    spawn_world_apply(&server, demux_receivers.world, demux_receivers.combat);

    let peer_ready_server = Arc::clone(server);
    let peer_ready_outbound = outbound_tx.clone();
    server.spawn_task(async move {
        let mut peer_ready_rx = peer_ready_rx;
        while let Some(peer) = peer_ready_rx.recv().await {
            if is_primary {
                super::cluster_world_time::send_bootstrap_to_peer(
                    &peer_ready_server,
                    &peer_ready_outbound,
                    peer,
                );
            }
            if let Some(input) = LIFECYCLE_IN.get() {
                if input
                    .try_send(LifecycleInput::PeerReady { peer: peer.0 })
                    .is_err()
                {
                    warn!(peer = peer.0, "cluster peer-ready notice dropped: lifecycle inbox full");
                }
            }
        }
    });

    let (fetch_tx, fetch_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_fetch_sender(fetch_tx);
    let (unwant_tx, unwant_rx) = mpsc::channel::<ClusterFetchRequest>(4096);
    set_cluster_unwant_sender(unwant_tx);
    let (logout_tx, logout_rx) = mpsc::channel::<ClusterLogoutGrace>(64);
    set_cluster_logout_sender(logout_tx);
    let (availability_tx, availability_rx) = mpsc::channel::<ClusterChunkAvailability>(4096);
    set_cluster_chunk_availability_sender(availability_tx);
    let (chunk_membership_tx, chunk_membership_rx) = mpsc::channel::<bool>(4096);
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
        availability_rx,
        chunk_membership_rx,
    ));

    spawn_entity_apply(&server, local, demux_receivers.entity);
    super::cluster_entity_boundary::install_boundary_outbox(
        local,
        ServerId(config.primary_server_id),
        outbound_tx.clone(),
    );

    server.spawn_task(accept_task(
        server.clone(),
        demux_receivers.accept,
    ));

    super::cluster_entity_apply::install_entity_outbox(local, fallback.clone(), outbound_tx.clone());
    cluster_chat_out::install_chat_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_admin_apply::install_admin_outbox(fallback.clone(), outbound_tx.clone());
    super::cluster_lobby_control::install_lobby_control_outbox(local, outbound_tx.clone());

    super::cluster_hide::install_hide_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_world_time::install_world_time_outbox(fallback.clone(), outbound_tx.clone());
    let (admin_tx, admin_rx) = mpsc::channel::<InboundParcel>(1024);
    let (chat_tx, chat_rx) = mpsc::channel::<InboundParcel>(1024);
    let (hide_tx, hide_rx) = mpsc::channel::<InboundParcel>(1024);
    let (presence_tx, presence_rx) = mpsc::channel::<InboundParcel>(1024);
    let (world_time_tx, world_time_rx) = mpsc::channel::<InboundParcel>(1024);
    let (playerdata_tx, playerdata_rx) = mpsc::channel::<InboundParcel>(1024);
    let (lobby_control_tx, lobby_control_rx) = mpsc::channel::<InboundParcel>(1024);
    let (boundary_tx, boundary_rx) = mpsc::channel::<InboundParcel>(1024);
    super::cluster_presence::install_presence_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_tick_pump::install_tick_pump_outbox(peer_ids.clone(), outbound_tx.clone());
    cluster_chat_in::spawn_control_fanout(
        server,
        demux_receivers.control,
        admin_tx,
        chat_tx,
        presence_tx,
        hide_tx,
        world_time_tx,
        playerdata_tx,
        lobby_control_tx,
        boundary_tx,
    );
    super::cluster_hide::spawn_hide_apply(server, hide_rx);
    super::cluster_world_time::spawn_world_time_apply(server, world_time_rx);
    cluster_chat_in::spawn_chat_delivery(server, local, chat_rx);
    super::cluster_presence::spawn_presence_apply(server, local, presence_rx);
    super::cluster_playerdata::spawn_playerdata_control(server, playerdata_rx);
    super::cluster_lobby_control::spawn_lobby_control_apply(server, local, lobby_control_rx);
    super::cluster_entity_boundary::spawn_boundary_actor(server, boundary_rx);

    server.spawn_task(admin_task(server.clone(), admin_rx));

    let (fused_tx, fused_rx) = mpsc::channel::<Vec<u8>>(8);
    let _ = FUSED_BRIDGE.set(fused_tx);
    server.spawn_task(fuse_task(fused_rx, outbound_tx.clone(), peer_ids.clone()));

    CLUSTER_IS_PRIMARY.store(is_primary, Ordering::Relaxed);
    CLUSTER_MESH_READY.store(true, Ordering::Relaxed);
    CLUSTER_LIFECYCLE_STATE.store(0, Ordering::Relaxed);
    CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
    WORLD_TIME_BOOTSTRAPPED.store(is_primary, Ordering::Release);
    server.spawn_task(async move {
        run_lifecycle(lifecycle, &mut lifecycle_in_rx, &lifecycle_out_tx).await;
    });
    server.spawn_task(lifecycle_effect_task(
        server.clone(),
        local,
        outbound_tx.clone(),
        peer_ids.clone(),
        chunk_membership_tx,
        lifecycle_out_rx,
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

fn chunk_diagnostic_cooldown_elapsed(stage: usize) -> bool {
    let now = chunk_now_millis();
    let last = CHUNK_DIAGNOSTIC_LAST_MILLIS[stage].load(Ordering::Relaxed);
    if now.saturating_sub(last) < 1_000 {
        return false;
    }
    CHUNK_DIAGNOSTIC_LAST_MILLIS[stage]
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
    mut availability_rx: mpsc::Receiver<ClusterChunkAvailability>,
    mut membership_rx: mpsc::Receiver<bool>,
) {
    let primary = ServerId(server.advanced_config.cluster.primary_server_id);
    let is_primary = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let mut directory = Directory::new();
    let mut wanted: HashSet<ChunkAddr> = HashSet::new();
    let mut pending: HashMap<ChunkAddr, (u16, u64)> = HashMap::new();
    let mut pings = PingTracker::new();
    let mut advertised: HashSet<ChunkAddr> = HashSet::new();
    let mut claims = PrimaryClaims::new();
    let mut stuck_reported: HashSet<ChunkAddr> = HashSet::new();
    let mut unroutable_reported: HashSet<ChunkAddr> = HashSet::new();
    let mut grace: HashMap<ChunkAddr, u64> = HashMap::new();
    let mut primary_waiters: HashMap<ChunkAddr, HashSet<ServerId>> = HashMap::new();
    let mut early_parcels: VecDeque<InboundParcel> = VecDeque::new();
    let mut admitted = false;
    publish_chunk_holders(&directory);
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            membership = membership_rx.recv() => {
                let Some(next) = membership else { break };
                admitted = next;
                if admitted {
                    advertise_loaded_chunks(
                        &server,
                        local,
                        &mut directory,
                        &mut advertised,
                        &fallback,
                        &outbound,
                    );
                    for addr in &wanted {
                        if request_chunk_once(
                            &directory,
                            &pings,
                            primary,
                            &fallback,
                            &outbound,
                            &mut pending,
                            *addr,
                        ) {
                            unroutable_reported.remove(addr);
                        }
                    }
                    while let Some(parcel) = early_parcels.pop_front() {
                        if cluster_peer_admitted(parcel.peer.0) {
                            handle_chunk_parcel(
                                &server,
                                local,
                                primary,
                                is_primary,
                                &mut directory,
                                &mut wanted,
                                &mut pending,
                                &mut pings,
                                &mut claims,
                                &mut primary_waiters,
                                &mut stuck_reported,
                                &mut unroutable_reported,
                                &fallback,
                                &outbound,
                                parcel,
                            );
                        }
                    }
                } else {
                    early_parcels.clear();
                    for addr in advertised.drain() {
                        directory.apply_drop(ChunkDrop { holder: local.0, chunk: addr });
                        announce_holds(
                            &outbound,
                            &fallback,
                            ChunkAnnounce::Release(ChunkDrop { holder: local.0, chunk: addr }),
                        );
                    }
                }
                publish_chunk_holders(&directory);
            }
            incoming = chunk_rx.recv() => {
                let Some(parcel) = incoming else { break };
                if admitted && cluster_peer_admitted(parcel.peer.0) {
                    handle_chunk_parcel(&server, local, primary, is_primary, &mut directory, &mut wanted, &mut pending, &mut pings, &mut claims, &mut primary_waiters, &mut stuck_reported, &mut unroutable_reported, &fallback, &outbound, parcel);
                } else if early_parcels.len() < 4096 {
                    early_parcels.push_back(parcel);
                } else {
                    error!("cluster chunk parcel arrived before membership and deferred inbox is full");
                }
                publish_chunk_holders(&directory);
                CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
            }
            want = want_rx.recv() => {
                let Some(request) = want else { break };
                let addr = ChunkAddr {
                    x: request.pos.x,
                    z: request.pos.y,
                };
                wanted.insert(addr);
                grace.remove(&addr);
                if admitted && request_chunk_once(
                    &directory,
                    &pings,
                    primary,
                    &fallback,
                    &outbound,
                    &mut pending,
                    addr,
                ) {
                    unroutable_reported.remove(&addr);
                }
                while let Ok(request) = want_rx.try_recv() {
                    let addr = ChunkAddr {
                        x: request.pos.x,
                        z: request.pos.y,
                    };
                    wanted.insert(addr);
                    grace.remove(&addr);
                    if admitted && request_chunk_once(
                        &directory,
                        &pings,
                        primary,
                        &fallback,
                        &outbound,
                        &mut pending,
                        addr,
                    ) {
                        unroutable_reported.remove(&addr);
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
            }
            unwant = unwant_rx.recv() => {
                let Some(request) = unwant else { break };
                reset_fetch_state(
                    &server,
                    &mut wanted,
                    &mut pending,
                    &mut stuck_reported,
                    &mut unroutable_reported,
                    ChunkAddr {
                        x: request.pos.x,
                        z: request.pos.y,
                    },
                    false,
                );
                while let Ok(request) = unwant_rx.try_recv() {
                    reset_fetch_state(
                        &server,
                        &mut wanted,
                        &mut pending,
                        &mut stuck_reported,
                        &mut unroutable_reported,
                        ChunkAddr {
                            x: request.pos.x,
                            z: request.pos.y,
                        },
                        false,
                    );
                }
                CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
            }
            logout = logout_rx.recv() => {
                let Some(entry) = logout else { break };
                for pos in &entry.chunks {
                    grace.insert(
                        ChunkAddr {
                            x: pos.x,
                            z: pos.y,
                        },
                        entry.expires_millis,
                    );
                }
            }
            availability = availability_rx.recv() => {
                let Some(availability) = availability else { break };
                if admitted {
                    handle_chunk_availability(
                    &server,
                    local,
                    is_primary,
                    &mut directory,
                    &mut advertised,
                    &mut primary_waiters,
                    &fallback,
                    &outbound,
                    availability,
                    );
                }
                publish_chunk_holders(&directory);
            }
            _ = sweep.tick() => {
                let now_millis = chunk_now_millis();
                if is_primary {
                    release_expired_claims(&server, &mut claims, now_millis);
                }
                if admitted {
                    report_stuck_fetches(&pending, &mut stuck_reported, now_millis);
                    report_unroutable_wants(&wanted, &pending, &mut unroutable_reported);
                }
                if !grace.is_empty() {
                    let expired: Vec<ChunkAddr> = grace
                        .iter()
                        .filter_map(|(addr, deadline)| (*deadline <= now_millis).then_some(*addr))
                        .collect();
                    for addr in expired {
                        grace.remove(&addr);
                        reset_fetch_state(
                            &server,
                            &mut wanted,
                            &mut pending,
                            &mut stuck_reported,
                            &mut unroutable_reported,
                            addr,
                            true,
                        );
                    }
                    CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
                }
            }
        }
    }
}

fn reset_fetch_state(
    server: &Arc<Server>,
    wanted: &mut HashSet<ChunkAddr>,
    pending: &mut HashMap<ChunkAddr, (u16, u64)>,
    stuck_reported: &mut HashSet<ChunkAddr>,
    unroutable_reported: &mut HashSet<ChunkAddr>,
    addr: ChunkAddr,
    unpin: bool,
) {
    wanted.remove(&addr);
    pending.remove(&addr);
    stuck_reported.remove(&addr);
    unroutable_reported.remove(&addr);
    let pos = Vector2::new(addr.x, addr.z);
    for world in server.worlds.load().iter() {
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

fn advertise_loaded_chunks(
    server: &Arc<Server>,
    local: ServerId,
    directory: &mut Directory,
    advertised: &mut HashSet<ChunkAddr>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
) {
    for world in server.worlds.load().iter() {
        for entry in world.level.loaded_chunks.iter() {
            if entry.value().status != ChunkStatus::Full {
                continue;
            }
            let addr = ChunkAddr {
                x: entry.key().x,
                z: entry.key().y,
            };
            directory.apply_advert(ChunkAdvert {
                holder: local.0,
                chunk: addr,
            });
            advertised.insert(addr);
            announce_holds(
                outbound,
                fallback,
                ChunkAnnounce::Acquire(ChunkAdvert {
                    holder: local.0,
                    chunk: addr,
                }),
            );
        }
    }
}

fn release_expired_claims(
    server: &Arc<Server>,
    claims: &mut PrimaryClaims,
    now_millis: u64,
) {
    for addr in claims.take_expired(now_millis) {
        let pos = Vector2::new(addr.x, addr.z);
        for world in server.worlds.load().iter() {
            world.level.finish_cluster_full_chunk_request(pos);
            world.level.unpin_cluster_chunk(&pos);
        }
    }
}

fn report_stuck_fetches(
    pending: &HashMap<ChunkAddr, (u16, u64)>,
    reported: &mut HashSet<ChunkAddr>,
    now_millis: u64,
) {
    for (addr, (from, at)) in pending {
        if fetch_stuck_since(*at, now_millis) && reported.insert(*addr) {
            error!(
                chunk_x = addr.x,
                chunk_z = addr.z,
                from = *from,
                waited_millis = now_millis.saturating_sub(*at),
                "cluster chunk fetch stuck without chunk"
            );
        }
    }
}

fn request_chunk_once(
    directory: &Directory,
    pings: &PingTracker,
    primary: ServerId,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    pending: &mut HashMap<ChunkAddr, (u16, u64)>,
    addr: ChunkAddr,
) -> bool {
    if pending.contains_key(&addr) {
        return true;
    }
    let Some(from) = chunk_source(directory, pings, primary, fallback, addr) else {
        return false;
    };
    let fetch = ChunkFetch { chunk: addr, from };
    let Ok(bytes) = encode_request(&fetch) else {
        error!(
            chunk_x = addr.x,
            chunk_z = addr.z,
            from,
            "cluster chunk request encode failed"
        );
        return false;
    };
    if outbound
        .try_send(OutboundParcel {
            peer: ServerId(from),
            header: StreamHeader::new(StreamKind::ChunkRequest, None),
            bytes,
        })
        .is_err()
    {
        error!(
            chunk_x = addr.x,
            chunk_z = addr.z,
            from,
            "cluster chunk request send failed"
        );
        return false;
    }
    pending.insert(addr, (from, chunk_now_millis()));
    true
}

fn report_unroutable_wants(
    wanted: &HashSet<ChunkAddr>,
    pending: &HashMap<ChunkAddr, (u16, u64)>,
    reported: &mut HashSet<ChunkAddr>,
) {
    reported.retain(|addr| wanted.contains(addr) && !pending.contains_key(addr));
    for addr in wanted {
        if pending.contains_key(addr) {
            continue;
        }
        if reported.insert(*addr) {
            error!(
                chunk_x = addr.x,
                chunk_z = addr.z,
                "cluster chunk want without routable holder"
            );
        }
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
        if !cluster_peer_admitted(*peer) {
            continue;
        }
        if outbound
            .try_send(OutboundParcel {
                peer: ServerId(*peer),
                header: StreamHeader::new(StreamKind::ChunkAdvert, None),
                bytes: bytes.clone(),
            })
            .is_err()
        {
            error!(peer, "cluster chunk advert send failed");
        }
    }
}

fn chunk_source(
    directory: &Directory,
    pings: &PingTracker,
    primary: ServerId,
    fallback: &[u16],
    chunk: ChunkAddr,
) -> Option<u16> {
    let holders: Vec<u16> = directory
        .sorted_holders(&chunk)
        .into_iter()
        .filter(|peer| cluster_peer_admitted(*peer))
        .collect();
    pings.best(&holders).or_else(|| {
        fallback
            .contains(&primary.0)
            .then_some(primary.0)
    })
}

fn send_chunk_payload(
    server: &Arc<Server>,
    local: ServerId,
    directory: &Directory,
    outbound: &mpsc::Sender<OutboundParcel>,
    chunk: ChunkAddr,
    peer: ServerId,
) -> bool {
    let Some(snapshot) = snapshot_chunk(server, chunk) else {
        if chunk_diagnostic_cooldown_elapsed(0) {
            warn!(
                target: "cluster_chunk",
                stage = "snapshot_absent",
                chunk_x = chunk.x,
                chunk_z = chunk.z,
                source = local.0,
                destination = peer.0,
                "cluster chunk transfer stopped before payload encoding"
            );
        }
        return false;
    };
    let mut holders = directory.sorted_holders(&chunk);
    if !holders.contains(&local.0) {
        holders.push(local.0);
    }
    let pendings = super::cluster_world_apply::pending_action_frames(chunk)
        .into_iter()
        .map(|bytes| PendingRef { bytes })
        .collect();
    let payload = ChunkPayload::new(chunk, local.0, snapshot, pendings, holders);
    let Ok(bytes) = encode_payload(&payload) else {
        error!(chunk_x = chunk.x, chunk_z = chunk.z, to = peer.0, "cluster chunk payload encode failed");
        return false;
    };
    let payload_bytes = bytes.len();
    if outbound
        .try_send(OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::ChunkData, None),
            bytes,
        })
        .is_err()
    {
        error!(chunk_x = chunk.x, chunk_z = chunk.z, to = peer.0, "cluster chunk snapshot send failed");
        false
    } else {
        SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
        if chunk_diagnostic_cooldown_elapsed(1) {
            info!(
                target: "cluster_chunk",
                stage = "payload_encoded_enqueued",
                chunk_x = chunk.x,
                chunk_z = chunk.z,
                source = local.0,
                destination = peer.0,
                payload_bytes,
                "cluster chunk payload entered transport"
            );
        }
        true
    }
}

pub(super) fn request_primary_chunk_load(server: &Arc<Server>, addr: ChunkAddr) {
    let Some(world) = server.worlds.load().first().cloned() else {
        return;
    };
    let pos = Vector2::new(addr.x, addr.z);
    world.level.pin_cluster_chunk(pos);
    world.level.request_cluster_full_chunk(pos);
}

fn handle_chunk_availability(
    server: &Arc<Server>,
    local: ServerId,
    is_primary: bool,
    directory: &mut Directory,
    advertised: &mut HashSet<ChunkAddr>,
    primary_waiters: &mut HashMap<ChunkAddr, HashSet<ServerId>>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    availability: ClusterChunkAvailability,
) {
    let (addr, full) = match availability {
        ClusterChunkAvailability::Full(pos) => (ChunkAddr { x: pos.x, z: pos.y }, true),
        ClusterChunkAvailability::Drop(pos) => (ChunkAddr { x: pos.x, z: pos.y }, false),
    };
    if full {
        let pos = Vector2::new(addr.x, addr.z);
        let is_full = server
            .worlds
            .load()
            .first()
            .is_some_and(|world| world.level.is_cluster_full(&pos));
        if !is_full {
            if chunk_warn_cooldown_elapsed() {
                error!(
                    chunk_x = addr.x,
                    chunk_z = addr.z,
                    "cluster full chunk availability arrived without a full chunk"
                );
            }
            return;
        }
        if is_primary {
            if chunk_diagnostic_cooldown_elapsed(2) {
                info!(
                    target: "cluster_chunk",
                    stage = "primary_full_available_save_requested",
                    chunk_x = addr.x,
                    chunk_z = addr.z,
                    source = local.0,
                    "cluster primary full chunk is retained and scheduled for save"
                );
            }
            if let Some(world) = server.worlds.load().first() {
                world.level.should_save.store(true, Ordering::Release);
                world.level.level_channel.notify();
            }
        }
        directory.apply_advert(ChunkAdvert { holder: local.0, chunk: addr });
        if advertised.insert(addr) {
            announce_holds(
                outbound,
                fallback,
                ChunkAnnounce::Acquire(ChunkAdvert { holder: local.0, chunk: addr }),
            );
            if chunk_diagnostic_cooldown_elapsed(3) {
                info!(
                    target: "cluster_chunk",
                    stage = "full_chunk_advertised",
                    chunk_x = addr.x,
                    chunk_z = addr.z,
                    source = local.0,
                    primary = is_primary,
                    peers = fallback.len(),
                    "cluster full chunk availability was advertised"
                );
            }
        }
        if is_primary {
            super::cluster_entity_boundary::notify_primary_chunk_full(addr);
            if let Some(waiters) = primary_waiters.remove(&addr) {
                let mut undelivered = HashSet::new();
                for peer in waiters {
                    if !send_chunk_payload(server, local, directory, outbound, addr, peer) {
                        undelivered.insert(peer);
                    }
                }
                if !undelivered.is_empty() {
                    error!(
                        chunk_x = addr.x,
                        chunk_z = addr.z,
                        waiters = undelivered.len(),
                        "cluster full chunk could not be delivered to waiting peer"
                    );
                    primary_waiters.insert(addr, undelivered);
                } else {
                    for world in server.worlds.load().iter() {
                        world.level.finish_cluster_full_chunk_request(pos);
                    }
                }
            }
        }
        return;
    }
    if advertised.remove(&addr) {
        directory.apply_drop(ChunkDrop { holder: local.0, chunk: addr });
        announce_holds(
            outbound,
            fallback,
            ChunkAnnounce::Release(ChunkDrop { holder: local.0, chunk: addr }),
        );
    }
    if is_primary {
        primary_waiters.remove(&addr);
    }
}

fn handle_chunk_parcel(
    server: &Arc<Server>,
    local: ServerId,
    primary: ServerId,
    is_primary: bool,
    directory: &mut Directory,
    wanted: &mut HashSet<ChunkAddr>,
    pending: &mut HashMap<ChunkAddr, (u16, u64)>,
    pings: &mut PingTracker,
    claims: &mut PrimaryClaims,
    primary_waiters: &mut HashMap<ChunkAddr, HashSet<ServerId>>,
    stuck_reported: &mut HashSet<ChunkAddr>,
    unroutable_reported: &mut HashSet<ChunkAddr>,
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
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
                if advert.holder != parcel.peer.0 {
                    warn!(peer = parcel.peer.0, holder = advert.holder, "cluster chunk acquire holder mismatch");
                    return;
                }
                directory.apply_advert(advert);
                if wanted.contains(&advert.chunk) {
                    if request_chunk_once(
                        directory,
                        pings,
                        primary,
                        fallback,
                        outbound,
                        pending,
                        advert.chunk,
                    ) {
                        unroutable_reported.remove(&advert.chunk);
                    }
                }
            }
            ChunkAnnounce::Release(drop) => {
                if drop.holder != parcel.peer.0 {
                    warn!(peer = parcel.peer.0, holder = drop.holder, "cluster chunk drop holder mismatch");
                    return;
                }
                directory.apply_drop(drop);
                if wanted.contains(&drop.chunk)
                    && pending.get(&drop.chunk).is_some_and(|(from, _)| *from == drop.holder)
                {
                    pending.remove(&drop.chunk);
                    stuck_reported.remove(&drop.chunk);
                    if chunk_source(directory, pings, primary, fallback, drop.chunk)
                        .is_some_and(|next| next != drop.holder)
                        && request_chunk_once(
                            directory,
                            pings,
                            primary,
                            fallback,
                            outbound,
                            pending,
                            drop.chunk,
                        )
                    {
                        unroutable_reported.remove(&drop.chunk);
                    } else {
                        error!(
                            chunk_x = drop.chunk.x,
                            chunk_z = drop.chunk.z,
                            source = drop.holder,
                            "cluster chunk source dropped without another eligible holder"
                        );
                    }
                }
            }
        }
        return;
    }
    if parcel.header.kind == StreamKind::ChunkRequest {
        let Ok(fetch) = decode_request(&parcel.bytes) else {
            info!(
                target: "cluster_chunk",
                peer = parcel.peer.0,
                bytes = parcel.bytes.len(),
                "cluster chunk request decode failed"
            );
            return;
        };
        if fetch.from != local.0 {
            warn!(peer = parcel.peer.0, requested = fetch.from, local = local.0, "cluster chunk request sent to wrong holder");
            return;
        }
        info!(
            target: "cluster_chunk",
            chunk_x = fetch.chunk.x,
            chunk_z = fetch.chunk.z,
            from = parcel.peer.0,
            "cluster chunk request received"
        );
        let now = chunk_now_millis();
        let first_claim = is_primary && !claims.is_retained(&fetch.chunk, now);
        if is_primary {
            claims.note_requested(fetch.chunk, now);
            if let Some(world) = server.worlds.load().first() {
                world
                    .level
                    .pin_cluster_chunk(Vector2::new(fetch.chunk.x, fetch.chunk.z));
            }
        }
        if send_chunk_payload(server, local, directory, outbound, fetch.chunk, parcel.peer) {
            return;
        }
        if !is_primary {
            announce_holds(
                outbound,
                fallback,
                ChunkAnnounce::Release(ChunkDrop {
                    holder: local.0,
                    chunk: fetch.chunk,
                }),
            );
            return;
        }
        primary_waiters.entry(fetch.chunk).or_default().insert(parcel.peer);
        if first_claim {
            request_primary_chunk_load(server, fetch.chunk);
        }
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
    let Some((expected, started)) = pending.get(&payload.chunk).copied() else {
        warn!(peer = parcel.peer.0, chunk_x = payload.chunk.x, chunk_z = payload.chunk.z, "cluster unsolicited chunk payload ignored");
        return;
    };
    if expected != parcel.peer.0 || payload.holder != parcel.peer.0 {
        warn!(peer = parcel.peer.0, expected, holder = payload.holder, chunk_x = payload.chunk.x, chunk_z = payload.chunk.z, "cluster payload source mismatch ignored");
        return;
    }
    if chunk_diagnostic_cooldown_elapsed(4) {
        info!(
            target: "cluster_chunk",
            stage = "payload_received",
            chunk_x = payload.chunk.x,
            chunk_z = payload.chunk.z,
            source = parcel.peer.0,
            destination = local.0,
            payload_bytes = parcel.bytes.len(),
            "cluster chunk payload reached secondary chunk actor"
        );
    }
    let worlds = server.worlds.load();
    let Some(world) = worlds.first() else {
        return;
    };
    match cluster_decode_snapshot(
        payload.chunk.x,
        payload.chunk.z,
        &payload.snapshot,
    ) {
        Some(chunk) if chunk.status == ChunkStatus::Full => {
            for holder in payload.holders.iter().copied().chain(std::iter::once(payload.holder)) {
                directory.apply_advert(ChunkAdvert { holder, chunk: payload.chunk });
            }
            super::cluster_world_apply::ingest_chunk_snapshot_pendings(
                payload.chunk,
                payload.pendings.into_iter().map(|pending| pending.bytes).collect(),
            );
            FETCHED_CHUNKS.fetch_add(1, Ordering::Relaxed);
            pings.record(parcel.peer.0, chunk_now_millis().saturating_sub(started));
            wanted.remove(&payload.chunk);
            pending.remove(&payload.chunk);
            stuck_reported.remove(&payload.chunk);
            unroutable_reported.remove(&payload.chunk);
            trace!(
                chunk_x = payload.chunk.x,
                chunk_z = payload.chunk.z,
                holder = payload.holder,
                fetched = FETCHED_CHUNKS.load(Ordering::Relaxed),
                "cluster chunk snapshot arrived"
            );
            world.level.store_cluster_snapshot(
                Vector2::new(payload.chunk.x, payload.chunk.z),
                &chunk,
            );
            if chunk_diagnostic_cooldown_elapsed(5) {
                info!(
                    target: "cluster_chunk",
                    stage = "payload_decoded_snapshot_stored",
                    chunk_x = payload.chunk.x,
                    chunk_z = payload.chunk.z,
                    source = parcel.peer.0,
                    destination = local.0,
                    "cluster full chunk snapshot is ready for secondary installation"
                );
            }
        }
        Some(_) => {
            if chunk_warn_cooldown_elapsed() {
                warn!(
                    chunk_x = payload.chunk.x,
                    chunk_z = payload.chunk.z,
                    "cluster chunk snapshot was not full"
                );
            }
        }
        None => {
            if chunk_warn_cooldown_elapsed() {
                warn!(
                    chunk_x = payload.chunk.x,
                    chunk_z = payload.chunk.z,
                    "cluster chunk snapshot decode failed"
                );
            }
        }
    }
}

fn snapshot_chunk(server: &Arc<Server>, addr: ChunkAddr) -> Option<Vec<u8>> {
    let worlds = server.worlds.load();
    let world = worlds.first()?;
    let pos = Vector2::new(addr.x, addr.z);
    if let Some(entry) = world.level.loaded_chunks.get(&pos) {
        if entry.value().status == ChunkStatus::Full {
            return Some(cluster_encode_snapshot(entry.value()));
        }
    }
    None
}

fn persist_admin(server: &Arc<Server>, mutation: &AdminMutation) {
    let Some(handle) = server.primary_save.as_ref() else {
        return;
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0);
    if submit_admin_mutation(handle, disciplined_tick_stamp(millis), mutation).is_err()
    {
        warn!("cluster admin mutation persist queue full");
    }
}

async fn admin_task(
    server: Arc<Server>,
    mut control_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut ops = InMemoryOpStore::new();
    let mut bans = InMemoryBanStore::new();
    while let Some(parcel) = control_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        note_cluster_handshake(&parcel);
        let Ok(message) = decode_control_message(&parcel.bytes) else {
            debug!("cluster control parcel is not an admin message");
            continue;
        };
        super::cluster_admin_apply::handle_admin_message_for_server(&server, &message);
        let mutation = match &message {
            AdminControlMessage::Mutation(mutation) => mutation,
            AdminControlMessage::MutationWithAudit(audit) => &audit.mutation,
            _ => continue,
        };
        if apply_mutation_to_stores(&mut ops, &mut bans, &mutation) {
            ADMIN_APPLIED.fetch_add(1, Ordering::Relaxed);
            info!(
                from = parcel.peer.0,
                applied = ADMIN_APPLIED.load(Ordering::Relaxed),
                "cluster admin mutation applied"
            );
            persist_admin(&server, &mutation);
        } else {
            debug!("cluster admin mutation already applied");
        }
    }
}

async fn accept_task(server: Arc<Server>, mut accept_rx: mpsc::Receiver<InboundParcel>) {
    while let Some(parcel) = accept_rx.recv().await {
        if parcel.header.kind != StreamKind::Accept {
            continue;
        }
        super::cluster_world_apply::submit_accept_batch(parcel.peer, parcel.bytes);
        if server.primary_save.is_some() {
            debug!(from = parcel.peer.0, "primary received holder acceptance");
        }
    }
}

async fn fuse_task(
    mut fused_rx: mpsc::Receiver<Vec<u8>>,
    outbound: mpsc::Sender<OutboundParcel>,
    peers: Vec<ServerId>,
) {
    while let Some(batch) = fused_rx.recv().await {
        cluster_datagram::forward_fused_batch(&batch);
        let mut sent = 0_u64;
        for peer in &peers {
            let parcel = OutboundParcel {
                peer: *peer,
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: batch.clone(),
            };
            if outbound.try_send(parcel).is_ok() {
                sent = sent.saturating_add(1);
            }
        }
        if sent > 0 {
            FUSED_FORWARDED.fetch_add(1, Ordering::Relaxed);
            debug!(
                bytes = batch.len(),
                peers = sent,
                "cluster fused tick forwarded"
            );
        }
    }
}

#[cfg(test)]
mod discipline_tests {
    use super::*;
    use pumpkin_cluster::ntp::{publish_shared_offset, withdraw_shared_offset};

    #[test]
    fn primary_fallback_is_routable_before_holder_advertisement() {
        let directory = Directory::new();
        let pings = PingTracker::new();
        assert_eq!(
            chunk_source(
                &directory,
                &pings,
                ServerId(0),
                &[0, 2],
                ChunkAddr { x: 12, z: -7 },
            ),
            Some(0)
        );
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
