use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use pumpkin_cluster::accept::{Acceptor, decode_accept};
use pumpkin_cluster::admin_sync::{
    AdminControlMessage, AdminMutation, InMemoryBanStore, InMemoryOpStore, OpGrant, OpStore,
    apply_mutation_to_stores, decode_control_message, mutation_parcels_for_peers,
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
    ChunkPayload, decode_announce, decode_payload, decode_request, encode_announce,
    encode_payload, encode_request,
};
use pumpkin_config::{ClusterConfig, ClusterRole};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::level::{
    ClusterFetchRequest, ClusterLogoutGrace, cluster_decode_snapshot, cluster_encode_snapshot,
    set_cluster_fetch_sender, set_cluster_has_peers, set_cluster_logout_sender,
    set_cluster_secondary, set_cluster_unwant_sender,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::Server;
use super::cluster_regions;
use super::cluster_combat_apply::spawn_combat_apply;
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
static CLUSTER_LIFECYCLE_STATE: AtomicU64 = AtomicU64::new(0);
static CLUSTER_MESH_READY: AtomicBool = AtomicBool::new(false);
static CLUSTER_IS_PRIMARY: AtomicBool = AtomicBool::new(false);
static CLUSTER_PENDING_SYNC: AtomicU64 = AtomicU64::new(0);
static CLUSTER_INFLIGHT_FETCH: AtomicU64 = AtomicU64::new(0);
static CLUSTER_DROPPED_SYNC: AtomicU64 = AtomicU64::new(0);
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
pub fn cluster_player_join_allowed() -> bool {
    if !CLUSTER_MESH_READY.load(Ordering::Relaxed) {
        return true;
    }
    if CLUSTER_IS_PRIMARY.load(Ordering::Relaxed) {
        return false;
    }
    true
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
    info!(
        context,
        players,
        owned_entities = 0,
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
    let request = LifecycleInput::RequestLeave {
        player_count,
        owned: Vec::new(),
        holders: Vec::new(),
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

fn send_handoff(outbound: &mpsc::Sender<OutboundParcel>, to: &[u16], request: &EntityHandoffRequest, local: u16) {
    let Ok(bytes) = postcard::to_allocvec(request) else {
        warn!(server_id = local, "cluster handoff encode failed");
        return;
    };
    if to.is_empty() {
        warn!(server_id = local, leaver = request.leaver, entities = request.handoffs.len(), "cluster handoff has no targets, broadcasting to mesh");
    }
    for peer in to {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        if outbound.try_send(parcel).is_err() {
            warn!(server_id = local, to = peer, "cluster handoff send dropped: mesh queue full");
        }
    }
    CLUSTER_HANDOFF_SENT.fetch_add(1, Ordering::Relaxed);
}

async fn lifecycle_effect_task(
    server: Arc<Server>,
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
    peers: Vec<ServerId>,
    mut effects: mpsc::Receiver<LifecycleEffect>,
    peer_reset: mpsc::Sender<u16>,
    peer_ready: mpsc::Sender<u16>,
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
                let targets: Vec<u16> = if fallback.is_empty() { Vec::new() } else { fallback.clone() };
                send_handoff(&outbound, &targets, &request, local.0);
            }
            LifecycleEffect::Joined { candidate } => {
                info!(server_id = local.0, candidate, "cluster peer joined");
                if candidate == local.0 {
                    CLUSTER_ADMITTED.store(true, Ordering::Relaxed);
                    CLUSTER_LIFECYCLE_STATE.store(2, Ordering::Relaxed);
                } else {
                    super::cluster_admin_apply::publish_ops_to_peer(&server, candidate);
                    super::cluster_hide::publish_hidden_to_peer(&server, candidate);
                    super::cluster_presence::publish_roster_to_peer(&server, candidate);
                    let _ = peer_ready.try_send(candidate);
                }
            }
            LifecycleEffect::Left { peer } => {
                info!(server_id = local.0, peer, "cluster peer left");
                if peer == local.0 {
                    CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
                } else {
                    super::cluster_presence::forget_remote_server(peer);
                    let _ = peer_reset.try_send(peer);
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
                    CLUSTER_ADMITTED.store(true, Ordering::Relaxed);
                } else if state == LifecycleState::Solo || state == LifecycleState::Joining {
                    CLUSTER_ADMITTED.store(false, Ordering::Relaxed);
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
            let _ = input.try_send(LifecycleInput::RemoteHandoff(request.clone()));
        }
        return;
    };
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
    server.spawn_task(async move {
        transport.run().await;
    });

    let (demux_senders, demux_receivers) = demux_channels(4096);
    let (control_tx, control_rx) = mpsc::channel(64);
    for peer in &peer_ids {
        let _ = control_tx.try_send(DemuxControl::OpenShared { peer: *peer });
    }
    let inbound_rx = mesh_channels.inbound_rx;
    let restart_rx = mesh_channels.restart_rx;
    let outbound_tx = mesh_channels.outbound_tx;
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

    spawn_world_apply(&server, demux_receivers.world);

    server.spawn_task(async move {
        let mut restart_rx = restart_rx;
        while let Some(peer) = restart_rx.recv().await {
            if let Some(input) = LIFECYCLE_IN.get() {
                let _ = input.try_send(LifecycleInput::NotePeerRestarted { peer: peer.0 });
                let _ = input.try_send(LifecycleInput::NotePeerConnected { peer: peer.0 });
            }
        }
    });

    let (peer_reset_tx, peer_reset_rx) = mpsc::channel::<u16>(64);
    let (peer_ready_tx, peer_ready_rx) = mpsc::channel::<u16>(64);
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
        peer_ready_rx,
    ));

    spawn_combat_apply(&server, local, demux_receivers.combat);
    spawn_entity_apply(&server, local, demux_receivers.entity);

    server.spawn_task(accept_task(
        server.clone(),
        demux_receivers.accept,
    ));

    super::cluster_entity_apply::install_entity_outbox(local, fallback.clone(), outbound_tx.clone());
    cluster_chat_out::install_chat_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_admin_apply::install_admin_outbox(fallback.clone(), outbound_tx.clone());
    super::cluster_invsee::install_invsee_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_moderation::install_moderation_outbox(local, outbound_tx.clone());

    super::cluster_hide::install_hide_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_world_time::install_world_time_outbox(fallback.clone(), outbound_tx.clone());
    let (admin_tx, admin_rx) = mpsc::channel::<InboundParcel>(1024);
    let (chat_tx, chat_rx) = mpsc::channel::<InboundParcel>(1024);
    let (hide_tx, hide_rx) = mpsc::channel::<InboundParcel>(1024);
    let (invsee_tx, invsee_rx) = mpsc::channel::<InboundParcel>(1024);
    let (presence_tx, presence_rx) = mpsc::channel::<InboundParcel>(1024);
    let (world_time_tx, world_time_rx) = mpsc::channel::<InboundParcel>(1024);
    super::cluster_presence::install_presence_outbox(local, fallback.clone(), outbound_tx.clone());
    super::cluster_tick_pump::install_tick_pump_outbox(peer_ids.clone(), outbound_tx.clone());
    cluster_chat_in::spawn_control_fanout(
        server,
        demux_receivers.control,
        admin_tx,
        chat_tx,
        invsee_tx,
        presence_tx,
        hide_tx,
        world_time_tx,
    );
    super::cluster_hide::spawn_hide_apply(server, hide_rx);
    super::cluster_world_time::spawn_world_time_apply(server, world_time_rx);
    cluster_chat_in::spawn_chat_delivery(server, local, chat_rx);
    super::cluster_invsee::spawn_invsee_apply(server, local, invsee_rx);
    super::cluster_presence::spawn_presence_apply(server, local, presence_rx);

    let admin_peers = fallback.clone();
    server.spawn_task(admin_task(
        server.clone(),
        local,
        admin_peers,
        outbound_tx.clone(),
        admin_rx,
    ));

    let (fused_tx, fused_rx) = mpsc::channel::<Vec<u8>>(8);
    let _ = FUSED_BRIDGE.set(fused_tx);
    server.spawn_task(fuse_task(fused_rx, outbound_tx.clone(), peer_ids.clone()));

    CLUSTER_IS_PRIMARY.store(is_primary, Ordering::Relaxed);
    CLUSTER_MESH_READY.store(true, Ordering::Relaxed);
    CLUSTER_LIFECYCLE_STATE.store(0, Ordering::Relaxed);
    CLUSTER_ADMITTED.store(peer_ids.is_empty(), Ordering::Relaxed);
    let (lifecycle_in_tx, mut lifecycle_in_rx) = mpsc::channel(32);
    let (lifecycle_out_tx, lifecycle_out_rx) = mpsc::channel::<LifecycleEffect>(32);
    let lifecycle = Lifecycle::new_solo(local.0);
    let peer_list: Vec<u16> = peer_ids.iter().map(|peer| peer.0).collect();
    let _ = lifecycle_in_tx.try_send(LifecycleInput::BeginJoin {
        cert_fingerprint: fingerprint,
        peers: peer_list,
    });
    let _ = LIFECYCLE_IN.set(lifecycle_in_tx);
    server.spawn_task(async move {
        run_lifecycle(lifecycle, &mut lifecycle_in_rx, &lifecycle_out_tx).await;
    });
    server.spawn_task(lifecycle_effect_task(
        server.clone(),
        local,
        outbound_tx.clone(),
        peer_ids.clone(),
        lifecycle_out_rx,
        peer_reset_tx,
        peer_ready_tx,
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
    mut peer_ready_rx: mpsc::Receiver<u16>,
) {
    let mut directory = Directory::new();
    let mut wanted: HashSet<ChunkAddr> = HashSet::new();
    let mut pending: HashMap<ChunkAddr, (u16, u64)> = HashMap::new();
    let mut pings = PingTracker::new();
    let mut advertised: HashSet<ChunkAddr> = HashSet::new();
    let mut claims = PrimaryClaims::new();
    let mut stuck_reported: HashSet<ChunkAddr> = HashSet::new();
    let mut unroutable_reported: HashSet<ChunkAddr> = HashSet::new();
    let mut grace: HashMap<ChunkAddr, u64> = HashMap::new();
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            incoming = chunk_rx.recv() => {
                let Some(parcel) = incoming else { break };
                handle_chunk_parcel(&server, local.0, &mut directory, &mut wanted, &mut pending, &mut pings, &mut advertised, &mut claims, &mut stuck_reported, &mut unroutable_reported, &fallback, &outbound, parcel).await;
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
                if request_chunk_once(
                    &directory,
                    &pings,
                    &fallback,
                    &outbound,
                    &mut pending,
                    addr,
                    None,
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
                    if request_chunk_once(
                        &directory,
                        &pings,
                        &fallback,
                        &outbound,
                        &mut pending,
                        addr,
                        None,
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
            reset = peer_reset_rx.recv() => {
                let Some(peer) = reset else { break };
                let mut retry: Vec<ChunkAddr> = Vec::new();
                pending.retain(|addr, (from, _)| {
                    if *from == peer {
                        retry.push(*addr);
                        false
                    } else {
                        true
                    }
                });
                for addr in retry {
                    stuck_reported.remove(&addr);
                    if request_chunk_once(
                        &directory,
                        &pings,
                        &fallback,
                        &outbound,
                        &mut pending,
                        addr,
                        Some(peer),
                    ) {
                        unroutable_reported.remove(&addr);
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
            }
            ready = peer_ready_rx.recv() => {
                let Some(_peer) = ready else { break };
                let retry: Vec<ChunkAddr> = pending
                    .keys()
                    .copied()
                    .filter(|addr| stuck_reported.contains(addr))
                    .collect();
                for addr in retry {
                    pending.remove(&addr);
                    stuck_reported.remove(&addr);
                    if request_chunk_once(
                        &directory,
                        &pings,
                        &fallback,
                        &outbound,
                        &mut pending,
                        addr,
                        None,
                    ) {
                        unroutable_reported.remove(&addr);
                    }
                }
                CLUSTER_INFLIGHT_FETCH.store(pending.len() as u64, Ordering::Relaxed);
            }
            _ = sweep.tick() => {
                let now_millis = chunk_now_millis();
                release_expired_claims(&server, &directory, &mut claims, now_millis);
                report_stuck_fetches(&pending, &mut stuck_reported, now_millis);
                report_unroutable_wants(&wanted, &pending, &mut unroutable_reported);
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
        let pos = Vector2::new(addr.x, addr.z);
        for world in server.worlds.load().iter() {
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
    fallback: &[u16],
    outbound: &mpsc::Sender<OutboundParcel>,
    pending: &mut HashMap<ChunkAddr, (u16, u64)>,
    addr: ChunkAddr,
    exclude: Option<u16>,
) -> bool {
    if pending.contains_key(&addr) {
        return true;
    }
    let holders: Vec<u16> = directory
        .sorted_holders(&addr)
        .into_iter()
        .filter(|holder| Some(*holder) != exclude)
        .collect();
    let peers: Vec<u16> = fallback
        .iter()
        .copied()
        .filter(|peer| Some(*peer) != exclude)
        .collect();
    let candidates: &[u16] = if holders.is_empty() {
        &peers
    } else {
        &holders
    };
    let Some(from) = pings.best(candidates) else {
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

async fn handle_chunk_parcel(
    server: &Arc<Server>,
    local: u16,
    directory: &mut Directory,
    wanted: &mut HashSet<ChunkAddr>,
    pending: &mut HashMap<ChunkAddr, (u16, u64)>,
    pings: &mut PingTracker,
    advertised: &mut HashSet<ChunkAddr>,
    claims: &mut PrimaryClaims,
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
                directory.apply_advert(advert);
                if wanted.contains(&advert.chunk) {
                    if request_chunk_once(
                        directory,
                        pings,
                        fallback,
                        outbound,
                        pending,
                        advert.chunk,
                        None,
                    ) {
                        unroutable_reported.remove(&advert.chunk);
                    }
                }
            }
            ChunkAnnounce::Release(drop) => {
                directory.apply_drop(drop);
                if wanted.contains(&drop.chunk)
                    && pending.get(&drop.chunk).is_some_and(|(from, _)| *from == drop.holder)
                {
                    pending.remove(&drop.chunk);
                    stuck_reported.remove(&drop.chunk);
                    if request_chunk_once(
                        directory,
                        pings,
                        fallback,
                        outbound,
                        pending,
                        drop.chunk,
                        Some(drop.holder),
                    ) {
                        unroutable_reported.remove(&drop.chunk);
                        let worlds = server.worlds.load();
                        if let Some(world) = worlds.first() {
                            world.level.clear_cluster_fetch_wanted(&Vector2::new(
                                drop.chunk.x,
                                drop.chunk.z,
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
            info!(
                target: "cluster_chunk",
                peer = parcel.peer.0,
                bytes = parcel.bytes.len(),
                "cluster chunk request decode failed"
            );
            return;
        };
        info!(
            target: "cluster_chunk",
            chunk_x = fetch.chunk.x,
            chunk_z = fetch.chunk.z,
            from = parcel.peer.0,
            "cluster chunk request received"
        );
        claims.note_requested(fetch.chunk, chunk_now_millis());
        if let Some(world) = server.worlds.load().first() {
            world
                .level
                .pin_cluster_chunk(Vector2::new(fetch.chunk.x, fetch.chunk.z));
        }
        let Some((snapshot, loaded)) = snapshot_chunk(server, fetch.chunk).await else {
            info!(
                target: "cluster_chunk",
                chunk_x = fetch.chunk.x,
                chunk_z = fetch.chunk.z,
                from = parcel.peer.0,
                "cluster chunk not held, drop announced"
            );
            announce_holds(
                outbound,
                fallback,
                ChunkAnnounce::Release(ChunkDrop {
                    holder: local,
                    chunk: fetch.chunk,
                }),
            );
            return;
        };
        info!(
            target: "cluster_chunk",
            chunk_x = fetch.chunk.x,
            chunk_z = fetch.chunk.z,
            to = parcel.peer.0,
            loaded,
            "cluster chunk snapshot ready"
        );
        let payload = ChunkPayload::new(fetch.chunk, local, snapshot, Vec::new(), vec![local]);
        let Ok(bytes) = encode_payload(&payload) else {
            info!(
                target: "cluster_chunk",
                chunk_x = fetch.chunk.x,
                chunk_z = fetch.chunk.z,
                to = parcel.peer.0,
                "cluster chunk payload encode failed"
            );
            return;
        };
        SERVED_CHUNKS.fetch_add(1, Ordering::Relaxed);
        if outbound
            .try_send(OutboundParcel {
                peer: parcel.peer,
                header: StreamHeader::new(StreamKind::ChunkData, None),
                bytes,
            })
            .is_err()
        {
            error!(
                target: "cluster_chunk",
                chunk_x = fetch.chunk.x,
                chunk_z = fetch.chunk.z,
                to = parcel.peer.0,
                "cluster chunk snapshot send failed"
            );
        } else {
            info!(
                target: "cluster_chunk",
                chunk_x = fetch.chunk.x,
                chunk_z = fetch.chunk.z,
                to = parcel.peer.0,
                served = SERVED_CHUNKS.load(Ordering::Relaxed),
                "cluster chunk snapshot served"
            );
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
    if !wanted.contains(&payload.chunk) {
        return;
    }
    if let Some((_, at)) = pending.get(&payload.chunk) {
        pings.record(parcel.peer.0, chunk_now_millis().saturating_sub(*at));
    }
    wanted.remove(&payload.chunk);
    pending.remove(&payload.chunk);
    stuck_reported.remove(&payload.chunk);
    unroutable_reported.remove(&payload.chunk);
    let worlds = server.worlds.load();
    let Some(world) = worlds.first() else {
        return;
    };
    match cluster_decode_snapshot(
        payload.chunk.x,
        payload.chunk.z,
        &payload.snapshot,
    ) {
        Some(chunk) => {
            FETCHED_CHUNKS.fetch_add(1, Ordering::Relaxed);
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
        None => {
            world.level.clear_cluster_fetch_wanted(&Vector2::new(
                payload.chunk.x,
                payload.chunk.z,
            ));
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

async fn snapshot_chunk(server: &Arc<Server>, addr: ChunkAddr) -> Option<(Vec<u8>, bool)> {
    let worlds = server.worlds.load();
    let world = worlds.first()?;
    let pos = Vector2::new(addr.x, addr.z);
    if let Some(entry) = world.level.loaded_chunks.get(&pos) {
        return Some((cluster_encode_snapshot(entry.value()), true));
    }
    world.level.get_or_fetch_chunk(pos, |_| ()).await;
    world
        .level
        .loaded_chunks
        .get(&pos)
        .map(|entry| (cluster_encode_snapshot(entry.value()), false))
}

fn seed_grants(server: &Arc<Server>) -> Vec<OpGrant> {
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
        return Vec::new();
    }
    let guard = server
        .data
        .operator_config
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard
        .ops
        .iter()
        .map(|op| {
            OpGrant::new(
                *op.uuid.as_bytes(),
                op.name.clone(),
                op.level as u8,
                op.bypasses_player_limit,
            )
        })
        .collect()
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
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut control_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut ops = InMemoryOpStore::new();
    let mut bans = InMemoryBanStore::new();
    for grant in seed_grants(&server) {
        let grant_uuid = uuid::Uuid::from_bytes(grant.uuid);
        if ops.grant(&grant) {
            info!(
                server_id = local.0,
                uuid = %grant_uuid,
                level = grant.level,
                "cluster admin seed grant applied locally"
            );
        }
        let mutation = AdminMutation::GrantOp(grant);
        match mutation_parcels_for_peers(&mutation, &peers) {
            Ok(parcels) => {
                let mut sent = 0_usize;
                for parcel in parcels {
                    let outbound_parcel = OutboundParcel {
                        peer: ServerId(parcel.peer),
                        header: StreamHeader::new(parcel.kind, None),
                        bytes: parcel.bytes,
                    };
                    if outbound.try_send(outbound_parcel).is_ok() {
                        sent = sent.saturating_add(1);
                    }
                }
                if sent == peers.len() {
                    info!(
                        uuid = %grant_uuid,
                        peers = peers.len(),
                        "cluster admin seed grant broadcast"
                    );
                } else {
                    warn!(
                        uuid = %grant_uuid,
                        sent = sent,
                        peers = peers.len(),
                        "cluster admin seed grant partially broadcast"
                    );
                }
            }
            Err(_) => {
                warn!(
                    uuid = %grant_uuid,
                    "cluster admin seed grant encode failed"
                );
            }
        }
    }
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
        super::cluster_moderation::handle_moderation_parcel(&server, &parcel);
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
    const MAX_SYNC_TICKS: usize = 256;
    const SYNC_EXPIRY_SECS: u64 = 30;
    let mut acceptor = Acceptor::new();
    let mut expiry = tokio::time::interval(Duration::from_secs(SYNC_EXPIRY_SECS));
    expiry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            incoming = accept_rx.recv() => {
                let Some(parcel) = incoming else { break };
                if parcel.header.kind != StreamKind::Accept {
                    continue;
                }
                match decode_accept(&parcel.bytes) {
                    Ok(batch) => {
                        if batch.is_empty() {
                            debug!(tick = batch.tick.0, "cluster accept batch applied");
                        } else {
                            info!(
                                tick = batch.tick.0,
                                decisions = batch.len(),
                                "cluster accept batch applied"
                            );
                        }
                        if let Some(handle) = server.primary_save.as_ref() {
                            let _ = handle.try_submit(batch.tick, parcel.bytes.clone());
                        }
                        acceptor.apply_accept(batch);
                        CLUSTER_PENDING_SYNC.store(acceptor.pending_ticks() as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        warn!("cluster accept batch decode failed");
                    }
                }
            }
            _ = expiry.tick() => {
                let dropped = acceptor.expire_stale(MAX_SYNC_TICKS);
                if dropped > 0 {
                    CLUSTER_DROPPED_SYNC.fetch_add(dropped as u64, Ordering::Relaxed);
                    warn!(
                        dropped,
                        pending = acceptor.pending_ticks(),
                        "cluster sync buckets expired stale entries without waiting"
                    );
                }
                CLUSTER_PENDING_SYNC.store(acceptor.pending_ticks() as u64, Ordering::Relaxed);
            }
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
