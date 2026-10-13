use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use quinn::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};
use quinn::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use quinn::rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as TlsError, SignatureScheme,
};
use tokio::sync::{mpsc, watch};

use crate::chunks::ChunkAnnounce;
use crate::identity::ServerId;
use crate::mesh::{self, MeshConfig, PinnedPeer};
use crate::protocol::ChunkKey;
use crate::protocol::StreamKind;
use crate::streams::{InboundParcel, OutboundParcel, StreamHeader, StreamKey};

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerStateSnapshot {
    pub connected: HashSet<ServerId>,
    pub datagram_limits: HashMap<ServerId, usize>,
    pub drop_epoch: HashMap<ServerId, u64>,
    pub admission_epoch: HashMap<ServerId, u64>,
    pub replacement_capacity_epoch: u64,
}

const DATAGRAM_BUFFER_BYTES: usize = 4 * 1024 * 1024;

const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);
const REPLACEMENT_QUIET_PERIOD: Duration = Duration::from_secs(3);
const PEER_DROP_TIMEOUT: Duration = Duration::from_secs(60);

const BASE_DIAL_BACKOFF: Duration = Duration::from_secs(1);

const MAX_DIAL_BACKOFF: Duration = Duration::from_secs(30);

const LOCAL_CERT_SAN: &str = "pumpkin-mesh";

const CHUNK_STREAM_PRIORITY: i32 = -16;

const ROUTER_BUFFER: usize = 4096;
const ADMISSION_ADVERT_LIMIT: usize = 16_384;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionPhase {
    Quarantined,
    Admission,
    Active,
}

#[derive(Debug, Default)]
pub struct TransportActivationState {
    desired_admissions: HashSet<ServerId>,
    pending_admissions: HashSet<ServerId>,
    pending_replacements: HashSet<ServerId>,
}

impl TransportActivationState {
    pub fn joined(&mut self, peer: ServerId) {
        self.desired_admissions.insert(peer);
        self.pending_admissions.insert(peer);
    }

    pub fn left(&mut self, peer: ServerId) {
        self.desired_admissions.remove(&peer);
        self.pending_admissions.remove(&peer);
        self.pending_replacements.remove(&peer);
    }

    pub fn admission_applied(&mut self, peer: ServerId) -> bool {
        self.pending_admissions.remove(&peer)
    }

    pub fn replacement_completed(&mut self, peer: ServerId) {
        self.pending_replacements.insert(peer);
    }

    pub fn publish_admissions(&self, sender: &watch::Sender<Arc<HashSet<ServerId>>>) {
        sender.send_replace(Arc::new(self.desired_admissions.clone()));
    }

    pub fn flush_replacements(&mut self, sender: &mpsc::Sender<ServerId>) {
        let peers: Vec<_> = self.pending_replacements.iter().copied().collect();
        for peer in peers {
            match sender.try_send(peer) {
                Ok(()) => {
                    self.pending_replacements.remove(&peer);
                }
                Err(mpsc::error::TrySendError::Full(_)) => return,
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    InvalidAddress { addr: String },
    KeyFile { path: String, message: String },
    IncompleteKeypair { missing_path: String },
    KeyGeneration { message: String },
    Tls { message: String },
    Bind { message: String },
    HeaderCodec { message: String },
    FrameTooLarge { bytes: usize },
    ConnectionLost,
    StreamClosed,
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidAddress { addr } => {
                write!(formatter, "invalid mesh address `{addr}`")
            }
            Self::KeyFile { path, message } => {
                write!(formatter, "key file `{path}` failed: {message}")
            }
            Self::IncompleteKeypair { missing_path } => {
                write!(formatter, "keypair half missing at `{missing_path}`")
            }
            Self::KeyGeneration { message } => {
                write!(formatter, "certificate generation failed: {message}")
            }
            Self::Tls { message } => write!(formatter, "TLS setup failed: {message}"),
            Self::Bind { message } => write!(formatter, "endpoint bind failed: {message}"),
            Self::HeaderCodec { message } => {
                write!(formatter, "stream header codec failed: {message}")
            }
            Self::FrameTooLarge { bytes } => {
                write!(formatter, "frame of {bytes} bytes exceeds the limit")
            }
            Self::ConnectionLost => write!(formatter, "connection lost"),
            Self::StreamClosed => write!(formatter, "stream closed"),
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Debug)]
pub struct TransportChannels {
    pub inbound_tx: mpsc::Sender<InboundParcel>,
    pub outbound_rx: mpsc::Receiver<OutboundParcel>,
    pub datagram_in_tx: mpsc::Sender<(ServerId, u64, Vec<u8>)>,
    pub datagram_out_rx: mpsc::Receiver<(ServerId, Vec<u8>)>,
    pub peer_state_tx: watch::Sender<Arc<PeerStateSnapshot>>,
    pub replacement_ready_rx: mpsc::Receiver<ServerId>,
    pub peer_admitted_rx: watch::Receiver<Arc<HashSet<ServerId>>>,
    pub drop_request_rx: mpsc::Receiver<ServerId>,
    pub generation_drop_request_rx: mpsc::Receiver<(ServerId, u64)>,
}

#[derive(Debug)]
pub struct MeshChannels {
    pub inbound_rx: mpsc::Receiver<InboundParcel>,
    pub outbound_tx: mpsc::Sender<OutboundParcel>,
    pub datagram_in_rx: mpsc::Receiver<(ServerId, u64, Vec<u8>)>,
    pub datagram_out_tx: mpsc::Sender<(ServerId, Vec<u8>)>,
    pub peer_state_rx: watch::Receiver<Arc<PeerStateSnapshot>>,
    pub replacement_ready_tx: mpsc::Sender<ServerId>,
    pub peer_admitted_tx: watch::Sender<Arc<HashSet<ServerId>>>,
    pub drop_request_tx: mpsc::Sender<ServerId>,
    pub generation_drop_request_tx: mpsc::Sender<(ServerId, u64)>,
}

#[must_use]
pub fn channel_pair(buffer: usize) -> (TransportChannels, MeshChannels) {
    let (inbound_tx, inbound_rx) = mpsc::channel(buffer);
    let (outbound_tx, outbound_rx) = mpsc::channel(buffer);
    let (datagram_in_tx, datagram_in_rx) = mpsc::channel(buffer);
    let (datagram_out_tx, datagram_out_rx) = mpsc::channel(buffer);
    let (peer_state_tx, peer_state_rx) = watch::channel(Arc::new(PeerStateSnapshot::default()));
    let (replacement_ready_tx, replacement_ready_rx) = mpsc::channel(buffer);
    let (peer_admitted_tx, peer_admitted_rx) = watch::channel(Arc::new(HashSet::new()));
    let (drop_request_tx, drop_request_rx) = mpsc::channel(buffer);
    let (generation_drop_request_tx, generation_drop_request_rx) = mpsc::channel(buffer);
    (
        TransportChannels {
            inbound_tx,
            outbound_rx,
            datagram_in_tx,
            datagram_out_rx,
            peer_state_tx,
            replacement_ready_rx,
            peer_admitted_rx,
            drop_request_rx,
            generation_drop_request_rx,
        },
        MeshChannels {
            inbound_rx,
            outbound_tx,
            datagram_in_rx,
            datagram_out_tx,
            peer_state_rx,
            replacement_ready_tx,
            peer_admitted_tx,
            drop_request_tx,
            generation_drop_request_tx,
        },
    )
}

pub struct Transport {
    endpoint: quinn::Endpoint,
    mesh: MeshConfig,
    channels: TransportChannels,
    local_cert_der: Vec<u8>,
}

struct EndpointGuard(quinn::Endpoint);

impl Drop for EndpointGuard {
    fn drop(&mut self) {
        self.0
            .close(quinn::VarInt::from_u32(0), b"transport stopped");
    }
}

impl Transport {
    pub fn bind(config: MeshConfig, channels: TransportChannels) -> Result<Self, TransportError> {
        let bind_addr: SocketAddr =
            config
                .bind_addr
                .parse()
                .map_err(|_| TransportError::InvalidAddress {
                    addr: config.bind_addr.clone(),
                })?;
        let (cert_der, key_der) = load_or_generate_keypair(&config)?;
        let endpoint = build_endpoint(&bind_addr, &cert_der, &key_der, &config.peers)?;
        Ok(Self {
            endpoint,
            mesh: config,
            channels,
            local_cert_der: cert_der,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.endpoint
            .local_addr()
            .map_err(|error| TransportError::Bind {
                message: error.to_string(),
            })
    }

    #[must_use]
    pub fn local_cert_fingerprint(&self) -> [u8; 32] {
        mesh::cert_fingerprint_sha256(&self.local_cert_der)
    }

    pub async fn run(self) {
        let Self {
            endpoint,
            mesh,
            channels,
            local_cert_der: _,
        } = self;
        let _endpoint_guard = EndpointGuard(endpoint.clone());
        let (router_tx, mut router_rx) = mpsc::channel::<RouterMsg>(ROUTER_BUFFER);
        for peer in &mesh.peers {
            if mesh.server < peer.server {
                tokio::spawn(dial_loop(endpoint.clone(), peer.clone(), router_tx.clone()));
            }
        }
        tokio::spawn(accept_loop(endpoint.clone(), router_tx.clone()));
        let TransportChannels {
            inbound_tx,
            outbound_rx,
            datagram_in_tx,
            mut datagram_out_rx,
            peer_state_tx,
            mut replacement_ready_rx,
            mut peer_admitted_rx,
            mut drop_request_rx,
            mut generation_drop_request_rx,
        } = channels;
        let forward_tx = router_tx.clone();
        tokio::spawn(async move {
            let mut outbound_rx = outbound_rx;
            while let Some(parcel) = outbound_rx.recv().await {
                if forward_tx.send(RouterMsg::Parcel(parcel)).await.is_err() {
                    return;
                }
            }
            let _ = forward_tx.send(RouterMsg::OutboundClosed).await;
        });
        let datagram_tx = router_tx.clone();
        tokio::spawn(async move {
            while let Some((peer, payload)) = datagram_out_rx.recv().await {
                if datagram_tx
                    .send(RouterMsg::Datagram(peer, payload))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = datagram_tx.send(RouterMsg::DatagramsClosed).await;
        });
        let drop_tx = router_tx.clone();
        tokio::spawn(async move {
            while let Some(peer) = drop_request_rx.recv().await {
                if drop_tx
                    .send(RouterMsg::ForceDrop(peer, None))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let generation_drop_tx = router_tx.clone();
        tokio::spawn(async move {
            while let Some((peer, generation)) = generation_drop_request_rx.recv().await {
                if generation_drop_tx
                    .send(RouterMsg::ForceDrop(peer, Some(generation)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let mut connections: HashMap<ServerId, quinn::Connection> = HashMap::new();
        let mut pending_replacements: HashMap<
            ServerId,
            (quinn::Connection, watch::Sender<ConnectionPhase>),
        > = HashMap::new();
        let mut admission_gates: HashMap<ServerId, watch::Sender<ConnectionPhase>> = HashMap::new();
        let mut applied_admissions = HashSet::new();
        let mut deferred_admission = HashMap::<ServerId, VecDeque<OutboundParcel>>::new();
        let mut deferred_admission_bytes = HashMap::<ServerId, usize>::new();
        let mut deferred_adverts =
            HashMap::<ServerId, HashMap<(u16, ChunkKey), OutboundParcel>>::new();
        let mut admission_only: HashSet<ServerId> = HashSet::new();
        let mut quarantined: HashSet<ServerId> = HashSet::new();
        let mut drop_gate = PeerDropGate::default();
        let mut streams: HashMap<StreamKey, StreamWriter> = HashMap::new();
        let mut peer_availability = PeerAvailability::default();
        let mut peer_state = PeerStateSnapshot::default();
        let mut outbound_open = true;
        let mut datagrams_open = true;
        let mut replacement_control_open = true;
        let mut admission_control_open = true;
        let mut drop_tick = tokio::time::interval(Duration::from_secs(1));
        drop_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while outbound_open || datagrams_open {
            let msg = tokio::select! {
                msg = router_rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    msg
                }
                _ = drop_tick.tick() => {
                    let now = Instant::now();
                    for peer in drop_gate.expire(now) {
                        quarantined.insert(peer);
                        note_peer_dropped(&peer_state_tx, &mut peer_state, peer);
                    }
                    continue;
                }
                ready = replacement_ready_rx.recv(), if replacement_control_open => {
                    let Some(peer) = ready else {
                        replacement_control_open = false;
                        continue;
                    };
                    peer_state.replacement_capacity_epoch =
                        peer_state.replacement_capacity_epoch.wrapping_add(1);
                    publish_peer_state(&peer_state_tx, &peer_state);
                    if let Some((conn, activation)) = pending_replacements.remove(&peer) {
                        if conn.close_reason().is_none() {
                            let closed = conn.clone();
                            connections.insert(peer, conn);
                            peer_availability.note_connected(peer);
                            let _ = activation.send(ConnectionPhase::Admission);
                            admission_gates.insert(peer, activation);
                            quarantined.remove(&peer);
                            note_peer_connected(
                                &peer_state_tx,
                                &mut peer_state,
                                peer,
                                closed.max_datagram_size(),
                            );
                            let disconnect_tx = router_tx.clone();
                            let stable_id = closed.stable_id();
                            tokio::spawn(async move {
                                let reason = closed.closed().await;
                                let _ = disconnect_tx
                                    .send(RouterMsg::Disconnected(peer, stable_id, reason.to_string()))
                                    .await;
                            });
                        } else {
                            quarantined.remove(&peer);
                            admission_only.insert(peer);
                        }
                    } else {
                        quarantined.remove(&peer);
                        admission_only.insert(peer);
                    }
                    continue;
                }
                changed = peer_admitted_rx.changed(), if admission_control_open => {
                    if changed.is_err() {
                        admission_control_open = false;
                        continue;
                    }
                    let desired = Arc::clone(&peer_admitted_rx.borrow_and_update());
                    let removed: Vec<_> = applied_admissions
                        .iter()
                        .filter(|peer| !desired.contains(peer))
                        .copied()
                        .collect();
                    for peer in removed {
                        applied_admissions.remove(&peer);
                        if let Some(activation) = admission_gates.get(&peer) {
                            let _ = activation.send(ConnectionPhase::Admission);
                        }
                    }
                    let added: Vec<_> = desired
                        .iter()
                        .filter(|peer| !applied_admissions.contains(peer))
                        .copied()
                        .collect();
                    for peer in added {
                        if !connections.contains_key(&peer) {
                            continue;
                        }
                        if let Some(activation) = admission_gates.get(&peer) {
                            let _ = activation.send(ConnectionPhase::Active);
                        }
                        tracing::debug!(server = peer.0, "cluster reliable peer admission active");
                        applied_admissions.insert(peer);
                        *peer_state.admission_epoch.entry(peer).or_default() += 1;
                        publish_peer_state(&peer_state_tx, &peer_state);
                        if let Some(mut deferred) = deferred_admission.remove(&peer) {
                            deferred_admission_bytes.remove(&peer);
                            while let Some(parcel) = deferred.pop_front() {
                                if !route_parcel(
                                    &connections,
                                    &mut streams,
                                    &mut peer_availability,
                                    parcel,
                                    &router_tx,
                                )
                                {
                                    if let Some(connection) = connections.remove(&peer) {
                                        connection.close(
                                            quinn::VarInt::from_u32(3),
                                            b"cluster deferred reliable stream failed",
                                        );
                                    }
                                    streams.retain(|key, _| key.peer != peer);
                                    drop_gate.note_lost(peer, Instant::now());
                                    break;
                                }
                            }
                        }
                        if let Some(adverts) = deferred_adverts.remove(&peer) {
                            for parcel in adverts.into_values() {
                                if !route_parcel(
                                    &connections,
                                    &mut streams,
                                    &mut peer_availability,
                                    parcel,
                                    &router_tx,
                                ) {
                                    if let Some(connection) = connections.remove(&peer) {
                                        connection.close(
                                            quinn::VarInt::from_u32(3),
                                            b"cluster deferred chunk advert stream failed",
                                        );
                                    }
                                    streams.retain(|key, _| key.peer != peer);
                                    drop_gate.note_lost(peer, Instant::now());
                                    break;
                                }
                            }
                        }
                    }
                    continue;
                }
            };
            match msg {
                RouterMsg::Connection(conn) => {
                    let Some(peer) = connection_peer(&conn, &mesh.peers) else {
                        tracing::warn!("closing connection from an unknown peer");
                        conn.close(quinn::VarInt::from_u32(1), b"unknown peer");
                        continue;
                    };
                    if let Some(existing) = connections.get(&peer) {
                        if existing.stable_id() == conn.stable_id() {
                            peer_availability.note_connected(peer);
                            continue;
                        }
                        conn.close(quinn::VarInt::from_u32(2), b"peer still active");
                        continue;
                    }
                    if admission_only.remove(&peer) {
                        let (activation, active) = watch::channel(ConnectionPhase::Admission);
                        start_connection_readers(
                            conn.clone(),
                            peer,
                            peer_state.drop_epoch.get(&peer).copied().unwrap_or(0),
                            inbound_tx.clone(),
                            datagram_in_tx.clone(),
                            active,
                        );
                        let closed = conn.clone();
                        connections.insert(peer, conn);
                        admission_gates.insert(peer, activation);
                        peer_availability.note_connected(peer);
                        note_peer_connected(
                            &peer_state_tx,
                            &mut peer_state,
                            peer,
                            closed.max_datagram_size(),
                        );
                        let disconnect_tx = router_tx.clone();
                        let stable_id = closed.stable_id();
                        tokio::spawn(async move {
                            let reason = closed.closed().await;
                            let _ = disconnect_tx
                                .send(RouterMsg::Disconnected(peer, stable_id, reason.to_string()))
                                .await;
                        });
                        continue;
                    }
                    if quarantined.contains(&peer) {
                        if pending_replacements.contains_key(&peer) {
                            conn.close(quinn::VarInt::from_u32(2), b"peer replacement pending");
                            continue;
                        }
                        let (activation, active) = watch::channel(ConnectionPhase::Quarantined);
                        start_connection_readers(
                            conn.clone(),
                            peer,
                            peer_state.drop_epoch.get(&peer).copied().unwrap_or(0),
                            inbound_tx.clone(),
                            datagram_in_tx.clone(),
                            active,
                        );
                        pending_replacements.insert(peer, (conn, activation));
                        continue;
                    }
                    if drop_gate.contains(peer) {
                        if !drop_gate.allow_replacement(peer, Instant::now()) {
                            conn.close(quinn::VarInt::from_u32(2), b"peer still active");
                            continue;
                        }
                        quarantined.insert(peer);
                        note_peer_dropped(&peer_state_tx, &mut peer_state, peer);
                        let (activation, active) = watch::channel(ConnectionPhase::Quarantined);
                        start_connection_readers(
                            conn.clone(),
                            peer,
                            peer_state.drop_epoch.get(&peer).copied().unwrap_or(0),
                            inbound_tx.clone(),
                            datagram_in_tx.clone(),
                            active,
                        );
                        pending_replacements.insert(peer, (conn, activation));
                        continue;
                    }
                    let closed = conn.clone();
                    if let Some((peer, activation)) = register_connection(
                        &mut connections,
                        &mut peer_availability,
                        &mesh.peers,
                        conn,
                        peer_state.drop_epoch.get(&peer).copied().unwrap_or(0),
                        &inbound_tx,
                        &datagram_in_tx,
                    ) {
                        admission_gates.insert(peer, activation);
                        drop_gate.clear(peer);
                        note_peer_connected(
                            &peer_state_tx,
                            &mut peer_state,
                            peer,
                            closed.max_datagram_size(),
                        );
                        let disconnect_tx = router_tx.clone();
                        let stable_id = closed.stable_id();
                        tokio::spawn(async move {
                            let reason = closed.closed().await;
                            let _ = disconnect_tx
                                .send(RouterMsg::Disconnected(peer, stable_id, reason.to_string()))
                                .await;
                        });
                    }
                }
                RouterMsg::Disconnected(peer, stable_id, reason) => {
                    if connections
                        .get(&peer)
                        .is_some_and(|connection| connection.stable_id() == stable_id)
                    {
                        tracing::warn!(server = peer.0, stable_id, %reason, "cluster peer connection closed");
                        deferred_admission.remove(&peer);
                        deferred_admission_bytes.remove(&peer);
                        deferred_adverts.remove(&peer);
                        connections.remove(&peer);
                        let awaiting_admission = admission_gates.remove(&peer).is_some();
                        applied_admissions.remove(&peer);
                        streams.retain(|key, _| key.peer != peer);
                        if awaiting_admission {
                            admission_only.insert(peer);
                            note_peer_dropped(&peer_state_tx, &mut peer_state, peer);
                        } else {
                            drop_gate.note_lost(peer, Instant::now());
                        }
                    }
                }
                RouterMsg::ForceDrop(peer, generation) => {
                    if !drop_generation_matches(
                        peer_state.drop_epoch.get(&peer).copied(),
                        generation,
                    ) {
                        continue;
                    }
                    deferred_admission.remove(&peer);
                    deferred_admission_bytes.remove(&peer);
                    deferred_adverts.remove(&peer);
                    if let Some(connection) = connections.remove(&peer) {
                        connection.close(
                            quinn::VarInt::from_u32(3),
                            b"cluster outbound capacity exhausted",
                        );
                    }
                    admission_gates.remove(&peer);
                    applied_admissions.remove(&peer);
                    streams.retain(|key, _| key.peer != peer);
                    if let Some((replacement, _)) = pending_replacements.remove(&peer) {
                        replacement.close(
                            quinn::VarInt::from_u32(3),
                            b"cluster outbound capacity exhausted",
                        );
                    }
                    quarantined.insert(peer);
                    drop_gate.clear(peer);
                    if peer_state.connected.contains(&peer) {
                        note_peer_dropped(&peer_state_tx, &mut peer_state, peer);
                    }
                }
                RouterMsg::StreamFailed(peer, stable_id) => {
                    if connections
                        .get(&peer)
                        .is_some_and(|connection| connection.stable_id() == stable_id)
                    {
                        deferred_admission.remove(&peer);
                        deferred_admission_bytes.remove(&peer);
                        deferred_adverts.remove(&peer);
                        if let Some(connection) = connections.remove(&peer) {
                            connection.close(
                                quinn::VarInt::from_u32(3),
                                b"cluster reliable stream failed",
                            );
                        }
                        admission_gates.remove(&peer);
                        applied_admissions.remove(&peer);
                        streams.retain(|key, _| key.peer != peer);
                        drop_gate.note_lost(peer, Instant::now());
                    }
                }
                RouterMsg::Parcel(parcel) => {
                    if admission_gates.contains_key(&parcel.peer)
                        && !applied_admissions.contains(&parcel.peer)
                    {
                        if parcel.header.kind != StreamKind::Control
                            || crate::membership::decode_handshake(&parcel.bytes).is_err()
                        {
                            let peer = parcel.peer;
                            if parcel.header.kind == StreamKind::ChunkAdvert
                                && let Ok(announce) = crate::xfer::decode_announce(&parcel.bytes)
                            {
                                let adverts = deferred_adverts.entry(peer).or_default();
                                if !defer_admission_advert(adverts, announce, parcel) {
                                    if let Some(connection) = connections.remove(&peer) {
                                        connection.close(
                                            quinn::VarInt::from_u32(3),
                                            b"cluster admission advert capacity exhausted",
                                        );
                                    }
                                    streams.retain(|stream, _| stream.peer != peer);
                                    drop_gate.note_lost(peer, Instant::now());
                                }
                                continue;
                            }
                            let next_bytes = deferred_admission_bytes
                                .get(&peer)
                                .copied()
                                .unwrap_or(0)
                                .saturating_add(parcel.bytes.len());
                            let deferred = deferred_admission.entry(peer).or_default();
                            if deferred.len() >= 1_200 || next_bytes > 16 * 1024 * 1024 {
                                deferred_admission.remove(&peer);
                                deferred_admission_bytes.remove(&peer);
                                if let Some(connection) = connections.remove(&peer) {
                                    connection.close(
                                        quinn::VarInt::from_u32(3),
                                        b"cluster admission reliable capacity exhausted",
                                    );
                                }
                                streams.retain(|key, _| key.peer != peer);
                                drop_gate.note_lost(peer, Instant::now());
                            } else {
                                deferred.push_back(parcel);
                                deferred_admission_bytes.insert(peer, next_bytes);
                            }
                            continue;
                        }
                    }
                    let peer = parcel.peer;
                    if !route_parcel(
                        &connections,
                        &mut streams,
                        &mut peer_availability,
                        parcel,
                        &router_tx,
                    ) && peer_state.connected.contains(&peer)
                    {
                        if let Some(connection) = connections.remove(&peer) {
                            connection.close(
                                quinn::VarInt::from_u32(3),
                                b"cluster reliable stream failed",
                            );
                        }
                        streams.retain(|key, _| key.peer != peer);
                        drop_gate.note_lost(peer, Instant::now());
                    }
                }
                RouterMsg::OutboundClosed => outbound_open = false,
                RouterMsg::Datagram(peer, payload) => {
                    if admission_gates.contains_key(&peer) && !applied_admissions.contains(&peer) {
                        continue;
                    }
                    route_datagram(&mut connections, &mut peer_availability, peer, payload);
                }
                RouterMsg::DatagramsClosed => datagrams_open = false,
            }
        }
        endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
    }
}

#[derive(Debug)]
enum RouterMsg {
    Connection(quinn::Connection),
    Disconnected(ServerId, usize, String),
    ForceDrop(ServerId, Option<u64>),
    StreamFailed(ServerId, usize),
    Parcel(OutboundParcel),
    OutboundClosed,
    Datagram(ServerId, Vec<u8>),
    DatagramsClosed,
}

struct StreamWriter {
    stable_id: usize,
    sender: mpsc::Sender<OutboundParcel>,
}

fn defer_admission_advert(
    adverts: &mut HashMap<(u16, ChunkKey), OutboundParcel>,
    announce: ChunkAnnounce,
    parcel: OutboundParcel,
) -> bool {
    let key = match announce {
        ChunkAnnounce::Acquire(advert) => (advert.holder, advert.chunk),
        ChunkAnnounce::Release(drop) => (drop.holder, drop.chunk),
    };
    if adverts.len() >= ADMISSION_ADVERT_LIMIT && !adverts.contains_key(&key) {
        return false;
    }
    adverts.insert(key, parcel);
    true
}

#[derive(Default)]
struct PeerAvailability {
    connected_once: HashSet<ServerId>,
    loss_reported: HashSet<ServerId>,
}

fn publish_peer_state(sender: &watch::Sender<Arc<PeerStateSnapshot>>, state: &PeerStateSnapshot) {
    sender.send_replace(Arc::new(state.clone()));
}

fn drop_generation_matches(current: Option<u64>, requested: Option<u64>) -> bool {
    requested.is_none_or(|generation| current == Some(generation))
}

fn note_peer_connected(
    sender: &watch::Sender<Arc<PeerStateSnapshot>>,
    state: &mut PeerStateSnapshot,
    peer: ServerId,
    datagram_limit: Option<usize>,
) {
    state.connected.insert(peer);
    state.drop_epoch.entry(peer).or_default();
    if let Some(limit) = datagram_limit {
        state.datagram_limits.insert(peer, limit);
    } else {
        state.datagram_limits.remove(&peer);
    }
    publish_peer_state(sender, state);
}

fn note_peer_dropped(
    sender: &watch::Sender<Arc<PeerStateSnapshot>>,
    state: &mut PeerStateSnapshot,
    peer: ServerId,
) {
    state.connected.remove(&peer);
    state.datagram_limits.remove(&peer);
    let epoch = state.drop_epoch.entry(peer).or_default();
    *epoch = epoch.saturating_add(1);
    publish_peer_state(sender, state);
}

#[derive(Default)]
struct PeerDropGate {
    lost: HashMap<ServerId, Instant>,
}

impl PeerDropGate {
    fn contains(&self, peer: ServerId) -> bool {
        self.lost.contains_key(&peer)
    }

    fn note_lost(&mut self, peer: ServerId, now: Instant) {
        self.lost.entry(peer).or_insert(now);
    }

    fn allow_replacement(&mut self, peer: ServerId, now: Instant) -> bool {
        let allowed = self
            .lost
            .get(&peer)
            .is_some_and(|since| now.saturating_duration_since(*since) >= REPLACEMENT_QUIET_PERIOD);
        if allowed {
            self.lost.remove(&peer);
        }
        allowed
    }

    fn clear(&mut self, peer: ServerId) {
        self.lost.remove(&peer);
    }

    fn expire(&mut self, now: Instant) -> Vec<ServerId> {
        let mut dropped: Vec<_> = self
            .lost
            .iter()
            .filter_map(|(peer, since)| {
                (now.saturating_duration_since(*since) >= PEER_DROP_TIMEOUT).then_some(*peer)
            })
            .collect();
        dropped.sort_unstable();
        for peer in &dropped {
            self.lost.remove(peer);
        }
        dropped
    }
}

#[derive(Debug, PartialEq, Eq)]
enum UnavailablePeer {
    Connecting,
    Lost,
    AlreadyReported,
}

impl PeerAvailability {
    fn note_connected(&mut self, peer: ServerId) {
        self.connected_once.insert(peer);
        self.loss_reported.remove(&peer);
    }

    fn unavailable(&mut self, peer: ServerId, connection_was_present: bool) -> UnavailablePeer {
        if !connection_was_present && !self.connected_once.contains(&peer) {
            return UnavailablePeer::Connecting;
        }
        if self.loss_reported.insert(peer) {
            UnavailablePeer::Lost
        } else {
            UnavailablePeer::AlreadyReported
        }
    }
}

pub fn load_or_generate_keypair(config: &MeshConfig) -> Result<(Vec<u8>, Vec<u8>), TransportError> {
    let cert_path = Path::new(&config.cert_path);
    let key_path = Path::new(&config.key_path);
    let cert_exists = cert_path.exists();
    let key_exists = key_path.exists();
    if cert_exists && key_exists {
        let cert_der = std::fs::read(cert_path).map_err(|error| TransportError::KeyFile {
            path: config.cert_path.clone(),
            message: error.to_string(),
        })?;
        let key_der = std::fs::read(key_path).map_err(|error| TransportError::KeyFile {
            path: config.key_path.clone(),
            message: error.to_string(),
        })?;
        if cert_der.is_empty() || key_der.is_empty() {
            return Err(TransportError::KeyFile {
                path: config.cert_path.clone(),
                message: String::from("keypair file is empty"),
            });
        }
        return Ok((cert_der, key_der));
    }
    if cert_exists || key_exists {
        let missing = if cert_exists {
            config.key_path.clone()
        } else {
            config.cert_path.clone()
        };
        return Err(TransportError::IncompleteKeypair {
            missing_path: missing,
        });
    }
    let generated = rcgen::generate_simple_self_signed(vec![String::from(LOCAL_CERT_SAN)])
        .map_err(|error| TransportError::KeyGeneration {
            message: error.to_string(),
        })?;
    let cert_der = generated.cert.der().to_vec();
    let key_der = generated.signing_key.serialize_der();
    for path in [cert_path, key_path] {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|error| TransportError::KeyFile {
                    path: path.to_string_lossy().into_owned(),
                    message: error.to_string(),
                })?;
            }
        }
    }
    std::fs::write(cert_path, &cert_der).map_err(|error| TransportError::KeyFile {
        path: config.cert_path.clone(),
        message: error.to_string(),
    })?;
    std::fs::write(key_path, &key_der).map_err(|error| TransportError::KeyFile {
        path: config.key_path.clone(),
        message: error.to_string(),
    })?;
    Ok((cert_der, key_der))
}

fn build_endpoint(
    bind_addr: &SocketAddr,
    cert_der: &[u8],
    key_der: &[u8],
    pins: &[PinnedPeer],
) -> Result<quinn::Endpoint, TransportError> {
    let provider = Arc::new(quinn::rustls::crypto::ring::default_provider());
    let server_crypto = quinn::rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?
        .with_client_cert_verifier(Arc::new(PinVerifier {
            pins: pins.to_vec(),
        }))
        .with_single_cert(
            vec![CertificateDer::from(cert_der.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der.to_vec())),
        )
        .map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?;
    let client_crypto = quinn::rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinVerifier {
            pins: pins.to_vec(),
        }))
        .with_client_auth_cert(
            vec![CertificateDer::from(cert_der.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der.to_vec())),
        )
        .map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(quinn::VarInt::from_u32(mesh::MAX_UNI_STREAMS));
    transport.datagram_receive_buffer_size(Some(DATAGRAM_BUFFER_BYTES));
    transport.datagram_send_buffer_size(DATAGRAM_BUFFER_BYTES);
    transport.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
    let transport = Arc::new(transport);
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(server_crypto).map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?,
    ));
    server_config.transport_config(transport.clone());
    let mut client_config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client_crypto).map_err(|error| TransportError::Tls {
            message: error.to_string(),
        })?,
    ));
    client_config.transport_config(transport);
    let mut endpoint = quinn::Endpoint::server(server_config, *bind_addr).map_err(|error| {
        TransportError::Bind {
            message: error.to_string(),
        }
    })?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

#[derive(Debug, Clone)]
struct PinVerifier {
    pins: Vec<PinnedPeer>,
}

impl PinVerifier {
    fn peer_for(&self, cert_der: &[u8]) -> Option<&PinnedPeer> {
        self.pins
            .iter()
            .find(|peer| mesh::peer_authorized(cert_der, peer))
    }

    fn verification_algorithms() -> quinn::rustls::crypto::WebPkiSupportedAlgorithms {
        quinn::rustls::crypto::ring::default_provider().signature_verification_algorithms
    }

    fn offered_schemes() -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
        ]
    }
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let cert_bytes: &[u8] = end_entity.as_ref();
        match self.peer_for(cert_bytes) {
            Some(_) => Ok(ServerCertVerified::assertion()),
            None => Err(TlsError::InvalidCertificate(
                CertificateError::UnknownIssuer,
            )),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &Self::verification_algorithms(),
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &Self::verification_algorithms(),
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        Self::offered_schemes()
    }
}

impl ClientCertVerifier for PinVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        let cert_bytes: &[u8] = end_entity.as_ref();
        match self.peer_for(cert_bytes) {
            Some(_) => Ok(ClientCertVerified::assertion()),
            None => Err(TlsError::InvalidCertificate(
                CertificateError::UnknownIssuer,
            )),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        quinn::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &Self::verification_algorithms(),
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        quinn::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &Self::verification_algorithms(),
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        Self::offered_schemes()
    }
}

async fn dial_loop(
    endpoint: quinn::Endpoint,
    peer: PinnedPeer,
    router_tx: mpsc::Sender<RouterMsg>,
) {
    let addr: SocketAddr = match peer.addr.parse() {
        Ok(addr) => addr,
        Err(_) => {
            tracing::warn!(
                server = peer.server.0,
                addr = peer.addr.as_str(),
                "pinned peer address is invalid; skipping dials"
            );
            return;
        }
    };
    let server_name = addr.ip().to_string();
    let mut backoff = BASE_DIAL_BACKOFF;
    loop {
        if router_tx.is_closed() {
            return;
        }
        let established = match endpoint.connect(addr, server_name.as_str()) {
            Ok(connecting) => connecting.await.ok(),
            Err(_) => None,
        };
        match established {
            Some(conn) => {
                backoff = BASE_DIAL_BACKOFF;
                if router_tx
                    .send(RouterMsg::Connection(conn.clone()))
                    .await
                    .is_err()
                {
                    return;
                }
                conn.closed().await;
                tokio::time::sleep(BASE_DIAL_BACKOFF).await;
            }
            None => {
                tokio::time::sleep(backoff).await;
                backoff = core::cmp::min(backoff.saturating_add(backoff), MAX_DIAL_BACKOFF);
            }
        }
    }
}

async fn accept_loop(endpoint: quinn::Endpoint, router_tx: mpsc::Sender<RouterMsg>) {
    loop {
        let Some(incoming) = endpoint.accept().await else {
            return;
        };
        let sender = router_tx.clone();
        tokio::spawn(async move {
            if let Ok(conn) = incoming.await {
                let _ = sender.send(RouterMsg::Connection(conn)).await;
            }
        });
    }
}

fn connection_peer(conn: &quinn::Connection, pins: &[PinnedPeer]) -> Option<ServerId> {
    let identity = conn.peer_identity()?;
    let certs = identity.downcast_ref::<Vec<CertificateDer<'static>>>()?;
    let leaf = certs.first()?;
    let cert_bytes: &[u8] = leaf.as_ref();
    mesh::peer_by_cert(pins, cert_bytes).map(|peer| peer.server)
}

fn register_connection(
    connections: &mut HashMap<ServerId, quinn::Connection>,
    peer_availability: &mut PeerAvailability,
    pins: &[PinnedPeer],
    conn: quinn::Connection,
    generation: u64,
    inbound_tx: &mpsc::Sender<InboundParcel>,
    datagram_in_tx: &mpsc::Sender<(ServerId, u64, Vec<u8>)>,
) -> Option<(ServerId, watch::Sender<ConnectionPhase>)> {
    let Some(peer) = connection_peer(&conn, pins) else {
        tracing::warn!("closing connection from an unknown peer");
        conn.close(quinn::VarInt::from_u32(1), b"unknown peer");
        return None;
    };
    if let Some(existing) = connections.get(&peer) {
        if existing.stable_id() == conn.stable_id() {
            peer_availability.note_connected(peer);
            return None;
        }
        conn.close(quinn::VarInt::from_u32(2), b"peer still active");
        return None;
    }
    let (activation, active) = watch::channel(ConnectionPhase::Admission);
    start_connection_readers(
        conn.clone(),
        peer,
        generation,
        inbound_tx.clone(),
        datagram_in_tx.clone(),
        active,
    );
    connections.insert(peer, conn);
    peer_availability.note_connected(peer);
    Some((peer, activation))
}

fn start_connection_readers(
    conn: quinn::Connection,
    peer: ServerId,
    generation: u64,
    inbound_tx: mpsc::Sender<InboundParcel>,
    datagram_in_tx: mpsc::Sender<(ServerId, u64, Vec<u8>)>,
    active: watch::Receiver<ConnectionPhase>,
) {
    tokio::spawn(serve_uni_streams(
        conn.clone(),
        peer,
        generation,
        inbound_tx,
        active.clone(),
    ));
    tokio::spawn(serve_datagrams(
        conn,
        peer,
        generation,
        datagram_in_tx,
        active,
    ));
}

async fn serve_uni_streams(
    conn: quinn::Connection,
    peer: ServerId,
    generation: u64,
    inbound_tx: mpsc::Sender<InboundParcel>,
    active: watch::Receiver<ConnectionPhase>,
) {
    loop {
        match conn.accept_uni().await {
            Ok(mut recv) => {
                if *active.borrow() != ConnectionPhase::Quarantined {
                    tokio::spawn(serve_stream(
                        recv,
                        peer,
                        generation,
                        inbound_tx.clone(),
                        active.clone(),
                    ));
                } else {
                    let _ = recv.stop(quinn::VarInt::from_u32(3));
                }
            }
            Err(_) => return,
        }
    }
}

async fn serve_stream(
    mut recv: quinn::RecvStream,
    peer: ServerId,
    generation: u64,
    inbound_tx: mpsc::Sender<InboundParcel>,
    mut phase: watch::Receiver<ConnectionPhase>,
) {
    let header_bytes = match read_frame(&mut recv).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) | Err(_) => return,
    };
    let header: StreamHeader = match postcard::from_bytes(&header_bytes) {
        Ok(header) => header,
        Err(_) => return,
    };
    if header.kind != StreamKind::Control {
        loop {
            let current = *phase.borrow();
            if current == ConnectionPhase::Active {
                break;
            }
            if current == ConnectionPhase::Quarantined || phase.changed().await.is_err() {
                return;
            }
        }
    }
    loop {
        match read_frame(&mut recv).await {
            Ok(Some(bytes)) => {
                if *phase.borrow() == ConnectionPhase::Admission
                    && (header.kind != StreamKind::Control
                        || crate::membership::decode_handshake(&bytes).is_err())
                {
                    loop {
                        let current = *phase.borrow();
                        if current == ConnectionPhase::Active {
                            break;
                        }
                        if current == ConnectionPhase::Quarantined || phase.changed().await.is_err()
                        {
                            return;
                        }
                    }
                }
                let parcel = InboundParcel {
                    peer,
                    generation,
                    header,
                    bytes,
                };
                let kind = parcel.header.kind;
                let size = parcel.bytes.len();
                if inbound_tx.send(parcel).await.is_err() {
                    return;
                }
                tracing::trace!(server = peer.0, generation, kind = ?kind, bytes = size, "cluster reliable inbound routed");
            }
            Ok(None) | Err(_) => return,
        }
    }
}

async fn serve_datagrams(
    conn: quinn::Connection,
    peer: ServerId,
    generation: u64,
    datagram_in_tx: mpsc::Sender<(ServerId, u64, Vec<u8>)>,
    active: watch::Receiver<ConnectionPhase>,
) {
    loop {
        match conn.read_datagram().await {
            Ok(data) => {
                if *active.borrow() != ConnectionPhase::Active {
                    continue;
                }
                if datagram_in_tx
                    .send((peer, generation, data.to_vec()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn log_unavailable_parcel(
    peer_availability: &mut PeerAvailability,
    parcel: &OutboundParcel,
    connection_was_present: bool,
) {
    let peer = parcel.peer;
    let kind = parcel.header.kind;
    let bytes = parcel.bytes.len();
    match peer_availability.unavailable(peer, connection_was_present) {
        UnavailablePeer::Connecting => {
            tracing::debug!(server = peer.0, kind = ?kind, bytes, "dropping a parcel while the peer is connecting");
        }
        UnavailablePeer::Lost => {
            tracing::warn!(server = peer.0, kind = ?kind, bytes, "dropping a parcel after the peer connection was lost");
        }
        UnavailablePeer::AlreadyReported => {
            tracing::debug!(server = peer.0, kind = ?kind, bytes, "dropping a parcel while the peer remains unavailable");
        }
    }
}

fn log_unavailable_datagram(
    peer_availability: &mut PeerAvailability,
    peer: ServerId,
    bytes: usize,
    connection_was_present: bool,
) {
    match peer_availability.unavailable(peer, connection_was_present) {
        UnavailablePeer::Connecting => {
            tracing::debug!(
                server = peer.0,
                bytes,
                "dropping a datagram while the peer is connecting"
            );
        }
        UnavailablePeer::Lost => {
            tracing::warn!(
                server = peer.0,
                bytes,
                "dropping a datagram after the peer connection was lost"
            );
        }
        UnavailablePeer::AlreadyReported => {
            tracing::debug!(
                server = peer.0,
                bytes,
                "dropping a datagram while the peer remains unavailable"
            );
        }
    }
}

fn route_parcel(
    connections: &HashMap<ServerId, quinn::Connection>,
    streams: &mut HashMap<StreamKey, StreamWriter>,
    peer_availability: &mut PeerAvailability,
    parcel: OutboundParcel,
    router_tx: &mpsc::Sender<RouterMsg>,
) -> bool {
    let peer = parcel.peer;
    let Some(conn) = connections.get(&peer) else {
        log_unavailable_parcel(peer_availability, &parcel, false);
        return false;
    };
    if conn.close_reason().is_some() {
        streams.retain(|key, _| key.peer != peer);
        log_unavailable_parcel(peer_availability, &parcel, true);
        return false;
    }
    let key = StreamKey::new(peer, parcel.header.kind, parcel.header.player);
    if streams
        .get(&key)
        .is_some_and(|writer| writer.stable_id != conn.stable_id())
    {
        streams.remove(&key);
    }
    let writer = streams.entry(key).or_insert_with(|| {
        let capacity = if key.kind == StreamKind::ChunkAdvert {
            ADMISSION_ADVERT_LIMIT
        } else {
            2_048
        };
        let (sender, mut receiver) = mpsc::channel::<OutboundParcel>(capacity);
        let conn = conn.clone();
        let router_tx = router_tx.clone();
        let stable_id = conn.stable_id();
        tokio::spawn(async move {
            let mut stream = match conn.open_uni().await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::warn!(server = peer.0, kind = ?key.kind, error = %error, "cluster reliable stream open failed");
                    let _ = router_tx.send(RouterMsg::StreamFailed(peer, stable_id)).await;
                    return;
                }
            };
            if key.kind == StreamKind::ChunkData {
                let _ = stream.set_priority(CHUNK_STREAM_PRIORITY);
            }
            let header = match crate::streams::encode_header(StreamHeader::new(key.kind, key.player)) {
                Ok(header) => header,
                Err(error) => {
                    tracing::warn!(server = peer.0, kind = ?key.kind, error = %error, "cluster reliable stream header encode failed");
                    let _ = router_tx.send(RouterMsg::StreamFailed(peer, stable_id)).await;
                    return;
                }
            };
            if let Err(error) = write_raw(&mut stream, &header).await {
                tracing::warn!(server = peer.0, kind = ?key.kind, error = %error, "cluster reliable stream header write failed");
                let _ = router_tx.send(RouterMsg::StreamFailed(peer, stable_id)).await;
                return;
            }
            while let Some(parcel) = receiver.recv().await {
                if let Err(error) = write_frame(&mut stream, &parcel.bytes).await {
                    tracing::warn!(server = peer.0, kind = ?key.kind, bytes = parcel.bytes.len(), error = %error, "cluster reliable stream write failed");
                    let _ = router_tx.send(RouterMsg::StreamFailed(peer, stable_id)).await;
                    return;
                }
            }
        });
        StreamWriter { stable_id, sender }
    });
    if writer.sender.try_send(parcel).is_err() {
        tracing::error!(server = peer.0, kind = ?key.kind, "cluster reliable stream queue exhausted");
        return false;
    }
    true
}

fn route_datagram(
    connections: &mut HashMap<ServerId, quinn::Connection>,
    peer_availability: &mut PeerAvailability,
    peer: ServerId,
    payload: Vec<u8>,
) {
    let bytes = payload.len();
    let Some(conn) = connections.remove(&peer) else {
        log_unavailable_datagram(peer_availability, peer, bytes, false);
        return;
    };
    if conn.close_reason().is_some() {
        log_unavailable_datagram(peer_availability, peer, bytes, true);
        return;
    }
    let Some(limit) = conn.max_datagram_size() else {
        tracing::warn!(
            server = peer.0,
            bytes,
            "dropping a datagram because the peer did not negotiate QUIC datagrams"
        );
        connections.insert(peer, conn);
        return;
    };
    if bytes > limit {
        tracing::warn!(
            server = peer.0,
            bytes,
            limit,
            "dropping a datagram larger than the negotiated QUIC payload"
        );
        connections.insert(peer, conn);
        return;
    }
    if let Err(error) = conn.send_datagram(payload.into()) {
        tracing::warn!(server = peer.0, bytes, error = %error, "dropping a datagram after failing to send it");
        if conn.close_reason().is_some() {
            return;
        }
    }
    connections.insert(peer, conn);
}

async fn write_frame(send: &mut quinn::SendStream, payload: &[u8]) -> Result<(), TransportError> {
    let declared = u32::try_from(payload.len()).map_err(|_| TransportError::FrameTooLarge {
        bytes: payload.len(),
    })?;
    send.write_all(&declared.to_le_bytes())
        .await
        .map_err(write_error)?;
    send.write_all(payload).await.map_err(write_error)?;
    Ok(())
}

async fn write_raw(send: &mut quinn::SendStream, frame: &[u8]) -> Result<(), TransportError> {
    send.write_all(frame).await.map_err(write_error)?;
    Ok(())
}

async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Option<Vec<u8>>, TransportError> {
    let mut len_bytes = [0_u8; 4];
    match recv.read_exact(&mut len_bytes).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(quinn::ReadExactError::ReadError(error)) => return Err(read_error(error)),
    }
    let declared = u32::from_le_bytes(len_bytes);
    let cap = u32::try_from(MAX_FRAME_BYTES).unwrap_or(u32::MAX);
    if declared > cap {
        return Err(TransportError::FrameTooLarge {
            bytes: MAX_FRAME_BYTES,
        });
    }
    let len = usize::try_from(declared).map_err(|_| TransportError::FrameTooLarge {
        bytes: MAX_FRAME_BYTES,
    })?;
    let mut payload = vec![0_u8; len];
    match recv.read_exact(&mut payload).await {
        Ok(()) => Ok(Some(payload)),
        Err(quinn::ReadExactError::FinishedEarly(_)) => Err(TransportError::StreamClosed),
        Err(quinn::ReadExactError::ReadError(error)) => Err(read_error(error)),
    }
}

fn write_error(error: quinn::WriteError) -> TransportError {
    match error {
        quinn::WriteError::ConnectionLost(_) => TransportError::ConnectionLost,
        quinn::WriteError::Stopped(_)
        | quinn::WriteError::ClosedStream
        | quinn::WriteError::ZeroRttRejected => TransportError::StreamClosed,
    }
}

fn read_error(error: quinn::ReadError) -> TransportError {
    match error {
        quinn::ReadError::ConnectionLost(_) => TransportError::ConnectionLost,
        quinn::ReadError::Reset(_)
        | quinn::ReadError::ClosedStream
        | quinn::ReadError::IllegalOrderedRead
        | quinn::ReadError::ZeroRttRejected => TransportError::StreamClosed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::identity::{GlobalPlayerId, PlayerSlot};
    use crate::protocol::{StreamKind, WorldId};

    static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn delayed_overflow_drop_cannot_close_replacement_generation() {
        assert!(drop_generation_matches(Some(4), Some(4)));
        assert!(!drop_generation_matches(Some(5), Some(4)));
        assert!(!drop_generation_matches(None, Some(4)));
        assert!(drop_generation_matches(Some(5), None));
    }

    #[test]
    fn admission_buffers_large_holder_view_without_losing_latest_drop() {
        let mut adverts = HashMap::new();
        let world = WorldId::new(0, 7);
        for x in 0..1_300 {
            let announce = ChunkAnnounce::Acquire(crate::chunks::ChunkAdvert {
                holder: 1,
                chunk: ChunkKey::new(world, x, 0),
            });
            let parcel = OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader::new(StreamKind::ChunkAdvert, None),
                bytes: crate::xfer::encode_announce(&announce).unwrap(),
            };
            assert!(defer_admission_advert(&mut adverts, announce, parcel));
        }
        let changed = ChunkAnnounce::Release(crate::chunks::ChunkDrop {
            holder: 1,
            chunk: ChunkKey::new(world, 5, 0),
        });
        let parcel = OutboundParcel {
            peer: ServerId(2),
            header: StreamHeader::new(StreamKind::ChunkAdvert, None),
            bytes: crate::xfer::encode_announce(&changed).unwrap(),
        };
        assert!(defer_admission_advert(&mut adverts, changed, parcel));
        assert_eq!(adverts.len(), 1_300);
        let final_five = adverts.get(&(1, ChunkKey::new(world, 5, 0))).unwrap();
        assert_eq!(
            crate::xfer::decode_announce(&final_five.bytes).unwrap(),
            changed
        );
    }

    async fn wait_for_peer_state(
        receiver: &mut watch::Receiver<Arc<PeerStateSnapshot>>,
        predicate: impl Fn(&PeerStateSnapshot) -> bool,
    ) -> Arc<PeerStateSnapshot> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = Arc::clone(&receiver.borrow_and_update());
                if predicate(&state) {
                    return state;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .unwrap()
    }

    fn scratch_paths(tag: &str) -> (String, String) {
        let id = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pumpkin-cluster-{tag}-{pid}-{id}",
            pid = std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (
            dir.join("cert.der").to_string_lossy().into_owned(),
            dir.join("key.der").to_string_lossy().into_owned(),
        )
    }

    fn mesh_config(
        server: u16,
        bind_addr: &str,
        cert_path: String,
        key_path: String,
        peers: Vec<PinnedPeer>,
    ) -> MeshConfig {
        MeshConfig {
            server: ServerId(server),
            bind_addr: String::from(bind_addr),
            cert_path,
            key_path,
            peers,
        }
    }

    fn pinned(server: u16, addr: &str, cert_der: &[u8]) -> PinnedPeer {
        PinnedPeer {
            server: ServerId(server),
            addr: String::from(addr),
            pubkey_sha256: mesh::cert_fingerprint_sha256(cert_der),
        }
    }

    async fn wait_for_handshake() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    async fn admit_peers(mesh: &mut MeshChannels, peers: &[ServerId]) {
        wait_for_peer_state(&mut mesh.peer_state_rx, |state| {
            peers.iter().all(|peer| state.connected.contains(peer))
        })
        .await;
        mesh.peer_admitted_tx
            .send_replace(Arc::new(peers.iter().copied().collect()));
        wait_for_peer_state(&mut mesh.peer_state_rx, |state| {
            peers
                .iter()
                .all(|peer| state.admission_epoch.contains_key(peer))
        })
        .await;
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn stream_header_roundtrips() {
        let header = StreamHeader {
            kind: StreamKind::PlayerWorld,
            player: Some(GlobalPlayerId::new(ServerId(3), PlayerSlot(7))),
        };
        let bytes = postcard::to_allocvec(&header).unwrap();
        assert_eq!(
            postcard::from_bytes::<StreamHeader>(&bytes).unwrap(),
            header
        );
    }

    #[test]
    fn shared_header_has_no_player() {
        let header = StreamHeader {
            kind: StreamKind::Control,
            player: None,
        };
        let bytes = postcard::to_allocvec(&header).unwrap();
        let back: StreamHeader = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back.player, None);
    }

    #[test]
    fn peer_availability_only_reports_one_loss_per_connection() {
        let peer = ServerId(2);
        let mut availability = PeerAvailability::default();

        assert_eq!(
            availability.unavailable(peer, false),
            UnavailablePeer::Connecting
        );

        availability.note_connected(peer);
        assert_eq!(availability.unavailable(peer, true), UnavailablePeer::Lost);
        assert_eq!(
            availability.unavailable(peer, false),
            UnavailablePeer::AlreadyReported
        );

        availability.note_connected(peer);
        assert_eq!(availability.unavailable(peer, false), UnavailablePeer::Lost);
    }

    #[test]
    fn lost_peer_replacement_and_drop_use_distinct_deadlines() {
        let peer = ServerId(2);
        let began = Instant::now();
        let mut gate = PeerDropGate::default();
        gate.note_lost(peer, began);

        assert!(!gate.allow_replacement(
            peer,
            began + REPLACEMENT_QUIET_PERIOD - Duration::from_millis(1)
        ));
        assert!(
            gate.expire(began + PEER_DROP_TIMEOUT - Duration::from_millis(1))
                .is_empty()
        );
        assert!(gate.allow_replacement(peer, began + REPLACEMENT_QUIET_PERIOD));

        gate.note_lost(peer, began);
        assert_eq!(gate.expire(began + PEER_DROP_TIMEOUT), vec![peer]);
    }

    #[test]
    fn saturated_replacement_activation_does_not_freeze_admission_state() {
        let (admitted_tx, admitted_rx) = watch::channel(Arc::new(HashSet::new()));
        let (replacement_tx, mut replacement_rx) = mpsc::channel(1);
        let mut state = TransportActivationState::default();

        state.joined(ServerId(7));
        state.joined(ServerId(8));
        state.publish_admissions(&admitted_tx);
        replacement_tx.try_send(ServerId(42)).unwrap();
        state.replacement_completed(ServerId(9));
        state.flush_replacements(&replacement_tx);
        state.left(ServerId(7));
        state.publish_admissions(&admitted_tx);

        assert_eq!(replacement_rx.try_recv().unwrap(), ServerId(42));
        state.flush_replacements(&replacement_tx);
        assert_eq!(replacement_rx.try_recv().unwrap(), ServerId(9));
        assert_eq!(admitted_rx.borrow().as_ref(), &HashSet::from([ServerId(8)]));
        assert!(!state.admission_applied(ServerId(7)));
        assert!(state.admission_applied(ServerId(8)));
        state.publish_admissions(&admitted_tx);
        assert_eq!(admitted_rx.borrow().as_ref(), &HashSet::from([ServerId(8)]));
    }

    #[test]
    fn keypair_generates_then_loads() {
        let (cert_path, key_path) = scratch_paths("keypair");
        let config = mesh_config(1, "127.0.0.1:0", cert_path, key_path, Vec::new());
        let first = load_or_generate_keypair(&config).unwrap();
        let second = load_or_generate_keypair(&config).unwrap();
        assert_eq!(first, second);
        assert!(!first.0.is_empty());
        assert!(!first.1.is_empty());
    }

    #[test]
    fn keypair_rejects_a_lone_half() {
        let (cert_path, key_path) = scratch_paths("half");
        std::fs::write(&cert_path, b"der").unwrap();
        let config = mesh_config(1, "127.0.0.1:0", cert_path, key_path, Vec::new());
        assert!(matches!(
            load_or_generate_keypair(&config),
            Err(TransportError::IncompleteKeypair { .. })
        ));
    }

    #[tokio::test]
    async fn unread_peer_state_does_not_block_parcels_and_datagrams() {
        let port_a = free_port();
        let port_b = free_port();
        let (cert_path_a, key_path_a) = scratch_paths("a-keys");
        let (cert_path_b, key_path_b) = scratch_paths("b-keys");
        let keys_a = load_or_generate_keypair(&mesh_config(
            1,
            "127.0.0.1:0",
            cert_path_a.clone(),
            key_path_a.clone(),
            Vec::new(),
        ))
        .unwrap();
        let keys_b = load_or_generate_keypair(&mesh_config(
            2,
            "127.0.0.1:0",
            cert_path_b.clone(),
            key_path_b.clone(),
            Vec::new(),
        ))
        .unwrap();
        let addr_a = format!("127.0.0.1:{port_a}");
        let addr_b = format!("127.0.0.1:{port_b}");
        let config_a = mesh_config(
            1,
            addr_a.as_str(),
            cert_path_a,
            key_path_a,
            vec![pinned(2, addr_b.as_str(), &keys_b.0)],
        );
        let config_b = mesh_config(
            2,
            addr_b.as_str(),
            cert_path_b,
            key_path_b,
            vec![pinned(1, addr_a.as_str(), &keys_a.0)],
        );
        let (transport_a, mut mesh_a) = channel_pair(1);
        let (transport_b, mut mesh_b) = channel_pair(1);
        let endpoint_a = Transport::bind(config_a, transport_a).unwrap();
        let endpoint_b = Transport::bind(config_b, transport_b).unwrap();
        let run_a = tokio::spawn(endpoint_a.run());
        let run_b = tokio::spawn(endpoint_b.run());
        wait_for_handshake().await;

        let header = StreamHeader {
            kind: StreamKind::PlayerVisual,
            player: Some(GlobalPlayerId::new(ServerId(1), PlayerSlot(4))),
        };
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![7],
            })
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), mesh_b.inbound_rx.recv())
                .await
                .is_err()
        );
        admit_peers(&mut mesh_a, &[ServerId(2)]).await;
        admit_peers(&mut mesh_b, &[ServerId(1)]).await;
        let admitted = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admitted.bytes, vec![7]);
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![1, 2, 3],
            })
            .await
            .unwrap();
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![4, 5],
            })
            .await
            .unwrap();
        mesh_a
            .datagram_out_tx
            .send((ServerId(2), vec![9, 9, 9]))
            .await
            .unwrap();

        let first = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.peer, ServerId(1));
        assert_eq!(first.header, header);
        assert_eq!(first.bytes, vec![1, 2, 3]);
        let second = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.bytes, vec![4, 5]);
        let (peer, _, datagram) =
            tokio::time::timeout(Duration::from_secs(10), mesh_b.datagram_in_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer, ServerId(1));
        assert_eq!(datagram, vec![9, 9, 9]);
        wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state.connected.contains(&ServerId(1))
        })
        .await;
        assert_eq!(
            mesh_b.peer_state_rx.borrow().drop_epoch.get(&ServerId(1)),
            Some(&0)
        );

        drop(mesh_a);
        drop(mesh_b);
        tokio::time::timeout(Duration::from_secs(10), run_a)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), run_b)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn chunk_flow_control_does_not_hold_control_stream() {
        let port_a = free_port();
        let port_b = free_port();
        let (cert_a, key_a) = scratch_paths("chunk-control-a");
        let (cert_b, key_b) = scratch_paths("chunk-control-b");
        let keys_a = load_or_generate_keypair(&mesh_config(
            1,
            "127.0.0.1:0",
            cert_a.clone(),
            key_a.clone(),
            Vec::new(),
        ))
        .unwrap();
        let keys_b = load_or_generate_keypair(&mesh_config(
            2,
            "127.0.0.1:0",
            cert_b.clone(),
            key_b.clone(),
            Vec::new(),
        ))
        .unwrap();
        let addr_a = format!("127.0.0.1:{port_a}");
        let addr_b = format!("127.0.0.1:{port_b}");
        let (transport_a, mut mesh_a) = channel_pair(256);
        let (transport_b, mut mesh_b) = channel_pair(256);
        let endpoint_a = Transport::bind(
            mesh_config(
                1,
                &addr_a,
                cert_a,
                key_a,
                vec![pinned(2, &addr_b, &keys_b.0)],
            ),
            transport_a,
        )
        .unwrap();
        let endpoint_b = Transport::bind(
            mesh_config(
                2,
                &addr_b,
                cert_b,
                key_b,
                vec![pinned(1, &addr_a, &keys_a.0)],
            ),
            transport_b,
        )
        .unwrap();
        let run_a = tokio::spawn(endpoint_a.run());
        let run_b = tokio::spawn(endpoint_b.run());
        wait_for_handshake().await;
        admit_peers(&mut mesh_a, &[ServerId(2)]).await;
        admit_peers(&mut mesh_b, &[ServerId(1)]).await;

        for sequence in 0..32_u8 {
            let mut bytes = vec![sequence; 256 * 1024];
            bytes[0] = sequence;
            mesh_a
                .outbound_tx
                .send(OutboundParcel {
                    peer: ServerId(2),
                    header: StreamHeader::new(StreamKind::ChunkData, None),
                    bytes,
                })
                .await
                .unwrap();
        }
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![0x42],
            })
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let parcel = mesh_b.inbound_rx.recv().await.unwrap();
                if parcel.header.kind == StreamKind::Control {
                    return parcel;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(received.bytes, vec![0x42]);

        drop(mesh_a);
        drop(mesh_b);
        run_a.abort();
        run_b.abort();
    }

    #[tokio::test]
    async fn unread_control_state_does_not_block_unrelated_peers() {
        let port_a = free_port();
        let port_b = free_port();
        let port_c = free_port();
        let (cert_a, key_a) = scratch_paths("saturation-a");
        let (cert_b, key_b) = scratch_paths("saturation-b");
        let (cert_c, key_c) = scratch_paths("saturation-c");
        let keys_a = load_or_generate_keypair(&mesh_config(
            1,
            "127.0.0.1:0",
            cert_a.clone(),
            key_a.clone(),
            Vec::new(),
        ))
        .unwrap();
        let keys_b = load_or_generate_keypair(&mesh_config(
            2,
            "127.0.0.1:0",
            cert_b.clone(),
            key_b.clone(),
            Vec::new(),
        ))
        .unwrap();
        let keys_c = load_or_generate_keypair(&mesh_config(
            3,
            "127.0.0.1:0",
            cert_c.clone(),
            key_c.clone(),
            Vec::new(),
        ))
        .unwrap();
        let addr_a = format!("127.0.0.1:{port_a}");
        let addr_b = format!("127.0.0.1:{port_b}");
        let addr_c = format!("127.0.0.1:{port_c}");
        let (transport_a, mut mesh_a) = channel_pair(1);
        let (transport_b, mut mesh_b) = channel_pair(1);
        let (transport_c, mut mesh_c) = channel_pair(1);
        let endpoint_a = Transport::bind(
            mesh_config(
                1,
                &addr_a,
                cert_a,
                key_a,
                vec![pinned(2, &addr_b, &keys_b.0)],
            ),
            transport_a,
        )
        .unwrap();
        let endpoint_b = Transport::bind(
            mesh_config(
                2,
                &addr_b,
                cert_b,
                key_b,
                vec![pinned(1, &addr_a, &keys_a.0), pinned(3, &addr_c, &keys_c.0)],
            ),
            transport_b,
        )
        .unwrap();
        let endpoint_c = Transport::bind(
            mesh_config(
                3,
                &addr_c,
                cert_c,
                key_c,
                vec![pinned(2, &addr_b, &keys_b.0)],
            ),
            transport_c,
        )
        .unwrap();
        let run_a = tokio::spawn(endpoint_a.run());
        let run_b = tokio::spawn(endpoint_b.run());
        let run_c = tokio::spawn(endpoint_c.run());
        wait_for_handshake().await;
        admit_peers(&mut mesh_a, &[ServerId(2)]).await;
        admit_peers(&mut mesh_b, &[ServerId(1), ServerId(3)]).await;
        admit_peers(&mut mesh_c, &[ServerId(2)]).await;
        let header = StreamHeader::new(StreamKind::PlayerVisual, None);
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![1],
            })
            .await
            .unwrap();
        mesh_c
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![3],
            })
            .await
            .unwrap();
        let mut bytes = Vec::new();
        for _ in 0..2 {
            bytes.push(
                tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes[0],
            );
        }
        bytes.sort_unstable();
        assert_eq!(bytes, vec![1, 3]);
        let state = wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state.connected.contains(&ServerId(1)) && state.connected.contains(&ServerId(3))
        })
        .await;
        assert_eq!(state.drop_epoch.get(&ServerId(1)), Some(&0));
        assert_eq!(state.drop_epoch.get(&ServerId(3)), Some(&0));
        drop(mesh_a);
        drop(mesh_b);
        drop(mesh_c);
        run_a.abort();
        run_b.abort();
        run_c.abort();
    }

    #[tokio::test]
    async fn replacement_after_quiet_period_drops_old_peer_before_connecting_new_one() {
        let port_a = free_port();
        let port_b = free_port();
        let port_a2 = free_port();
        let (cert_path_a, key_path_a) = scratch_paths("restart-a-keys");
        let (cert_path_b, key_path_b) = scratch_paths("restart-b-keys");
        let keys_a = load_or_generate_keypair(&mesh_config(
            1,
            "127.0.0.1:0",
            cert_path_a.clone(),
            key_path_a.clone(),
            Vec::new(),
        ))
        .unwrap();
        let keys_b = load_or_generate_keypair(&mesh_config(
            2,
            "127.0.0.1:0",
            cert_path_b.clone(),
            key_path_b.clone(),
            Vec::new(),
        ))
        .unwrap();
        let addr_a = format!("127.0.0.1:{port_a}");
        let addr_b = format!("127.0.0.1:{port_b}");
        let addr_a2 = format!("127.0.0.1:{port_a2}");
        let config_a = mesh_config(
            1,
            addr_a.as_str(),
            cert_path_a.clone(),
            key_path_a.clone(),
            vec![pinned(2, addr_b.as_str(), &keys_b.0)],
        );
        let config_b = mesh_config(
            2,
            addr_b.as_str(),
            cert_path_b.clone(),
            key_path_b.clone(),
            vec![pinned(1, addr_a.as_str(), &keys_a.0)],
        );
        let (transport_a, mut mesh_a) = channel_pair(32);
        let (transport_b, mut mesh_b) = channel_pair(32);
        let endpoint_a = Transport::bind(config_a, transport_a).unwrap();
        let endpoint_b = Transport::bind(config_b, transport_b).unwrap();
        let run_a = tokio::spawn(endpoint_a.run());
        let run_b = tokio::spawn(endpoint_b.run());
        wait_for_handshake().await;
        admit_peers(&mut mesh_a, &[ServerId(2)]).await;
        admit_peers(&mut mesh_b, &[ServerId(1)]).await;
        let header = StreamHeader {
            kind: StreamKind::Control,
            player: None,
        };
        mesh_a
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![1],
            })
            .await
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.bytes, vec![1]);
        wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state.connected.contains(&ServerId(1))
        })
        .await;
        run_a.abort();
        let _ = run_a.await;
        assert!(
            mesh_b
                .peer_state_rx
                .borrow()
                .drop_epoch
                .get(&ServerId(1))
                .copied()
                .unwrap_or(0)
                == 0
        );
        tokio::time::sleep(REPLACEMENT_QUIET_PERIOD).await;
        let config_a2 = mesh_config(
            1,
            addr_a2.as_str(),
            cert_path_a,
            key_path_a,
            vec![pinned(2, addr_b.as_str(), &keys_b.0)],
        );
        let (transport_a2, mut mesh_a2) = channel_pair(32);
        let endpoint_a2 = Transport::bind(config_a2, transport_a2).unwrap();
        let run_a2 = tokio::spawn(endpoint_a2.run());
        wait_for_peer_state(&mut mesh_a2.peer_state_rx, |state| {
            state.connected.contains(&ServerId(2))
        })
        .await;
        admit_peers(&mut mesh_a2, &[ServerId(2)]).await;
        wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state.drop_epoch.get(&ServerId(1)).copied().unwrap_or(0) == 1
        })
        .await;
        mesh_a2
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![0],
            })
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), mesh_b.inbound_rx.recv())
                .await
                .is_err()
        );
        mesh_b.replacement_ready_tx.send(ServerId(1)).await.unwrap();
        wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state.replacement_capacity_epoch == 1 && state.connected.contains(&ServerId(1))
        })
        .await;
        mesh_a2
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader {
                    kind: StreamKind::PlayerVisual,
                    player: None,
                },
                bytes: vec![2],
            })
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), mesh_b.inbound_rx.recv())
                .await
                .is_err()
        );
        mesh_b
            .peer_admitted_tx
            .send_replace(Arc::new(HashSet::from([ServerId(1)])));
        wait_for_peer_state(&mut mesh_b.peer_state_rx, |state| {
            state
                .admission_epoch
                .get(&ServerId(1))
                .copied()
                .unwrap_or(0)
                == 2
        })
        .await;
        mesh_a2
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header: StreamHeader {
                    kind: StreamKind::PlayerCombat,
                    player: None,
                },
                bytes: vec![3],
            })
            .await
            .unwrap();
        let mut resumed = Vec::new();
        for _ in 0..3 {
            let parcel = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(parcel.peer, ServerId(1));
            resumed.push(parcel.bytes[0]);
        }
        resumed.sort_unstable();
        assert_eq!(resumed, vec![0, 2, 3]);
        drop(mesh_a);
        drop(mesh_a2);
        drop(mesh_b);
        tokio::time::timeout(Duration::from_secs(10), run_a2)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), run_b)
            .await
            .unwrap()
            .unwrap();
    }
}
