
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use quinn::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};
use quinn::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use quinn::rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as TlsError,
    SignatureScheme,
};
use tokio::sync::mpsc;

use crate::identity::ServerId;
use crate::mesh::{self, MeshConfig, PinnedPeer};
use crate::protocol::StreamKind;
use crate::streams::{InboundParcel, OutboundParcel, StreamHeader, StreamKey};

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

const DATAGRAM_BUFFER_BYTES: usize = 4 * 1024 * 1024;

const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

const BASE_DIAL_BACKOFF: Duration = Duration::from_secs(1);

const MAX_DIAL_BACKOFF: Duration = Duration::from_secs(30);

const LOCAL_CERT_SAN: &str = "pumpkin-mesh";

const CHUNK_STREAM_PRIORITY: i32 = -16;

const ROUTER_BUFFER: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    InvalidAddress {
        addr: String,
    },
    KeyFile {
        path: String,
        message: String,
    },
    IncompleteKeypair {
        missing_path: String,
    },
    KeyGeneration {
        message: String,
    },
    Tls {
        message: String,
    },
    Bind {
        message: String,
    },
    HeaderCodec {
        message: String,
    },
    FrameTooLarge {
        bytes: usize,
    },
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
    pub datagram_in_tx: mpsc::Sender<(ServerId, Vec<u8>)>,
    pub datagram_out_rx: mpsc::Receiver<(ServerId, Vec<u8>)>,
    pub restart_tx: mpsc::Sender<ServerId>,
}

#[derive(Debug)]
pub struct MeshChannels {
    pub inbound_rx: mpsc::Receiver<InboundParcel>,
    pub outbound_tx: mpsc::Sender<OutboundParcel>,
    pub datagram_in_rx: mpsc::Receiver<(ServerId, Vec<u8>)>,
    pub datagram_out_tx: mpsc::Sender<(ServerId, Vec<u8>)>,
    pub restart_rx: mpsc::Receiver<ServerId>,
}

#[must_use]
pub fn channel_pair(buffer: usize) -> (TransportChannels, MeshChannels) {
    let (inbound_tx, inbound_rx) = mpsc::channel(buffer);
    let (outbound_tx, outbound_rx) = mpsc::channel(buffer);
    let (datagram_in_tx, datagram_in_rx) = mpsc::channel(buffer);
    let (datagram_out_tx, datagram_out_rx) = mpsc::channel(buffer);
    let (restart_tx, restart_rx) = mpsc::channel(buffer);
    (
        TransportChannels {
            inbound_tx,
            outbound_rx,
            datagram_in_tx,
            datagram_out_rx,
            restart_tx,
        },
        MeshChannels {
            inbound_rx,
            outbound_tx,
            datagram_in_rx,
            datagram_out_tx,
            restart_rx,
        },
    )
}

pub struct Transport {
    endpoint: quinn::Endpoint,
    mesh: MeshConfig,
    channels: TransportChannels,
    local_cert_der: Vec<u8>,
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
        let (router_tx, mut router_rx) = mpsc::channel::<RouterMsg>(ROUTER_BUFFER);
        for peer in &mesh.peers {
            if mesh.server < peer.server {
                tokio::spawn(dial_loop(
                    endpoint.clone(),
                    peer.clone(),
                    router_tx.clone(),
                ));
            }
        }
        tokio::spawn(accept_loop(endpoint.clone(), router_tx.clone()));
        let TransportChannels {
            inbound_tx,
            outbound_rx,
            datagram_in_tx,
            mut datagram_out_rx,
            restart_tx,
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
        let mut connections: HashMap<ServerId, quinn::Connection> = HashMap::new();
        let mut streams: HashMap<StreamKey, quinn::SendStream> = HashMap::new();
        let mut peer_availability = PeerAvailability::default();
        let mut outbound_open = true;
        let mut datagrams_open = true;
        while outbound_open || datagrams_open {
            let Some(msg) = router_rx.recv().await else {
                break;
            };
            match msg {
                RouterMsg::Connection(conn) => {
                    if let Some(peer) = register_connection(
                        &mut connections,
                        &mut streams,
                        &mut peer_availability,
                        &mesh.peers,
                        conn,
                        &inbound_tx,
                        &datagram_in_tx,
                    ) {
                        if restart_tx.try_send(peer).is_err() {
                            tracing::warn!(server = peer.0, "dropping a peer restart signal");
                            let _ = restart_tx.send(peer).await;
                        }
                    }
                }
                RouterMsg::Parcel(parcel) => {
                    route_parcel(
                        &mut connections,
                        &mut streams,
                        &mut peer_availability,
                        parcel,
                    )
                    .await;
                }
                RouterMsg::OutboundClosed => outbound_open = false,
                RouterMsg::Datagram(peer, payload) => {
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
    Parcel(OutboundParcel),
    OutboundClosed,
    Datagram(ServerId, Vec<u8>),
    DatagramsClosed,
}

#[derive(Default)]
struct PeerAvailability {
    connected_once: HashSet<ServerId>,
    loss_reported: HashSet<ServerId>,
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

pub fn load_or_generate_keypair(
    config: &MeshConfig,
) -> Result<(Vec<u8>, Vec<u8>), TransportError> {
    let cert_path = Path::new(&config.cert_path);
    let key_path = Path::new(&config.key_path);
    let cert_exists = cert_path.exists();
    let key_exists = key_path.exists();
    if cert_exists && key_exists {
        let cert_der =
            std::fs::read(cert_path).map_err(|error| TransportError::KeyFile {
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
        return Err(TransportError::IncompleteKeypair { missing_path: missing });
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
    let mut endpoint =
        quinn::Endpoint::server(server_config, *bind_addr).map_err(|error| {
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

    fn verification_algorithms(
    ) -> quinn::rustls::crypto::WebPkiSupportedAlgorithms {
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
                if router_tx.send(RouterMsg::Connection(conn.clone())).await.is_err() {
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
    streams: &mut HashMap<StreamKey, quinn::SendStream>,
    peer_availability: &mut PeerAvailability,
    pins: &[PinnedPeer],
    conn: quinn::Connection,
    inbound_tx: &mpsc::Sender<InboundParcel>,
    datagram_in_tx: &mpsc::Sender<(ServerId, Vec<u8>)>,
) -> Option<ServerId> {
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
        streams.retain(|key, _| key.peer != peer);
        existing.close(quinn::VarInt::from_u32(0), b"replaced");
        tokio::spawn(serve_uni_streams(conn.clone(), peer, inbound_tx.clone()));
        tokio::spawn(serve_datagrams(conn.clone(), peer, datagram_in_tx.clone()));
        connections.insert(peer, conn);
        peer_availability.note_connected(peer);
        return Some(peer);
    }
    tokio::spawn(serve_uni_streams(conn.clone(), peer, inbound_tx.clone()));
    tokio::spawn(serve_datagrams(conn.clone(), peer, datagram_in_tx.clone()));
    connections.insert(peer, conn);
    peer_availability.note_connected(peer);
    Some(peer)
}

async fn serve_uni_streams(
    conn: quinn::Connection,
    peer: ServerId,
    inbound_tx: mpsc::Sender<InboundParcel>,
) {
    loop {
        match conn.accept_uni().await {
            Ok(recv) => {
                tokio::spawn(serve_stream(recv, peer, inbound_tx.clone()));
            }
            Err(_) => return,
        }
    }
}

async fn serve_stream(
    mut recv: quinn::RecvStream,
    peer: ServerId,
    inbound_tx: mpsc::Sender<InboundParcel>,
) {
    let header_bytes = match read_frame(&mut recv).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) | Err(_) => return,
    };
    let header: StreamHeader = match postcard::from_bytes(&header_bytes) {
        Ok(header) => header,
        Err(_) => return,
    };
    loop {
        match read_frame(&mut recv).await {
            Ok(Some(bytes)) => {
                let parcel = InboundParcel { peer, header, bytes };
                if inbound_tx.send(parcel).await.is_err() {
                    return;
                }
            }
            Ok(None) | Err(_) => return,
        }
    }
}

async fn serve_datagrams(
    conn: quinn::Connection,
    peer: ServerId,
    datagram_in_tx: mpsc::Sender<(ServerId, Vec<u8>)>,
) {
    loop {
        match conn.read_datagram().await {
            Ok(data) => {
                if datagram_in_tx.send((peer, data.to_vec())).await.is_err() {
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
            tracing::debug!(server = peer.0, bytes, "dropping a datagram while the peer is connecting");
        }
        UnavailablePeer::Lost => {
            tracing::warn!(server = peer.0, bytes, "dropping a datagram after the peer connection was lost");
        }
        UnavailablePeer::AlreadyReported => {
            tracing::debug!(server = peer.0, bytes, "dropping a datagram while the peer remains unavailable");
        }
    }
}

async fn route_parcel(
    connections: &mut HashMap<ServerId, quinn::Connection>,
    streams: &mut HashMap<StreamKey, quinn::SendStream>,
    peer_availability: &mut PeerAvailability,
    parcel: OutboundParcel,
) {
    let peer = parcel.peer;
    let Some(conn) = connections.remove(&peer) else {
        log_unavailable_parcel(peer_availability, &parcel, false);
        return;
    };
    if conn.close_reason().is_some() {
        streams.retain(|key, _| key.peer != peer);
        log_unavailable_parcel(peer_availability, &parcel, true);
        return;
    }
    let key = StreamKey::new(peer, parcel.header.kind, parcel.header.player);
    if !streams.contains_key(&key) {
        let mut fresh = match conn.open_uni().await {
            Ok(fresh) => fresh,
            Err(error) => {
                tracing::warn!(server = peer.0, kind = ?parcel.header.kind, error = %error, "dropping a parcel after failing to open a uni stream");
                streams.retain(|existing, _| existing.peer != peer);
                if conn.close_reason().is_none() {
                    connections.insert(peer, conn);
                }
                return;
            }
        };
        if parcel.header.kind == StreamKind::ChunkData {
            let _ = fresh.set_priority(CHUNK_STREAM_PRIORITY);
        }
        let header_frame = match crate::streams::encode_header(parcel.header) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(server = peer.0, kind = ?parcel.header.kind, error = %error, "dropping a parcel after failing to encode a stream header");
                connections.insert(peer, conn);
                return;
            }
        };
        if let Err(error) = write_raw(&mut fresh, &header_frame).await {
            tracing::warn!(server = peer.0, kind = ?parcel.header.kind, error = %error, "dropping a parcel after failing to write a stream header");
            connections.insert(peer, conn);
            return;
        }
        streams.insert(key, fresh);
    }
    let mut broken = false;
    if let Some(send) = streams.get_mut(&key) {
        if let Err(error) = write_frame(send, &parcel.bytes).await {
            tracing::warn!(server = peer.0, kind = ?parcel.header.kind, bytes = parcel.bytes.len(), error = %error, "dropping a parcel after failing to write it");
            broken = true;
        }
    }
    if broken {
        streams.remove(&key);
        if conn.close_reason().is_some() {
            streams.retain(|existing, _| existing.peer != peer);
            return;
        }
    }
    connections.insert(peer, conn);
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
    if let Err(error) = conn.send_datagram(payload.into()) {
        tracing::warn!(server = peer.0, bytes, error = %error, "dropping a datagram after failing to send it");
        if conn.close_reason().is_some() {
            return;
        }
    }
    connections.insert(peer, conn);
}

async fn write_frame(
    send: &mut quinn::SendStream,
    payload: &[u8],
) -> Result<(), TransportError> {
    let declared =
        u32::try_from(payload.len()).map_err(|_| TransportError::FrameTooLarge {
            bytes: payload.len(),
        })?;
    send.write_all(&declared.to_le_bytes())
        .await
        .map_err(write_error)?;
    send.write_all(payload).await.map_err(write_error)?;
    Ok(())
}

async fn write_raw(
    send: &mut quinn::SendStream,
    frame: &[u8],
) -> Result<(), TransportError> {
    send.write_all(frame).await.map_err(write_error)?;
    Ok(())
}

async fn read_frame(
    recv: &mut quinn::RecvStream,
) -> Result<Option<Vec<u8>>, TransportError> {
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
    use crate::protocol::StreamKind;

    static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

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
        assert_eq!(postcard::from_bytes::<StreamHeader>(&bytes).unwrap(), header);
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
    async fn loopback_parcels_and_datagrams_roundtrip() {
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
        let (transport_a, mesh_a) = channel_pair(32);
        let (transport_b, mut mesh_b) = channel_pair(32);
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
        let (peer, datagram) =
            tokio::time::timeout(Duration::from_secs(10), mesh_b.datagram_in_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer, ServerId(1));
        assert_eq!(datagram, vec![9, 9, 9]);

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
    async fn reconnect_after_gap_reports_restart_and_resumes() {
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
        let (transport_a, mesh_a) = channel_pair(32);
        let (transport_b, mut mesh_b) = channel_pair(32);
        let endpoint_a = Transport::bind(config_a, transport_a).unwrap();
        let endpoint_b = Transport::bind(config_b, transport_b).unwrap();
        let run_a = tokio::spawn(endpoint_a.run());
        let run_b = tokio::spawn(endpoint_b.run());
        wait_for_handshake().await;
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
        let linked = tokio::time::timeout(Duration::from_secs(10), mesh_b.restart_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(linked, ServerId(1));
        run_a.abort();
        let _ = run_a.await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let config_a2 = mesh_config(
            1,
            addr_a2.as_str(),
            cert_path_a,
            key_path_a,
            vec![pinned(2, addr_b.as_str(), &keys_b.0)],
        );
        let (transport_a2, mesh_a2) = channel_pair(32);
        let endpoint_a2 = Transport::bind(config_a2, transport_a2).unwrap();
        let run_a2 = tokio::spawn(endpoint_a2.run());
        let peer = tokio::time::timeout(Duration::from_secs(10), mesh_b.restart_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer, ServerId(1));
        mesh_a2
            .outbound_tx
            .send(OutboundParcel {
                peer: ServerId(2),
                header,
                bytes: vec![2],
            })
            .await
            .unwrap();
        let resumed = tokio::time::timeout(Duration::from_secs(10), mesh_b.inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.peer, ServerId(1));
        assert_eq!(resumed.bytes, vec![2]);
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
