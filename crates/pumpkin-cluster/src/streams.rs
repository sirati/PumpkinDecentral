//! QUIC uni-stream multiplexing for cluster mesh links.
//!
//! Topology: every mesh connection carries one long-lived uni stream per
//! `(StreamKind, player)` pair. Player-scoped kinds ([`PLAYER_STREAM_KINDS`],
//! four per player) isolate per-player tick state so a slow consumer stalls
//! only its own stream, while shared kinds ([`SHARED_STREAM_KINDS`]) carry
//! global entity, chunk, control and accept traffic. [`StreamRegistry`] tracks
//! which streams the local side opened and drops late or unknown frames.
//! [`UniStreamBudget`] and [`mux_keys_for_peer`] keep the total stream count
//! inside the QUIC `max_concurrent_uni_streams` limit at high player counts.
//!
//! Encoding is zero-copy by convention: `*_into` functions append postcard
//! bytes onto a caller-owned `Vec<u8>` (no intermediate buffers, ownership
//! moves instead of copies), `*_to_slice` functions write header frames into
//! stack buffers with no allocation at all, and payloads travel as borrowed
//! `&[u8]` until the single copy into the caller's frame buffer.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::{StreamKind, TickBatch};

pub const DEFAULT_QUEUE_DEPTH: usize = 64;

pub const HEADER_PREFIX_LEN: usize = 4;

pub const PLAYER_STREAM_KINDS: [StreamKind; 4] = [
    StreamKind::PlayerVisual,
    StreamKind::PlayerTransient,
    StreamKind::PlayerWorld,
    StreamKind::PlayerCombat,
];

pub const SHARED_STREAM_KINDS: [StreamKind; 9] = [
    StreamKind::EntityPos,
    StreamKind::EntityVisual,
    StreamKind::EntityTransient,
    StreamKind::EntityCombat,
    StreamKind::Control,
    StreamKind::ChunkRequest,
    StreamKind::ChunkData,
    StreamKind::ChunkAdvert,
    StreamKind::Accept,
];

/// Uni streams opened per player on each mesh connection.
pub const PLAYER_STREAM_COUNT: usize = 4;

/// Shared (player-independent) uni streams on each mesh connection.
pub const SHARED_STREAM_COUNT: usize = 9;

const _: () = assert!(PLAYER_STREAM_COUNT == PLAYER_STREAM_KINDS.len());
const _: () = assert!(SHARED_STREAM_COUNT == SHARED_STREAM_KINDS.len());

/// Highest player count multiplexed over one mesh connection.
///
/// [`MAX_MUX_STREAMS_PER_PEER`] uni streams stay under the QUIC
/// `max_concurrent_uni_streams` transport limit with headroom to spare.
pub const MAX_PLAYERS_PER_PEER: usize = 200_000;

/// Highest uni-stream count on a single mesh connection.
///
/// Four player streams per player plus the shared global streams.
pub const MAX_MUX_STREAMS_PER_PEER: usize =
    MAX_PLAYERS_PER_PEER * PLAYER_STREAM_COUNT + SHARED_STREAM_COUNT;

const _: () = assert!(MAX_MUX_STREAMS_PER_PEER as u32 <= crate::mesh::MAX_UNI_STREAMS);

/// Largest postcard encoding of any [`StreamHeader`].
///
/// A server id plus a player slot plus the kind discriminant always fit; the
/// bound lets header frames live in fixed stack buffers.
pub const MAX_HEADER_BODY_LEN: usize = 32;

/// Largest `[u32 len prefix | postcard header]` frame prefix.
pub const MAX_HEADER_FRAME_LEN: usize = HEADER_PREFIX_LEN + MAX_HEADER_BODY_LEN;

/// Whether `kind` opens one uni stream per player.
#[must_use]
pub const fn is_player_kind(kind: StreamKind) -> bool {
    matches!(
        kind,
        StreamKind::PlayerVisual
            | StreamKind::PlayerTransient
            | StreamKind::PlayerWorld
            | StreamKind::PlayerCombat
    )
}

/// Whether `kind` rides a single shared uni stream per connection.
#[must_use]
pub const fn is_shared_kind(kind: StreamKind) -> bool {
    !is_player_kind(kind)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamHeader {
    pub kind: StreamKind,
    pub player: Option<GlobalPlayerId>,
}

impl StreamHeader {
    #[must_use]
    pub const fn new(kind: StreamKind, player: Option<GlobalPlayerId>) -> Self {
        Self { kind, player }
    }

    #[must_use]
    pub const fn category(self) -> DemuxCategory {
        category_for(self.kind)
    }

    /// Whether this header belongs on a per-player uni stream.
    #[must_use]
    pub const fn expects_player(self) -> bool {
        is_player_kind(self.kind)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundParcel {
    pub peer: ServerId,
    pub header: StreamHeader,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundParcel {
    pub peer: ServerId,
    pub header: StreamHeader,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamError {
    pub message: String,
}

impl core::fmt::Display for StreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StreamError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderFrame {
    pub header: StreamHeader,
    pub len: usize,
}

/// postcard size of `header` without allocating the encoding.
///
/// Measures into a fixed stack buffer, so sizing a frame never touches the heap.
pub fn header_body_len(header: StreamHeader) -> Result<usize, StreamError> {
    let mut probe = [0_u8; MAX_HEADER_BODY_LEN];
    postcard::to_slice(&header, &mut probe)
        .map(|used| used.len())
        .map_err(|error| StreamError {
            message: format!("measure stream header: {error}"),
        })
}

/// Appends `[u32 len | postcard header]` onto `out`, returning the buffer.
///
/// Grows `out` in place: no intermediate header allocation, ownership of the
/// caller's buffer moves through instead of copying.
#[must_use]
pub fn encode_header_into(header: StreamHeader, mut out: Vec<u8>) -> Result<Vec<u8>, StreamError> {
    let start = out.len();
    out.extend_from_slice(&[0_u8; HEADER_PREFIX_LEN]);
    out = postcard::to_extend(&header, out).map_err(|error| StreamError {
        message: format!("encode stream header: {error}"),
    })?;
    let body_len = out.len() - start - HEADER_PREFIX_LEN;
    let prefix = u32::try_from(body_len).map_err(|_| StreamError {
        message: String::from("encode stream header: body too large"),
    })?;
    out[start..start + HEADER_PREFIX_LEN].copy_from_slice(&prefix.to_le_bytes());
    Ok(out)
}

/// Writes `[u32 len | postcard header]` into `buf` with zero allocation.
///
/// Returns the bytes written; `buf` needs at least [`MAX_HEADER_FRAME_LEN`]
/// bytes. Backs QUIC send paths that frame headers on the stack.
pub fn encode_header_to_slice(
    header: StreamHeader,
    buf: &mut [u8],
) -> Result<usize, StreamError> {
    let too_small = || StreamError {
        message: String::from("encode stream header: buffer too small"),
    };
    let body_len = {
        let slot = buf.get_mut(HEADER_PREFIX_LEN..).ok_or_else(too_small)?;
        postcard::to_slice(&header, slot)
            .map_err(|error| StreamError {
                message: format!("encode stream header: {error}"),
            })?
            .len()
    };
    let prefix = u32::try_from(body_len).map_err(|_| StreamError {
        message: String::from("encode stream header: body too large"),
    })?;
    buf[..HEADER_PREFIX_LEN]
        .copy_from_slice(&prefix.to_le_bytes());
    Ok(HEADER_PREFIX_LEN + body_len)
}

/// Total wire bytes for a header plus `payload_len` payload bytes.
///
/// Pre-size caller buffers with this plus `Vec::reserve` to frame payloads
/// with zero reallocations at high stream counts.
pub fn frame_len(header: StreamHeader, payload_len: usize) -> Result<usize, StreamError> {
    header_body_len(header)?
        .checked_add(HEADER_PREFIX_LEN)
        .and_then(|framed| framed.checked_add(payload_len))
        .ok_or_else(|| StreamError {
            message: String::from("frame length overflows"),
        })
}

/// Appends the postcard encoding of `batch` onto `out`, returning the buffer.
///
/// Reuses the caller's allocation across ticks: reserve once, then hand the
/// same buffer back each tick with no per-tick allocation beyond growth.
pub fn encode_batch_into(batch: &TickBatch, out: Vec<u8>) -> Result<Vec<u8>, StreamError> {
    postcard::to_extend(batch, out).map_err(|error| StreamError {
        message: format!("encode outbound batch: {error}"),
    })
}

/// Appends `[header frame | payload]` onto `out`, returning the buffer.
///
/// The payload slice is borrowed and copied once into the caller's buffer;
/// pre-size with [`frame_len`] plus `reserve` to send with zero reallocations.
pub fn assemble_frame_into(
    header: StreamHeader,
    payload: &[u8],
    out: Vec<u8>,
) -> Result<Vec<u8>, StreamError> {
    let mut out = encode_header_into(header, out)?;
    out.extend_from_slice(payload);
    Ok(out)
}

/// Appends `[header frame | parcel payload]` onto `out`, returning the buffer.
pub fn parcel_frame_into(parcel: &OutboundParcel, out: Vec<u8>) -> Result<Vec<u8>, StreamError> {
    assemble_frame_into(parcel.header, &parcel.bytes, out)
}

/// Builds an outbound parcel, encoding `batch` with no intermediate buffer.
///
/// Hot-path form of [`outbound_for_batch`]: the encoding lands directly in
/// `scratch`, which is then moved into the parcel instead of copied.
pub fn outbound_for_batch_into(
    peer: ServerId,
    kind: StreamKind,
    player: Option<GlobalPlayerId>,
    batch: &TickBatch,
    scratch: Vec<u8>,
) -> Result<OutboundParcel, StreamError> {
    let bytes = encode_batch_into(batch, scratch)?;
    Ok(OutboundParcel {
        peer,
        header: StreamHeader::new(kind, player),
        bytes,
    })
}

#[must_use]
pub fn encode_header(header: StreamHeader) -> Result<Vec<u8>, StreamError> {
    encode_header_into(header, Vec::with_capacity(MAX_HEADER_FRAME_LEN))
}

pub fn split_header_frame(buffer: &[u8]) -> Result<HeaderFrame, StreamError> {
    if buffer.len() < HEADER_PREFIX_LEN {
        return Err(StreamError {
            message: String::from("decode stream header: buffer shorter than prefix"),
        });
    }
    let mut prefix = [0_u8; HEADER_PREFIX_LEN];
    prefix.copy_from_slice(&buffer[..HEADER_PREFIX_LEN]);
    let body_len = u32::from_le_bytes(prefix) as usize;
    let total = body_len.checked_add(HEADER_PREFIX_LEN).ok_or_else(|| StreamError {
        message: String::from("decode stream header: frame length overflows"),
    })?;
    if buffer.len() < total {
        return Err(StreamError {
            message: String::from("decode stream header: buffer shorter than frame"),
        });
    }
    let header =
        postcard::from_bytes(&buffer[HEADER_PREFIX_LEN..total]).map_err(|error| StreamError {
            message: format!("decode stream header: {error}"),
        })?;
    Ok(HeaderFrame { header, len: total })
}

pub fn decode_header(frame: &[u8]) -> Result<StreamHeader, StreamError> {
    split_header_frame(frame).map(|parsed| parsed.header)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub peer: ServerId,
    pub kind: StreamKind,
    pub player: Option<GlobalPlayerId>,
}

impl StreamKey {
    #[must_use]
    pub const fn new(peer: ServerId, kind: StreamKind, player: Option<GlobalPlayerId>) -> Self {
        Self { peer, kind, player }
    }

    /// Whether this key addresses a per-player uni stream.
    #[must_use]
    pub const fn is_player(self) -> bool {
        is_player_kind(self.kind)
    }

    /// Whether this key addresses a shared global uni stream.
    #[must_use]
    pub const fn is_shared(self) -> bool {
        is_shared_kind(self.kind)
    }
}

#[derive(Debug, Default)]
pub struct StreamRegistry {
    open: HashSet<StreamKey>,
    tombstones: HashSet<StreamKey>,
    dropped_late: u64,
    dropped_unknown: u64,
}

impl StreamRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            open: HashSet::new(),
            tombstones: HashSet::new(),
            dropped_late: 0,
            dropped_unknown: 0,
        }
    }

    pub fn open(
        &mut self,
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    ) -> bool {
        let key = StreamKey::new(peer, kind, player);
        self.tombstones.remove(&key);
        self.open.insert(key)
    }

    pub fn close(
        &mut self,
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    ) -> bool {
        let key = StreamKey::new(peer, kind, player);
        let was_open = self.open.remove(&key);
        self.tombstones.insert(key);
        was_open
    }

    pub fn open_player(&mut self, peer: ServerId, player: GlobalPlayerId) {
        for kind in PLAYER_STREAM_KINDS {
            self.open(peer, kind, Some(player));
        }
    }

    pub fn close_player(&mut self, peer: ServerId, player: GlobalPlayerId) {
        for kind in PLAYER_STREAM_KINDS {
            self.close(peer, kind, Some(player));
        }
    }

    pub fn open_shared(&mut self, peer: ServerId) {
        for kind in SHARED_STREAM_KINDS {
            self.open(peer, kind, None);
        }
    }

    pub fn close_shared(&mut self, peer: ServerId) {
        for kind in SHARED_STREAM_KINDS {
            self.close(peer, kind, None);
        }
    }

    #[must_use]
    pub fn is_open(
        &self,
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    ) -> bool {
        self.open.contains(&StreamKey::new(peer, kind, player))
    }

    #[must_use]
    pub fn is_tombstoned(
        &self,
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    ) -> bool {
        self.tombstones
            .contains(&StreamKey::new(peer, kind, player))
    }

    pub fn should_deliver(
        &mut self,
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    ) -> bool {
        let key = StreamKey::new(peer, kind, player);
        if self.open.contains(&key) {
            return true;
        }
        if self.tombstones.contains(&key) {
            self.dropped_late = self.dropped_late.saturating_add(1);
        } else {
            self.dropped_unknown = self.dropped_unknown.saturating_add(1);
        }
        false
    }

    #[must_use]
    pub const fn dropped_late(&self) -> u64 {
        self.dropped_late
    }

    #[must_use]
    pub const fn dropped_unknown(&self) -> u64 {
        self.dropped_unknown
    }

    #[must_use]
    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    /// Counts currently open streams toward `peer`.
    #[must_use]
    pub fn open_count_for_peer(&self, peer: ServerId) -> usize {
        self.open.iter().filter(|key| key.peer == peer).count()
    }

    /// Uni-stream count for `player_count` players plus shared streams.
    #[must_use]
    pub fn mux_stream_count(player_count: usize) -> usize {
        player_count
            .saturating_mul(PLAYER_STREAM_COUNT)
            .saturating_add(SHARED_STREAM_COUNT)
    }

    /// Pre-reserves registry space for `player_count` players plus shared.
    ///
    /// Call before [`StreamRegistry::open_mux`] when many players join at once
    /// so the registry never rehashes mid-tick.
    pub fn reserve_for_players(&mut self, player_count: usize) {
        let streams = Self::mux_stream_count(player_count);
        self.open.reserve(streams);
        self.tombstones.reserve(streams);
    }

    /// Opens the full multiplex for `peer`: four streams per player plus shared.
    ///
    /// Idempotent: reopening a live stream clears its tombstone and keeps it open.
    pub fn open_mux(&mut self, peer: ServerId, players: &[GlobalPlayerId]) {
        self.reserve_for_players(players.len());
        for player in players {
            self.open_player(peer, *player);
        }
        self.open_shared(peer);
    }

    /// Closes the full multiplex for `peer`, tombstoning late in-flight frames.
    pub fn close_mux(&mut self, peer: ServerId, players: &[GlobalPlayerId]) {
        for player in players {
            self.close_player(peer, *player);
        }
        self.close_shared(peer);
    }

    /// Forgets every stream for `peer`, open or tombstoned.
    ///
    /// Call when the QUIC connection itself goes away; the next connection
    /// starts from a clean registry instead of stale tombstones.
    pub fn close_peer(&mut self, peer: ServerId) {
        self.open.retain(|key| key.peer != peer);
        self.tombstones.retain(|key| key.peer != peer);
    }
}

/// QUIC uni-stream budget for one mesh connection.
///
/// The connection carries four streams per player plus the shared global
/// streams; [`UniStreamBudget::fits`] guarantees the total stays under the
/// transport's `max_concurrent_uni_streams` limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniStreamBudget {
    /// Players multiplexed over the connection.
    pub players: usize,
}

impl UniStreamBudget {
    /// Budget for multiplexing `players` players over one connection.
    #[must_use]
    pub const fn new(players: usize) -> Self {
        Self { players }
    }

    /// Total uni streams: four per player plus the shared global streams.
    #[must_use]
    pub const fn total_streams(self) -> usize {
        self.players
            .saturating_mul(PLAYER_STREAM_COUNT)
            .saturating_add(SHARED_STREAM_COUNT)
    }

    /// Whether the multiplex fits the QUIC stream limit.
    #[must_use]
    pub const fn fits(self) -> bool {
        self.total_streams() <= MAX_MUX_STREAMS_PER_PEER
    }
}

/// Enumerates every uni stream for `peer`: four per player plus shared.
///
/// The returned keys double as the open set handed to [`StreamRegistry`] and
/// as the lazy-open plan for the QUIC sender: one uni stream per key, header
/// written once, then length-prefixed frames for the stream's lifetime.
#[must_use]
pub fn mux_keys_for_peer(peer: ServerId, players: &[GlobalPlayerId]) -> Vec<StreamKey> {
    let mut keys = Vec::with_capacity(StreamRegistry::mux_stream_count(players.len()));
    for player in players {
        for kind in PLAYER_STREAM_KINDS {
            keys.push(StreamKey::new(peer, kind, Some(*player)));
        }
    }
    for kind in SHARED_STREAM_KINDS {
        keys.push(StreamKey::new(peer, kind, None));
    }
    keys
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DemuxCategory {
    Visual,
    Transient,
    World,
    Combat,
    Entity,
    Control,
    Chunk,
    Accept,
}

#[must_use]
pub const fn category_for(kind: StreamKind) -> DemuxCategory {
    match kind {
        StreamKind::PlayerVisual => DemuxCategory::Visual,
        StreamKind::PlayerTransient => DemuxCategory::Transient,
        StreamKind::PlayerWorld => DemuxCategory::World,
        StreamKind::PlayerCombat => DemuxCategory::Combat,
        StreamKind::EntityPos
        | StreamKind::EntityVisual
        | StreamKind::EntityTransient
        | StreamKind::EntityCombat => DemuxCategory::Entity,
        StreamKind::Control => DemuxCategory::Control,
        StreamKind::ChunkRequest | StreamKind::ChunkData | StreamKind::ChunkAdvert => {
            DemuxCategory::Chunk
        }
        StreamKind::Accept => DemuxCategory::Accept,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemuxControl {
    Open {
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    },
    Close {
        peer: ServerId,
        kind: StreamKind,
        player: Option<GlobalPlayerId>,
    },
    OpenPlayer {
        peer: ServerId,
        player: GlobalPlayerId,
    },
    ClosePlayer {
        peer: ServerId,
        player: GlobalPlayerId,
    },
    OpenShared {
        peer: ServerId,
    },
    CloseShared {
        peer: ServerId,
    },
}

impl DemuxControl {
    pub fn apply(self, registry: &mut StreamRegistry) {
        match self {
            Self::Open { peer, kind, player } => {
                registry.open(peer, kind, player);
            }
            Self::Close { peer, kind, player } => {
                registry.close(peer, kind, player);
            }
            Self::OpenPlayer { peer, player } => {
                registry.open_player(peer, player);
            }
            Self::ClosePlayer { peer, player } => {
                registry.close_player(peer, player);
            }
            Self::OpenShared { peer } => {
                registry.open_shared(peer);
            }
            Self::CloseShared { peer } => {
                registry.close_shared(peer);
            }
        }
    }
}

#[derive(Debug)]
pub struct DemuxSenders {
    pub visual: mpsc::Sender<InboundParcel>,
    pub transient: mpsc::Sender<InboundParcel>,
    pub world: mpsc::Sender<InboundParcel>,
    pub combat: mpsc::Sender<InboundParcel>,
    pub entity: mpsc::Sender<InboundParcel>,
    pub control: mpsc::Sender<InboundParcel>,
    pub chunk: mpsc::Sender<InboundParcel>,
    pub accept: mpsc::Sender<InboundParcel>,
}

impl DemuxSenders {
    #[must_use]
    pub const fn sender_for(&self, category: DemuxCategory) -> &mpsc::Sender<InboundParcel> {
        match category {
            DemuxCategory::Visual => &self.visual,
            DemuxCategory::Transient => &self.transient,
            DemuxCategory::World => &self.world,
            DemuxCategory::Combat => &self.combat,
            DemuxCategory::Entity => &self.entity,
            DemuxCategory::Control => &self.control,
            DemuxCategory::Chunk => &self.chunk,
            DemuxCategory::Accept => &self.accept,
        }
    }
}

#[derive(Debug)]
pub struct DemuxReceivers {
    pub visual: mpsc::Receiver<InboundParcel>,
    pub transient: mpsc::Receiver<InboundParcel>,
    pub world: mpsc::Receiver<InboundParcel>,
    pub combat: mpsc::Receiver<InboundParcel>,
    pub entity: mpsc::Receiver<InboundParcel>,
    pub control: mpsc::Receiver<InboundParcel>,
    pub chunk: mpsc::Receiver<InboundParcel>,
    pub accept: mpsc::Receiver<InboundParcel>,
}

#[must_use]
pub fn demux_channels(depth: usize) -> (DemuxSenders, DemuxReceivers) {
    let depth = depth.max(1);
    let (visual_tx, visual_rx) = mpsc::channel(depth);
    let (transient_tx, transient_rx) = mpsc::channel(depth);
    let (world_tx, world_rx) = mpsc::channel(depth);
    let (combat_tx, combat_rx) = mpsc::channel(depth);
    let (entity_tx, entity_rx) = mpsc::channel(depth);
    let (control_tx, control_rx) = mpsc::channel(depth);
    let (chunk_tx, chunk_rx) = mpsc::channel(depth);
    let (accept_tx, accept_rx) = mpsc::channel(depth);
    let senders = DemuxSenders {
        visual: visual_tx,
        transient: transient_tx,
        world: world_tx,
        combat: combat_tx,
        entity: entity_tx,
        control: control_tx,
        chunk: chunk_tx,
        accept: accept_tx,
    };
    let receivers = DemuxReceivers {
        visual: visual_rx,
        transient: transient_rx,
        world: world_rx,
        combat: combat_rx,
        entity: entity_rx,
        control: control_rx,
        chunk: chunk_rx,
        accept: accept_rx,
    };
    (senders, receivers)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DemuxStats {
    pub visual: u64,
    pub transient: u64,
    pub world: u64,
    pub combat: u64,
    pub entity: u64,
    pub control: u64,
    pub chunk: u64,
    pub accept: u64,
    pub dropped_late: u64,
    pub dropped_unknown: u64,
    pub dropped_queue_closed: u64,
}

impl DemuxStats {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            visual: 0,
            transient: 0,
            world: 0,
            combat: 0,
            entity: 0,
            control: 0,
            chunk: 0,
            accept: 0,
            dropped_late: 0,
            dropped_unknown: 0,
            dropped_queue_closed: 0,
        }
    }

    pub const fn note_forwarded(&mut self, category: DemuxCategory) {
        match category {
            DemuxCategory::Visual => self.visual = self.visual.saturating_add(1),
            DemuxCategory::Transient => self.transient = self.transient.saturating_add(1),
            DemuxCategory::World => self.world = self.world.saturating_add(1),
            DemuxCategory::Combat => self.combat = self.combat.saturating_add(1),
            DemuxCategory::Entity => self.entity = self.entity.saturating_add(1),
            DemuxCategory::Control => self.control = self.control.saturating_add(1),
            DemuxCategory::Chunk => self.chunk = self.chunk.saturating_add(1),
            DemuxCategory::Accept => self.accept = self.accept.saturating_add(1),
        }
    }

    #[must_use]
    pub const fn total_forwarded(&self) -> u64 {
        self.visual
            .saturating_add(self.transient)
            .saturating_add(self.world)
            .saturating_add(self.combat)
            .saturating_add(self.entity)
            .saturating_add(self.control)
            .saturating_add(self.chunk)
            .saturating_add(self.accept)
    }
}

pub async fn run_demux(
    mut inbound: mpsc::Receiver<InboundParcel>,
    mut control: mpsc::Receiver<DemuxControl>,
    senders: DemuxSenders,
    mut registry: StreamRegistry,
) -> DemuxStats {
    let mut stats = DemuxStats::new();
    let mut control_open = true;
    loop {
        if control_open {
            match control.try_recv() {
                Ok(message) => {
                    message.apply(&mut registry);
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    control_open = false;
                }
            }
        }
        let Some(parcel) = inbound.recv().await else {
            break;
        };
        if !registry.should_deliver(parcel.peer, parcel.header.kind, parcel.header.player) {
            continue;
        }
        let category = parcel.header.category();
        if senders.sender_for(category).send(parcel).await.is_err() {
            stats.dropped_queue_closed = stats.dropped_queue_closed.saturating_add(1);
        } else {
            stats.note_forwarded(category);
        }
    }
    stats.dropped_late = registry.dropped_late();
    stats.dropped_unknown = registry.dropped_unknown();
    stats
}

#[must_use]
pub fn make_outbound(
    peer: ServerId,
    header: StreamHeader,
    bytes: Vec<u8>,
) -> OutboundParcel {
    OutboundParcel { peer, header, bytes }
}

pub fn outbound_for_batch(
    peer: ServerId,
    kind: StreamKind,
    player: Option<GlobalPlayerId>,
    batch: &TickBatch,
) -> Result<OutboundParcel, StreamError> {
    outbound_for_batch_into(peer, kind, player, batch, Vec::new())
}

pub fn assemble_frame(header: StreamHeader, payload: &[u8]) -> Result<Vec<u8>, StreamError> {
    let capacity = frame_len(header, payload.len())?;
    assemble_frame_into(header, payload, Vec::with_capacity(capacity))
}

pub fn parcel_frame(parcel: &OutboundParcel) -> Result<Vec<u8>, StreamError> {
    assemble_frame(parcel.header, &parcel.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    const fn test_peer() -> ServerId {
        ServerId(7)
    }

    const fn test_player() -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(7), PlayerSlot(3))
    }

    fn parcel_for(kind: StreamKind, player: Option<GlobalPlayerId>) -> InboundParcel {
        InboundParcel {
            peer: test_peer(),
            header: StreamHeader::new(kind, player),
            bytes: vec![1, 2, 3],
        }
    }

    #[test]
    fn header_roundtrip_with_player() {
        let header = StreamHeader::new(StreamKind::PlayerWorld, Some(test_player()));
        let frame = encode_header(header).unwrap();
        assert_eq!(decode_header(&frame).unwrap(), header);
    }

    #[test]
    fn header_roundtrip_shared() {
        let header = StreamHeader::new(StreamKind::Accept, None);
        let frame = encode_header(header).unwrap();
        let parsed = split_header_frame(&frame).unwrap();
        assert_eq!(parsed.header, header);
        assert_eq!(parsed.len, frame.len());
    }

    #[test]
    fn header_decode_ignores_trailing_payload() {
        let header = StreamHeader::new(StreamKind::Control, None);
        let mut frame = encode_header(header).unwrap();
        frame.extend_from_slice(&[9, 9, 9]);
        assert_eq!(decode_header(&frame).unwrap(), header);
    }

    #[test]
    fn header_decode_rejects_garbage() {
        assert!(decode_header(&[]).is_err());
        assert!(decode_header(&[1, 2, 3]).is_err());
        assert!(decode_header(&[10, 0, 0, 0, 1]).is_err());
        assert!(decode_header(&[1, 0, 0, 0, 0xFF]).is_err());
    }

    #[test]
    fn registry_open_close_counts_late_frames() {
        let mut registry = StreamRegistry::new();
        let player = Some(test_player());
        assert!(registry.open(test_peer(), StreamKind::PlayerVisual, player));
        assert!(!registry.open(test_peer(), StreamKind::PlayerVisual, player));
        assert!(registry.is_open(test_peer(), StreamKind::PlayerVisual, player));
        assert!(registry.should_deliver(test_peer(), StreamKind::PlayerVisual, player));

        assert!(registry.close(test_peer(), StreamKind::PlayerVisual, player));
        assert!(!registry.is_open(test_peer(), StreamKind::PlayerVisual, player));
        assert!(registry.is_tombstoned(test_peer(), StreamKind::PlayerVisual, player));
        assert!(!registry.should_deliver(test_peer(), StreamKind::PlayerVisual, player));
        assert!(!registry.should_deliver(test_peer(), StreamKind::PlayerVisual, player));
        assert_eq!(registry.dropped_late, 2);
        assert_eq!(registry.dropped_unknown, 0);

        assert!(!registry.should_deliver(test_peer(), StreamKind::PlayerCombat, player));
        assert_eq!(registry.dropped_unknown, 1);

        assert!(registry.open(test_peer(), StreamKind::PlayerVisual, player));
        assert!(!registry.is_tombstoned(test_peer(), StreamKind::PlayerVisual, player));
        assert!(registry.should_deliver(test_peer(), StreamKind::PlayerVisual, player));
    }

    #[test]
    fn registry_login_logout_helpers() {
        let mut registry = StreamRegistry::new();
        registry.open_player(test_peer(), test_player());
        assert_eq!(registry.open_count(), PLAYER_STREAM_KINDS.len());
        for kind in PLAYER_STREAM_KINDS {
            assert!(registry.is_open(test_peer(), kind, Some(test_player())));
        }
        registry.close_player(test_peer(), test_player());
        assert_eq!(registry.open_count(), 0);
        for kind in PLAYER_STREAM_KINDS {
            assert!(registry.is_tombstoned(test_peer(), kind, Some(test_player())));
            assert!(!registry.should_deliver(test_peer(), kind, Some(test_player())));
        }
        assert_eq!(registry.dropped_late, PLAYER_STREAM_KINDS.len() as u64);
    }

    #[test]
    fn registry_shared_helpers() {
        let mut registry = StreamRegistry::new();
        registry.open_shared(test_peer());
        assert_eq!(registry.open_count(), SHARED_STREAM_KINDS.len());
        registry.close_shared(test_peer());
        assert_eq!(registry.open_count(), 0);
        assert!(!registry.should_deliver(test_peer(), StreamKind::Accept, None));
        assert_eq!(registry.dropped_late, 1);
    }

    #[test]
    fn control_messages_apply_to_registry() {
        let mut registry = StreamRegistry::new();
        DemuxControl::OpenPlayer {
            peer: test_peer(),
            player: test_player(),
        }
        .apply(&mut registry);
        assert!(registry.is_open(test_peer(), StreamKind::PlayerCombat, Some(test_player())));
        DemuxControl::ClosePlayer {
            peer: test_peer(),
            player: test_player(),
        }
        .apply(&mut registry);
        assert!(registry.is_tombstoned(test_peer(), StreamKind::PlayerCombat, Some(test_player())));
        DemuxControl::OpenShared { peer: test_peer() }.apply(&mut registry);
        assert!(registry.is_open(test_peer(), StreamKind::Control, None));
        DemuxControl::CloseShared { peer: test_peer() }.apply(&mut registry);
        assert!(registry.is_tombstoned(test_peer(), StreamKind::Control, None));
    }

    #[test]
    fn every_stream_kind_has_a_category() {
        let kinds = [
            StreamKind::PlayerVisual,
            StreamKind::PlayerTransient,
            StreamKind::PlayerWorld,
            StreamKind::PlayerCombat,
            StreamKind::EntityPos,
            StreamKind::EntityVisual,
            StreamKind::EntityTransient,
            StreamKind::EntityCombat,
            StreamKind::Control,
            StreamKind::ChunkRequest,
            StreamKind::ChunkData,
            StreamKind::ChunkAdvert,
            StreamKind::Accept,
        ];
        assert_eq!(category_for(StreamKind::PlayerVisual), DemuxCategory::Visual);
        assert_eq!(
            category_for(StreamKind::PlayerTransient),
            DemuxCategory::Transient
        );
        assert_eq!(category_for(StreamKind::PlayerWorld), DemuxCategory::World);
        assert_eq!(category_for(StreamKind::PlayerCombat), DemuxCategory::Combat);
        for kind in [
            StreamKind::EntityPos,
            StreamKind::EntityVisual,
            StreamKind::EntityTransient,
            StreamKind::EntityCombat,
        ] {
            assert_eq!(category_for(kind), DemuxCategory::Entity);
        }
        assert_eq!(category_for(StreamKind::Control), DemuxCategory::Control);
        assert_eq!(category_for(StreamKind::ChunkRequest), DemuxCategory::Chunk);
        assert_eq!(category_for(StreamKind::ChunkData), DemuxCategory::Chunk);
        assert_eq!(category_for(StreamKind::ChunkAdvert), DemuxCategory::Chunk);
        assert_eq!(category_for(StreamKind::Accept), DemuxCategory::Accept);
        assert_eq!(kinds.len(), 13);
    }

    #[tokio::test]
    async fn demux_routes_to_category_queues() {
        let mut registry = StreamRegistry::new();
        registry.open_player(test_peer(), test_player());
        registry.open_shared(test_peer());
        let (inbound_tx, inbound_rx) = mpsc::channel(16);
        let (control_tx, control_rx) = mpsc::channel(4);
        let (senders, mut queues) = demux_channels(8);

        let task = tokio::spawn(run_demux(inbound_rx, control_rx, senders, registry));
        drop(control_tx);

        let player = Some(test_player());
        inbound_tx
            .send(parcel_for(StreamKind::PlayerVisual, player))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::PlayerTransient, player))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::PlayerWorld, player))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::PlayerCombat, player))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::EntityPos, None))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::Control, None))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::ChunkData, None))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::Accept, None))
            .await
            .unwrap();
        drop(inbound_tx);

        let stats = task.await.unwrap();
        assert_eq!(stats.total_forwarded(), 8);
        assert_eq!(stats.dropped_late, 0);
        assert_eq!(stats.dropped_unknown, 0);
        assert_eq!(queues.visual.recv().await.unwrap().header.kind, StreamKind::PlayerVisual);
        assert_eq!(
            queues.transient.recv().await.unwrap().header.kind,
            StreamKind::PlayerTransient
        );
        assert_eq!(queues.world.recv().await.unwrap().header.kind, StreamKind::PlayerWorld);
        assert_eq!(queues.combat.recv().await.unwrap().header.kind, StreamKind::PlayerCombat);
        assert_eq!(queues.entity.recv().await.unwrap().header.kind, StreamKind::EntityPos);
        assert_eq!(queues.control.recv().await.unwrap().header.kind, StreamKind::Control);
        assert_eq!(queues.chunk.recv().await.unwrap().header.kind, StreamKind::ChunkData);
        assert_eq!(queues.accept.recv().await.unwrap().header.kind, StreamKind::Accept);
    }

    #[tokio::test]
    async fn demux_drops_late_frames_after_close() {
        let mut registry = StreamRegistry::new();
        registry.open_player(test_peer(), test_player());
        registry.close_player(test_peer(), test_player());
        let (inbound_tx, inbound_rx) = mpsc::channel(4);
        let (control_tx, control_rx) = mpsc::channel(4);
        let (senders, mut queues) = demux_channels(4);

        let task = tokio::spawn(run_demux(inbound_rx, control_rx, senders, registry));
        drop(control_tx);
        inbound_tx
            .send(parcel_for(StreamKind::PlayerVisual, Some(test_player())))
            .await
            .unwrap();
        inbound_tx
            .send(parcel_for(StreamKind::PlayerCombat, Some(test_player())))
            .await
            .unwrap();
        drop(inbound_tx);

        let stats = task.await.unwrap();
        assert_eq!(stats.total_forwarded(), 0);
        assert_eq!(stats.dropped_late, 2);
        assert!(queues.visual.try_recv().is_err());
        assert!(queues.combat.try_recv().is_err());
    }

    #[tokio::test]
    async fn demux_applies_control_before_routing() {
        let registry = StreamRegistry::new();
        let (inbound_tx, inbound_rx) = mpsc::channel(4);
        let (control_tx, control_rx) = mpsc::channel(4);
        let (senders, mut queues) = demux_channels(4);

        control_tx
            .send(DemuxControl::OpenPlayer {
                peer: test_peer(),
                player: test_player(),
            })
            .await
            .unwrap();
        drop(control_tx);
        let task = tokio::spawn(run_demux(inbound_rx, control_rx, senders, registry));
        inbound_tx
            .send(parcel_for(StreamKind::PlayerCombat, Some(test_player())))
            .await
            .unwrap();
        drop(inbound_tx);

        let stats = task.await.unwrap();
        assert_eq!(stats.total_forwarded(), 1);
        assert_eq!(queues.combat.recv().await.unwrap().header.kind, StreamKind::PlayerCombat);
    }

    #[test]
    fn mux_helpers_roundtrip() {
        let batch = TickBatch::new(crate::time::TickStamp(5));
        assert!(batch.is_empty());
        let parcel = outbound_for_batch(
            test_peer(),
            StreamKind::PlayerVisual,
            Some(test_player()),
            &batch,
        )
        .unwrap();
        assert_eq!(parcel.peer, test_peer());
        assert_eq!(parcel.header.kind, StreamKind::PlayerVisual);
        let back = crate::codec::decode_batch(&parcel.bytes).unwrap();
        assert_eq!(back, batch);

        let direct = make_outbound(parcel.peer, parcel.header, parcel.bytes.clone());
        assert_eq!(direct, parcel);

        let frame = parcel_frame(&parcel).unwrap();
        let parsed = split_header_frame(&frame).unwrap();
        assert_eq!(parsed.header, parcel.header);
        assert_eq!(&frame[parsed.len..], parcel.bytes.as_slice());
    }

    #[test]
    fn zero_copy_encoders_match_allocating_forms() {
        let header = StreamHeader::new(StreamKind::PlayerCombat, Some(test_player()));
        assert!(header.expects_player());
        assert!(!StreamHeader::new(StreamKind::Accept, None).expects_player());

        let owned = encode_header(header).unwrap();
        let appended = encode_header_into(header, Vec::new()).unwrap();
        assert_eq!(owned, appended);
        assert_eq!(header_body_len(header).unwrap() + HEADER_PREFIX_LEN, owned.len());

        let mut stack = [0_u8; MAX_HEADER_FRAME_LEN];
        let written = encode_header_to_slice(header, &mut stack).unwrap();
        assert_eq!(&stack[..written], owned.as_slice());
        assert!(encode_header_to_slice(header, &mut stack[..2]).is_err());

        assert_eq!(frame_len(header, 3).unwrap(), owned.len() + 3);

        let batch = TickBatch::new(crate::time::TickStamp(9));
        let parcel =
            outbound_for_batch(test_peer(), StreamKind::EntityPos, None, &batch).unwrap();
        let reused = outbound_for_batch_into(
            test_peer(),
            StreamKind::EntityPos,
            None,
            &batch,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(parcel, reused);

        let framed = parcel_frame(&reused).unwrap();
        let reframed = parcel_frame_into(&reused, Vec::new()).unwrap();
        assert_eq!(framed, reframed);
    }

    #[test]
    fn mux_layout_covers_players_plus_shared() {
        let other = GlobalPlayerId::new(ServerId(7), PlayerSlot(4));
        let players = [test_player(), other];
        let keys = mux_keys_for_peer(test_peer(), &players);
        assert_eq!(keys.len(), StreamRegistry::mux_stream_count(players.len()));
        assert_eq!(keys.len(), 2 * PLAYER_STREAM_COUNT + SHARED_STREAM_COUNT);
        for key in &keys {
            assert_eq!(key.is_player(), key.player.is_some());
            assert_eq!(key.is_shared(), key.player.is_none());
        }

        let budget = UniStreamBudget::new(players.len());
        assert_eq!(budget.total_streams(), keys.len());
        assert!(budget.fits());
        assert!(UniStreamBudget::new(MAX_PLAYERS_PER_PEER).fits());
        assert!(!UniStreamBudget::new(MAX_PLAYERS_PER_PEER + 1).fits());
        assert_eq!(
            UniStreamBudget::new(MAX_PLAYERS_PER_PEER).total_streams(),
            MAX_MUX_STREAMS_PER_PEER
        );

        let mut registry = StreamRegistry::new();
        registry.open_mux(test_peer(), &players);
        assert_eq!(registry.open_count(), keys.len());
        assert_eq!(registry.open_count_for_peer(test_peer()), keys.len());
        for key in &keys {
            assert!(registry.is_open(key.peer, key.kind, key.player));
        }
        registry.close_mux(test_peer(), &players);
        assert_eq!(registry.open_count(), 0);
        assert_eq!(registry.open_count_for_peer(test_peer()), 0);

        registry.open_mux(test_peer(), &players);
        registry.close_peer(test_peer());
        assert_eq!(registry.open_count(), 0);
        assert_eq!(registry.open_count_for_peer(test_peer()), 0);
        assert!(
            !registry.should_deliver(test_peer(), StreamKind::PlayerVisual, Some(test_player()))
        );
    }
}
