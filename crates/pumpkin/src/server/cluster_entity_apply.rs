use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::entities::{
    ENTITY_STREAM_KINDS, EntityCombatUpdate, EntityDespawn, EntityGhosts, EntityPosUpdate,
    EntitySpawn, EntityTransientUpdate, EntityVisualUpdate, GhostState, route_to_holders,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkAddr, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_util::math::vector2::Vector2;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram};

static ENTITY_APPLIED: AtomicU64 = AtomicU64::new(0);
static ENTITY_DROPPED: AtomicU64 = AtomicU64::new(0);
static ENTITY_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static ENTITY_EMITTED: AtomicU64 = AtomicU64::new(0);
static ENTITY_MALFORMED: AtomicU64 = AtomicU64::new(0);

struct EntityOutbox {
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static ENTITY_OUTBOX: OnceLock<EntityOutbox> = OnceLock::new();
static ENTITY_POS_BRIDGE: OnceLock<mpsc::Sender<(ServerId, Vec<EntityPosUpdate>)>> =
    OnceLock::new();

pub const ENTITY_SPAWN_MAGIC: u32 = 0x454E5350;
pub const ENTITY_DESPAWN_MAGIC: u32 = 0x454E4458;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TaggedEntitySpawn {
    pub magic: u32,
    pub update: EntitySpawn,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TaggedEntityDespawn {
    pub magic: u32,
    pub update: EntityDespawn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityPosStreamFrame {
    Spawn,
    Despawn,
    Datagram,
    LegacyBarePos,
}

fn classify_entity_pos_stream(bytes: &[u8]) -> Result<EntityPosStreamFrame, postcard::Error> {
    let (magic, _) = postcard::take_from_bytes::<u32>(bytes)?;
    Ok(match magic {
        ENTITY_SPAWN_MAGIC => EntityPosStreamFrame::Spawn,
        ENTITY_DESPAWN_MAGIC => EntityPosStreamFrame::Despawn,
        ENTITY_POS_MAGIC => EntityPosStreamFrame::Datagram,
        _ => EntityPosStreamFrame::LegacyBarePos,
    })
}

#[must_use]
pub fn entity_emitted() -> u64 {
    ENTITY_EMITTED.load(Ordering::Relaxed)
}

pub fn install_entity_outbox(
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = ENTITY_OUTBOX.set(EntityOutbox {
        local,
        peers,
        outbound,
    });
}

pub fn install_entity_pos_bridge(sink: mpsc::Sender<(ServerId, Vec<EntityPosUpdate>)>) {
    let _ = ENTITY_POS_BRIDGE.set(sink);
}

#[must_use]
pub fn entity_stream_kinds() -> [StreamKind; 4] {
    ENTITY_STREAM_KINDS
}

fn try_emit_entity(kind: StreamKind, bytes: &[u8]) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if outbox.peers.is_empty() {
        return 0;
    }
    let mut sent = 0_u64;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(kind, None),
            bytes: bytes.to_vec(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        } else {
            note_dropped("full");
        }
    }
    if sent > 0 {
        ENTITY_EMITTED.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn emit_entity_parcel(owner: ServerId, kind: StreamKind, bytes: &[u8]) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if owner != outbox.local {
        note_dropped("owner");
        return 0;
    }
    try_emit_entity(kind, bytes)
}

pub fn forward_entity_pos_datagram(peer: ServerId, updates: Vec<EntityPosUpdate>) -> bool {
    if updates.is_empty() {
        return false;
    }
    let Some(bridge) = ENTITY_POS_BRIDGE.get() else {
        note_dropped("no-bridge");
        return false;
    };
    if bridge.try_send((peer, updates)).is_ok() {
        true
    } else {
        note_dropped("full");
        false
    }
}

pub fn spawn_entity_datagram_apply(
    server: &Arc<Server>,
    mut datagram_rx: mpsc::Receiver<(ServerId, Vec<u8>)>,
) {
    server.spawn_task(async move {
        while let Some((peer, bytes)) = datagram_rx.recv().await {
            let is_entity_position = matches!(
                postcard::take_from_bytes::<u32>(&bytes),
                Ok((magic, _)) if magic == ENTITY_POS_MAGIC
            );
            if !is_entity_position {
                continue;
            }
            let datagram = match postcard::from_bytes::<EntityPosDatagram>(&bytes) {
                Ok(datagram) if datagram.is_consistent() && !datagram.is_empty() => datagram,
                Ok(_) => {
                    note_dropped("entity-pos-envelope");
                    continue;
                }
                Err(error) => {
                    note_malformed_pos_frame(peer, &bytes, &error);
                    continue;
                }
            };
            ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            forward_entity_pos_datagram(peer, datagram.updates);
        }
    });
}

#[must_use]
pub fn entity_applied() -> u64 {
    ENTITY_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn entity_dropped() -> u64 {
    ENTITY_DROPPED.load(Ordering::Relaxed)
}

#[must_use]
pub fn entity_datagrams() -> u64 {
    ENTITY_DATAGRAMS.load(Ordering::Relaxed)
}

/// Number of invalid entity position frames received from cluster peers.
///
/// This remains a metric even though the warning is deliberately rate-limited:
/// a connected peer can otherwise turn one malformed frame into a log/CPU
/// amplification loop.
#[must_use]
pub fn entity_malformed() -> u64 {
    ENTITY_MALFORMED.load(Ordering::Relaxed)
}

fn chunk_holders(server: &Server, local: ServerId, chunk: ChunkAddr) -> Vec<u16> {
    let pos = Vector2::new(chunk.x, chunk.z);
    let loaded = server
        .worlds
        .load()
        .iter()
        .any(|world| world.level.is_chunk_loaded(&pos));
    if loaded {
        vec![local.0]
    } else {
        Vec::new()
    }
}

fn holds_chunk(server: &Server, local: ServerId, chunk: ChunkAddr) -> bool {
    route_to_holders(chunk, &|addr| chunk_holders(server, local, addr)).contains(&local.0)
}

fn owned_by_sender(peer: ServerId, owner: ServerId, local: ServerId) -> bool {
    if owner == local {
        return false;
    }
    peer == owner
}

fn note_applied() {
    ENTITY_APPLIED.fetch_add(1, Ordering::Relaxed);
}

fn note_dropped(reason: &str) {
    ENTITY_DROPPED.fetch_add(1, Ordering::Relaxed);
    debug!(reason = reason, "cluster entity update dropped");
}

fn note_malformed_pos_frame(peer: ServerId, bytes: &[u8], error: &impl std::fmt::Display) {
    let occurrence = ENTITY_MALFORMED.fetch_add(1, Ordering::Relaxed) + 1;
    note_dropped("decode");

    // The first frame makes a protocol/version issue visible.  Thereafter,
    // report at most one per 1,024 bad frames from all authenticated peers.
    // Keeping the exact count preserves diagnostics without allowing a bad
    // peer to fill the log or consume a core formatting warnings.
    if occurrence == 1 || occurrence % 1024 == 0 {
        warn!(
            peer = peer.0,
            bytes = bytes.len(),
            occurrence,
            %error,
            "dropping malformed cluster entity position frame"
        );
    }
}

pub fn apply_pos_update(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    update: &EntityPosUpdate,
) -> bool {
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if ghosts.apply_pos(update) {
        note_applied();
        return true;
    }
    ghosts.by_ref.insert(
        update.entity,
        GhostState {
            chunk: update.entity.chunk,
            last_tick: update.tick,
            pos: update.pos,
            yaw: update.yaw,
            pitch: update.pitch,
            kind: 0,
        },
    );
    note_applied();
    true
}

pub fn apply_pos_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let update: EntityPosUpdate = match postcard::from_bytes(bytes) {
        Ok(update) => update,
        Err(error) => {
            note_malformed_pos_frame(peer, bytes, &error);
            return false;
        }
    };
    apply_pos_update(server, ghosts, local, peer, &update)
}

fn insert_spawn(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    update: &EntitySpawn,
) -> bool {
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    ghosts.spawn(update);
    note_applied();
    true
}

fn remove_despawn(
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    update: &EntityDespawn,
) -> bool {
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if ghosts.despawn(update) {
        note_applied();
        return true;
    }
    note_dropped("unknown");
    false
}

pub fn apply_spawn_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let tagged: TaggedEntitySpawn = match postcard::from_bytes(bytes) {
        Ok(tagged) => tagged,
        Err(error) => {
            warn!(%error, "cluster entity spawn decode failed");
            note_dropped("decode");
            return false;
        }
    };
    if tagged.magic != ENTITY_SPAWN_MAGIC {
        note_dropped("magic");
        return false;
    }
    insert_spawn(server, ghosts, local, peer, &tagged.update)
}

pub fn apply_despawn_bytes(
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let tagged: TaggedEntityDespawn = match postcard::from_bytes(bytes) {
        Ok(tagged) => tagged,
        Err(error) => {
            warn!(%error, "cluster entity despawn decode failed");
            note_dropped("decode");
            return false;
        }
    };
    if tagged.magic != ENTITY_DESPAWN_MAGIC {
        note_dropped("magic");
        return false;
    }
    remove_despawn(ghosts, local, peer, &tagged.update)
}

/// Applies the reliable `EntityPos` stream's explicitly tagged payloads.
///
/// Positions normally travel in QUIC datagrams.  Spawn/despawn lifecycle
/// events use the reliable `EntityPos` stream and have their own magic
/// values.  Older peers briefly used the position-datagram envelope on that
/// stream, so accept that *complete, validated* envelope as well.  Crucially,
/// do not try each unrelated postcard struct until one happens to fail: a
/// full position datagram then looked like a malformed bare position update
/// and produced one warning for every entity batch.
fn apply_pos_stream_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let frame = match classify_entity_pos_stream(bytes) {
        Ok(frame) => frame,
        Err(error) => {
            note_malformed_pos_frame(peer, bytes, &error);
            return false;
        }
    };
    match frame {
        EntityPosStreamFrame::Spawn => apply_spawn_bytes(server, ghosts, local, peer, bytes),
        EntityPosStreamFrame::Despawn => apply_despawn_bytes(ghosts, local, peer, bytes),
        EntityPosStreamFrame::Datagram => {
            let datagram = match postcard::from_bytes::<EntityPosDatagram>(bytes) {
                Ok(datagram) => datagram,
                Err(error) => {
                    note_malformed_pos_frame(peer, bytes, &error);
                    return false;
                }
            };
            if !datagram.is_consistent() || datagram.is_empty() {
                note_dropped("entity-pos-envelope");
                return false;
            }
            forward_entity_pos_datagram(peer, datagram.updates)
        }
        // Bare EntityPosUpdate was the original reliable-stream format.
        // Retain it only as a fully decoded compatibility path; arbitrary
        // data is never accepted merely because it shares this stream kind.
        EntityPosStreamFrame::LegacyBarePos => {
            apply_pos_bytes(server, ghosts, local, peer, bytes)
        }
    }
}

pub fn apply_visual_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let update: EntityVisualUpdate = match postcard::from_bytes(bytes) {
        Ok(update) => update,
        Err(error) => {
            warn!(%error, "cluster entity visual decode failed");
            note_dropped("decode");
            return false;
        }
    };
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if ghosts.apply_visual(&update) {
        note_applied();
        true
    } else {
        note_dropped("unknown");
        false
    }
}

pub fn apply_transient_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let update: EntityTransientUpdate = match postcard::from_bytes(bytes) {
        Ok(update) => update,
        Err(error) => {
            warn!(%error, "cluster entity transient decode failed");
            note_dropped("decode");
            return false;
        }
    };
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if ghosts.apply_transient(&update) {
        note_applied();
        true
    } else {
        note_dropped("unknown");
        false
    }
}

pub fn apply_combat_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    bytes: &[u8],
) -> bool {
    let update: EntityCombatUpdate = match postcard::from_bytes(bytes) {
        Ok(update) => update,
        Err(error) => {
            warn!(%error, "cluster entity combat decode failed");
            note_dropped("decode");
            return false;
        }
    };
    if !owned_by_sender(peer, update.entity.owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if ghosts.apply_combat(&update) {
        note_applied();
        true
    } else {
        note_dropped("unknown");
        false
    }
}

fn apply_parcel(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    parcel: &InboundParcel,
) {
    match parcel.header.kind {
        StreamKind::EntityPos => {
            apply_pos_stream_bytes(server, ghosts, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityVisual => {
            apply_visual_bytes(server, ghosts, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityTransient => {
            apply_transient_bytes(server, ghosts, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityCombat => {
            apply_combat_bytes(server, ghosts, local, parcel.peer, &parcel.bytes);
        }
        _ => {
            note_dropped("kind");
        }
    }
}

pub fn spawn_entity_apply(
    server: &Arc<Server>,
    local: ServerId,
    mut entity_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let (pos_tx, mut pos_rx) = mpsc::channel(64);
        install_entity_pos_bridge(pos_tx);
        let mut ghosts = EntityGhosts::new();
        let mut first = true;
        loop {
            tokio::select! {
                parcel = entity_rx.recv() => {
                    let Some(parcel) = parcel else { break };
                    ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                    if first {
                        first = false;
                        debug!(from = parcel.peer.0, kind = ?parcel.header.kind, "cluster entity stream started");
                    }
                    apply_parcel(&task_server, &mut ghosts, local, &parcel);
                }
                batch = pos_rx.recv() => {
                    let Some((peer, updates)) = batch else { break };
                    ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                    for update in &updates {
                        apply_pos_update(&task_server, &mut ghosts, local, peer, update);
                    }
                }
            }
        }
        debug!("cluster entity stream closed");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::EntityRef;
    use pumpkin_cluster::time::TickStamp;

    fn test_ref(owner: u16, local_id: i32) -> EntityRef {
        EntityRef {
            owner: ServerId(owner),
            local_id,
            chunk: ChunkAddr { x: 0, z: 0 },
        }
    }

    fn test_spawn() -> EntitySpawn {
        EntitySpawn {
            entity: test_ref(2, 7),
            tick: TickStamp(10),
            kind: 3,
            pos: [1.0, 2.0, 3.0],
            yaw: 90.0,
            pitch: 0.0,
        }
    }

    fn test_despawn() -> EntityDespawn {
        EntityDespawn {
            entity: test_ref(2, 7),
            tick: TickStamp(11),
        }
    }

    fn test_pos() -> EntityPosUpdate {
        EntityPosUpdate {
            entity: test_ref(2, 7),
            tick: TickStamp(12),
            pos: [4.0, 5.0, 6.0],
            vel: [0.1, 0.0, 0.0],
            yaw: 180.0,
            pitch: 5.0,
        }
    }

    #[test]
    fn lifecycle_and_pos_streams_decode_disjointly() {
        fn only<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> bool {
            match postcard::take_from_bytes::<T>(bytes) {
                Ok((_, rest)) => rest.is_empty(),
                Err(_) => false,
            }
        }
        let spawn_bytes = postcard::to_allocvec(&test_spawn()).expect("spawn encodes");
        let despawn_bytes = postcard::to_allocvec(&test_despawn()).expect("despawn encodes");
        let pos_bytes = postcard::to_allocvec(&test_pos()).expect("pos encodes");
        assert!(only::<EntitySpawn>(&spawn_bytes));
        assert!(!only::<EntityDespawn>(&spawn_bytes));
        assert!(!only::<EntityPosUpdate>(&spawn_bytes));
        assert!(only::<EntityDespawn>(&despawn_bytes));
        assert!(!only::<EntitySpawn>(&despawn_bytes));
        assert!(!only::<EntityPosUpdate>(&despawn_bytes));
        assert!(only::<EntityPosUpdate>(&pos_bytes));
        assert!(!only::<EntitySpawn>(&pos_bytes));
        assert!(!only::<EntityDespawn>(&pos_bytes));
    }

    #[test]
    fn lifecycle_tags_differ_and_roundtrip() {
        assert_ne!(ENTITY_SPAWN_MAGIC, ENTITY_DESPAWN_MAGIC);
        let spawn = TaggedEntitySpawn {
            magic: ENTITY_SPAWN_MAGIC,
            update: test_spawn(),
        };
        let despawn = TaggedEntityDespawn {
            magic: ENTITY_DESPAWN_MAGIC,
            update: test_despawn(),
        };
        let spawn_bytes = postcard::to_allocvec(&spawn).expect("spawn encodes");
        let despawn_bytes = postcard::to_allocvec(&despawn).expect("despawn encodes");
        assert_eq!(
            postcard::from_bytes::<TaggedEntitySpawn>(&spawn_bytes),
            Ok(spawn)
        );
        assert_eq!(
            postcard::from_bytes::<TaggedEntityDespawn>(&despawn_bytes),
            Ok(despawn)
        );
        let (spawn_magic, _) =
            postcard::take_from_bytes::<u32>(&spawn_bytes).expect("spawn magic peeks");
        let (despawn_magic, _) =
            postcard::take_from_bytes::<u32>(&despawn_bytes).expect("despawn magic peeks");
        assert_eq!(spawn_magic, ENTITY_SPAWN_MAGIC);
        assert_eq!(despawn_magic, ENTITY_DESPAWN_MAGIC);
    }

    #[test]
    fn reliable_entity_pos_accepts_the_tagged_datagram_envelope() {
        let envelope = EntityPosDatagram {
            magic: ENTITY_POS_MAGIC,
            count: 1,
            tick: TickStamp(12),
            updates: vec![test_pos()],
        };
        let bytes = postcard::to_allocvec(&envelope).expect("envelope encodes");

        // A position envelope is not a bare position row.  Before dispatch
        // was made explicit, this fell through to the latter decoder and
        // repeatedly emitted the unterminated-varint warning seen on node 2.
        assert!(postcard::from_bytes::<EntityPosUpdate>(&bytes).is_err());
        assert_eq!(
            classify_entity_pos_stream(&bytes),
            Ok(EntityPosStreamFrame::Datagram)
        );
        assert!(postcard::from_bytes::<EntityPosDatagram>(&bytes)
            .expect("tagged envelope decodes")
            .is_consistent());
    }

    #[test]
    fn invalid_entity_pos_stream_header_is_rejected_before_payload_decode() {
        assert!(classify_entity_pos_stream(&[0x80; 5]).is_err());
    }

    #[test]
    fn lifecycle_rejects_wrong_magic() {
        let spawn = TaggedEntitySpawn {
            magic: ENTITY_DESPAWN_MAGIC,
            update: test_spawn(),
        };
        let bytes = postcard::to_allocvec(&spawn).expect("spawn encodes");
        let decoded: TaggedEntitySpawn =
            postcard::from_bytes(&bytes).expect("spawn envelope decodes");
        assert_ne!(decoded.magic, ENTITY_SPAWN_MAGIC);
        let despawn = TaggedEntityDespawn {
            magic: ENTITY_SPAWN_MAGIC,
            update: test_despawn(),
        };
        assert_ne!(despawn.magic, ENTITY_DESPAWN_MAGIC);
    }

    #[test]
    fn emit_without_outbox_is_noop() {
        assert_eq!(
            emit_entity_parcel(ServerId(1), StreamKind::EntityPos, &[0x00]),
            0
        );
    }

    #[test]
    fn forward_without_bridge_drops() {
        assert!(!forward_entity_pos_datagram(ServerId(2), vec![test_pos()]));
        assert!(!forward_entity_pos_datagram(ServerId(2), Vec::new()));
    }

    #[test]
    fn stream_kinds_cover_all_families() {
        let kinds = entity_stream_kinds();
        assert_eq!(kinds.len(), 4);
        assert!(kinds.contains(&StreamKind::EntityPos));
        assert!(kinds.contains(&StreamKind::EntityVisual));
        assert!(kinds.contains(&StreamKind::EntityTransient));
        assert!(kinds.contains(&StreamKind::EntityCombat));
    }
}
