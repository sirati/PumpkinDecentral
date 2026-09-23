use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::entities::{
    ENTITY_STREAM_KINDS, EntityCombatUpdate, EntityDespawn, EntityGhosts, EntityPosUpdate,
    EntityRef, EntitySpawn, EntityTransientUpdate, EntityVisualState, EntityVisualUpdate,
    route_to_holders,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram};
use crate::entity::EntityBase;
use crate::entity::cluster_entity_replica::RemoteEntityReplica;

type EntityReplicaKey = (ServerId, i32, u32);
type EntityReplicas = HashMap<EntityReplicaKey, Arc<RemoteEntityReplica>>;

fn replica_key(entity: EntityRef) -> EntityReplicaKey {
    (entity.origin, entity.local_id, entity.generation)
}

#[must_use]
pub fn current_entity_owner(server: &Server, entity: GlobalEntityId) -> Option<ServerId> {
    server.cluster_entity_owners.load().get(&entity).copied()
}

pub fn publish_entity_owner(server: &Server, entity: GlobalEntityId, owner: ServerId) {
    server.cluster_entity_owners.rcu(|owners| {
        let mut next = (**owners).clone();
        next.insert(entity, owner);
        next
    });
}

pub fn remove_entity_owner(server: &Server, entity: GlobalEntityId) {
    server.cluster_entity_owners.rcu(|owners| {
        let mut next = (**owners).clone();
        next.remove(&entity);
        next
    });
}

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

fn try_emit_entity(kind: StreamKind, chunk: ChunkKey, bytes: &[u8]) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if outbox.peers.is_empty() {
        return 0;
    }
    let mut sent = 0_u64;
    for peer in super::cluster_datagram::holders_for_chunk(chunk) {
        let parcel = OutboundParcel {
            peer: ServerId(peer),
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

pub fn emit_entity_parcel(owner: ServerId, kind: StreamKind, chunk: ChunkKey, bytes: &[u8]) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if owner != outbox.local {
        note_dropped("owner");
        return 0;
    }
    try_emit_entity(kind, chunk, bytes)
}

pub fn emit_entity_parcel_to(
    owner: ServerId,
    kind: StreamKind,
    peers: &[u16],
    bytes: &[u8],
) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if owner != outbox.local {
        note_dropped("owner");
        return 0;
    }
    let mut sent = 0_u64;
    for peer in peers {
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

fn chunk_holders(server: &Server, local: ServerId, chunk: ChunkKey) -> Vec<u16> {
    let pos = Vector2::new(chunk.x(), chunk.z());
    let loaded = server.worlds.load().iter().any(|world| {
        super::cluster_world_delta::world_id(world) == chunk.world
            && world.level.is_chunk_loaded(&pos)
    });
    if loaded { vec![local.0] } else { Vec::new() }
}

fn holds_chunk(server: &Server, local: ServerId, chunk: ChunkKey) -> bool {
    route_to_holders(chunk, &|addr| chunk_holders(server, local, addr)).contains(&local.0)
}

fn owned_by_sender(peer: ServerId, current_owner: Option<ServerId>, local: ServerId) -> bool {
    let Some(owner) = current_owner else {
        return false;
    };
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
    replicas: &EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityPosUpdate,
) -> bool {
    if !owned_by_sender(
        peer,
        current_entity_owner(server, update.entity.id()),
        local,
    ) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if ghosts.apply_pos(update) {
        if let Some(replica) = replicas.get(&replica_key(update.entity)) {
            let entity = replica.get_entity();
            let position = Vector3::new(update.pos[0], update.pos[1], update.pos[2]);
            entity.set_pos(position);
            entity
                .velocity
                .store(Vector3::new(update.vel[0], update.vel[1], update.vel[2]));
            entity.yaw.store(update.yaw);
            entity.body_yaw.store(update.yaw);
            entity.head_yaw.store(update.yaw);
            entity.pitch.store(update.pitch);
            let world = entity.world.load();
            world
                .entity_tracker
                .update_entity_position(replica.as_ref(), &world);
        }
        note_applied();
        return true;
    }
    note_dropped("unknown");
    false
}

pub fn apply_pos_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &EntityReplicas,
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
    apply_pos_update(server, ghosts, replicas, local, peer, &update)
}

fn insert_spawn(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    update: &EntitySpawn,
) -> bool {
    let id = update.entity.id();
    let current_owner = current_entity_owner(server, id);
    if current_owner.is_some() && !owned_by_sender(peer, current_owner, local) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    ghosts.spawn(update);
    publish_entity_owner(server, id, peer);
    note_applied();
    true
}

fn remove_despawn(
    server: &Server,
    ghosts: &mut EntityGhosts,
    local: ServerId,
    peer: ServerId,
    update: &EntityDespawn,
) -> bool {
    if !owned_by_sender(
        peer,
        current_entity_owner(server, update.entity.id()),
        local,
    ) {
        note_dropped("owner");
        return false;
    }
    if ghosts.despawn(update) {
        remove_entity_owner(server, update.entity.id());
        note_applied();
        return true;
    }
    note_dropped("unknown");
    false
}

pub fn apply_spawn_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &mut EntityReplicas,
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
    if !insert_spawn(server, ghosts, local, peer, &tagged.update) {
        return false;
    }
    let Some(entity_type) = pumpkin_data::entity::EntityType::from_raw(tagged.update.kind) else {
        let _ = ghosts.despawn(&EntityDespawn {
            entity: tagged.update.entity,
            tick: tagged.update.tick,
        });
        note_dropped("entity-type");
        return false;
    };
    let Some(world) = server
        .worlds
        .load()
        .iter()
        .find(|world| {
            world.dimension.id == tagged.update.entity.chunk.world.dimension
                && xxhash_rust::xxh64::xxh64(world.get_world_name().as_bytes(), 0)
                    == tagged.update.entity.chunk.world.key
        })
        .cloned()
    else {
        note_dropped("world");
        return false;
    };
    let key = replica_key(tagged.update.entity);
    if let Some(replica) = replicas.get(&key) {
        if !Arc::ptr_eq(&replica.get_entity().world.load(), &world) {
            note_dropped("world-change");
            return false;
        }
        let entity = replica.get_entity();
        entity.set_pos(Vector3::new(
            tagged.update.pos[0],
            tagged.update.pos[1],
            tagged.update.pos[2],
        ));
        entity.yaw.store(tagged.update.yaw);
        entity.body_yaw.store(tagged.update.yaw);
        entity.head_yaw.store(tagged.update.yaw);
        entity.pitch.store(tagged.update.pitch);
        apply_visual_state(replica.as_ref(), tagged.update.visual, true);
        world
            .entity_tracker
            .update_entity_position(replica.as_ref(), &world);
        return true;
    }
    let stale: Vec<_> = replicas
        .keys()
        .filter(|(owner, local_id, _)| {
            *owner == tagged.update.entity.origin && *local_id == tagged.update.entity.local_id
        })
        .copied()
        .collect();
    for stale_key in stale {
        let Some(previous) = replicas.remove(&stale_key) else {
            continue;
        };
        previous
            .get_entity()
            .world
            .load()
            .remove_cluster_replica(previous.as_ref());
    }
    let replica = RemoteEntityReplica::new(
        Arc::clone(&world),
        tagged.update.entity,
        entity_type,
        Vector3::new(
            tagged.update.pos[0],
            tagged.update.pos[1],
            tagged.update.pos[2],
        ),
    );
    replica.get_entity().yaw.store(tagged.update.yaw);
    replica.get_entity().body_yaw.store(tagged.update.yaw);
    replica.get_entity().head_yaw.store(tagged.update.yaw);
    replica.get_entity().pitch.store(tagged.update.pitch);
    apply_visual_state(replica.as_ref(), tagged.update.visual, false);
    world.add_cluster_replica(Arc::clone(&replica) as Arc<dyn EntityBase>);
    replicas.insert(key, replica);
    true
}

pub fn apply_despawn_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &mut EntityReplicas,
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
    if !remove_despawn(server, ghosts, local, peer, &tagged.update) {
        return false;
    }
    if let Some(replica) = replicas.remove(&replica_key(tagged.update.entity)) {
        replica
            .get_entity()
            .world
            .load()
            .remove_cluster_replica(replica.as_ref());
    }
    true
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
    replicas: &mut EntityReplicas,
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
        EntityPosStreamFrame::Spawn => {
            apply_spawn_bytes(server, ghosts, replicas, local, peer, bytes)
        }
        EntityPosStreamFrame::Despawn => {
            apply_despawn_bytes(server, ghosts, replicas, local, peer, bytes)
        }
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
            apply_pos_bytes(server, ghosts, replicas, local, peer, bytes)
        }
    }
}

pub fn apply_visual_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &EntityReplicas,
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
    if !owned_by_sender(
        peer,
        current_entity_owner(server, update.entity.id()),
        local,
    ) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if !ghosts.apply_visual(&update) {
        note_dropped("unknown");
        return false;
    }
    let Some(replica) = replicas.get(&replica_key(update.entity)) else {
        note_dropped("replica");
        return false;
    };
    apply_visual_state(replica.as_ref(), update.visual, true);
    note_applied();
    true
}

fn apply_visual_state(replica: &RemoteEntityReplica, visual: EntityVisualState, broadcast: bool) {
    let entity = replica.get_entity();
    entity.set_sneaking(visual.flags & 1 != 0);
    entity.set_sprinting(visual.flags & 2 != 0);
    entity.set_swimming(visual.flags & 4 != 0);
    entity.set_invisible(visual.flags & 8 != 0);
    entity.set_glowing(visual.flags & 16 != 0);
    let fall_flying = visual.flags & 32 != 0;
    if entity.is_fall_flying() != fall_flying {
        entity.set_fall_flying(fall_flying);
    }
    let Some(living) = replica.get_living_entity() else {
        return;
    };
    let mut changes = Vec::with_capacity(visual.equipment.len());
    for (index, equipment) in visual.equipment.into_iter().enumerate() {
        let Some(slot) = equipment_slot(u8::try_from(index).unwrap_or(u8::MAX)) else {
            continue;
        };
        let stack = if equipment.count == 0 || equipment.item == 0 {
            ItemStack::EMPTY.clone()
        } else if let Some(item) = Item::from_id(equipment.item) {
            ItemStack::new(equipment.count, item)
        } else {
            note_dropped("item");
            continue;
        };
        changes.push((slot.clone(), stack));
    }
    if let Ok(mut current) = living.entity_equipment.try_lock() {
        for (slot, stack) in &changes {
            current.put(slot, stack.clone());
        }
    } else {
        note_dropped("equipment-busy");
        return;
    }
    if broadcast {
        living.send_equipment_changes(&changes);
    }
}

fn equipment_slot(slot: u8) -> Option<&'static EquipmentSlot> {
    match slot {
        0 => Some(&EquipmentSlot::MAIN_HAND),
        1 => Some(&EquipmentSlot::OFF_HAND),
        2 => Some(&EquipmentSlot::FEET),
        3 => Some(&EquipmentSlot::LEGS),
        4 => Some(&EquipmentSlot::CHEST),
        5 => Some(&EquipmentSlot::HEAD),
        6 => Some(&EquipmentSlot::BODY),
        7 => Some(&EquipmentSlot::SADDLE),
        _ => None,
    }
}

pub fn apply_transient_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &EntityReplicas,
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
    if !owned_by_sender(
        peer,
        current_entity_owner(server, update.entity.id()),
        local,
    ) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if !ghosts.apply_transient(&update) {
        note_dropped("unknown");
        return false;
    }
    let Some(replica) = replicas.get(&replica_key(update.entity)) else {
        note_dropped("replica");
        return false;
    };
    let bedrock = update.bedrock_action.and_then(|action| {
        pumpkin_protocol::bedrock::server::actor_event::ActorEventID::try_from(action).ok()
    });
    replica
        .get_entity()
        .world
        .load()
        .broadcast_cluster_entity_event(replica.get_entity(), update.action as i8, bedrock);
    note_applied();
    true
}

pub fn apply_combat_bytes(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &EntityReplicas,
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
    if !owned_by_sender(
        peer,
        current_entity_owner(server, update.entity.id()),
        local,
    ) {
        note_dropped("owner");
        return false;
    }
    if !holds_chunk(server, local, update.entity.chunk) {
        note_dropped("chunk");
        return false;
    }
    if !ghosts.apply_combat(&update) {
        note_dropped("unknown");
        return false;
    }
    let Some(replica) = replicas.get(&replica_key(update.entity)) else {
        note_dropped("replica");
        return false;
    };
    replica.get_entity().world.load().broadcast_damage_event(
        replica.get_entity(),
        i32::from(update.kind),
        None,
        None,
        None,
    );
    note_applied();
    true
}

fn apply_parcel(
    server: &Server,
    ghosts: &mut EntityGhosts,
    replicas: &mut EntityReplicas,
    local: ServerId,
    parcel: &InboundParcel,
) {
    match parcel.header.kind {
        StreamKind::EntityPos => {
            apply_pos_stream_bytes(server, ghosts, replicas, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityVisual => {
            apply_visual_bytes(server, ghosts, replicas, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityTransient => {
            apply_transient_bytes(server, ghosts, replicas, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityCombat => {
            apply_combat_bytes(server, ghosts, replicas, local, parcel.peer, &parcel.bytes);
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
        let mut replicas = EntityReplicas::new();
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
                    apply_parcel(&task_server, &mut ghosts, &mut replicas, local, &parcel);
                }
                batch = pos_rx.recv() => {
                    let Some((peer, updates)) = batch else { break };
                    ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                    for update in &updates {
                        apply_pos_update(
                            &task_server,
                            &mut ghosts,
                            &replicas,
                            local,
                            peer,
                            update,
                        );
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
            origin: ServerId(owner),
            local_id,
            generation: 0,
            chunk: ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 7), 0, 0),
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
            visual: EntityVisualState::default(),
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
        assert!(
            postcard::from_bytes::<EntityPosDatagram>(&bytes)
                .expect("tagged envelope decodes")
                .is_consistent()
        );
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
            emit_entity_parcel(
                ServerId(1),
                StreamKind::EntityPos,
                ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 7), 0, 0),
                &[0x00],
            ),
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
