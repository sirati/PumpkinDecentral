use std::cell::RefCell;
use std::collections::HashMap;
#[cfg(feature = "cluster-e2e")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam::queue::SegQueue;
use pumpkin_cluster::entities::{
    EntityCombatUpdate, EntityDespawn, EntityEquipmentState, EntityPosUpdate, EntityRef,
    EntitySpawn, EntityTransientUpdate, EntityVisualState, EntityVisualUpdate, holder_transition,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkKey, StreamKind, WorldId};
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::EntityType;
use pumpkin_util::math::get_section_cord;
use pumpkin_util::math::vector3::Vector3;
use serde::{Deserialize, Serialize};

use super::Server;
use super::cluster_datagram::{
    forward_entity_datagram_to, holders_for_chunk, peer_datagram_payload_limit,
};
use super::cluster_entity_apply::{
    ENTITY_DESPAWN_MAGIC, ENTITY_SPAWN_MAGIC, TaggedEntityDespawn, TaggedEntitySpawn,
    emit_entity_parcel, emit_entity_parcel_to,
};
use crate::entity::cluster_entity_replica::RemoteEntityReplica;
use crate::entity::cluster_player_ghost::RemotePlayerReplica;
use crate::entity::{Entity, EntityBase};

pub const ENTITY_POS_MAGIC: u32 = 0x454E5459;
pub const ENTITY_POS_PER_DATAGRAM: usize = 16;
const STAGED_EVENT_BOUND: usize = 4096;
const VISUAL_REMEMBER_TICKS: u16 = 2;
#[cfg(feature = "cluster-e2e")]
static NATURAL_SPAWN_RECORDED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityPosDatagram {
    pub magic: u32,
    pub count: u16,
    pub tick: TickStamp,
    pub updates: Vec<EntityPosUpdate>,
}

impl EntityPosDatagram {
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        self.magic == ENTITY_POS_MAGIC && usize::from(self.count) == self.updates.len()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}

#[derive(Serialize)]
struct BorrowedEntityPosDatagram<'a> {
    magic: u32,
    count: u16,
    tick: TickStamp,
    updates: &'a [EntityPosUpdate],
}

fn encode_entity_pos_payloads(
    tick: TickStamp,
    updates: &[EntityPosUpdate],
    max_payload: usize,
) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    let mut start = 0;
    while start < updates.len() {
        let mut buffer = [0_u8; super::cluster_datagram::MAX_DATAGRAM_BYTES];
        let mut accepted = None;
        for end in start + 1..=updates.len().min(start + ENTITY_POS_PER_DATAGRAM) {
            let packet = BorrowedEntityPosDatagram {
                magic: ENTITY_POS_MAGIC,
                count: u16::try_from(end - start).unwrap_or(u16::MAX),
                tick,
                updates: &updates[start..end],
            };
            let Ok(bytes) = postcard::to_slice(&packet, &mut buffer) else {
                break;
            };
            if bytes.len() > max_payload {
                break;
            }
            accepted = Some((end, bytes.to_vec()));
        }
        if let Some((end, bytes)) = accepted {
            payloads.push(bytes);
            start = end;
        } else {
            note_emit_dropped();
            start += 1;
        }
    }
    payloads
}

enum StagedEntityEvent {
    Spawn(EntitySpawn),
    Despawn(EntityDespawn),
    Visual(EntityVisualUpdate),
    Transient(EntityTransientUpdate),
    Combat(EntityCombatUpdate),
    BoundarySpawn(EntitySpawn, Vec<u16>),
    BoundaryDespawn(EntityDespawn, Vec<u16>),
}

impl StagedEntityEvent {
    fn owner(&self) -> ServerId {
        match self {
            Self::Spawn(update) => update.entity.origin,
            Self::Despawn(update) => update.entity.origin,
            Self::Visual(update) => update.entity.origin,
            Self::Transient(update) => update.entity.origin,
            Self::Combat(update) => update.entity.origin,
            Self::BoundarySpawn(update, _) => update.entity.origin,
            Self::BoundaryDespawn(update, _) => update.entity.origin,
        }
    }

    fn chunk(&self) -> ChunkKey {
        match self {
            Self::Spawn(update) => update.entity.chunk,
            Self::Despawn(update) => update.entity.chunk,
            Self::Visual(update) => update.entity.chunk,
            Self::Transient(update) => update.entity.chunk,
            Self::Combat(update) => update.entity.chunk,
            Self::BoundarySpawn(update, _) => update.entity.chunk,
            Self::BoundaryDespawn(update, _) => update.entity.chunk,
        }
    }

    const fn stream_kind(&self) -> StreamKind {
        match self {
            Self::Spawn(_)
            | Self::Despawn(_)
            | Self::BoundarySpawn(_, _)
            | Self::BoundaryDespawn(_, _) => EntityPosUpdate::stream_kind(),
            Self::Visual(_) => EntityVisualUpdate::stream_kind(),
            Self::Transient(_) => EntityTransientUpdate::stream_kind(),
            Self::Combat(_) => EntityCombatUpdate::stream_kind(),
        }
    }
}

static STAGED_EVENTS: SegQueue<StagedEntityEvent> = SegQueue::new();
static STAGED_LEN: AtomicUsize = AtomicUsize::new(0);
static ENTITY_EMIT_DROPPED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static POS_WRITE: RefCell<Vec<EntityPosUpdate>> = const { RefCell::new(Vec::new()) };
    static POS_READY: RefCell<Vec<EntityPosUpdate>> = const { RefCell::new(Vec::new()) };
    static LAST_VISUAL: RefCell<HashMap<i32, (EntityVisualState, u16)>> =
        RefCell::new(HashMap::new());
    static LAST_ENTITY_CHUNKS: RefCell<HashMap<(i32, u64), ChunkKey>> =
        RefCell::new(HashMap::new());
}

#[must_use]
pub fn entity_emit_dropped() -> u64 {
    ENTITY_EMIT_DROPPED.load(Ordering::Relaxed)
}

#[must_use]
pub fn staged_event_len() -> usize {
    STAGED_LEN.load(Ordering::Relaxed)
}

fn note_emit_dropped() {
    ENTITY_EMIT_DROPPED.fetch_add(1, Ordering::Relaxed);
}

fn stage_event(event: StagedEntityEvent) {
    if STAGED_LEN.fetch_add(1, Ordering::Relaxed) >= STAGED_EVENT_BOUND {
        STAGED_LEN.fetch_sub(1, Ordering::Relaxed);
        note_emit_dropped();
        return;
    }
    STAGED_EVENTS.push(event);
}

fn entity_world_id(world: &crate::world::World) -> WorldId {
    WorldId::new(
        world.dimension.id,
        xxhash_rust::xxh64::xxh64(world.get_world_name().as_bytes(), 0),
    )
}

fn entity_chunk(world: &crate::world::World, pos: &Vector3<f64>) -> ChunkKey {
    ChunkKey::new(
        entity_world_id(world),
        get_section_cord(pos.x.floor() as i32),
        get_section_cord(pos.z.floor() as i32),
    )
}

fn entity_flags(entity: &Entity) -> u8 {
    let mut flags = 0_u8;
    if entity.sneaking.load(Ordering::Relaxed) {
        flags |= 1;
    }
    if entity.sprinting.load(Ordering::Relaxed) {
        flags |= 2;
    }
    if entity.swimming.load(Ordering::Relaxed) {
        flags |= 4;
    }
    if entity.invisible.load(Ordering::Relaxed) {
        flags |= 8;
    }
    if entity.glowing.load(Ordering::Relaxed) {
        flags |= 16;
    }
    if entity.fall_flying.load(Ordering::Relaxed) {
        flags |= 32;
    }
    flags
}

fn entity_equipment(entity: &dyn EntityBase) -> [EntityEquipmentState; 8] {
    let mut snapshot = [EntityEquipmentState::default(); 8];
    let Some(living) = entity.get_living_entity() else {
        return snapshot;
    };
    let Ok(equipment) = living.entity_equipment.try_lock() else {
        return snapshot;
    };
    for (slot, stack) in equipment.equipment.iter() {
        let index = usize::try_from(slot.discriminant()).unwrap_or(usize::MAX);
        if let Some(target) = snapshot.get_mut(index) {
            *target = EntityEquipmentState {
                item: stack.item.id,
                count: stack.item_count,
            };
        }
    }
    snapshot
}

fn entity_visual_snapshot(entity: &dyn EntityBase) -> EntityVisualState {
    EntityVisualState {
        equipment: entity_equipment(entity),
        flags: entity_flags(entity.get_entity()),
    }
}

fn entity_pos_row(
    owner: ServerId,
    tick: TickStamp,
    world: &crate::world::World,
    entity: &dyn EntityBase,
) -> EntityPosUpdate {
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    let vel = inner.velocity.load();
    EntityPosUpdate {
        entity: EntityRef {
            origin: owner,
            local_id: inner.entity_id,
            generation: inner.cluster_generation(),
            chunk: entity_chunk(world, &pos),
        },
        tick,
        pos: [pos.x, pos.y, pos.z],
        vel: [vel.x, vel.y, vel.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
    }
}

fn visual_is_fresh(entity_id: i32, snapshot: EntityVisualState, tick: TickStamp) -> bool {
    LAST_VISUAL
        .try_with(|last| {
            if let Ok(mut last) = last.try_borrow_mut() {
                let previous = last.insert(entity_id, (snapshot, tick.0));
                previous.is_none_or(|(seen, _)| seen != snapshot)
            } else {
                false
            }
        })
        .unwrap_or(false)
}

fn prune_visual_memory(tick: TickStamp) {
    LAST_VISUAL
        .try_with(|last| {
            if let Ok(mut last) = last.try_borrow_mut() {
                last.retain(|_, (_, seen)| tick.0.wrapping_sub(*seen) <= VISUAL_REMEMBER_TICKS);
            }
        })
        .ok();
}

fn sample_entity_visual(
    owner: ServerId,
    tick: TickStamp,
    world: &crate::world::World,
    entity: &dyn EntityBase,
    visual: EntityVisualState,
) {
    let inner = entity.get_entity();
    if !visual_is_fresh(inner.entity_id, visual, tick) {
        return;
    }
    let pos = inner.pos.load();
    stage_event(StagedEntityEvent::Visual(EntityVisualUpdate {
        entity: EntityRef {
            origin: owner,
            local_id: inner.entity_id,
            generation: inner.cluster_generation(),
            chunk: entity_chunk(world, &pos),
        },
        tick,
        visual,
    }));
}

fn sample_entity_rows(server: &Server, owner: ServerId, tick: TickStamp) -> usize {
    let mut sampled = 0_usize;
    let mut seen = std::collections::HashSet::new();
    for world in server.worlds.load().iter() {
        for entity in world.entities.load().iter() {
            if entity.get_player().is_some()
                || entity.cast_any().is::<RemotePlayerReplica>()
                || entity.cast_any().is::<RemoteEntityReplica>()
            {
                continue;
            }
            let row = entity_pos_row(owner, tick, world, entity.as_ref());
            if super::cluster_entity_apply::current_entity_owner(server, row.entity.id())
                != Some(owner)
            {
                super::cluster_entity_apply::publish_entity_owner(server, row.entity.id(), owner);
            }
            let visual = entity_visual_snapshot(entity.as_ref());
            let identity = (row.entity.local_id, row.entity.generation);
            seen.insert(identity);
            LAST_ENTITY_CHUNKS
                .try_with(|chunks| {
                    let Ok(mut chunks) = chunks.try_borrow_mut() else {
                        return;
                    };
                    let previous = chunks.insert(identity, row.entity.chunk);
                    if let Some(previous) = previous
                        && previous != row.entity.chunk
                    {
                        let (leave, enter) = holder_transition(
                            &holders_for_chunk(previous),
                            &holders_for_chunk(row.entity.chunk),
                        );
                        if !leave.is_empty() {
                            stage_event(StagedEntityEvent::BoundaryDespawn(
                                EntityDespawn {
                                    entity: EntityRef {
                                        chunk: previous,
                                        ..row.entity
                                    },
                                    tick,
                                },
                                leave,
                            ));
                        }
                        if !enter.is_empty() {
                            let inner = entity.get_entity();
                            stage_event(StagedEntityEvent::BoundarySpawn(
                                EntitySpawn {
                                    entity: row.entity,
                                    tick,
                                    kind: inner.entity_type.id,
                                    pos: row.pos,
                                    yaw: row.yaw,
                                    pitch: row.pitch,
                                    visual,
                                },
                                enter,
                            ));
                        }
                    }
                    if previous.is_none() {
                        let inner = entity.get_entity();
                        stage_event(StagedEntityEvent::Spawn(EntitySpawn {
                            entity: row.entity,
                            tick,
                            kind: inner.entity_type.id,
                            pos: row.pos,
                            yaw: row.yaw,
                            pitch: row.pitch,
                            visual,
                        }));
                    }
                })
                .ok();
            POS_WRITE
                .try_with(|write| {
                    if let Ok(mut write) = write.try_borrow_mut() {
                        write.push(row);
                    }
                })
                .ok();
            sample_entity_visual(owner, tick, world, entity.as_ref(), visual);
            sampled = sampled.saturating_add(1);
        }
    }
    LAST_ENTITY_CHUNKS
        .try_with(|chunks| {
            if let Ok(mut chunks) = chunks.try_borrow_mut() {
                chunks.retain(|identity, _| seen.contains(identity));
            }
        })
        .ok();
    prune_visual_memory(tick);
    sampled
}

fn flush_staged_events() {
    let mut drained = 0_usize;
    while drained < STAGED_EVENT_BOUND {
        let Some(event) = STAGED_EVENTS.pop() else {
            break;
        };
        STAGED_LEN.fetch_sub(1, Ordering::Relaxed);
        drained = drained.saturating_add(1);
        let owner = event.owner();
        let kind = event.stream_kind();
        let chunk = event.chunk();
        let targets = match &event {
            StagedEntityEvent::BoundarySpawn(_, targets)
            | StagedEntityEvent::BoundaryDespawn(_, targets) => Some(targets.clone()),
            _ => None,
        };
        let encoded = match event {
            StagedEntityEvent::Spawn(update) => postcard::to_allocvec(&TaggedEntitySpawn {
                magic: ENTITY_SPAWN_MAGIC,
                update,
            }),
            StagedEntityEvent::Despawn(update) => postcard::to_allocvec(&TaggedEntityDespawn {
                magic: ENTITY_DESPAWN_MAGIC,
                update,
            }),
            StagedEntityEvent::Visual(update) => postcard::to_allocvec(&update),
            StagedEntityEvent::Transient(update) => postcard::to_allocvec(&update),
            StagedEntityEvent::Combat(update) => postcard::to_allocvec(&update),
            StagedEntityEvent::BoundarySpawn(update, _) => {
                postcard::to_allocvec(&TaggedEntitySpawn {
                    magic: ENTITY_SPAWN_MAGIC,
                    update,
                })
            }
            StagedEntityEvent::BoundaryDespawn(update, _) => {
                postcard::to_allocvec(&TaggedEntityDespawn {
                    magic: ENTITY_DESPAWN_MAGIC,
                    update,
                })
            }
        };
        match encoded {
            Ok(bytes) => {
                if let Some(targets) = targets {
                    emit_entity_parcel_to(owner, kind, &targets, &bytes);
                } else {
                    emit_entity_parcel(owner, kind, chunk, &bytes);
                }
            }
            Err(_) => note_emit_dropped(),
        }
    }
}

fn fuse_entity_pos(tick: TickStamp) {
    POS_WRITE
        .try_with(|write| {
            POS_READY
                .try_with(|ready| {
                    let (Ok(mut write), Ok(mut ready)) =
                        (write.try_borrow_mut(), ready.try_borrow_mut())
                    else {
                        return;
                    };
                    std::mem::swap(&mut *write, &mut *ready);
                    if ready.is_empty() {
                        return;
                    }
                    let mut by_peer: std::collections::BTreeMap<u16, Vec<EntityPosUpdate>> =
                        std::collections::BTreeMap::new();
                    for update in ready.iter() {
                        for peer in holders_for_chunk(update.entity.chunk) {
                            by_peer.entry(peer).or_default().push(*update);
                        }
                    }
                    ready.clear();
                    for (peer, updates) in by_peer {
                        let peer = ServerId(peer);
                        let max_payload = peer_datagram_payload_limit(peer);
                        for bytes in encode_entity_pos_payloads(tick, &updates, max_payload) {
                            forward_entity_datagram_to(peer, bytes);
                        }
                    }
                })
                .ok();
        })
        .ok();
}

fn cluster_owner(server: &Server) -> Option<ServerId> {
    if !server.advanced_config.cluster.enabled {
        return None;
    }
    Some(ServerId(server.advanced_config.cluster.server_id))
}

fn cluster_tick(_server: &Server) -> TickStamp {
    TickStamp::now()
}

fn is_remote_replica(server: &Server, entity: &Entity) -> bool {
    server.worlds.load().iter().any(|world| {
        world.entities.load().iter().any(|candidate| {
            candidate.cast_any().is::<RemoteEntityReplica>()
                && std::ptr::eq(candidate.get_entity(), entity)
        })
    })
}

pub fn actor_for_entity_id(
    server: &Server,
    world: &crate::world::World,
    entity_id: i32,
) -> Option<pumpkin_cluster::actor::ActorId> {
    let entities = world.entities.load();
    let entity = entities
        .iter()
        .find(|candidate| candidate.get_entity().entity_id == entity_id)?;
    if let Some(replica) = entity.cast_any().downcast_ref::<RemoteEntityReplica>() {
        return Some(pumpkin_cluster::actor::ActorId::Entity(
            replica.cluster_ref.id(),
        ));
    }
    let inner = entity.get_entity();
    let position = inner.pos.load();
    Some(pumpkin_cluster::actor::ActorId::Entity(
        EntityRef {
            origin: ServerId(server.advanced_config.cluster.server_id),
            local_id: inner.entity_id,
            generation: inner.cluster_generation(),
            chunk: entity_chunk(world, &position),
        }
        .id(),
    ))
}

pub fn entity_id_for_global(
    server: &Server,
    world: &crate::world::World,
    entity: pumpkin_cluster::protocol::GlobalEntityId,
) -> Option<i32> {
    world.entities.load().iter().find_map(|candidate| {
        let candidate_id =
            if let Some(replica) = candidate.cast_any().downcast_ref::<RemoteEntityReplica>() {
                replica.cluster_ref.id()
            } else {
                let inner = candidate.get_entity();
                pumpkin_cluster::protocol::GlobalEntityId {
                    origin: ServerId(server.advanced_config.cluster.server_id),
                    local_id: inner.entity_id,
                    generation: inner.cluster_generation(),
                }
            };
        (candidate_id == entity).then(|| candidate.get_entity().entity_id)
    })
}

pub fn stage_spawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.get_player().is_some() || is_remote_replica(server, entity.get_entity()) {
        return;
    }
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    let world = inner.world.load();
    let spawn = EntitySpawn {
        entity: EntityRef {
            origin: owner,
            local_id: inner.entity_id,
            generation: inner.cluster_generation(),
            chunk: entity_chunk(&world, &pos),
        },
        tick: cluster_tick(server),
        kind: inner.entity_type.id,
        pos: [pos.x, pos.y, pos.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
        visual: entity_visual_snapshot(entity),
    };
    super::cluster_entity_apply::publish_entity_owner(server, spawn.entity.id(), owner);
    stage_event(StagedEntityEvent::Spawn(spawn));
}

#[cfg(feature = "cluster-e2e")]
pub fn record_natural_spawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if !matches!(
        server.advanced_config.cluster.role,
        pumpkin_config::ClusterRole::Primary
    ) || NATURAL_SPAWN_RECORDED.swap(true, Ordering::Relaxed)
    {
        return;
    }
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    let world = inner.world.load();
    let chunk = entity_chunk(&world, &pos);
    tracing::info!(
        target: "cluster_e2e",
        event = "primary_natural_entity_spawn",
        owner = owner.0,
        local_id = inner.entity_id,
        generation = inner.cluster_generation(),
        dimension = chunk.world.dimension,
        world = chunk.world.key,
        chunk_x = chunk.x(),
        chunk_z = chunk.z(),
        uuid = %inner.entity_uuid,
        entity_type = inner.entity_type.id,
    );
}

pub fn stage_despawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.get_player().is_some() || is_remote_replica(server, entity.get_entity()) {
        return;
    }
    let inner = entity.get_entity();
    let world = inner.world.load();
    let despawn = EntityDespawn {
        entity: EntityRef {
            origin: owner,
            local_id: inner.entity_id,
            generation: inner.cluster_generation(),
            chunk: entity_chunk(&world, &inner.pos.load()),
        },
        tick: cluster_tick(server),
    };
    stage_event(StagedEntityEvent::Despawn(despawn));
    super::cluster_entity_apply::remove_entity_owner(server, despawn.entity.id());
}

pub fn stage_transient_if_cluster(
    server: &Server,
    entity: &Entity,
    action: u8,
    bedrock_action: Option<u8>,
) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.entity_type.id == EntityType::PLAYER.id || is_remote_replica(server, entity) {
        return;
    }
    let pos = entity.pos.load();
    let world = entity.world.load();
    stage_event(StagedEntityEvent::Transient(EntityTransientUpdate {
        entity: EntityRef {
            origin: owner,
            local_id: entity.entity_id,
            generation: entity.cluster_generation(),
            chunk: entity_chunk(&world, &pos),
        },
        tick: cluster_tick(server),
        action,
        bedrock_action,
        value: 0,
    }));
}

pub fn stage_combat_if_cluster(server: &Server, entity: &Entity, kind: u8) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.entity_type.id == EntityType::PLAYER.id || is_remote_replica(server, entity) {
        return;
    }
    let pos = entity.pos.load();
    let world = entity.world.load();
    stage_event(StagedEntityEvent::Combat(EntityCombatUpdate {
        entity: EntityRef {
            origin: owner,
            local_id: entity.entity_id,
            generation: entity.cluster_generation(),
            chunk: entity_chunk(&world, &pos),
        },
        tick: cluster_tick(server),
        kind,
        amount: 0,
    }));
}

#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub fn sample_entities_tick(server: &Server) -> usize {
    if !server.advanced_config.cluster.enabled {
        return 0;
    }
    let owner = ServerId(server.advanced_config.cluster.server_id);
    let tick = TickStamp::now();
    let sampled = sample_entity_rows(server, owner, tick);
    flush_staged_events();
    fuse_entity_pos(tick);
    sampled
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::EntityRef;

    fn test_row() -> EntityPosUpdate {
        EntityPosUpdate {
            entity: EntityRef {
                origin: ServerId(2),
                local_id: 7,
                generation: 0,
                chunk: ChunkKey::new(WorldId::new(0, 7), 0, 0),
            },
            tick: TickStamp(12),
            pos: [4.0, 5.0, 6.0],
            vel: [0.1, 0.0, 0.0],
            yaw: 180.0,
            pitch: 5.0,
        }
    }

    fn drain_staged() {
        while STAGED_EVENTS.pop().is_some() {
            STAGED_LEN.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn entity_pos_datagram_roundtrips_with_magic() {
        let datagram = EntityPosDatagram {
            magic: ENTITY_POS_MAGIC,
            count: 1,
            tick: TickStamp(12),
            updates: vec![test_row()],
        };
        let bytes = postcard::to_allocvec(&datagram).expect("entity datagram encodes");
        let decoded: EntityPosDatagram =
            postcard::from_bytes(&bytes).expect("entity datagram decodes");
        assert!(decoded.is_consistent());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.updates[0], test_row());
        let (magic, _) =
            postcard::take_from_bytes::<u32>(&bytes).expect("magic peeks without full decode");
        assert_eq!(magic, ENTITY_POS_MAGIC);
    }

    #[test]
    fn entity_movement_splits_at_encoded_peer_budget_without_losing_rows() {
        let updates: Vec<_> = (0..9)
            .map(|id| {
                let mut row = test_row();
                row.entity.local_id = id;
                row
            })
            .collect();
        let one = postcard::to_allocvec(&EntityPosDatagram {
            magic: ENTITY_POS_MAGIC,
            count: 1,
            tick: TickStamp(12),
            updates: vec![updates[0]],
        })
        .unwrap();
        let packets = encode_entity_pos_payloads(TickStamp(12), &updates, one.len() + 1);
        assert!(packets.len() > 1);
        assert!(packets.iter().all(|packet| packet.len() <= one.len() + 1));
        let decoded: Vec<_> = packets
            .iter()
            .map(|packet| postcard::from_bytes::<EntityPosDatagram>(packet).unwrap())
            .flat_map(|packet| packet.updates)
            .collect();
        assert_eq!(decoded, updates);
    }

    fn staged_despawn() -> StagedEntityEvent {
        StagedEntityEvent::Despawn(EntityDespawn {
            entity: EntityRef {
                origin: ServerId(9),
                local_id: 3,
                generation: 0,
                chunk: ChunkKey::new(WorldId::new(0, 7), 1, 1),
            },
            tick: TickStamp(4),
        })
    }

    #[test]
    fn staged_event_queue_bounds_and_flushes() {
        drain_staged();
        let before = entity_emit_dropped();
        for _ in 0..(STAGED_EVENT_BOUND + 8) {
            stage_event(staged_despawn());
        }
        assert!(entity_emit_dropped() > before);
        assert_eq!(staged_event_len(), STAGED_EVENT_BOUND);
        flush_staged_events();
        assert_eq!(staged_event_len(), 0);
    }
}
