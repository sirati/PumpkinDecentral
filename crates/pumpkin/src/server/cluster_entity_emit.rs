use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam::queue::SegQueue;
use pumpkin_cluster::entities::{
    EntityCombatUpdate, EntityDespawn, EntityPosUpdate, EntityRef, EntitySpawn,
    EntityTransientUpdate, EntityVisualUpdate,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkAddr, StreamKind};
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::EntityType;
use pumpkin_util::math::get_section_cord;
use pumpkin_util::math::vector3::Vector3;
use serde::{Deserialize, Serialize};

use super::Server;
use super::cluster_datagram::forward_entity_datagrams;
use super::cluster_entity_apply::{
    ENTITY_DESPAWN_MAGIC, ENTITY_SPAWN_MAGIC, TaggedEntityDespawn, TaggedEntitySpawn,
    emit_entity_parcel,
};
use crate::entity::{Entity, EntityBase};

pub const ENTITY_POS_MAGIC: u32 = 0x454E5459;
pub const ENTITY_POS_PER_DATAGRAM: usize = 16;
const STAGED_EVENT_BOUND: usize = 4096;
const VISUAL_REMEMBER_TICKS: u16 = 2;

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

enum StagedEntityEvent {
    Spawn(EntitySpawn),
    Despawn(EntityDespawn),
    Visual(EntityVisualUpdate),
    Transient(EntityTransientUpdate),
    Combat(EntityCombatUpdate),
}

impl StagedEntityEvent {
    fn owner(&self) -> ServerId {
        match self {
            Self::Spawn(update) => update.entity.owner,
            Self::Despawn(update) => update.entity.owner,
            Self::Visual(update) => update.entity.owner,
            Self::Transient(update) => update.entity.owner,
            Self::Combat(update) => update.entity.owner,
        }
    }

    const fn stream_kind(&self) -> StreamKind {
        match self {
            Self::Spawn(_) | Self::Despawn(_) => EntityPosUpdate::stream_kind(),
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
    static LAST_VISUAL: RefCell<HashMap<i32, ((u8, u16, u8), u16)>> =
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

fn entity_chunk(pos: &Vector3<f64>) -> ChunkAddr {
    ChunkAddr {
        x: get_section_cord(pos.x.floor() as i32),
        z: get_section_cord(pos.z.floor() as i32),
    }
}

fn saturating_entity_kind(entity_type_id: u16) -> u8 {
    entity_type_id.min(u16::from(u8::MAX)) as u8
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

fn entity_equipment(entity: &dyn EntityBase) -> (u8, u16) {
    let Some(living) = entity.get_living_entity() else {
        return (0, 0);
    };
    let Ok(equipment) = living.entity_equipment.try_lock() else {
        return (0, 0);
    };
    for (slot, stack) in equipment.equipment.iter() {
        if stack.is_empty() {
            continue;
        }
        return (
            u8::try_from(slot.discriminant()).unwrap_or(u8::MAX),
            stack.item.id,
        );
    }
    (0, 0)
}

fn entity_visual_snapshot(entity: &dyn EntityBase) -> (u8, u16, u8) {
    let (slot, item) = entity_equipment(entity);
    (slot, item, entity_flags(entity.get_entity()))
}

fn entity_pos_row(owner: ServerId, tick: TickStamp, entity: &dyn EntityBase) -> EntityPosUpdate {
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    let vel = inner.velocity.load();
    EntityPosUpdate {
        entity: EntityRef {
            owner,
            local_id: inner.entity_id,
            chunk: entity_chunk(&pos),
        },
        tick,
        pos: [pos.x, pos.y, pos.z],
        vel: [vel.x, vel.y, vel.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
    }
}

fn visual_is_fresh(entity_id: i32, snapshot: (u8, u16, u8), tick: TickStamp) -> bool {
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

fn sample_entity_visual(owner: ServerId, tick: TickStamp, entity: &dyn EntityBase) {
    let inner = entity.get_entity();
    let snapshot = entity_visual_snapshot(entity);
    if !visual_is_fresh(inner.entity_id, snapshot, tick) {
        return;
    }
    let pos = inner.pos.load();
    stage_event(StagedEntityEvent::Visual(EntityVisualUpdate {
        entity: EntityRef {
            owner,
            local_id: inner.entity_id,
            chunk: entity_chunk(&pos),
        },
        tick,
        slot: snapshot.0,
        item: snapshot.1,
        flags: snapshot.2,
    }));
}

fn sample_entity_rows(server: &Server, owner: ServerId, tick: TickStamp) -> usize {
    let mut sampled = 0_usize;
    for world in server.worlds.load().iter() {
        for entity in world.entities.load().iter() {
            if entity.get_player().is_some() {
                continue;
            }
            let row = entity_pos_row(owner, tick, entity.as_ref());
            POS_WRITE
                .try_with(|write| {
                    if let Ok(mut write) = write.try_borrow_mut() {
                        write.push(row);
                    }
                })
                .ok();
            sample_entity_visual(owner, tick, entity.as_ref());
            sampled = sampled.saturating_add(1);
        }
    }
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
        };
        match encoded {
            Ok(bytes) => {
                emit_entity_parcel(owner, kind, &bytes);
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
                    let mut payloads = Vec::new();
                    for chunk in ready.chunks(ENTITY_POS_PER_DATAGRAM) {
                        let datagram = EntityPosDatagram {
                            magic: ENTITY_POS_MAGIC,
                            count: u16::try_from(chunk.len()).unwrap_or(u16::MAX),
                            tick,
                            updates: chunk.to_vec(),
                        };
                        match postcard::to_allocvec(&datagram) {
                            Ok(bytes) => payloads.push(bytes),
                            Err(_) => note_emit_dropped(),
                        }
                    }
                    ready.clear();
                    if !payloads.is_empty() {
                        forward_entity_datagrams(&payloads);
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

pub fn stage_spawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.get_player().is_some() {
        return;
    }
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    stage_event(StagedEntityEvent::Spawn(EntitySpawn {
        entity: EntityRef {
            owner,
            local_id: inner.entity_id,
            chunk: entity_chunk(&pos),
        },
        tick: cluster_tick(server),
        kind: saturating_entity_kind(inner.entity_type.id),
        pos: [pos.x, pos.y, pos.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
    }));
}

pub fn stage_despawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.get_player().is_some() {
        return;
    }
    let inner = entity.get_entity();
    stage_event(StagedEntityEvent::Despawn(EntityDespawn {
        entity: EntityRef {
            owner,
            local_id: inner.entity_id,
            chunk: entity_chunk(&inner.pos.load()),
        },
        tick: cluster_tick(server),
    }));
}

pub fn stage_transient_if_cluster(server: &Server, entity: &Entity, action: u8) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.entity_type.id == EntityType::PLAYER.id {
        return;
    }
    let pos = entity.pos.load();
    stage_event(StagedEntityEvent::Transient(EntityTransientUpdate {
        entity: EntityRef {
            owner,
            local_id: entity.entity_id,
            chunk: entity_chunk(&pos),
        },
        tick: cluster_tick(server),
        action,
        value: 0,
    }));
}

pub fn stage_combat_if_cluster(server: &Server, entity: &Entity, kind: u8) {
    let Some(owner) = cluster_owner(server) else {
        return;
    };
    if entity.entity_type.id == EntityType::PLAYER.id {
        return;
    }
    let pos = entity.pos.load();
    stage_event(StagedEntityEvent::Combat(EntityCombatUpdate {
        entity: EntityRef {
            owner,
            local_id: entity.entity_id,
            chunk: entity_chunk(&pos),
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
                owner: ServerId(2),
                local_id: 7,
                chunk: ChunkAddr { x: 0, z: 0 },
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

    fn staged_despawn() -> StagedEntityEvent {
        StagedEntityEvent::Despawn(EntityDespawn {
            entity: EntityRef {
                owner: ServerId(9),
                local_id: 3,
                chunk: ChunkAddr { x: 1, z: 1 },
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

    #[test]
    fn entity_kind_saturates_to_u8() {
        assert_eq!(saturating_entity_kind(3), 3);
        assert_eq!(saturating_entity_kind(u16::from(u8::MAX)), u8::MAX);
        assert_eq!(saturating_entity_kind(u16::MAX), u8::MAX);
    }
}
