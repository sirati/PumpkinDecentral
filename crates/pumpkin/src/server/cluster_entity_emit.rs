use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::entities::{
    EntityCombatUpdate, EntityDespawn, EntityPosUpdate, EntityRef, EntitySpawn, EntitySpawnState,
    EntityTransientUpdate, EntityVisualUpdate, ItemStackState, OwnerTable, PlayerEntityState,
    PlayerSpawnState, fanout_to_holders, route_to_holders,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{ChunkAddr, PlayerGameMode, StreamKind};
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::EntityType;
use pumpkin_util::math::get_section_cord;
use pumpkin_util::math::vector3::Vector3;
use serde::{Deserialize, Serialize};
use pumpkin_nbt::{Nbt, NbtCompound};

use super::Server;
use super::cluster_datagram::forward_entity_datagrams;
use super::cluster_entity_apply::{
    ENTITY_DESPAWN_MAGIC, ENTITY_SPAWN_MAGIC, TaggedEntityDespawn, TaggedEntitySpawn,
    emit_entity_parcel,
};
use crate::entity::{Entity, EntityBase, player::Player};

pub const ENTITY_POS_MAGIC: u32 = 0x454E5459;
pub const ENTITY_POS_PER_DATAGRAM: usize = 16;
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

static ENTITY_EMIT_DROPPED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static POS_WRITE: RefCell<Vec<EntityPosUpdate>> = const { RefCell::new(Vec::new()) };
    static POS_READY: RefCell<Vec<EntityPosUpdate>> = const { RefCell::new(Vec::new()) };
    static LAST_VISUAL: RefCell<HashMap<(ServerId, i32), ((u8, u16, u8), u16)>> =
        RefCell::new(HashMap::new());
    static LAST_HOLDERS: RefCell<HashMap<(ServerId, i32), Vec<u16>>> = RefCell::new(HashMap::new());
}

#[must_use]
pub fn entity_emit_dropped() -> u64 {
    ENTITY_EMIT_DROPPED.load(Ordering::Relaxed)
}

#[must_use]
pub fn staged_event_len() -> usize {
    0
}

fn note_emit_dropped() {
    ENTITY_EMIT_DROPPED.fetch_add(1, Ordering::Relaxed);
}

fn stage_event(event: StagedEntityEvent) {
    emit_event(event);
}

fn entity_chunk(pos: &Vector3<f64>) -> ChunkAddr {
    ChunkAddr {
        x: get_section_cord(pos.x.floor() as i32),
        z: get_section_cord(pos.z.floor() as i32),
    }
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
    living.cluster_equipment_snapshot()
}

fn entity_visual_snapshot(entity: &dyn EntityBase) -> (u8, u16, u8) {
    let (slot, item) = entity_equipment(entity);
    (slot, item, entity_flags(entity.get_entity()))
}

pub(crate) fn nbt_bytes(mut nbt: NbtCompound) -> Vec<u8> {
    Nbt::from(std::mem::take(&mut nbt)).write_unnamed().to_vec()
}

fn spawn_state(entity: &dyn EntityBase) -> Option<EntitySpawnState> {
    if let Some(player) = entity.get_player() {
        return player_spawn_state(player).map(EntitySpawnState::Player);
    }
    if let Some(item) = entity.get_item_entity() {
        let stack = item.cluster_stack_snapshot();
        let mut entity_nbt = NbtCompound::new();
        item.get_entity().write_nbt(&mut entity_nbt);
        return Some(EntitySpawnState::ItemDrop {
            stack: ItemStackState {
                item_id: stack.item,
                count: stack.count,
                nbt: stack.nbt,
            },
            entity_nbt: nbt_bytes(entity_nbt),
        });
    }
    let mut entity_nbt = NbtCompound::new();
    entity.write_nbt(&mut entity_nbt);
    Some(EntitySpawnState::Entity {
        nbt: nbt_bytes(entity_nbt),
    })
}

fn player_gamemode(gamemode: pumpkin_util::GameMode) -> PlayerGameMode {
    match gamemode {
        pumpkin_util::GameMode::Survival => PlayerGameMode::Survival,
        pumpkin_util::GameMode::Creative => PlayerGameMode::Creative,
        pumpkin_util::GameMode::Adventure => PlayerGameMode::Adventure,
        pumpkin_util::GameMode::Spectator => PlayerGameMode::Spectator,
    }
}

fn player_spawn_state(player: &Player) -> Option<PlayerSpawnState> {
    let gid = player.cluster_gid()?;
    let entity = player.get_entity();
    let world = entity.world.load_full();
    let mut nbt = NbtCompound::new();
    player.write_nbt(&mut nbt);
    Some(PlayerSpawnState {
        gid,
        uuid: player.gameprofile.id.into_bytes(),
        name: player.gameprofile.name.clone(),
        properties: super::cluster_presence::presence_properties_for(player),
        gamemode: player_gamemode(player.gamemode.load()),
        source_reserved_entity_id: player.entity_id(),
        world: world.get_world_name().to_string(),
        dimension: world.dimension.minecraft_name.to_string(),
        entity: PlayerEntityState {
            velocity: {
                let velocity = entity.velocity.load();
                [velocity.x, velocity.y, velocity.z]
            },
            on_ground: entity.on_ground.load(Ordering::Relaxed),
            flags: entity_flags(entity),
            fire_ticks: entity.fire_ticks.load(Ordering::Relaxed),
            health_milli: (player.living_entity.health.load().max(0.0) * 1000.0)
                .min(f32::from(u16::MAX)) as u16,
            absorption_milli: (player.living_entity.absorption.load().max(0.0) * 1000.0)
                .min(f32::from(u16::MAX)) as u16,
            fall_distance_milli: (player.living_entity.fall_distance.load() * 1000.0) as i32,
            food: player.get_food_level(),
            saturation_milli: (player.get_saturation().max(0.0) * 1000.0)
                .min(f32::from(u16::MAX)) as u16,
            experience_level: player.experience_level.load(Ordering::Relaxed),
            experience_progress_milli: (player.experience_progress.load().max(0.0) * 1000.0)
                .min(f32::from(u16::MAX)) as u16,
            experience_points: player.experience_points.load(Ordering::Relaxed),
            entity_nbt: nbt_bytes(nbt),
        },
    })
}

fn entity_pos_row(reference: EntityRef, tick: TickStamp, entity: &dyn EntityBase) -> EntityPosUpdate {
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    let vel = inner.velocity.load();
    EntityPosUpdate {
        entity: EntityRef { chunk: entity_chunk(&pos), ..reference },
        tick,
        pos: [pos.x, pos.y, pos.z],
        vel: [vel.x, vel.y, vel.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
    }
}

fn visual_is_fresh(identity: (ServerId, i32), snapshot: (u8, u16, u8), tick: TickStamp) -> bool {
    LAST_VISUAL
        .try_with(|last| {
            if let Ok(mut last) = last.try_borrow_mut() {
                let previous = last.insert(identity, (snapshot, tick.0));
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

fn sample_entity_visual(reference: EntityRef, tick: TickStamp, entity: &dyn EntityBase) {
    let inner = entity.get_entity();
    let snapshot = entity_visual_snapshot(entity);
    if !visual_is_fresh((reference.origin, reference.local_id), snapshot, tick) {
        return;
    }
    let pos = inner.pos.load();
    stage_event(StagedEntityEvent::Visual(EntityVisualUpdate {
        entity: EntityRef { chunk: entity_chunk(&pos), ..reference },
        tick,
        slot: snapshot.0,
        item: snapshot.1,
        flags: snapshot.2,
    }));
}

fn sample_entity_rows(server: &Server, owner: ServerId, tick: TickStamp) -> usize {
    let mut sampled = 0_usize;
    let mut owners = OwnerTable::new(owner);
    for world in server.worlds.load().iter() {
        for entity in world.entities.load().iter() {
            if entity.get_player().is_some() {
                continue;
            }
            let Some(reference) = local_entity_ref(server, entity.as_ref()) else {
                continue;
            };
            if super::cluster_entity_boundary::is_frozen(reference) {
                continue;
            }
            owners.insert_origin(reference.origin, reference.local_id, reference.chunk);
            let _ = owners.note_origin_moved(reference.origin, reference.local_id, reference.chunk);
            let row = entity_pos_row(reference, tick, entity.as_ref());
            let _ = super::cluster_entity_apply::sync_owned_entity_infallible(row);
            POS_WRITE
                .try_with(|write| {
                    if let Ok(mut write) = write.try_borrow_mut() {
                        write.push(row);
                    }
                })
                .ok();
            sample_entity_visual(reference, tick, entity.as_ref());
            let holders = route_to_holders(reference.chunk, &super::cluster::chunk_holders);
            let changed = LAST_HOLDERS
                .try_with(|known| {
                    let Ok(mut known) = known.try_borrow_mut() else {
                        return false;
                    };
                    let identity = (reference.origin, reference.local_id);
                    let previous = known.insert(identity, holders);
                    previous.as_ref() != known.get(&identity)
                })
                .unwrap_or(false);
            if changed {
                let inner = entity.get_entity();
                let pos = inner.pos.load();
                let Some(state) = spawn_state(entity.as_ref()) else {
                    continue;
                };
                let spawn = EntitySpawn {
                    entity: reference,
                    tick,
                    kind: inner.entity_type.id,
                    pos: [pos.x, pos.y, pos.z],
                    yaw: inner.yaw.load(),
                    pitch: inner.pitch.load(),
                    state,
                };
                let _ = super::cluster_entity_apply::register_owned_entity_pair(
                    spawn.entity,
                    entity.clone(),
                    spawn.clone(),
                );
                stage_event(StagedEntityEvent::Spawn(spawn));
            }
            sampled = sampled.saturating_add(1);
        }
        for player in world.players.load().iter() {
            if player.client.is_none() {
                continue;
            }
            let entity = player.clone() as Arc<dyn EntityBase>;
            let Some(reference) = local_entity_ref(server, entity.as_ref()) else {
                continue;
            };
            let _ = super::cluster_entity_apply::sync_owned_entity_infallible(entity_pos_row(
                reference,
                tick,
                entity.as_ref(),
            ));
            let holders = route_to_holders(reference.chunk, &super::cluster::chunk_holders);
            let changed = LAST_HOLDERS
                .try_with(|known| {
                    let Ok(mut known) = known.try_borrow_mut() else {
                        return false;
                    };
                    let identity = (reference.origin, reference.local_id);
                    let previous = known.insert(identity, holders);
                    previous.as_ref() != known.get(&identity)
                })
                .unwrap_or(false);
            if changed {
                if let Some(spawn) = spawn_update(server, entity.as_ref()) {
                    let _ = super::cluster_entity_apply::register_owned_entity_pair(
                        spawn.entity,
                        entity,
                        spawn.clone(),
                    );
                    stage_event(StagedEntityEvent::Spawn(spawn));
                }
            }
            sampled = sampled.saturating_add(1);
        }
    }
    prune_visual_memory(tick);
    sampled
}

fn emit_event(event: StagedEntityEvent) {
    let owner = event.owner();
    let kind = event.stream_kind();
    let chunk = match &event {
        StagedEntityEvent::Spawn(update) => update.entity.chunk,
        StagedEntityEvent::Despawn(update) => update.entity.chunk,
        StagedEntityEvent::Visual(update) => update.entity.chunk,
        StagedEntityEvent::Transient(update) => update.entity.chunk,
        StagedEntityEvent::Combat(update) => update.entity.chunk,
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
    };
    match encoded {
        Ok(bytes) => {
            emit_entity_parcel(owner, chunk, kind, &bytes);
        }
        Err(_) => note_emit_dropped(),
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
                    let mut routed: BTreeMap<u16, Vec<EntityPosUpdate>> = BTreeMap::new();
                    for update in ready.iter().copied() {
                        for peer in fanout_to_holders(
                            update.entity.chunk,
                            &super::cluster::chunk_holders,
                            update.entity.owner,
                        ) {
                            routed.entry(peer).or_default().push(update);
                        }
                    }
                    let mut payloads = Vec::new();
                    for (peer, updates) in routed {
                        for rows in updates.chunks(ENTITY_POS_PER_DATAGRAM) {
                            let datagram = EntityPosDatagram {
                                magic: ENTITY_POS_MAGIC,
                                count: u16::try_from(rows.len()).unwrap_or(u16::MAX),
                                tick,
                                updates: rows.to_vec(),
                            };
                            match postcard::to_allocvec(&datagram) {
                                Ok(bytes) => payloads.push((ServerId(peer), bytes)),
                                Err(_) => note_emit_dropped(),
                            }
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

fn cluster_tick() -> Option<TickStamp> {
    super::cluster::disciplined_tick_now()
}

fn local_entity_ref(server: &Server, entity: &dyn EntityBase) -> Option<EntityRef> {
    let owner = cluster_owner(server)?;
    if entity.get_player().is_some_and(|player| player.client.is_none()) {
        return None;
    }
    let inner = entity.get_entity();
    let recorded = inner.cluster_owner.load(Ordering::Relaxed);
    if recorded == u16::MAX {
        inner.cluster_owner.store(owner.0, Ordering::Relaxed);
        inner.cluster_origin_server.store(owner.0, Ordering::Relaxed);
        inner
            .cluster_origin_id
            .store(inner.entity_id, Ordering::Relaxed);
    } else if recorded != owner.0 {
        return None;
    }
    let pos = inner.pos.load();
    Some(EntityRef {
        origin: if inner.cluster_origin_server.load(Ordering::Relaxed) == u16::MAX {
            owner
        } else {
            ServerId(inner.cluster_origin_server.load(Ordering::Relaxed))
        },
        owner,
        local_id: inner.cluster_origin_id.load(Ordering::Relaxed),
        chunk: entity_chunk(&pos),
    })
}

fn spawn_update(server: &Server, entity: &dyn EntityBase) -> Option<EntitySpawn> {
    let Some(reference) = local_entity_ref(server, entity) else {
        return None;
    };
    let tick = cluster_tick()?;
    let inner = entity.get_entity();
    let pos = inner.pos.load();
    Some(EntitySpawn {
        entity: reference,
        tick,
        kind: inner.entity_type.id,
        pos: [pos.x, pos.y, pos.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
        state: spawn_state(entity)?,
    })
}

pub(crate) fn boundary_handoff(
    server: &Server,
    entity: &dyn EntityBase,
    successor: ServerId,
) -> Option<pumpkin_cluster::entities::EntityHandoff> {
    if entity.get_player().is_some() {
        return None;
    }
    let spawn = spawn_update(server, entity)?;
    if matches!(spawn.state, EntitySpawnState::Player(_)) {
        return None;
    }
    let velocity = entity.get_entity().velocity.load();
    Some(pumpkin_cluster::entities::EntityHandoff {
        origin: pumpkin_cluster::entities::EntityOrigin {
            server: spawn.entity.origin,
            local_id: spawn.entity.local_id,
        },
        previous_owner: spawn.entity.owner,
        successor,
        spawn,
        velocity: [velocity.x, velocity.y, velocity.z],
    })
}

pub fn stage_spawn_arc_if_cluster(server: &Server, entity: Arc<dyn EntityBase>) {
    let Some(spawn) = spawn_update(server, entity.as_ref()) else {
        return;
    };
    if let EntitySpawnState::ItemDrop { stack, .. } = &spawn.state {
        super::cluster_entity_apply::update_item_stack_snapshot(
            spawn.entity,
            pumpkin_cluster::inventory::InventoryStack {
                item: stack.item_id,
                count: stack.count,
                nbt: stack.nbt.clone(),
            },
        );
    }
    let _ = super::cluster_entity_apply::register_owned_entity_pair(
        spawn.entity,
        entity,
        spawn.clone(),
    );
    stage_event(StagedEntityEvent::Spawn(spawn));
}

pub fn stage_despawn_if_cluster(server: &Server, entity: &dyn EntityBase) {
    let Some(reference) = local_entity_ref(server, entity) else {
        return;
    };
    let Some(tick) = cluster_tick() else { return };
    let inner = entity.get_entity();
    let _ = super::cluster_entity_apply::remove_owned_entity_pair(reference);
    super::cluster_entity_apply::update_item_stack_snapshot(
        reference,
        pumpkin_cluster::inventory::InventoryStack::empty(),
    );
    stage_event(StagedEntityEvent::Despawn(EntityDespawn {
        entity: EntityRef { chunk: entity_chunk(&inner.pos.load()), ..reference },
        tick,
    }));
}

pub fn stage_transient_if_cluster(server: &Server, entity: &Entity, action: u8) {
    let Some(owner) = cluster_owner(server) else { return };
    let recorded = entity.cluster_owner.load(Ordering::Relaxed);
    if entity.entity_type.id == EntityType::PLAYER.id || (recorded != u16::MAX && recorded != owner.0) {
        return;
    }
    if recorded == u16::MAX {
        entity.cluster_owner.store(owner.0, Ordering::Relaxed);
        entity.cluster_origin_server.store(owner.0, Ordering::Relaxed);
        entity.cluster_origin_id.store(entity.entity_id, Ordering::Relaxed);
    }
    let Some(tick) = cluster_tick() else { return };
    let pos = entity.pos.load();
    stage_event(StagedEntityEvent::Transient(EntityTransientUpdate {
        entity: EntityRef {
            origin: ServerId(entity.cluster_origin_server.load(Ordering::Relaxed)),
            owner,
            local_id: entity.cluster_origin_id.load(Ordering::Relaxed),
            chunk: entity_chunk(&pos),
        },
        tick,
        action,
        value: 0,
    }));
}

pub fn stage_combat_if_cluster(server: &Server, entity: &Entity, kind: u8) {
    let Some(owner) = cluster_owner(server) else { return };
    let recorded = entity.cluster_owner.load(Ordering::Relaxed);
    if entity.entity_type.id == EntityType::PLAYER.id || (recorded != u16::MAX && recorded != owner.0) {
        return;
    }
    if recorded == u16::MAX {
        entity.cluster_owner.store(owner.0, Ordering::Relaxed);
        entity.cluster_origin_server.store(owner.0, Ordering::Relaxed);
        entity.cluster_origin_id.store(entity.entity_id, Ordering::Relaxed);
    }
    let Some(tick) = cluster_tick() else { return };
    let pos = entity.pos.load();
    stage_event(StagedEntityEvent::Combat(EntityCombatUpdate {
        entity: EntityRef {
            origin: ServerId(entity.cluster_origin_server.load(Ordering::Relaxed)),
            owner,
            local_id: entity.cluster_origin_id.load(Ordering::Relaxed),
            chunk: entity_chunk(&pos),
        },
        tick,
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
    let Some(tick) = cluster_tick() else { return 0 };
    let sampled = sample_entity_rows(server, owner, tick);
    fuse_entity_pos(tick);
    sampled
}

#[must_use]
pub fn locally_owned_entities(server: &Server) -> Vec<pumpkin_cluster::lifecycle::OwnedEntity> {
    let Some(owner) = cluster_owner(server) else {
        return Vec::new();
    };
    let Some(tick) = cluster_tick() else {
        return Vec::new();
    };
    let mut owners = OwnerTable::new(owner);
    let mut entities = BTreeMap::new();
    for world in server.worlds.load().iter() {
        for entity in world.entities.load().iter() {
            let Some(reference) = local_entity_ref(server, entity.as_ref()) else {
                continue;
            };
            owners.insert_origin(reference.origin, reference.local_id, reference.chunk);
            entities.insert((reference.origin, reference.local_id), entity.clone());
        }
    }
    let mut owned: Vec<_> = owners
        .entries
        .into_iter()
        .filter_map(|((origin, local_id), chunk)| {
            let entity = entities.get(&(origin, local_id))?;
            let inner = entity.get_entity();
            let state = spawn_state(entity.as_ref())?;
            Some(pumpkin_cluster::lifecycle::OwnedEntity {
                spawn: EntitySpawn {
                    entity: EntityRef {
                        origin,
                        owner,
                        local_id,
                        chunk,
                    },
                    tick,
                    kind: inner.entity_type.id,
                    pos: [inner.pos.load().x, inner.pos.load().y, inner.pos.load().z],
                    yaw: inner.yaw.load(),
                    pitch: inner.pitch.load(),
                    state,
                },
                velocity: {
                    let velocity = inner.velocity.load();
                    [velocity.x, velocity.y, velocity.z]
                },
            })
        })
        .collect();
    owned.sort_by_key(|entity| (entity.spawn.entity.origin, entity.spawn.entity.local_id));
    owned
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::EntityRef;

    fn test_row() -> EntityPosUpdate {
        EntityPosUpdate {
            entity: EntityRef {
                origin: ServerId(2),
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
    fn entity_kind_keeps_the_full_registry_id() {
        let update = EntitySpawn {
            entity: EntityRef {
                origin: ServerId(1),
                owner: ServerId(1),
                local_id: 2,
                chunk: ChunkAddr { x: 3, z: 4 },
            },
            tick: TickStamp(5),
            kind: u16::MAX,
            pos: [0.0; 3],
            yaw: 0.0,
            pitch: 0.0,
            state: EntitySpawnState::Entity { nbt: Vec::new() },
        };
        let bytes = postcard::to_allocvec(&update).expect("entity spawn encodes");
        let decoded: EntitySpawn = postcard::from_bytes(&bytes).expect("entity spawn decodes");
        assert_eq!(decoded.kind, u16::MAX);
    }
}
