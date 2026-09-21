use std::collections::{BTreeMap, HashMap};
use std::cell::RefCell;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::rc::Rc;

use arc_swap::ArcSwap;
use pumpkin_cluster::combat::{CapturedAttackSnapshot, CapturedAttackState, CapturedAttackTargetSnapshot, CombatDualJournal, CombatVerdict};
use pumpkin_cluster::entities::{
    ENTITY_STREAM_KINDS, EntityCombatUpdate, EntityDespawn, EntityPosUpdate, EntityRef,
    EntityHandoff, EntitySpawn, EntitySpawnState, EntityTransientUpdate, EntityVisualUpdate,
    fanout_to_holders, route_to_holders, should_accept,
};
use pumpkin_cluster::entity_action::{EntityDualJournal, EntityEffects, EntityMutationVerdict};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::inventory::InventoryStack;
use pumpkin_cluster::protocol::{AttackItemDrop, CapturedAttack, CapturedAttackOutcome, CapturedAttackTargetOutcome, ChunkAddr, EntityMutationTarget, EntityMutationUpdate, NonLivingAttackOutcome, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::world_delta::TransactionalExplosionUpdate;
use pumpkin_data::entity::EntityType;
use pumpkin_nbt::Nbt;
use pumpkin_nbt::deserializer::NbtReadHelperJava;
use pumpkin_util::math::get_section_cord;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::GameMode;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use super::Server;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram, nbt_bytes};
use crate::entity::{Entity, EntityBase, item::ItemEntity, living::{CapturedAttackTargetBinding, LivingCapturedAttackState, LivingEntity}, player::Player, r#type::from_type};
use crate::entity::nonliving_attack::{NonLivingCapturedAttackState, nonliving_attack_before};
use crate::net::{GameProfile, PlayerConfig};

static ENTITY_APPLIED: AtomicU64 = AtomicU64::new(0);
static ENTITY_DROPPED: AtomicU64 = AtomicU64::new(0);
static ENTITY_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static ENTITY_EMITTED: AtomicU64 = AtomicU64::new(0);
static ENTITY_MALFORMED: AtomicU64 = AtomicU64::new(0);

struct EntityOutbox {
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
}

struct EntityReplica {
    reference: EntityRef,
    entity: Arc<dyn EntityBase>,
    journal: EntityDualJournal,
    combat_journal: CombatDualJournal,
    ground_present: bool,
}

struct EntityTruthPair {
    reference: EntityRef,
    target: EntityMutationTarget,
    local: Arc<dyn EntityBase>,
    ground: Arc<dyn EntityBase>,
    journal: EntityDualJournal,
    combat_journal: CombatDualJournal,
    ground_present: bool,
}

struct ExplosionDropEntity {
    reference: EntityRef,
    entity: Arc<dyn EntityBase>,
    spawn: EntitySpawn,
    ground: Option<Arc<dyn EntityBase>>,
}

struct ExplosionDamageUndo {
    entity: Arc<dyn EntityBase>,
    snapshot: pumpkin_cluster::combat::CombatStateSnapshot,
}

struct ExplosionDamageChange {
    local: Arc<dyn EntityBase>,
    ground: Option<Arc<dyn EntityBase>>,
    health_milli: u32,
}

struct PairEffects<'a> {
    living: &'a LivingEntity,
    target: EntityMutationTarget,
}

impl EntityEffects for PairEffects<'_> {
    fn effect(
        &self,
        target: EntityMutationTarget,
        effect: u16,
    ) -> Option<pumpkin_cluster::protocol::StatusEffectState> {
        entity_target_matches(self.target, target).then(|| {
            self.living
                .cluster_effect_mutation(target)
                .effect(target, effect)
        })?
    }

    fn set_effect(
        &mut self,
        target: EntityMutationTarget,
        effect: pumpkin_cluster::protocol::StatusEffectState,
    ) -> bool {
        entity_target_matches(self.target, target)
            && self
                .living
                .cluster_effect_mutation(target)
                .set_effect(target, effect)
    }

    fn remove_effect(&mut self, target: EntityMutationTarget, effect: u16) -> bool {
        entity_target_matches(self.target, target)
            && self
                .living
                .cluster_effect_mutation(target)
                .remove_effect(target, effect)
    }
}

#[derive(Default)]
struct EntityTruthPairs {
    by_origin: HashMap<(ServerId, i32), EntityTruthPair>,
}

enum EntityDualInput {
    Register {
        reference: EntityRef,
        local: Arc<dyn EntityBase>,
        spawn: EntitySpawn,
    },
    Stage(EntityMutationUpdate),
    Promote {
        cluster_seed: u64,
        tick: pumpkin_cluster::time::TickStamp,
        accepted: Vec<EntityMutationUpdate>,
    },
    Undo(EntityMutationUpdate),
    SyncInfallible(EntityPosUpdate),
    StageCombat(CapturedAttack),
    PromoteCombat {
        cluster_seed: u64,
        tick: pumpkin_cluster::time::TickStamp,
        accepted: Vec<CapturedAttack>,
    },
    UndoCombat(CapturedAttack),
    ApplyTransactionalExplosion {
        world: Arc<crate::world::World>,
        update: TransactionalExplosionUpdate,
    },
    BoundaryMaterialize {
        handoff: EntityHandoff,
        events: mpsc::Sender<BoundaryMaterialization>,
    },
    Remove(EntityRef),
}

#[derive(Debug, Clone)]
pub struct BoundaryMaterialization {
    pub handoff: EntityHandoff,
    pub accepted: bool,
}

#[derive(Default)]
struct EntityReplicas {
    by_origin: HashMap<(ServerId, i32), EntityReplica>,
}

impl EntityReplicas {
    fn get(&self, reference: EntityRef) -> Option<&EntityReplica> {
        self.by_origin.get(&(reference.origin, reference.local_id))
    }

    fn insert(&mut self, reference: EntityRef, entity: Arc<dyn EntityBase>) {
        self.by_origin.insert(
            (reference.origin, reference.local_id),
            EntityReplica {
                reference,
                entity: Arc::clone(&entity),
                journal: EntityDualJournal::new(),
                combat_journal: CombatDualJournal::new(),
                ground_present: true,
            },
        );
    }

    fn remove(&mut self, reference: EntityRef) -> Option<EntityReplica> {
        self.by_origin.remove(&(reference.origin, reference.local_id))
    }

    fn update_reference(&mut self, reference: EntityRef) {
        if let Some(replica) = self.by_origin.get_mut(&(reference.origin, reference.local_id)) {
            replica.reference = reference;
        }
    }
}

static ENTITY_OUTBOX: OnceLock<EntityOutbox> = OnceLock::new();
static ENTITY_POS_BRIDGE: OnceLock<mpsc::Sender<(ServerId, Vec<EntityPosUpdate>)>> =
    OnceLock::new();
static ENTITY_HANDOFF_BRIDGE: OnceLock<mpsc::Sender<(ServerId, Vec<EntityHandoff>)>> =
    OnceLock::new();
static ENTITY_DUAL_BRIDGE: OnceLock<mpsc::Sender<EntityDualInput>> = OnceLock::new();
static ENTITY_BOUNDARY_MATERIALIZE_BRIDGE: OnceLock<mpsc::Sender<EntityDualInput>> =
    OnceLock::new();
static ITEM_SNAPSHOTS: std::sync::LazyLock<ArcSwap<BTreeMap<(ServerId, i32), InventoryStack>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));

pub const ENTITY_SPAWN_MAGIC: u32 = 0x454E5350;
pub const ENTITY_DESPAWN_MAGIC: u32 = 0x454E4458;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    _peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = ENTITY_OUTBOX.set(EntityOutbox { local, outbound });
}

pub fn install_entity_pos_bridge(sink: mpsc::Sender<(ServerId, Vec<EntityPosUpdate>)>) {
    let _ = ENTITY_POS_BRIDGE.set(sink);
}

pub fn forward_entity_handoffs(peer: ServerId, handoffs: Vec<EntityHandoff>) -> bool {
    if handoffs.is_empty() {
        return false;
    }
    let Some(bridge) = ENTITY_HANDOFF_BRIDGE.get() else {
        note_dropped("no-handoff-bridge");
        return false;
    };
    if bridge.try_send((peer, handoffs)).is_ok() {
        true
    } else {
        note_dropped("handoff-full");
        false
    }
}

pub fn register_owned_entity_pair(
    reference: EntityRef,
    local: Arc<dyn EntityBase>,
    spawn: EntitySpawn,
) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge
        .try_send(EntityDualInput::Register {
            reference,
            local,
            spawn,
        })
        .is_ok()
}

pub fn stage_owned_entity_mutation(update: EntityMutationUpdate) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::Stage(update)).is_ok()
}

pub fn promote_owned_entity_mutations(
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: Vec<EntityMutationUpdate>,
) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge
        .try_send(EntityDualInput::Promote {
            cluster_seed,
            tick,
            accepted,
        })
        .is_ok()
}

pub fn undo_owned_entity_mutation(update: EntityMutationUpdate) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::Undo(update)).is_ok()
}

pub fn sync_owned_entity_infallible(update: EntityPosUpdate) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::SyncInfallible(update)).is_ok()
}

pub fn stage_captured_attack(attack: CapturedAttack) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::StageCombat(attack)).is_ok()
}

pub fn promote_captured_attacks(
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: Vec<CapturedAttack>,
) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge
        .try_send(EntityDualInput::PromoteCombat {
            cluster_seed,
            tick,
            accepted,
        })
        .is_ok()
}

pub fn undo_captured_attack(attack: CapturedAttack) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::UndoCombat(attack)).is_ok()
}

pub fn apply_transactional_explosion_entities(
    world: Arc<crate::world::World>,
    update: TransactionalExplosionUpdate,
) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge
        .try_send(EntityDualInput::ApplyTransactionalExplosion { world, update })
        .is_ok()
}

pub fn queue_boundary_materialization(
    handoff: EntityHandoff,
    events: mpsc::Sender<BoundaryMaterialization>,
) -> bool {
    let Some(bridge) = ENTITY_BOUNDARY_MATERIALIZE_BRIDGE.get() else {
        return false;
    };
    bridge
        .try_send(EntityDualInput::BoundaryMaterialize { handoff, events })
        .is_ok()
}

pub fn remove_owned_entity_pair(reference: EntityRef) -> bool {
    let Some(bridge) = ENTITY_DUAL_BRIDGE.get() else {
        return false;
    };
    bridge.try_send(EntityDualInput::Remove(reference)).is_ok()
}

#[must_use]
pub fn entity_stream_kinds() -> [StreamKind; 4] {
    ENTITY_STREAM_KINDS
}

fn try_emit_entity(owner: ServerId, chunk: ChunkAddr, kind: StreamKind, bytes: &[u8]) -> u64 {
    let Some(outbox) = ENTITY_OUTBOX.get() else {
        return 0;
    };
    if owner != outbox.local {
        note_dropped("owner");
        return 0;
    }
    let mut sent = 0_u64;
    for peer in fanout_to_holders(chunk, &super::cluster::chunk_holders, outbox.local) {
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

pub fn emit_entity_parcel(owner: ServerId, chunk: ChunkAddr, kind: StreamKind, bytes: &[u8]) -> u64 {
    try_emit_entity(owner, chunk, kind, bytes)
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

#[must_use]
pub fn entity_malformed() -> u64 {
    ENTITY_MALFORMED.load(Ordering::Relaxed)
}

#[must_use]
pub fn entity_ref_for(entity: &dyn EntityBase) -> Option<EntityRef> {
    let inner = entity.get_entity();
    let owner = inner.cluster_owner.load(Ordering::Relaxed);
    if owner == u16::MAX {
        return None;
    }
    let pos = inner.pos.load();
    Some(EntityRef {
        origin: ServerId(inner.cluster_origin_server.load(Ordering::Relaxed)),
        owner: ServerId(owner),
        local_id: inner.cluster_origin_id.load(Ordering::Relaxed),
        chunk: ChunkAddr {
            x: get_section_cord(pos.x.floor() as i32),
            z: get_section_cord(pos.z.floor() as i32),
        },
    })
}

#[must_use]
pub fn entity_is_owned_by(entity: &dyn EntityBase, local: ServerId) -> bool {
    entity_ref_for(entity).is_some_and(|reference| reference.owner == local)
}

#[must_use]
pub fn entity_by_ref(server: &Server, reference: EntityRef) -> Option<Arc<dyn EntityBase>> {
    server.worlds.load().iter().find_map(|world| {
        world
            .players
            .load()
            .iter()
            .find_map(|player| {
                let entity = player.clone() as Arc<dyn EntityBase>;
                entity_ref_for(entity.as_ref()).filter(|candidate| {
                    candidate.origin == reference.origin && candidate.local_id == reference.local_id
                })?;
                Some(entity)
            })
            .or_else(|| {
                world.entities.load().iter().find_map(|entity| {
                    entity_ref_for(entity.as_ref()).filter(|candidate| {
                        candidate.origin == reference.origin
                            && candidate.local_id == reference.local_id
                    })?;
                    Some(entity.clone())
                })
            })
    })
}

#[must_use]
pub fn entity_holders(reference: EntityRef) -> Vec<ServerId> {
    fanout_to_holders(reference.chunk, &super::cluster::chunk_holders, ServerId(u16::MAX))
        .into_iter()
        .map(ServerId)
        .collect()
}

#[must_use]
pub fn item_stack_snapshot(reference: EntityRef) -> Option<InventoryStack> {
    ITEM_SNAPSHOTS
        .load()
        .get(&(reference.origin, reference.local_id))
        .cloned()
}

pub fn update_item_stack_snapshot(reference: EntityRef, stack: InventoryStack) {
    ITEM_SNAPSHOTS.rcu(|snapshots| {
        let mut next = (**snapshots).clone();
        if stack.is_empty() {
            next.remove(&(reference.origin, reference.local_id));
        } else {
            next.insert((reference.origin, reference.local_id), stack.clone());
        }
        Arc::new(next)
    });
}

fn apply_item_spawn_snapshot(reference: EntityRef, state: &EntitySpawnState) {
    match state {
        EntitySpawnState::ItemDrop { stack, .. } => update_item_stack_snapshot(
            reference,
            InventoryStack {
                item: stack.item_id,
                count: stack.count,
                nbt: stack.nbt.clone(),
            },
        ),
        EntitySpawnState::Entity { .. } | EntitySpawnState::Player(_) => {}
    }
}

fn remove_item_spawn_snapshot(reference: EntityRef) {
    ITEM_SNAPSHOTS.rcu(|snapshots| {
        let mut next = (**snapshots).clone();
        next.remove(&(reference.origin, reference.local_id));
        Arc::new(next)
    });
}

pub fn remove_transferred_entities(server: &Server, local: ServerId, handoffs: &[EntityHandoff]) -> usize {
    let mut remove = Vec::new();
    let mut seen = HashMap::new();
    for handoff in handoffs {
        if !handoff.is_consistent() || handoff.previous_owner != local {
            return 0;
        }
        if seen
            .insert((handoff.origin.server, handoff.origin.local_id), ())
            .is_some()
        {
            return 0;
        }
        let mut matching = Vec::new();
        for world in server.worlds.load().iter() {
            for entity in world.entities.load().iter() {
                let Some(reference) = entity_ref_for(entity.as_ref()) else {
                    continue;
                };
                if reference.origin == handoff.origin.server
                    && reference.local_id == handoff.origin.local_id
                    && reference.owner == handoff.previous_owner
                {
                    matching.push(entity.clone());
                }
            }
        }
        if matching.len() != 1 {
            return 0;
        }
        remove.push((matching.remove(0), handoff.successor));
    }
    let count = remove.len();
    for (entity, successor) in remove {
        entity
            .get_entity()
            .cluster_owner
            .store(local.0, Ordering::Relaxed);
        if let Some(reference) = entity_ref_for(entity.as_ref()) {
            let _ = remove_owned_entity_pair(reference);
            remove_item_spawn_snapshot(reference);
        }
        entity
            .get_entity()
            .cluster_owner
            .store(successor.0, Ordering::Relaxed);
        entity.get_entity().world.load().remove_entity(entity.as_ref());
    }
    count
}

pub fn remove_primary_persisted_boundary_entity(
    server: &Server,
    local: ServerId,
    reference: EntityRef,
) -> bool {
    let entity = server.worlds.load().iter().find_map(|world| {
        world.entities.load().iter().find_map(|entity| {
            entity_ref_for(entity.as_ref()).filter(|candidate| {
                candidate.origin == reference.origin
                    && candidate.local_id == reference.local_id
                    && candidate.owner == local
            })?;
            Some((Arc::clone(world), Arc::clone(entity)))
        })
    });
    let Some((world, entity)) = entity else {
        return false;
    };
    if !world.remove_cluster_entity_transaction(entity.as_ref()) {
        return false;
    }
    let _ = remove_owned_entity_pair(reference);
    remove_item_spawn_snapshot(reference);
    true
}

fn note_applied() {
    ENTITY_APPLIED.fetch_add(1, Ordering::Relaxed);
}

fn note_dropped(reason: &str) {
    ENTITY_DROPPED.fetch_add(1, Ordering::Relaxed);
    debug!(reason, "cluster entity update dropped");
}

fn note_malformed_pos_frame(peer: ServerId, bytes: &[u8], error: &impl std::fmt::Display) {
    let occurrence = ENTITY_MALFORMED.fetch_add(1, Ordering::Relaxed) + 1;
    note_dropped("decode");
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

fn accepted(peer: ServerId, local: ServerId, reference: &EntityRef) -> bool {
    should_accept(peer, local, reference, &super::cluster::chunk_holders)
}

fn replica_world(server: &Server, reference: &EntityRef) -> Option<Arc<crate::world::World>> {
    let chunk = Vector2::new(reference.chunk.x, reference.chunk.z);
    server
        .worlds
        .load()
        .iter()
        .find(|world| world.level.is_chunk_loaded(&chunk))
        .cloned()
}

fn player_replica_world(
    server: &Server,
    state: &pumpkin_cluster::entities::PlayerSpawnState,
) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| {
            world.get_world_name() == state.world
                && world.dimension.minecraft_name == state.dimension
        })
        .cloned()
}

fn replica_uuid(reference: EntityRef) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(b"PMPR");
    bytes[4..6].copy_from_slice(&reference.origin.0.to_be_bytes());
    bytes[6..10].copy_from_slice(&reference.local_id.to_be_bytes());
    Uuid::from_bytes(bytes)
}

fn read_nbt(bytes: &[u8]) -> Option<pumpkin_nbt::NbtCompound> {
    let mut cursor = Cursor::new(bytes);
    let mut reader = NbtReadHelperJava::new(&mut cursor);
    Nbt::read_unnamed(&mut reader).ok().map(|nbt| nbt.root_tag)
}

fn apply_spawn_state(entity: &dyn EntityBase, state: &EntitySpawnState) -> bool {
    match state {
        EntitySpawnState::Entity { nbt } => {
            let Some(nbt) = read_nbt(nbt) else {
                return false;
            };
            entity.read_nbt_non_mut(&nbt);
            true
        }
        EntitySpawnState::ItemDrop { stack, entity_nbt } => {
            let Some(item) = entity.get_item_entity() else {
                return false;
            };
            let Some(entity_nbt) = read_nbt(entity_nbt) else {
                return false;
            };
            let stack = InventoryStack {
                item: stack.item_id,
                count: stack.count,
                nbt: stack.nbt.clone(),
            };
            item.apply_cluster_full_nbt(&entity_nbt, &stack)
        }
        EntitySpawnState::Player(_) => false,
    }
}

fn apply_player_state(player: &Player, state: &pumpkin_cluster::entities::PlayerSpawnState) -> bool {
    let Some(entity_nbt) = read_nbt(&state.entity.entity_nbt) else {
        return false;
    };
    player.read_nbt_non_mut(&entity_nbt);
    let entity = player.get_entity();
    entity.velocity.store(Vector3::new(
        state.entity.velocity[0],
        state.entity.velocity[1],
        state.entity.velocity[2],
    ));
    entity.on_ground.store(state.entity.on_ground, Ordering::Relaxed);
    entity.sneaking.store(state.entity.flags & 1 != 0, Ordering::Relaxed);
    entity.sprinting.store(state.entity.flags & 2 != 0, Ordering::Relaxed);
    entity.swimming.store(state.entity.flags & 4 != 0, Ordering::Relaxed);
    entity.invisible.store(state.entity.flags & 8 != 0, Ordering::Relaxed);
    entity.glowing.store(state.entity.flags & 16 != 0, Ordering::Relaxed);
    entity.fall_flying.store(state.entity.flags & 32 != 0, Ordering::Relaxed);
    entity.fire_ticks.store(state.entity.fire_ticks, Ordering::Relaxed);
    player
        .living_entity
        .health
        .store(f32::from(state.entity.health_milli) / 1000.0);
    player
        .living_entity
        .absorption
        .store(f32::from(state.entity.absorption_milli) / 1000.0);
    player
        .living_entity
        .fall_distance
        .store(state.entity.fall_distance_milli as f32 / 1000.0);
    player
        .experience_level
        .store(state.entity.experience_level, Ordering::Relaxed);
    player
        .experience_progress
        .store(f32::from(state.entity.experience_progress_milli) / 1000.0);
    player
        .experience_points
        .store(state.entity.experience_points, Ordering::Relaxed);
    player.hunger_manager.level.store(state.entity.food);
    player
        .hunger_manager
        .saturation
        .store(f32::from(state.entity.saturation_milli) / 1000.0);
    true
}

fn materialize_player_replica(
    world: &Arc<crate::world::World>,
    reference: EntityRef,
    update: &EntitySpawn,
    state: &pumpkin_cluster::entities::PlayerSpawnState,
) -> Option<Arc<dyn EntityBase>> {
    if !state.is_authenticated_for(reference) {
        return None;
    }
    let profile = super::cluster_presence::remote_replica_profile(state.gid)?;
    if profile.uuid != state.uuid
        || profile.name != state.name
        || profile.properties != state.properties
        || profile.gamemode != state.gamemode
    {
        return None;
    }
    let profile = GameProfile {
        id: Uuid::from_bytes(state.uuid),
        name: state.name.clone(),
        properties: ArcSwap::from_pointee(super::cluster_chat_pm::protocol_properties(&state.properties)),
        profile_actions: None,
    };
    let player = Arc::new(Player::new_replica(
        profile,
        PlayerConfig::default(),
        world,
        player_gamemode(state.gamemode),
        None,
    ));
    player.set_cluster_gid(Some(state.gid));
    let entity = player.get_entity();
    entity.cluster_owner.store(reference.owner.0, Ordering::Relaxed);
    entity
        .cluster_origin_server
        .store(reference.origin.0, Ordering::Relaxed);
    entity
        .cluster_origin_id
        .store(reference.local_id, Ordering::Relaxed);
    entity.set_pos(Vector3::new(update.pos[0], update.pos[1], update.pos[2]));
    entity.yaw.store(update.yaw);
    entity.pitch.store(update.pitch);
    if !apply_player_state(player.as_ref(), state) {
        return None;
    }
    player.init_data_tracker();
    world.add_player_replica(&player).ok()?;
    Some(player)
}

const fn player_gamemode(mode: pumpkin_cluster::protocol::PlayerGameMode) -> GameMode {
    match mode {
        pumpkin_cluster::protocol::PlayerGameMode::Survival => GameMode::Survival,
        pumpkin_cluster::protocol::PlayerGameMode::Creative => GameMode::Creative,
        pumpkin_cluster::protocol::PlayerGameMode::Adventure => GameMode::Adventure,
        pumpkin_cluster::protocol::PlayerGameMode::Spectator => GameMode::Spectator,
    }
}

fn truth_target(reference: EntityRef, entity: &dyn EntityBase) -> Option<EntityMutationTarget> {
    entity.get_player().and_then(Player::cluster_gid).map_or_else(
        || Some(EntityMutationTarget::Entity(reference)),
        |gid| {
            Some(EntityMutationTarget::Player {
                gid,
                chunk: reference.chunk,
            })
        },
    )
}

fn entity_target_matches(left: EntityMutationTarget, right: EntityMutationTarget) -> bool {
    left.same_identity(right)
}

fn materialize_ground_entity(
    reference: EntityRef,
    local: &Arc<dyn EntityBase>,
    spawn: &EntitySpawn,
) -> Option<Arc<dyn EntityBase>> {
    let world = local.get_entity().world.load_full();
    let ground: Arc<dyn EntityBase> = if let Some(player) = local.get_player() {
        let ground = Arc::new(Player::new_replica(
            player.gameprofile.clone(),
            (*player.config.load_full()).clone(),
            &world,
            player.gamemode.load(),
            Some(player.entity_id()),
        ));
        ground.set_cluster_gid(player.cluster_gid());
        ground
    } else {
        let entity_type = EntityType::from_raw(spawn.kind)?;
        from_type(entity_type, Vector3::new(spawn.pos[0], spawn.pos[1], spawn.pos[2]), &world, replica_uuid(reference))
    };
    let inner = ground.get_entity();
    inner.cluster_owner.store(reference.owner.0, Ordering::Relaxed);
    inner
        .cluster_origin_server
        .store(reference.origin.0, Ordering::Relaxed);
    inner
        .cluster_origin_id
        .store(reference.local_id, Ordering::Relaxed);
    inner.set_pos(Vector3::new(spawn.pos[0], spawn.pos[1], spawn.pos[2]));
    inner.yaw.store(spawn.yaw);
    inner.pitch.store(spawn.pitch);
    let applied = match (&spawn.state, ground.get_player()) {
        (EntitySpawnState::Player(state), Some(player)) => {
            state.is_authenticated_for(reference) && apply_player_state(player, state)
        }
        (EntitySpawnState::Player(_), None) => false,
        (state, _) => apply_spawn_state(ground.as_ref(), state),
    };
    if !applied {
        return None;
    }
    Some(ground)
}

fn explosion_health_matches(entity: &Arc<dyn EntityBase>, expected: u32) -> bool {
    entity
        .get_living_entity()
        .is_some_and(|living| (living.health.load().max(0.0) * 1000.0).round() as u32 == expected)
}

fn explosion_drop_entity(
    world: &Arc<crate::world::World>,
    drop: &pumpkin_cluster::world_delta::ExplosionDropRef,
    tick: pumpkin_cluster::time::TickStamp,
    local: ServerId,
) -> Option<ExplosionDropEntity> {
    let stack = ItemEntity::item_stack_from_cluster_snapshot(&drop.stack)?;
    let pos = Vector3::new(
        f64::from(drop.source.x) + 0.5,
        f64::from(drop.source.y) + 0.5,
        f64::from(drop.source.z) + 0.5,
    );
    let item = Arc::new(ItemEntity::new(Entity::new(world.clone(), pos, &EntityType::ITEM), stack));
    let inner = item.get_entity();
    inner.cluster_owner.store(drop.entity.owner.0, Ordering::Relaxed);
    inner
        .cluster_origin_server
        .store(drop.entity.origin.0, Ordering::Relaxed);
    inner
        .cluster_origin_id
        .store(drop.entity.local_id, Ordering::Relaxed);
    let mut nbt = pumpkin_nbt::compound::NbtCompound::new();
    inner.write_nbt(&mut nbt);
    let spawn = EntitySpawn {
        entity: drop.entity,
        tick,
        kind: EntityType::ITEM.id,
        pos: [pos.x, pos.y, pos.z],
        yaw: inner.yaw.load(),
        pitch: inner.pitch.load(),
        state: EntitySpawnState::ItemDrop {
            stack: pumpkin_cluster::entities::ItemStackState {
                item_id: drop.stack.item,
                count: drop.stack.count,
                nbt: drop.stack.nbt.clone(),
            },
            entity_nbt: nbt_bytes(nbt),
        },
    };
    let entity = item as Arc<dyn EntityBase>;
    let ground = (drop.entity.owner == local)
        .then(|| materialize_ground_entity(drop.entity, &entity, &spawn))
        .flatten();
    if drop.entity.owner == local && ground.is_none() {
        return None;
    }
    Some(ExplosionDropEntity {
        reference: drop.entity,
        entity,
        spawn,
        ground,
    })
}

fn apply_transactional_explosion_entities_now(
    pairs: &mut EntityTruthPairs,
    replicas: &mut EntityReplicas,
    local: ServerId,
    world: &Arc<crate::world::World>,
    update: &TransactionalExplosionUpdate,
) -> bool {
    if !update.is_valid() {
        return false;
    }
    if update.edits.iter().any(|edit| {
        super::cluster_world_apply::read_delta_state(world, edit.pos) != Some(edit.old_state)
            || pumpkin_data::BlockStateId::new(edit.new_state).is_none()
    }) {
        return false;
    }
    if update.damages.iter().any(|damage| {
        let identity = (damage.target.origin, damage.target.local_id);
        if let Some(pair) = pairs.by_origin.get(&identity) {
            return !explosion_health_matches(&pair.local, damage.expected_health_milli)
                || !explosion_health_matches(&pair.ground, damage.expected_health_milli);
        }
        replicas
            .by_origin
            .get(&identity)
            .is_none_or(|replica| !explosion_health_matches(&replica.entity, damage.expected_health_milli))
    }) {
        return false;
    }
    let mut damage_changes = Vec::with_capacity(update.damages.len());
    let mut damage_undo = Vec::with_capacity(update.damages.len() * 2);
    for damage in &update.damages {
        let identity = (damage.target.origin, damage.target.local_id);
        if let Some(pair) = pairs.by_origin.get(&identity) {
            damage_undo.push(ExplosionDamageUndo {
                entity: Arc::clone(&pair.local),
                snapshot: pair
                    .local
                    .get_living_entity()
                    .map(LivingEntity::cluster_combat_snapshot)
                    .unwrap_or_else(|| unreachable!()),
            });
            damage_undo.push(ExplosionDamageUndo {
                entity: Arc::clone(&pair.ground),
                snapshot: pair
                    .ground
                    .get_living_entity()
                    .map(LivingEntity::cluster_combat_snapshot)
                    .unwrap_or_else(|| unreachable!()),
            });
            damage_changes.push(ExplosionDamageChange {
                local: Arc::clone(&pair.local),
                ground: Some(Arc::clone(&pair.ground)),
                health_milli: damage.new_health_milli,
            });
            continue;
        }
        let Some(replica) = replicas.by_origin.get(&identity) else {
            return false;
        };
        damage_undo.push(ExplosionDamageUndo {
            entity: Arc::clone(&replica.entity),
            snapshot: replica
                .entity
                .get_living_entity()
                .map(LivingEntity::cluster_combat_snapshot)
                .unwrap_or_else(|| unreachable!()),
        });
        damage_changes.push(ExplosionDamageChange {
            local: Arc::clone(&replica.entity),
            ground: None,
            health_milli: damage.new_health_milli,
        });
    }
    if update.drops.iter().any(|drop| {
        let identity = (drop.entity.origin, drop.entity.local_id);
        pairs.by_origin.contains_key(&identity) || replicas.by_origin.contains_key(&identity)
    }) {
        return false;
    }
    let mut drops = Vec::with_capacity(update.drops.len());
    for drop in &update.drops {
        let Some(entity) = explosion_drop_entity(world, drop, update.tick(), local) else {
            return false;
        };
        drops.push(entity);
    }
    let mut applied_blocks: Vec<pumpkin_cluster::world_delta::BlockDelta> =
        Vec::with_capacity(update.edits.len());
    for edit in &update.edits {
        if !super::cluster_world_apply::write_delta_state(world, edit.pos, edit.new_state) {
            for prior in applied_blocks.into_iter().rev() {
                let _ = super::cluster_world_apply::write_delta_state(world, prior.pos, prior.old_state);
            }
            return false;
        }
        applied_blocks.push(*edit);
    }
    for damage in &damage_changes {
        let local_living = damage
            .local
            .get_living_entity()
            .unwrap_or_else(|| unreachable!());
        local_living.set_health(damage.health_milli as f32 / 1000.0);
        if let Some(ground) = &damage.ground {
            let ground_living = ground
                .get_living_entity()
                .unwrap_or_else(|| unreachable!());
            ground_living.set_health(damage.health_milli as f32 / 1000.0);
        }
    }
    let mut spawned: Vec<ExplosionDropEntity> = Vec::new();
    for drop in drops {
        drop.entity.init_data_tracker();
        if !world.add_cluster_entity_transaction(Arc::clone(&drop.entity)) {
            for spawned_drop in spawned.into_iter().rev() {
                let _ = world.remove_cluster_entity_transaction(spawned_drop.entity.as_ref());
                remove_item_spawn_snapshot(spawned_drop.reference);
                remove_truth_pair(pairs, spawned_drop.reference);
                let _ = replicas.remove(spawned_drop.reference);
            }
            for damage in damage_undo {
                if let Some(living) = damage.entity.get_living_entity() {
                    let _ = living.cluster_restore_combat_snapshot(&damage.snapshot);
                }
            }
            for prior in applied_blocks.into_iter().rev() {
                let _ = super::cluster_world_apply::write_delta_state(world, prior.pos, prior.old_state);
            }
            return false;
        }
        apply_item_spawn_snapshot(drop.reference, &drop.spawn.state);
        if let Some(ground) = &drop.ground {
            let target = EntityMutationTarget::Entity(drop.reference);
            pairs.by_origin.insert(
                (drop.reference.origin, drop.reference.local_id),
                EntityTruthPair {
                    reference: drop.reference,
                    target,
                    local: Arc::clone(&drop.entity),
                    ground: Arc::clone(ground),
                    journal: EntityDualJournal::new(),
                    combat_journal: CombatDualJournal::new(),
                    ground_present: true,
                },
            );
        } else {
            replicas.insert(drop.reference, Arc::clone(&drop.entity));
        }
        spawned.push(drop);
    }
    super::cluster_world_apply::promote_transactional_explosion_truth(
        world,
        update.tick(),
        update,
    );
    true
}

fn register_truth_pair(
    pairs: &mut EntityTruthPairs,
    local: ServerId,
    reference: EntityRef,
    entity: Arc<dyn EntityBase>,
    spawn: EntitySpawn,
) {
    if reference.owner != local || pairs.by_origin.contains_key(&(reference.origin, reference.local_id)) {
        return;
    }
    let Some(target) = truth_target(reference, entity.as_ref()) else {
        return;
    };
    let Some(ground) = materialize_ground_entity(reference, &entity, &spawn) else {
        note_dropped("ground-materialize");
        return;
    };
    pairs.by_origin.insert(
        (reference.origin, reference.local_id),
        EntityTruthPair {
            reference,
            target,
            local: entity,
            ground,
            journal: EntityDualJournal::new(),
            combat_journal: CombatDualJournal::new(),
            ground_present: true,
        },
    );
}

fn remove_truth_pair(pairs: &mut EntityTruthPairs, reference: EntityRef) {
    pairs.by_origin.remove(&(reference.origin, reference.local_id));
}

fn sync_truth_pair(pairs: &mut EntityTruthPairs, update: EntityPosUpdate) {
    let Some(pair) = pairs
        .by_origin
        .get_mut(&(update.entity.origin, update.entity.local_id))
    else {
        return;
    };
    let inner = pair.ground.get_entity();
    inner.set_pos(Vector3::new(update.pos[0], update.pos[1], update.pos[2]));
    inner.velocity.store(Vector3::new(update.vel[0], update.vel[1], update.vel[2]));
    inner.yaw.store(update.yaw);
    inner.pitch.store(update.pitch);
    pair.reference = update.entity;
    pair.target = truth_target(update.entity, pair.local.as_ref())
        .unwrap_or(EntityMutationTarget::Entity(update.entity));
}

fn pair_for_update_mut(
    pairs: &mut EntityTruthPairs,
    target: EntityMutationTarget,
) -> Option<&mut EntityTruthPair> {
    match target {
        EntityMutationTarget::Entity(reference) => pairs.by_origin.get_mut(&(reference.origin, reference.local_id)),
        EntityMutationTarget::Player { gid, .. } => pairs
            .by_origin
            .values_mut()
            .find(|pair| {
                matches!(
                    pair.target,
                    EntityMutationTarget::Player {
                        gid: pair_gid,
                        ..
                    } if pair_gid == gid
                )
            }),
    }
}

fn stage_truth_mutation(pairs: &mut EntityTruthPairs, update: EntityMutationUpdate) {
    let Some(pair) = pair_for_update_mut(pairs, update.target) else {
        note_dropped("truth-pair");
        return;
    };
    let Some(living) = pair.local.get_living_entity() else {
        note_dropped("truth-local");
        return;
    };
    let mut effects = PairEffects {
        living,
        target: pair.target,
    };
    if matches!(pair.journal.stage_local_optimistic(&mut effects, update), EntityMutationVerdict::Rejected) {
        note_dropped("truth-stage");
    }
}

fn stage_replica_mutation(replicas: &mut EntityReplicas, update: EntityMutationUpdate) {
    let Some(replica) = replica_for_target_mut(replicas, update.target) else {
        note_dropped("truth-pair");
        return;
    };
    let Some(living) = replica.entity.get_living_entity() else {
        note_dropped("truth-local");
        return;
    };
    let mut effects = living.cluster_effect_mutation(update.target);
    if matches!(
        replica.journal.stage_local_optimistic(&mut effects, update),
        EntityMutationVerdict::Rejected
    ) {
        note_dropped("truth-stage");
    }
}

fn undo_truth_mutation(pairs: &mut EntityTruthPairs, update: EntityMutationUpdate) {
    let Some(pair) = pair_for_update_mut(pairs, update.target) else {
        note_dropped("truth-pair");
        return;
    };
    let Some(living) = pair.local.get_living_entity() else {
        note_dropped("truth-local");
        return;
    };
    let mut effects = PairEffects {
        living,
        target: pair.target,
    };
    if !pair.journal.undo_local_loser(&mut effects, update) {
        note_dropped("truth-undo");
    }
}

fn undo_replica_mutation(replicas: &mut EntityReplicas, update: EntityMutationUpdate) {
    let Some(replica) = replica_for_target_mut(replicas, update.target) else {
        note_dropped("truth-pair");
        return;
    };
    let Some(living) = replica.entity.get_living_entity() else {
        note_dropped("truth-local");
        return;
    };
    let mut effects = living.cluster_effect_mutation(update.target);
    if !replica.journal.undo_local_loser(&mut effects, update) {
        note_dropped("truth-undo");
    }
}

struct CapturedAttackEntities {
    actor: Arc<dyn EntityBase>,
    targets: Vec<(EntityMutationTarget, Arc<dyn EntityBase>)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CombatTruth {
    Local,
    Ground,
    Holder,
}

struct NonLivingLifecycleContext<'a> {
    pairs: &'a mut EntityTruthPairs,
    replicas: &'a mut EntityReplicas,
    local: ServerId,
    truth: CombatTruth,
    tick: pumpkin_cluster::time::TickStamp,
    local_bindings: Vec<(EntityMutationTarget, Arc<dyn EntityBase>)>,
    ground_bindings: Vec<(EntityMutationTarget, Arc<dyn EntityBase>)>,
    expected_drops: Vec<(EntityMutationTarget, Vec<AttackItemDrop>)>,
    observed_drops: Vec<(EntityMutationTarget, Vec<AttackItemDrop>)>,
}

impl NonLivingLifecycleContext<'_> {
    fn entity(&self, target: EntityMutationTarget) -> Option<Arc<dyn EntityBase>> {
        let bindings = if self.truth == CombatTruth::Ground {
            &self.ground_bindings
        } else {
            &self.local_bindings
        };
        bindings
            .iter()
            .find(|(known, _)| known.same_identity(target))
            .map(|(_, entity)| Arc::clone(entity))
    }

    fn target_reference(target: EntityMutationTarget) -> Option<EntityRef> {
        match target {
            EntityMutationTarget::Entity(reference) => Some(reference),
            EntityMutationTarget::Player { .. } => None,
        }
    }

    fn present(&self, target: EntityMutationTarget) -> Option<bool> {
        let reference = Self::target_reference(target)?;
        match self.truth {
            CombatTruth::Ground => self
                .pairs
                .by_origin
                .get(&(reference.origin, reference.local_id))
                .map(|pair| pair.ground_present)
                .or_else(|| self.replicas.by_origin.get(&(reference.origin, reference.local_id)).map(|replica| replica.ground_present)),
            CombatTruth::Local | CombatTruth::Holder => self.entity(target).map(|entity| {
                entity
                    .get_entity()
                    .world
                    .load()
                    .entities
                    .load()
                    .iter()
                    .any(|current| Arc::ptr_eq(current, &entity))
            }),
        }
    }

    fn set_present(&mut self, target: EntityMutationTarget, present: bool) -> bool {
        let Some(reference) = Self::target_reference(target) else {
            return false;
        };
        match self.truth {
            CombatTruth::Ground => {
                if let Some(pair) = self.pairs.by_origin.get_mut(&(reference.origin, reference.local_id)) {
                    pair.ground_present = present;
                    return true;
                }
                if let Some(replica) = self.replicas.by_origin.get_mut(&(reference.origin, reference.local_id)) {
                    replica.ground_present = present;
                    return true;
                }
                false
            }
            CombatTruth::Local | CombatTruth::Holder => {
                let Some(entity) = self.entity(target) else {
                    return false;
                };
                let world = entity.get_entity().world.load_full();
                let is_present = world
                    .entities
                    .load()
                    .iter()
                    .any(|current| Arc::ptr_eq(current, &entity));
                if is_present == present {
                    return true;
                }
                if present {
                    world.add_cluster_entity_transaction(entity)
                } else {
                    world.detach_cluster_entity_transaction(&entity)
                }
            }
        }
    }

    fn expected_for(&self, target: EntityMutationTarget) -> Vec<AttackItemDrop> {
        self.expected_drops
            .iter()
            .find(|(known, _)| known.same_identity(target))
            .map(|(_, drops)| drops.clone())
            .unwrap_or_default()
    }

    fn drops_present(&self, drops: &[AttackItemDrop]) -> bool {
        drops.iter().all(|drop| {
            let identity = (drop.entity.origin, drop.entity.local_id);
            self.pairs
                .by_origin
                .get(&identity)
                .is_some_and(|pair| self.truth != CombatTruth::Ground || pair.ground_present)
                || self
                    .replicas
                    .by_origin
                    .get(&identity)
                    .is_some_and(|replica| self.truth != CombatTruth::Ground || replica.ground_present)
        })
    }

    fn record_observed(&mut self, target: EntityMutationTarget, drops: Vec<AttackItemDrop>) {
        if let Some((_, known)) = self
            .observed_drops
            .iter_mut()
            .find(|(known, _)| known.same_identity(target))
        {
            *known = drops;
        } else {
            self.observed_drops.push((target, drops));
        }
    }

    fn observed_for(&self, target: EntityMutationTarget) -> Vec<AttackItemDrop> {
        self.observed_drops
            .iter()
            .find(|(known, _)| known.same_identity(target))
            .map(|(_, drops)| drops.clone())
            .unwrap_or_default()
    }

    fn snapshot_outcome(
        &mut self,
        target: EntityMutationTarget,
        outcome: NonLivingAttackOutcome,
    ) -> Option<NonLivingAttackOutcome> {
        let present = self.present(target)?;
        let expected = self.expected_for(target);
        let spawned = self.drops_present(&expected).then_some(expected).unwrap_or_default();
        self.record_observed(target, spawned.clone());
        Some(nonliving_outcome_with_lifecycle(outcome, present, spawned))
    }

    fn spawn_drop(&mut self, source: &Arc<dyn EntityBase>, drop: &AttackItemDrop) -> bool {
        let identity = (drop.entity.origin, drop.entity.local_id);
        if let Some(pair) = self.pairs.by_origin.get_mut(&identity) {
            if self.truth == CombatTruth::Ground && !pair.ground_present {
                pair.ground_present = true;
                return true;
            }
            return false;
        }
        if let Some(replica) = self.replicas.by_origin.get_mut(&identity) {
            if self.truth == CombatTruth::Ground && !replica.ground_present {
                replica.ground_present = true;
                return true;
            }
            return false;
        }
        if self.truth == CombatTruth::Ground {
            return true;
        }
        let Some(stack) = ItemEntity::item_stack_from_cluster_snapshot(&drop.stack) else {
            return false;
        };
        let world = source.get_entity().world.load_full();
        let position = source.get_entity().pos.load();
        let item = Arc::new(ItemEntity::new(
            Entity::from_uuid_with_id(
                drop.entity.local_id,
                replica_uuid(drop.entity),
                Arc::clone(&world),
                position,
                &EntityType::ITEM,
            ),
            stack,
        ));
        let entity: Arc<dyn EntityBase> = item;
        let inner = entity.get_entity();
        inner.cluster_owner.store(drop.entity.owner.0, Ordering::Relaxed);
        inner.cluster_origin_server.store(drop.entity.origin.0, Ordering::Relaxed);
        inner.cluster_origin_id.store(drop.entity.local_id, Ordering::Relaxed);
        let Some(captured_nbt) = read_nbt(&drop.entity_nbt) else {
            return false;
        };
        if captured_nbt.get_string("id") != Some("minecraft:item")
            || captured_nbt.get_uuid("UUID") != Some(replica_uuid(drop.entity))
            || captured_nbt.get_list("Pos").is_none()
            || captured_nbt.get_list("Motion").is_none()
            || captured_nbt.get_list("Rotation").is_none()
            || captured_nbt.get_compound("Item").is_none()
        {
            return false;
        }
        let spawn = EntitySpawn {
            entity: drop.entity,
            tick: self.tick,
            kind: EntityType::ITEM.id,
            pos: [position.x, position.y, position.z],
            yaw: inner.yaw.load(),
            pitch: inner.pitch.load(),
            state: EntitySpawnState::ItemDrop {
                stack: pumpkin_cluster::entities::ItemStackState {
                    item_id: drop.stack.item,
                    count: drop.stack.count,
                    nbt: drop.stack.nbt.clone(),
                },
                entity_nbt: drop.entity_nbt.clone(),
            },
        };
        if !apply_spawn_state(entity.as_ref(), &spawn.state) {
            return false;
        }
        entity.init_data_tracker();
        if !world.add_cluster_entity_transaction(Arc::clone(&entity)) {
            return false;
        }
        apply_item_spawn_snapshot(drop.entity, &spawn.state);
        if drop.entity.owner == self.local {
            let Some(ground) = materialize_ground_entity(drop.entity, &entity, &spawn) else {
                let _ = world.detach_cluster_entity_transaction(&entity);
                remove_item_spawn_snapshot(drop.entity);
                return false;
            };
            self.pairs.by_origin.insert(
                identity,
                EntityTruthPair {
                    reference: drop.entity,
                    target: EntityMutationTarget::Entity(drop.entity),
                    local: entity,
                    ground,
                    journal: EntityDualJournal::new(),
                    combat_journal: CombatDualJournal::new(),
                    ground_present: self.truth == CombatTruth::Ground,
                },
            );
        } else {
            self.replicas.insert(drop.entity, entity);
        }
        true
    }

    fn remove_drop(&mut self, drop: &AttackItemDrop) -> bool {
        let identity = (drop.entity.origin, drop.entity.local_id);
        if let Some(pair) = self.pairs.by_origin.get(&identity) {
            let world = pair.local.get_entity().world.load_full();
            let removed = world.detach_cluster_entity_transaction(&pair.local);
            if removed {
                let _ = self.pairs.by_origin.remove(&identity);
                remove_item_spawn_snapshot(drop.entity);
                return true;
            }
            return false;
        }
        if let Some(replica) = self.replicas.by_origin.get(&identity) {
            let world = replica.entity.get_entity().world.load_full();
            let removed = world.detach_cluster_entity_transaction(&replica.entity);
            if removed {
                let _ = self.replicas.by_origin.remove(&identity);
                remove_item_spawn_snapshot(drop.entity);
                return true;
            }
            return false;
        }
        false
    }
}

fn nonliving_outcome_with_lifecycle(
    outcome: NonLivingAttackOutcome,
    present: bool,
    spawned: Vec<AttackItemDrop>,
) -> NonLivingAttackOutcome {
    match outcome {
        NonLivingAttackOutcome::ItemFrame(mut outcome) => {
            outcome.removed_before = !present;
            outcome.removed_after = !present;
            outcome.drops = spawned.clone();
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::ItemFrame(outcome)
        }
        NonLivingAttackOutcome::ArmorStand(mut outcome) => {
            outcome.removed_before = !present;
            outcome.removed_after = !present;
            outcome.drops = spawned.clone();
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::ArmorStand(outcome)
        }
        NonLivingAttackOutcome::Vehicle(mut outcome) => {
            outcome.removed_before = !present;
            outcome.removed_after = !present;
            outcome.drops = spawned.clone();
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::Vehicle(outcome)
        }
        NonLivingAttackOutcome::Item(mut outcome) => {
            outcome.removed_before = !present;
            outcome.removed_after = !present;
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::Item(outcome)
        }
        NonLivingAttackOutcome::Interaction(mut outcome) => {
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::Interaction(outcome)
        }
        NonLivingAttackOutcome::Destroy(mut outcome) => {
            outcome.removed_before = !present;
            outcome.removed_after = !present;
            outcome.drops = spawned.clone();
            outcome.lifecycle.present_before = present;
            outcome.lifecycle.present_after = present;
            outcome.lifecycle.spawned = spawned;
            NonLivingAttackOutcome::Destroy(outcome)
        }
        NonLivingAttackOutcome::Noop(kind) => NonLivingAttackOutcome::Noop(kind),
    }
}

struct EntityDualCapturedAttackState<'a> {
    living: LivingCapturedAttackState<'a>,
    nonliving: Rc<RefCell<NonLivingLifecycleContext<'a>>>,
    truth: CombatTruth,
}

impl CapturedAttackState for EntityDualCapturedAttackState<'_> {
    fn captured_attack_snapshot(&self, attack: &CapturedAttack) -> Option<CapturedAttackSnapshot> {
        let mut snapshot = self.living.captured_attack_snapshot(attack)?;
        if attack.outcome != CapturedAttackOutcome::NoDamage {
            for target in attack.targets() {
                if !matches!(target.outcome, pumpkin_cluster::protocol::CapturedAttackTargetOutcome::NonLiving(_)) {
                    continue;
                }
                let mut nonliving = self.nonliving.borrow_mut();
                nonliving.truth = self.truth;
                let outcome = nonliving.captured_nonliving_attack_snapshot(target.target)?;
                snapshot.targets.push(CapturedAttackTargetSnapshot::NonLiving {
                    target: target.target,
                    outcome,
                });
            }
        }
        Some(snapshot)
    }

    fn apply_captured_attack(&mut self, attack: &CapturedAttack) -> bool {
        if attack.outcome == CapturedAttackOutcome::NoDamage {
            return self.living.apply_captured_attack(attack);
        }
        let Some(before) = self.living.captured_attack_snapshot(attack) else {
            return false;
        };
        if !self.living.apply_captured_attack(attack) {
            return false;
        }
        let mut nonliving = self.nonliving.borrow_mut();
        nonliving.truth = self.truth;
        if !apply_nonliving_captured_attack_lifecycle(&mut *nonliving, attack) {
            let _ = self.living.restore_captured_attack_snapshot(&before);
            return false;
        }
        true
    }

    fn restore_captured_attack_snapshot(&mut self, snapshot: &CapturedAttackSnapshot) -> bool {
        let living = CapturedAttackSnapshot {
            actor: snapshot.actor.clone(),
            targets: snapshot.targets.iter().filter(|target| matches!(target, CapturedAttackTargetSnapshot::Living { .. })).cloned().collect(),
        };
        let Some(living_current) = self.living.snapshot_for_restore(&living) else {
            return false;
        };
        let mut nonliving_current = Vec::new();
        {
            let mut nonliving = self.nonliving.borrow_mut();
            nonliving.truth = self.truth;
            for target in snapshot
                .targets
                .iter()
                .filter(|target| matches!(target, CapturedAttackTargetSnapshot::NonLiving { .. }))
            {
                let CapturedAttackTargetSnapshot::NonLiving { target, .. } = target else {
                    return false;
                };
                let Some(outcome) = nonliving.captured_nonliving_attack_snapshot(*target) else {
                    return false;
                };
                nonliving_current.push(CapturedAttackTargetSnapshot::NonLiving {
                    target: *target,
                    outcome,
                });
            }
        }
        if !self.living.restore_captured_attack_snapshot(&living) {
            return false;
        }
        let mut restored = Vec::new();
        for target in snapshot
            .targets
            .iter()
            .filter(|target| matches!(target, CapturedAttackTargetSnapshot::NonLiving { .. }))
        {
            let restored_target = {
                let mut nonliving = self.nonliving.borrow_mut();
                nonliving.truth = self.truth;
                restore_nonliving_captured_attack_lifecycle(&mut *nonliving, target)
            };
            if restored_target {
                restored.push(target.target());
                continue;
            }
            let mut nonliving = self.nonliving.borrow_mut();
            nonliving.truth = self.truth;
            for prior in restored.into_iter().rev() {
                if let Some(current) = nonliving_current
                    .iter()
                    .find(|current| current.target().same_identity(prior))
                {
                    let CapturedAttackTargetSnapshot::NonLiving { target, outcome } = current else {
                        continue;
                    };
                    let _ = nonliving.restore_nonliving_snapshot_exact(*target, outcome);
                }
            }
            let _ = self.living.restore_captured_attack_snapshot(&living_current);
            return false;
        }
        true
    }
}

impl NonLivingCapturedAttackState for NonLivingLifecycleContext<'_> {
    fn captured_nonliving_attack_snapshot(
        &mut self,
        target: EntityMutationTarget,
    ) -> Option<NonLivingAttackOutcome> {
        let outcome = self.entity(target)?.cluster_nonliving_attack_snapshot()?;
        self.snapshot_outcome(target, outcome)
    }

    fn apply_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        outcome: &NonLivingAttackOutcome,
    ) -> bool {
        let Some(lifecycle) = outcome.lifecycle() else {
            return self
                .entity(target)
                .is_some_and(|entity| entity.cluster_apply_nonliving_attack(outcome));
        };
        if self.present(target) != Some(lifecycle.present_before) {
            return false;
        }
        let Some(entity) = self.entity(target) else {
            return false;
        };
        let Some(before) = entity.cluster_nonliving_attack_snapshot() else {
            return false;
        };
        if !entity.cluster_apply_nonliving_attack(outcome) {
            return false;
        }
        if !self.set_present(target, lifecycle.present_after) {
            let _ = entity.cluster_restore_nonliving_attack(&before);
            return false;
        }
        let mut spawned = Vec::new();
        for drop in &lifecycle.spawned {
            if self.spawn_drop(&entity, drop) {
                spawned.push(drop.clone());
                continue;
            }
            for prior in spawned.iter().rev() {
                let _ = self.remove_drop(prior);
            }
            let _ = self.set_present(target, lifecycle.present_before);
            let _ = entity.cluster_restore_nonliving_attack(&before);
            return false;
        }
        self.record_observed(target, lifecycle.spawned.clone());
        true
    }

    fn restore_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        snapshot: &NonLivingAttackOutcome,
    ) -> bool {
        let Some(lifecycle) = snapshot.lifecycle() else {
            return self
                .entity(target)
                .is_some_and(|entity| entity.cluster_restore_nonliving_attack(snapshot));
        };
        let Some(entity) = self.entity(target) else {
            return false;
        };
        let Some(current) = entity.cluster_nonliving_attack_snapshot() else {
            return false;
        };
        let current_presence = self.present(target);
        if current_presence.is_none() {
            return false;
        }
        let observed = self.observed_for(target);
        let drops = if observed.is_empty() {
            self.expected_for(target)
        } else {
            observed
        };
        for drop in drops.iter().rev() {
            if !self.remove_drop(drop) {
                for prior in drops.iter().filter(|prior| !prior.entity.same_identity(drop.entity)) {
                    let _ = self.spawn_drop(&entity, prior);
                }
                return false;
            }
        }
        if !self.set_present(target, lifecycle.present_before) {
            for drop in &drops {
                let _ = self.spawn_drop(&entity, drop);
            }
            return false;
        }
        if entity.cluster_restore_nonliving_attack(snapshot) {
            return true;
        }
        let _ = self.set_present(target, current_presence.unwrap_or(false));
        let _ = entity.cluster_restore_nonliving_attack(&current);
        for drop in &drops {
            let _ = self.spawn_drop(&entity, drop);
        }
        false
    }
}

impl NonLivingLifecycleContext<'_> {
    fn restore_nonliving_snapshot_exact(
        &mut self,
        target: EntityMutationTarget,
        snapshot: &NonLivingAttackOutcome,
    ) -> bool {
        let Some(lifecycle) = snapshot.lifecycle() else {
            return self
                .entity(target)
                .is_some_and(|entity| entity.cluster_restore_nonliving_attack(snapshot));
        };
        let Some(entity) = self.entity(target) else {
            return false;
        };
        let Some(before) = entity.cluster_nonliving_attack_snapshot() else {
            return false;
        };
        let Some(before_present) = self.present(target) else {
            return false;
        };
        if !entity.cluster_restore_nonliving_attack(snapshot)
            || !self.set_present(target, lifecycle.present_after)
        {
            let _ = self.set_present(target, before_present);
            let _ = entity.cluster_restore_nonliving_attack(&before);
            return false;
        }
        let mut spawned = Vec::new();
        for drop in &lifecycle.spawned {
            let identity = (drop.entity.origin, drop.entity.local_id);
            if self.pairs.by_origin.contains_key(&identity)
                || self.replicas.by_origin.contains_key(&identity)
            {
                continue;
            }
            if self.spawn_drop(&entity, drop) {
                spawned.push(drop.clone());
                continue;
            }
            for prior in spawned.iter().rev() {
                let _ = self.remove_drop(prior);
            }
            let _ = self.set_present(target, before_present);
            let _ = entity.cluster_restore_nonliving_attack(&before);
            return false;
        }
        self.record_observed(target, lifecycle.spawned.clone());
        true
    }
}

fn apply_nonliving_captured_attack_lifecycle(
    state: &mut impl NonLivingCapturedAttackState,
    attack: &CapturedAttack,
) -> bool {
    let mut applied = Vec::new();
    for target in attack.targets() {
        let pumpkin_cluster::protocol::CapturedAttackTargetOutcome::NonLiving(outcome) = &target.outcome else {
            continue;
        };
        let Some(before) = state.captured_nonliving_attack_snapshot(target.target) else {
            for (prior_target, prior_before) in applied.into_iter().rev() {
                let _ = state.restore_captured_nonliving_attack(prior_target, &prior_before);
            }
            return false;
        };
        if before != nonliving_attack_before(outcome) || !state.apply_captured_nonliving_attack(target.target, outcome) {
            for (prior_target, prior_before) in applied.into_iter().rev() {
                let _ = state.restore_captured_nonliving_attack(prior_target, &prior_before);
            }
            return false;
        }
        applied.push((target.target, before));
    }
    true
}

fn restore_nonliving_captured_attack_lifecycle(
    state: &mut impl NonLivingCapturedAttackState,
    snapshot: &CapturedAttackTargetSnapshot,
) -> bool {
    let CapturedAttackTargetSnapshot::NonLiving { target, outcome } = snapshot else {
        return false;
    };
    state.restore_captured_nonliving_attack(*target, outcome)
}

fn attack_target_entity(
    pairs: &EntityTruthPairs,
    replicas: &EntityReplicas,
    target: EntityMutationTarget,
    ground: bool,
) -> Option<Arc<dyn EntityBase>> {
    pairs
        .by_origin
        .values()
        .find(|pair| pair.target.same_identity(target))
        .map(|pair| if ground { Arc::clone(&pair.ground) } else { Arc::clone(&pair.local) })
        .or_else(|| {
            replicas.by_origin.values().find_map(|replica| {
                truth_target(replica.reference, replica.entity.as_ref())
                    .is_some_and(|candidate| candidate.same_identity(target))
                    .then(|| Arc::clone(&replica.entity))
            })
        })
}

fn captured_attack_entities(
    pairs: &EntityTruthPairs,
    replicas: &EntityReplicas,
    attack: &CapturedAttack,
    ground: bool,
) -> Option<CapturedAttackEntities> {
    let actor = pairs
        .by_origin
        .get(&(attack.attacker.origin, attack.attacker.local_id))
        .map(|pair| if ground { Arc::clone(&pair.ground) } else { Arc::clone(&pair.local) })
        .or_else(|| replicas.by_origin.get(&(attack.attacker.origin, attack.attacker.local_id)).map(|replica| Arc::clone(&replica.entity)))?;
    let reference = pairs
        .by_origin
        .get(&(attack.attacker.origin, attack.attacker.local_id))
        .map(|pair| pair.reference)
        .or_else(|| replicas.by_origin.get(&(attack.attacker.origin, attack.attacker.local_id)).map(|replica| replica.reference))?;
    if !attack.attacker_matches_resolved_actor_entity(attack.actor, reference) || actor.get_player().is_none() {
        return None;
    }
    let mut targets: Vec<(EntityMutationTarget, Arc<dyn EntityBase>)> =
        Vec::with_capacity(1 + attack.sweeping.len());
    for target in attack.targets() {
        if targets.iter().any(|(known, _)| known.same_identity(target.target)) {
            return None;
        }
        targets.push((target.target, attack_target_entity(pairs, replicas, target.target, ground)?));
    }
    Some(CapturedAttackEntities { actor, targets })
}

fn captured_attack_entities_for(
    pairs: &EntityTruthPairs,
    replicas: &EntityReplicas,
    attacks: &[CapturedAttack],
    ground: bool,
) -> Option<CapturedAttackEntities> {
    let first = attacks.first()?;
    let mut entities = captured_attack_entities(pairs, replicas, first, ground)?;
    for attack in attacks.iter().skip(1) {
        if !attack.attacker.same_identity(first.attacker)
            || !attack.attacker_matches_resolved_actor_entity(attack.actor, first.attacker)
        {
            return None;
        }
        for target in attack.targets() {
            if entities.targets.iter().any(|(known, _)| known.same_identity(target.target)) {
                continue;
            }
            entities.targets.push((
                target.target,
                attack_target_entity(pairs, replicas, target.target, ground)?,
            ));
        }
    }
    Some(entities)
}

fn nonliving_lifecycle_context<'a>(
    pairs: &'a mut EntityTruthPairs,
    replicas: &'a mut EntityReplicas,
    local: ServerId,
    truth: CombatTruth,
    entities: &CapturedAttackEntities,
    ground_entities: Option<&CapturedAttackEntities>,
    attacks: &[CapturedAttack],
) -> Option<Rc<RefCell<NonLivingLifecycleContext<'a>>>> {
    let mut local_bindings = Vec::new();
    let mut ground_bindings = Vec::new();
    let mut expected_drops = Vec::new();
    for attack in attacks {
        for target in attack.targets() {
            let pumpkin_cluster::protocol::CapturedAttackTargetOutcome::NonLiving(outcome) = &target.outcome else {
                continue;
            };
            let (_, entity) = entities
                .targets
                .iter()
                .find(|(known, _)| known.same_identity(target.target))?;
            if !local_bindings
                .iter()
                .any(|(known, _): &(EntityMutationTarget, Arc<dyn EntityBase>)| known.same_identity(target.target))
            {
                local_bindings.push((target.target, Arc::clone(entity)));
            }
            if let Some(ground_entities) = ground_entities {
                let (_, entity) = ground_entities
                    .targets
                    .iter()
                    .find(|(known, _)| known.same_identity(target.target))?;
                if !ground_bindings
                    .iter()
                    .any(|(known, _): &(EntityMutationTarget, Arc<dyn EntityBase>)| known.same_identity(target.target))
                {
                    ground_bindings.push((target.target, Arc::clone(entity)));
                }
            }
            if let Some(lifecycle) = outcome.lifecycle() {
                expected_drops.push((target.target, lifecycle.spawned.clone()));
            }
        }
    }
    Some(Rc::new(RefCell::new(NonLivingLifecycleContext {
        pairs,
        replicas,
        local,
        truth,
        tick: attacks.first()?.tick,
        local_bindings: local_bindings.clone(),
        ground_bindings: if ground_bindings.is_empty() { local_bindings } else { ground_bindings },
        expected_drops,
        observed_drops: Vec::new(),
    })))
}

fn captured_attack_state<'a>(
    entities: &'a CapturedAttackEntities,
    no_targets: bool,
    context: Rc<RefCell<NonLivingLifecycleContext<'a>>>,
    attacks: &[CapturedAttack],
    truth: CombatTruth,
) -> Option<EntityDualCapturedAttackState<'a>> {
    let actor = entities.actor.get_player()?;
    if no_targets {
        return Some(EntityDualCapturedAttackState {
            living: LivingCapturedAttackState::new(actor, Vec::new()),
            nonliving: context,
            truth,
        });
    }
    let mut living_targets = Vec::with_capacity(entities.targets.len());
    for attack in attacks {
        for target in attack.targets() {
            let (_, entity) = entities
                .targets
                .iter()
                .find(|(known, _)| known.same_identity(target.target))?;
            if matches!(target.outcome, pumpkin_cluster::protocol::CapturedAttackTargetOutcome::Living(_)) {
                let living = entity.get_living_entity()?;
                if !living_targets
                    .iter()
                    .any(|known: &CapturedAttackTargetBinding<'_>| known.target.same_identity(target.target))
                {
                    living_targets.push(CapturedAttackTargetBinding {
                        target: target.target,
                        living,
                    });
                }
            }
        }
    }
    Some(EntityDualCapturedAttackState {
        living: LivingCapturedAttackState::new(actor, living_targets),
        nonliving: context,
        truth,
    })
}

fn captured_attack_is_living(attack: &CapturedAttack) -> bool {
    attack
        .targets()
        .all(|target| matches!(target.outcome, CapturedAttackTargetOutcome::Living(_)))
}

fn owned_attack_entity(
    pairs: &EntityTruthPairs,
    target: EntityMutationTarget,
    ground: bool,
) -> Option<Arc<dyn EntityBase>> {
    pairs
        .by_origin
        .values()
        .find(|pair| pair.target.same_identity(target))
        .map(|pair| if ground { Arc::clone(&pair.ground) } else { Arc::clone(&pair.local) })
}

fn holder_attack_entity(
    replicas: &EntityReplicas,
    target: EntityMutationTarget,
) -> Option<Arc<dyn EntityBase>> {
    replicas.by_origin.values().find_map(|replica| {
        truth_target(replica.reference, replica.entity.as_ref())
            .is_some_and(|known| known.same_identity(target))
            .then(|| Arc::clone(&replica.entity))
    })
}

fn apply_living_attack_targets(
    attack: &CapturedAttack,
    attacker_id: i32,
    mut resolve: impl FnMut(EntityMutationTarget) -> Option<Arc<dyn EntityBase>>,
) -> bool {
    if attack.outcome == CapturedAttackOutcome::NoDamage {
        return attack.targets().all(|target| target.outcome.is_noop());
    }
    let mut changes = Vec::new();
    for target in attack.targets() {
        let CapturedAttackTargetOutcome::Living(_) = &target.outcome else {
            return false;
        };
        if changes
            .iter()
            .any(|(known, _, _): &(EntityMutationTarget, Arc<dyn EntityBase>, CapturedAttackTargetSnapshot)| {
                known.same_identity(target.target)
            })
        {
            return false;
        }
        let Some(entity) = resolve(target.target) else {
            continue;
        };
        let snapshot = {
            let Some(living) = entity.get_living_entity() else {
                return false;
            };
            if !living.cluster_captured_attack_target_precondition(target) {
                return false;
            }
            living.cluster_captured_attack_target_snapshot(target.target)
        };
        changes.push((
            target.target,
            entity,
            snapshot,
        ));
    }
    let mut applied = 0;
    for (target, entity, _) in &changes {
        let Some(living) = entity.get_living_entity() else {
            return false;
        };
        let Some(captured) = attack.targets().find(|captured| captured.target.same_identity(*target)) else {
            return false;
        };
        if !living.cluster_apply_captured_attack_target(captured, attacker_id, i64::from(attack.tick.0), true) {
            for (_, prior, snapshot) in changes[..applied].iter().rev() {
                if let Some(living) = prior.get_living_entity() {
                    let _ = living.cluster_restore_captured_attack_target(snapshot);
                }
            }
            return false;
        }
        applied += 1;
    }
    true
}

fn apply_owned_attacker_ground(pairs: &EntityTruthPairs, attack: &CapturedAttack) -> bool {
    let Some(entity) = pairs
        .by_origin
        .get(&(attack.attacker.origin, attack.attacker.local_id))
        .map(|pair| Arc::clone(&pair.ground))
    else {
        return true;
    };
    let Some(player) = entity.get_player() else {
        return false;
    };
    player.cluster_captured_attack_actor_precondition(&attack.attacker_effects)
        && player.cluster_apply_captured_attack_actor(&attack.attacker_effects)
}

fn owned_attacker_ground_ready(pairs: &EntityTruthPairs, attack: &CapturedAttack) -> bool {
    let Some(entity) = pairs
        .by_origin
        .get(&(attack.attacker.origin, attack.attacker.local_id))
        .map(|pair| Arc::clone(&pair.ground))
    else {
        return true;
    };
    entity
        .get_player()
        .is_some_and(|player| player.cluster_captured_attack_actor_precondition(&attack.attacker_effects))
}

fn promote_living_captured_attacks(
    pairs: &mut EntityTruthPairs,
    replicas: &mut EntityReplicas,
    local: ServerId,
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: &[CapturedAttack],
) {
    let mut ordered: Vec<_> = accepted
        .iter()
        .filter(|attack| attack.tick == tick && captured_attack_is_living(attack))
        .cloned()
        .collect();
    pumpkin_cluster::combat::sort_captured_attacks(cluster_seed, tick, &mut ordered);
    let staged_truth: Vec<_> = pairs
        .by_origin
        .values()
        .flat_map(|pair| pair.combat_journal.pending_matching(tick, captured_attack_is_living))
        .collect();
    for staged in staged_truth {
        if !ordered.iter().any(|accepted| accepted.same_identity(&staged)) {
            undo_truth_combat(pairs, replicas, local, staged);
        }
    }
    let staged_replicas: Vec<_> = replicas
        .by_origin
        .values()
        .flat_map(|replica| replica.combat_journal.pending_matching(tick, captured_attack_is_living))
        .collect();
    for staged in staged_replicas {
        if !ordered.iter().any(|accepted| accepted.same_identity(&staged)) {
            undo_replica_combat(pairs, replicas, local, staged);
        }
    }
    for attack in &ordered {
        let local_actor = pairs
            .by_origin
            .contains_key(&(attack.attacker.origin, attack.attacker.local_id));
        let Some(attacker) = pairs
            .by_origin
            .get(&(attack.attacker.origin, attack.attacker.local_id))
            .map(|pair| Arc::clone(&pair.local))
            .or_else(|| replicas.by_origin.get(&(attack.attacker.origin, attack.attacker.local_id)).map(|replica| Arc::clone(&replica.entity)))
        else {
            note_dropped("captured-attack-actor");
            continue;
        };
        let attacker_id = attacker.get_entity().entity_id;
        if !owned_attacker_ground_ready(pairs, attack)
            || !apply_living_attack_targets(attack, attacker_id, |target| owned_attack_entity(pairs, target, true))
        {
            if local_actor {
                undo_truth_combat(pairs, replicas, local, attack.clone());
            }
            note_dropped("captured-attack-ground");
            continue;
        }
        if !local_actor
            && (!apply_living_attack_targets(attack, attacker_id, |target| owned_attack_entity(pairs, target, false))
                || !apply_living_attack_targets(attack, attacker_id, |target| holder_attack_entity(replicas, target)))
        {
            note_dropped("captured-attack-local");
            continue;
        }
        if !apply_owned_attacker_ground(pairs, attack) {
            if local_actor {
                undo_truth_combat(pairs, replicas, local, attack.clone());
            }
            note_dropped("captured-attack-actor");
            continue;
        }
        if local_actor {
            if let Some(pair) = pairs.by_origin.get_mut(&(attack.attacker.origin, attack.attacker.local_id)) {
                let _ = pair.combat_journal.take_pending_matching(tick, |pending| pending.same_identity(attack));
            }
        }
    }
}

fn stage_truth_combat(pairs: &mut EntityTruthPairs, replicas: &mut EntityReplicas, local: ServerId, attack: CapturedAttack) {
    let Some(entities) = captured_attack_entities(pairs, replicas, &attack, false) else {
        note_dropped("captured-attack-state");
        return;
    };
    let identity = (attack.attacker.origin, attack.attacker.local_id);
    let Some(pair) = pairs.by_origin.get_mut(&identity) else {
        note_dropped("captured-attack-pair");
        return;
    };
    let mut journal = std::mem::replace(&mut pair.combat_journal, CombatDualJournal::new());
    let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Local, &entities, None, std::slice::from_ref(&attack)) else {
        pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        note_dropped("captured-attack-local");
        return;
    };
    let verdict = {
        let Some(mut state) = captured_attack_state(&entities, attack.outcome == CapturedAttackOutcome::NoDamage, context, std::slice::from_ref(&attack), CombatTruth::Local) else {
            pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            note_dropped("captured-attack-local");
            return;
        };
        journal.stage_local_optimistic(&mut state, attack)
    };
    pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
    if matches!(verdict, CombatVerdict::Rejected) {
        note_dropped("captured-attack-stage");
    }
}

fn stage_replica_combat(pairs: &mut EntityTruthPairs, replicas: &mut EntityReplicas, local: ServerId, attack: CapturedAttack) {
    let Some(entities) = captured_attack_entities(pairs, replicas, &attack, false) else {
        note_dropped("captured-attack-state");
        return;
    };
    let identity = (attack.attacker.origin, attack.attacker.local_id);
    let Some(replica) = replicas.by_origin.get_mut(&identity) else {
        note_dropped("captured-attack-replica");
        return;
    };
    let mut journal = std::mem::replace(&mut replica.combat_journal, CombatDualJournal::new());
    let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Holder, &entities, None, std::slice::from_ref(&attack)) else {
        replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        note_dropped("captured-attack-local");
        return;
    };
    let verdict = {
        let Some(mut state) = captured_attack_state(&entities, attack.outcome == CapturedAttackOutcome::NoDamage, context, std::slice::from_ref(&attack), CombatTruth::Holder) else {
            replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            note_dropped("captured-attack-local");
            return;
        };
        journal.stage_local_optimistic(&mut state, attack)
    };
    replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
    if matches!(verdict, CombatVerdict::Rejected) {
        note_dropped("captured-attack-stage");
    }
}

fn undo_truth_combat(pairs: &mut EntityTruthPairs, replicas: &mut EntityReplicas, local: ServerId, attack: CapturedAttack) {
    let Some(entities) = captured_attack_entities(pairs, replicas, &attack, false) else {
        note_dropped("captured-attack-state");
        return;
    };
    let identity = (attack.attacker.origin, attack.attacker.local_id);
    let Some(pair) = pairs.by_origin.get_mut(&identity) else {
        note_dropped("captured-attack-pair");
        return;
    };
    let mut journal = std::mem::replace(&mut pair.combat_journal, CombatDualJournal::new());
    let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Local, &entities, None, std::slice::from_ref(&attack)) else {
        pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        note_dropped("captured-attack-local");
        return;
    };
    let undone = {
        let Some(mut state) = captured_attack_state(&entities, attack.outcome == CapturedAttackOutcome::NoDamage, context, std::slice::from_ref(&attack), CombatTruth::Local) else {
            pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            note_dropped("captured-attack-local");
            return;
        };
        journal.undo_local_loser(&mut state, attack)
    };
    pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
    if !undone {
        note_dropped("captured-attack-undo");
    }
}

fn undo_replica_combat(pairs: &mut EntityTruthPairs, replicas: &mut EntityReplicas, local: ServerId, attack: CapturedAttack) {
    let Some(entities) = captured_attack_entities(pairs, replicas, &attack, false) else {
        note_dropped("captured-attack-state");
        return;
    };
    let identity = (attack.attacker.origin, attack.attacker.local_id);
    let Some(replica) = replicas.by_origin.get_mut(&identity) else {
        note_dropped("captured-attack-replica");
        return;
    };
    let mut journal = std::mem::replace(&mut replica.combat_journal, CombatDualJournal::new());
    let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Holder, &entities, None, std::slice::from_ref(&attack)) else {
        replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        note_dropped("captured-attack-local");
        return;
    };
    let undone = {
        let Some(mut state) = captured_attack_state(&entities, attack.outcome == CapturedAttackOutcome::NoDamage, context, std::slice::from_ref(&attack), CombatTruth::Holder) else {
            replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            note_dropped("captured-attack-local");
            return;
        };
        journal.undo_local_loser(&mut state, attack)
    };
    replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
    if !undone {
        note_dropped("captured-attack-undo");
    }
}

fn promote_truth_combat(
    pairs: &mut EntityTruthPairs,
    replicas: &mut EntityReplicas,
    local: ServerId,
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: &[CapturedAttack],
) {
    let identities: Vec<_> = pairs.by_origin.keys().copied().collect();
    for identity in identities {
        let Some(reference) = pairs.by_origin.get(&identity).map(|pair| pair.reference) else { continue };
        let relevant: Vec<_> = accepted
            .iter()
            .filter(|attack| attack.attacker.same_identity(reference))
            .cloned()
            .collect();
        if relevant.is_empty() {
            continue;
        }
        let Some(local_entities) = captured_attack_entities_for(pairs, replicas, &relevant, false) else { continue };
        let Some(ground_entities) = captured_attack_entities_for(pairs, replicas, &relevant, true) else { continue };
        let no_targets = relevant.iter().all(|attack| attack.outcome == CapturedAttackOutcome::NoDamage);
        let Some(pair) = pairs.by_origin.get_mut(&identity) else { continue };
        let mut journal = std::mem::replace(&mut pair.combat_journal, CombatDualJournal::new());
        let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Local, &local_entities, Some(&ground_entities), &relevant) else {
            pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            continue;
        };
        let promoted = {
            if let (Some(mut local_state), Some(mut ground_state)) = (
                captured_attack_state(&local_entities, no_targets, Rc::clone(&context), &relevant, CombatTruth::Local),
                captured_attack_state(&ground_entities, no_targets, context, &relevant, CombatTruth::Ground),
            ) {
                let _ = journal.promote_globally_accepted(
                    &mut local_state,
                    &mut ground_state,
                    cluster_seed,
                    tick,
                    &relevant,
                );
                true
            } else {
                false
            }
        };
        pairs.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        if !promoted {
            note_dropped("captured-attack-promote");
        }
    }
}

fn promote_replica_combat(
    pairs: &mut EntityTruthPairs,
    replicas: &mut EntityReplicas,
    local: ServerId,
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: &[CapturedAttack],
) {
    let identities: Vec<_> = replicas.by_origin.keys().copied().collect();
    for identity in identities {
        let Some(reference) = replicas.by_origin.get(&identity).map(|replica| replica.reference) else { continue };
        let relevant: Vec<_> = accepted
            .iter()
            .filter(|attack| attack.attacker.same_identity(reference))
            .cloned()
            .collect();
        if relevant.is_empty() {
            continue;
        }
        let Some(entities) = captured_attack_entities_for(pairs, replicas, &relevant, false) else { continue };
        let no_targets = relevant.iter().all(|attack| attack.outcome == CapturedAttackOutcome::NoDamage);
        let Some(replica) = replicas.by_origin.get_mut(&identity) else { continue };
        let mut journal = std::mem::replace(&mut replica.combat_journal, CombatDualJournal::new());
        let Some(context) = nonliving_lifecycle_context(pairs, replicas, local, CombatTruth::Holder, &entities, None, &relevant) else {
            replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
            continue;
        };
        let resolution = {
            let Some(mut holder) = captured_attack_state(&entities, no_targets, context, &relevant, CombatTruth::Holder) else {
                replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
                continue;
            };
            journal.resolve_globally_accepted_single_truth(&mut holder, cluster_seed, tick, &relevant)
        };
        replicas.by_origin.get_mut(&identity).unwrap().combat_journal = journal;
        if resolution.local_rejected != 0 {
            note_dropped("captured-attack-replica");
        }
    }
}

fn promote_truth_mutations(
    pairs: &mut EntityTruthPairs,
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: &[EntityMutationUpdate],
) {
    for pair in pairs.by_origin.values_mut() {
        let Some(local) = pair.local.get_living_entity() else {
            continue;
        };
        let Some(ground) = pair.ground.get_living_entity() else {
            continue;
        };
        let mut local_effects = PairEffects {
            living: local,
            target: pair.target,
        };
        let mut ground_effects = PairEffects {
            living: ground,
            target: pair.target,
        };
        let relevant: Vec<_> = accepted
            .iter()
            .copied()
            .filter(|update| entity_target_matches(pair.target, update.target))
            .collect();
        let _ = pair.journal.promote_globally_accepted(
            &mut local_effects,
            &mut ground_effects,
            cluster_seed,
            tick,
            &relevant,
        );
    }
}

fn replica_for_target_mut(
    replicas: &mut EntityReplicas,
    target: EntityMutationTarget,
) -> Option<&mut EntityReplica> {
    match target {
        EntityMutationTarget::Entity(reference) => replicas
            .by_origin
            .get_mut(&(reference.origin, reference.local_id)),
        EntityMutationTarget::Player { gid, .. } => replicas.by_origin.values_mut().find(|replica| {
            replica
                .entity
                .get_player()
                .and_then(Player::cluster_gid)
                .is_some_and(|candidate| candidate == gid)
        }),
    }
}

fn apply_accepted_replica_mutations(
    replicas: &mut EntityReplicas,
    cluster_seed: u64,
    tick: pumpkin_cluster::time::TickStamp,
    accepted: &[EntityMutationUpdate],
) {
    for replica in replicas.by_origin.values_mut() {
        let Some(living) = replica.entity.get_living_entity() else {
            continue;
        };
        let target = truth_target(replica.reference, replica.entity.as_ref())
            .unwrap_or(EntityMutationTarget::Entity(replica.reference));
        let relevant: Vec<_> = accepted
            .iter()
            .copied()
            .filter(|update| target.same_identity(update.target))
            .collect();
        let mut effects = living.cluster_effect_mutation(target);
        let resolution = replica.journal.resolve_globally_accepted_single_truth(
            &mut effects,
            cluster_seed,
            tick,
            &relevant,
        );
        if resolution.local_rejected != 0 {
            note_dropped("replica-mutation");
        }
    }
}

fn apply_pos_update(
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityPosUpdate,
) -> bool {
    if !accepted(peer, local, &update.entity) {
        note_dropped("owner-or-holder");
        return false;
    }
    let Some(replica) = replicas.get(update.entity) else {
        note_dropped("unknown");
        return false;
    };
    let inner = replica.entity.get_entity();
    inner.set_pos(Vector3::new(update.pos[0], update.pos[1], update.pos[2]));
    inner.velocity.store(Vector3::new(update.vel[0], update.vel[1], update.vel[2]));
    inner.yaw.store(update.yaw);
    inner.pitch.store(update.pitch);
    replicas.update_reference(update.entity);
    note_applied();
    true
}

fn insert_spawn(
    server: &Server,
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntitySpawn,
) -> bool {
    if !accepted(peer, local, &update.entity) {
        note_dropped("owner-or-holder");
        return false;
    }
    insert_spawn_materialized(server, replicas, update)
}

fn insert_spawn_materialized(
    server: &Server,
    replicas: &mut EntityReplicas,
    update: &EntitySpawn,
) -> bool {
    let Some(entity_type) = EntityType::from_raw(update.kind) else {
        note_dropped("kind");
        return false;
    };
    if entity_type.id == EntityType::PLAYER.id {
        return insert_player_spawn(server, replicas, update);
    }
    let Some(world) = replica_world(server, &update.entity) else {
        note_dropped("world");
        return false;
    };
    if let Some(existing) = replicas.get(update.entity) {
        let inner = existing.entity.get_entity();
        inner.cluster_owner.store(update.entity.owner.0, Ordering::Relaxed);
        inner
            .cluster_origin_server
            .store(update.entity.origin.0, Ordering::Relaxed);
        inner
            .cluster_origin_id
            .store(update.entity.local_id, Ordering::Relaxed);
        inner.set_pos(Vector3::new(update.pos[0], update.pos[1], update.pos[2]));
        inner.yaw.store(update.yaw);
        inner.pitch.store(update.pitch);
        if !apply_spawn_state(existing.entity.as_ref(), &update.state) {
            note_dropped("state");
            return false;
        }
        apply_item_spawn_snapshot(update.entity, &update.state);
        replicas.update_reference(update.entity);
        note_applied();
        return true;
    }
    let entity = from_type(
        entity_type,
        Vector3::new(update.pos[0], update.pos[1], update.pos[2]),
        &world,
        replica_uuid(update.entity),
    );
    let inner = entity.get_entity();
    inner.cluster_owner.store(update.entity.owner.0, Ordering::Relaxed);
    inner
        .cluster_origin_server
        .store(update.entity.origin.0, Ordering::Relaxed);
    inner
        .cluster_origin_id
        .store(update.entity.local_id, Ordering::Relaxed);
    inner.yaw.store(update.yaw);
    inner.pitch.store(update.pitch);
    if !apply_spawn_state(entity.as_ref(), &update.state) {
        note_dropped("state");
        return false;
    }
    entity.init_data_tracker();
    world.add_entity_silent(entity.clone());
    apply_item_spawn_snapshot(update.entity, &update.state);
    replicas.insert(update.entity, entity);
    note_applied();
    true
}

fn materialize_boundary_handoff(
    server: &Server,
    pairs: &mut EntityTruthPairs,
    replicas: &mut EntityReplicas,
    local: ServerId,
    handoff: &EntityHandoff,
) -> bool {
    if !handoff.is_consistent()
        || handoff.successor != local
        || matches!(&handoff.spawn.state, EntitySpawnState::Player(_))
    {
        return false;
    }
    let reference = handoff.destination_ref();
    let chunk = Vector2::new(reference.chunk.x, reference.chunk.z);
    if !server
        .worlds
        .load()
        .iter()
        .any(|world| world.level.is_cluster_full(&chunk))
    {
        return false;
    }
    if pairs
        .by_origin
        .get(&(reference.origin, reference.local_id))
        .is_some_and(|pair| pair.reference == reference)
    {
        return true;
    }
    if let Some(replica) = replicas.get(reference) {
        let entity = Arc::clone(&replica.entity);
        let inner = entity.get_entity();
        inner.set_pos(Vector3::new(
            handoff.spawn.pos[0],
            handoff.spawn.pos[1],
            handoff.spawn.pos[2],
        ));
        inner.yaw.store(handoff.spawn.yaw);
        inner.pitch.store(handoff.spawn.pitch);
        inner.velocity.store(Vector3::new(
            handoff.velocity[0],
            handoff.velocity[1],
            handoff.velocity[2],
        ));
        if !apply_spawn_state(entity.as_ref(), &handoff.spawn.state) {
            return false;
        }
        let Some(ground) = materialize_ground_entity(reference, &entity, &handoff.destination_spawn())
        else {
            return false;
        };
        inner.cluster_owner.store(local.0, Ordering::Relaxed);
        inner
            .cluster_origin_server
            .store(reference.origin.0, Ordering::Relaxed);
        inner
            .cluster_origin_id
            .store(reference.local_id, Ordering::Relaxed);
        let _ = replicas.remove(reference);
        let target = EntityMutationTarget::Entity(reference);
        pairs.by_origin.insert(
            (reference.origin, reference.local_id),
            EntityTruthPair {
                reference,
                target,
                local: Arc::clone(&entity),
                ground,
                journal: EntityDualJournal::new(),
                combat_journal: CombatDualJournal::new(),
                ground_present: true,
            },
        );
        super::cluster_entity_emit::stage_spawn_arc_if_cluster(server, entity);
        return true;
    }
    let Some(entity_type) = EntityType::from_raw(handoff.spawn.kind) else {
        return false;
    };
    if entity_type.id == EntityType::PLAYER.id {
        return false;
    }
    let Some(world) = server
        .worlds
        .load()
        .iter()
        .find(|world| world.level.is_cluster_full(&chunk))
        .cloned()
    else {
        return false;
    };
    let entity = from_type(
        entity_type,
        Vector3::new(
            handoff.spawn.pos[0],
            handoff.spawn.pos[1],
            handoff.spawn.pos[2],
        ),
        &world,
        replica_uuid(reference),
    );
    let inner = entity.get_entity();
    inner.cluster_owner.store(local.0, Ordering::Relaxed);
    inner
        .cluster_origin_server
        .store(reference.origin.0, Ordering::Relaxed);
    inner
        .cluster_origin_id
        .store(reference.local_id, Ordering::Relaxed);
    inner.set_pos(Vector3::new(
        handoff.spawn.pos[0],
        handoff.spawn.pos[1],
        handoff.spawn.pos[2],
    ));
    inner.yaw.store(handoff.spawn.yaw);
    inner.pitch.store(handoff.spawn.pitch);
    inner.velocity.store(Vector3::new(
        handoff.velocity[0],
        handoff.velocity[1],
        handoff.velocity[2],
    ));
    if !apply_spawn_state(entity.as_ref(), &handoff.spawn.state) {
        return false;
    }
    let entity: Arc<dyn EntityBase> = entity;
    let Some(ground) = materialize_ground_entity(reference, &entity, &handoff.destination_spawn())
    else {
        return false;
    };
    entity.init_data_tracker();
    if !world.add_cluster_entity_transaction(Arc::clone(&entity)) {
        return false;
    }
    apply_item_spawn_snapshot(reference, &handoff.spawn.state);
    let target = EntityMutationTarget::Entity(reference);
    pairs.by_origin.insert(
        (reference.origin, reference.local_id),
        EntityTruthPair {
            reference,
            target,
            local: Arc::clone(&entity),
            ground,
            journal: EntityDualJournal::new(),
            combat_journal: CombatDualJournal::new(),
            ground_present: true,
        },
    );
    super::cluster_entity_emit::stage_spawn_arc_if_cluster(server, entity);
    true
}

fn insert_player_spawn(
    server: &Server,
    replicas: &mut EntityReplicas,
    update: &EntitySpawn,
) -> bool {
    let EntitySpawnState::Player(state) = &update.state else {
        note_dropped("player-state");
        return false;
    };
    if let Some(existing) = replicas.get(update.entity) {
        let Some(player) = existing.entity.get_player() else {
            note_dropped("player-replica");
            return false;
        };
        let entity = player.get_entity();
        entity.cluster_owner.store(update.entity.owner.0, Ordering::Relaxed);
        entity
            .cluster_origin_server
            .store(update.entity.origin.0, Ordering::Relaxed);
        entity
            .cluster_origin_id
            .store(update.entity.local_id, Ordering::Relaxed);
        entity.set_pos(Vector3::new(update.pos[0], update.pos[1], update.pos[2]));
        entity.yaw.store(update.yaw);
        entity.pitch.store(update.pitch);
        if !apply_player_state(player, state) {
            note_dropped("player-state");
            return false;
        }
        replicas.update_reference(update.entity);
        note_applied();
        return true;
    }
    let Some(world) = player_replica_world(server, state) else {
        note_dropped("world");
        return false;
    };
    let Some(player) = materialize_player_replica(&world, update.entity, update, state) else {
        note_dropped("player-materialize");
        return false;
    };
    replicas.insert(update.entity, player);
    note_applied();
    true
}

fn apply_handoff(
    server: &Server,
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    handoff: &EntityHandoff,
) -> bool {
    if peer != handoff.previous_owner
        || !handoff.is_consistent()
        || !handoff.applies_to(local)
        || !route_to_holders(handoff.spawn.entity.chunk, &super::cluster::chunk_holders)
            .contains(&local.0)
    {
        note_dropped("handoff");
        return false;
    }
    let spawn = handoff.destination_spawn();
    if !insert_spawn_materialized(server, replicas, &spawn) {
        return false;
    }
    let Some(replica) = replicas.get(spawn.entity) else {
        note_dropped("handoff-materialize");
        return false;
    };
    replica.entity.get_entity().velocity.store(Vector3::new(
        handoff.velocity[0],
        handoff.velocity[1],
        handoff.velocity[2],
    ));
    note_applied();
    true
}

fn remove_despawn(
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityDespawn,
) -> bool {
    if !accepted(peer, local, &update.entity) {
        note_dropped("owner-or-holder");
        return false;
    }
    let Some(replica) = replicas.remove(update.entity) else {
        note_dropped("unknown");
        return false;
    };
    remove_item_spawn_snapshot(update.entity);
    if let Some(player) = replica.entity.get_player() {
        let _ = player
            .get_entity()
            .world
            .load()
            .remove_player_replica(player);
    } else {
        replica.entity.get_entity().world.load().remove_entity(replica.entity.as_ref());
    }
    note_applied();
    true
}

fn apply_visual(
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityVisualUpdate,
) -> bool {
    if !accepted(peer, local, &update.entity) {
        note_dropped("owner-or-holder");
        return false;
    }
    let Some(replica) = replicas.get(update.entity) else {
        note_dropped("unknown");
        return false;
    };
    let entity = replica.entity.get_entity();
    entity.sneaking.store(update.flags & 1 != 0, Ordering::Relaxed);
    entity.sprinting.store(update.flags & 2 != 0, Ordering::Relaxed);
    entity.swimming.store(update.flags & 4 != 0, Ordering::Relaxed);
    entity.invisible.store(update.flags & 8 != 0, Ordering::Relaxed);
    entity.glowing.store(update.flags & 16 != 0, Ordering::Relaxed);
    entity.fall_flying.store(update.flags & 32 != 0, Ordering::Relaxed);
    note_applied();
    true
}

fn apply_transient(
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityTransientUpdate,
) -> bool {
    if !accepted(peer, local, &update.entity) || replicas.get(update.entity).is_none() {
        note_dropped("owner-holder-or-unknown");
        return false;
    }
    note_applied();
    true
}

fn apply_combat(
    replicas: &mut EntityReplicas,
    local: ServerId,
    peer: ServerId,
    update: &EntityCombatUpdate,
) -> bool {
    if !accepted(peer, local, &update.entity) || replicas.get(update.entity).is_none() {
        note_dropped("owner-holder-or-unknown");
        return false;
    }
    note_applied();
    true
}

fn apply_pos_bytes(
    replicas: &mut EntityReplicas,
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
    apply_pos_update(replicas, local, peer, &update)
}

fn apply_spawn_bytes(
    server: &Server,
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
    insert_spawn(server, replicas, local, peer, &tagged.update)
}

fn apply_despawn_bytes(
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
    remove_despawn(replicas, local, peer, &tagged.update)
}

fn apply_pos_stream_bytes(
    server: &Server,
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
        EntityPosStreamFrame::Spawn => apply_spawn_bytes(server, replicas, local, peer, bytes),
        EntityPosStreamFrame::Despawn => apply_despawn_bytes(replicas, local, peer, bytes),
        EntityPosStreamFrame::Datagram => {
            let datagram = match postcard::from_bytes::<EntityPosDatagram>(bytes) {
                Ok(datagram) if datagram.is_consistent() && !datagram.is_empty() => datagram,
                Ok(_) => {
                    note_dropped("entity-pos-envelope");
                    return false;
                }
                Err(error) => {
                    note_malformed_pos_frame(peer, bytes, &error);
                    return false;
                }
            };
            forward_entity_pos_datagram(peer, datagram.updates)
        }
        EntityPosStreamFrame::LegacyBarePos => apply_pos_bytes(replicas, local, peer, bytes),
    }
}

fn apply_parcel(server: &Server, replicas: &mut EntityReplicas, local: ServerId, parcel: &InboundParcel) {
    match parcel.header.kind {
        StreamKind::EntityPos => {
            apply_pos_stream_bytes(server, replicas, local, parcel.peer, &parcel.bytes);
        }
        StreamKind::EntityVisual => match postcard::from_bytes::<EntityVisualUpdate>(&parcel.bytes) {
            Ok(update) => { apply_visual(replicas, local, parcel.peer, &update); }
            Err(error) => { warn!(%error, "cluster entity visual decode failed"); note_dropped("decode"); }
        },
        StreamKind::EntityTransient => match postcard::from_bytes::<EntityTransientUpdate>(&parcel.bytes) {
            Ok(update) => { apply_transient(replicas, local, parcel.peer, &update); }
            Err(error) => { warn!(%error, "cluster entity transient decode failed"); note_dropped("decode"); }
        },
        StreamKind::EntityCombat => match postcard::from_bytes::<EntityCombatUpdate>(&parcel.bytes) {
            Ok(update) => { apply_combat(replicas, local, parcel.peer, &update); }
            Err(error) => { warn!(%error, "cluster entity combat decode failed"); note_dropped("decode"); }
        },
        _ => note_dropped("kind"),
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
        let (handoff_tx, mut handoff_rx) = mpsc::channel(64);
        let _ = ENTITY_HANDOFF_BRIDGE.set(handoff_tx);
        let (dual_tx, mut dual_rx) = mpsc::channel(256);
        let _ = ENTITY_BOUNDARY_MATERIALIZE_BRIDGE.set(dual_tx.clone());
        let _ = ENTITY_DUAL_BRIDGE.set(dual_tx);
        let mut replicas = EntityReplicas::default();
        let mut pairs = EntityTruthPairs::default();
        loop {
            tokio::select! {
                parcel = entity_rx.recv() => {
                    let Some(parcel) = parcel else { break };
                    ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                    apply_parcel(&task_server, &mut replicas, local, &parcel);
                }
                batch = pos_rx.recv() => {
                    let Some((peer, updates)) = batch else { break };
                    ENTITY_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
                    for update in &updates {
                        apply_pos_update(&mut replicas, local, peer, update);
                    }
                }
                handoff = handoff_rx.recv() => {
                    let Some((peer, handoffs)) = handoff else { break };
                    for handoff in &handoffs {
                        apply_handoff(&task_server, &mut replicas, local, peer, handoff);
                    }
                }
                dual = dual_rx.recv() => {
                    let Some(dual) = dual else { break };
                    match dual {
                        EntityDualInput::Register { reference, local: entity, spawn } => {
                            register_truth_pair(&mut pairs, local, reference, entity, spawn);
                        }
                        EntityDualInput::Stage(update) => {
                            if pair_for_update_mut(&mut pairs, update.target).is_some() {
                                stage_truth_mutation(&mut pairs, update);
                            } else {
                                stage_replica_mutation(&mut replicas, update);
                            }
                        }
                        EntityDualInput::Promote { cluster_seed, tick, accepted } => {
                            promote_truth_mutations(&mut pairs, cluster_seed, tick, &accepted);
                            apply_accepted_replica_mutations(
                                &mut replicas,
                                cluster_seed,
                                tick,
                                &accepted,
                            );
                        }
                        EntityDualInput::Undo(update) => {
                            if pair_for_update_mut(&mut pairs, update.target).is_some() {
                                undo_truth_mutation(&mut pairs, update);
                            } else {
                                undo_replica_mutation(&mut replicas, update);
                            }
                        }
                        EntityDualInput::SyncInfallible(update) => {
                            sync_truth_pair(&mut pairs, update);
                        }
                        EntityDualInput::StageCombat(attack) => {
                            if pairs.by_origin.contains_key(&(attack.attacker.origin, attack.attacker.local_id)) {
                                stage_truth_combat(&mut pairs, &mut replicas, local, attack);
                            } else {
                                stage_replica_combat(&mut pairs, &mut replicas, local, attack);
                            }
                        }
                        EntityDualInput::PromoteCombat { cluster_seed, tick, accepted } => {
                            promote_living_captured_attacks(
                                &mut pairs,
                                &mut replicas,
                                local,
                                cluster_seed,
                                tick,
                                &accepted,
                            );
                            let nonliving: Vec<_> = accepted
                                .iter()
                                .filter(|attack| !captured_attack_is_living(attack))
                                .cloned()
                                .collect();
                            promote_truth_combat(
                                &mut pairs,
                                &mut replicas,
                                local,
                                cluster_seed,
                                tick,
                                &nonliving,
                            );
                            promote_replica_combat(
                                &mut pairs,
                                &mut replicas,
                                local,
                                cluster_seed,
                                tick,
                                &nonliving,
                            );
                        }
                        EntityDualInput::UndoCombat(attack) => {
                            if pairs.by_origin.contains_key(&(attack.attacker.origin, attack.attacker.local_id)) {
                                undo_truth_combat(&mut pairs, &mut replicas, local, attack);
                            } else {
                                undo_replica_combat(&mut pairs, &mut replicas, local, attack);
                            }
                        }
                        EntityDualInput::ApplyTransactionalExplosion { world, update } => {
                            if !apply_transactional_explosion_entities_now(
                                &mut pairs,
                                &mut replicas,
                                local,
                                &world,
                                &update,
                            ) {
                                note_dropped("transactional-explosion");
                            }
                        }
                        EntityDualInput::BoundaryMaterialize { handoff, events } => {
                            let accepted = materialize_boundary_handoff(
                                &task_server,
                                &mut pairs,
                                &mut replicas,
                                local,
                                &handoff,
                            );
                            let _ = events.try_send(BoundaryMaterialization { handoff, accepted });
                        }
                        EntityDualInput::Remove(reference) => remove_truth_pair(&mut pairs, reference),
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod nonliving_lifecycle_tests {
    use super::*;
    use pumpkin_config::world::LevelConfig;
    use pumpkin_data::{dimension::Dimension, item::Item, item_stack::ItemStack};
    use pumpkin_util::world_seed::Seed;
    use pumpkin_world::{level::Level, world_info::LevelData};

    fn world() -> (Arc<crate::world::World>, tempfile::TempDir) {
        let folder = tempfile::tempdir().unwrap();
        let level = Level::from_root_folder(
            &LevelConfig::default(),
            folder.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
        );
        let world = Arc::new(crate::world::World::load(
            level,
            Arc::new(ArcSwap::from_pointee(LevelData::default(Seed(0)))),
            Dimension::OVERWORLD,
            Arc::new(crate::block::registry::BlockRegistry::default()),
            std::sync::Weak::<Server>::new(),
        ));
        (world, folder)
    }

    fn item(world: Arc<crate::world::World>) -> Arc<dyn EntityBase> {
        let entity: Arc<dyn EntityBase> = Arc::new(ItemEntity::new(
            Entity::new(world, Vector3::new(0.0, 64.0, 0.0), &EntityType::ITEM),
            ItemStack::new(1, &Item::STONE),
        ));
        entity.init_data_tracker();
        entity
    }

    fn reference(local_id: i32, owner: ServerId) -> EntityRef {
        EntityRef {
            origin: owner,
            owner,
            local_id,
            chunk: ChunkAddr { x: 0, z: 0 },
        }
    }

    fn context<'a>(
        pairs: &'a mut EntityTruthPairs,
        replicas: &'a mut EntityReplicas,
        local: ServerId,
        target: EntityRef,
        entity: Arc<dyn EntityBase>,
    ) -> NonLivingLifecycleContext<'a> {
        NonLivingLifecycleContext {
            pairs,
            replicas,
            local,
            truth: CombatTruth::Local,
            tick: pumpkin_cluster::time::TickStamp(9),
            local_bindings: vec![(EntityMutationTarget::Entity(target), Arc::clone(&entity))],
            ground_bindings: vec![(EntityMutationTarget::Entity(target), entity)],
            expected_drops: Vec::new(),
            observed_drops: Vec::new(),
        }
    }

    fn actor_effects() -> pumpkin_cluster::protocol::CapturedAttackActorEffects {
        pumpkin_cluster::protocol::CapturedAttackActorEffects {
            cooldown: pumpkin_cluster::protocol::AttackCooldownTransition { before: 0, after: 0 },
            velocity: pumpkin_cluster::protocol::AttackVelocityTransition {
                before_bits: [0; 3],
                after_bits: [0; 3],
            },
            last_attacking_id_before: 0,
            last_attacking_id_after: 0,
            last_attack_tick_before: 0,
            last_attack_tick_after: 0,
            fall_distance_before_bits: 0,
            fall_distance_after_bits: 0,
            exhaustion_before_bits: 0,
            exhaustion_after_bits: 0,
            item: None,
            stats: Vec::new(),
            advancements: Vec::new(),
        }
    }

    fn item_drop_outcome(
        entity: &Arc<dyn EntityBase>,
        drops: Vec<AttackItemDrop>,
    ) -> NonLivingAttackOutcome {
        let NonLivingAttackOutcome::Item(mut outcome) = entity.cluster_nonliving_attack_snapshot().unwrap() else {
            unreachable!();
        };
        outcome.lifecycle = pumpkin_cluster::protocol::NonLivingAttackLifecycle {
            present_before: true,
            present_after: true,
            spawned: drops,
        };
        NonLivingAttackOutcome::Item(outcome)
    }

    #[test]
    fn lifecycle_owner_pair_holder_replica_and_exact_arc_reinsertion() {
        let (world, _folder) = world();
        let local = ServerId(1);
        let source_ref = reference(401, local);
        let source = item(Arc::clone(&world));
        let source_inner = source.get_entity();
        source_inner.cluster_owner.store(local.0, Ordering::Relaxed);
        source_inner.cluster_origin_server.store(local.0, Ordering::Relaxed);
        source_inner.cluster_origin_id.store(source_ref.local_id, Ordering::Relaxed);
        assert!(world.add_cluster_entity_transaction(Arc::clone(&source)));
        let mut pairs = EntityTruthPairs::default();
        pairs.by_origin.insert(
            (source_ref.origin, source_ref.local_id),
            EntityTruthPair {
                reference: source_ref,
                target: EntityMutationTarget::Entity(source_ref),
                local: Arc::clone(&source),
                ground: Arc::clone(&source),
                journal: EntityDualJournal::new(),
                combat_journal: CombatDualJournal::new(),
                ground_present: true,
            },
        );
        let stack = ItemEntity::cluster_stack_from_item_stack(&ItemStack::new(1, &Item::STONE));
        let owner_drop = crate::entity::nonliving_attack::capture_attack_item_drop(
            source.as_ref(),
            reference(402, local),
            stack.clone(),
        )
        .unwrap();
        let mut replicas = EntityReplicas::default();
        {
            let mut lifecycle = context(
                &mut pairs,
                &mut replicas,
                local,
                source_ref,
                Arc::clone(&source),
            );
            assert!(lifecycle.spawn_drop(&source, &owner_drop));
            assert!(lifecycle.set_present(EntityMutationTarget::Entity(source_ref), false));
            assert!(lifecycle.set_present(EntityMutationTarget::Entity(source_ref), true));
        }
        let owner_pair = pairs
            .by_origin
            .get(&(owner_drop.entity.origin, owner_drop.entity.local_id))
            .unwrap();
        assert_eq!(owner_pair.local.get_entity().entity_id, owner_drop.entity.local_id);
        assert!(!owner_pair.ground_present);
        assert!(world.entities.load().iter().any(|entity| Arc::ptr_eq(entity, &source)));
        {
            let mut lifecycle = context(
                &mut pairs,
                &mut replicas,
                local,
                source_ref,
                Arc::clone(&source),
            );
            lifecycle.truth = CombatTruth::Ground;
            assert!(lifecycle.spawn_drop(&source, &owner_drop));
        }
        assert!(pairs
            .by_origin
            .get(&(owner_drop.entity.origin, owner_drop.entity.local_id))
            .unwrap()
            .ground_present);
        let holder_drop = crate::entity::nonliving_attack::capture_attack_item_drop(
            source.as_ref(),
            reference(403, ServerId(2)),
            stack,
        )
        .unwrap();
        {
            let mut lifecycle = context(
                &mut pairs,
                &mut replicas,
                local,
                source_ref,
                Arc::clone(&source),
            );
            assert!(lifecycle.spawn_drop(&source, &holder_drop));
        }
        let holder = replicas.get(holder_drop.entity).unwrap();
        assert_eq!(holder.entity.get_entity().entity_id, holder_drop.entity.local_id);
        assert!(pairs
            .by_origin
            .contains_key(&(owner_drop.entity.origin, owner_drop.entity.local_id)));
        assert!(replicas
            .by_origin
            .contains_key(&(holder_drop.entity.origin, holder_drop.entity.local_id)));
    }

    #[test]
    fn lifecycle_multitarget_spawn_failure_restores_exact_first_target_registration() {
        let (world, _folder) = world();
        let local = ServerId(1);
        let first_ref = reference(501, local);
        let second_ref = reference(502, local);
        let first = item(Arc::clone(&world));
        let second = item(Arc::clone(&world));
        for (entity, reference) in [(&first, first_ref), (&second, second_ref)] {
            let inner = entity.get_entity();
            inner.cluster_owner.store(local.0, Ordering::Relaxed);
            inner.cluster_origin_server.store(local.0, Ordering::Relaxed);
            inner.cluster_origin_id.store(reference.local_id, Ordering::Relaxed);
            assert!(world.add_cluster_entity_transaction(Arc::clone(entity)));
        }
        let stack = ItemEntity::cluster_stack_from_item_stack(&ItemStack::new(1, &Item::STONE));
        let first_drop = crate::entity::nonliving_attack::capture_attack_item_drop(
            first.as_ref(),
            reference(503, local),
            stack.clone(),
        )
        .unwrap();
        let failed_drop = AttackItemDrop {
            entity: reference(504, local),
            stack,
            entity_nbt: vec![0],
        };
        let attack = CapturedAttack {
            actor: pumpkin_cluster::identity::ActionActor::Server(local),
            seq: pumpkin_cluster::identity::PlayerSeq(1),
            tick: pumpkin_cluster::time::TickStamp(9),
            attacker: first_ref,
            outcome: CapturedAttackOutcome::Landed,
            primary: pumpkin_cluster::protocol::CapturedAttackTarget {
                target: EntityMutationTarget::Entity(first_ref),
                outcome: pumpkin_cluster::protocol::CapturedAttackTargetOutcome::NonLiving(
                    item_drop_outcome(&first, vec![first_drop.clone()]),
                ),
            },
            sweeping: vec![pumpkin_cluster::protocol::CapturedAttackTarget {
                target: EntityMutationTarget::Entity(second_ref),
                outcome: pumpkin_cluster::protocol::CapturedAttackTargetOutcome::NonLiving(
                    item_drop_outcome(&second, vec![failed_drop]),
                ),
            }],
            attacker_effects: actor_effects(),
        };
        let mut pairs = EntityTruthPairs::default();
        let mut replicas = EntityReplicas::default();
        let mut lifecycle = NonLivingLifecycleContext {
            pairs: &mut pairs,
            replicas: &mut replicas,
            local,
            truth: CombatTruth::Local,
            tick: attack.tick,
            local_bindings: vec![
                (EntityMutationTarget::Entity(first_ref), Arc::clone(&first)),
                (EntityMutationTarget::Entity(second_ref), Arc::clone(&second)),
            ],
            ground_bindings: vec![
                (EntityMutationTarget::Entity(first_ref), Arc::clone(&first)),
                (EntityMutationTarget::Entity(second_ref), Arc::clone(&second)),
            ],
            expected_drops: Vec::new(),
            observed_drops: Vec::new(),
        };
        assert!(!apply_nonliving_captured_attack_lifecycle(&mut lifecycle, &attack));
        assert!(!lifecycle
            .pairs
            .by_origin
            .contains_key(&(first_drop.entity.origin, first_drop.entity.local_id)));
        assert!(world.entities.load().iter().any(|entity| Arc::ptr_eq(entity, &first)));
        assert!(world.entities.load().iter().any(|entity| Arc::ptr_eq(entity, &second)));
    }
}
