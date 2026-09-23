use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::movement::{AcceptedPos, RemotePlayerTable};
use pumpkin_cluster::protocol::{PlayerReplicaSnapshot, PlayerVisualSnapshot, WorldId};
use pumpkin_cluster::wire::MovementBatch;
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_protocol::bedrock::client::move_player::CMovePlayer;
use pumpkin_protocol::codec::var_ulong::VarULong;
use pumpkin_protocol::java::client::play::{CEntityPositionSync, CHeadRot};
use pumpkin_util::math::vector3::Vector3;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use super::cluster_entity_apply::forward_entity_pos_datagram;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram};
use crate::entity::EntityBase;
use crate::entity::cluster_player_ghost::RemotePlayerReplica;

static GHOST_APPLIED: AtomicU64 = AtomicU64::new(0);
static GHOST_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static REMOTE_PLAYER_POSITIONS: LazyLock<arc_swap::ArcSwap<HashMap<GlobalPlayerId, AcceptedPos>>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));
static REMOTE_PLAYER_GHOSTS: LazyLock<
    arc_swap::ArcSwap<HashMap<GlobalPlayerId, Arc<RemotePlayerReplica>>>,
> = LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));

fn world_for_id(server: &Server, id: WorldId) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == id)
        .cloned()
}

#[must_use]
pub fn remote_player_position(gid: GlobalPlayerId) -> Option<AcceptedPos> {
    REMOTE_PLAYER_POSITIONS.load().get(&gid).copied()
}

#[must_use]
pub fn remote_player_replica(gid: GlobalPlayerId) -> Option<Arc<RemotePlayerReplica>> {
    REMOTE_PLAYER_GHOSTS.load().get(&gid).cloned()
}

#[must_use]
pub fn remote_player_gid(entity: &dyn EntityBase) -> Option<GlobalPlayerId> {
    entity
        .cast_any()
        .downcast_ref::<RemotePlayerReplica>()
        .map(|replica| replica.gid)
}

fn publish_remote_player_positions(samples: &[AcceptedPos]) {
    if samples.is_empty() {
        return;
    }
    let mut positions = REMOTE_PLAYER_POSITIONS.load().as_ref().clone();
    for sample in samples {
        positions.insert(sample.gid, *sample);
    }
    REMOTE_PLAYER_POSITIONS.store(Arc::new(positions));
}

fn ghost_for_sample(
    _server: &Arc<Server>,
    sample: &AcceptedPos,
) -> Option<Arc<RemotePlayerReplica>> {
    REMOTE_PLAYER_GHOSTS.load().get(&sample.gid).cloned()
}

fn apply_replica_visual(replica: &RemotePlayerReplica, visual: PlayerVisualSnapshot) {
    let entity = replica.get_entity();
    entity.set_sneaking(visual.sneaking);
    entity.set_sprinting(visual.sprinting);
    if visual.swimming {
        entity.set_swimming(true);
    }
    if visual.gliding {
        entity.set_fall_flying(true);
    }
    entity.set_synced_data(
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMISATION,
        visual.skin_layers,
    );
    entity.set_synced_data(
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMIZATION_ID,
        visual.skin_layers,
    );
    let slots = [
        EquipmentSlot::MAIN_HAND,
        EquipmentSlot::OFF_HAND,
        EquipmentSlot::FEET,
        EquipmentSlot::LEGS,
        EquipmentSlot::CHEST,
        EquipmentSlot::HEAD,
    ];
    let mut equipment = Vec::new();
    for (slot, state) in slots.into_iter().zip(visual.equipment) {
        let stack = if state.item == 0 || state.count == 0 {
            ItemStack::EMPTY.clone()
        } else if let Some(item) = Item::from_id(state.item) {
            ItemStack::new(state.count, item)
        } else {
            continue;
        };
        equipment.push((slot, stack));
    }
    replica.living_entity.send_equipment_changes(&equipment);
    if visual.blocking {
        replica.living_entity.set_active_hand(
            pumpkin_util::Hand::Right,
            ItemStack::EMPTY.clone(),
            0,
        );
    }
}

fn apply_remote_player_samples(server: &Arc<Server>, samples: &[AcceptedPos]) {
    publish_remote_player_positions(samples);
    for sample in samples {
        let Some(ghost) = ghost_for_sample(server, sample) else {
            continue;
        };
        let entity = ghost.get_entity();
        let position = Vector3::new(sample.pos[0], sample.pos[1], sample.pos[2]);
        let velocity = Vector3::new(sample.vel[0], sample.vel[1], sample.vel[2]);
        entity.set_pos(position);
        entity.velocity.store(velocity);
        entity.yaw.store(sample.yaw);
        entity.head_yaw.store(sample.head_yaw);
        entity.pitch.store(sample.pitch);
        entity
            .on_ground
            .store(sample.on_ground, std::sync::atomic::Ordering::Relaxed);
        let world = entity.world.load();
        world
            .entity_tracker
            .update_entity_position(ghost.as_ref(), &world);
        let java_packet = CEntityPositionSync::new(
            entity.entity_id.into(),
            position,
            velocity,
            sample.yaw,
            sample.pitch,
            sample.on_ground,
        );
        let bedrock_packet = CMovePlayer::new(
            VarULong(entity.entity_id as u64),
            Vector3::new(
                position.x as f32,
                position.y as f32 + entity.entity_type.eye_height,
                position.z as f32,
            ),
            sample.pitch,
            sample.yaw,
            sample.head_yaw,
            CMovePlayer::MODE_NORMAL,
            sample.on_ground,
            VarULong(0),
            0,
            0,
            VarULong(0),
        );
        if let Some(tracked) = world.entity_tracker.get_tracked_entity(entity.entity_id) {
            tracked.send_to_tracking_players_editioned(&java_packet, &bedrock_packet, &world);
            tracked.send_to_tracking_players(
                &CHeadRot::new(
                    entity.entity_id.into(),
                    (sample.head_yaw * 256.0 / 360.0) as u8,
                ),
                &world,
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoundaryApplyOutcome {
    Applied,
    UnauthorizedSource,
    AwaitingPresence,
    AwaitingActivePresence,
    AwaitingWorld,
    AwaitingChunk,
}

pub fn apply_remote_player_boundary(
    server: &Arc<Server>,
    snapshot: PlayerReplicaSnapshot,
) -> BoundaryApplyOutcome {
    let update = snapshot.movement;
    if remote_player_replica(update.gid).is_none() {
        let Some(entry) = super::cluster_presence::remote_presence_entry(server, update.gid) else {
            return BoundaryApplyOutcome::AwaitingPresence;
        };
        if entry.in_lobby {
            return BoundaryApplyOutcome::AwaitingActivePresence;
        }
        let Some(world) = world_for_id(server, update.world) else {
            return BoundaryApplyOutcome::AwaitingWorld;
        };
        let chunk = pumpkin_cluster::protocol::ChunkKey::new(
            update.world,
            (update.pos[0].floor() as i32).div_euclid(16),
            (update.pos[2].floor() as i32).div_euclid(16),
        );
        if !server
            .cluster_owned_chunk_blocks
            .load()
            .contains_key(&chunk)
        {
            return BoundaryApplyOutcome::AwaitingChunk;
        }
        let replica = RemotePlayerReplica::new(
            Arc::clone(&world),
            update.gid,
            uuid::Uuid::from_bytes(entry.uuid),
            entry.name,
            Vector3::new(update.pos[0], update.pos[1], update.pos[2]),
            pumpkin_util::GameMode::try_from(snapshot.visual.game_mode)
                .unwrap_or(pumpkin_util::GameMode::Survival),
        );
        apply_replica_visual(&replica, snapshot.visual);
        world.add_cluster_replica(Arc::clone(&replica) as Arc<dyn EntityBase>);
        let mut replicas = REMOTE_PLAYER_GHOSTS.load().as_ref().clone();
        replicas.insert(update.gid, replica);
        REMOTE_PLAYER_GHOSTS.store(Arc::new(replicas));
    } else if let Some(replica) = remote_player_replica(update.gid) {
        apply_replica_visual(&replica, snapshot.visual);
    }
    apply_remote_player_samples(
        server,
        &[AcceptedPos {
            gid: update.gid,
            seq: update.seq,
            tick: update.tick,
            world: update.world,
            pos: update.pos,
            vel: update.vel,
            yaw: update.yaw,
            head_yaw: update.head_yaw,
            pitch: update.pitch,
            on_ground: update.on_ground,
        }],
    );
    BoundaryApplyOutcome::Applied
}

pub fn remove_remote_player_ghost(server: &Arc<Server>, gid: GlobalPlayerId) {
    let mut ghosts = REMOTE_PLAYER_GHOSTS.load().as_ref().clone();
    let ghost = ghosts.remove(&gid);
    if ghost.is_some() {
        REMOTE_PLAYER_GHOSTS.store(Arc::new(ghosts));
    }
    if let Some(ghost) = ghost {
        let world = ghost.get_entity().world.load();
        world.remove_cluster_replica(ghost.as_ref());
    }
    let mut positions = REMOTE_PLAYER_POSITIONS.load().as_ref().clone();
    if positions.remove(&gid).is_some() {
        REMOTE_PLAYER_POSITIONS.store(Arc::new(positions));
    }
    let _ = server;
}

/// Returns how many accepted remote-player samples have been applied to ghosts.
#[must_use]
pub fn ghost_applied() -> u64 {
    GHOST_APPLIED.load(Ordering::Relaxed)
}

/// Returns how many position datagrams have been received from the mesh.
#[must_use]
pub fn ghost_datagrams() -> u64 {
    GHOST_DATAGRAMS.load(Ordering::Relaxed)
}

/// Applies one raw datagram payload to the ghost table and attached worlds.
///
/// Returns the number of fresh samples applied.
fn carries_entity_pos(bytes: &[u8]) -> bool {
    matches!(
        postcard::take_from_bytes::<u32>(bytes),
        Ok((magic, _)) if magic == ENTITY_POS_MAGIC
    )
}

pub fn apply_entity_pos_datagram(_server: &Arc<Server>, peer: ServerId, bytes: &[u8]) -> usize {
    let datagram = match postcard::from_bytes::<EntityPosDatagram>(bytes) {
        Ok(datagram) => datagram,
        Err(error) => {
            warn!(%error, "cluster entity pos datagram decode failed");
            return 0;
        }
    };
    if !datagram.is_consistent() {
        warn!("cluster entity pos datagram failed validation");
        return 0;
    }
    if datagram.is_empty() {
        return 0;
    }
    let delivered = datagram.len();
    if forward_entity_pos_datagram(peer, datagram.updates) {
        delivered
    } else {
        0
    }
}

pub fn apply_datagram_bytes(
    server: &Arc<Server>,
    table: &mut RemotePlayerTable,
    _local: ServerId,
    peer: ServerId,
    bytes: Vec<u8>,
) -> usize {
    if carries_entity_pos(&bytes) {
        return apply_entity_pos_datagram(server, peer, &bytes);
    }
    let datagram = match MovementBatch::decode(bytes.into()) {
        Ok(datagram) => datagram,
        Err(error) => {
            warn!(?error, "cluster ghost pos datagram decode failed");
            return 0;
        }
    };
    let accepted = table.apply_updates(datagram.updates());
    if accepted.is_empty() {
        return 0;
    }
    apply_remote_player_samples(server, &accepted);
    GHOST_APPLIED.fetch_add(accepted.len() as u64, Ordering::Relaxed);
    accepted.len()
}

/// Spawns the background task that applies incoming position datagrams to ghosts.
///
/// Each accepted [`pumpkin_cluster::movement::AcceptedPos`] updates the matching
/// remote player's ghost position via the world's entity tracker.
pub fn spawn_ghost_apply(
    server: &Arc<Server>,
    local: ServerId,
    mut datagram_in_rx: mpsc::Receiver<(ServerId, Vec<u8>)>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut table = RemotePlayerTable::new();
        let mut first = true;
        while let Some((peer, bytes)) = datagram_in_rx.recv().await {
            GHOST_DATAGRAMS.fetch_add(1, Ordering::Relaxed);
            if first {
                first = false;
                debug!(from = peer.0, "cluster ghost pos stream started");
            }
            apply_datagram_bytes(&task_server, &mut table, local, peer, bytes);
        }
        debug!("cluster ghost pos stream closed");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{PlayerSeq, PlayerSlot};
    use pumpkin_cluster::time::TickStamp;

    #[test]
    fn remote_position_directory_exposes_latest_sample() {
        let gid = GlobalPlayerId::new(ServerId(u16::MAX), PlayerSlot(u16::MAX - 1));
        let sample = AcceptedPos {
            gid,
            seq: PlayerSeq(3),
            tick: TickStamp(7),
            world: WorldId::new(0, 7),
            pos: [12.0, 65.0, -4.0],
            vel: [0.25, 0.0, -0.5],
            yaw: 90.0,
            head_yaw: 75.0,
            pitch: -15.0,
            on_ground: true,
        };
        publish_remote_player_positions(&[sample]);
        assert_eq!(remote_player_position(gid), Some(sample));
    }
}
