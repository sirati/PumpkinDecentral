use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::codec::decode_pos;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::movement::{AcceptedPos, RemotePlayerTable};
use pumpkin_protocol::bedrock::client::move_player::CMovePlayer;
use pumpkin_protocol::codec::var_ulong::VarULong;
use pumpkin_protocol::java::client::play::CEntityPositionSync;
use pumpkin_util::math::vector3::Vector3;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use super::cluster_entity_apply::forward_entity_pos_datagram;
use super::cluster_entity_emit::{ENTITY_POS_MAGIC, EntityPosDatagram};
use crate::entity::EntityBase;
use crate::entity::cluster_player_ghost::ClusterPlayerGhost;

static GHOST_APPLIED: AtomicU64 = AtomicU64::new(0);
static GHOST_DATAGRAMS: AtomicU64 = AtomicU64::new(0);
static REMOTE_PLAYER_POSITIONS: LazyLock<arc_swap::ArcSwap<HashMap<GlobalPlayerId, AcceptedPos>>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));
static REMOTE_PLAYER_GHOSTS: LazyLock<arc_swap::ArcSwap<HashMap<GlobalPlayerId, Arc<ClusterPlayerGhost>>>> =
    LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));

#[must_use]
pub fn remote_player_position(gid: GlobalPlayerId) -> Option<AcceptedPos> {
    REMOTE_PLAYER_POSITIONS.load().get(&gid).copied()
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

fn ghost_for_sample(server: &Arc<Server>, sample: &AcceptedPos) -> Option<Arc<ClusterPlayerGhost>> {
    let entry = super::cluster_presence::remote_presence_entry(sample.gid)?;
    if entry.in_lobby {
        return None;
    }
    if let Some(ghost) = REMOTE_PLAYER_GHOSTS.load().get(&sample.gid) {
        return Some(Arc::clone(ghost));
    }
    let world = server.worlds.load().first()?.clone();
    let ghost = ClusterPlayerGhost::new(
        Arc::clone(&world),
        uuid::Uuid::from_bytes(entry.uuid),
        entry.name,
        Vector3::new(sample.pos[0], sample.pos[1], sample.pos[2]),
    );
    world
        .entity_tracker
        .add_entity(&(Arc::clone(&ghost) as Arc<dyn EntityBase>), &world);
    let mut ghosts = REMOTE_PLAYER_GHOSTS.load().as_ref().clone();
    ghosts.insert(sample.gid, Arc::clone(&ghost));
    REMOTE_PLAYER_GHOSTS.store(Arc::new(ghosts));
    Some(ghost)
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
        entity.pitch.store(sample.pitch);
        entity.head_yaw.store(sample.yaw);
        let world = entity.world.load();
        world.entity_tracker.update_entity_position(ghost.as_ref(), &world);
        let java_packet = CEntityPositionSync::new(
            entity.entity_id.into(),
            position,
            velocity,
            sample.yaw,
            sample.pitch,
            true,
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
            sample.yaw,
            CMovePlayer::MODE_NORMAL,
            true,
            VarULong(0),
            0,
            0,
            VarULong(0),
        );
        if let Some(tracked) = world.entity_tracker.get_tracked_entity(entity.entity_id) {
            tracked.send_to_tracking_players_editioned(&java_packet, &bedrock_packet, &world);
        }
    }
}

pub fn remove_remote_player_ghost(server: &Arc<Server>, gid: GlobalPlayerId) {
    let mut ghosts = REMOTE_PLAYER_GHOSTS.load().as_ref().clone();
    let ghost = ghosts.remove(&gid);
    if ghost.is_some() {
        REMOTE_PLAYER_GHOSTS.store(Arc::new(ghosts));
    }
    if let Some(ghost) = ghost {
        let world = ghost.get_entity().world.load();
        world.entity_tracker.remove_entity(ghost.as_ref(), &world);
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
    bytes: &[u8],
) -> usize {
    if carries_entity_pos(bytes) {
        return apply_entity_pos_datagram(server, peer, bytes);
    }
    let datagram = match decode_pos(bytes) {
        Ok(datagram) => datagram,
        Err(error) => {
            warn!(%error, "cluster ghost pos datagram decode failed");
            return 0;
        }
    };
    if !datagram.is_consistent() {
        warn!("cluster ghost pos datagram count mismatch");
        return 0;
    }
    let accepted = table.apply_pos_datagram(&datagram);
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
            apply_datagram_bytes(&task_server, &mut table, local, peer, &bytes);
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
            pos: [12.0, 65.0, -4.0],
            vel: [0.25, 0.0, -0.5],
            yaw: 90.0,
            pitch: -15.0,
        };
        publish_remote_player_positions(&[sample]);
        assert_eq!(remote_player_position(gid), Some(sample));
    }
}
