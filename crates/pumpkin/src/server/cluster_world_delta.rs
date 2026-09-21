use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{BlockPos as ClusterBlockPos, ChunkAddr, StreamKind};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{
    BlockDelta, WorldDelta, capture_explosion_update, capture_random_tick_delta,
    capture_redstone_update, chunk_of_block, encode_frame,
};
use pumpkin_data::BlockStateId;
use pumpkin_util::math::position::BlockPos;
use tokio::sync::mpsc;

use crate::world::World;

static WORLD_DELTA_OUTBOX: OnceLock<WorldDeltaOutbox> = OnceLock::new();
static WORLD_DELTA_EMITTED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_DROPPED: AtomicU64 = AtomicU64::new(0);

pub struct WorldDeltaOutbox {
    pub peers: Vec<ServerId>,
    pub outbound: mpsc::Sender<OutboundParcel>,
}

#[must_use]
pub fn mesh_active() -> bool {
    WORLD_DELTA_OUTBOX.get().is_some_and(|outbox| !outbox.peers.is_empty())
}

#[must_use]
pub fn primary_produces() -> bool {
    mesh_active() && !pumpkin_world::level::is_cluster_secondary()
}

pub fn install_world_delta_outbox(
    peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = WORLD_DELTA_OUTBOX.set(WorldDeltaOutbox { peers, outbound });
}

#[must_use]
pub fn holder_of(world: &World) -> ServerId {
    world.server.upgrade().map_or(ServerId(0), |server| {
        ServerId(server.advanced_config.cluster.server_id)
    })
}

#[must_use]
pub fn world_delta_emitted() -> u64 {
    WORLD_DELTA_EMITTED.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_delta_dropped() -> u64 {
    WORLD_DELTA_DROPPED.load(Ordering::Relaxed)
}

fn forward_frame(bytes: Vec<u8>) {
    let Some(outbox) = WORLD_DELTA_OUTBOX.get() else {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if outbox.peers.is_empty() {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let mut sent = 0_u64;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: *peer,
            header: StreamHeader::new(StreamKind::PlayerWorld, None),
            bytes: bytes.clone(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent == 0 {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
    } else {
        WORLD_DELTA_EMITTED.fetch_add(sent, Ordering::Relaxed);
    }
}

pub fn emit_random_tick_delta(
    _holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    edits: Vec<BlockDelta>,
) {
    if pumpkin_world::level::is_cluster_secondary() {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let Some(update) = capture_random_tick_delta(chunk, tick, edits) else {
        return;
    };
    match encode_frame(&WorldDelta::RandomTick(update)) {
        Ok(bytes) => forward_frame(bytes),
        Err(_) => {
            WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn emit_redstone_update(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    trigger: ClusterBlockPos,
    edits: Vec<BlockDelta>,
) {
    let Some(update) = capture_redstone_update(holder, chunk, tick, trigger, edits) else {
        return;
    };
    match encode_frame(&WorldDelta::Redstone(update)) {
        Ok(bytes) => forward_frame(bytes),
        Err(_) => {
            WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn emit_explosion_update(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    center: ClusterBlockPos,
    edits: Vec<BlockDelta>,
) {
    let Some(update) = capture_explosion_update(holder, chunk, tick, center, edits) else {
        return;
    };
    match encode_frame(&WorldDelta::Explosion(update)) {
        Ok(bytes) => forward_frame(bytes),
        Err(_) => {
            WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn emit_redstone_write(
    world: &World,
    trigger: &BlockPos,
    edit_pos: &BlockPos,
    old_state: u16,
    new_state: u16,
) {
    if old_state == new_state || !mesh_active() {
        return;
    }
    let edit = BlockDelta {
        pos: ClusterBlockPos {
            x: edit_pos.0.x,
            y: edit_pos.0.y,
            z: edit_pos.0.z,
        },
        old_state,
        new_state,
    };
    emit_redstone_update(
        holder_of(world),
        chunk_of_block(edit_pos.0.x, edit_pos.0.z),
        TickStamp::now(),
        ClusterBlockPos {
            x: trigger.0.x,
            y: trigger.0.y,
            z: trigger.0.z,
        },
        vec![edit],
    );
}

pub fn emit_explosion_blocks(
    world: &World,
    center: &BlockPos,
    destroyed: &[(BlockPos, u16)],
) {
    if destroyed.is_empty() || !mesh_active() {
        return;
    }
    let new_state = BlockStateId::AIR.as_u16();
    let mut grouped: HashMap<ChunkAddr, Vec<BlockDelta>> = HashMap::new();
    for (pos, old_state) in destroyed {
        grouped
            .entry(chunk_of_block(pos.0.x, pos.0.z))
            .or_default()
            .push(BlockDelta {
                pos: ClusterBlockPos {
                    x: pos.0.x,
                    y: pos.0.y,
                    z: pos.0.z,
                },
                old_state: *old_state,
                new_state,
            });
    }
    let holder = holder_of(world);
    let tick = TickStamp::now();
    let center_cluster = ClusterBlockPos {
        x: center.0.x,
        y: center.0.y,
        z: center.0.z,
    };
    for (chunk, edits) in grouped {
        emit_explosion_update(holder, chunk, tick, center_cluster, edits);
    }
}
