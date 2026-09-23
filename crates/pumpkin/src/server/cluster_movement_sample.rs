//! Per-tick movement sampler for the cluster mesh.
//!
//! Every server tick the local players' position, velocity and facing are
//! pushed into a thread-local double bank and handed to the fuse side as a
//! boxed [`Bank`](pumpkin_cluster::banks::Bank) over an
//! [`mpsc`](tokio::sync::mpsc) channel. The sampling path touches only
//! thread-local state and lock-free channel operations, so it never blocks
//! the tick thread and takes no locks.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pumpkin_cluster::banks::{FuseEnds, WorkerEnds, bank_pipes};
use pumpkin_cluster::codec::encode_batch;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq};
use pumpkin_cluster::protocol::{ChunkKey, PosUpdate, TickBatch, WorldId};
use pumpkin_cluster::time::TickStamp;

use crate::entity::{EntityBase, player::Player};

use super::Server;

thread_local! {
    static WORKER: RefCell<Option<WorkerEnds>> = const { RefCell::new(None) };
    static PARKED_FUSE: RefCell<Option<FuseEnds>> = const { RefCell::new(None) };
    static FUSED: RefCell<Option<FuseEnds>> = const { RefCell::new(None) };
    static SEQS: RefCell<HashMap<GlobalPlayerId, PlayerSeq>> =
        RefCell::new(HashMap::new());
    static PLAYER_CHUNKS: RefCell<HashMap<GlobalPlayerId, ChunkKey>> =
        RefCell::new(HashMap::new());
    static MISSING_GIDS: RefCell<HashSet<i32>> = RefCell::new(HashSet::new());
}

fn ensure_worker() {
    let installed = WORKER
        .try_with(|worker| worker.borrow().is_some())
        .unwrap_or(true);
    if installed {
        return;
    }
    let (worker_ends, fuse_ends) = bank_pipes();
    WORKER
        .try_with(|worker| {
            *worker.borrow_mut() = Some(worker_ends);
        })
        .ok();
    PARKED_FUSE
        .try_with(|parked| {
            *parked.borrow_mut() = Some(fuse_ends);
        })
        .ok();
}

fn next_seq(gid: GlobalPlayerId) -> PlayerSeq {
    SEQS.try_with(|seqs| {
        if let Ok(mut seqs) = seqs.try_borrow_mut() {
            let next = seqs
                .get(&gid)
                .map_or(PlayerSeq(0), |seen| PlayerSeq(seen.0.wrapping_add(1)));
            seqs.insert(gid, next);
            next
        } else {
            PlayerSeq(0)
        }
    })
    .unwrap_or(PlayerSeq(0))
}

/// Installs the thread-local worker bank on this thread.
///
/// Returns the fuse ends so the caller can forward handed-off banks to the
/// mesh. Returns [`None`] if a worker is already installed.
#[must_use]
pub fn install() -> Option<FuseEnds> {
    ensure_worker();
    take_parked_fuse_ends()
}

/// Takes fuse ends parked by lazy initialization, if any.
#[must_use]
pub fn take_parked_fuse_ends() -> Option<FuseEnds> {
    PARKED_FUSE
        .try_with(|parked| parked.borrow_mut().take())
        .unwrap_or_default()
}

/// Pushes one movement sample into the thread-local write bank.
pub fn sample_local(
    gid: GlobalPlayerId,
    tick: TickStamp,
    world: WorldId,
    pos: [f64; 3],
    vel: [f64; 3],
    yaw: f32,
    head_yaw: f32,
    pitch: f32,
    on_ground: bool,
) -> PosUpdate {
    sample_local_with_seq(
        gid,
        next_seq(gid),
        tick,
        world,
        pos,
        vel,
        yaw,
        head_yaw,
        pitch,
        on_ground,
    )
}

fn sample_local_with_seq(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    world: WorldId,
    pos: [f64; 3],
    vel: [f64; 3],
    yaw: f32,
    head_yaw: f32,
    pitch: f32,
    on_ground: bool,
) -> PosUpdate {
    ensure_worker();
    let update = PosUpdate {
        gid,
        seq,
        tick,
        world,
        pos,
        vel,
        yaw,
        head_yaw,
        pitch,
        on_ground,
    };
    WORKER
        .try_with(|worker| {
            if let Ok(mut worker) = worker.try_borrow_mut() {
                if let Some(worker) = worker.as_mut() {
                    worker.bank.pos.push(update);
                }
            }
        })
        .ok();
    update
}

/// Samples one player's position, velocity and facing into the write bank.
pub fn sample_player(
    player: &Player,
    server: &Server,
    gid: GlobalPlayerId,
    tick: TickStamp,
) -> PosUpdate {
    let entity = &player.living_entity.entity;
    let world = player.world();
    if let Some(frame) = server.cluster_controller.effective_for_tick(gid, tick) {
        let effective = frame.effective;
        return sample_local_with_seq(
            gid,
            next_seq(gid),
            tick,
            world_id(&world),
            effective.position,
            effective.velocity,
            effective.yaw,
            entity.head_yaw.load(),
            effective.pitch,
            effective.on_ground,
        );
    }
    let pos = entity.pos.load();
    let vel = entity.velocity.load();
    sample_local(
        gid,
        tick,
        world_id(&world),
        [pos.x, pos.y, pos.z],
        [vel.x, vel.y, vel.z],
        entity.yaw.load(),
        entity.head_yaw.load(),
        entity.pitch.load(),
        entity.on_ground.load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn world_id(world: &crate::world::World) -> WorldId {
    WorldId::new(
        world.dimension.id,
        xxhash_rust::xxh64::xxh64(world.get_world_name().as_bytes(), 0),
    )
}

/// Hands the filled write bank to the fuse side as a boxed bank over mpsc.
///
/// Skips the handoff when no samples were collected this tick so idle ticks
/// never apply backpressure to the channel.
pub fn end_tick() {
    WORKER
        .try_with(|worker| {
            if let Ok(mut worker) = worker.try_borrow_mut() {
                if let Some(worker) = worker.as_mut() {
                    if !worker.bank.is_empty() {
                        worker.end_tick();
                    }
                }
            }
        })
        .ok();
}

/// Returns how many movement samples are staged in the write bank.
#[must_use]
pub fn pending_len() -> usize {
    WORKER
        .try_with(|worker| {
            worker
                .try_borrow()
                .map(|worker| worker.as_ref().map_or(0, |worker| worker.bank.pos.len()))
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

/// Claims the parked fuse ends onto the pump slot on this thread.
///
/// Calls [`install`] once so the tick thread owns its fuse ends, then reuses
/// the installed ends on later ticks. Touches only thread-local state.
fn ensure_fused() {
    let installed = FUSED
        .try_with(|fused| fused.borrow().is_some())
        .unwrap_or(true);
    if installed {
        return;
    }
    if let Some(ends) = install() {
        FUSED
            .try_with(|fused| {
                *fused.borrow_mut() = Some(ends);
            })
            .ok();
    }
}

/// Drains ready movement banks, encodes one batch, and forwards it to the mesh.
///
/// Polls the installed fuse ends with [`try_recv`](tokio::sync::mpsc::Receiver::try_recv),
/// folds every handed-off boxed [`Bank`](pumpkin_cluster::banks::Bank) into a
/// single [`TickBatch`], recycles each bank over the empty channel, encodes
/// via codec, and feeds [`forward_movement_batch`](super::cluster::forward_movement_batch)
/// so [`forward_fused_batch`](super::cluster_datagram::forward_fused_batch)
/// and the stream fanout in the fuse task emit position datagrams. Skips
/// empty ticks and encode failures without blocking and takes no locks.
pub fn pump_movement_fuse(tick: TickStamp) {
    ensure_fused();
    let batch = FUSED
        .try_with(|fused| {
            let Ok(mut fused) = fused.try_borrow_mut() else {
                return None;
            };
            let ends = fused.as_mut()?;
            let mut batch = TickBatch::new(tick);
            let mut drained = false;
            while let Ok(mut bank) = ends.full_rx.try_recv() {
                batch.append_bank(&bank);
                bank.clear();
                let _ = ends.empty_tx.try_send(bank);
                drained = true;
            }
            drained.then_some(batch)
        })
        .unwrap_or(None);
    let Some(batch) = batch else {
        return;
    };
    if batch.is_empty() {
        return;
    }
    match encode_batch(&batch) {
        Ok(bytes) => super::cluster::forward_movement_batch(bytes),
        Err(error) => tracing::warn!(%error, "cluster movement fuse encode failed"),
    }
}

/// Samples every local player and hands the bank off. Call once per tick.
///
/// Drains ready banks through [`pump_movement_fuse`] after sampling so
/// position datagrams flow to the mesh each tick.
///
/// Returns how many players were sampled. Does nothing when the cluster mesh
/// is disabled.
#[must_use]
pub fn sample_server_tick(server: &Arc<Server>, tick: TickStamp) -> usize {
    if !server.advanced_config.cluster.enabled {
        return 0;
    }
    ensure_worker();
    let mut sampled = 0;
    let mut chunks = HashMap::new();
    let mut visuals = Vec::new();
    for player in server.get_all_players() {
        if player.is_in_cluster_lobby()
            || player
                .cluster_departing
                .load(std::sync::atomic::Ordering::Acquire)
        {
            continue;
        }
        let Some(gid) = player.cluster_gid() else {
            let entity_id = player.living_entity.entity.entity_id;
            MISSING_GIDS.with_borrow_mut(|seen| {
                if seen.len() < 1024 && seen.insert(entity_id) {
                    tracing::error!(entity_id, "cluster joined player missing global identity");
                }
            });
            continue;
        };
        let movement = sample_player(&player, server, gid, tick);
        let chunk = ChunkKey::new(
            movement.world,
            (movement.pos[0].floor() as i32).div_euclid(16),
            (movement.pos[2].floor() as i32).div_euclid(16),
        );
        PLAYER_CHUNKS.with_borrow_mut(|known| {
            if let Some(from) = known.insert(gid, chunk)
                && from != chunk
            {
                super::cluster_tick_pump::stage_player_chunk_transition(
                    pumpkin_cluster::protocol::PlayerChunkTransition {
                        gid,
                        seq: movement.seq,
                        tick,
                        from,
                        to: chunk,
                        snapshot: pumpkin_cluster::protocol::PlayerReplicaSnapshot {
                            movement,
                            visual: super::cluster_tick_pump::player_visual_snapshot(gid),
                        },
                    },
                );
            }
        });
        chunks.insert(gid, chunk);
        visuals.push(super::cluster_tick_pump::PlayerVisualBase {
            gid,
            sneaking: player.get_entity().is_sneaking(),
            sprinting: player.get_entity().is_sprinting(),
            swimming: player.get_entity().is_swimming(),
            gliding: player.get_entity().is_fall_flying(),
            skin_layers: player.config.load().skin_parts,
            game_mode: player.gamemode.load() as u8,
        });
        sampled += 1;
    }
    PLAYER_CHUNKS.with_borrow_mut(|known| known.retain(|gid, _| chunks.contains_key(gid)));
    super::cluster_datagram::publish_local_player_chunks(chunks);
    super::cluster_tick_pump::publish_player_visual_bases(visuals);
    end_tick();
    pump_movement_fuse(tick);
    sampled
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};

    use super::*;

    #[test]
    fn samples_land_in_write_bank() {
        let _ = take_parked_fuse_ends();
        let gid = GlobalPlayerId::new(ServerId(9), PlayerSlot(1));
        sample_local(
            gid,
            TickStamp(7),
            WorldId::new(0, 7),
            [1.0, 2.0, 3.0],
            [0.0; 3],
            90.0,
            90.0,
            0.0,
            true,
        );
        assert_eq!(pending_len(), 1);
        end_tick();
        assert_eq!(pending_len(), 0);
    }
}
