use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::codec::encode_batch;
use pumpkin_cluster::combat::drain_ordered_combat;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{StreamKind, TickBatch};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::transient::drain_transient_bank;
use pumpkin_cluster::visual::drain_visual_bank;
use tokio::sync::mpsc;

use super::Server;
use crate::net::java::JavaClient;

const ORDER_SEED: u64 = 0;

struct TickPumpOutbox {
    peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static TICK_PUMP_OUTBOX: OnceLock<TickPumpOutbox> = OnceLock::new();
static TICK_PUMP_FORWARDED: AtomicU64 = AtomicU64::new(0);
static TICK_PUMP_DROPPED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn tick_pump_forwarded() -> u64 {
    TICK_PUMP_FORWARDED.load(Ordering::Relaxed)
}

#[must_use]
pub fn tick_pump_dropped() -> u64 {
    TICK_PUMP_DROPPED.load(Ordering::Relaxed)
}

pub fn install_tick_pump_outbox(peers: Vec<ServerId>, outbound: mpsc::Sender<OutboundParcel>) {
    let _ = TICK_PUMP_OUTBOX.set(TickPumpOutbox { peers, outbound });
}

fn staged_tick_batch(tick: TickStamp) -> Option<TickBatch> {
    let visual = drain_visual_bank();
    let transient = drain_transient_bank();
    let combat = drain_ordered_combat(ORDER_SEED, tick);
    let break_block = JavaClient::drain_break_outbox();
    let place_block = JavaClient::drain_place_outbox();
    if visual.is_empty()
        && transient.is_empty()
        && combat.is_empty()
        && break_block.is_empty()
        && place_block.is_empty()
    {
        return None;
    }
    let mut batch = TickBatch::new(tick);
    batch.append_bank(&visual);
    batch.append_bank(&transient);
    batch.hit_player.extend(combat.hit_player);
    batch.hit_entity.extend(combat.hit_entity);
    batch.fire.extend(combat.fire);
    batch.break_block = break_block;
    batch.place_block = place_block;
    Some(batch)
}

fn forward_family(kind: StreamKind, bytes: &[u8]) -> bool {
    let Some(outbox) = TICK_PUMP_OUTBOX.get() else {
        return false;
    };
    if outbox.peers.is_empty() {
        return false;
    }
    let mut sent = false;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: *peer,
            header: StreamHeader::new(kind, None),
            bytes: bytes.to_vec(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = true;
        }
    }
    sent
}

#[must_use]
pub fn pump_staged_tick(server: &Server) -> usize {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0);
    let tick = super::cluster::disciplined_tick_stamp(millis);
    let Some(batch) = staged_tick_batch(tick) else {
        return 0;
    };
    if !server.advanced_config.cluster.enabled {
        return 0;
    }
    let families = [
        (
            !batch.armor.is_empty()
                || !batch.held.is_empty()
                || !batch.sneak.is_empty()
                || !batch.sprint.is_empty()
                || !batch.blocking.is_empty()
                || !batch.swing.is_empty()
                || !batch.skin.is_empty(),
            StreamKind::PlayerVisual,
        ),
        (
            !batch.eat_start.is_empty()
                || !batch.eat_abort.is_empty()
                || !batch.break_anim.is_empty(),
            StreamKind::PlayerTransient,
        ),
        (
            !batch.break_block.is_empty() || !batch.place_block.is_empty(),
            StreamKind::PlayerWorld,
        ),
        (
            !batch.hit_player.is_empty()
                || !batch.hit_entity.is_empty()
                || !batch.fire.is_empty(),
            StreamKind::PlayerCombat,
        ),
    ];
    let bytes = match encode_batch(&batch) {
        Ok(bytes) => bytes,
        Err(error) => {
            TICK_PUMP_DROPPED.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, "cluster tick pump encode failed");
            return 0;
        }
    };
    let mut forwarded = 0;
    for (live, kind) in families {
        if live {
            if forward_family(kind, &bytes) {
                forwarded += 1;
                TICK_PUMP_FORWARDED.fetch_add(1, Ordering::Relaxed);
            } else {
                TICK_PUMP_DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    forwarded
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::PlayerSlot;

    fn reset_staged_banks() {
        let _ = staged_tick_batch(TickStamp(0));
    }

    fn gid(server: u16, player: u16) -> pumpkin_cluster::identity::GlobalPlayerId {
        pumpkin_cluster::identity::GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn empty_banks_yield_no_batch() {
        reset_staged_banks();
        assert!(staged_tick_batch(TickStamp(1)).is_none());
    }

    #[test]
    fn staged_visual_update_lands_in_batch() {
        reset_staged_banks();
        pumpkin_cluster::visual::emit_swing(Some(gid(3, 4)), TickStamp(9), 0);
        let batch = staged_tick_batch(TickStamp(9)).expect("staged swing");
        assert_eq!(batch.swing.len(), 1);
        assert!(batch.hit_player.is_empty());
        assert!(batch.eat_start.is_empty());
        assert!(staged_tick_batch(TickStamp(9)).is_none());
    }

    #[test]
    fn staged_combat_update_lands_in_batch() {
        reset_staged_banks();
        let attacker = gid(3, 4);
        let target = gid(3, 5);
        let seq = pumpkin_cluster::combat::next_combat_seq(attacker);
        pumpkin_cluster::combat::stage_hit_player(pumpkin_cluster::combat::capture_hit_player(
            attacker,
            seq,
            TickStamp(9),
            target,
            500,
        ));
        let batch = staged_tick_batch(TickStamp(9)).expect("staged hit");
        assert_eq!(batch.hit_player.len(), 1);
        assert!(batch.swing.is_empty());
    }

    #[test]
    fn staged_world_updates_land_in_batch() {
        reset_staged_banks();
        let player = gid(3, 4);
        JavaClient::stage_break_for_test(pumpkin_cluster::break_emit::capture_break(
            player,
            pumpkin_cluster::identity::PlayerSeq(2),
            TickStamp(9),
            pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 2 },
            17,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));
        JavaClient::stage_place_for_test(pumpkin_cluster::place_emit::capture_place(
            player,
            pumpkin_cluster::identity::PlayerSeq(3),
            TickStamp(9),
            pumpkin_cluster::protocol::BlockPos { x: 2, y: 64, z: 2 },
            18,
            pumpkin_cluster::inventory::INV_MAIN,
            0,
            1,
            1,
            0,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));
        let batch = staged_tick_batch(TickStamp(9)).expect("staged world updates");
        assert_eq!(batch.break_block.len(), 1);
        assert_eq!(batch.place_block.len(), 1);
    }
}
