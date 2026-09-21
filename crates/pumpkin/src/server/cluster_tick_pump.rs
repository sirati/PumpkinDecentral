use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::codec::encode_batch;
use pumpkin_cluster::combat::drain_ordered_combat;
use pumpkin_cluster::identity::{ActionActor, GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::{ChunkAddr, StreamKind, TickBatch};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::transient::drain_transient_bank;
use pumpkin_cluster::visual::drain_visual_bank;
use tokio::sync::mpsc;

use super::Server;

const ORDER_SEED: u64 = 0;

struct TickPumpOutbox {
    peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static TICK_PUMP_OUTBOX: OnceLock<TickPumpOutbox> = OnceLock::new();
static TICK_PUMP_FORWARDED: AtomicU64 = AtomicU64::new(0);
static TICK_PUMP_DROPPED: AtomicU64 = AtomicU64::new(0);
static COMBAT_UNROUTABLE_WARN_AT: AtomicU64 = AtomicU64::new(0);

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
    if visual.is_empty()
        && transient.is_empty()
        && combat.is_empty()
    {
        return None;
    }
    let mut batch = TickBatch::new(tick);
    batch.append_bank(&visual);
    batch.append_bank(&transient);
    batch.attacks.extend(combat.attacks);
    batch.fire.extend(combat.fire);
    batch.entity_mutations.extend(combat.entity_mutations);
    Some(batch)
}

fn forward_family(kind: StreamKind, player: GlobalPlayerId, bytes: &[u8]) -> bool {
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
            header: StreamHeader::new(kind, Some(player)),
            bytes: bytes.to_vec(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = true;
        }
    }
    sent
}

fn forward_player_batches(
    server: &Server,
    kind: StreamKind,
    batches: BTreeMap<GlobalPlayerId, TickBatch>,
) -> usize {
    let mut forwarded = 0;
    for (player, batch) in batches {
        let bytes = match encode_batch(&batch) {
            Ok(bytes) => bytes,
            Err(error) => {
                TICK_PUMP_DROPPED.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(%error, ?kind, ?player, "cluster tick pump encode failed");
                continue;
            }
        };
        let sent = match kind {
            StreamKind::PlayerCombat => forward_combat_batch(server, player, &batch, &bytes),
            _ => forward_family(kind, player, &bytes),
        };
        if sent {
            forwarded += 1;
            TICK_PUMP_FORWARDED.fetch_add(1, Ordering::Relaxed);
        } else {
            TICK_PUMP_DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
    forwarded
}

pub fn combat_chunks(batch: &TickBatch) -> Vec<ChunkAddr> {
    let mut chunks: Vec<_> = batch
        .attacks
        .iter()
        .flat_map(|attack| attack.targets().map(|target| target.target.chunk()))
        .chain(batch.fire.iter().map(|update| update.chunk))
        .chain(batch.entity_mutations.iter().map(|update| update.chunk()))
        .collect();
    chunks.sort();
    chunks.dedup();
    chunks
}

fn warn_unroutable_combat(player: GlobalPlayerId) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |age| age.as_millis() as u64);
    let previous = COMBAT_UNROUTABLE_WARN_AT.load(Ordering::Relaxed);
    if now.saturating_sub(previous) < 60_000 {
        return;
    }
    if COMBAT_UNROUTABLE_WARN_AT
        .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        tracing::warn!(?player, "cluster combat action has no holder target");
    }
}

fn forward_combat_batch(
    server: &Server,
    player: GlobalPlayerId,
    batch: &TickBatch,
    bytes: &[u8],
) -> bool {
    let Some(outbox) = TICK_PUMP_OUTBOX.get() else {
        return false;
    };
    let local = ServerId(server.advanced_config.cluster.server_id);
    let mut targets = Vec::new();
    for chunk in combat_chunks(batch) {
        targets.extend(
            super::cluster::chunk_holders(chunk)
                .into_iter()
                .map(ServerId),
        );
    }
    targets.retain(|target| *target != local);
    targets.sort_unstable();
    targets.dedup();
    if targets.is_empty() {
        warn_unroutable_combat(player);
        return false;
    }
    let mut sent = false;
    for peer in targets {
        let parcel = OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::PlayerCombat, Some(player)),
            bytes: bytes.to_vec(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = true;
        } else {
            tracing::warn!(peer = peer.0, ?player, "cluster combat action delivery queue full");
        }
    }
    sent
}

macro_rules! append_player_updates {
    ($batches:expr, $tick:expr, $updates:expr, $field:ident) => {
        for update in $updates {
            $batches
                .entry(update.gid)
                .or_insert_with(|| TickBatch::new($tick))
                .$field
                .push(update.clone());
        }
    };
}

fn player_batches_for_family(batch: &TickBatch, kind: StreamKind) -> BTreeMap<GlobalPlayerId, TickBatch> {
    let mut batches = BTreeMap::new();
    match kind {
        StreamKind::PlayerVisual => {
            append_player_updates!(batches, batch.tick, &batch.armor, armor);
            append_player_updates!(batches, batch.tick, &batch.held, held);
            append_player_updates!(batches, batch.tick, &batch.sneak, sneak);
            append_player_updates!(batches, batch.tick, &batch.sprint, sprint);
            append_player_updates!(batches, batch.tick, &batch.blocking, blocking);
            append_player_updates!(batches, batch.tick, &batch.swing, swing);
            append_player_updates!(batches, batch.tick, &batch.skin, skin);
        }
        StreamKind::PlayerTransient => {
            append_player_updates!(batches, batch.tick, &batch.eat_start, eat_start);
            append_player_updates!(batches, batch.tick, &batch.eat_abort, eat_abort);
            append_player_updates!(batches, batch.tick, &batch.break_anim, break_anim);
        }
        StreamKind::PlayerWorld => {
            append_player_updates!(batches, batch.tick, &batch.break_block, break_block);
            append_player_updates!(batches, batch.tick, &batch.place_block, place_block);
            append_player_updates!(batches, batch.tick, &batch.inv_ops, inv_ops);
        }
        StreamKind::PlayerCombat => {
            append_player_updates!(batches, batch.tick, &batch.fire, fire);
            for attack in &batch.attacks {
                if let ActionActor::Player(player) = attack.actor {
                    batches
                        .entry(player)
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .attacks
                        .push(attack.clone());
                }
            }
        }
        _ => {}
    }
    batches
}

fn server_combat_batches(batch: &TickBatch) -> BTreeMap<ActionActor, TickBatch> {
    let mut batches = BTreeMap::new();
    for attack in &batch.attacks {
        if matches!(attack.actor, ActionActor::Server(_)) {
            batches
                .entry(attack.actor)
                .or_insert_with(|| TickBatch::new(batch.tick))
                .attacks
                .push(attack.clone());
        }
    }
    for update in &batch.entity_mutations {
        if matches!(update.actor, ActionActor::Server(_)) {
            batches
                .entry(update.actor)
                .or_insert_with(|| TickBatch::new(batch.tick))
                .entity_mutations
                .push(*update);
        }
    }
    batches
}

#[must_use]
pub fn pump_staged_tick(server: &Server) -> usize {
    let Some(tick) = super::cluster::disciplined_tick_now() else {
        tracing::warn!("cluster staged action tick skipped before NTP discipline is available");
        return 0;
    };
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
            !batch.break_block.is_empty() || !batch.place_block.is_empty() || !batch.inv_ops.is_empty(),
            StreamKind::PlayerWorld,
        ),
        (
            !batch.attacks.is_empty()
                || !batch.fire.is_empty()
                || !batch.entity_mutations.is_empty(),
            StreamKind::PlayerCombat,
        ),
    ];
    let mut forwarded = 0;
    for (live, kind) in families {
        if live {
            let mut player_batches = player_batches_for_family(&batch, kind);
            if matches!(kind, StreamKind::PlayerWorld) {
                for local_batch in player_batches.values() {
                    super::cluster_world_apply::submit_local_optimistic_batch(local_batch.clone());
                }
                continue;
            }
            if matches!(kind, StreamKind::PlayerCombat) {
                for update in &batch.entity_mutations {
                    if let ActionActor::Player(player) = update.actor {
                        player_batches
                            .entry(player)
                            .or_insert_with(|| TickBatch::new(batch.tick))
                            .entity_mutations
                            .push(*update);
                    }
                }
                let server_batches = server_combat_batches(&batch);
                for local_batch in player_batches.values() {
                    super::cluster_world_apply::submit_local_optimistic_batch(local_batch.clone());
                }
                for local_batch in server_batches.into_values() {
                    super::cluster_world_apply::submit_local_optimistic_batch(local_batch);
                }
                continue;
            }
            forwarded += forward_player_batches(server, kind, player_batches);
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
        assert!(batch.attacks.is_empty());
        assert!(batch.eat_start.is_empty());
        assert!(staged_tick_batch(TickStamp(9)).is_none());
    }

    #[test]
    fn captured_world_updates_land_in_batch() {
        let player = gid(3, 4);
        let mut batch = TickBatch::new(TickStamp(9));
        batch.break_block.push(pumpkin_cluster::break_emit::capture_break(
            player,
            pumpkin_cluster::identity::PlayerSeq(2),
            TickStamp(9),
            pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 2 },
            17,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));
        batch.place_block.push(pumpkin_cluster::place_emit::capture_place(
            player,
            pumpkin_cluster::identity::PlayerSeq(3),
            TickStamp(9),
            pumpkin_cluster::protocol::BlockPos { x: 2, y: 64, z: 2 },
            0,
            18,
            pumpkin_cluster::inventory::INV_MAIN,
            0,
            1,
            1,
            0,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));
        assert_eq!(batch.break_block.len(), 1);
        assert_eq!(batch.place_block.len(), 1);
    }

    #[test]
    fn world_batches_keep_each_players_actions_on_its_own_stream() {
        let first = gid(3, 4);
        let second = gid(3, 5);
        let tick = TickStamp(9);
        let mut batch = TickBatch::new(tick);
        batch.break_block.push(pumpkin_cluster::break_emit::capture_break(
            first,
            pumpkin_cluster::identity::PlayerSeq(2),
            tick,
            pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 2 },
            17,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));
        batch.place_block.push(pumpkin_cluster::place_emit::capture_place(
            second,
            pumpkin_cluster::identity::PlayerSeq(3),
            tick,
            pumpkin_cluster::protocol::BlockPos { x: 2, y: 64, z: 2 },
            0,
            18,
            pumpkin_cluster::inventory::INV_MAIN,
            0,
            1,
            1,
            0,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
        ));

        let routed = player_batches_for_family(&batch, StreamKind::PlayerWorld);
        assert_eq!(routed.len(), 2);
        assert_eq!(routed[&first].break_block.len(), 1);
        assert!(routed[&first].place_block.is_empty());
        assert_eq!(routed[&second].place_block.len(), 1);
        assert!(routed[&second].break_block.is_empty());
    }
}
