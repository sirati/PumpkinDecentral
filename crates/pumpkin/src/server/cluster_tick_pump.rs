use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::codec::encode_batch;
use pumpkin_cluster::combat::drain_ordered_combat;
use pumpkin_cluster::completion::LocalTickBatch;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::{ChunkKey, PlayerVisualSnapshot, StreamKind, TickBatch};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::transient::drain_transient_bank;
use pumpkin_cluster::visual::drain_visual_bank;
use tokio::sync::mpsc;

use super::Server;
use crate::net::java::JavaClient;

const ORDER_SEED: u64 = 0;

struct TickPumpOutbox {
    local_world: mpsc::Sender<LocalTickBatch>,
}

static TICK_PUMP_OUTBOX: OnceLock<TickPumpOutbox> = OnceLock::new();
static TICK_PUMP_FORWARDED: AtomicU64 = AtomicU64::new(0);
static TICK_PUMP_DROPPED: AtomicU64 = AtomicU64::new(0);
static PLAYER_VISUAL_SNAPSHOTS: LazyLock<
    arc_swap::ArcSwap<HashMap<GlobalPlayerId, PlayerVisualSnapshot>>,
> = LazyLock::new(|| arc_swap::ArcSwap::from_pointee(HashMap::new()));

thread_local! {
    static PLAYER_CHUNK_TRANSITIONS: RefCell<Vec<pumpkin_cluster::protocol::PlayerChunkTransition>> =
        const { RefCell::new(Vec::new()) };
}

pub fn stage_player_chunk_transition(transition: pumpkin_cluster::protocol::PlayerChunkTransition) {
    PLAYER_CHUNK_TRANSITIONS.with_borrow_mut(|transitions| transitions.push(transition));
}

pub struct PlayerVisualBase {
    pub gid: GlobalPlayerId,
    pub sneaking: bool,
    pub sprinting: bool,
    pub swimming: bool,
    pub gliding: bool,
    pub skin_layers: u8,
    pub game_mode: u8,
}

#[must_use]
pub fn player_visual_snapshot(gid: GlobalPlayerId) -> PlayerVisualSnapshot {
    PLAYER_VISUAL_SNAPSHOTS
        .load()
        .get(&gid)
        .copied()
        .unwrap_or_default()
}

pub fn publish_player_visual_bases(bases: Vec<PlayerVisualBase>) {
    let mut snapshots = PLAYER_VISUAL_SNAPSHOTS.load().as_ref().clone();
    for base in bases {
        let snapshot = snapshots.entry(base.gid).or_default();
        snapshot.sneaking = base.sneaking;
        snapshot.sprinting = base.sprinting;
        snapshot.swimming = base.swimming;
        snapshot.gliding = base.gliding;
        snapshot.skin_layers = base.skin_layers;
        snapshot.game_mode = base.game_mode;
    }
    PLAYER_VISUAL_SNAPSHOTS.store(Arc::new(snapshots));
}

fn publish_visual_snapshots(batch: &TickBatch) {
    let mut snapshots = PLAYER_VISUAL_SNAPSHOTS.load().as_ref().clone();
    for update in &batch.equipment {
        if let Some(slot) = snapshots
            .entry(update.gid)
            .or_default()
            .equipment
            .get_mut(usize::from(update.slot))
        {
            slot.item = update.item;
            slot.count = update.count;
        }
    }
    for update in &batch.held {
        snapshots.entry(update.gid).or_default().held_slot = update.slot;
    }
    for update in &batch.sneak {
        snapshots.entry(update.gid).or_default().sneaking = update.active;
    }
    for update in &batch.sprint {
        snapshots.entry(update.gid).or_default().sprinting = update.active;
    }
    for update in &batch.pose {
        let snapshot = snapshots.entry(update.gid).or_default();
        snapshot.swimming = update.swimming;
        snapshot.gliding = update.gliding;
    }
    for update in &batch.blocking {
        snapshots.entry(update.gid).or_default().blocking = update.active;
    }
    for update in &batch.skin {
        snapshots.entry(update.gid).or_default().skin_layers = update.mask;
    }
    PLAYER_VISUAL_SNAPSHOTS.store(Arc::new(snapshots));
}

#[must_use]
pub fn tick_pump_forwarded() -> u64 {
    TICK_PUMP_FORWARDED.load(Ordering::Relaxed)
}

#[must_use]
pub fn tick_pump_dropped() -> u64 {
    TICK_PUMP_DROPPED.load(Ordering::Relaxed)
}

pub fn install_tick_pump_outbox(local_world: mpsc::Sender<LocalTickBatch>) {
    let _ = TICK_PUMP_OUTBOX.set(TickPumpOutbox { local_world });
}

fn staged_tick_batch(tick: TickStamp) -> Option<TickBatch> {
    let visual = drain_visual_bank();
    let transient = drain_transient_bank();
    let combat = drain_ordered_combat(ORDER_SEED, tick);
    let break_block = JavaClient::drain_break_outbox();
    let place_block = JavaClient::drain_place_outbox();
    let player_chunk_transition = PLAYER_CHUNK_TRANSITIONS.with_borrow_mut(core::mem::take);
    if visual.is_empty()
        && transient.is_empty()
        && combat.is_empty()
        && break_block.is_empty()
        && place_block.is_empty()
        && player_chunk_transition.is_empty()
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
    batch.player_chunk_transition = player_chunk_transition
        .into_iter()
        .map(|mut transition| {
            transition.tick = tick;
            transition
        })
        .collect();
    Some(batch)
}

fn route_family(
    batch: &TickBatch,
    kind: StreamKind,
    player_holders: impl Fn(GlobalPlayerId) -> Vec<u16>,
    chunk_holders: impl Fn(ChunkKey) -> Vec<u16>,
) -> Vec<(ServerId, Option<GlobalPlayerId>, TickBatch)> {
    let mut routed: BTreeMap<(u16, Option<GlobalPlayerId>), TickBatch> = BTreeMap::new();
    let player_targets = |gid: GlobalPlayerId| player_holders(gid);
    let chunk_targets = |chunk: ChunkKey| chunk_holders(chunk);
    macro_rules! route_player_updates {
        ($field:ident) => {
            for update in &batch.$field {
                for peer in player_targets(update.gid) {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .$field
                        .push(update.clone());
                }
            }
        };
    }
    match kind {
        StreamKind::PlayerBoundary => {
            for update in &batch.player_chunk_transition {
                let mut peers = chunk_targets(update.from);
                peers.extend(chunk_targets(update.to));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .player_chunk_transition
                        .push(*update);
                }
            }
        }
        StreamKind::PlayerVisual => {
            route_player_updates!(equipment);
            route_player_updates!(held);
            route_player_updates!(sneak);
            route_player_updates!(sprint);
            route_player_updates!(pose);
            route_player_updates!(blocking);
            route_player_updates!(swing);
            route_player_updates!(skin);
        }
        StreamKind::PlayerTransient => {
            route_player_updates!(eat_start);
            route_player_updates!(eat_abort);
            route_player_updates!(break_anim);
        }
        StreamKind::PlayerWorld => {
            for update in &batch.break_block {
                let mut peers = chunk_targets(update.actor_chunk);
                peers.extend(chunk_targets(update.chunk));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .break_block
                        .push(*update);
                }
            }
            for update in &batch.place_block {
                let mut peers = chunk_targets(update.actor_chunk);
                peers.extend(chunk_targets(update.chunk));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .place_block
                        .push(*update);
                }
            }
            for update in &batch.inv_ops {
                let ActorId::Player(gid) = update.actor else {
                    continue;
                };
                let mut peers: Vec<_> = update
                    .affected
                    .iter()
                    .flat_map(|chunk| chunk_targets(*chunk))
                    .collect();
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .inv_ops
                        .push(update.clone());
                }
            }
            for update in &batch.player_administrative {
                let mut peers: Vec<_> = update
                    .affected
                    .iter()
                    .flat_map(|chunk| chunk_targets(*chunk))
                    .collect();
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.target)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .player_administrative
                        .push(update.clone());
                }
            }
            for update in &batch.item_action {
                let ActorId::Player(gid) = update.actor else {
                    continue;
                };
                let mut peers: Vec<_> = update
                    .affected
                    .iter()
                    .flat_map(|chunk| chunk_targets(*chunk))
                    .collect();
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .item_action
                        .push(update.clone());
                }
            }
        }
        StreamKind::EntityWorld => {
            for update in &batch.entity_chunk_transition {
                let mut peers = chunk_targets(update.from);
                peers.extend(chunk_targets(update.to));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, None))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .entity_chunk_transition
                        .push(*update);
                }
            }
            for update in &batch.administrative {
                for peer in chunk_targets(update.chunk) {
                    routed
                        .entry((peer, None))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .administrative
                        .push(update.clone());
                }
            }
            for update in &batch.inv_ops {
                if !matches!(update.actor, ActorId::Entity(_)) {
                    continue;
                }
                let mut peers: Vec<_> = update
                    .affected
                    .iter()
                    .flat_map(|chunk| chunk_targets(*chunk))
                    .collect();
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, None))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .inv_ops
                        .push(update.clone());
                }
            }
            for update in &batch.item_action {
                if !matches!(update.actor, ActorId::Entity(_)) {
                    continue;
                }
                let mut peers: Vec<_> = update
                    .affected
                    .iter()
                    .flat_map(|chunk| chunk_targets(*chunk))
                    .collect();
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, None))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .item_action
                        .push(update.clone());
                }
            }
        }
        StreamKind::PlayerCombat => {
            for update in &batch.hit_player {
                let mut peers = chunk_targets(update.actor_chunk);
                peers.extend(chunk_targets(update.target_chunk));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .hit_player
                        .push(*update);
                }
            }
            for update in &batch.hit_entity {
                let mut peers = chunk_targets(update.actor_chunk);
                peers.extend(chunk_targets(update.target.chunk));
                peers.sort_unstable();
                peers.dedup();
                for peer in peers {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .hit_entity
                        .push(*update);
                }
            }
            for update in &batch.fire {
                for peer in chunk_targets(update.actor_chunk) {
                    routed
                        .entry((peer, Some(update.gid)))
                        .or_insert_with(|| TickBatch::new(batch.tick))
                        .fire
                        .push(*update);
                }
            }
        }
        _ => {}
    }
    routed
        .into_iter()
        .map(|((peer, gid), batch)| (ServerId(peer), gid, batch))
        .collect()
}

#[must_use]
pub fn pump_staged_tick(server: &Server) -> usize {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0);
    let tick = super::cluster::disciplined_tick_stamp(millis);
    if !server.advanced_config.cluster.enabled {
        return 0;
    }
    let Some(outbox) = TICK_PUMP_OUTBOX.get() else {
        return 0;
    };
    let Ok(permit) = outbox.local_world.try_reserve() else {
        return 0;
    };
    let batch = staged_tick_batch(tick).unwrap_or_else(|| TickBatch::new(tick));
    let actor_intents = server.cluster_controller.drain_actor_intents();
    let view_intents = server.cluster_controller.drain_view_intents();
    permit.send(LocalTickBatch {
        batch: batch.clone(),
        actor_intents,
        view_intents,
    });
    publish_visual_snapshots(&batch);
    0
}

pub(super) fn routed_tick_parcels(
    batch: &TickBatch,
    connected: &HashSet<ServerId>,
) -> (Vec<OutboundParcel>, Vec<ServerId>) {
    if batch.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let families = [
        StreamKind::PlayerBoundary,
        StreamKind::PlayerVisual,
        StreamKind::PlayerTransient,
        StreamKind::PlayerWorld,
        StreamKind::PlayerCombat,
        StreamKind::EntityWorld,
    ];
    let mut parcels = Vec::new();
    let mut failed = Vec::new();
    for kind in families {
        for (peer, gid, routed) in route_family(
            batch,
            kind,
            super::cluster_datagram::holders_for_player,
            super::cluster_datagram::holders_for_chunk,
        ) {
            if !connected.contains(&peer) {
                continue;
            }
            match encode_batch(&routed) {
                Ok(bytes) => parcels.push(OutboundParcel {
                    peer,
                    header: StreamHeader::new(kind, gid),
                    bytes,
                }),
                Err(_) => {
                    failed.push(peer);
                    TICK_PUMP_DROPPED.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    failed.sort_unstable();
    failed.dedup();
    TICK_PUMP_FORWARDED.fetch_add(parcels.len() as u64, Ordering::Relaxed);
    (parcels, failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::PlayerSlot;

    const WORLD: pumpkin_cluster::protocol::WorldId = pumpkin_cluster::protocol::WorldId::new(0, 7);

    const fn chunk(x: i32, z: i32) -> ChunkKey {
        ChunkKey::new(WORLD, x, z)
    }

    fn gid(server: u16, player: u16) -> pumpkin_cluster::identity::GlobalPlayerId {
        pumpkin_cluster::identity::GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn reliable_visual_batches_are_isolated_by_actor_and_holder() {
        let first = gid(3, 4);
        let second = gid(3, 5);
        let mut batch = TickBatch::new(TickStamp(9));
        batch.swing.push(pumpkin_cluster::protocol::SwingUpdate {
            gid: first,
            seq: pumpkin_cluster::identity::PlayerSeq(1),
            tick: TickStamp(9),
            hand: 0,
        });
        batch.swing.push(pumpkin_cluster::protocol::SwingUpdate {
            gid: second,
            seq: pumpkin_cluster::identity::PlayerSeq(1),
            tick: TickStamp(9),
            hand: 1,
        });

        let routed = route_family(
            &batch,
            StreamKind::PlayerVisual,
            |actor| if actor == first { vec![2, 3] } else { vec![3] },
            |_| Vec::new(),
        );

        assert_eq!(routed.len(), 3);
        for (peer, actor, batch) in routed {
            assert!(peer == ServerId(2) || peer == ServerId(3));
            assert!(batch.swing.iter().all(|update| Some(update.gid) == actor));
            if peer == ServerId(2) {
                assert_eq!(actor, Some(first));
            }
        }
    }

    #[test]
    fn boundary_world_action_reaches_both_chunk_holder_sets_once() {
        let actor = gid(3, 4);
        let mut batch = TickBatch::new(TickStamp(9));
        batch
            .break_block
            .push(pumpkin_cluster::break_emit::capture_break(
                actor,
                pumpkin_cluster::identity::PlayerSeq(1),
                TickStamp(9),
                pumpkin_cluster::protocol::BlockPos { x: 16, y: 64, z: 0 },
                17,
                chunk(0, 0),
                chunk(1, 0),
            ));

        let routed = route_family(
            &batch,
            StreamKind::PlayerWorld,
            |_| Vec::new(),
            |chunk| {
                if chunk.x() == 0 {
                    vec![2, 3]
                } else {
                    vec![3, 4]
                }
            },
        );
        let peers: Vec<_> = routed.iter().map(|(peer, _, _)| peer.0).collect();
        assert_eq!(peers, vec![2, 3, 4]);
        assert!(routed.iter().all(
            |(_, routed_actor, routed_batch)| *routed_actor == Some(actor)
                && routed_batch.break_block.len() == 1
        ));
    }
}
