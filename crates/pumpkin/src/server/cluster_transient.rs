use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::{BreakAnimUpdate, EatAbortUpdate, EatStartUpdate, StreamKind};
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_cluster::transient::{BreakAnimAction, RemoteBreakAnims, RemoteEating};
use pumpkin_inventory::player::player_inventory::PlayerInventory;
use pumpkin_util::Hand;
use pumpkin_util::math::position::BlockPos;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use crate::entity::player::Player;
use crate::world::BlockBreakingProgress;

static TRANSIENT_BATCHES: AtomicU64 = AtomicU64::new(0);
static TRANSIENT_APPLIED: AtomicU64 = AtomicU64::new(0);
static TRANSIENT_DROPPED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn transient_batches() -> u64 {
    TRANSIENT_BATCHES.load(Ordering::Relaxed)
}

#[must_use]
pub fn transient_applied() -> u64 {
    TRANSIENT_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn transient_dropped() -> u64 {
    TRANSIENT_DROPPED.load(Ordering::Relaxed)
}

fn note_applied() {
    TRANSIENT_APPLIED.fetch_add(1, Ordering::Relaxed);
}

fn note_dropped(reason: &str) {
    TRANSIENT_DROPPED.fetch_add(1, Ordering::Relaxed);
    debug!(reason = reason, "cluster transient update dropped");
}

fn find_player(server: &Arc<Server>, gid: GlobalPlayerId) -> Option<Arc<Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.cluster_gid() == Some(gid) {
                return Some(Arc::clone(player));
            }
        }
    }
    None
}

fn hand_for_slot(slot: u8) -> Hand {
    if usize::from(slot) == PlayerInventory::OFF_HAND_SLOT {
        Hand::Left
    } else {
        Hand::Right
    }
}

pub fn apply_eat_start(
    server: &Arc<Server>,
    eating: &mut HashMap<GlobalPlayerId, RemoteEating>,
    local: ServerId,
    update: &EatStartUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    let state = eating.entry(update.gid).or_insert_with(RemoteEating::new);
    if !state.apply_start(update) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    let hand = hand_for_slot(update.slot);
    let Some(stack) = player.inventory().try_get_stack_in_hand(hand) else {
        note_dropped("busy");
        return;
    };
    if !pumpkin_cluster::transient::eat_inv_precondition_met(
        update,
        stack.item.id,
        stack.item_count,
    ) {
        note_dropped("inv");
        return;
    }
    let duration = stack.get_max_use_time();
    player.living_entity.set_active_hand(hand, stack, duration);
    note_applied();
}

pub fn apply_eat_abort(
    server: &Arc<Server>,
    eating: &mut HashMap<GlobalPlayerId, RemoteEating>,
    local: ServerId,
    update: &EatAbortUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    let state = eating.entry(update.gid).or_insert_with(RemoteEating::new);
    if !state.apply_abort(update) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    player.living_entity.clear_active_hand();
    note_applied();
}

pub fn apply_break_anim(
    server: &Arc<Server>,
    breaks: &mut RemoteBreakAnims,
    local: ServerId,
    update: &BreakAnimUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    let Some(action) = breaks.apply_break_anim(update) else {
        note_dropped("stale");
        return;
    };
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    let world = player.world();
    match action {
        BreakAnimAction::Stage { pos, stage } => {
            world.set_block_breaking(
                &player.living_entity.entity,
                BlockPos::new(pos.x, pos.y, pos.z),
                BlockBreakingProgress::Update {
                    stage: i32::from(stage),
                    speed: None,
                },
            );
            note_applied();
        }
        BreakAnimAction::Stop { pos } => {
            world.set_block_breaking(
                &player.living_entity.entity,
                BlockPos::new(pos.x, pos.y, pos.z),
                BlockBreakingProgress::Stop,
            );
            note_applied();
        }
    }
}

pub fn apply_batch_bytes(
    server: &Arc<Server>,
    eating: &mut HashMap<GlobalPlayerId, RemoteEating>,
    breaks: &mut RemoteBreakAnims,
    local: ServerId,
    bytes: &[u8],
) {
    let batch = match decode_batch(bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster transient batch decode failed");
            note_dropped("decode");
            return;
        }
    };
    for update in &batch.eat_start {
        apply_eat_start(server, eating, local, update);
    }
    for update in &batch.eat_abort {
        apply_eat_abort(server, eating, local, update);
    }
    for update in &batch.break_anim {
        apply_break_anim(server, breaks, local, update);
    }
}

fn apply_parcel(
    server: &Arc<Server>,
    eating: &mut HashMap<GlobalPlayerId, RemoteEating>,
    breaks: &mut RemoteBreakAnims,
    local: ServerId,
    parcel: &InboundParcel,
) {
    if parcel.header.kind != StreamKind::PlayerTransient {
        note_dropped("kind");
        return;
    }
    apply_batch_bytes(server, eating, breaks, local, &parcel.bytes);
}

pub async fn transient_apply_task(
    server: Arc<Server>,
    local: ServerId,
    mut transient_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut eating: HashMap<GlobalPlayerId, RemoteEating> = HashMap::new();
    let mut breaks = RemoteBreakAnims::new();
    let mut first = true;
    while let Some(parcel) = transient_rx.recv().await {
        TRANSIENT_BATCHES.fetch_add(1, Ordering::Relaxed);
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster transient stream started");
        }
        apply_parcel(&server, &mut eating, &mut breaks, local, &parcel);
    }
    debug!("cluster transient stream closed");
}

/// Spawns the background task that applies incoming transient batches.
///
/// Fresh eat updates start or abort eating on the matching remote player's
/// ghost, and fresh break animation updates start or stop the block-break
/// animation at the reported position.
pub fn spawn_transient_apply(
    server: &Arc<Server>,
    local: ServerId,
    transient_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(transient_apply_task(task_server, local, transient_rx));
}
