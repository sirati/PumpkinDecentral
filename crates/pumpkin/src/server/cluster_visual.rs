use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use pumpkin_cluster::protocol::{
    BlockingUpdate, EquipmentUpdate, HeldUpdate, PoseUpdate, SkinLayersUpdate, SneakUpdate,
    SprintUpdate, StreamKind, SwingUpdate,
};
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_util::Hand;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use crate::entity::EntityBase;
use crate::entity::cluster_player_ghost::RemotePlayerReplica;
use crate::entity::player::Player;

static VISUAL_BATCHES: AtomicU64 = AtomicU64::new(0);
static VISUAL_APPLIED: AtomicU64 = AtomicU64::new(0);
static VISUAL_DROPPED: AtomicU64 = AtomicU64::new(0);

pub fn emit_player_snapshot(player: &Player) {
    let Some(gid) = player.cluster_gid() else {
        return;
    };
    let tick =
        pumpkin_cluster::visual::tick_from_counter(player.tick_counter.load(Ordering::Relaxed));
    let inventory = player.inventory();
    let selected = inventory.get_selected_slot();
    pumpkin_cluster::visual::emit_held(Some(gid), tick, selected);
    let slots = [
        (0, usize::from(selected)),
        (
            1,
            pumpkin_inventory::player::player_inventory::PlayerInventory::OFF_HAND_SLOT,
        ),
        (2, 36),
        (3, 37),
        (4, 38),
        (5, 39),
    ];
    for (wire_slot, inventory_slot) in slots {
        let stack = inventory.get_slot(inventory_slot);
        pumpkin_cluster::visual::emit_equipment(
            Some(gid),
            tick,
            wire_slot,
            if stack.is_empty() { 0 } else { stack.item.id },
            stack.item_count,
        );
    }
    let entity = player.get_entity();
    pumpkin_cluster::visual::emit_sneak(Some(gid), tick, entity.is_sneaking());
    pumpkin_cluster::visual::emit_sprint(Some(gid), tick, entity.is_sprinting());
    pumpkin_cluster::visual::emit_pose(
        Some(gid),
        tick,
        entity.is_swimming(),
        entity.is_fall_flying(),
    );
    pumpkin_cluster::visual::emit_skin(Some(gid), tick, player.config.load().skin_parts);
}

/// Returns how many visual batches have been received from the mesh.
#[must_use]
pub fn visual_batches() -> u64 {
    VISUAL_BATCHES.load(Ordering::Relaxed)
}

/// Returns how many visual updates have been applied to local player entities.
#[must_use]
pub fn visual_applied() -> u64 {
    VISUAL_APPLIED.load(Ordering::Relaxed)
}

/// Returns how many visual updates have been dropped (echo, stale, unknown player, invalid payload).
#[must_use]
pub fn visual_dropped() -> u64 {
    VISUAL_DROPPED.load(Ordering::Relaxed)
}

fn note_applied() {
    VISUAL_APPLIED.fetch_add(1, Ordering::Relaxed);
}

fn note_dropped(reason: &str) {
    VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
    debug!(reason = reason, "cluster visual update dropped");
}

/// Per-peer visual apply state.
///
/// All visual updates emitted for one player share a single sequence counter
/// (see `pumpkin_cluster::visual`), so one last-seen sequence per
/// [`GlobalPlayerId`] orders armor, held, sneak, sprint, blocking, swing and
/// skin updates against each other.
#[derive(Debug, Default)]
pub struct VisualApplyState {
    last_seq: HashMap<(GlobalPlayerId, u8), PlayerSeq>,
}

impl VisualApplyState {
    /// Creates empty visual apply state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn is_fresh(&mut self, gid: GlobalPlayerId, kind: u8, seq: PlayerSeq) -> bool {
        let key = (gid, kind);
        match self.last_seq.get(&key) {
            Some(seen) if !seq.is_newer_than(*seen) => false,
            _ => {
                self.last_seq.insert(key, seq);
                true
            }
        }
    }
}

fn find_replica(gid: GlobalPlayerId) -> Option<Arc<RemotePlayerReplica>> {
    super::cluster_ghost::remote_player_replica(gid)
}

pub fn apply_equipment(state: &mut VisualApplyState, local: ServerId, update: &EquipmentUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 0, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    let equipment_slot = match update.slot {
        0 => EquipmentSlot::MAIN_HAND,
        1 => EquipmentSlot::OFF_HAND,
        2 => EquipmentSlot::FEET,
        3 => EquipmentSlot::LEGS,
        4 => EquipmentSlot::CHEST,
        5 => EquipmentSlot::HEAD,
        _ => {
            note_dropped("slot");
            return;
        }
    };
    let stack = if update.item == 0 || update.count == 0 {
        ItemStack::EMPTY.clone()
    } else {
        let Some(item) = Item::from_id(update.item) else {
            note_dropped("item");
            return;
        };
        ItemStack::new(update.count, item)
    };
    replica
        .living_entity
        .send_equipment_changes(&[(equipment_slot, stack)]);
    note_applied();
}

/// Applies a remote held-slot update to the matching local player entity.
pub fn apply_held(state: &mut VisualApplyState, local: ServerId, update: &HeldUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 1, update.seq) {
        note_dropped("stale");
        return;
    }
    if update.slot > 8 {
        note_dropped("slot");
        return;
    }
    let Some(_replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    note_applied();
}

/// Applies a remote sneak update to the matching local player entity.
pub fn apply_sneak(state: &mut VisualApplyState, local: ServerId, update: &SneakUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 2, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    replica.get_entity().set_sneaking(update.active);
    note_applied();
}

/// Applies a remote sprint update to the matching local player entity.
pub fn apply_sprint(state: &mut VisualApplyState, local: ServerId, update: &SprintUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 3, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    replica.get_entity().set_sprinting(update.active);
    note_applied();
}

pub fn apply_pose(state: &mut VisualApplyState, local: ServerId, update: &PoseUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 7, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    let entity = replica.get_entity();
    entity.set_swimming(update.swimming);
    if entity.is_fall_flying() != update.gliding {
        entity.set_fall_flying(update.gliding);
    }
    note_applied();
}

/// Applies a remote blocking update to the matching local player entity.
///
/// Blocking is mirrored through the ghost's own main-hand item so shield
/// blocking renders for local viewers; stopping clears the active hand.
pub fn apply_blocking(state: &mut VisualApplyState, local: ServerId, update: &BlockingUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 4, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    if update.active {
        let stack = ItemStack::EMPTY.clone();
        replica.living_entity.set_active_hand(Hand::Right, stack, 0);
    } else {
        replica.living_entity.clear_active_hand();
    }
    note_applied();
}

/// Applies a remote swing update to the matching local player entity.
///
/// `hand` follows the emit mapping where `0` is the main arm and `1` is the
/// off hand.
pub fn apply_swing(state: &mut VisualApplyState, local: ServerId, update: &SwingUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 5, update.seq) {
        note_dropped("stale");
        return;
    }
    let hand = match update.hand {
        0 => Hand::Right,
        1 => Hand::Left,
        _ => {
            note_dropped("hand");
            return;
        }
    };
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    match hand {
        Hand::Right => replica.living_entity.swing_hand(),
        Hand::Left => replica.living_entity.swing_off_hand(),
    }
    note_applied();
}

/// Applies a remote skin-layers update to the matching local player entity.
pub fn apply_skin(state: &mut VisualApplyState, local: ServerId, update: &SkinLayersUpdate) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, 6, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(replica) = find_replica(update.gid) else {
        note_dropped("unknown");
        return;
    };
    replica.get_entity().set_synced_data(
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMISATION,
        update.mask,
    );
    replica.get_entity().set_synced_data(
        pumpkin_data::tracked_data::player::PLAYER_MODE_CUSTOMIZATION_ID,
        update.mask,
    );
    note_applied();
}

/// Decodes one visual batch and applies every visual update it carries.
pub fn apply_batch_bytes(state: &mut VisualApplyState, local: ServerId, bytes: &[u8]) {
    let batch = match decode_batch(bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster visual batch decode failed");
            note_dropped("decode");
            return;
        }
    };
    for update in &batch.equipment {
        apply_equipment(state, local, update);
    }
    for update in &batch.held {
        apply_held(state, local, update);
    }
    for update in &batch.sneak {
        apply_sneak(state, local, update);
    }
    for update in &batch.sprint {
        apply_sprint(state, local, update);
    }
    for update in &batch.pose {
        apply_pose(state, local, update);
    }
    for update in &batch.blocking {
        apply_blocking(state, local, update);
    }
    for update in &batch.swing {
        apply_swing(state, local, update);
    }
    for update in &batch.skin {
        apply_skin(state, local, update);
    }
}

fn apply_parcel(state: &mut VisualApplyState, local: ServerId, parcel: &InboundParcel) {
    if parcel.header.kind != StreamKind::PlayerVisual {
        note_dropped("kind");
        return;
    }
    apply_batch_bytes(state, local, &parcel.bytes);
}

pub async fn visual_apply_task(local: ServerId, mut visual_rx: mpsc::Receiver<InboundParcel>) {
    let mut state = VisualApplyState::new();
    let mut first = true;
    while let Some(parcel) = visual_rx.recv().await {
        VISUAL_BATCHES.fetch_add(1, Ordering::Relaxed);
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster visual stream started");
        }
        apply_parcel(&mut state, local, &parcel);
    }
    debug!("cluster visual stream closed");
}

pub fn spawn_visual_apply(
    server: &Arc<Server>,
    local: ServerId,
    visual_rx: mpsc::Receiver<InboundParcel>,
) {
    server.spawn_task(visual_apply_task(local, visual_rx));
}
