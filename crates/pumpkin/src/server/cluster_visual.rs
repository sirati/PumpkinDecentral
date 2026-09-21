use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use pumpkin_cluster::protocol::{
    ArmorUpdate, BlockingUpdate, HeldUpdate, SkinLayersUpdate, SneakUpdate, SprintUpdate,
    StreamKind, SwingUpdate,
};
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_inventory::player::player_inventory::PlayerInventory;
use pumpkin_util::Hand;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;

static VISUAL_BATCHES: AtomicU64 = AtomicU64::new(0);
static VISUAL_APPLIED: AtomicU64 = AtomicU64::new(0);
static VISUAL_DROPPED: AtomicU64 = AtomicU64::new(0);

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
    last_seq: HashMap<GlobalPlayerId, PlayerSeq>,
}

impl VisualApplyState {
    /// Creates empty visual apply state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn is_fresh(&mut self, gid: GlobalPlayerId, seq: PlayerSeq) -> bool {
        match self.last_seq.get(&gid) {
            Some(seen) if !seq.is_newer_than(*seen) => false,
            _ => {
                self.last_seq.insert(gid, seq);
                true
            }
        }
    }
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

/// Applies a remote armor update to the matching local player entity.
///
/// `slot` is a [`PlayerInventory`] slot index and must resolve to an armor
/// [`EquipmentSlot`]; `item` is a raw item id where `0` clears the slot.
pub fn apply_armor(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &ArmorUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    let slot_index = usize::from(update.slot);
    let Some(equipment_slot) = player
        .inventory()
        .equipment_slots
        .get(&slot_index)
        .cloned()
    else {
        note_dropped("slot");
        return;
    };
    if !equipment_slot.is_armor_slot() {
        note_dropped("slot");
        return;
    }
    let stack = if update.item == 0 {
        ItemStack::EMPTY.clone()
    } else {
        let Some(item) = Item::from_id(update.item) else {
            note_dropped("item");
            return;
        };
        ItemStack::new(1, item)
    };
    player.inventory().set_slot(slot_index, stack.clone());
    player
        .living_entity
        .send_equipment_changes(&[(equipment_slot, stack)]);
    note_applied();
}

/// Applies a remote held-slot update to the matching local player entity.
pub fn apply_held(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &HeldUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    if !PlayerInventory::is_valid_hotbar_index(usize::from(update.slot)) {
        note_dropped("slot");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    player.inventory().set_selected_slot(update.slot);
    let stack = player.inventory().held_item();
    player
        .living_entity
        .send_equipment_changes(&[(EquipmentSlot::MAIN_HAND, stack)]);
    note_applied();
}

/// Applies a remote sneak update to the matching local player entity.
pub fn apply_sneak(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &SneakUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    player.get_entity().set_sneaking(update.active);
    player.update_player_pose();
    note_applied();
}

/// Applies a remote sprint update to the matching local player entity.
pub fn apply_sprint(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &SprintUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    player.set_sprinting(update.active);
    player.update_player_pose();
    note_applied();
}

/// Applies a remote blocking update to the matching local player entity.
///
/// Blocking is mirrored through the ghost's own main-hand item so shield
/// blocking renders for local viewers; stopping clears the active hand.
pub fn apply_blocking(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &BlockingUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    if update.active {
        let hand = Hand::Right;
        let stack = player.inventory().get_stack_in_hand(hand);
        let duration = stack.get_max_use_time();
        player
            .living_entity
            .set_active_hand(hand, stack, duration);
        player.start_using_item(hand);
    } else {
        player.living_entity.clear_active_hand();
        player.stop_using_item();
    }
    note_applied();
}

/// Applies a remote swing update to the matching local player entity.
///
/// `hand` follows the emit mapping where `0` is the main arm and `1` is the
/// off hand.
pub fn apply_swing(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &SwingUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
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
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    player.swing_hand(hand, false);
    note_applied();
}

/// Applies a remote skin-layers update to the matching local player entity.
pub fn apply_skin(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    update: &SkinLayersUpdate,
) {
    if update.gid.server == local {
        note_dropped("echo");
        return;
    }
    if !state.is_fresh(update.gid, update.seq) {
        note_dropped("stale");
        return;
    }
    let Some(player) = find_player(server, update.gid) else {
        note_dropped("unknown");
        return;
    };
    let current = player.config.load();
    if current.skin_parts != update.mask {
        let mut config = (**current).clone();
        config.skin_parts = update.mask;
        player.config.store(Arc::new(config));
        player.send_client_information();
    }
    note_applied();
}

/// Decodes one visual batch and applies every visual update it carries.
pub fn apply_batch_bytes(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    bytes: &[u8],
) {
    let batch = match decode_batch(bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster visual batch decode failed");
            note_dropped("decode");
            return;
        }
    };
    for update in &batch.armor {
        apply_armor(server, state, local, update);
    }
    for update in &batch.held {
        apply_held(server, state, local, update);
    }
    for update in &batch.sneak {
        apply_sneak(server, state, local, update);
    }
    for update in &batch.sprint {
        apply_sprint(server, state, local, update);
    }
    for update in &batch.blocking {
        apply_blocking(server, state, local, update);
    }
    for update in &batch.swing {
        apply_swing(server, state, local, update);
    }
    for update in &batch.skin {
        apply_skin(server, state, local, update);
    }
}

fn apply_parcel(
    server: &Arc<Server>,
    state: &mut VisualApplyState,
    local: ServerId,
    parcel: &InboundParcel,
) {
    if parcel.header.kind != StreamKind::PlayerVisual {
        note_dropped("kind");
        return;
    }
    apply_batch_bytes(server, state, local, &parcel.bytes);
}

/// Runs the background task that applies incoming visual batches to local player entities.
///
/// Each fresh armor, held, sneak, sprint, blocking, swing and skin update is
/// mirrored onto the player entity carrying the same cluster id.
pub async fn visual_apply_task(
    server: Arc<Server>,
    local: ServerId,
    mut visual_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut state = VisualApplyState::new();
    let mut first = true;
    while let Some(parcel) = visual_rx.recv().await {
        VISUAL_BATCHES.fetch_add(1, Ordering::Relaxed);
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster visual stream started");
        }
        apply_parcel(&server, &mut state, local, &parcel);
    }
    debug!("cluster visual stream closed");
}

/// Spawns the background task that applies incoming visual batches.
///
/// Fresh visual updates are mirrored onto the matching local player entity so
/// remote armor, held item, sneak, sprint, blocking, swing and skin layers
/// stay visible to local viewers.
pub fn spawn_visual_apply(
    server: &Arc<Server>,
    local: ServerId,
    visual_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(visual_apply_task(task_server, local, visual_rx));
}
