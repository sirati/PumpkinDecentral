use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use pumpkin_cluster::identity::{ActionActor, ActionSeq, ServerId};
use pumpkin_cluster::interact::{
    AtomicInteractKind, InteractEdit, InteractGroup, InteractUpdate, InventoryPrecondition,
    SemanticInventoryUse, capture_atomic_interact, capture_linked_door_toggle,
    capture_linked_trapdoor_toggle,
};
use pumpkin_cluster::inventory::{INV_MAIN, INV_OFFHAND, InvLoc, InvOpKind, InventoryStack, capture_semantic_inv_op};
use pumpkin_cluster::protocol::{BlockPos as ClusterBlockPos, ChunkAddr};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{
    BlockDelta, ExplosionDropRef, ExplosionEntityDamageRef, WorldActionGroup, WorldDelta,
    WorldDependency, capture_causal_redstone_update, capture_random_tick_delta,
    capture_transactional_explosion_update, chunk_of_block,
};
pub use pumpkin_cluster::world_driven::{
    decode_interact_frame, encode_interact_frame, is_interact_frame,
};
use pumpkin_cluster::world_driven::{WorldDrivenFrame, encode_action};
use pumpkin_data::item_stack::ItemStack;
use pumpkin_inventory::player::player_inventory::PlayerInventory;
use pumpkin_util::math::position::BlockPos;
use tracing::warn;

use crate::world::World;
use crate::entity::player::Player;

static WORLD_DELTA_INSTALLED: OnceLock<()> = OnceLock::new();
static WORLD_DELTA_EMITTED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_DROPPED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_UNDISCIPLINED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_UNCAUSAL: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_ACTION_SEQ: AtomicU16 = AtomicU16::new(0);

#[must_use]
pub fn mesh_active() -> bool {
    WORLD_DELTA_INSTALLED.get().is_some()
}

#[must_use]
pub fn primary_produces() -> bool {
    mesh_active() && !pumpkin_world::level::is_cluster_secondary()
}

pub fn install_world_delta_outbox(
    _local: ServerId,
    _outbound: tokio::sync::mpsc::Sender<pumpkin_cluster::streams::OutboundParcel>,
) {
    let _ = WORLD_DELTA_INSTALLED.set(());
}

#[must_use]
pub fn holder_of(world: &World) -> Option<ServerId> {
    world
        .server
        .upgrade()
        .map(|server| ServerId(server.advanced_config.cluster.server_id))
}

#[must_use]
pub fn world_delta_emitted() -> u64 {
    WORLD_DELTA_EMITTED.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_delta_dropped() -> u64 {
    WORLD_DELTA_DROPPED.load(Ordering::Relaxed)
}

fn disciplined_tick() -> Option<TickStamp> {
    let tick = super::cluster::disciplined_tick_now();
    if tick.is_none() {
        let skipped = WORLD_DELTA_UNDISCIPLINED.fetch_add(1, Ordering::Relaxed);
        if skipped % 1200 == 0 {
            warn!(skipped, "cluster world delta skipped without disciplined clock");
        }
    }
    tick
}

pub fn world_tick_stamp() -> Option<TickStamp> {
    disciplined_tick()
}

fn forward_action(actor: ActionActor, seq: ActionSeq, frame: WorldDrivenFrame) {
    let chunk = frame.chunk();
    let Ok(action) = encode_action(actor, seq, &frame) else {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        warn!(?actor, ?seq, "cluster world-driven action encode failed");
        return;
    };
    WORLD_DELTA_EMITTED.fetch_add(1, Ordering::Relaxed);
    super::cluster_world_apply::submit_local_world_frame(chunk, action.bytes);
}

fn interact_group(player: &Player, anchor: ClusterBlockPos) -> Option<InteractGroup> {
    if !mesh_active() {
        return None;
    }
    let gid = player.cluster_gid()?;
    let tick = disciplined_tick()?;
    Some(InteractGroup::new(
        gid,
        player.next_cluster_world_seq(),
        tick,
        anchor,
    ))
}

fn emit_interact(update: InteractUpdate) {
    let actor = ActionActor::Player(update.gid());
    let seq = ActionSeq(update.seq().0);
    forward_action(actor, seq, WorldDrivenFrame::Interact(update));
}

fn stack_snapshot(stack: &ItemStack) -> InventoryStack {
    if stack.is_empty() {
        return InventoryStack::empty();
    }
    let mut compound = pumpkin_nbt::compound::NbtCompound::new();
    stack.write_item_stack(&mut compound);
    InventoryStack {
        item: stack.item.id,
        count: stack.item_count,
        nbt: pumpkin_nbt::Nbt::from(compound).write_unnamed().to_vec(),
    }
    .normalized()
}

#[must_use]
pub fn inventory_location_for_slot(slot: usize) -> Option<InvLoc> {
    if slot == PlayerInventory::OFF_HAND_SLOT {
        Some(InvLoc::new(INV_OFFHAND, 0))
    } else if slot < PlayerInventory::OFF_HAND_SLOT {
        Some(InvLoc::new(INV_MAIN, slot as u16))
    } else {
        None
    }
}

fn item_use(
    group: InteractGroup,
    slot: usize,
    before: &ItemStack,
    consumes_item: bool,
) -> Option<SemanticInventoryUse> {
    if !consumes_item {
        return None;
    }
    let loc = inventory_location_for_slot(slot)?;
    let stack = stack_snapshot(before);
    let op = capture_semantic_inv_op(
        group.gid,
        group.seq,
        group.tick,
        InvOpKind::Consume,
        loc,
        loc,
        stack.clone(),
        stack.clone(),
        1,
    )?;
    Some(SemanticInventoryUse {
        precondition: InventoryPrecondition { loc, stack },
        op,
    })
}

pub fn emit_end_eye_insert(
    player: &Player,
    pos: &BlockPos,
    old_state: u16,
    new_state: u16,
    slot: usize,
    before: &ItemStack,
    consumes_item: bool,
) {
    let anchor = ClusterBlockPos {
        x: pos.0.x,
        y: pos.0.y,
        z: pos.0.z,
    };
    let Some(group) = interact_group(player, anchor) else {
        return;
    };
    let inventory = item_use(group, slot, before, consumes_item);
    if consumes_item && inventory.is_none() {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        warn!(?group.gid, "cluster end-eye action missing semantic inventory use");
        return;
    }
    let edit = InteractEdit::new(anchor, old_state, new_state);
    if let Some(update) = capture_atomic_interact(
        group,
        AtomicInteractKind::EndEye,
        chunk_of_block(anchor.x, anchor.z),
        vec![edit],
        inventory,
    ) {
        emit_interact(InteractUpdate::Atomic(update));
    }
}

pub fn emit_anchor_charge_action(
    player: &Player,
    pos: &BlockPos,
    old_state: u16,
    new_state: u16,
    slot: usize,
    before: &ItemStack,
    consumes_item: bool,
) {
    let anchor = ClusterBlockPos {
        x: pos.0.x,
        y: pos.0.y,
        z: pos.0.z,
    };
    let Some(group) = interact_group(player, anchor) else {
        return;
    };
    let inventory = item_use(group, slot, before, consumes_item);
    if consumes_item && inventory.is_none() {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        warn!(?group.gid, "cluster respawn-anchor action missing semantic inventory use");
        return;
    }
    let edit = InteractEdit::new(anchor, old_state, new_state);
    if let Some(update) = capture_atomic_interact(
        group,
        AtomicInteractKind::Anchor,
        chunk_of_block(anchor.x, anchor.z),
        vec![edit],
        inventory,
    ) {
        emit_interact(InteractUpdate::Atomic(update));
    }
}

pub fn emit_linked_door_toggle(
    player: &Player,
    lower: (&BlockPos, u16, u16),
    upper: (&BlockPos, u16, u16),
) {
    let anchor = ClusterBlockPos {
        x: lower.0.0.x,
        y: lower.0.0.y,
        z: lower.0.0.z,
    };
    let Some(group) = interact_group(player, anchor) else {
        return;
    };
    let chunk = chunk_of_block(anchor.x, anchor.z);
    let lower = InteractEdit::new(anchor, lower.1, lower.2);
    let upper = InteractEdit::new(
        ClusterBlockPos {
            x: upper.0.0.x,
            y: upper.0.0.y,
            z: upper.0.0.z,
        },
        upper.1,
        upper.2,
    );
    if let Some(update) = capture_linked_door_toggle(group, chunk, lower, upper) {
        emit_interact(InteractUpdate::Atomic(update));
    }
}

pub fn emit_linked_trapdoor_toggle(player: &Player, pos: &BlockPos, old_state: u16, new_state: u16) {
    let anchor = ClusterBlockPos {
        x: pos.0.x,
        y: pos.0.y,
        z: pos.0.z,
    };
    let Some(group) = interact_group(player, anchor) else {
        return;
    };
    let chunk = chunk_of_block(anchor.x, anchor.z);
    let edit = InteractEdit::new(anchor, old_state, new_state);
    if let Some(update) = capture_linked_trapdoor_toggle(group, chunk, vec![edit]) {
        emit_interact(InteractUpdate::Atomic(update));
    }
}

pub fn emit_random_tick_delta(
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    edits: Vec<BlockDelta>,
) {
    if pumpkin_world::level::is_cluster_secondary() {
        WORLD_DELTA_DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let Some(update) = capture_random_tick_delta(holder, chunk, tick, edits) else {
        return;
    };
    let seq = ActionSeq(WORLD_DELTA_ACTION_SEQ.fetch_add(1, Ordering::Relaxed));
    forward_action(
        ActionActor::Server(holder),
        seq,
        WorldDrivenFrame::Delta(WorldDelta::RandomTick(update)),
    );
}

pub fn emit_causal_redstone_update(
    group: WorldActionGroup,
    chunk: ChunkAddr,
    edits: Vec<BlockDelta>,
    dependencies: Vec<WorldDependency>,
) {
    let Some(update) = capture_causal_redstone_update(group, chunk, edits, dependencies) else {
        return;
    };
    forward_action(
        ActionActor::Server(group.reference.owner),
        ActionSeq(group.reference.ordinal),
        WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(update)),
    );
}

pub fn emit_transactional_explosion_update(
    group: WorldActionGroup,
    chunk: ChunkAddr,
    center: ClusterBlockPos,
    edits: Vec<BlockDelta>,
    drops: Vec<ExplosionDropRef>,
    damages: Vec<ExplosionEntityDamageRef>,
) {
    let Some(update) =
        capture_transactional_explosion_update(group, chunk, center, edits, drops, damages)
    else {
        return;
    };
    forward_action(
        ActionActor::Server(group.reference.owner),
        ActionSeq(group.reference.ordinal),
        WorldDrivenFrame::Delta(WorldDelta::TransactionalExplosion(update)),
    );
}

pub fn emit_redstone_write(
    world: &World,
    trigger: &BlockPos,
    edit_pos: &BlockPos,
    old_state: u16,
    new_state: u16,
) {
    if old_state == new_state || !mesh_active() || crate::world::random_tick_edits_active() {
        return;
    }
    let skipped = WORLD_DELTA_UNCAUSAL.fetch_add(1, Ordering::Relaxed);
    if skipped % 1200 == 0 {
        warn!(
            holder = ?holder_of(world),
            trigger_x = trigger.0.x,
            trigger_y = trigger.0.y,
            trigger_z = trigger.0.z,
            edit_x = edit_pos.0.x,
            edit_y = edit_pos.0.y,
            edit_z = edit_pos.0.z,
            "cluster redstone delta skipped without causal group"
        );
    }
}

pub fn emit_explosion_blocks(
    world: &World,
    center: &BlockPos,
    destroyed: &[(BlockPos, u16)],
) {
    if destroyed.is_empty() || !mesh_active() || crate::world::random_tick_edits_active() {
        return;
    }
    let skipped = WORLD_DELTA_UNCAUSAL.fetch_add(1, Ordering::Relaxed);
    if skipped % 1200 == 0 {
        warn!(
            holder = ?holder_of(world),
            center_x = center.0.x,
            center_y = center.0.y,
            center_z = center.0.z,
            destroyed = destroyed.len(),
            "cluster explosion delta skipped without owner transaction"
        );
    }
}
