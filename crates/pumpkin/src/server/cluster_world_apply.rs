use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use pumpkin_cluster::accept::{AcceptBatch, Acceptor, ConflictKey, LocalChunkOutcome, build_accept, decode_accept, encode_accept};
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::{ActionActor, ActionSeq, GlobalPlayerId, ServerId};
use pumpkin_cluster::order::{order_action_actors, order_players};
use pumpkin_cluster::protocol::{
    BlockPos as ClusterBlockPos, BreakBlockUpdate, ChunkAddr, PlaceBlockUpdate, StreamKind,
    TickBatch,
};
use pumpkin_cluster::reconcile::ReconcilePlan;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{
    BlockDelta, FallibleClaim, FallibleDecision, RandomTickDecision, RandomTickDelta, WorldDelta,
    WorldActionGroup, WorldActionRef, WorldDeltaAcceptance, apply_fallible_edit,
    apply_random_tick_edit, capture_causal_redstone_update, chunk_of_block,
    elect_fallible_claim, make_delta_undo,
};
use pumpkin_cluster::world_driven::{
    WorldDrivenAction, WorldDrivenFrame, apply_accepted as apply_world_driven,
    decode_action as decode_world_driven, judge_action as judge_world_driven,
    encode_action as encode_world_driven, order_actions as order_world_driven,
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;

pub const CLUSTER_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

static WORLD_BATCHES: AtomicU64 = AtomicU64::new(0);
static WORLD_DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);
static BREAK_APPLIED: AtomicU64 = AtomicU64::new(0);
static BREAK_REJECTED: AtomicU64 = AtomicU64::new(0);
static PLACE_APPLIED: AtomicU64 = AtomicU64::new(0);
static PLACE_REJECTED: AtomicU64 = AtomicU64::new(0);

static WORLD_BREAK_METRICS: pumpkin_cluster::break_emit::BreakMetrics =
    pumpkin_cluster::break_emit::BreakMetrics::new();
static WORLD_DELTA_APPLIED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_REVERTED: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_CONFLICTS: AtomicU64 = AtomicU64::new(0);
static WORLD_DELTA_DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);
static LOCAL_OPTIMISTIC_BATCHES: OnceLock<mpsc::Sender<TickBatch>> = OnceLock::new();
static SNAPSHOT_PENDING_BATCHES: OnceLock<mpsc::Sender<TickBatch>> = OnceLock::new();
static SNAPSHOT_PENDING_FRAMES: OnceLock<mpsc::Sender<Vec<u8>>> = OnceLock::new();
static LOCAL_WORLD_DRIVEN: OnceLock<mpsc::Sender<Vec<u8>>> = OnceLock::new();
static WORLD_ACCEPT_INPUT: OnceLock<mpsc::Sender<(ServerId, Vec<u8>)>> = OnceLock::new();
static INVENTORY_SEEDS: OnceLock<
    mpsc::Sender<(GlobalPlayerId, Vec<pumpkin_cluster::invsee::InvseeSlot>)>,
> = OnceLock::new();
static ACCEPTED_TICK_COPY: OnceLock<mpsc::Sender<TickBatch>> = OnceLock::new();
static WORLD_ACTION_OUTBOX: OnceLock<WorldActionOutbox> = OnceLock::new();
static PENDING_ACTION_FRAMES: std::sync::LazyLock<ArcSwap<BTreeMap<ChunkAddr, Vec<Vec<u8>>>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));
#[derive(Clone)]
struct ReplicatedInventory {
    selected: u8,
    slots: Vec<pumpkin_cluster::invsee::InvseeSlot>,
}

static INVENTORY_REPLICAS: std::sync::LazyLock<ArcSwap<BTreeMap<GlobalPlayerId, ReplicatedInventory>>> =
    std::sync::LazyLock::new(|| ArcSwap::from_pointee(BTreeMap::new()));

pub struct WorldActionOutbox {
    outbound: mpsc::Sender<OutboundParcel>,
}

pub fn install_world_action_outbox(outbound: mpsc::Sender<OutboundParcel>) {
    let _ = WORLD_ACTION_OUTBOX.set(WorldActionOutbox { outbound });
}

#[must_use]
pub fn pending_action_frames(chunk: ChunkAddr) -> Vec<Vec<u8>> {
    PENDING_ACTION_FRAMES
        .load()
        .get(&chunk)
        .cloned()
        .unwrap_or_default()
}

pub fn submit_accept_batch(holder: ServerId, bytes: Vec<u8>) {
    let Some(sender) = WORLD_ACCEPT_INPUT.get() else {
        tracing::warn!("cluster world acceptance actor is not installed");
        return;
    };
    if sender.try_send((holder, bytes)).is_err() {
        tracing::warn!("cluster world acceptance input queue full");
    }
}

pub fn ingest_chunk_snapshot_pendings(chunk: ChunkAddr, frames: Vec<Vec<u8>>) {
    let Some(sender) = SNAPSHOT_PENDING_BATCHES.get() else {
        warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster snapshot action actor is not installed");
        return;
    };
    for bytes in frames {
        if let Ok((action, _)) = decode_world_driven(&bytes) {
            if action.chunk != chunk {
                warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster snapshot pending frame did not target its chunk");
            } else if let Some(frames) = SNAPSHOT_PENDING_FRAMES.get() {
                if frames.try_send(bytes).is_err() {
                    warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster snapshot frame input queue full");
                }
            }
            continue;
        }
        match decode_batch(&bytes) {
            Ok(batch) if batch_chunks(&batch).contains(&chunk) => {
                if sender.try_send(batch).is_err() {
                    warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster snapshot action input queue full");
                }
            }
            Ok(_) => warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster snapshot pending action did not target its chunk"),
            Err(error) => warn!(chunk_x = chunk.x, chunk_z = chunk.z, %error, "cluster snapshot pending action decode failed"),
        }
    }
}

pub fn submit_local_optimistic_batch(batch: TickBatch) {
    let Some(sender) = LOCAL_OPTIMISTIC_BATCHES.get() else {
        tracing::warn!(tick = batch.tick.0, "cluster world action actor is not installed");
        return;
    };
    if sender.try_send(batch).is_err() {
        tracing::warn!("cluster world action actor input queue full");
    }
}

pub fn submit_local_world_frame(chunk: ChunkAddr, bytes: Vec<u8>) {
    let Ok((action, _)) = decode_world_driven(&bytes) else {
        warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster local world frame decode failed");
        return;
    };
    if action.chunk != chunk {
        warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster local world frame chunk mismatch");
        return;
    }
    let Some(sender) = LOCAL_WORLD_DRIVEN.get() else {
        warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster world action actor is not installed");
        return;
    };
    if sender.try_send(bytes).is_err() {
        warn!(chunk_x = chunk.x, chunk_z = chunk.z, "cluster world action actor input queue full");
    }
}

pub fn submit_local_inventory_op(operation: pumpkin_cluster::inventory::InventoryOp) -> bool {
    submit_local_inventory_ops(vec![operation])
}

pub fn submit_local_inventory_ops(
    operations: Vec<pumpkin_cluster::inventory::InventoryOp>,
) -> bool {
    let Some(first) = operations.first() else {
        return true;
    };
    if operations.iter().any(|operation| operation.tick != first.tick) {
        warn!("cluster inventory operation group crosses tick boundary");
        return false;
    }
    let mut batch = TickBatch::new(first.tick);
    batch.inv_ops = operations;
    let Some(sender) = LOCAL_OPTIMISTIC_BATCHES.get() else {
        warn!("cluster world action actor is not installed");
        return false;
    };
    sender.try_send(batch).is_ok()
}

#[must_use]
pub fn replicated_inventory_snapshot(
    gid: GlobalPlayerId,
    owner_name: String,
) -> Option<pumpkin_cluster::invsee::InventorySnapshot> {
    let replica = INVENTORY_REPLICAS.load().get(&gid)?.clone();
    Some(pumpkin_cluster::invsee::InventorySnapshot::new(
        gid,
        owner_name,
        replica.selected,
        replica.slots,
    ))
}

pub fn seed_replicated_inventory(
    gid: GlobalPlayerId,
    selected: u8,
    mut slots: Vec<pumpkin_cluster::invsee::InvseeSlot>,
) {
    slots.sort_by_key(|slot| slot.index);
    slots.dedup_by_key(|slot| slot.index);
    slots.truncate(pumpkin_cluster::invsee::INVSEE_MAX_SLOTS);
    let mut replicas = (*INVENTORY_REPLICAS.load_full()).clone();
    replicas.insert(gid, ReplicatedInventory { selected, slots });
    INVENTORY_REPLICAS.store(Arc::new(replicas));
    let Some(sender) = INVENTORY_SEEDS.get() else {
        warn!(?gid, "cluster inventory actor is not installed for handoff seed");
        return;
    };
    let replicas = INVENTORY_REPLICAS.load();
    let Some(replica) = replicas.get(&gid) else {
        return;
    };
    if sender.try_send((gid, replica.slots.clone())).is_err() {
        warn!(?gid, "cluster inventory handoff seed queue full");
    }
}

pub fn install_accepted_tick_copy(sender: mpsc::Sender<TickBatch>) {
    let _ = ACCEPTED_TICK_COPY.set(sender);
}

#[must_use]
pub fn world_delta_applied() -> u64 {
    WORLD_DELTA_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_delta_reverted() -> u64 {
    WORLD_DELTA_REVERTED.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_delta_conflicts() -> u64 {
    WORLD_DELTA_CONFLICTS.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_delta_decode_errors() -> u64 {
    WORLD_DELTA_DECODE_ERRORS.load(Ordering::Relaxed)
}

#[derive(Debug, Default)]
pub struct WorldDeltaState {
    pub acceptance: WorldDeltaAcceptance,
    pub ledger: HashMap<(TickStamp, ClusterBlockPos), FallibleClaim>,
    pub promote_queue: BTreeSet<TickStamp>,
}

impl WorldDeltaState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn pending_claims(&self) -> usize {
        self.ledger.len()
    }

    #[must_use]
    pub fn queued_promotions(&self) -> usize {
        self.promote_queue.len()
    }
}

pub(crate) fn read_delta_state(world: &crate::world::World, pos: ClusterBlockPos) -> Option<u16> {
    let world_pos = cluster_pos(pos);
    if world.level.cluster_dual_enabled() {
        Some(world.level.cluster_get_block_state(&world_pos).as_u16())
    } else {
        world
            .get_block_state_id_if_loaded(&world_pos)
            .map(|current| current.as_u16())
    }
}

pub(crate) fn write_delta_state(
    world: &Arc<crate::world::World>,
    pos: ClusterBlockPos,
    state: u16,
) -> bool {
    let Some(new_id) = pumpkin_data::BlockStateId::new(state) else {
        return false;
    };
    let world_pos = cluster_pos(pos);
    world.level.set_block_state(&world_pos, new_id);
    let confirmed = world
        .get_block_state_id_if_loaded(&world_pos)
        .is_some_and(|current| current.as_u16() == state);
    if confirmed {
        mirror_to_dual(world, &world_pos, new_id);
    }
    confirmed
}

fn revert_delta_claim(world: &Arc<crate::world::World>, claim: &FallibleClaim) {
    if write_delta_state(world, claim.pos, claim.undo.old_state) {
        WORLD_DELTA_REVERTED.fetch_add(1, Ordering::Relaxed);
    }
}

#[must_use]
pub fn revert_delta_plan(world: &Arc<crate::world::World>, plan: &ReconcilePlan) -> u64 {
    let mut reverted = 0_u64;
    plan.apply_blocks(|pos, old_state| {
        if write_delta_state(world, pos, old_state) {
            reverted = reverted.saturating_add(1);
        }
    });
    if reverted > 0 {
        WORLD_DELTA_REVERTED.fetch_add(reverted, Ordering::Relaxed);
    }
    reverted
}

pub fn apply_random_tick_delta(
    world: &Arc<crate::world::World>,
    update: &RandomTickDelta,
) {
    for edit in &update.edits {
        let Some(current) = read_delta_state(world, edit.pos) else {
            continue;
        };
        if let RandomTickDecision::Apply { .. } = apply_random_tick_edit(current, edit) {
            if write_delta_state(world, edit.pos, edit.new_state) {
                WORLD_DELTA_APPLIED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub fn apply_fallible_delta(
    world: &Arc<crate::world::World>,
    state: &mut WorldDeltaState,
    peer: ServerId,
    actor: ActionActor,
    seq: ActionSeq,
    chunk: ChunkAddr,
    tick: TickStamp,
    trigger: ClusterBlockPos,
    edits: &[BlockDelta],
) {
    state.acceptance.require(tick, chunk, &[action_origin(actor).0]);
    for edit in edits {
        let Some(current_raw) = read_delta_state(world, edit.pos) else {
            continue;
        };
        if current_raw == edit.new_state {
            state.acceptance.accept(tick, chunk, peer.0, &[]);
            continue;
        }
        let key = (tick, edit.pos);
        let incoming = FallibleClaim {
            actor,
            seq,
            tick,
            trigger,
            pos: edit.pos,
            undo: make_delta_undo(current_raw),
            new_state: edit.new_state,
        };
        match state.ledger.get(&key).copied() {
            Some(stored) if stored.new_state == edit.new_state => {
                state.acceptance.accept(tick, chunk, peer.0, &[]);
            }
            Some(stored) => {
                if elect_fallible_claim(CLUSTER_SEED, &stored, &incoming) == incoming {
                    revert_delta_claim(world, &stored);
                    if write_delta_state(world, edit.pos, edit.new_state) {
                        state.ledger.insert(key, incoming);
                        WORLD_DELTA_APPLIED.fetch_add(1, Ordering::Relaxed);
                    }
                    state.acceptance.accept(tick, chunk, peer.0, &[]);
                } else {
                    WORLD_DELTA_CONFLICTS.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => match apply_fallible_edit(current_raw, edit) {
                FallibleDecision::Apply { undo } => {
                    if write_delta_state(world, edit.pos, edit.new_state) {
                        state.ledger.insert(
                            key,
                            FallibleClaim {
                                actor,
                                seq,
                                tick,
                                trigger,
                                pos: edit.pos,
                                undo,
                                new_state: edit.new_state,
                            },
                        );
                        state.acceptance.accept(tick, chunk, peer.0, &[]);
                        WORLD_DELTA_APPLIED.fetch_add(1, Ordering::Relaxed);
                    }
                }
                FallibleDecision::Conflict { .. } => {
                    WORLD_DELTA_CONFLICTS.fetch_add(1, Ordering::Relaxed);
                }
            },
        }
    }
    state.promote_queue.insert(tick);
    drain_complete_fallible_ticks(world, state);
}

#[must_use]
pub fn group_winners_by_chunk(
    winners: &[(ClusterBlockPos, u16)],
) -> HashMap<ChunkAddr, Vec<(ClusterBlockPos, u16)>> {
    let mut by_chunk: HashMap<ChunkAddr, Vec<(ClusterBlockPos, u16)>> = HashMap::new();
    for (pos, new_state) in winners {
        by_chunk
            .entry(chunk_of_block(pos.x, pos.z))
            .or_default()
            .push((*pos, *new_state));
    }
    by_chunk
}

fn promote_chunk_winners(
    world: &crate::world::World,
    winners: &[(ClusterBlockPos, u16)],
) {
    use pumpkin_world::level::ClusterBlockEdit;
    use pumpkin_util::math::vector2::Vector2;
    let grouped = group_winners_by_chunk(winners);
    let mut by_chunk: HashMap<ChunkAddr, Vec<ClusterBlockEdit>> = HashMap::new();
    for (chunk, entries) in &grouped {
        let mut accepted: Vec<ClusterBlockEdit> = Vec::with_capacity(entries.len());
        for (pos, new_state) in entries {
            let Some(state_id) = pumpkin_data::BlockStateId::new(*new_state) else {
                continue;
            };
            let world_pos = cluster_pos(*pos);
            let (_, relative) = world_pos.chunk_and_chunk_relative_position();
            accepted.push(ClusterBlockEdit {
                x: relative.x as usize,
                y: relative.y,
                z: relative.z as usize,
                state: state_id,
            });
        }
        by_chunk.insert(*chunk, accepted);
    }
    for (chunk, accepted) in &by_chunk {
        let coordinate = Vector2::new(chunk.x, chunk.z);
        world.level.cluster_promote_tick(&coordinate, accepted, &[]);
    }
}

pub fn drain_complete_fallible_ticks(
    world: &crate::world::World,
    state: &mut WorldDeltaState,
) {
    while let Some(tick) = state.promote_queue.iter().next().copied() {
        if !state.acceptance.is_complete(tick) {
            break;
        }
        state.promote_queue.remove(&tick);
        world.level.cluster_note_ground_tick(tick.0);
        let winners: Vec<(ClusterBlockPos, u16)> = state
            .ledger
            .iter()
            .filter(|((entry_tick, _), _)| *entry_tick == tick)
            .map(|(_, claim)| (claim.pos, claim.new_state))
            .collect();
        if world.level.cluster_dual_enabled() {
            promote_chunk_winners(world, &winners);
        }
        world.level.cluster_note_applied_tick(tick.0);
        state.acceptance.remove_tick(tick);
        state.ledger.retain(|key, _| key.0 != tick);
    }
}

pub fn apply_world_delta_frame(
    server: &Server,
    state: &mut WorldDeltaState,
    peer: ServerId,
    frame: &WorldDelta,
) {
    match frame {
        WorldDelta::RandomTick(update) => {
            if let Some(world) = default_world(server) {
                apply_random_tick_delta(&world, update);
            }
        }
        WorldDelta::Redstone(update) => {
            let _ = (update, state, peer);
        }
        WorldDelta::Explosion(update) => {
            let _ = (update, state, peer);
        }
        WorldDelta::CausalRedstone(_) | WorldDelta::TransactionalExplosion(_) => {}
    }
}

#[must_use]
pub fn world_batches() -> u64 {
    WORLD_BATCHES.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_decode_errors() -> u64 {
    WORLD_DECODE_ERRORS.load(Ordering::Relaxed)
}

#[must_use]
pub fn break_applied() -> u64 {
    BREAK_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn break_rejected() -> u64 {
    BREAK_REJECTED.load(Ordering::Relaxed)
}

#[must_use]
pub fn place_applied() -> u64 {
    PLACE_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn place_rejected() -> u64 {
    PLACE_REJECTED.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryStackDelta {
    pub slot: u8,
    pub count_before: u8,
    pub count_after: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakApplyOutcome {
    pub applied: bool,
    pub undo_old_state: u16,
    pub inventory: Option<InventoryStackDelta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceApplyOutcome {
    pub applied: bool,
    pub previous_state: u16,
    pub inventory: Option<InventoryStackDelta>,
}

#[must_use]
pub fn dirt_state_id() -> u16 {
    pumpkin_data::Block::DIRT.default_state.id.as_u16()
}

#[must_use]
pub fn wood_state_ids() -> [u16; 2] {
    [
        pumpkin_data::Block::OAK_PLANKS.default_state.id.as_u16(),
        pumpkin_data::Block::OAK_LOG.default_state.id.as_u16(),
    ]
}

#[must_use]
pub fn is_dirt_state(state: u16) -> bool {
    state == dirt_state_id()
}

#[must_use]
pub fn is_wood_state(state: u16) -> bool {
    wood_state_ids().contains(&state)
}

#[must_use]
pub fn dirt_vs_wood_conflict(expected_old_state: u16, current_state: u16) -> bool {
    if expected_old_state == current_state {
        return false;
    }
    if is_dirt_state(expected_old_state) && is_wood_state(current_state) {
        return true;
    }
    if is_wood_state(expected_old_state) && is_dirt_state(current_state) {
        return true;
    }
    true
}

#[must_use]
pub fn break_conflict(expected_old_state: u16, current_state: u16) -> bool {
    dirt_vs_wood_conflict(expected_old_state, current_state)
}

#[must_use]
pub fn place_conflict(new_state: u16, current_state: u16) -> bool {
    new_state == current_state
}

fn default_world(server: &Server) -> Option<Arc<crate::world::World>> {
    server.worlds.load().first().cloned()
}

fn cluster_pos(update_pos: pumpkin_cluster::protocol::BlockPos) -> pumpkin_util::math::position::BlockPos {
    pumpkin_util::math::position::BlockPos::new(update_pos.x, update_pos.y, update_pos.z)
}

fn mirror_to_dual(
    world: &crate::world::World,
    pos: &pumpkin_util::math::position::BlockPos,
    state: pumpkin_data::BlockStateId,
) {
    if !world.level.cluster_dual_enabled() {
        return;
    }
    world.level.cluster_set_block_state(pos, state);
}

#[must_use]
pub fn apply_break_atomic(
    server: &Arc<Server>,
    update: &BreakBlockUpdate,
) -> BreakApplyOutcome {
    let Some(world) = default_world(server) else {
        BREAK_REJECTED.fetch_add(1, Ordering::Relaxed);
        return BreakApplyOutcome {
            applied: false,
            undo_old_state: update.expected_old_state,
            inventory: None,
        };
    };
    apply_break_to_world(&world, update)
}

#[must_use]
pub fn apply_break_to_world(
    world: &Arc<crate::world::World>,
    update: &BreakBlockUpdate,
) -> BreakApplyOutcome {
    use pumpkin_world::world::BlockFlags;
    let pos = cluster_pos(update.pos);
    let Some(current) = world.get_block_state_id_if_loaded(&pos) else {
        BREAK_REJECTED.fetch_add(1, Ordering::Relaxed);
        return BreakApplyOutcome {
            applied: false,
            undo_old_state: update.expected_old_state,
            inventory: None,
        };
    };
    let current_raw = current.as_u16();
    let undo = match pumpkin_cluster::break_emit::apply_remote_break(
        current_raw,
        update,
        &WORLD_BREAK_METRICS,
    ) {
        pumpkin_cluster::break_emit::RemoteBreakDecision::Accept { undo } => undo,
        pumpkin_cluster::break_emit::RemoteBreakDecision::RejectStale { current, .. } => {
            BREAK_REJECTED.fetch_add(1, Ordering::Relaxed);
            return BreakApplyOutcome {
                applied: false,
                undo_old_state: current,
                inventory: None,
            };
        }
    };
    let applied = world
        .break_block(&pos, None, BlockFlags::SKIP_DROPS | BlockFlags::NOTIFY_ALL)
        .is_some();
    if applied {
        if let Some(truth) = world.get_block_state_id_if_loaded(&pos) {
            mirror_to_dual(world, &pos, truth);
        }
        BREAK_APPLIED.fetch_add(1, Ordering::Relaxed);
        BreakApplyOutcome {
            applied: true,
            undo_old_state: undo.old_state,
            inventory: None,
        }
    } else {
        BREAK_REJECTED.fetch_add(1, Ordering::Relaxed);
        BreakApplyOutcome {
            applied: false,
            undo_old_state: undo.old_state,
            inventory: None,
        }
    }
}

#[must_use]
pub fn apply_place_atomic(
    server: &Arc<Server>,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    update: &PlaceBlockUpdate,
) -> PlaceApplyOutcome {
    let Some(world) = default_world(server) else {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        return PlaceApplyOutcome {
            applied: false,
            previous_state: update.new_state,
            inventory: None,
        };
    };
    apply_place_to_world(&world, ledger, update)
}

#[must_use]
pub fn apply_place_to_world(
    world: &Arc<crate::world::World>,
    _ledger: &mut pumpkin_cluster::inventory::InvLedger,
    update: &PlaceBlockUpdate,
) -> PlaceApplyOutcome {
    let pos = cluster_pos(update.pos);
    let Some(current) = world.get_block_state_id_if_loaded(&pos) else {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        return PlaceApplyOutcome {
            applied: false,
            previous_state: update.new_state,
            inventory: None,
        };
    };
    let current_raw = current.as_u16();
    if current_raw != update.expected_old_state {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        return PlaceApplyOutcome {
            applied: false,
            previous_state: current_raw,
            inventory: None,
        };
    }
    let Some(new_id) = pumpkin_data::BlockStateId::new(update.new_state) else {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        return PlaceApplyOutcome {
            applied: false,
            previous_state: current_raw,
            inventory: None,
        };
    };
    let delta = InventoryStackDelta {
        slot: update.slot,
        count_before: update.count_before,
        count_after: update.count_after,
    };
    world.level.set_block_state(&pos, new_id);
    let confirmed = world
        .get_block_state_id_if_loaded(&pos)
        .is_some_and(|state| state.as_u16() == update.new_state);
    if confirmed {
        mirror_to_dual(world, &pos, new_id);
        PLACE_APPLIED.fetch_add(1, Ordering::Relaxed);
        PlaceApplyOutcome {
            applied: true,
            previous_state: current_raw,
            inventory: Some(delta),
        }
    } else {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        PlaceApplyOutcome {
            applied: false,
            previous_state: current_raw,
            inventory: Some(delta),
        }
    }
}

pub fn sort_breaks_for_tick(tick: TickStamp, updates: &mut [BreakBlockUpdate]) {
    updates.sort_by(|left, right| {
        order_players(CLUSTER_SEED, tick, left.gid, right.gid)
            .then_with(|| {
                (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
            })
            .then_with(|| left.seq.0.cmp(&right.seq.0))
    });
}

pub fn sort_places_for_tick(tick: TickStamp, updates: &mut [PlaceBlockUpdate]) {
    updates.sort_by(|left, right| {
        order_players(CLUSTER_SEED, tick, left.gid, right.gid)
            .then_with(|| {
                (left.pos.x, left.pos.y, left.pos.z).cmp(&(right.pos.x, right.pos.y, right.pos.z))
            })
            .then_with(|| left.seq.0.cmp(&right.seq.0))
    });
}

pub fn sort_batch_for_apply(batch: &mut TickBatch) {
    sort_breaks_for_tick(batch.tick, &mut batch.break_block);
    sort_places_for_tick(batch.tick, &mut batch.place_block);
    pumpkin_cluster::inventory::sort_inv_ops_for_tick(batch.tick, &mut batch.inv_ops);
    pumpkin_cluster::combat::sort_captured_attacks(CLUSTER_SEED, batch.tick, &mut batch.attacks);
    pumpkin_cluster::combat::sort_fire(CLUSTER_SEED, batch.tick, &mut batch.fire);
    pumpkin_cluster::entity_action::sort_entity_mutations(
        CLUSTER_SEED,
        batch.tick,
        &mut batch.entity_mutations,
    );
}

#[derive(Debug, Default)]
pub struct WorldTickBuckets {
    pending: BTreeMap<TickStamp, Vec<(TickBatch, bool)>>,
    driven: BTreeMap<TickStamp, Vec<(WorldDrivenAction, WorldDrivenFrame, bool)>>,
    acceptor: Acceptor,
    holders: BTreeMap<(TickStamp, ChunkAddr), Vec<u16>>,
    unroutable: HashSet<TickStamp>,
}

impl WorldTickBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_batch(
        &mut self,
        _server: &Server,
        local: ServerId,
        batch: TickBatch,
        already_optimistic: bool,
    ) -> Option<AcceptBatch> {
        let tick = batch.tick;
        let chunks = batch_chunks(&batch);
        let mut outcomes = Vec::new();
        for chunk in chunks {
            let mut holders = super::cluster::chunk_holders(chunk);
            holders.sort_unstable();
            holders.dedup();
            if holders.is_empty() {
                self.unroutable.insert(tick);
                warn!(
                    tick = tick.0,
                    chunk_x = chunk.x,
                    chunk_z = chunk.z,
                    "cluster world action has no actual chunk holders"
                );
                continue;
            }
            self.acceptor.require(tick, chunk, &holders);
            self.holders.insert((tick, chunk), holders);
            outcomes.push(LocalChunkOutcome {
                chunk,
                accepted_actors: batch_actors_for_chunk(&batch, chunk),
                winners: winners_for_chunk(tick, &batch, chunk),
                new_holders: Vec::new(),
            });
        }
        self.pending.entry(tick).or_default().push((batch, already_optimistic));
        self.publish_pending();
        if outcomes.is_empty() {
            return None;
        }
        let accept = build_accept(tick, outcomes);
        self.acceptor.apply_accept_from(local.0, accept.clone());
        Some(accept)
    }

    pub fn push_world_driven(
        &mut self,
        _server: &Server,
        local: ServerId,
        action: WorldDrivenAction,
        frame: WorldDrivenFrame,
        already_optimistic: bool,
    ) -> Option<AcceptBatch> {
        let tick = action.tick;
        let chunk = action.chunk;
        let mut holders = super::cluster::chunk_holders(chunk);
        holders.sort_unstable();
        holders.dedup();
        if holders.is_empty() {
            self.unroutable.insert(tick);
            warn!(tick = tick.0, chunk_x = chunk.x, chunk_z = chunk.z, "cluster world-driven action has no actual chunk holders");
            return None;
        }
        self.acceptor.require(tick, chunk, &holders);
        self.holders.insert((tick, chunk), holders);
        self.pending.entry(tick).or_default();
        self.driven
            .entry(tick)
            .or_default()
            .push((action.clone(), frame.clone(), already_optimistic));
        self.publish_pending();
        let accept = build_accept(
            tick,
            vec![LocalChunkOutcome {
                chunk,
                accepted_actors: vec![action.actor],
                winners: world_driven_winners(&action, &frame),
                new_holders: Vec::new(),
            }],
        );
        self.acceptor.apply_accept_from(local.0, accept.clone());
        Some(accept)
    }

    pub fn apply_accept(&mut self, holder: ServerId, batch: AcceptBatch) {
        for decision in &batch.decisions {
            let holders = self
                .holders
                .entry((batch.tick, decision.chunk))
                .or_default();
            holders.extend(decision.new_holders.iter().copied());
            holders.sort_unstable();
            holders.dedup();
        }
        self.acceptor.apply_accept_from(holder.0, batch);
    }

    pub fn targets(&self, batch: &AcceptBatch, local: ServerId) -> Vec<ServerId> {
        let mut targets = Vec::new();
        for decision in &batch.decisions {
            if let Some(holders) = self.holders.get(&(batch.tick, decision.chunk)) {
                targets.extend(
                    holders
                        .iter()
                        .copied()
                        .filter(|holder| *holder != local.0)
                        .map(ServerId),
                );
            }
        }
        targets.sort();
        targets.dedup();
        targets
    }

    pub fn action_targets(&self, batch: &TickBatch, local: ServerId) -> Vec<ServerId> {
        let mut targets = Vec::new();
        for chunk in batch_chunks(batch) {
            if let Some(holders) = self.holders.get(&(batch.tick, chunk)) {
                targets.extend(
                    holders
                        .iter()
                        .copied()
                        .filter(|holder| *holder != local.0)
                        .map(ServerId),
                );
            }
        }
        targets.sort();
        targets.dedup();
        targets
    }

    pub fn frame_targets(&self, tick: TickStamp, chunk: ChunkAddr, local: ServerId) -> Vec<ServerId> {
        self.holders
            .get(&(tick, chunk))
            .into_iter()
            .flatten()
            .copied()
            .filter(|holder| *holder != local.0)
            .map(ServerId)
            .collect()
    }

    pub fn pop_ready(&mut self) -> Option<(TickStamp, Vec<(TickBatch, bool)>)> {
        let smallest = self.pending.keys().next().copied()?;
        if !self.unroutable.contains(&smallest) && self.acceptor.is_globally_accepted(smallest) {
            let batches = self.pending.remove(&smallest)?;
            self.acceptor.remove_tick(smallest);
            self.holders.retain(|(tick, _), _| *tick != smallest);
            self.publish_pending();
            Some((smallest, batches))
        } else {
            None
        }
    }

    fn take_world_driven(
        &mut self,
        tick: TickStamp,
    ) -> Vec<(WorldDrivenAction, WorldDrivenFrame, bool)> {
        self.driven.remove(&tick).unwrap_or_default()
    }

    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.pending.len()
    }

    fn publish_pending(&self) {
        let mut frames: BTreeMap<ChunkAddr, Vec<Vec<u8>>> = BTreeMap::new();
        for batches in self.pending.values() {
            for (batch, _) in batches {
                let Ok(bytes) = pumpkin_cluster::codec::encode_batch(batch) else {
                    continue;
                };
                for chunk in batch_chunks(batch) {
                    frames.entry(chunk).or_default().push(bytes.clone());
                }
            }
        }
        for actions in self.driven.values() {
            for (action, _, _) in actions {
                frames
                    .entry(action.chunk)
                    .or_default()
                    .push(action.bytes.clone());
            }
        }
        PENDING_ACTION_FRAMES.store(Arc::new(frames));
    }
}

fn batch_chunks(batch: &TickBatch) -> Vec<ChunkAddr> {
    let mut chunks = Vec::new();
    chunks.extend(batch.break_block.iter().map(|update| update.chunk));
    chunks.extend(batch.place_block.iter().map(|update| update.chunk));
    chunks.extend(
        batch
            .attacks
            .iter()
            .flat_map(|attack| attack.targets().map(|target| target.target.chunk())),
    );
    chunks.extend(batch.attacks.iter().map(|attack| attack.attacker.chunk));
    chunks.extend(batch.fire.iter().map(|update| update.chunk));
    chunks.extend(batch.entity_mutations.iter().map(|update| update.chunk()));
    chunks.extend(
        batch
            .inv_ops
            .iter()
            .filter_map(|operation| operation.pickup.as_ref().map(|pickup| pickup.entity.chunk)),
    );
    chunks.sort();
    chunks.dedup();
    chunks
}

fn batch_actor(batch: &TickBatch) -> Option<ActionActor> {
    batch
        .break_block
        .first()
        .map(|update| ActionActor::Player(update.gid))
        .or_else(|| batch.place_block.first().map(|update| ActionActor::Player(update.gid)))
        .or_else(|| batch.inv_ops.first().map(|operation| ActionActor::Player(operation.gid)))
        .or_else(|| batch.attacks.first().map(|attack| attack.actor))
        .or_else(|| batch.fire.first().map(|update| ActionActor::Player(update.gid)))
        .or_else(|| batch.entity_mutations.first().map(|update| update.actor))
}

fn batch_actors_for_chunk(batch: &TickBatch, chunk: ChunkAddr) -> Vec<ActionActor> {
    let mut actors = Vec::new();
    actors.extend(
        batch
            .break_block
            .iter()
            .filter(|update| update.chunk == chunk)
            .map(|update| ActionActor::Player(update.gid)),
    );
    actors.extend(
        batch
            .place_block
            .iter()
            .filter(|update| update.chunk == chunk)
            .map(|update| ActionActor::Player(update.gid)),
    );
    actors.extend(
        batch
            .attacks
            .iter()
            .filter(|attack| {
                attack.attacker.chunk == chunk
                    || attack.targets().any(|target| target.target.chunk() == chunk)
            })
            .map(|attack| attack.actor),
    );
    actors.extend(
        batch
            .fire
            .iter()
            .filter(|update| update.chunk == chunk)
            .map(|update| ActionActor::Player(update.gid)),
    );
    actors.extend(
        batch
            .entity_mutations
            .iter()
            .filter(|update| update.chunk() == chunk)
            .map(|update| update.actor),
    );
    actors.extend(
        batch
            .inv_ops
            .iter()
            .filter(|operation| operation.pickup.as_ref().is_some_and(|pickup| pickup.entity.chunk == chunk))
            .map(|operation| ActionActor::Player(operation.gid)),
    );
    actors.sort();
    actors.dedup();
    actors
}

fn world_driven_winners(
    action: &WorldDrivenAction,
    frame: &WorldDrivenFrame,
) -> Vec<(ConflictKey, ActionActor)> {
    let mut winners: Vec<_> = frame
        .edits()
        .into_iter()
        .map(|edit| (ConflictKey::for_block(edit.pos), action.actor))
        .collect();
    winners.sort();
    winners.dedup();
    winners
}

fn winners_for_chunk(
    tick: TickStamp,
    batch: &TickBatch,
    chunk: ChunkAddr,
) -> Vec<(ConflictKey, ActionActor)> {
    let mut claims: Vec<(ConflictKey, ActionActor)> = batch
        .break_block
        .iter()
        .filter(|update| update.chunk == chunk)
        .map(|update| (ConflictKey::for_block(update.pos), ActionActor::Player(update.gid)))
        .chain(
            batch
                .place_block
                .iter()
                .filter(|update| update.chunk == chunk)
                .map(|update| (ConflictKey::for_block(update.pos), ActionActor::Player(update.gid))),
        )
        .collect();
    claims.extend(
        batch
            .attacks
            .iter()
            .flat_map(|attack| {
                attack.targets().filter_map(move |target| {
                    (target.target.chunk() == chunk).then_some((
                        match target.target {
                            pumpkin_cluster::protocol::EntityMutationTarget::Entity(entity) => {
                                ConflictKey::for_entity(entity)
                            }
                            pumpkin_cluster::protocol::EntityMutationTarget::Player { gid, .. } => {
                                ConflictKey::for_player(gid)
                            }
                        },
                        attack.actor,
                    ))
                })
            }),
    );
    claims.extend(
        batch
            .entity_mutations
            .iter()
            .filter(|update| update.chunk() == chunk)
            .map(|update| (pumpkin_cluster::entity_action::conflict_key(*update), update.actor)),
    );
    claims.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| action_actor_order(tick, left.1, right.1))
    });
    claims.dedup_by_key(|entry| entry.0);
    claims
}

fn action_actor_order(tick: TickStamp, left: ActionActor, right: ActionActor) -> std::cmp::Ordering {
    order_action_actors(CLUSTER_SEED, tick, left, right)
}

pub fn apply_batch_to_server(
    server: &Arc<Server>,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    batch: &TickBatch,
) {
    let mut ordered = batch.clone();
    sort_batch_for_apply(&mut ordered);
    for op in &ordered.inv_ops {
        if ordered
            .place_block
            .iter()
            .any(|update| place_consumes_operation(update, op))
        {
            continue;
        }
        if ledger.apply_op(op) == pumpkin_cluster::inventory::InvVerdict::Applied {
            publish_inventory_operation(ledger, op);
        }
    }
    for update in &ordered.break_block {
        let _ = apply_break_atomic(server, update);
    }
    for update in &ordered.place_block {
        let consume = ordered
            .inv_ops
            .iter()
            .find(|operation| place_consumes_operation(update, operation));
        if update.count_before != update.count_after && consume.is_none() {
            warn!(?update.gid, tick = update.tick.0, "cluster place missing semantic inventory consume");
            continue;
        }
        if !place_preconditions_match(server, update) {
            PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if let Some(operation) = consume
            && ledger.apply_op(operation) != pumpkin_cluster::inventory::InvVerdict::Applied
        {
            PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let outcome = apply_place_atomic(server, ledger, update);
        if outcome.applied {
            if let Some(operation) = consume {
                publish_inventory_operation(ledger, operation);
            }
        } else if let Some(operation) = consume
            && let Some(preconditions) = &operation.preconditions
        {
            ledger.record_stack(operation.gid, operation.src, preconditions.source_before.clone());
        }
    }
}

fn place_consumes_operation(
    update: &PlaceBlockUpdate,
    operation: &pumpkin_cluster::inventory::InventoryOp,
) -> bool {
    let location = pumpkin_cluster::place_emit::place_inv_loc(update);
    operation.gid == update.gid
        && operation.tick == update.tick
        && operation.kind == pumpkin_cluster::inventory::InvOpKind::Consume
        && operation.src == location
        && operation.dst == location
        && operation.item == update.item
        && operation.count == update.count_before.saturating_sub(update.count_after)
        && operation.preconditions.as_ref().is_some_and(|preconditions| {
            preconditions.source_before.item == update.item
                && preconditions.source_before.count == update.count_before
                && preconditions.destination_before == preconditions.source_before
        })
}

fn place_preconditions_match(server: &Arc<Server>, update: &PlaceBlockUpdate) -> bool {
    default_world(server).is_some_and(|world| {
        world
            .get_block_state_id_if_loaded(&cluster_pos(update.pos))
            .is_some_and(|state| state.as_u16() == update.expected_old_state)
            && pumpkin_data::BlockStateId::new(update.new_state).is_some()
    })
}

fn apply_optimistic_inventory(
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    batch: &TickBatch,
) {
    for operation in &batch.inv_ops {
        if ledger.apply_op(operation) == pumpkin_cluster::inventory::InvVerdict::Applied {
            publish_inventory_operation(ledger, operation);
        } else {
            warn!(?operation.gid, tick = operation.tick.0, "cluster optimistic inventory operation rejected");
        }
    }
}

fn apply_optimistic_driven_inventory(
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    frame: &WorldDrivenFrame,
) {
    let WorldDrivenFrame::Interact(pumpkin_cluster::interact::InteractUpdate::Atomic(atomic)) = frame else {
        return;
    };
    let Some(inventory) = &atomic.inventory else {
        return;
    };
    if ledger.apply_op(&inventory.op) == pumpkin_cluster::inventory::InvVerdict::Applied {
        publish_inventory_operation(ledger, &inventory.op);
    } else {
        warn!(?inventory.op.gid, tick = inventory.op.tick.0, "cluster optimistic interaction inventory operation rejected");
    }
}

fn stage_optimistic_entity_mutations(batch: &mut TickBatch) {
    for update in &batch.entity_mutations {
        let _ = super::cluster_entity_apply::stage_owned_entity_mutation(*update);
    }
}

fn pickup_reservation_key(operation: &pumpkin_cluster::inventory::InventoryOp) -> Option<Vec<u8>> {
    let pickup = operation.pickup.as_ref()?;
    postcard::to_allocvec(&(
        operation.gid,
        operation.src,
        operation.dst,
        operation.preconditions.as_ref(),
        pickup,
    ))
    .ok()
}

fn reserve_local_pickups(
    reservations: &mut HashSet<Vec<u8>>,
    batch: &mut TickBatch,
) {
    batch.inv_ops.retain(|operation| {
        let Some(key) = pickup_reservation_key(operation) else {
            return true;
        };
        reservations.insert(key)
    });
}

fn release_pickup_reservations(
    reservations: &mut HashSet<Vec<u8>>,
    batches: &[(TickBatch, bool)],
) {
    for (batch, already_optimistic) in batches {
        if !already_optimistic {
            continue;
        }
        for operation in &batch.inv_ops {
            if let Some(key) = pickup_reservation_key(operation) {
                reservations.remove(&key);
            }
        }
    }
}

fn invsee_index(location: pumpkin_cluster::inventory::InvLoc) -> u16 {
    u16::from(location.inv).saturating_mul(128).saturating_add(location.slot)
}

fn publish_inventory_operation(
    ledger: &pumpkin_cluster::inventory::InvLedger,
    operation: &pumpkin_cluster::inventory::InventoryOp,
) {
    let mut replicas = (*INVENTORY_REPLICAS.load_full()).clone();
    let replica = replicas.entry(operation.gid).or_insert_with(|| ReplicatedInventory {
        selected: 0,
        slots: Vec::new(),
    });
    let slots = &mut replica.slots;
    for location in [operation.src, operation.dst] {
        let index = invsee_index(location);
        let stack = ledger
            .get_stack(operation.gid, location)
            .unwrap_or_else(pumpkin_cluster::inventory::InventoryStack::empty);
        let slot = pumpkin_cluster::invsee::InvseeSlot::new(
            index,
            stack.item,
            stack.count,
            stack.nbt,
        );
        if let Some(existing) = slots.iter_mut().find(|existing| existing.index == index) {
            *existing = slot;
        } else {
            slots.push(slot);
        }
    }
    slots.sort_by_key(|slot| slot.index);
    INVENTORY_REPLICAS.store(Arc::new(replicas));
}

fn seed_inventory_ledger(
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    gid: GlobalPlayerId,
    slots: Vec<pumpkin_cluster::invsee::InvseeSlot>,
) {
    for slot in slots {
        let location = if slot.index == 40 {
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_OFFHAND, 0)
        } else {
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, slot.index)
        };
        ledger.record_stack(
            gid,
            location,
            pumpkin_cluster::inventory::InventoryStack {
                item: slot.item_id,
                count: slot.count,
                nbt: slot.nbt,
            },
        );
    }
}

fn merge_world_batches(tick: TickStamp, batches: &[(TickBatch, bool)]) -> TickBatch {
    let mut merged = TickBatch::new(tick);
    for (batch, already_optimistic) in batches {
        if !already_optimistic {
            merged.break_block.extend_from_slice(&batch.break_block);
            merged.place_block.extend_from_slice(&batch.place_block);
            merged.inv_ops.extend_from_slice(&batch.inv_ops);
            merged.fire.extend_from_slice(&batch.fire);
        }
        merged.attacks.extend_from_slice(&batch.attacks);
        merged.entity_mutations.extend_from_slice(&batch.entity_mutations);
    }
    merged
}

fn promote_accepted_world_truth(
    world: &crate::world::World,
    tick: TickStamp,
    batches: &[(TickBatch, bool)],
    driven: &[(WorldDrivenAction, WorldDrivenFrame, bool)],
) {
    let mut winners = Vec::new();
    for (batch, _) in batches {
        for update in &batch.break_block {
            if let Some(state) = read_delta_state(world, update.pos) {
                winners.push((update.pos, state));
            }
        }
        for update in &batch.place_block {
            if let Some(state) = read_delta_state(world, update.pos) {
                winners.push((update.pos, state));
            }
        }
    }
    for (_, frame, _) in driven {
        for edit in frame.edits() {
            if let Some(state) = read_delta_state(world, edit.pos) {
                winners.push((edit.pos, state));
            }
        }
    }
    winners.sort_by_key(|(pos, _)| (pos.x, pos.y, pos.z));
    winners.dedup_by_key(|(pos, _)| *pos);
    if world.level.cluster_dual_enabled() {
        promote_chunk_winners(world, &winners);
    }
    world.level.cluster_note_applied_tick(tick.0);
}

pub(crate) fn promote_transactional_explosion_truth(
    world: &crate::world::World,
    tick: TickStamp,
    update: &pumpkin_cluster::world_delta::TransactionalExplosionUpdate,
) {
    let mut winners = Vec::new();
    for edit in &update.edits {
        if let Some(state) = read_delta_state(world, edit.pos) {
            winners.push((edit.pos, state));
        }
    }
    winners.sort_by_key(|(pos, _)| (pos.x, pos.y, pos.z));
    winners.dedup_by_key(|(pos, _)| *pos);
    if world.level.cluster_dual_enabled() {
        promote_chunk_winners(world, &winners);
    }
    world.level.cluster_note_applied_tick(tick.0);
}

fn dependency_ordinal(cause: WorldActionRef, dependency: pumpkin_cluster::world_delta::WorldDependency) -> u16 {
    let mut value = u64::from(cause.owner.0)
        ^ u64::from(cause.tick.0).rotate_left(9)
        ^ u64::from(cause.ordinal).rotate_left(17)
        ^ (cause.anchor.x as u32 as u64).rotate_left(23)
        ^ (cause.anchor.y as u32 as u64).rotate_left(31)
        ^ (cause.anchor.z as u32 as u64).rotate_left(39);
    value ^= (dependency.target.x as u32 as u64).rotate_left(7);
    value ^= (dependency.target.y as u32 as u64).rotate_left(29);
    value ^= (dependency.target.z as u32 as u64).rotate_left(47);
    value ^= u64::from(dependency.kind as u8).rotate_left(55);
    value as u16
}

fn causal_reference(frame: &WorldDrivenFrame) -> Option<WorldActionRef> {
    match frame {
        WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(update)) => Some(update.group.reference),
        _ => None,
    }
}

fn world_driven_actor_is_valid(action: &WorldDrivenAction, frame: &WorldDrivenFrame) -> bool {
    match frame {
        WorldDrivenFrame::Delta(WorldDelta::RandomTick(update)) => {
            action.actor == ActionActor::Server(update.holder)
        }
        WorldDrivenFrame::Delta(WorldDelta::Redstone(update)) => {
            action.actor == ActionActor::Server(update.holder)
        }
        WorldDrivenFrame::Delta(WorldDelta::Explosion(update)) => {
            action.actor == ActionActor::Server(update.holder)
        }
        WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(update)) => {
            action.actor == ActionActor::Server(update.owner())
                && action.seq == ActionSeq(update.group.reference.ordinal)
        }
        WorldDrivenFrame::Delta(WorldDelta::TransactionalExplosion(update)) => {
            action.actor == ActionActor::Server(update.owner())
                && action.seq == ActionSeq(update.group.reference.ordinal)
        }
        WorldDrivenFrame::Interact(update) => {
            action.actor == ActionActor::Player(update.gid())
                && action.seq.0 == update.seq().0
        }
    }
}

fn action_origin(actor: ActionActor) -> ServerId {
    match actor {
        ActionActor::Player(player) => player.server,
        ActionActor::Server(server) => server,
    }
}

fn enqueue_accepted_dependencies(
    server: &Arc<Server>,
    buckets: &mut WorldTickBuckets,
    local: ServerId,
    frame: &WorldDrivenFrame,
) {
    let Some(cause) = causal_reference(frame) else {
        return;
    };
    for dependency in frame.dependencies().iter().copied() {
        if dependency.cause != cause || !dependency.is_valid() {
            warn!(tick = cause.tick.0, "cluster causal dependency rejected after accepted source");
            continue;
        }
        let chunk = chunk_of_block(dependency.target.x, dependency.target.z);
        let group = WorldActionGroup::cascade(
            cause.owner,
            dependency.fire_tick,
            dependency_ordinal(cause, dependency),
            dependency.target,
            cause,
        );
        let Some(update) = capture_causal_redstone_update(
            group,
            chunk,
            vec![BlockDelta {
                pos: dependency.target,
                old_state: dependency.expected_old_state,
                new_state: dependency.new_state,
            }],
            Vec::new(),
        ) else {
            warn!(tick = dependency.fire_tick.0, "cluster causal dependency could not be encoded");
            continue;
        };
        let frame = WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(update));
        let Ok(action) = encode_world_driven(
            ActionActor::Server(cause.owner),
            ActionSeq(dependency_ordinal(cause, dependency)),
            &frame,
        ) else {
            warn!(tick = dependency.fire_tick.0, "cluster causal dependency encode failed");
            continue;
        };
        let bytes = action.bytes.clone();
        if let Some(accept) = buckets.push_world_driven(server, local, action.clone(), frame, false) {
            forward_world_driven(buckets, &action, bytes, local);
            forward_accept(buckets, &accept, local);
        } else {
            warn!(tick = dependency.fire_tick.0, chunk_x = chunk.x, chunk_z = chunk.z, "cluster causal dependency has no holder route");
        }
    }
}

fn resolve_globally_accepted(
    server: &Arc<Server>,
    buckets: &mut WorldTickBuckets,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    combat: &mut super::cluster_combat_apply::CombatApplyState,
    pickup_reservations: &mut HashSet<Vec<u8>>,
) {
    let world = default_world(server);
    while let Some((tick, batches)) = buckets.pop_ready() {
        release_pickup_reservations(pickup_reservations, &batches);
        let mut driven = buckets.take_world_driven(tick);
        let driven_for_promotion: Vec<_> = driven
            .iter()
            .filter(|(_, frame, _)| frame.transactional_explosion().is_none())
            .cloned()
            .collect();
        if let Some(world) = world.as_ref() {
            world.level.cluster_note_ground_tick(tick.0);
        }
        let merged = merge_world_batches(tick, &batches);
        if !merged.is_empty() {
            apply_batch_to_server(server, ledger, &merged);
            super::cluster_combat_apply::apply_accepted_batch(server, combat, &merged);
        }
        driven.sort_by(|left, right| {
            order_world_driven(
                CLUSTER_SEED,
                &(left.0.clone(), left.1.clone()),
                &(right.0.clone(), right.1.clone()),
            )
        });
        let mut accepted_causal_sources = Vec::new();
        if let Some(world) = world.as_ref() {
            for (action, frame, already_optimistic) in driven {
                if let Some(update) = frame.transactional_explosion() {
                    if already_optimistic {
                        warn!(tick = tick.0, "cluster optimistic explosion was not staged by transaction actor");
                    } else if !super::cluster_entity_apply::apply_transactional_explosion_entities(
                        Arc::clone(world),
                        update.clone(),
                    ) {
                        warn!(tick = tick.0, "cluster explosion transaction actor input queue full");
                    }
                    continue;
                }
                if already_optimistic {
                    if causal_reference(&frame).is_some() {
                        accepted_causal_sources.push(frame);
                    }
                    continue;
                }
                let applied = match action.actor {
                    ActionActor::Player(gid) => ledger.with_cells(gid, |inventory| {
                        let judged = judge_world_driven(
                            &frame,
                            |pos| read_delta_state(world, pos),
                            Some(&*inventory),
                        );
                        matches!(
                            judged,
                            pumpkin_cluster::world_driven::WorldDrivenDecision::Accept { .. }
                        ) && apply_world_driven(
                            &frame,
                            |pos, state| write_delta_state(world, pos, state),
                            Some(inventory),
                        )
                    }),
                    ActionActor::Server(_) => matches!(
                        judge_world_driven(&frame, |pos| read_delta_state(world, pos), None),
                        pumpkin_cluster::world_driven::WorldDrivenDecision::Accept { .. }
                    ) && apply_world_driven(
                        &frame,
                        |pos, state| write_delta_state(world, pos, state),
                        None,
                    ),
                };
                if applied {
                    if causal_reference(&frame).is_some() {
                        accepted_causal_sources.push(frame);
                    }
                } else {
                    warn!(tick = tick.0, "cluster world-driven accepted application rejected");
                }
            }
        }
        let mut changed_players = BTreeSet::new();
        for (batch, _) in &batches {
            changed_players.extend(batch.break_block.iter().map(|update| update.gid));
            changed_players.extend(batch.place_block.iter().map(|update| update.gid));
            changed_players.extend(batch.inv_ops.iter().map(|operation| operation.gid));
        }
        for gid in changed_players {
            let _ = super::cluster_playerdata::stage_accepted_playerdata(server, gid, tick);
        }
        let local = ServerId(server.advanced_config.cluster.server_id);
        let primary_copy = local_origin_actions(tick, &batches, local);
        if !primary_copy.is_empty() {
            if let Some(copy) = ACCEPTED_TICK_COPY.get() {
                if copy.try_send(primary_copy).is_err() {
                    warn!(tick = tick.0, "cluster accepted tick copy queue full");
                }
            }
        }
        if let Some(world) = world.as_ref() {
            promote_accepted_world_truth(world, tick, &batches, &driven_for_promotion);
        }
        let local = ServerId(server.advanced_config.cluster.server_id);
        for source in &accepted_causal_sources {
            enqueue_accepted_dependencies(server, buckets, local, source);
        }
    }
}

fn local_origin_actions(
    tick: TickStamp,
    batches: &[(TickBatch, bool)],
    local: ServerId,
) -> TickBatch {
    let mut copy = TickBatch::new(tick);
    for (batch, _) in batches {
        copy.break_block.extend(
            batch
                .break_block
                .iter()
                .filter(|update| update.gid.server == local)
                .copied(),
        );
        copy.place_block.extend(
            batch
                .place_block
                .iter()
                .filter(|update| update.gid.server == local)
                .copied(),
        );
        copy.inv_ops.extend(
            batch
                .inv_ops
                .iter()
                .filter(|operation| operation.gid.server == local)
                .cloned(),
        );
        copy.attacks.extend(
            batch
                .attacks
                .iter()
                .filter(|attack| match attack.actor {
                    ActionActor::Player(gid) => gid.server == local,
                    ActionActor::Server(server) => server == local,
                })
                .cloned(),
        );
        copy.fire.extend(
            batch
                .fire
                .iter()
                .filter(|update| update.gid.server == local)
                .copied(),
        );
        copy.entity_mutations.extend(
            batch
                .entity_mutations
                .iter()
                .filter(|update| match update.actor {
                    ActionActor::Player(gid) => gid.server == local,
                    ActionActor::Server(server) => server == local,
                })
                .copied(),
        );
    }
    copy
}

pub fn apply_batch_bytes(
    server: &Arc<Server>,
    buckets: &mut WorldTickBuckets,
    _delta: &mut WorldDeltaState,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    combat: &mut super::cluster_combat_apply::CombatApplyState,
    pickup_reservations: &mut HashSet<Vec<u8>>,
    parcel_peer: ServerId,
    bytes: &[u8],
) {
    if let Ok((action, frame)) = decode_world_driven(bytes) {
        let expected_primary = ServerId(server.advanced_config.cluster.primary_server_id);
        if matches!(frame, WorldDrivenFrame::Delta(WorldDelta::RandomTick(_)))
            && action.actor != ActionActor::Server(expected_primary)
        {
            warn!(peer = parcel_peer.0, "cluster random tick did not declare configured primary actor");
            return;
        }
        if !world_driven_actor_is_valid(&action, &frame) {
            warn!(peer = parcel_peer.0, ?action.actor, "cluster world-driven actor did not match frame");
            return;
        }
        if action_origin(action.actor) != parcel_peer {
            warn!(peer = parcel_peer.0, ?action.actor, "cluster world-driven actor did not match stream peer");
            return;
        }
        let local = ServerId(server.advanced_config.cluster.server_id);
        if let Some(accept) = buckets.push_world_driven(server, local, action, frame, false) {
            forward_accept(buckets, &accept, local);
        }
        resolve_globally_accepted(server, buckets, ledger, combat, pickup_reservations);
        return;
    }
    let batch = match decode_batch(bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster world batch decode failed");
            WORLD_DECODE_ERRORS.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    WORLD_BATCHES.fetch_add(1, Ordering::Relaxed);
    let local = ServerId(server.advanced_config.cluster.server_id);
    let accept = buckets.push_batch(server, local, batch, false);
    if let Some(accept) = accept.as_ref() {
        forward_accept(buckets, accept, local);
    }
    resolve_globally_accepted(server, buckets, ledger, combat, pickup_reservations);
}

fn forward_accept(buckets: &WorldTickBuckets, batch: &AcceptBatch, local: ServerId) {
    let Some(outbox) = WORLD_ACTION_OUTBOX.get() else {
        warn!(tick = batch.tick.0, "cluster world acceptance outbox is not installed");
        return;
    };
    let Ok(bytes) = encode_accept(batch) else {
        warn!(tick = batch.tick.0, "cluster world acceptance encode failed");
        return;
    };
    for peer in buckets.targets(batch, local) {
        if outbox
            .outbound
            .try_send(OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: bytes.clone(),
            })
            .is_err()
        {
            warn!(peer = peer.0, tick = batch.tick.0, "cluster world acceptance delivery queue full");
        }
    }
}

fn forward_action_batch(buckets: &WorldTickBuckets, batch: &TickBatch, local: ServerId) {
    let Some(outbox) = WORLD_ACTION_OUTBOX.get() else {
        warn!(tick = batch.tick.0, "cluster world action outbox is not installed");
        return;
    };
    let Some(actor) = batch_actor(batch) else {
        return;
    };
    let kind = if !batch.attacks.is_empty()
        || !batch.fire.is_empty()
        || !batch.entity_mutations.is_empty()
    {
        StreamKind::PlayerCombat
    } else {
        StreamKind::PlayerWorld
    };
    let Ok(bytes) = pumpkin_cluster::codec::encode_batch(batch) else {
        warn!(tick = batch.tick.0, ?actor, "cluster world action encode failed");
        return;
    };
    let header_player = match actor {
        ActionActor::Player(player) => Some(player),
        ActionActor::Server(_) => None,
    };
    for peer in buckets.action_targets(batch, local) {
        if outbox
            .outbound
            .try_send(OutboundParcel {
                peer,
                header: StreamHeader::new(kind, header_player),
                bytes: bytes.clone(),
            })
            .is_err()
        {
            warn!(peer = peer.0, tick = batch.tick.0, ?actor, "cluster world action delivery queue full");
        }
    }
}

fn forward_world_driven(
    buckets: &WorldTickBuckets,
    action: &WorldDrivenAction,
    bytes: Vec<u8>,
    local: ServerId,
) {
    let Some(outbox) = WORLD_ACTION_OUTBOX.get() else {
        warn!(tick = action.tick.0, "cluster world action outbox is not installed");
        return;
    };
    for peer in buckets.frame_targets(action.tick, action.chunk, local) {
        let player = match action.actor {
            ActionActor::Player(player) => Some(player),
            ActionActor::Server(_) => None,
        };
        if outbox.outbound.try_send(OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::PlayerWorld, player),
            bytes: bytes.clone(),
        }).is_err() {
            warn!(peer = peer.0, tick = action.tick.0, "cluster world-driven action delivery queue full");
        }
    }
}

pub fn spawn_world_apply(
    server: &Arc<Server>,
    mut world_rx: mpsc::Receiver<InboundParcel>,
    mut combat_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    let (local_tx, mut local_rx) = mpsc::channel(1024);
    let _ = LOCAL_OPTIMISTIC_BATCHES.set(local_tx);
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel(1024);
    let _ = SNAPSHOT_PENDING_BATCHES.set(snapshot_tx);
    let (snapshot_frame_tx, mut snapshot_frame_rx) = mpsc::channel(1024);
    let _ = SNAPSHOT_PENDING_FRAMES.set(snapshot_frame_tx);
    let (accept_tx, mut accept_rx) = mpsc::channel(1024);
    let _ = WORLD_ACCEPT_INPUT.set(accept_tx);
    let (world_driven_tx, mut world_driven_rx) = mpsc::channel(1024);
    let _ = LOCAL_WORLD_DRIVEN.set(world_driven_tx);
    let (inventory_seed_tx, mut inventory_seed_rx) = mpsc::channel(1024);
    let _ = INVENTORY_SEEDS.set(inventory_seed_tx);
    server.spawn_task(async move {
        let mut buckets = WorldTickBuckets::new();
        let mut delta = WorldDeltaState::new();
        let mut ledger = pumpkin_cluster::inventory::InvLedger::new();
        let mut combat = super::cluster_combat_apply::CombatApplyState::new();
        let mut pickup_reservations = HashSet::new();
        let mut first = true;
        loop {
            tokio::select! {
                local = local_rx.recv() => {
                    let Some(mut batch) = local else { break };
                    reserve_local_pickups(&mut pickup_reservations, &mut batch);
                    stage_optimistic_entity_mutations(&mut batch);
                    if batch.is_empty() {
                        continue;
                    }
                    let local = ServerId(task_server.advanced_config.cluster.server_id);
                    apply_optimistic_inventory(&mut ledger, &batch);
                    if let Some(accept) = buckets.push_batch(&task_server, local, batch, true) {
                        let batches = buckets.pending.get(&accept.tick).expect("queued local world batch");
                        if let Some((batch, _)) = batches.last() {
                            forward_action_batch(&buckets, batch, local);
                        }
                        forward_accept(&buckets, &accept, local);
                    }
                    resolve_globally_accepted(&task_server, &mut buckets, &mut ledger, &mut combat, &mut pickup_reservations);
                }
                driven = world_driven_rx.recv() => {
                    let Some(bytes) = driven else { break };
                    let Ok((action, frame)) = decode_world_driven(&bytes) else {
                        warn!("cluster local world-driven frame decode failed");
                        continue;
                    };
                    let local = ServerId(task_server.advanced_config.cluster.server_id);
                    apply_optimistic_driven_inventory(&mut ledger, &frame);
                    if let Some(accept) = buckets.push_world_driven(&task_server, local, action.clone(), frame, true) {
                        forward_world_driven(&buckets, &action, bytes, local);
                        forward_accept(&buckets, &accept, local);
                    }
                    resolve_globally_accepted(&task_server, &mut buckets, &mut ledger, &mut combat, &mut pickup_reservations);
                }
                seed = inventory_seed_rx.recv() => {
                    let Some((gid, slots)) = seed else { break };
                    seed_inventory_ledger(&mut ledger, gid, slots);
                }
                acceptance = accept_rx.recv() => {
                    let Some((holder, bytes)) = acceptance else { break };
                    match decode_accept(&bytes) {
                        Ok(batch) => buckets.apply_accept(holder, batch),
                        Err(error) => warn!(%error, "cluster world acceptance decode failed"),
                    }
                    resolve_globally_accepted(&task_server, &mut buckets, &mut ledger, &mut combat, &mut pickup_reservations);
                }
                snapshot = snapshot_rx.recv() => {
                    let Some(batch) = snapshot else { break };
                    let local = ServerId(task_server.advanced_config.cluster.server_id);
                    if let Some(accept) = buckets.push_batch(&task_server, local, batch, false) {
                        forward_accept(&buckets, &accept, local);
                    }
                    resolve_globally_accepted(&task_server, &mut buckets, &mut ledger, &mut combat, &mut pickup_reservations);
                }
                snapshot_frame = snapshot_frame_rx.recv() => {
                    let Some(bytes) = snapshot_frame else { break };
                    let Ok((action, frame)) = decode_world_driven(&bytes) else {
                        warn!("cluster snapshot world-driven frame decode failed");
                        continue;
                    };
                    let local = ServerId(task_server.advanced_config.cluster.server_id);
                    if let Some(accept) = buckets.push_world_driven(&task_server, local, action, frame, false) {
                        forward_accept(&buckets, &accept, local);
                    }
                    resolve_globally_accepted(&task_server, &mut buckets, &mut ledger, &mut combat, &mut pickup_reservations);
                }
                incoming = world_rx.recv() => {
                    let Some(parcel) = incoming else { break };
                    if parcel.header.kind != StreamKind::PlayerWorld {
                        continue;
                    }
                    if first {
                        first = false;
                        debug!(from = parcel.peer.0, "cluster world stream started");
                    }
                    let parcel_peer = parcel.peer;
                    apply_batch_bytes(
                        &task_server,
                        &mut buckets,
                        &mut delta,
                        &mut ledger,
                        &mut combat,
                        &mut pickup_reservations,
                        parcel_peer,
                        &parcel.bytes,
                    );
                }
                incoming = combat_rx.recv() => {
                    let Some(parcel) = incoming else { break };
                    if parcel.header.kind != StreamKind::PlayerCombat {
                        continue;
                    }
                    let Ok(batch) = decode_batch(&parcel.bytes) else {
                        warn!(peer = parcel.peer.0, "cluster combat batch decode failed");
                        continue;
                    };
                    let authentic = |gid: GlobalPlayerId| parcel.header.player == Some(gid) && gid.server == parcel.peer;
                    if !batch.attacks.iter().all(|attack| match attack.actor {
                            ActionActor::Player(gid) => authentic(gid),
                            ActionActor::Server(server) => {
                                parcel.header.player.is_none() && server == parcel.peer
                            }
                        })
                        || !batch.fire.iter().all(|update| authentic(update.gid))
                        || !batch.entity_mutations.iter().all(|update| match update.actor {
                            ActionActor::Player(gid) => authentic(gid),
                            ActionActor::Server(server) => {
                                parcel.header.player.is_none() && server == parcel.peer
                            }
                        })
                    {
                        warn!(peer = parcel.peer.0, "cluster combat stream player mismatch");
                        continue;
                    }
                    apply_batch_bytes(
                        &task_server,
                        &mut buckets,
                        &mut delta,
                        &mut ledger,
                        &mut combat,
                        &mut pickup_reservations,
                        parcel.peer,
                        &parcel.bytes,
                    );
                }
            }
        }
        debug!("cluster world stream closed");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};
    use pumpkin_cluster::identity::GlobalPlayerId;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn dirt_wood_conflict_flags_mismatch() {
        assert!(!dirt_vs_wood_conflict(7, 7));
        assert!(dirt_vs_wood_conflict(dirt_state_id(), wood_state_ids()[0]));
        assert!(dirt_vs_wood_conflict(wood_state_ids()[1], dirt_state_id()));
        assert!(dirt_vs_wood_conflict(3, 4));
    }

    #[test]
    fn place_conflict_flags_duplicate() {
        assert!(place_conflict(9, 9));
        assert!(!place_conflict(9, 10));
    }

    #[test]
    fn tick_ordering_is_deterministic_across_arrivals() {
        let tick = TickStamp(41);
        let first = gid(1, 1);
        let second = gid(2, 1);
        let mut arrival_a = [second, first];
        let mut arrival_b = [first, second];
        for arrival in [&mut arrival_a, &mut arrival_b] {
            arrival.sort_by(|left, right| order_players(CLUSTER_SEED, tick, *left, *right));
        }
        assert_eq!(arrival_a, arrival_b);
    }

    #[test]
    fn winners_group_by_chunk_for_promotion() {
        let winners = [
            (
                pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 1 },
                8_u16,
            ),
            (
                pumpkin_cluster::protocol::BlockPos { x: 2, y: 64, z: 3 },
                10_u16,
            ),
            (
                pumpkin_cluster::protocol::BlockPos { x: 20, y: 65, z: 21 },
                0_u16,
            ),
        ];
        let grouped = group_winners_by_chunk(&winners);
        assert_eq!(grouped.len(), 2);
        let home = pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 };
        let away = pumpkin_cluster::protocol::ChunkAddr { x: 1, z: 1 };
        assert_eq!(grouped[&home].len(), 2);
        assert_eq!(grouped[&away].len(), 1);
    }

    #[test]
    fn inventory_delta_consumes_one() {
        let delta = InventoryStackDelta {
            slot: 0,
            count_before: 64,
            count_after: 63,
        };
        assert_eq!(delta.count_before.saturating_sub(1), delta.count_after);
    }

    #[test]
    fn primary_copy_contains_only_originating_peer_actions() {
        let tick = TickStamp(51);
        let local = ServerId(3);
        let remote = ServerId(4);
        let mut batch = TickBatch::new(tick);
        batch.break_block.push(BreakBlockUpdate {
            gid: gid(local.0, 1),
            seq: pumpkin_cluster::identity::PlayerSeq(1),
            tick,
            pos: pumpkin_cluster::protocol::BlockPos { x: 0, y: 64, z: 0 },
            expected_old_state: 1,
            chunk: ChunkAddr { x: 0, z: 0 },
        });
        batch.break_block.push(BreakBlockUpdate {
            gid: gid(remote.0, 1),
            seq: pumpkin_cluster::identity::PlayerSeq(1),
            tick,
            pos: pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 0 },
            expected_old_state: 1,
            chunk: ChunkAddr { x: 0, z: 0 },
        });
        let copy = local_origin_actions(tick, &[(batch, false)], local);
        assert_eq!(copy.break_block.len(), 1);
        assert_eq!(copy.break_block[0].gid.server, local);
    }
}
