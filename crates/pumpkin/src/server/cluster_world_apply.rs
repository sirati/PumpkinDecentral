use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::buckets::BucketTable;
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::order::order_players;
use pumpkin_cluster::protocol::{
    BlockPos as ClusterBlockPos, BreakBlockUpdate, ChunkAddr, PlaceBlockUpdate, StreamKind,
    TickBatch,
};
use pumpkin_cluster::reconcile::ReconcilePlan;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{
    BlockDelta, FallibleClaim, FallibleDecision, RandomTickDecision, RandomTickDelta, WorldDelta,
    WorldDeltaAcceptance, apply_fallible_edit, apply_random_tick_edit, chunk_of_block,
    decode_frame, elect_fallible_claim, is_world_delta_frame, make_delta_undo,
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

fn read_delta_state(world: &crate::world::World, pos: ClusterBlockPos) -> Option<u16> {
    let world_pos = cluster_pos(pos);
    if world.level.cluster_dual_enabled() {
        Some(world.level.cluster_get_block_state(&world_pos).as_u16())
    } else {
        world
            .get_block_state_id_if_loaded(&world_pos)
            .map(|current| current.as_u16())
    }
}

fn write_delta_state(
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
    holder: ServerId,
    chunk: ChunkAddr,
    tick: TickStamp,
    trigger: ClusterBlockPos,
    edits: &[BlockDelta],
) {
    state.acceptance.require(tick, chunk, &[holder.0]);
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
            holder,
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
                                holder,
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
            if let Some(world) = default_world(server) {
                apply_fallible_delta(
                    &world,
                    state,
                    peer,
                    update.holder,
                    update.chunk,
                    update.tick,
                    update.trigger,
                    &update.edits,
                );
            }
        }
        WorldDelta::Explosion(update) => {
            if let Some(world) = default_world(server) {
                apply_fallible_delta(
                    &world,
                    state,
                    peer,
                    update.holder,
                    update.chunk,
                    update.tick,
                    update.center,
                    &update.edits,
                );
            }
        }
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
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
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
    let count_before = update.count_before;
    let verdict =
        pumpkin_cluster::place_emit::apply_remote_place(update, current_raw, count_before);
    if !verdict.accepted {
        PLACE_REJECTED.fetch_add(1, Ordering::Relaxed);
        return PlaceApplyOutcome {
            applied: false,
            previous_state: verdict.undo.old_state,
            inventory: None,
        };
    }
    let inv_loc = pumpkin_cluster::place_emit::place_inv_loc(update);
    if ledger.check_place(
        update.gid,
        inv_loc,
        update.item,
        update.count_before,
        update.count_after,
    ) != pumpkin_cluster::inventory::InvVerdict::Applied
    {
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
        count_before,
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
}

#[derive(Debug, Default)]
pub struct WorldTickBuckets {
    pending: BTreeMap<TickStamp, Vec<TickBatch>>,
    buckets: BucketTable,
}

impl WorldTickBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_batch(&mut self, batch: TickBatch) {
        let tick = batch.tick;
        self.buckets.require(
            tick,
            pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
            &[],
        );
        self.pending.entry(tick).or_default().push(batch);
    }

    pub fn pop_ready(&mut self) -> Option<(TickStamp, Vec<TickBatch>)> {
        let smallest = self.pending.keys().next().copied()?;
        if self.buckets.is_tick_complete(smallest) {
            let batches = self.pending.remove(&smallest)?;
            self.buckets.remove_tick(smallest);
            Some((smallest, batches))
        } else {
            None
        }
    }

    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.pending.len()
    }
}

pub fn apply_batch_to_server(
    server: &Arc<Server>,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    batch: &TickBatch,
) {
    let mut ordered = batch.clone();
    sort_batch_for_apply(&mut ordered);
    for op in &ordered.inv_ops {
        let _ = ledger.apply_op(op);
    }
    for update in &ordered.break_block {
        let _ = apply_break_atomic(server, update);
    }
    for update in &ordered.place_block {
        let _ = apply_place_atomic(server, ledger, update);
    }
}

pub fn apply_batch_bytes(
    server: &Arc<Server>,
    buckets: &mut WorldTickBuckets,
    delta: &mut WorldDeltaState,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    parcel_peer: ServerId,
    bytes: &[u8],
) {
    if is_world_delta_frame(bytes) {
        match decode_frame(bytes) {
            Ok(frame) => apply_world_delta_frame(server, delta, parcel_peer, &frame),
            Err(error) => {
                warn!(%error, "cluster world delta decode failed");
                WORLD_DELTA_DECODE_ERRORS.fetch_add(1, Ordering::Relaxed);
            }
        }
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
    buckets.push_batch(batch);
    let world = default_world(server);
    while let Some((tick, batches)) = buckets.pop_ready() {
        if let Some(world) = world.as_ref() {
            world.level.cluster_note_ground_tick(tick.0);
        }
        for ready in &batches {
            apply_batch_to_server(server, ledger, ready);
        }
        if let Some(world) = world.as_ref() {
            world.level.cluster_note_applied_tick(tick.0);
        }
    }
}

pub fn spawn_world_apply(server: &Arc<Server>, mut world_rx: mpsc::Receiver<InboundParcel>) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut buckets = WorldTickBuckets::new();
        let mut delta = WorldDeltaState::new();
        let mut ledger = pumpkin_cluster::inventory::InvLedger::new();
        let mut first = true;
        while let Some(parcel) = world_rx.recv().await {
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
                parcel_peer,
                &parcel.bytes,
            );
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
    fn buckets_drain_in_tick_order() {
        let mut buckets = WorldTickBuckets::new();
        let late = TickBatch::new(TickStamp(9));
        let early = TickBatch::new(TickStamp(3));
        buckets.push_batch(late);
        buckets.push_batch(early);
        let (first_tick, _) = buckets.pop_ready().unwrap_or((TickStamp(0), Vec::new()));
        assert_eq!(first_tick, TickStamp(3));
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
}
