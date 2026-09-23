//! Tick-bank double buffer between simulation workers and the fuse task.
//!
//! Shape: each worker fills one [`Bank`] while the fuse owns the other.
//! [`bank_pipes`] hands both ends back as a boxed swap plus two fixed-1
//! channels (`full` worker->fuse, `empty` fuse->worker).
//!
//! Rules the code below encodes, so callers do not have to look elsewhere:
//! - [`WorkerEnds::end_tick`] never blocks and never locks: it only uses
//!   `try_recv`/`try_send`, then `mem::swap`s the active [`Box<Bank>`].
//! - Both banks are allocated by [`bank_pipes`]. The empty channel recycles
//!   the cleared box; no later rotation allocates another bank.
//! - When the fuse has not returned a bank, the worker keeps accumulating
//!   records in its current bank. Every record retains its own tick identity.
//! - Slow-fuse noise is throttled: worker backpressure and fuse tick backlog
//!   each log at most once per minute via `tracing::warn!`.

use tokio::sync::mpsc;

use crate::inventory::InventoryOp;
use crate::protocol::{
    BlockingUpdate, BreakAnimUpdate, BreakBlockUpdate, EatAbortUpdate, EatStartUpdate,
    EquipmentUpdate, FireProjectileUpdate, HeldUpdate, HitEntityUpdate, HitPlayerUpdate,
    PlaceBlockUpdate, PosUpdate, PoseUpdate, SkinLayersUpdate, SneakUpdate, SprintUpdate,
    SwingUpdate, TickBatch,
};
use crate::time::TickStamp;

/// Capacity of the worker->fuse handoff: exactly one boxed bank in flight.
const FULL_CAP: usize = 1;
/// Capacity of the fuse->worker recycle: at most one cleared box parked.
const EMPTY_CAP: usize = 1;
/// How often a slow fuse may log; keeps a wedged fuse from spamming.
const SLOW_WARN_COOLDOWN: core::time::Duration = core::time::Duration::from_secs(60);

/// One tick of staged cluster updates; the write bank workers fill.
#[derive(Debug, Default)]
pub struct Bank {
    pub pos: Vec<PosUpdate>,
    pub equipment: Vec<EquipmentUpdate>,
    pub held: Vec<HeldUpdate>,
    pub sneak: Vec<SneakUpdate>,
    pub sprint: Vec<SprintUpdate>,
    pub pose: Vec<PoseUpdate>,
    pub blocking: Vec<BlockingUpdate>,
    pub swing: Vec<SwingUpdate>,
    pub skin: Vec<SkinLayersUpdate>,
    pub eat_start: Vec<EatStartUpdate>,
    pub eat_abort: Vec<EatAbortUpdate>,
    pub break_anim: Vec<BreakAnimUpdate>,
    pub break_block: Vec<BreakBlockUpdate>,
    pub place_block: Vec<PlaceBlockUpdate>,
    pub inv_ops: Vec<InventoryOp>,
    pub hit_player: Vec<HitPlayerUpdate>,
    pub hit_entity: Vec<HitEntityUpdate>,
    pub fire: Vec<FireProjectileUpdate>,
}

impl Bank {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps allocations, drops rows; what the fuse returns to `empty`.
    pub fn clear(&mut self) {
        self.pos.clear();
        self.equipment.clear();
        self.held.clear();
        self.sneak.clear();
        self.sprint.clear();
        self.pose.clear();
        self.blocking.clear();
        self.swing.clear();
        self.skin.clear();
        self.eat_start.clear();
        self.eat_abort.clear();
        self.break_anim.clear();
        self.break_block.clear();
        self.place_block.clear();
        self.inv_ops.clear();
        self.hit_player.clear();
        self.hit_entity.clear();
        self.fire.clear();
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pos.is_empty()
            && self.equipment.is_empty()
            && self.held.is_empty()
            && self.sneak.is_empty()
            && self.sprint.is_empty()
            && self.pose.is_empty()
            && self.blocking.is_empty()
            && self.swing.is_empty()
            && self.skin.is_empty()
            && self.eat_start.is_empty()
            && self.eat_abort.is_empty()
            && self.break_anim.is_empty()
            && self.break_block.is_empty()
            && self.place_block.is_empty()
            && self.inv_ops.is_empty()
            && self.hit_player.is_empty()
            && self.hit_entity.is_empty()
            && self.fire.is_empty()
    }
}

/// Lossy-handoff counters; both only grow when the fuse is slower than tick.
#[derive(Debug, Default)]
pub struct HandoffCounters {
    pub delayed_rotations: u64,
    pub failed_handoffs: u64,
}

/// Worker side: the active write [`Bank`] plus the two fixed-1 pipe ends.
///
/// `bank` is the only box the tick thread touches. `full_tx` moves the
/// filled box to the fuse, `empty_rx` reaps the cleared box coming back.
#[derive(Debug)]
pub struct WorkerEnds {
    pub full_tx: mpsc::Sender<Box<Bank>>,
    pub empty_rx: mpsc::Receiver<Box<Bank>>,
    pub bank: Box<Bank>,
    spare: Option<Box<Bank>>,
    pub counters: HandoffCounters,
    slow_warn_at: Option<tokio::time::Instant>,
}

/// Fuse side: drains `full`, clears each bank, parks it back on `empty`.
#[derive(Debug)]
pub struct FuseEnds {
    pub full_rx: mpsc::Receiver<Box<Bank>>,
    pub empty_tx: mpsc::Sender<Box<Bank>>,
}

/// Builds the double bank: one active box plus two fixed-1 channels.
#[must_use]
pub fn bank_pipes() -> (WorkerEnds, FuseEnds) {
    let (full_tx, full_rx) = mpsc::channel(FULL_CAP);
    let (empty_tx, empty_rx) = mpsc::channel(EMPTY_CAP);
    (
        WorkerEnds {
            full_tx,
            empty_rx,
            bank: Box::new(Bank::new()),
            spare: Some(Box::new(Bank::new())),
            counters: HandoffCounters::default(),
            slow_warn_at: None,
        },
        FuseEnds { full_rx, empty_tx },
    )
}

impl WorkerEnds {
    /// Swaps the write bank into `full` without blocking or locking.
    ///
    pub fn end_tick(&mut self) -> bool {
        let mut next = match self.spare.take().or_else(|| self.empty_rx.try_recv().ok()) {
            Some(bank) => bank,
            None => {
                self.counters.delayed_rotations = self.counters.delayed_rotations.saturating_add(1);
                self.warn_slow_fuse();
                return false;
            }
        };
        core::mem::swap(&mut self.bank, &mut next);
        if let Err(error) = self.full_tx.try_send(next) {
            self.counters.failed_handoffs = self.counters.failed_handoffs.saturating_add(1);
            let mut filled = error.into_inner();
            core::mem::swap(&mut self.bank, &mut filled);
            self.spare = Some(filled);
            self.warn_slow_fuse();
            return false;
        }
        true
    }

    /// 1/min warn while every tick hits a still-full `full` channel.
    fn warn_slow_fuse(&mut self) {
        let now = tokio::time::Instant::now();
        let due = match self.slow_warn_at {
            None => true,
            Some(last) => now.duration_since(last) >= SLOW_WARN_COOLDOWN,
        };
        if due {
            self.slow_warn_at = Some(now);
            tracing::warn!(
                delayed_rotations = self.counters.delayed_rotations,
                failed_handoffs = self.counters.failed_handoffs,
                "cluster fuse slower than tick; retaining tick buckets"
            );
        }
    }
}

/// Fuse task: one [`TickBatch`] per tick stamp, then recycle each bank.
#[derive(Debug)]
pub struct Fuse {
    pub last_lag_warn: Option<tokio::time::Instant>,
    pub lag_warnings: u64,
}

impl Fuse {
    #[must_use]
    pub fn new() -> Self {
        Self {
            last_lag_warn: None,
            lag_warnings: 0,
        }
    }

    /// Drains every ready bank for each tick, encodes, forwards, recycles.
    ///
    /// Only the fuse awaits here; workers never do. A queued tick stamp
    /// means encode/forward is slower than tick, which logs at most 1/min.
    pub async fn run(
        &mut self,
        tick_rx: &mut mpsc::Receiver<TickStamp>,
        ends: &mut [FuseEnds],
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) {
        while let Some(tick) = tick_rx.recv().await {
            self.warn_tick_backlog(tick_rx.len());
            let batch = Self::drain(ends, tick);
            match crate::codec::encode_batch(&batch) {
                Ok(bytes) => {
                    if out_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "cluster fuse encode failed");
                }
            }
        }
    }

    /// Moves each ready box into `batch`, clears it, parks it on `empty`.
    ///
    /// Lock-free: only `try_recv`/`try_send`. A missing or full `empty`
    /// slot just skips recycling; the worker allocates next tick.
    fn drain(ends: &mut [FuseEnds], tick: TickStamp) -> TickBatch {
        let mut batch = TickBatch::new(tick);
        for ends_of_worker in ends.iter_mut() {
            while let Ok(mut bank) = ends_of_worker.full_rx.try_recv() {
                batch.append_bank(&bank);
                bank.clear();
                let _ = ends_of_worker.empty_tx.try_send(bank);
            }
        }
        batch
    }

    /// 1/min warn while tick stamps queue faster than drain+encode+forward.
    fn warn_tick_backlog(&mut self, pending_ticks: usize) {
        if pending_ticks == 0 {
            return;
        }
        let now = tokio::time::Instant::now();
        let due = match self.last_lag_warn {
            None => true,
            Some(last) => now.duration_since(last) >= SLOW_WARN_COOLDOWN,
        };
        if due {
            self.last_lag_warn = Some(now);
            self.lag_warnings = self.lag_warnings.saturating_add(1);
            tracing::warn!(pending_ticks, "cluster fuse behind tick; ticks queuing");
        }
    }
}

impl Default for Fuse {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use crate::protocol::EquipmentUpdate;

    fn sample_equipment() -> EquipmentUpdate {
        EquipmentUpdate {
            gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(2)),
            seq: PlayerSeq(3),
            tick: TickStamp(4),
            slot: 1,
            item: 42,
            count: 1,
        }
    }

    #[test]
    fn handoff_recycles_bank() {
        let (mut worker, mut fuse) = bank_pipes();
        worker.bank.equipment.push(sample_equipment());
        assert!(worker.end_tick());

        let mut bank = fuse.full_rx.try_recv().unwrap();
        assert_eq!(bank.equipment.len(), 1);
        bank.clear();
        fuse.empty_tx.try_send(bank).unwrap();

        worker.bank.equipment.push(sample_equipment());
        assert!(worker.end_tick());
        assert_eq!(worker.counters.delayed_rotations, 0);
        assert_eq!(fuse.full_rx.try_recv().unwrap().equipment.len(), 1);
    }

    #[test]
    fn backpressure_keeps_data() {
        let (mut worker, mut fuse) = bank_pipes();
        worker.bank.equipment.push(sample_equipment());
        assert!(worker.end_tick());
        worker.bank.equipment.push(sample_equipment());
        assert!(!worker.end_tick());
        worker.bank.equipment.push(sample_equipment());
        assert!(!worker.end_tick());
        assert_eq!(worker.counters.delayed_rotations, 2);
        let mut returned = fuse.full_rx.try_recv().unwrap();
        returned.clear();
        fuse.empty_tx.try_send(returned).unwrap();
        assert!(worker.end_tick());
        assert_eq!(fuse.full_rx.try_recv().unwrap().equipment.len(), 2);
    }

    #[tokio::test]
    async fn fuse_forwards_batch() {
        let (mut worker, fuse_end) = bank_pipes();
        let mut fuse_ends = [fuse_end];
        let (tick_tx, mut tick_rx) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mut fuse = Fuse::new();

        worker.bank.equipment.push(sample_equipment());
        worker.end_tick();
        tick_tx.send(TickStamp(9)).await.unwrap();
        drop(tick_tx);

        fuse.run(&mut tick_rx, &mut fuse_ends, &out_tx).await;
        let bytes = out_rx.recv().await.unwrap();
        let batch = crate::codec::decode_batch(&bytes).unwrap();
        assert_eq!(batch.tick, TickStamp(9));
        assert_eq!(batch.equipment.len(), 1);
    }
}
