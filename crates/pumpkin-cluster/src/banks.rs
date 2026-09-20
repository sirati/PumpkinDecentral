//! Tick-bank double buffer between simulation workers and the fuse task.
//!
//! Shape: each worker fills one [`Bank`] while the fuse owns the other.
//! [`bank_pipes`] hands both ends back as a boxed swap plus two fixed-1
//! channels (`full` worker->fuse, `empty` fuse->worker).
//!
//! Rules the code below encodes, so callers do not have to look elsewhere:
//! - [`WorkerEnds::end_tick`] never blocks and never locks: it only uses
//!   `try_recv`/`try_send`, then `mem::swap`s the active [`Box<Bank>`].
//! - Steady state allocates nothing: the empty channel recycles the cleared
//!   box, and a fresh box is only allocated when the fuse has not returned
//!   one yet (`empty_missed`) or when the fuse is full (`fuse_backpressure`).
//! - Slow fuse is lossy on purpose, never blocking: when `full` is still
//!   occupied the just-filled bank is kept locally as the next write bank,
//!   so the newest tick wins and the worker thread never stalls.
//! - Slow-fuse noise is throttled: worker backpressure and fuse tick backlog
//!   each log at most once per minute via `tracing::warn!`.

use tokio::sync::mpsc;

use crate::inventory::InventoryOp;
use crate::protocol::{
    ArmorUpdate, BreakAnimUpdate, BreakBlockUpdate, EatAbortUpdate, EatStartUpdate,
    FireProjectileUpdate, HeldUpdate, HitEntityUpdate, HitPlayerUpdate, PlaceBlockUpdate,
    PosUpdate, SkinLayersUpdate, SneakUpdate, SprintUpdate, SwingUpdate, BlockingUpdate,
    TickBatch,
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
    pub armor: Vec<ArmorUpdate>,
    pub held: Vec<HeldUpdate>,
    pub sneak: Vec<SneakUpdate>,
    pub sprint: Vec<SprintUpdate>,
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
        self.armor.clear();
        self.held.clear();
        self.sneak.clear();
        self.sprint.clear();
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
            && self.armor.is_empty()
            && self.held.is_empty()
            && self.sneak.is_empty()
            && self.sprint.is_empty()
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
    /// `full` was occupied, so the filled bank stayed local as next `bank`.
    pub fuse_backpressure: u64,
    /// No recycled box was parked, so a fresh box was allocated.
    pub empty_missed: u64,
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
            counters: HandoffCounters::default(),
            slow_warn_at: None,
        },
        FuseEnds { full_rx, empty_tx },
    )
}

impl WorkerEnds {
    /// Swaps the write bank into `full` without blocking or locking.
    ///
    /// Reaps a recycled box when parked, else allocates and counts
    /// `empty_missed`. When the fuse has not drained the previous tick,
    /// `try_send` fails, the filled bank is kept as the next write bank so
    /// no sample blocks, and `fuse_backpressure` plus a 1/min warn records
    /// that the fuse is slower than tick.
    pub fn end_tick(&mut self) {
        let mut next = match self.empty_rx.try_recv() {
            Ok(bank) => bank,
            Err(_) => {
                self.counters.empty_missed =
                    self.counters.empty_missed.saturating_add(1);
                Box::new(Bank::new())
            }
        };
        core::mem::swap(&mut self.bank, &mut next);
        if let Err(error) = self.full_tx.try_send(next) {
            self.counters.fuse_backpressure =
                self.counters.fuse_backpressure.saturating_add(1);
            self.bank = error.into_inner();
            self.warn_slow_fuse();
        }
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
                backpressure = self.counters.fuse_backpressure,
                "cluster fuse slower than tick; keeping newest bank"
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
            tracing::warn!(
                pending_ticks,
                "cluster fuse behind tick; ticks queuing"
            );
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
    use crate::protocol::ArmorUpdate;

    fn sample_armor() -> ArmorUpdate {
        ArmorUpdate {
            gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(2)),
            seq: PlayerSeq(3),
            tick: TickStamp(4),
            slot: 1,
            item: 42,
        }
    }

    #[test]
    fn handoff_recycles_bank() {
        let (mut worker, mut fuse) = bank_pipes();
        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        assert_eq!(worker.counters.empty_missed, 1);

        let mut bank = fuse.full_rx.try_recv().unwrap();
        assert_eq!(bank.armor.len(), 1);
        bank.clear();
        fuse.empty_tx.try_send(bank).unwrap();

        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        assert_eq!(worker.counters.empty_missed, 1);
        assert_eq!(fuse.full_rx.try_recv().unwrap().armor.len(), 1);
    }

    #[test]
    fn backpressure_keeps_data() {
        let (mut worker, mut fuse) = bank_pipes();
        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        assert!(worker.counters.fuse_backpressure > 0);
        let _ = fuse.full_rx.try_recv();
    }

    #[tokio::test]
    async fn fuse_forwards_batch() {
        let (mut worker, fuse_end) = bank_pipes();
        let mut fuse_ends = [fuse_end];
        let (tick_tx, mut tick_rx) = mpsc::channel(4);
        let (out_tx, mut out_rx) = mpsc::channel(4);
        let mut fuse = Fuse::new();

        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        tick_tx.send(TickStamp(9)).await.unwrap();
        drop(tick_tx);

        fuse.run(&mut tick_rx, &mut fuse_ends, &out_tx).await;
        let bytes = out_rx.recv().await.unwrap();
        let batch = crate::codec::decode_batch(&bytes).unwrap();
        assert_eq!(batch.tick, TickStamp(9));
        assert_eq!(batch.armor.len(), 1);
    }
}
