use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};

use tokio::sync::{mpsc, oneshot};
use tokio::sync::oneshot::error::TryRecvError;

use crate::accept::{AcceptBatch, Acceptor};
use crate::banks::{Bank, Fuse, FuseEnds, WorkerEnds, bank_pipes};
use crate::protocol::{ChunkAddr, TickBatch};
use crate::time::TickStamp;

const OUTBOX_CAPACITY: usize = 8;

static CLUSTER_ENABLED: AtomicBool = AtomicBool::new(false);
static PUBLISHED_TICK: AtomicU16 = AtomicU16::new(0);
static PENDING_WORKER_SENDER: OnceLock<mpsc::UnboundedSender<FuseEnds>> = OnceLock::new();

thread_local! {
    static CURRENT: RefCell<Option<WorkerEnds>> = RefCell::new(None);
    static UNCLAIMED_FUSE_ENDS: RefCell<Option<FuseEnds>> = RefCell::new(None);
    static HANDED_OFF_TICK: Cell<Option<u16>> = Cell::new(None);
    static OWNER: RefCell<Option<ClusterTick>> = RefCell::new(None);
}

#[must_use]
pub fn is_cluster_enabled() -> bool {
    CLUSTER_ENABLED.load(Ordering::Relaxed)
}

pub fn with_bank(apply: impl FnOnce(&mut Bank)) {
    if !is_cluster_enabled() {
        let mut ephemeral = Bank::new();
        apply(&mut ephemeral);
        return;
    }
    let published = PUBLISHED_TICK.load(Ordering::Relaxed);
    let mut pending_apply = Some(apply);
    let applied = match CURRENT.try_with(|slot| {
        let mut guard = match slot.try_borrow_mut() {
            Ok(guard) => guard,
            Err(_) => return false,
        };
        if guard.is_none() {
            let (worker, fuse_ends) = bank_pipes();
            *guard = Some(worker);
            publish_fuse_ends(fuse_ends);
        }
        let Some(worker) = guard.as_mut() else {
            return false;
        };
        flush_unclaimed_fuse_ends();
        if tick_boundary_crossed(published) && !worker.bank.is_empty() {
            worker.end_tick();
        }
        match pending_apply.take() {
            Some(run) => {
                run(&mut worker.bank);
                true
            }
            None => false,
        }
    }) {
        Ok(applied) => applied,
        Err(_) => false,
    };
    if !applied && let Some(run) = pending_apply {
        let mut ephemeral = Bank::new();
        run(&mut ephemeral);
    }
}

pub fn end_tick_all(tick: TickStamp) {
    ensure_tick_owner();
    PUBLISHED_TICK.store(tick.0, Ordering::Relaxed);
    match OWNER.try_with(|slot| {
        if let Ok(mut guard) = slot.try_borrow_mut() {
            if let Some(owner) = guard.as_mut() {
                owner.advance(tick);
            }
        }
    }) {
        Ok(()) => {}
        Err(_) => {}
    }
}

#[must_use]
pub fn take_fused_outbox() -> Option<mpsc::Receiver<Vec<u8>>> {
    match OWNER.try_with(|slot| match slot.try_borrow_mut() {
        Ok(mut guard) => guard.as_mut().and_then(ClusterTick::take_outbox_receiver),
        Err(_) => None,
    }) {
        Ok(receiver) => receiver,
        Err(_) => None,
    }
}

fn ensure_tick_owner() {
    match OWNER.try_with(|slot| {
        if let Ok(mut guard) = slot.try_borrow_mut() {
            if guard.is_none() {
                let owner = ClusterTick::new();
                owner.activate();
                *guard = Some(owner);
            }
        }
    }) {
        Ok(()) => {}
        Err(_) => {}
    }
}

fn publish_fuse_ends(fuse_ends: FuseEnds) {
    match PENDING_WORKER_SENDER.get() {
        Some(sender) => match sender.send(fuse_ends) {
            Ok(()) => {}
            Err(send_error) => stash_unclaimed_fuse_ends(send_error.0),
        },
        None => stash_unclaimed_fuse_ends(fuse_ends),
    }
}

fn stash_unclaimed_fuse_ends(fuse_ends: FuseEnds) {
    match UNCLAIMED_FUSE_ENDS.try_with(|slot| {
        if let Ok(mut guard) = slot.try_borrow_mut() {
            if guard.is_none() {
                *guard = Some(fuse_ends);
            }
        }
    }) {
        Ok(()) => {}
        Err(_) => {}
    }
}

fn flush_unclaimed_fuse_ends() {
    let unclaimed = match UNCLAIMED_FUSE_ENDS.try_with(|slot| match slot.try_borrow_mut() {
        Ok(mut guard) => guard.take(),
        Err(_) => None,
    }) {
        Ok(ends) => ends,
        Err(_) => None,
    };
    if let Some(fuse_ends) = unclaimed {
        publish_fuse_ends(fuse_ends);
    }
}

fn tick_boundary_crossed(published: u16) -> bool {
    match HANDED_OFF_TICK.try_with(|seen| {
        if seen.get() == Some(published) {
            false
        } else {
            seen.set(Some(published));
            true
        }
    }) {
        Ok(crossed) => crossed,
        Err(_) => false,
    }
}

pub struct TickBuckets {
    queues: HashMap<TickStamp, VecDeque<TickBatch>>,
}

impl TickBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
        }
    }

    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.queues.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queues.values().all(VecDeque::is_empty)
    }

    #[must_use]
    pub fn bucket_len(&self, tick: TickStamp) -> usize {
        self.queues.get(&tick).map_or(0, VecDeque::len)
    }

    pub fn queue_bucket(&mut self, batch: TickBatch) {
        self.queues.entry(batch.tick).or_default().push_back(batch);
    }

    pub fn drop_tick(&mut self, tick: TickStamp) -> bool {
        self.queues.remove(&tick).is_some()
    }

    pub fn drain_tick(&mut self, tick: TickStamp) -> Vec<TickBatch> {
        match self.queues.remove(&tick) {
            None => Vec::new(),
            Some(queue) => queue.into(),
        }
    }

    pub fn apply_accepted_tick(
        &mut self,
        tick: TickStamp,
        mut apply: impl FnMut(&TickBatch),
    ) -> usize {
        match self.queues.remove(&tick) {
            None => 0,
            Some(queue) => {
                for batch in &queue {
                    apply(batch);
                }
                queue.len()
            }
        }
    }
}

impl Default for TickBuckets {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn bucket_channel(capacity: usize) -> (mpsc::Sender<TickBatch>, mpsc::Receiver<TickBatch>) {
    mpsc::channel(capacity.max(1))
}

#[must_use]
pub fn accept_channel(capacity: usize) -> (mpsc::Sender<AcceptBatch>, mpsc::Receiver<AcceptBatch>) {
    mpsc::channel(capacity.max(1))
}

pub fn drain_bucket_channel(
    receiver: &mut mpsc::Receiver<TickBatch>,
    buckets: &mut TickBuckets,
) -> usize {
    let mut drained = 0;
    while let Ok(batch) = receiver.try_recv() {
        buckets.queue_bucket(batch);
        drained += 1;
    }
    drained
}

pub fn drain_accept_channel(
    receiver: &mut mpsc::Receiver<AcceptBatch>,
    acceptor: &mut Acceptor,
) -> usize {
    let mut drained = 0;
    while let Ok(batch) = receiver.try_recv() {
        acceptor.apply_accept(batch);
        drained += 1;
    }
    drained
}

#[must_use]
pub fn apply_accepted_ground_truth(
    buckets: &mut TickBuckets,
    acceptor: &Acceptor,
    tick: TickStamp,
    apply: impl FnMut(&TickBatch),
) -> usize {
    if acceptor.is_globally_accepted(tick) {
        buckets.apply_accepted_tick(tick, apply)
    } else {
        0
    }
}

pub struct ClusterTick {
    fuse: Option<Fuse>,
    worker_ends: Vec<FuseEnds>,
    buckets: TickBuckets,
    acceptor: Acceptor,
    pending_worker_receiver: mpsc::UnboundedReceiver<FuseEnds>,
    pending_worker_sender: mpsc::UnboundedSender<FuseEnds>,
    fused_sender: mpsc::Sender<Vec<u8>>,
    fused_receiver: Option<mpsc::Receiver<Vec<u8>>>,
    pending_stamp: Option<TickStamp>,
    inflight_fuse: Option<oneshot::Receiver<FuseResult>>,
}

struct FuseResult {
    fuse: Fuse,
    worker_ends: Vec<FuseEnds>,
}

impl ClusterTick {
    #[must_use]
    pub fn new() -> Self {
        let (pending_worker_sender, pending_worker_receiver) = mpsc::unbounded_channel();
        let (fused_sender, fused_receiver) = mpsc::channel(OUTBOX_CAPACITY);
        Self {
            fuse: Some(Fuse::new()),
            worker_ends: Vec::new(),
            pending_worker_receiver,
            pending_worker_sender,
            fused_sender,
            fused_receiver: Some(fused_receiver),
            pending_stamp: None,
            inflight_fuse: None,
            buckets: TickBuckets::new(),
            acceptor: Acceptor::new(),
        }
    }

    fn activate(&self) {
        match PENDING_WORKER_SENDER.set(self.pending_worker_sender.clone()) {
            Ok(()) => {}
            Err(_) => {}
        }
        CLUSTER_ENABLED.store(true, Ordering::Relaxed);
    }

    fn take_outbox_receiver(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.fused_receiver.take()
    }

    fn advance(&mut self, tick: TickStamp) {
        while let Ok(fuse_ends) = self.pending_worker_receiver.try_recv() {
            self.worker_ends.push(fuse_ends);
        }
        self.reclaim_fuse();
        self.pending_stamp = Some(tick);
        self.spawn_fuse_when_idle();
    }

    fn reclaim_fuse(&mut self) {
        let reclaimed = match self.inflight_fuse.as_mut() {
            Some(returned) => match returned.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Closed) => {
                    self.fuse = Some(Fuse::new());
                    self.inflight_fuse = None;
                    None
                }
                Err(TryRecvError::Empty) => None,
            },
            None => None,
        };
        if let Some(result) = reclaimed {
            self.fuse = Some(result.fuse);
            self.worker_ends.extend(result.worker_ends);
            self.inflight_fuse = None;
        }
    }

    fn spawn_fuse_when_idle(&mut self) {
        if self.inflight_fuse.is_some() {
            return;
        }
        let Some(stamp) = self.pending_stamp.take() else {
            return;
        };
        if self.fused_sender.is_closed() {
            return;
        }
        let Some(fuse) = self.fuse.take() else {
            self.pending_stamp = Some(stamp);
            return;
        };
        let worker_ends = core::mem::take(&mut self.worker_ends);
        if worker_ends.is_empty() {
            self.fuse = Some(fuse);
            return;
        }
        let (tick_sender, tick_receiver) = mpsc::channel(1);
        let (result_sender, result_receiver) = oneshot::channel();
        match tokio::runtime::Handle::try_current() {
            Err(_) => {
                self.fuse = Some(fuse);
                self.worker_ends = worker_ends;
                self.pending_stamp = Some(stamp);
            }
            Ok(handle) => {
                match tick_sender.try_send(stamp) {
                    Ok(()) => {}
                    Err(send_error) => {
                        self.fuse = Some(fuse);
                        self.worker_ends = worker_ends;
                        self.pending_stamp = Some(send_error.into_inner());
                        return;
                    }
                }
                drop(handle.spawn(run_fuse(
                    fuse,
                    tick_receiver,
                    worker_ends,
                    self.fused_sender.clone(),
                    result_sender,
                )));
                self.inflight_fuse = Some(result_receiver);
            }
        }
    }
}

impl ClusterTick {
    pub fn queue_bucket(&mut self, batch: TickBatch) {
        self.buckets.queue_bucket(batch);
    }

    pub fn drain_bucket_channel(&mut self, receiver: &mut mpsc::Receiver<TickBatch>) -> usize {
        drain_bucket_channel(receiver, &mut self.buckets)
    }

    pub fn drain_accept_channel(&mut self, receiver: &mut mpsc::Receiver<AcceptBatch>) -> usize {
        drain_accept_channel(receiver, &mut self.acceptor)
    }

    pub fn require_holder(&mut self, tick: TickStamp, chunk: ChunkAddr, holders: &[u16]) {
        self.acceptor.require(tick, chunk, holders);
    }

    pub fn apply_accept(&mut self, batch: AcceptBatch) {
        self.acceptor.apply_accept(batch);
    }

    #[must_use]
    pub fn is_accepted(&self, tick: TickStamp) -> bool {
        self.acceptor.is_globally_accepted(tick)
    }

    pub fn apply_accepted_tick(
        &mut self,
        tick: TickStamp,
        apply: impl FnMut(&TickBatch),
    ) -> usize {
        apply_accepted_ground_truth(&mut self.buckets, &self.acceptor, tick, apply)
    }

    #[must_use]
    pub fn pending_bucket_ticks(&self) -> usize {
        self.buckets.pending_ticks()
    }

    #[must_use]
    pub fn bucket_len(&self, tick: TickStamp) -> usize {
        self.buckets.bucket_len(tick)
    }

    pub fn drop_buckets(&mut self, tick: TickStamp) -> bool {
        self.buckets.drop_tick(tick)
    }
}

impl Default for ClusterTick {
    fn default() -> Self {
        Self::new()
    }
}

async fn run_fuse(
    mut fuse: Fuse,
    mut tick_receiver: mpsc::Receiver<TickStamp>,
    mut worker_ends: Vec<FuseEnds>,
    fused_sender: mpsc::Sender<Vec<u8>>,
    result_sender: oneshot::Sender<FuseResult>,
) {
    fuse
        .run(&mut tick_receiver, &mut worker_ends, &fused_sender)
        .await;
    match result_sender.send(FuseResult { fuse, worker_ends }) {
        Ok(()) => {}
        Err(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_batch;
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
    fn closure_runs_on_empty_bank() {
        with_bank(|bank| {
            assert!(bank.is_empty());
        });
    }

    #[test]
    fn fuse_round_trip_single_worker() {
        let mut owner = ClusterTick::new();
        let (mut worker, fuse_ends) = bank_pipes();
        worker.bank.armor.push(sample_armor());
        worker.end_tick();
        owner.worker_ends.push(fuse_ends);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (tick_sender, mut tick_receiver) = mpsc::channel(1);
            tick_sender.send(TickStamp(9)).await.unwrap();
            drop(tick_sender);
            let (fused_sender, mut fused_receiver) = mpsc::channel(1);
            let mut fuse = owner.fuse.take().unwrap();
            fuse.run(&mut tick_receiver, &mut owner.worker_ends, &fused_sender)
                .await;
            let bytes = fused_receiver.recv().await.unwrap();
            let batch = decode_batch(&bytes).unwrap();
            assert_eq!(batch.tick, TickStamp(9));
            assert_eq!(batch.armor.len(), 1);
        });
    }

    #[tokio::test]
    async fn worker_handoff_and_tick_owner_flow() {
        let was_enabled = CLUSTER_ENABLED.load(Ordering::Relaxed);
        let previous_tick = PUBLISHED_TICK.load(Ordering::Relaxed);
        CLUSTER_ENABLED.store(true, Ordering::Relaxed);
        PUBLISHED_TICK.store(41, Ordering::Relaxed);
        with_bank(|bank| {
            bank.armor.push(sample_armor());
        });
        PUBLISHED_TICK.store(42, Ordering::Relaxed);
        with_bank(|bank| {
            assert!(bank.is_empty());
        });
        let unclaimed = match UNCLAIMED_FUSE_ENDS.try_with(|slot| match slot.try_borrow_mut()
        {
            Ok(mut guard) => guard.take(),
            Err(_) => None,
        }) {
            Ok(ends) => ends,
            Err(_) => None,
        };
        let mut fuse_ends = unclaimed.unwrap();
        let handed_off = fuse_ends.full_rx.try_recv().unwrap();
        assert_eq!(handed_off.armor.len(), 1);
        end_tick_all(TickStamp(43));
        assert!(is_cluster_enabled());
        assert!(take_fused_outbox().is_some());
        assert!(take_fused_outbox().is_none());
        CLUSTER_ENABLED.store(was_enabled, Ordering::Relaxed);
        PUBLISHED_TICK.store(previous_tick, Ordering::Relaxed);
    }
}
