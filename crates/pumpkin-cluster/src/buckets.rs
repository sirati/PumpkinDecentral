//! Per-tick buckets queue not-yet-accepted updates.
//!
//! A global resolve drains one tick only when every chunk in that tick is
//! accepted, then applies the whole agreed set to ground truth at once.
//! Partial ticks never reach ground truth; a tick is either fully queued or
//! fully applied.
//!
//! Concurrency is a single owner behind `tokio::sync::mpsc` channels. Senders
//! push [`BucketInput`] events, one [`BucketActor`] task owns every
//! [`TickBucket`] plus the [`GroundTruth`], and commit notifications leave
//! through a second channel. State never crosses tasks by reference, so no
//! shared guard is needed.
//!
//! Event flow per tick:
//! ```text
//! queue(update) -> require(chunk, holders) -> accept(chunk, peer)* ->
//! resolve(tick) -> apply_agreed(agreed_tick) -> notify(tick)
//! ```

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;

use crate::protocol::ChunkAddr;
use crate::time::TickStamp;

#[derive(Debug, Default)]
pub struct ChunkAcceptance {
    pub holders: HashSet<u16>,
    pub accepted: HashSet<u16>,
}

impl ChunkAcceptance {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_holder(&mut self, peer: u16) {
        self.holders.insert(peer);
    }

    pub fn add_holders(&mut self, peers: &[u16]) {
        self.holders.extend(peers.iter().copied());
    }

    pub fn accept(&mut self, peer: u16) {
        self.accepted.insert(peer);
    }

    pub fn accept_many(&mut self, peers: &[u16]) {
        self.accepted.extend(peers.iter().copied());
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.accepted.is_superset(&self.holders)
    }

    #[must_use]
    pub fn missing(&self) -> Vec<u16> {
        let mut out: Vec<u16> = self.holders.difference(&self.accepted).copied().collect();
        out.sort_unstable();
        out
    }
}

#[derive(Debug, Default)]
pub struct BucketTable {
    pub buckets: HashMap<TickStamp, HashMap<ChunkAddr, ChunkAcceptance>>,
}

impl BucketTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require(&mut self, tick: TickStamp, chunk: ChunkAddr, holders: &[u16]) {
        self.buckets
            .entry(tick)
            .or_default()
            .entry(chunk)
            .or_default()
            .add_holders(holders);
    }

    pub fn accept(
        &mut self,
        tick: TickStamp,
        chunk: ChunkAddr,
        peer: u16,
        new_holders: &[u16],
    ) {
        let acceptance = self
            .buckets
            .entry(tick)
            .or_default()
            .entry(chunk)
            .or_default();
        acceptance.add_holders(new_holders);
        acceptance.accept(peer);
    }

    pub fn merge_holders(&mut self, tick: TickStamp, chunk: ChunkAddr, new_holders: &[u16]) {
        self.buckets
            .entry(tick)
            .or_default()
            .entry(chunk)
            .or_default()
            .add_holders(new_holders);
    }

    pub fn accept_peers(&mut self, tick: TickStamp, chunk: ChunkAddr, peers: &[u16]) {
        self.buckets
            .entry(tick)
            .or_default()
            .entry(chunk)
            .or_default()
            .accept_many(peers);
    }

    #[must_use]
    pub fn is_tick_complete(&self, tick: TickStamp) -> bool {
        match self.buckets.get(&tick) {
            None => true,
            Some(chunks) => chunks
                .values()
                .all(ChunkAcceptance::is_complete),
        }
    }

    #[must_use]
    pub fn missing(&self, tick: TickStamp, chunk: ChunkAddr) -> Vec<u16> {
        match self.buckets.get(&tick).and_then(|chunks| chunks.get(&chunk)) {
            Some(acceptance) => acceptance.missing(),
            None => Vec::new(),
        }
    }

    pub fn remove_tick(&mut self, tick: TickStamp) -> bool {
        self.buckets.remove(&tick).is_some()
    }

    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.buckets.len()
    }

    pub fn drain_all_without_waiting(&mut self) -> usize {
        let dropped = self.buckets.len();
        self.buckets.clear();
        dropped
    }

    pub fn evict_holder(&mut self, peer: u16) -> usize {
        let mut touched = 0;
        for chunks in self.buckets.values_mut() {
            for acceptance in chunks.values_mut() {
                if acceptance.holders.remove(&peer) {
                    touched += 1;
                }
                acceptance.accepted.remove(&peer);
            }
        }
        touched
    }

    pub fn expire_stale(&mut self, max_ticks: usize) -> usize {
        if self.buckets.len() <= max_ticks {
            return 0;
        }
        let mut ticks: Vec<TickStamp> = self.buckets.keys().copied().collect();
        ticks.sort();
        let drop_count = ticks.len() - max_ticks;
        for tick in ticks.into_iter().take(drop_count) {
            self.buckets.remove(&tick);
        }
        drop_count
    }
}

/// One not-yet-accepted update parked inside its tick bucket.
///
/// The tick itself lives on the owning [`TickBucket`]; the update only names
/// its chunk so the agreed set can be grouped without a second lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedUpdate<P> {
    /// Chunk this update belongs to.
    pub chunk: ChunkAddr,
    /// Opaque payload applied to ground truth only after global resolve.
    pub payload: P,
}

impl<P> QueuedUpdate<P> {
    /// Stages a payload for `chunk` without touching ground truth.
    #[must_use]
    pub fn new(chunk: ChunkAddr, payload: P) -> Self {
        Self { chunk, payload }
    }
}

/// Per-tick bucket: queues every update first, tracks acceptance per chunk.
///
/// Nothing stored here is visible to readers. The bucket releases its queue
/// only through [`TickBucket::drain_agreed`], which succeeds solely when
/// every tracked chunk is fully accepted.
#[derive(Debug)]
pub struct TickBucket<P> {
    tick: TickStamp,
    queued: Vec<QueuedUpdate<P>>,
    acceptance: HashMap<ChunkAddr, ChunkAcceptance>,
}

impl<P> TickBucket<P> {
    /// Opens an empty bucket for `tick`.
    #[must_use]
    pub fn new(tick: TickStamp) -> Self {
        Self {
            tick,
            queued: Vec::new(),
            acceptance: HashMap::new(),
        }
    }

    /// Tick this bucket queues for.
    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.tick
    }

    /// Parks `update` until global resolve; never applies it.
    pub fn queue(&mut self, update: QueuedUpdate<P>) {
        self.queued.push(update);
    }

    /// Records which peers must accept `chunk` before the tick can resolve.
    pub fn require(&mut self, chunk: ChunkAddr, holders: &[u16]) {
        self.acceptance
            .entry(chunk)
            .or_default()
            .add_holders(holders);
    }

    /// Records one peer acceptance, widening holders when gossip arrives late.
    pub fn accept(&mut self, chunk: ChunkAddr, peer: u16, new_holders: &[u16]) {
        let acceptance = self.acceptance.entry(chunk).or_default();
        acceptance.add_holders(new_holders);
        acceptance.accept(peer);
    }

    /// Whether every tracked chunk has all holders accepted.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.acceptance.values().all(ChunkAcceptance::is_complete)
    }

    /// Holders still missing for `chunk`; empty when unknown or complete.
    #[must_use]
    pub fn missing(&self, chunk: ChunkAddr) -> Vec<u16> {
        self.acceptance
            .get(&chunk)
            .map_or_else(Vec::new, ChunkAcceptance::missing)
    }

    /// Queued updates targeting `chunk`, in queue order.
    #[must_use]
    pub fn queued_for(&self, chunk: ChunkAddr) -> Vec<&QueuedUpdate<P>> {
        self.queued
            .iter()
            .filter(|update| update.chunk == chunk)
            .collect()
    }

    /// Number of parked updates.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queued.len()
    }

    /// Whether no updates are parked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Number of chunks tracked for acceptance.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.acceptance.len()
    }

    /// Moves the whole queue out at once iff the tick is fully accepted.
    ///
    /// Returns `None` without touching the queue while any chunk is still
    /// waiting, so callers cannot apply a partial tick.
    pub fn drain_agreed(&mut self) -> Option<AgreedTick<P>> {
        if !self.is_complete() {
            return None;
        }
        let updates = std::mem::take(&mut self.queued);
        Some(AgreedTick {
            tick: self.tick,
            updates,
        })
    }
}

/// The agreed set for one tick: the only value [`GroundTruth`] accepts.
///
/// Carries every queued update for the tick, so application is all-or-nothing
/// by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgreedTick<P> {
    tick: TickStamp,
    updates: Vec<QueuedUpdate<P>>,
}

impl<P> AgreedTick<P> {
    /// Wraps pre-agreed updates; prefer [`TickBucket::drain_agreed`].
    #[must_use]
    pub fn new(tick: TickStamp, updates: Vec<QueuedUpdate<P>>) -> Self {
        Self { tick, updates }
    }

    /// Tick being resolved.
    #[must_use]
    pub const fn tick(&self) -> TickStamp {
        self.tick
    }

    /// Updates applied together, in queue order.
    #[must_use]
    pub fn updates(&self) -> &[QueuedUpdate<P>] {
        &self.updates
    }

    /// Consumes the agreed set into its updates.
    #[must_use]
    pub fn into_updates(self) -> Vec<QueuedUpdate<P>> {
        self.updates
    }

    /// Number of updates applied together.
    #[must_use]
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    /// Whether the agreed set carries no updates.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// Chunks touched by this tick, sorted and deduplicated.
    #[must_use]
    pub fn chunks(&self) -> Vec<ChunkAddr> {
        let mut chunks: Vec<ChunkAddr> =
            self.updates.iter().map(|update| update.chunk).collect();
        chunks.sort_unstable_by(|left, right| (left.x, left.z).cmp(&(right.x, right.z)));
        chunks.dedup();
        chunks
    }
}

/// Ground truth: authoritative state written only by whole agreed ticks.
///
/// Readers never see a half-applied tick because the sole write path,
/// [`GroundTruth::apply_agreed`], consumes an [`AgreedTick`] and appends all
/// of its updates before returning.
#[derive(Debug)]
pub struct GroundTruth<P> {
    state: HashMap<ChunkAddr, Vec<P>>,
    applied: Vec<TickStamp>,
}

impl<P> Default for GroundTruth<P> {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
            applied: Vec::new(),
        }
    }
}

impl<P> GroundTruth<P> {
    /// Empty ground truth with no applied ticks.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies the entire agreed set at once; returns updates appended.
    ///
    /// The tick is recorded even when empty so resolve gaps stay visible.
    pub fn apply_agreed(&mut self, agreed: AgreedTick<P>) -> usize {
        let count = agreed.updates.len();
        for update in agreed.updates {
            self.state.entry(update.chunk).or_default().push(update.payload);
        }
        self.applied.push(agreed.tick);
        count
    }

    /// Committed payloads for `chunk`, in apply order.
    #[must_use]
    pub fn get(&self, chunk: ChunkAddr) -> &[P] {
        self.state.get(&chunk).map_or(&[], Vec::as_slice)
    }

    /// Ticks applied so far, in apply order.
    #[must_use]
    pub fn applied_ticks(&self) -> &[TickStamp] {
        &self.applied
    }

    /// Whether `tick` was already applied.
    #[must_use]
    pub fn contains_tick(&self, tick: TickStamp) -> bool {
        self.applied.contains(&tick)
    }

    /// Number of payloads committed across all chunks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.values().map(Vec::len).sum()
    }

    /// Whether nothing has been committed yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.values().all(Vec::is_empty)
    }
}

/// Channel event drives the bucket owner; no shared state crosses tasks.
#[derive(Debug)]
pub enum BucketInput<P> {
    /// Park an update in its tick bucket.
    Queue {
        /// Tick bucket receiving the update.
        tick: TickStamp,
        /// Not-yet-accepted update.
        update: QueuedUpdate<P>,
    },
    /// Declare which peers must accept `chunk` for `tick`.
    Require {
        /// Tick bucket to track.
        tick: TickStamp,
        /// Chunk awaiting acceptance.
        chunk: ChunkAddr,
        /// Peers that must accept.
        holders: Vec<u16>,
    },
    /// Record one peer acceptance, widening holders on late gossip.
    Accept {
        /// Tick bucket being accepted.
        tick: TickStamp,
        /// Chunk the peer accepted.
        chunk: ChunkAddr,
        /// Peer that accepted.
        peer: u16,
        /// Newly discovered holders.
        new_holders: Vec<u16>,
    },
    /// Attempt global resolve: apply the agreed set for `tick` at once.
    Resolve {
        /// Tick to resolve when complete.
        tick: TickStamp,
    },
}

/// Single owner of per-tick buckets plus ground truth.
///
/// Receives [`BucketInput`] over `mpsc`, applies complete ticks to its own
/// [`GroundTruth`], and announces each commit over a second `mpsc` channel.
/// Drive it with [`BucketActor::run`] on one task; hand out only the sender
/// halves.
#[derive(Debug)]
pub struct BucketActor<P> {
    pending: HashMap<TickStamp, TickBucket<P>>,
    ground: GroundTruth<P>,
    inputs: mpsc::Receiver<BucketInput<P>>,
    committed: mpsc::Sender<TickStamp>,
}

impl<P> BucketActor<P> {
    /// Takes channel halves; this task becomes the sole state owner.
    pub fn new(
        inputs: mpsc::Receiver<BucketInput<P>>,
        committed: mpsc::Sender<TickStamp>,
    ) -> Self {
        Self {
            pending: HashMap::new(),
            ground: GroundTruth::new(),
            inputs,
            committed,
        }
    }

    /// Takes channel halves with a pre-seeded ground truth.
    pub fn with_ground(
        ground: GroundTruth<P>,
        inputs: mpsc::Receiver<BucketInput<P>>,
        committed: mpsc::Sender<TickStamp>,
    ) -> Self {
        Self {
            pending: HashMap::new(),
            ground,
            inputs,
            committed,
        }
    }

    /// Ground truth owned by this task; visible only to the owner.
    #[must_use]
    pub const fn ground(&self) -> &GroundTruth<P> {
        &self.ground
    }

    /// How many tick buckets are still waiting.
    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.pending.len()
    }

    /// Handles one event; returns the tick when a resolve commits it.
    pub fn step(&mut self, msg: BucketInput<P>) -> Option<TickStamp> {
        match msg {
            BucketInput::Queue { tick, update } => {
                self.pending
                    .entry(tick)
                    .or_insert_with(|| TickBucket::new(tick))
                    .queue(update);
                None
            }
            BucketInput::Require {
                tick,
                chunk,
                holders,
            } => {
                self.pending
                    .entry(tick)
                    .or_insert_with(|| TickBucket::new(tick))
                    .require(chunk, &holders);
                None
            }
            BucketInput::Accept {
                tick,
                chunk,
                peer,
                new_holders,
            } => {
                self.pending
                    .entry(tick)
                    .or_insert_with(|| TickBucket::new(tick))
                    .accept(chunk, peer, &new_holders);
                None
            }
            BucketInput::Resolve { tick } => {
                let agreed = self
                    .pending
                    .get_mut(&tick)
                    .and_then(TickBucket::drain_agreed)?;
                let resolved = agreed.tick();
                self.ground.apply_agreed(agreed);
                self.pending.remove(&tick);
                let _ = self.committed.try_send(resolved);
                Some(resolved)
            }
        }
    }

    /// Runs until senders drop, then returns the owned ground truth.
    #[must_use]
    pub async fn run(mut self) -> GroundTruth<P> {
        while let Some(msg) = self.inputs.recv().await {
            if let Some(resolved) = self.step(msg) {
                let _ = self.committed.send(resolved).await;
            }
        }
        self.ground
    }
}

/// Builds the input/commit channel pair backing a [`BucketActor`].
///
/// Returns `(input_tx, input_rx, committed_tx, committed_rx)` so the owner
/// task keeps the receivers while every producer holds only senders.
#[must_use]
pub fn bucket_channels<P>(
    input_depth: usize,
    commit_depth: usize,
) -> (
    mpsc::Sender<BucketInput<P>>,
    mpsc::Receiver<BucketInput<P>>,
    mpsc::Sender<TickStamp>,
    mpsc::Receiver<TickStamp>,
) {
    let (input_tx, input_rx) = mpsc::channel(input_depth.max(1));
    let (committed_tx, committed_rx) = mpsc::channel(commit_depth.max(1));
    (input_tx, input_rx, committed_tx, committed_rx)
}

/// Spawns the single-owner task and returns its handle.
///
/// The task exits when all input senders drop, yielding the final
/// [`GroundTruth`]. Producers and observers communicate only through the
/// `mpsc` halves they already hold.
pub fn spawn_bucket_actor<P>(
    inputs: mpsc::Receiver<BucketInput<P>>,
    committed: mpsc::Sender<TickStamp>,
) -> tokio::task::JoinHandle<GroundTruth<P>>
where
    P: Send + 'static,
{
    tokio::spawn(BucketActor::new(inputs, committed).run())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    #[test]
    fn completes_when_all_holders_accept() {
        let mut table = BucketTable::new();
        let tick = TickStamp(3);
        table.require(tick, chunk(0, 0), &[1, 2]);
        assert!(!table.is_tick_complete(tick));
        table.accept(tick, chunk(0, 0), 1, &[]);
        assert!(!table.is_tick_complete(tick));
        table.accept(tick, chunk(0, 0), 2, &[]);
        assert!(table.is_tick_complete(tick));
        assert!(table.remove_tick(tick));
    }

    #[test]
    fn late_holders_reopen_acceptance() {
        let mut table = BucketTable::new();
        let tick = TickStamp(4);
        table.require(tick, chunk(0, 0), &[1]);
        table.accept(tick, chunk(0, 0), 1, &[]);
        assert!(table.is_tick_complete(tick));
        table.accept(tick, chunk(0, 0), 1, &[2]);
        assert!(!table.is_tick_complete(tick));
        table.accept(tick, chunk(0, 0), 2, &[]);
        assert!(table.is_tick_complete(tick));
    }

    #[test]
    fn untracked_tick_is_complete() {
        let table = BucketTable::new();
        assert!(table.is_tick_complete(TickStamp(99)));
    }

    #[test]
    fn bucket_holds_queue_until_every_chunk_accepts() {
        let tick = TickStamp(7);
        let mut bucket: TickBucket<u16> = TickBucket::new(tick);
        bucket.queue(QueuedUpdate::new(chunk(0, 0), 11));
        bucket.queue(QueuedUpdate::new(chunk(1, 0), 22));
        bucket.require(chunk(0, 0), &[1]);
        bucket.require(chunk(1, 0), &[2]);
        assert!(bucket.drain_agreed().is_none());
        bucket.accept(chunk(0, 0), 1, &[]);
        assert!(bucket.drain_agreed().is_none());
        bucket.accept(chunk(1, 0), 2, &[]);
        let agreed = bucket.drain_agreed().expect("complete tick drains");
        assert_eq!(agreed.tick(), tick);
        assert_eq!(agreed.len(), 2);
        assert!(bucket.is_empty());
    }

    #[test]
    fn ground_truth_applies_agreed_set_at_once() {
        let tick = TickStamp(8);
        let mut ground: GroundTruth<u16> = GroundTruth::new();
        let agreed = AgreedTick::new(
            tick,
            vec![
                QueuedUpdate::new(chunk(0, 0), 1),
                QueuedUpdate::new(chunk(0, 0), 2),
                QueuedUpdate::new(chunk(5, 5), 3),
            ],
        );
        assert_eq!(ground.apply_agreed(agreed), 3);
        assert_eq!(ground.get(chunk(0, 0)), &[1, 2]);
        assert_eq!(ground.get(chunk(5, 5)), &[3]);
        assert_eq!(ground.applied_ticks(), &[tick]);
    }

    #[test]
    fn actor_resolve_commits_only_when_complete() {
        let (_input_tx, input_rx) = mpsc::channel(8);
        let (committed_tx, _committed_rx) = mpsc::channel(8);
        let mut actor: BucketActor<u16> = BucketActor::new(input_rx, committed_tx);
        let tick = TickStamp(9);
        let addr = chunk(0, 0);
        actor.step(BucketInput::Queue {
            tick,
            update: QueuedUpdate::new(addr, 42),
        });
        actor.step(BucketInput::Require {
            tick,
            chunk: addr,
            holders: vec![3],
        });
        assert!(actor.step(BucketInput::Resolve { tick }).is_none());
        assert!(actor.ground().is_empty());
        actor.step(BucketInput::Accept {
            tick,
            chunk: addr,
            peer: 3,
            new_holders: Vec::new(),
        });
        assert_eq!(actor.step(BucketInput::Resolve { tick }), Some(tick));
        assert_eq!(actor.ground().get(addr), &[42]);
        assert_eq!(actor.pending_ticks(), 0);
    }

    #[tokio::test]
    async fn actor_run_applies_tick_and_returns_ground() {
        let (input_tx, input_rx) = mpsc::channel(8);
        let (committed_tx, mut committed_rx) = mpsc::channel(8);
        let tick = TickStamp(11);
        let addr = chunk(2, 3);
        let handle = spawn_bucket_actor::<u16>(input_rx, committed_tx);
        input_tx
            .send(BucketInput::Queue {
                tick,
                update: QueuedUpdate::new(addr, 7),
            })
            .await
            .expect("send queue");
        input_tx
            .send(BucketInput::Require {
                tick,
                chunk: addr,
                holders: vec![1],
            })
            .await
            .expect("send require");
        input_tx
            .send(BucketInput::Accept {
                tick,
                chunk: addr,
                peer: 1,
                new_holders: Vec::new(),
            })
            .await
            .expect("send accept");
        input_tx
            .send(BucketInput::Resolve { tick })
            .await
            .expect("send resolve");
        drop(input_tx);
        let ground = handle.await.expect("actor joins");
        assert_eq!(ground.get(addr), &[7]);
        assert!(ground.contains_tick(tick));
        assert_eq!(committed_rx.recv().await, Some(tick));
    }
}
