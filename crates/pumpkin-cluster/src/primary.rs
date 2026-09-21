//! Disk-only primary: persists world plus playerdata, serves snapshots, hosts no players.
//!
//! The primary is the cluster's single disk owner. It ingests globally
//! accepted ticks, folds every tick into the staged world image plus the
//! playerdata map, and runs the existing save path once per accepted tick.
//! Secondaries keep local copies only and disable disk writes in cluster mode.
//!
//! ```text
//! secondaries --AcceptedTick---> PrimaryHandle --mpsc--> PrimaryInbox --save--> disk
//! secondaries --snapshot request-> PrimaryHandle --mpsc--> PrimaryInbox --WorldSnapshot--> secondary
//! players -----join-------------> check_player_login() --> Err(PrimaryJoinRejection)
//! ```
//!
//! Single ownership keeps this module lock-free: the primary task alone owns
//! its [`PrimaryState`] and both inboxes, so persistence and snapshot serving
//! share no locks and cross threads over `mpsc` channels only.
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::task::Poll;

use tokio::sync::mpsc;
use tracing::debug;

use crate::time::TickStamp;

/// Capacity of the accepted-tick queue feeding the primary save task.
pub const DEFAULT_PRIMARY_SAVE_QUEUE_CAPACITY: usize = 64;

/// Capacity of the snapshot-request queue served by the primary task.
pub const DEFAULT_PRIMARY_SNAPSHOT_QUEUE_CAPACITY: usize = 16;

/// The primary never hosts players. Secondaries own every client connection.
pub const PRIMARY_HOSTS_PLAYERS: bool = false;

/// Globally accepted tick the primary folds into disk state and snapshots.
#[derive(Debug)]
pub struct AcceptedTick {
    /// Wrapping tick stamp carried by the accepted batch.
    pub tick: TickStamp,
    /// Opaque world plus playerdata delta decoded by the apply closure.
    pub payload: Vec<u8>,
}

/// Offers one accepted tick to the primary without blocking.
/// Returns the tick when the queue is full so callers can shed or retry.
pub fn try_submit(
    tx: &mpsc::Sender<AcceptedTick>,
    tick: TickStamp,
    payload: Vec<u8>,
) -> Result<(), AcceptedTick> {
    let offered = AcceptedTick { tick, payload };
    match tx.try_send(offered) {
        Ok(()) => Ok(()),
        Err(error) => Err(error.into_inner()),
    }
}

/// Sender side of the primary save queue. Carries accepted ticks only, never players.
#[derive(Debug, Clone)]
pub struct PrimarySaveHandle {
    tx: mpsc::Sender<AcceptedTick>,
    shutdown_tx: mpsc::Sender<()>,
}

impl PrimarySaveHandle {
    /// Bundles a live save sender with its shutdown sender.
    #[must_use]
    pub fn new(tx: mpsc::Sender<AcceptedTick>, shutdown_tx: mpsc::Sender<()>) -> Self {
        Self { tx, shutdown_tx }
    }

    /// Offers one accepted tick without blocking.
    pub fn try_submit(&self, tick: TickStamp, payload: Vec<u8>) -> Result<(), AcceptedTick> {
        try_submit(&self.tx, tick, payload)
    }

    /// Signals the save task to stop after draining what it already holds.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.try_send(());
    }
}

/// Receiver side of the primary save queue. Applies ticks in order, saving after each one.
#[derive(Debug)]
pub struct PrimarySaveInbox {
    rx: mpsc::Receiver<AcceptedTick>,
    shutdown: mpsc::Receiver<()>,
}

impl PrimarySaveInbox {
    /// Bundles a live save receiver with its shutdown receiver.
    #[must_use]
    pub fn new(rx: mpsc::Receiver<AcceptedTick>, shutdown: mpsc::Receiver<()>) -> Self {
        Self { rx, shutdown }
    }

    /// Applies queued ticks in send order and saves after every tick.
    /// Stops on shutdown or once every sender is gone and the queue is empty.
    pub async fn run<Apply, Save, SaveFuture>(self, mut apply: Apply, mut save: Save)
    where
        Apply: FnMut(&AcceptedTick),
        Save: FnMut() -> SaveFuture,
        SaveFuture: Future<Output = ()>,
    {
        let Self {
            mut rx,
            mut shutdown,
        } = self;
        loop {
            match rx.try_recv() {
                Ok(accepted) => {
                    apply(&accepted);
                    save().await;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {
                    let next = poll_fn(|cx| {
                        if shutdown.poll_recv(cx).is_ready() {
                            return Poll::Ready(None);
                        }
                        rx.poll_recv(cx)
                    })
                    .await;
                    let Some(accepted) = next else {
                        break;
                    };
                    apply(&accepted);
                    save().await;
                }
            }
        }
        debug!("Primary save task stopped");
    }
}

/// Builds the accepted-tick plus shutdown channel pair. Never carries players.
#[must_use]
pub fn primary_save_channel(capacity: usize) -> (PrimarySaveHandle, PrimarySaveInbox) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let (shutdown_tx, shutdown) = mpsc::channel(1);
    (
        PrimarySaveHandle::new(tx, shutdown_tx),
        PrimarySaveInbox::new(rx, shutdown),
    )
}

/// Reports whether this node accepts player connections. The primary never does.
#[must_use]
pub const fn primary_accepts_players() -> bool {
    PRIMARY_HOSTS_PLAYERS
}

/// Rejection returned for every player login attempt directed at the primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryJoinRejection;

impl PrimaryJoinRejection {
    /// Human-readable reason pointing the player at a secondary.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        "primary hosts no players; join a secondary"
    }
}

impl core::fmt::Display for PrimaryJoinRejection {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.reason())
    }
}

impl std::error::Error for PrimaryJoinRejection {}

/// Admission gate for player connections. Always rejects on the primary.
#[must_use]
pub const fn check_player_login() -> Result<(), PrimaryJoinRejection> {
    Err(PrimaryJoinRejection)
}

/// One player's persisted blob as staged, saved, loaded, and snapshotted by the primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerdataRecord {
    /// Persisted player identity carried alongside the blob.
    pub player_id: [u8; 16],
    /// Opaque playerdata bytes decoded from the accepted tick.
    pub bytes: Vec<u8>,
}

impl PlayerdataRecord {
    /// Stages one player's blob for the next primary save.
    #[must_use]
    pub fn new(player_id: [u8; 16], bytes: Vec<u8>) -> Self {
        Self { player_id, bytes }
    }

    /// Byte length of the staged playerdata blob.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Reports whether the staged playerdata blob is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Point-in-time copy of everything the primary persists, served to secondaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldSnapshot {
    /// Newest accepted tick folded into this snapshot, if any tick arrived yet.
    pub tick: Option<TickStamp>,
    /// Latest staged world image.
    pub world_bytes: Vec<u8>,
    /// Playerdata blobs sorted by player id for deterministic serving.
    pub playerdata: Vec<PlayerdataRecord>,
}

impl WorldSnapshot {
    /// Builds a snapshot with playerdata sorted by player id.
    #[must_use]
    pub fn new(
        tick: Option<TickStamp>,
        world_bytes: Vec<u8>,
        mut playerdata: Vec<PlayerdataRecord>,
    ) -> Self {
        playerdata.sort_by(|left, right| left.player_id.cmp(&right.player_id));
        Self {
            tick,
            world_bytes,
            playerdata,
        }
    }

    /// Reports whether the snapshot carries neither world nor playerdata bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.world_bytes.is_empty() && self.playerdata.is_empty()
    }

    /// Counts the playerdata blobs carried by this snapshot.
    #[must_use]
    pub fn player_count(&self) -> usize {
        self.playerdata.len()
    }

    /// Finds one player's blob inside this snapshot.
    #[must_use]
    pub fn player(&self, player_id: &[u8; 16]) -> Option<&PlayerdataRecord> {
        self.playerdata
            .iter()
            .find(|record| &record.player_id == player_id)
    }
}

/// Disk image the primary loads at boot and writes on every accepted tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryLoad {
    /// Newest accepted tick folded into this disk image, if any tick arrived yet.
    pub tick: Option<TickStamp>,
    /// Persisted world image restored at boot.
    pub world_bytes: Vec<u8>,
    /// Persisted playerdata blobs restored at boot.
    pub playerdata: Vec<PlayerdataRecord>,
}

impl PrimaryLoad {
    /// Empty disk image used for first boot with no save yet.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Reports whether the disk image carries neither world nor playerdata bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.world_bytes.is_empty() && self.playerdata.is_empty()
    }

    /// Counts the playerdata blobs carried by this disk image.
    #[must_use]
    pub fn player_count(&self) -> usize {
        self.playerdata.len()
    }
}

/// Single-owner working state of the primary task: staged world plus playerdata.
/// Lives inside the primary task only, so persistence needs no locks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryState {
    last_tick: Option<TickStamp>,
    world_bytes: Vec<u8>,
    playerdata: HashMap<[u8; 16], Vec<u8>>,
}

impl PrimaryState {
    /// Blank staged state used before the first load or accepted tick.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Restores staged state from the disk image loaded at boot.
    #[must_use]
    pub fn from_load(load: &PrimaryLoad) -> Self {
        let mut state = Self::empty();
        state.last_tick = load.tick;
        state.world_bytes = load.world_bytes.clone();
        for record in &load.playerdata {
            state
                .playerdata
                .insert(record.player_id, record.bytes.clone());
        }
        state
    }

    /// Exports staged state back into a loadable disk image with sorted playerdata.
    #[must_use]
    pub fn into_load(self) -> PrimaryLoad {
        let mut playerdata: Vec<PlayerdataRecord> = self
            .playerdata
            .into_iter()
            .map(|(player_id, bytes)| PlayerdataRecord { player_id, bytes })
            .collect();
        playerdata.sort_by(|left, right| left.player_id.cmp(&right.player_id));
        PrimaryLoad {
            tick: self.last_tick,
            world_bytes: self.world_bytes,
            playerdata,
        }
    }

    /// Folds one accepted tick into staged state: records the tick and stages its payload
    /// as the latest world image. Callers decoding richer payloads stage playerdata
    /// separately with [`PrimaryState::remember_playerdata`] before saving.
    pub fn apply_accepted(&mut self, tick: &AcceptedTick) {
        self.note_tick(tick.tick);
        self.stage_world(tick.payload.clone());
    }

    /// Records the newest accepted tick without touching staged bytes.
    pub fn note_tick(&mut self, tick: TickStamp) {
        self.last_tick = Some(tick);
    }

    /// Replaces the staged world image decoded from the latest accepted tick.
    pub fn stage_world(&mut self, bytes: Vec<u8>) {
        self.world_bytes = bytes;
    }

    /// Stages one player's blob. Reports whether the staged map changed.
    pub fn remember_playerdata(&mut self, record: PlayerdataRecord) -> bool {
        match self.playerdata.get(&record.player_id) {
            Some(current) if current == &record.bytes => false,
            _ => {
                self.playerdata
                    .insert(record.player_id, record.bytes);
                true
            }
        }
    }

    /// Drops one player's staged blob. Reports whether a blob was present.
    pub fn forget_playerdata(&mut self, player_id: &[u8; 16]) -> bool {
        self.playerdata.remove(player_id).is_some()
    }

    /// Copies staged state into a snapshot with playerdata sorted by player id.
    #[must_use]
    pub fn snapshot(&self) -> WorldSnapshot {
        let mut playerdata: Vec<PlayerdataRecord> = self
            .playerdata
            .iter()
            .map(|(player_id, bytes)| PlayerdataRecord {
                player_id: *player_id,
                bytes: bytes.clone(),
            })
            .collect();
        playerdata.sort_by(|left, right| left.player_id.cmp(&right.player_id));
        WorldSnapshot {
            tick: self.last_tick,
            world_bytes: self.world_bytes.clone(),
            playerdata,
        }
    }

    /// Newest accepted tick folded into staged state, if any arrived yet.
    #[must_use]
    pub fn last_tick(&self) -> Option<TickStamp> {
        self.last_tick
    }

    /// Currently staged world image bytes.
    #[must_use]
    pub fn world_bytes(&self) -> &[u8] {
        &self.world_bytes
    }

    /// Currently staged blob for one player, if present.
    #[must_use]
    pub fn player_bytes(&self, player_id: &[u8; 16]) -> Option<&[u8]> {
        self.playerdata.get(player_id).map(Vec::as_slice)
    }

    /// Counts the staged playerdata blobs.
    #[must_use]
    pub fn player_count(&self) -> usize {
        self.playerdata.len()
    }

    /// Reports whether a player's blob is currently staged.
    #[must_use]
    pub fn contains_player(&self, player_id: &[u8; 16]) -> bool {
        self.playerdata.contains_key(player_id)
    }

    /// Reports whether staged state holds neither world nor playerdata bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.world_bytes.is_empty() && self.playerdata.is_empty()
    }
}

/// Snapshot request a secondary hands to the primary with its own reply channel.
/// Reply travels over `mpsc` as well, so serving needs no locks and no oneshots.
#[derive(Debug)]
pub struct PrimarySnapshotRequest {
    reply: mpsc::Sender<WorldSnapshot>,
}

impl PrimarySnapshotRequest {
    /// Wraps the requester's reply sender into a servable snapshot request.
    #[must_use]
    pub fn new(reply: mpsc::Sender<WorldSnapshot>) -> Self {
        Self { reply }
    }

    /// Answers with the latest snapshot. Reports whether the reply was queued.
    pub fn answer(&self, snapshot: WorldSnapshot) -> bool {
        self.reply.try_send(snapshot).is_ok()
    }

    /// Reclaims the reply sender when the request queue is full.
    #[must_use]
    pub fn into_reply(self) -> mpsc::Sender<WorldSnapshot> {
        self.reply
    }

    /// Reports whether the requester already hung up its reply channel.
    #[must_use]
    pub fn reply_closed(&self) -> bool {
        self.reply.is_closed()
    }
}

/// Offers one snapshot request to the primary without blocking.
pub fn try_request_snapshot(
    tx: &mpsc::Sender<PrimarySnapshotRequest>,
    reply: mpsc::Sender<WorldSnapshot>,
) -> Result<(), mpsc::Sender<WorldSnapshot>> {
    let offered = PrimarySnapshotRequest::new(reply);
    match tx.try_send(offered) {
        Ok(()) => Ok(()),
        Err(error) => Err(error.into_inner().into_reply()),
    }
}

/// Sender side of the primary snapshot queue. Fans secondaries out of disk reads.
#[derive(Debug, Clone)]
pub struct PrimarySnapshotHandle {
    tx: mpsc::Sender<PrimarySnapshotRequest>,
}

impl PrimarySnapshotHandle {
    /// Bundles a live snapshot-request sender.
    #[must_use]
    pub fn new(tx: mpsc::Sender<PrimarySnapshotRequest>) -> Self {
        Self { tx }
    }

    /// Offers one snapshot request without blocking. Returns the reply sender when full.
    pub fn try_request(
        &self,
        reply: mpsc::Sender<WorldSnapshot>,
    ) -> Result<(), mpsc::Sender<WorldSnapshot>> {
        try_request_snapshot(&self.tx, reply)
    }
}

/// Receiver side of the primary snapshot queue. Answers from the latest staged state.
#[derive(Debug)]
pub struct PrimarySnapshotInbox {
    rx: mpsc::Receiver<PrimarySnapshotRequest>,
}

impl PrimarySnapshotInbox {
    /// Bundles a live snapshot-request receiver.
    #[must_use]
    pub fn new(rx: mpsc::Receiver<PrimarySnapshotRequest>) -> Self {
        Self { rx }
    }

    /// Counts the snapshot requests waiting for the latest staged state.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    /// Reports whether any snapshot request is waiting.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    /// Answers one waiting request from staged state. Reports whether a reply was queued.
    pub fn serve_one(&mut self, state: &PrimaryState) -> bool {
        let Ok(request) = self.rx.try_recv() else {
            return false;
        };
        request.answer(state.snapshot())
    }

    /// Answers every waiting request from the same staged state.
    /// Closed or full replies are skipped without blocking later requests.
    pub fn serve_all(&mut self, state: &PrimaryState) -> usize {
        let snapshot = state.snapshot();
        let mut answered = 0;
        while let Ok(request) = self.rx.try_recv() {
            if request.answer(snapshot.clone()) {
                answered += 1;
            }
        }
        answered
    }

    /// Waits for the next request, answers it, and reports whether the queue stays open.
    pub async fn serve_next(&mut self, state: &PrimaryState) -> bool {
        let Some(request) = self.rx.recv().await else {
            return false;
        };
        request.answer(state.snapshot())
    }
}

/// Builds the snapshot-request channel pair used to fan secondaries out of disk reads.
#[must_use]
pub fn primary_snapshot_channel(
    capacity: usize,
) -> (PrimarySnapshotHandle, PrimarySnapshotInbox) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (
        PrimarySnapshotHandle::new(tx),
        PrimarySnapshotInbox::new(rx),
    )
}

/// Sender side of the full primary role: accepted ticks in, snapshots out, players never.
#[derive(Debug, Clone)]
pub struct PrimaryHandle {
    save: PrimarySaveHandle,
    snapshots: PrimarySnapshotHandle,
}

impl PrimaryHandle {
    /// Bundles the save sender with the snapshot sender into one primary handle.
    #[must_use]
    pub fn new(save: PrimarySaveHandle, snapshots: PrimarySnapshotHandle) -> Self {
        Self { save, snapshots }
    }

    /// Offers one accepted tick without blocking.
    pub fn try_submit(&self, tick: TickStamp, payload: Vec<u8>) -> Result<(), AcceptedTick> {
        self.save.try_submit(tick, payload)
    }

    /// Offers one snapshot request without blocking. Returns the reply sender when full.
    pub fn try_request_snapshot(
        &self,
        reply: mpsc::Sender<WorldSnapshot>,
    ) -> Result<(), mpsc::Sender<WorldSnapshot>> {
        self.snapshots.try_request(reply)
    }

    /// Signals the primary task to stop after draining what it already holds.
    pub fn shutdown(&self) {
        self.save.shutdown();
    }
}

/// Receiver side of the full primary role. One task owns the state and both queues,
/// so persisting world plus playerdata and serving snapshots needs no locks.
#[derive(Debug)]
pub struct PrimaryInbox {
    saves: PrimarySaveInbox,
    snapshots: PrimarySnapshotInbox,
}

impl PrimaryInbox {
    /// Bundles the save inbox with the snapshot inbox into one primary task inbox.
    #[must_use]
    pub fn new(saves: PrimarySaveInbox, snapshots: PrimarySnapshotInbox) -> Self {
        Self { saves, snapshots }
    }

    /// Persists every accepted tick into world plus playerdata state, saves after each
    /// tick, and answers snapshot requests from the latest staged state.
    /// The apply closure folds the tick into `state`; the save closure flushes both
    /// the staged world image and the playerdata map down the existing save path.
    pub async fn run<Apply, Save, SaveFuture>(
        self,
        mut state: PrimaryState,
        mut apply: Apply,
        mut save: Save,
    )
    where
        Apply: FnMut(&mut PrimaryState, &AcceptedTick),
        Save: FnMut(&PrimaryState) -> SaveFuture,
        SaveFuture: Future<Output = ()>,
    {
        let Self {
            mut saves,
            mut snapshots,
        } = self;
        loop {
            while let Ok(accepted) = saves.rx.try_recv() {
                apply(&mut state, &accepted);
                save(&state).await;
                snapshots.serve_all(&state);
            }
            snapshots.serve_all(&state);
            if saves.rx.is_closed() && snapshots.rx.is_closed() {
                break;
            }
            if saves.shutdown.try_recv().is_ok() {
                break;
            }
            enum Wake {
                Tick(AcceptedTick),
                Snapshot(PrimarySnapshotRequest),
                Stop,
            }
            let wake = poll_fn(|cx| {
                if saves.shutdown.poll_recv(cx).is_ready() {
                    return Poll::Ready(Wake::Stop);
                }
                if let Poll::Ready(ticket) = saves.rx.poll_recv(cx)
                    && let Some(accepted) = ticket
                {
                    return Poll::Ready(Wake::Tick(accepted));
                }
                if let Poll::Ready(requested) = snapshots.rx.poll_recv(cx)
                    && let Some(request) = requested
                {
                    return Poll::Ready(Wake::Snapshot(request));
                }
                if saves.rx.is_closed() && snapshots.rx.is_closed() {
                    return Poll::Ready(Wake::Stop);
                }
                Poll::Pending
            })
            .await;
            match wake {
                Wake::Tick(accepted) => {
                    apply(&mut state, &accepted);
                    save(&state).await;
                    snapshots.serve_all(&state);
                }
                Wake::Snapshot(request) => {
                    request.answer(state.snapshot());
                }
                Wake::Stop => break,
            }
        }
        snapshots.serve_all(&state);
        debug!("Primary task stopped");
    }
}

/// Builds both primary channels at once: save queue plus snapshot queue.
#[must_use]
pub fn primary_channel(
    save_capacity: usize,
    snapshot_capacity: usize,
) -> (PrimaryHandle, PrimaryInbox) {
    let (save_handle, save_inbox) = primary_save_channel(save_capacity);
    let (snapshot_handle, snapshot_inbox) = primary_snapshot_channel(snapshot_capacity);
    (
        PrimaryHandle::new(save_handle, snapshot_handle),
        PrimaryInbox::new(save_inbox, snapshot_inbox),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn full_queue_returns_tick() {
        let (tx, _rx) = mpsc::channel(1);
        try_submit(&tx, TickStamp(1), Vec::from([1_u8])).unwrap();
        let back = try_submit(&tx, TickStamp(2), Vec::from([2_u8]));
        assert!(back.is_err());
    }

    #[test]
    fn zero_capacity_channel_still_accepts_one_tick() {
        let (handle, _inbox) = primary_save_channel(0);
        handle.try_submit(TickStamp(1), Vec::from([1_u8])).unwrap();
    }

    #[tokio::test]
    async fn saver_applies_in_order_before_saving() {
        let (handle, inbox) = primary_save_channel(8);
        handle.try_submit(TickStamp(7), Vec::from([7_u8])).unwrap();
        handle.try_submit(TickStamp(8), Vec::from([8_u8])).unwrap();
        drop(handle);

        let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
        let saves = Arc::new(AtomicUsize::new(0));
        let applied_task = Arc::clone(&applied);
        let saves_task = Arc::clone(&saves);
        inbox
            .run(
                move |accepted| {
                    applied_task.lock().unwrap().push(accepted.tick.0);
                },
                || {
                    saves_task.fetch_add(1, Ordering::Relaxed);
                    async {}
                },
            )
            .await;

        assert_eq!(*applied.lock().unwrap(), [7_u16, 8_u16]);
        assert_eq!(saves.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn saver_stops_on_shutdown_signal() {
        let (handle, inbox) = primary_save_channel(8);
        handle.shutdown();
        let applied = Arc::new(AtomicUsize::new(0));
        let applied_task = Arc::clone(&applied);
        inbox
            .run(
                move |_| {
                    applied_task.fetch_add(1, Ordering::Relaxed);
                },
                || async {},
            )
            .await;

        assert_eq!(applied.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn primary_hosts_no_players() {
        assert!(!PRIMARY_HOSTS_PLAYERS);
        assert!(!primary_accepts_players());
        assert!(check_player_login().is_err());
        assert_eq!(
            PrimaryJoinRejection.reason(),
            "primary hosts no players; join a secondary"
        );
    }

    #[test]
    fn state_stages_world_and_playerdata_then_roundtrips_load() {
        let mut state = PrimaryState::empty();
        assert!(state.is_empty());
        state.apply_accepted(&AcceptedTick {
            tick: TickStamp(9),
            payload: Vec::from([1_u8, 2_u8]),
        });
        assert_eq!(state.last_tick(), Some(TickStamp(9)));
        assert_eq!(state.world_bytes(), &[1_u8, 2_u8]);
        let record = PlayerdataRecord::new([7_u8; 16], Vec::from([3_u8]));
        assert!(state.remember_playerdata(record.clone()));
        assert!(!state.remember_playerdata(record));
        assert_eq!(state.player_count(), 1);
        assert!(state.contains_player(&[7_u8; 16]));
        assert_eq!(state.player_bytes(&[7_u8; 16]), Some([3_u8].as_slice()));
        assert!(!state.forget_playerdata(&[9_u8; 16]));
        assert!(state.forget_playerdata(&[7_u8; 16]));
        state.remember_playerdata(PlayerdataRecord::new([7_u8; 16], Vec::from([3_u8])));
        let snapshot = state.snapshot();
        assert_eq!(snapshot.tick, Some(TickStamp(9)));
        assert_eq!(snapshot.world_bytes, Vec::from([1_u8, 2_u8]));
        assert_eq!(snapshot.player_count(), 1);
        let restored = PrimaryState::from_load(&state.clone().into_load());
        assert_eq!(restored.snapshot(), snapshot);
        assert!(PrimaryLoad::empty().is_empty());
        assert!(!state.clone().into_load().is_empty());
    }

    #[tokio::test]
    async fn snapshot_inbox_answers_from_latest_state() {
        let (handle, mut inbox) = primary_snapshot_channel(4);
        let mut state = PrimaryState::empty();
        state.apply_accepted(&AcceptedTick {
            tick: TickStamp(3),
            payload: Vec::from([9_u8]),
        });
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        handle.try_request(reply_tx).unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(!inbox.is_empty());
        assert!(inbox.serve_one(&state));
        assert!(!inbox.serve_one(&state));
        let snapshot = timeout(Duration::from_secs(5), reply_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.tick, Some(TickStamp(3)));
        assert_eq!(snapshot.world_bytes, Vec::from([9_u8]));
    }

    #[tokio::test]
    async fn primary_run_persists_every_tick_and_serves_snapshots() {
        let (handle, inbox) = primary_channel(8, 8);
        handle
            .try_submit(TickStamp(11), Vec::from([1_u8]))
            .unwrap();
        handle
            .try_submit(TickStamp(12), Vec::from([2_u8]))
            .unwrap();

        let saves = Arc::new(AtomicUsize::new(0));
        let saves_task = Arc::clone(&saves);
        let task = tokio::spawn(async move {
            inbox
                .run(
                    PrimaryState::empty(),
                    |state, tick| {
                        state.apply_accepted(tick);
                    },
                    |_| {
                        saves_task.fetch_add(1, Ordering::Relaxed);
                        async {}
                    },
                )
                .await;
        });

        let mut waited = 0;
        while saves.load(Ordering::Relaxed) < 2 {
            tokio::task::yield_now().await;
            waited += 1;
            assert!(waited < 10_000, "primary did not persist both ticks");
        }
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        handle.try_request_snapshot(reply_tx).unwrap();
        let snapshot = timeout(Duration::from_secs(5), reply_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.tick, Some(TickStamp(12)));
        assert_eq!(snapshot.world_bytes, Vec::from([2_u8]));
        handle.shutdown();
        timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
        assert_eq!(saves.load(Ordering::Relaxed), 2);
    }
}
