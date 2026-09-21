//! Accept votes and globally-accepted tick application.
//!
//! Each peer publishes one [`AcceptBatch`] vote per tick describing which
//! [`GlobalPlayerId`]s it accepted, which [`ConflictKey`] winners it observed,
//! and which holder peers it newly discovered. [`Acceptor`] folds those votes
//! into a [`BucketTable`] and reports when every required holder has accepted
//! a tick via [`Acceptor::is_globally_accepted`]. Callers apply buffered tick
//! payloads only through [`apply_if_globally_accepted`], so unaccepted ticks
//! never reach the world.
//!
//! Deterministic ordering is enforced at the vote boundary: [`build_accept`]
//! sorts decisions by chunk and normalizes every decision payload
//! (`accepted_gids`, `winners`, `new_holders`), so identical votes encode to
//! identical bytes on every peer and [`Acceptor`] buffering stays comparable
//! with `==`.
//!
//! Concurrency is `mpsc`-only. Votes travel over a
//! [`tokio::sync::mpsc`] channel created by [`accept_channel`] and are folded
//! with [`drain_accept_votes`]. [`Acceptor`] itself holds plain maps and sets
//! behind `&mut` borrows; it contains no `Mutex`, `RwLock`, or shared
//! interior mutability. One task owns the `Acceptor`, drains its receiver,
//! and gates application.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::buckets::BucketTable;
use crate::identity::GlobalPlayerId;
use crate::protocol::{BlockPos, ChunkAddr, EntityRef};
use crate::time::TickStamp;

/// Stable identity of one contested outcome inside a tick.
///
/// Tags keep block, entity, and player conflicts in disjoint key spaces so a
/// single winner map can resolve every conflict deterministically.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ConflictKey {
    pub tag: u8,
    pub payload: [u8; 12],
}

impl ConflictKey {
    pub const BLOCK_TAG: u8 = 0;
    pub const ENTITY_TAG: u8 = 1;
    pub const PLAYER_TAG: u8 = 2;

    /// Encodes a block position into the block key space.
    #[must_use]
    pub const fn for_block(pos: BlockPos) -> Self {
        let x = pos.x.to_le_bytes();
        let y = pos.y.to_le_bytes();
        let z = pos.z.to_le_bytes();
        Self {
            tag: Self::BLOCK_TAG,
            payload: [
                x[0], x[1], x[2], x[3], y[0], y[1], y[2], y[3], z[0], z[1],
                z[2], z[3],
            ],
        }
    }

    /// Encodes an entity reference into the entity key space.
    #[must_use]
    pub const fn for_entity(target: EntityRef) -> Self {
        let owner = target.owner.0.to_le_bytes();
        let id = target.local_id.to_le_bytes();
        Self {
            tag: Self::ENTITY_TAG,
            payload: [
                owner[0], owner[1], id[0], id[1], id[2], id[3], 0, 0, 0, 0, 0,
                0,
            ],
        }
    }

    /// Encodes a player identity into the player key space.
    #[must_use]
    pub const fn for_player(target: GlobalPlayerId) -> Self {
        let server = target.server.0.to_le_bytes();
        let player = target.player.0.to_le_bytes();
        Self {
            tag: Self::PLAYER_TAG,
            payload: [
                server[0], server[1], player[0], player[1], 0, 0, 0, 0, 0, 0,
                0, 0,
            ],
        }
    }
}

/// One peer's vote for a single chunk inside a tick.
///
/// `accepted_gids` lists the player identities this voter observed,
/// `winners` records the conflict resolutions it observed, and `new_holders`
/// advertises holder peers the voter discovered after the tick started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDecision {
    pub chunk: ChunkAddr,
    pub accepted_gids: Vec<GlobalPlayerId>,
    pub winners: Vec<(ConflictKey, GlobalPlayerId)>,
    pub new_holders: Vec<u16>,
}

impl ChunkDecision {
    /// Sorts and dedupes every payload so equal votes compare and encode equal.
    pub fn normalize(&mut self) {
        self.accepted_gids.sort();
        self.accepted_gids.dedup();
        self.winners.sort();
        self.winners.dedup();
        self.new_holders.sort_unstable();
        self.new_holders.dedup();
    }
}

/// One peer's accept vote for a whole tick: the unit sent over the channel.
///
/// Decisions are kept sorted by chunk; see [`build_accept`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptBatch {
    pub tick: TickStamp,
    pub decisions: Vec<ChunkDecision>,
}

impl AcceptBatch {
    /// Creates an empty vote for `tick`; decisions are appended by builders.
    #[must_use]
    pub fn new(tick: TickStamp) -> Self {
        Self {
            tick,
            decisions: Vec::new(),
        }
    }

    /// Reports whether this vote carries no chunk decisions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.decisions.is_empty()
    }

    /// Counts the chunk decisions carried by this vote.
    #[must_use]
    pub fn len(&self) -> usize {
        self.decisions.len()
    }

    /// Normalizes every decision payload and sorts decisions by chunk.
    pub fn normalize(&mut self) {
        for decision in &mut self.decisions {
            decision.normalize();
        }
        self.decisions.sort_by(|left, right| left.chunk.cmp(&right.chunk));
    }
}

/// Local simulation outcome for one chunk, prior to vote encoding.
///
/// Kept separate from [`ChunkDecision`] so call sites name the pre-network
/// value explicitly; [`build_accept`] converts and normalizes outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChunkOutcome {
    pub chunk: ChunkAddr,
    pub accepted_gids: Vec<GlobalPlayerId>,
    pub winners: Vec<(ConflictKey, GlobalPlayerId)>,
    pub new_holders: Vec<u16>,
}

/// Builds a canonical vote: normalized payloads sorted by chunk.
///
/// Every decision is sorted and deduped first, then decisions are ordered by
/// [`ChunkAddr`], so all peers produce byte-identical votes for identical
/// outcomes and duplicate deliveries stay `==`-comparable downstream.
#[must_use]
pub fn build_accept(tick: TickStamp, local_outcomes: Vec<LocalChunkOutcome>) -> AcceptBatch {
    let mut batch = AcceptBatch {
        tick,
        decisions: local_outcomes
            .into_iter()
            .map(|outcome| ChunkDecision {
                chunk: outcome.chunk,
                accepted_gids: outcome.accepted_gids,
                winners: outcome.winners,
                new_holders: outcome.new_holders,
            })
            .collect(),
    };
    batch.normalize();
    batch
}

/// Codec failure for [`encode_accept`] and [`decode_accept`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptCodecError {
    pub message: String,
}

impl core::fmt::Display for AcceptCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AcceptCodecError {}

/// Encodes a vote to the wire form carried inside accept parcels.
pub fn encode_accept(batch: &AcceptBatch) -> Result<Vec<u8>, AcceptCodecError> {
    postcard::to_allocvec(batch).map_err(|error| AcceptCodecError {
        message: format!("encode accept: {error}"),
    })
}

/// Decodes a vote previously produced by [`encode_accept`].
pub fn decode_accept(bytes: &[u8]) -> Result<AcceptBatch, AcceptCodecError> {
    postcard::from_bytes(bytes).map_err(|error| AcceptCodecError {
        message: format!("decode accept: {error}"),
    })
}

/// Folds accept votes and tracks global acceptance per tick.
///
/// The table records required holders per chunk, `seen` dedupes repeat
/// `(tick, chunk, peer)` acceptances so replays are idempotent, and
/// `buffered` retains every distinct vote for replay and inspection.
/// Holders merge before acceptances each batch so a late-discovered holder
/// reopens an already-complete tick until it votes.
#[derive(Debug, Default)]
pub struct Acceptor {
    table: BucketTable,
    seen: HashSet<(TickStamp, ChunkAddr, u16)>,
    buffered: HashMap<TickStamp, Vec<AcceptBatch>>,
}

impl Acceptor {
    /// Creates an empty acceptor with no required holders.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Borrows the underlying holder/acceptance table for inspection.
    #[must_use]
    pub fn table(&self) -> &BucketTable {
        &self.table
    }

    /// Declares which holder peers must accept `chunk` at `tick`.
    pub fn require(&mut self, tick: TickStamp, chunk: ChunkAddr, holders: &[u16]) {
        self.table.require(tick, chunk, holders);
    }

    /// Folds one vote: merges new holders, then records fresh acceptances.
    ///
    /// Fresh peers are applied in ascending order so table updates are
    /// deterministic regardless of vote payload order. The vote is buffered
    /// once per distinct value; byte-identical replays change nothing.
    pub fn apply_accept(&mut self, batch: AcceptBatch) {
        for decision in &batch.decisions {
            self.table
                .merge_holders(batch.tick, decision.chunk, &decision.new_holders);
        }
        for decision in &batch.decisions {
            let mut fresh: Vec<u16> = Vec::new();
            for gid in &decision.accepted_gids {
                let peer = gid.server.0;
                if self.seen.insert((batch.tick, decision.chunk, peer))
                    && !fresh.contains(&peer)
                {
                    fresh.push(peer);
                }
            }
            if !fresh.is_empty() {
                fresh.sort_unstable();
                self.table
                    .accept_peers(batch.tick, decision.chunk, &fresh);
            }
        }
        let slot = self.buffered.entry(batch.tick).or_default();
        if !slot.contains(&batch) {
            slot.push(batch);
        }
    }

    /// Decodes wire bytes and folds the resulting vote.
    pub fn apply_encoded(&mut self, bytes: &[u8]) -> Result<(), AcceptCodecError> {
        decode_accept(bytes).map(|batch| self.apply_accept(batch))
    }

    /// Reports whether every required holder accepted `tick`.
    ///
    /// Ticks with no requirements count as accepted, so idle ticks apply
    /// immediately through [`apply_if_globally_accepted`].
    #[must_use]
    pub fn is_globally_accepted(&self, tick: TickStamp) -> bool {
        self.table.is_tick_complete(tick)
    }

    /// Lists required holders of `chunk` that have not accepted `tick`, sorted.
    #[must_use]
    pub fn missing_holders(&self, tick: TickStamp, chunk: ChunkAddr) -> Vec<u16> {
        self.table.missing(tick, chunk)
    }

    /// Counts ticks with outstanding holder requirements.
    #[must_use]
    pub fn pending_ticks(&self) -> usize {
        self.table.pending_ticks()
    }

    /// Counts ticks with at least one buffered vote.
    #[must_use]
    pub fn buffered_ticks(&self) -> usize {
        self.buffered.len()
    }

    /// Counts distinct buffered votes for `tick`.
    #[must_use]
    pub fn buffered_batches(&self, tick: TickStamp) -> usize {
        self.buffered.get(&tick).map_or(0, Vec::len)
    }

    /// Reports whether any vote is buffered for `tick`.
    #[must_use]
    pub fn has_buffered(&self, tick: TickStamp) -> bool {
        self.buffered.contains_key(&tick)
    }

    /// Drops table state, dedupe markers, and buffered votes for `tick`.
    pub fn remove_tick(&mut self, tick: TickStamp) -> bool {
        let from_table = self.table.remove_tick(tick);
        self.seen.retain(|key| key.0 != tick);
        let from_buffer = self.buffered.remove(&tick).is_some();
        from_table || from_buffer
    }

    pub fn drain_all_without_waiting(&mut self) -> usize {
        let dropped = self.table.drain_all_without_waiting();
        self.seen.clear();
        let buffered = self.buffered.len();
        self.buffered.clear();
        dropped.max(buffered)
    }

    pub fn evict_holder(&mut self, peer: u16) -> usize {
        self.seen.retain(|key| key.2 != peer);
        for batches in self.buffered.values_mut() {
            batches.retain(|batch| {
                !batch.decisions.iter().all(|decision| {
                    decision.accepted_gids.iter().all(|gid| gid.server.0 == peer)
                })
            });
        }
        self.table.evict_holder(peer)
    }

    pub fn expire_stale(&mut self, max_ticks: usize) -> usize {
        let dropped = self.table.expire_stale(max_ticks);
        if self.buffered.len() > max_ticks {
            let mut ticks: Vec<TickStamp> = self.buffered.keys().copied().collect();
            ticks.sort();
            let drop_count = ticks.len() - max_ticks;
            for tick in ticks.into_iter().take(drop_count) {
                self.buffered.remove(&tick);
                self.seen.retain(|key| key.0 != tick);
            }
            return dropped.max(drop_count);
        }
        dropped
    }
}

/// Creates the `mpsc`-only vote transport for [`AcceptBatch`] votes.
///
/// The sender side is cloned per voter task; the single owning task holds the
/// receiver alongside its [`Acceptor`] and pumps it with
/// [`drain_accept_votes`]. No locks guard the exchange: ownership of the
/// channel ends plus `&mut` borrows provide the synchronization.
#[must_use]
pub fn accept_channel(capacity: usize) -> (mpsc::Sender<AcceptBatch>, mpsc::Receiver<AcceptBatch>) {
    mpsc::channel(capacity.max(1))
}

/// Folds every queued vote into `acceptor` without blocking.
///
/// Uses [`mpsc::Receiver::try_recv`] in a loop, so the owner never awaits
/// while holding `&mut` access. Returns the number of votes folded.
pub fn drain_accept_votes(
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

/// Applies buffered tick payloads only once their tick is globally accepted.
///
/// Items keep caller order; when [`Acceptor::is_globally_accepted`] is false
/// nothing runs and `0` returns. Returns the number of applied items.
/// Pair with [`drain_accept_votes`]: drain votes first, then gate the tick
/// through this function.
pub fn apply_if_globally_accepted<Item>(
    acceptor: &Acceptor,
    tick: TickStamp,
    items: &[Item],
    mut apply: impl FnMut(&Item),
) -> usize {
    if !acceptor.is_globally_accepted(tick) {
        return 0;
    }
    for item in items {
        apply(item);
    }
    items.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    fn outcome(
        addr: ChunkAddr,
        gids: &[GlobalPlayerId],
        new_holders: &[u16],
    ) -> LocalChunkOutcome {
        LocalChunkOutcome {
            chunk: addr,
            accepted_gids: gids.to_vec(),
            winners: Vec::new(),
            new_holders: new_holders.to_vec(),
        }
    }

    #[test]
    fn late_holder_reopens() {
        let mut acceptor = Acceptor::new();
        let tick = TickStamp(9);
        let addr = chunk(0, 0);
        acceptor.require(tick, addr, &[1]);
        assert!(!acceptor.is_globally_accepted(tick));
        acceptor.apply_accept(build_accept(tick, vec![outcome(addr, &[gid(1, 0)], &[])]));
        assert!(acceptor.is_globally_accepted(tick));
        acceptor.apply_accept(build_accept(tick, vec![outcome(addr, &[], &[2])]));
        assert!(!acceptor.is_globally_accepted(tick));
        assert_eq!(acceptor.missing_holders(tick, addr), vec![2]);
        acceptor.apply_accept(build_accept(tick, vec![outcome(addr, &[gid(2, 0)], &[])]));
        assert!(acceptor.is_globally_accepted(tick));
    }

    #[test]
    fn duplicate_accept_idempotent() {
        let mut acceptor = Acceptor::new();
        let tick = TickStamp(3);
        let addr = chunk(1, 2);
        acceptor.require(tick, addr, &[1, 2]);
        let batch = build_accept(
            tick,
            vec![outcome(addr, &[gid(1, 0), gid(2, 5)], &[])],
        );
        acceptor.apply_accept(batch.clone());
        assert!(acceptor.is_globally_accepted(tick));
        assert_eq!(acceptor.buffered_batches(tick), 1);
        acceptor.apply_accept(batch);
        assert!(acceptor.is_globally_accepted(tick));
        assert_eq!(acceptor.buffered_batches(tick), 1);
        assert!(acceptor.missing_holders(tick, addr).is_empty());
    }

    #[test]
    fn future_tick_buffering() {
        let mut acceptor = Acceptor::new();
        let past = TickStamp(11);
        let future = TickStamp(12);
        let addr = chunk(0, 0);
        acceptor.require(past, addr, &[1]);
        acceptor.require(future, addr, &[1]);
        acceptor.apply_accept(build_accept(future, vec![outcome(addr, &[gid(1, 0)], &[])]));
        assert!(acceptor.has_buffered(future));
        assert!(acceptor.is_globally_accepted(future));
        assert!(!acceptor.is_globally_accepted(past));
        acceptor.apply_accept(build_accept(past, vec![outcome(addr, &[gid(1, 0)], &[])]));
        assert!(acceptor.is_globally_accepted(past));
        assert!(acceptor.is_globally_accepted(future));
        assert_eq!(acceptor.buffered_ticks(), 2);
    }

    #[test]
    fn codec_roundtrip() {
        let tick = TickStamp(42);
        let addr = chunk(-3, 7);
        let block = BlockPos { x: 1, y: 64, z: -2 };
        let batch = AcceptBatch {
            tick,
            decisions: vec![ChunkDecision {
                chunk: addr,
                accepted_gids: vec![gid(1, 2)],
                winners: vec![
                    (ConflictKey::for_block(block), gid(1, 2)),
                    (
                        ConflictKey::for_entity(EntityRef {
                            owner: ServerId(1),
                            local_id: 77,
                            chunk: addr,
                        }),
                        gid(2, 0),
                    ),
                    (ConflictKey::for_player(gid(4, 4)), gid(4, 4)),
                ],
                new_holders: vec![3],
            }],
        };
        let bytes = encode_accept(&batch).unwrap();
        assert_eq!(decode_accept(&bytes).unwrap(), batch);
        assert!(decode_accept(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn build_sorts_decisions_by_chunk() {
        let tick = TickStamp(5);
        let batch = build_accept(
            tick,
            vec![outcome(chunk(9, 0), &[], &[]), outcome(chunk(1, 0), &[], &[])],
        );
        assert_eq!(batch.decisions[0].chunk, chunk(1, 0));
        assert_eq!(batch.decisions[1].chunk, chunk(9, 0));
    }

    #[test]
    fn build_normalizes_payload_order() {
        let tick = TickStamp(6);
        let addr = chunk(0, 1);
        let block = BlockPos { x: 0, y: 0, z: 0 };
        let batch = build_accept(
            tick,
            vec![LocalChunkOutcome {
                chunk: addr,
                accepted_gids: vec![gid(2, 0), gid(1, 0), gid(2, 0)],
                winners: vec![
                    (ConflictKey::for_block(block), gid(2, 0)),
                    (ConflictKey::for_block(block), gid(1, 0)),
                ],
                new_holders: vec![3, 1, 3],
            }],
        );
        assert_eq!(
            batch.decisions[0].accepted_gids,
            vec![gid(1, 0), gid(2, 0)]
        );
        assert_eq!(
            batch.decisions[0].winners,
            vec![
                (ConflictKey::for_block(block), gid(1, 0)),
                (ConflictKey::for_block(block), gid(2, 0)),
            ]
        );
        assert_eq!(batch.decisions[0].new_holders, vec![1, 3]);
    }

    #[test]
    fn gated_apply_runs_only_when_globally_accepted() {
        let mut acceptor = Acceptor::new();
        let tick = TickStamp(7);
        let addr = chunk(4, 4);
        acceptor.require(tick, addr, &[1]);
        let items = vec![10_u32, 20_u32];
        let mut applied: Vec<u32> = Vec::new();
        let ran =
            apply_if_globally_accepted(&acceptor, tick, &items, |item| applied.push(*item));
        assert_eq!(ran, 0);
        assert!(applied.is_empty());
        acceptor.apply_accept(build_accept(tick, vec![outcome(addr, &[gid(1, 0)], &[])]));
        let ran =
            apply_if_globally_accepted(&acceptor, tick, &items, |item| applied.push(*item));
        assert_eq!(ran, 2);
        assert_eq!(applied, items);
    }

    #[tokio::test]
    async fn votes_drain_over_mpsc_without_locks() {
        let tick = TickStamp(8);
        let addr = chunk(2, 2);
        let (sender, mut receiver) = accept_channel(8);
        sender
            .send(build_accept(tick, vec![outcome(addr, &[gid(5, 0)], &[])]))
            .await
            .unwrap();
        drop(sender);
        let mut acceptor = Acceptor::new();
        acceptor.require(tick, addr, &[5]);
        assert_eq!(drain_accept_votes(&mut receiver, &mut acceptor), 1);
        assert!(acceptor.is_globally_accepted(tick));
        assert_eq!(drain_accept_votes(&mut receiver, &mut acceptor), 0);
    }
}
