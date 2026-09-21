//! Deterministic block-conflict resolution.
//!
//! Contract, in one place:
//! - Every contender in a [`Resolver`] belongs to exactly one [`ConflictKey`].
//! - Within a key, at most one contender per player survives (`dedupe_by_player`).
//! - Validity is checked first: stale breaks and occupied-cell places become
//!   losers before any ranking runs (`is_rejected`).
//! - The winner is the minimum eligible contender under a tick-hash total
//!   ordering of players, reduced pairwise (`resolve_pair` / `reduce_winner`).
//! - That ordering is `hash(cluster_seed, tick, server, player)` with a
//!   `(server, player)` id tiebreak, so equal hashes still order totally.
//! - Loser and resolution lists are re-sorted into that same order, so output
//!   never depends on ingest/arrival order or on `HashMap` iteration order.
//!
//! Replicas ingest the same contender set in different arrival orders and must
//! still elect the same winner with the same loser list. Keep every step below
//! a pure function of `(cluster_seed, tick, contender contents)`; never branch
//! on arrival index, pointer address, or hash-map iteration order except through
//! the documented total orders.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::order::order_players;
use crate::protocol::{BlockPos, BlockUndo, BreakBlockUpdate, EntityRef, PlaceBlockUpdate};
use crate::time::TickStamp;

/// Identity of one conflict cell.
///
/// Total order is structural and content-only: blocks sort before players,
/// players before entities, and each variant compares its coordinates/ids.
/// Sorting resolutions by this key keeps multi-key output deterministic
/// regardless of grouping-map iteration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConflictKey {
    Block(BlockPos),
    Player(GlobalPlayerId),
    Entity(EntityRef),
}

impl Ord for ConflictKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Block(left), Self::Block(right)) => {
                (left.x, left.y, left.z).cmp(&(right.x, right.y, right.z))
            }
            (Self::Block(_), _) => Ordering::Less,
            (Self::Player(_), Self::Block(_)) => Ordering::Greater,
            (Self::Player(left), Self::Player(right)) => left.cmp(right),
            (Self::Player(_), _) => Ordering::Less,
            (Self::Entity(_), Self::Block(_) | Self::Player(_)) => Ordering::Greater,
            (Self::Entity(left), Self::Entity(right)) => (left.owner, left.local_id, left.chunk)
                .cmp(&(right.owner, right.local_id, right.chunk)),
        }
    }
}

impl PartialOrd for ConflictKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<BlockPos> for ConflictKey {
    fn from(pos: BlockPos) -> Self {
        Self::Block(pos)
    }
}

impl From<GlobalPlayerId> for ConflictKey {
    fn from(target: GlobalPlayerId) -> Self {
        Self::Player(target)
    }
}

impl From<EntityRef> for ConflictKey {
    fn from(target: EntityRef) -> Self {
        Self::Entity(target)
    }
}

/// Undo payload carried by a losing contender.
pub trait Undo {
    #[must_use]
    fn undo_payload(&self) -> BlockUndo;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakUndo(pub BlockUndo);

impl Undo for BreakUndo {
    fn undo_payload(&self) -> BlockUndo {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceUndo(pub BlockUndo);

impl Undo for PlaceUndo {
    fn undo_payload(&self) -> BlockUndo {
        self.0
    }
}

/// What a contender wants to do to the key cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContenderKind {
    Break(BreakBlockUpdate),
    Place(PlaceBlockUpdate),
}

/// One player's claim on one [`ConflictKey`], plus the undo to run if it loses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Contender {
    pub gid: GlobalPlayerId,
    pub kind: ContenderKind,
    pub undo: BlockUndo,
}

impl Contender {
    #[must_use]
    pub fn break_claim(update: BreakBlockUpdate, undo: BlockUndo) -> Self {
        Self {
            gid: update.gid,
            kind: ContenderKind::Break(update),
            undo,
        }
    }

    #[must_use]
    pub fn place_claim(update: PlaceBlockUpdate, undo: BlockUndo) -> Self {
        Self {
            gid: update.gid,
            kind: ContenderKind::Place(update),
            undo,
        }
    }

    /// Conflict cell this claim contends for.
    #[must_use]
    pub fn key(&self) -> ConflictKey {
        match &self.kind {
            ContenderKind::Break(update) => ConflictKey::Block(update.pos),
            ContenderKind::Place(update) => ConflictKey::Block(update.pos),
        }
    }

    /// Per-player sequence number used only for same-player dedupe.
    #[must_use]
    pub fn seq(&self) -> PlayerSeq {
        match &self.kind {
            ContenderKind::Break(update) => update.seq,
            ContenderKind::Place(update) => update.seq,
        }
    }
}

impl Undo for Contender {
    fn undo_payload(&self) -> BlockUndo {
        self.undo
    }
}

/// Pairwise step of the total order.
///
/// Returns the smaller player under `order_players(cluster_seed, tick, ..)`,
/// which is `hash(seed, tick, server, player)` with a `(server, player)`
/// tiebreak. `Ordering::Equal` can only happen for the same player, so the
/// left input wins ties and the step stays associative and commutative:
/// folding any grouping or arrival order elects the same global minimum.
#[must_use]
pub fn resolve_pair(
    cluster_seed: u64,
    tick: TickStamp,
    left: GlobalPlayerId,
    right: GlobalPlayerId,
) -> GlobalPlayerId {
    match order_players(cluster_seed, tick, left, right) {
        Ordering::Less | Ordering::Equal => left,
        Ordering::Greater => right,
    }
}

/// Validity gate for breaks: a break whose expectation mismatches ground state
/// is stale and loses before ranking, no matter its tick-hash rank.
#[must_use]
pub fn is_break_stale(update: &BreakBlockUpdate, ground_state: u16) -> bool {
    update.expected_old_state != ground_state
}

/// Validity gate for places: a place targeting a movement-occupied cell defers
/// and loses before ranking, no matter its tick-hash rank.
#[must_use]
pub fn is_cell_occupied(pos: BlockPos, occupied_cells: &[BlockPos]) -> bool {
    occupied_cells.contains(&pos)
}

/// Decided outcome for one [`ConflictKey`]: optional winner plus every loser
/// (validity rejections first in rank order, then ranked-out eligibles).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub key: ConflictKey,
    pub winner: Option<Contender>,
    pub losers: Vec<Contender>,
}

impl Resolution {
    /// Undo payloads for every loser, in [`Resolution::losers`] order.
    #[must_use]
    pub fn loser_undos(&self) -> Vec<BlockUndo> {
        self.losers.iter().map(Contender::undo_payload).collect()
    }
}

/// Accumulates contenders for one tick, then resolves each key independently.
///
/// Determinism pipeline in [`Resolver::resolve_with_context`]:
/// 1. group by content key (insertion order inside each group is ingest order),
/// 2. collapse duplicates per player with [`pick_newer`],
/// 3. split validity rejections from eligible contenders,
/// 4. fold eligibles pairwise to their tick-hash minimum,
/// 5. re-sort losers and resolutions by total order.
/// Steps 4-5 erase any trace of arrival order.
#[derive(Debug, Default)]
pub struct Resolver {
    cluster_seed: u64,
    tick: TickStamp,
    contenders: Vec<Contender>,
}

impl Resolver {
    /// Seeds the tick-hash ordering every later decision uses.
    #[must_use]
    pub fn new(cluster_seed: u64, tick: TickStamp) -> Self {
        Self {
            cluster_seed,
            tick,
            contenders: Vec::new(),
        }
    }

    pub fn ingest_break(&mut self, update: BreakBlockUpdate, undo: BlockUndo) {
        self.ingest(Contender::break_claim(update, undo));
    }

    pub fn ingest_place(&mut self, update: PlaceBlockUpdate, undo: BlockUndo) {
        self.ingest(Contender::place_claim(update, undo));
    }

    /// Pushes one claim; ordering happens at resolve time, not here, so ingest
    /// order never leaks into the decision except as documented dedupe input.
    pub fn ingest(&mut self, contender: Contender) {
        self.contenders.push(contender);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.contenders.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.contenders.is_empty()
    }

    /// Resolves with no ground truth and no occupied cells.
    #[must_use]
    pub fn resolve(&self) -> Vec<Resolution> {
        self.resolve_with_context(&HashMap::new(), &[])
    }

    /// Resolves every grouped key: dedupe per player, reject invalid claims,
    /// elect the pairwise tick-hash minimum of the survivors, and return
    /// resolutions sorted by [`ConflictKey`]. Losers are sorted by the same
    /// player total order (with a content tiebreak), so two replicas that saw
    /// the same contenders in different orders return identical vectors.
    #[must_use]
    pub fn resolve_with_context(
        &self,
        ground_states: &HashMap<ConflictKey, u16>,
        occupied_cells: &[BlockPos],
    ) -> Vec<Resolution> {
        let mut grouped: HashMap<ConflictKey, Vec<Contender>> = HashMap::new();
        for contender in &self.contenders {
            grouped.entry(contender.key()).or_default().push(*contender);
        }
        let mut out = Vec::with_capacity(grouped.len());
        for (key, group) in grouped {
            let unique = dedupe_by_player(&group);
            let mut rejected = Vec::new();
            let mut eligible: Vec<&Contender> = Vec::new();
            for contender in &unique {
                if is_rejected(contender, ground_states, occupied_cells) {
                    rejected.push(*contender);
                } else {
                    eligible.push(contender);
                }
            }
            let winner = reduce_winner(self.cluster_seed, self.tick, &eligible).copied();
            let mut losers = rejected;
            for contender in eligible {
                if Some(*contender) != winner {
                    losers.push(*contender);
                }
            }
            losers.sort_by(|left, right| {
                order_players(self.cluster_seed, self.tick, left.gid, right.gid)
                    .then_with(|| left.gid.cmp(&right.gid))
            });
            out.push(Resolution {
                key,
                winner,
                losers,
            });
        }
        out.sort_by(|left, right| left.key.cmp(&right.key));
        out
    }
}

/// Content value compared when two claims from the same player tie on sequence:
/// break expectation or place intent. Never an arrival index.
fn tiebreak_value(contender: &Contender) -> u16 {
    match &contender.kind {
        ContenderKind::Break(update) => update.expected_old_state,
        ContenderKind::Place(update) => update.new_state,
    }
}

/// Stable kind rank for same-player dedupe fallback: breaks sort before places.
/// Keeps the fallback a pure function of contender contents.
fn contender_kind_rank(contender: &Contender) -> u8 {
    match &contender.kind {
        ContenderKind::Break(_) => 0,
        ContenderKind::Place(_) => 1,
    }
}

/// Fallback total order for same-player, same-key duplicates whose sequences
/// do not decide (equal, or the ambiguous half-wrap distance). Compares only
/// contender contents so merging is commutative: `pick(a, b) == pick(b, a)`
/// up to full equality, where either choice is observationally identical.
fn dedupe_fallback_cmp(current: &Contender, incoming: &Contender) -> Ordering {
    contender_kind_rank(current)
        .cmp(&contender_kind_rank(incoming))
        .then_with(|| tiebreak_value(current).cmp(&tiebreak_value(incoming)))
        .then_with(|| current.seq().0.cmp(&incoming.seq().0))
        .then_with(|| current.undo.old_state.cmp(&incoming.undo.old_state))
        .then_with(|| {
            current
                .undo
                .count_before
                .cmp(&incoming.undo.count_before)
        })
}

/// Collapses two same-player, same-key claims to one, independent of call order.
///
/// Newer `PlayerSeq` wins when exactly one side is newer; otherwise the larger
/// [`dedupe_fallback_cmp`] wins, keeping the first on a full tie. A strict
/// comparison (never an arrival-favoring `>=`) is what keeps per-player dedupe
/// deterministic across replicas.
fn pick_newer(current: &Contender, incoming: &Contender) -> Contender {
    let incoming_newer = incoming.seq().is_newer_than(current.seq());
    let current_newer = current.seq().is_newer_than(incoming.seq());
    if incoming_newer && !current_newer {
        *incoming
    } else if current_newer && !incoming_newer {
        *current
    } else {
        match dedupe_fallback_cmp(current, incoming) {
            Ordering::Less => *incoming,
            Ordering::Greater => *current,
            Ordering::Equal => *current,
        }
    }
}

/// Keeps one claim per player within a key group by folding duplicates with
/// [`pick_newer`]. Because that fold is commutative, the surviving set (though
/// accumulated in ingest order) is identical for every arrival permutation.
fn dedupe_by_player(group: &[Contender]) -> Vec<Contender> {
    let mut unique: Vec<Contender> = Vec::with_capacity(group.len());
    for contender in group {
        let mut merged = false;
        for slot in unique.iter_mut() {
            if slot.gid == contender.gid {
                *slot = pick_newer(slot, contender);
                merged = true;
                break;
            }
        }
        if !merged {
            unique.push(*contender);
        }
    }
    unique
}

/// Validity split, evaluated before any ranking: stale breaks and
/// occupied-cell places lose regardless of tick-hash rank.
fn is_rejected(
    contender: &Contender,
    ground_states: &HashMap<ConflictKey, u16>,
    occupied_cells: &[BlockPos],
) -> bool {
    match &contender.kind {
        ContenderKind::Break(update) => match ground_states.get(&contender.key()) {
            Some(ground) => is_break_stale(update, *ground),
            None => false,
        },
        ContenderKind::Place(update) => is_cell_occupied(update.pos, occupied_cells),
    }
}

/// Pairwise minimum over eligible contenders under the tick-hash total order.
///
/// Each step keeps `resolve_pair(current, next)`, so the fold computes the
/// global minimum: associative and commutative, hence independent of grouping
/// and arrival order. Returns `None` only when nothing was eligible.
fn reduce_winner<'c>(
    cluster_seed: u64,
    tick: TickStamp,
    eligible: &[&'c Contender],
) -> Option<&'c Contender> {
    let mut current = *eligible.first()?;
    for contender in eligible.iter().skip(1) {
        if resolve_pair(cluster_seed, tick, current.gid, contender.gid) == contender.gid {
            current = *contender;
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::protocol::ChunkAddr;

    const DIRT: u16 = 1;
    const WOOD: u16 = 5;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn break_contender(player: GlobalPlayerId, pos: BlockPos, expected: u16) -> Contender {
        Contender::break_claim(
            BreakBlockUpdate {
                gid: player,
                seq: PlayerSeq(0),
                tick: TickStamp(11),
                pos,
                expected_old_state: expected,
                chunk: ChunkAddr { x: 0, z: 0 },
            },
            BlockUndo {
                old_state: expected,
                count_before: 64,
            },
        )
    }

    fn place_contender(player: GlobalPlayerId, pos: BlockPos, new_state: u16) -> Contender {
        Contender::place_claim(
            PlaceBlockUpdate {
                gid: player,
                seq: PlayerSeq(0),
                tick: TickStamp(11),
                pos,
                new_state,
                inv: crate::inventory::INV_MAIN,
                slot: 3,
                item: 40,
                count_before: 64,
                count_after: 63,
                chunk: ChunkAddr { x: 0, z: 0 },
            },
            BlockUndo {
                old_state: DIRT,
                count_before: 64,
            },
        )
    }

    fn winner_of(resolved: &[Resolution]) -> Option<GlobalPlayerId> {
        resolved
            .first()
            .and_then(|resolution| resolution.winner.as_ref())
            .map(|contender| contender.gid)
    }

    #[test]
    fn pairwise_winner_commutes_over_all_arrival_orders() {
        let seed = 0x5EED_u64;
        let tick = TickStamp(11);
        let pos = BlockPos { x: 3, y: 64, z: -2 };
        let players = [gid(1, 1), gid(2, 3), gid(1, 7)];
        let orders: [[usize; 3]; 6] = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let mut winners = Vec::with_capacity(orders.len());
        for order in orders {
            let mut resolver = Resolver::new(seed, tick);
            for index in order {
                resolver.ingest(break_contender(players[index], pos, DIRT));
            }
            let resolved = resolver.resolve();
            assert_eq!(resolved.len(), 1);
            winners.push(winner_of(&resolved));
        }
        let first = winners[0];
        assert!(winners.iter().all(|winner| *winner == first));
        let expected = resolve_pair(
            seed,
            tick,
            resolve_pair(seed, tick, players[0], players[1]),
            players[2],
        );
        assert_eq!(first, Some(expected));
    }

    #[test]
    fn stale_break_loses_despite_winning_rank() {
        let tick = TickStamp(11);
        let pos = BlockPos { x: 0, y: 64, z: 0 };
        let stale_player = gid(1, 1);
        let fresh_player = gid(2, 2);
        for seed in 0..64_u64 {
            let mut resolver = Resolver::new(seed, tick);
            resolver.ingest(break_contender(stale_player, pos, DIRT));
            resolver.ingest(break_contender(fresh_player, pos, WOOD));
            let mut ground = HashMap::new();
            ground.insert(ConflictKey::Block(pos), WOOD);
            let resolved = resolver.resolve_with_context(&ground, &[]);
            assert_eq!(resolved.len(), 1);
            let outcome = resolved.first().map(|resolution| {
                (
                    resolution.winner.as_ref().map(|c| c.gid),
                    resolution
                        .losers
                        .iter()
                        .map(|c| (c.gid, c.undo.old_state))
                        .collect::<Vec<(GlobalPlayerId, u16)>>(),
                )
            });
            assert_eq!(
                outcome,
                Some((Some(fresh_player), vec![(stale_player, DIRT)]))
            );
        }
        let mut solo = Resolver::new(7, tick);
        solo.ingest(break_contender(stale_player, pos, DIRT));
        let mut ground = HashMap::new();
        ground.insert(ConflictKey::Block(pos), WOOD);
        let resolved = solo.resolve_with_context(&ground, &[]);
        assert_eq!(resolved.len(), 1);
        assert_eq!(winner_of(&resolved), None);
        assert_eq!(resolved.first().map(|r| r.losers.len()), Some(1));
        assert_eq!(
            resolved
                .first()
                .map(|r| r.loser_undos().len()),
            Some(1)
        );
    }

    #[test]
    fn three_way_reduction_ignores_grouping_and_arrival_order() {
        let seed = 911_u64;
        let tick = TickStamp(21);
        let pos = BlockPos { x: -4, y: 70, z: 9 };
        let players = [gid(3, 1), gid(1, 9), gid(2, 5)];
        let left_grouped = resolve_pair(
            seed,
            tick,
            resolve_pair(seed, tick, players[0], players[1]),
            players[2],
        );
        let right_grouped = resolve_pair(
            seed,
            tick,
            players[0],
            resolve_pair(seed, tick, players[1], players[2]),
        );
        assert_eq!(left_grouped, right_grouped);
        let orders: [[usize; 3]; 6] = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let mut outcomes = Vec::with_capacity(orders.len());
        for order in orders {
            let mut resolver = Resolver::new(seed, tick);
            for index in order {
                resolver.ingest(place_contender(players[index], pos, 7));
            }
            let resolved = resolver.resolve();
            assert_eq!(resolved.len(), 1);
            let outcome = resolved.first().map(|resolution| {
                (
                    resolution.winner.as_ref().map(|c| c.gid),
                    resolution
                        .losers
                        .iter()
                        .map(|c| c.gid)
                        .collect::<Vec<GlobalPlayerId>>(),
                )
            });
            outcomes.push(outcome);
        }
        assert!(outcomes.iter().all(|outcome| *outcome == outcomes[0]));
        assert_eq!(
            outcomes[0]
                .as_ref()
                .and_then(|(winner, _)| *winner),
            Some(left_grouped)
        );
    }

    #[test]
    fn placement_on_occupied_cell_defers_to_movement() {
        let seed = 4242_u64;
        let tick = TickStamp(30);
        let pos = BlockPos { x: 8, y: 65, z: 8 };
        let placer = gid(1, 4);
        let breaker = gid(2, 6);
        let occupied = [pos];

        let mut contested = Resolver::new(seed, tick);
        contested.ingest(place_contender(placer, pos, 9));
        contested.ingest(break_contender(breaker, pos, DIRT));
        let resolved = contested.resolve_with_context(&HashMap::new(), &occupied);
        assert_eq!(resolved.len(), 1);
        assert_eq!(winner_of(&resolved), Some(breaker));
        assert!(
            resolved
                .first()
                .is_some_and(|r| r.losers.iter().any(|c| c.gid == placer))
        );

        let mut open = Resolver::new(seed, tick);
        open.ingest(place_contender(placer, pos, 9));
        open.ingest(break_contender(breaker, pos, DIRT));
        let resolved_open = open.resolve_with_context(&HashMap::new(), &[]);
        assert_eq!(
            winner_of(&resolved_open),
            Some(resolve_pair(seed, tick, placer, breaker))
        );

        let mut lone = Resolver::new(seed, tick);
        lone.ingest(place_contender(placer, pos, 9));
        let resolved_lone = lone.resolve_with_context(&HashMap::new(), &occupied);
        assert_eq!(resolved_lone.len(), 1);
        assert_eq!(winner_of(&resolved_lone), None);
        assert_eq!(resolved_lone.first().map(|r| r.losers.len()), Some(1));
    }
}
