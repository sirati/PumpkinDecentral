use std::cell::RefCell;
use std::collections::HashMap;

use crate::banks::Bank;
use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{
    ChunkKey, EntityRef, FireProjectileUpdate, HitEntityUpdate, HitPlayerUpdate, WorldId,
};
use crate::time::TickStamp;

#[must_use]
pub fn capture_hit_player(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    actor_chunk: ChunkKey,
    target: GlobalPlayerId,
    target_chunk: ChunkKey,
    damage_milli: u16,
) -> HitPlayerUpdate {
    HitPlayerUpdate {
        gid,
        seq,
        tick,
        actor_chunk,
        target,
        target_chunk,
        damage_milli,
    }
}

#[must_use]
pub fn capture_hit_entity(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    actor_chunk: ChunkKey,
    target: EntityRef,
    damage_milli: u16,
) -> HitEntityUpdate {
    HitEntityUpdate {
        gid,
        seq,
        tick,
        actor_chunk,
        target,
        damage_milli,
    }
}

#[must_use]
pub fn capture_fire(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    actor_chunk: ChunkKey,
    kind: u8,
    charge_milli: u16,
    dir: [f32; 3],
) -> FireProjectileUpdate {
    FireProjectileUpdate {
        gid,
        seq,
        tick,
        actor_chunk,
        kind,
        charge_milli,
        dir,
    }
}

#[must_use]
pub fn chunk_of_pos(world: WorldId, x: f64, z: f64) -> ChunkKey {
    ChunkKey::new(
        world,
        (x.floor() as i32).div_euclid(16),
        (z.floor() as i32).div_euclid(16),
    )
}

#[derive(Debug, Default)]
pub struct CombatSeqClock {
    next: HashMap<GlobalPlayerId, u16>,
}

impl CombatSeqClock {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn issue(&mut self, gid: GlobalPlayerId) -> PlayerSeq {
        let counter = self.next.entry(gid).or_insert(0);
        let seq = PlayerSeq(*counter);
        *counter = counter.wrapping_add(1);
        seq
    }
}

thread_local! {
    static COMBAT_SEQ: RefCell<CombatSeqClock> = RefCell::new(CombatSeqClock::new());
}

pub fn next_combat_seq(gid: GlobalPlayerId) -> PlayerSeq {
    COMBAT_SEQ.with(|clock| clock.borrow_mut().issue(gid))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HitSnapshot {
    pub health_milli: u32,
    pub effects_generation: u64,
}

impl HitSnapshot {
    #[must_use]
    pub const fn new(health_milli: u32, effects_generation: u64) -> Self {
        Self {
            health_milli,
            effects_generation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CombatLimits {
    pub max_damage_milli: u16,
    pub max_charge_milli: u16,
    pub max_tick_age: u16,
}

impl CombatLimits {
    #[must_use]
    pub const fn default_limits() -> Self {
        Self {
            max_damage_milli: 60_000,
            max_charge_milli: 5_000,
            max_tick_age: 20,
        }
    }
}

impl Default for CombatLimits {
    fn default() -> Self {
        Self::default_limits()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombatRejectReason {
    StaleSequence,
    StaleTick,
    SelfHit,
    DamageOutOfRange,
    ChargeOutOfRange,
    BadDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitDecision {
    Accept { undo: HitSnapshot },
    Reject(CombatRejectReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FireDecision {
    Accept,
    Reject(CombatRejectReason),
}

#[must_use]
pub const fn is_seq_fresh(last: Option<PlayerSeq>, seq: PlayerSeq) -> bool {
    match last {
        None => true,
        Some(previous) => seq.is_newer_than(previous),
    }
}

#[must_use]
pub const fn is_tick_fresh(now: TickStamp, tick: TickStamp, max_age: u16) -> bool {
    now.distance_since(tick) <= max_age
}

#[must_use]
pub fn is_dir_sane(dir: [f32; 3]) -> bool {
    let finite = dir[0].is_finite() && dir[1].is_finite() && dir[2].is_finite();
    finite && dir[0] * dir[0] + dir[1] * dir[1] + dir[2] * dir[2] > 0.0
}

#[must_use]
pub fn validate_hit_player(
    update: &HitPlayerUpdate,
    last_seq: Option<PlayerSeq>,
    now: TickStamp,
    before: HitSnapshot,
    limits: &CombatLimits,
) -> HitDecision {
    if update.gid == update.target {
        return HitDecision::Reject(CombatRejectReason::SelfHit);
    }
    if !is_seq_fresh(last_seq, update.seq) {
        return HitDecision::Reject(CombatRejectReason::StaleSequence);
    }
    if !is_tick_fresh(now, update.tick, limits.max_tick_age) {
        return HitDecision::Reject(CombatRejectReason::StaleTick);
    }
    if update.damage_milli > limits.max_damage_milli {
        return HitDecision::Reject(CombatRejectReason::DamageOutOfRange);
    }
    HitDecision::Accept { undo: before }
}

#[must_use]
pub fn validate_hit_entity(
    update: &HitEntityUpdate,
    last_seq: Option<PlayerSeq>,
    now: TickStamp,
    before: HitSnapshot,
    limits: &CombatLimits,
) -> HitDecision {
    if !is_seq_fresh(last_seq, update.seq) {
        return HitDecision::Reject(CombatRejectReason::StaleSequence);
    }
    if !is_tick_fresh(now, update.tick, limits.max_tick_age) {
        return HitDecision::Reject(CombatRejectReason::StaleTick);
    }
    if update.damage_milli > limits.max_damage_milli {
        return HitDecision::Reject(CombatRejectReason::DamageOutOfRange);
    }
    HitDecision::Accept { undo: before }
}

#[must_use]
pub fn validate_fire(
    update: &FireProjectileUpdate,
    last_seq: Option<PlayerSeq>,
    now: TickStamp,
    limits: &CombatLimits,
) -> FireDecision {
    if !is_seq_fresh(last_seq, update.seq) {
        return FireDecision::Reject(CombatRejectReason::StaleSequence);
    }
    if !is_tick_fresh(now, update.tick, limits.max_tick_age) {
        return FireDecision::Reject(CombatRejectReason::StaleTick);
    }
    if update.charge_milli > limits.max_charge_milli {
        return FireDecision::Reject(CombatRejectReason::ChargeOutOfRange);
    }
    if !is_dir_sane(update.dir) {
        return FireDecision::Reject(CombatRejectReason::BadDirection);
    }
    FireDecision::Accept
}

thread_local! {
    static LOCAL_COMBAT_BANK: RefCell<Bank> = RefCell::new(Bank::new());
}

pub fn with_combat_bank<F, R>(mut closure: F) -> R
where
    F: FnMut(&mut Bank) -> R,
{
    LOCAL_COMBAT_BANK
        .try_with(|bank| match bank.try_borrow_mut() {
            Ok(mut guard) => closure(&mut guard),
            Err(_) => closure(&mut Bank::new()),
        })
        .unwrap_or_else(|_| closure(&mut Bank::new()))
}

pub fn stage_hit_player(update: HitPlayerUpdate) {
    with_combat_bank(|bank| bank.hit_player.push(update));
}

pub fn stage_hit_entity(update: HitEntityUpdate) {
    with_combat_bank(|bank| bank.hit_entity.push(update));
}

pub fn stage_fire(update: FireProjectileUpdate) {
    with_combat_bank(|bank| bank.fire.push(update));
}

#[derive(Debug, Default)]
pub struct DrainedCombat {
    pub hit_player: Vec<HitPlayerUpdate>,
    pub hit_entity: Vec<HitEntityUpdate>,
    pub fire: Vec<FireProjectileUpdate>,
}

impl DrainedCombat {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hit_player.is_empty() && self.hit_entity.is_empty() && self.fire.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.hit_player.len() + self.hit_entity.len() + self.fire.len()
    }
}

#[must_use]
pub fn drain_combat_updates() -> DrainedCombat {
    with_combat_bank(|bank| DrainedCombat {
        hit_player: core::mem::take(&mut bank.hit_player),
        hit_entity: core::mem::take(&mut bank.hit_entity),
        fire: core::mem::take(&mut bank.fire),
    })
}

/// Bow parcel kind carried in [`FireProjectileUpdate::kind`].
///
/// A bow shot scales arrow speed with `charge_milli`; see the combat apply
/// side for the exact power curve. Kept here so producers and the ordering
/// below agree on which parcels count as bow shots.
pub const FIRE_KIND_BOW: u8 = 0;
/// Crossbow parcel kind carried in [`FireProjectileUpdate::kind`].
///
/// A crossbow shot fires at full power regardless of `charge_milli`, which
/// only records how the shot was loaded. Kept here so producers and the
/// ordering below agree on which parcels count as crossbow shots.
pub const FIRE_KIND_CROSSBOW: u8 = 1;

/// Reports whether `kind` names a bow or crossbow parcel.
///
/// The fuse only emits [`FIRE_KIND_BOW`] and [`FIRE_KIND_CROSSBOW`]; unknown
/// kinds are still ordered deterministically by [`order_fire`] but callers
/// can use this helper to spot unexpected producers.
#[must_use]
pub const fn is_fire_kind_supported(kind: u8) -> bool {
    matches!(kind, FIRE_KIND_BOW | FIRE_KIND_CROSSBOW)
}

/// Ranks one attacker for `tick` under `cluster_seed`.
///
/// Thin wrapper over [`crate::order::rank_tick_player`]: every peer hashes
/// the same `(seed, tick, attacker)` triple, so every peer computes the same
/// rank without exchanging data. Higher ranks win; ties are impossible to
/// observe alone because [`order_combat_attackers`] breaks them by id.
#[must_use]
pub fn rank_combat_attacker(cluster_seed: u64, tick: TickStamp, attacker: GlobalPlayerId) -> u64 {
    crate::order::rank_tick_player(cluster_seed, tick, attacker)
}

/// Orders two attackers for the same `tick`.
///
/// Tick hash first via [`crate::order::order_players`], attacker id
/// (`server`, then `player`) as the tiebreak. Antisymmetric and transitive,
/// so sorting any parcel batch with it converges on every peer.
#[must_use]
pub fn order_combat_attackers(
    cluster_seed: u64,
    tick: TickStamp,
    left: GlobalPlayerId,
    right: GlobalPlayerId,
) -> std::cmp::Ordering {
    crate::order::order_players(cluster_seed, tick, left, right)
}

/// Compares two hit-player parcels in conflict order.
///
/// Conflict key is the victim [`HitPlayerUpdate::target`], so contenders on
/// the same victim sort adjacently; within a victim, attacker tick-hash rank
/// decides application order with attacker id as tiebreak, then `seq`,
/// `damage_milli` for a total order. Peers applying the sorted batch in
/// order therefore attribute the same killing blow.
#[must_use]
pub fn order_hit_player(
    cluster_seed: u64,
    tick: TickStamp,
    left: &HitPlayerUpdate,
    right: &HitPlayerUpdate,
) -> std::cmp::Ordering {
    left.target
        .cmp(&right.target)
        .then_with(|| order_combat_attackers(cluster_seed, tick, left.gid, right.gid))
        .then_with(|| left.seq.0.cmp(&right.seq.0))
        .then_with(|| left.damage_milli.cmp(&right.damage_milli))
}

/// Compares two hit-entity parcels in conflict order.
///
/// Conflict key is the victim [`EntityRef`] ordered by `(owner, local_id,
/// chunk)`; within a victim, attacker tick-hash rank decides application
/// order with attacker id as tiebreak, then `seq`, `damage_milli` for a
/// total order.
#[must_use]
pub fn order_hit_entity(
    cluster_seed: u64,
    tick: TickStamp,
    left: &HitEntityUpdate,
    right: &HitEntityUpdate,
) -> std::cmp::Ordering {
    entity_target_cmp(&left.target, &right.target)
        .then_with(|| order_combat_attackers(cluster_seed, tick, left.gid, right.gid))
        .then_with(|| left.seq.0.cmp(&right.seq.0))
        .then_with(|| left.damage_milli.cmp(&right.damage_milli))
}

/// Compares two bow/crossbow fire parcels in conflict order.
///
/// Fire parcels carry no victim, so the attacker tick-hash rank is the
/// leading key with attacker id as tiebreak, then `seq` for same-shooter
/// bursts, then `kind`, `charge_milli`, and direction bits for a total
/// order. Bow ([`FIRE_KIND_BOW`]) and crossbow ([`FIRE_KIND_CROSSBOW`])
/// parcels share the same ordering; `kind` only breaks ties.
#[must_use]
pub fn order_fire(
    cluster_seed: u64,
    tick: TickStamp,
    left: &FireProjectileUpdate,
    right: &FireProjectileUpdate,
) -> std::cmp::Ordering {
    order_combat_attackers(cluster_seed, tick, left.gid, right.gid)
        .then_with(|| left.seq.0.cmp(&right.seq.0))
        .then_with(|| left.kind.cmp(&right.kind))
        .then_with(|| left.charge_milli.cmp(&right.charge_milli))
        .then_with(|| dir_bits(left.dir).cmp(&dir_bits(right.dir)))
}

/// Sorts hit-player parcels into conflict order in place.
///
/// Stable sort by [`order_hit_player`]; equal elements keep arrival order,
/// which is fine because they compare equal on every ranking field.
pub fn sort_hit_player(cluster_seed: u64, tick: TickStamp, hits: &mut [HitPlayerUpdate]) {
    hits.sort_by(|left, right| order_hit_player(cluster_seed, tick, left, right));
}

/// Sorts hit-entity parcels into conflict order in place.
///
/// Stable sort by [`order_hit_entity`]; see [`sort_hit_player`] for why
/// stability is enough for the trailing tie.
pub fn sort_hit_entity(cluster_seed: u64, tick: TickStamp, hits: &mut [HitEntityUpdate]) {
    hits.sort_by(|left, right| order_hit_entity(cluster_seed, tick, left, right));
}

/// Sorts bow/crossbow fire parcels into conflict order in place.
///
/// Stable sort by [`order_fire`]; keeps bursts from one shooter adjacent in
/// `seq` order while interleaving shooters deterministically by tick hash.
pub fn sort_fire(cluster_seed: u64, tick: TickStamp, shots: &mut [FireProjectileUpdate]) {
    shots.sort_by(|left, right| order_fire(cluster_seed, tick, left, right));
}

/// Sorts every lane of a drained combat batch into conflict order.
///
/// Call after [`drain_combat_updates`] (or use [`drain_ordered_combat`]) so
/// the fused batch encodes identically on every peer: hit-player by victim
/// then attacker rank, hit-entity by entity then attacker rank, fire by
/// attacker rank. Each lane is an independent stable sort.
pub fn sort_drained_combat(cluster_seed: u64, tick: TickStamp, drained: &mut DrainedCombat) {
    sort_hit_player(cluster_seed, tick, &mut drained.hit_player);
    sort_hit_entity(cluster_seed, tick, &mut drained.hit_entity);
    sort_fire(cluster_seed, tick, &mut drained.fire);
}

/// Drains the thread-local combat bank already in conflict order.
///
/// Convenience wrapper: [`drain_combat_updates`] plus
/// [`sort_drained_combat`]. The returned lanes are ready to append to the
/// outgoing tick batch without further sorting.
#[must_use]
pub fn drain_ordered_combat(cluster_seed: u64, tick: TickStamp) -> DrainedCombat {
    let mut drained = drain_combat_updates();
    sort_drained_combat(cluster_seed, tick, &mut drained);
    drained
}

fn entity_target_cmp(left: &EntityRef, right: &EntityRef) -> std::cmp::Ordering {
    (left.origin, left.local_id, left.chunk).cmp(&(right.origin, right.local_id, right.chunk))
}

fn dir_bits(dir: [f32; 3]) -> [u32; 3] {
    [dir[0].to_bits(), dir[1].to_bits(), dir[2].to_bits()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    const WORLD: WorldId = WorldId::new(0, 7);
    const CHUNK: ChunkKey = ChunkKey::new(WORLD, 0, 0);

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn hit_player_update(seq: u16, tick: u16, damage_milli: u16) -> HitPlayerUpdate {
        capture_hit_player(
            gid(1, 1),
            PlayerSeq(seq),
            TickStamp(tick),
            CHUNK,
            gid(1, 2),
            CHUNK,
            damage_milli,
        )
    }

    fn snapshot() -> HitSnapshot {
        HitSnapshot::new(19_000, 7)
    }

    #[test]
    fn seq_clock_issues_per_gid_and_wraps() {
        let mut clock = CombatSeqClock::new();
        let id = gid(3, 3);
        assert_eq!(clock.issue(id), PlayerSeq(0));
        assert_eq!(clock.issue(gid(3, 4)), PlayerSeq(0));
        assert_eq!(clock.issue(id), PlayerSeq(1));
        for _ in 0..(u16::MAX - 2) {
            let _ = clock.issue(id);
        }
        assert_eq!(clock.issue(id), PlayerSeq(u16::MAX));
        assert_eq!(clock.issue(id), PlayerSeq(0));
        assert!(clock.issue(id).is_newer_than(PlayerSeq(0)));
    }

    #[test]
    fn striped_seq_increases() {
        let id = gid(9, 9);
        let first = next_combat_seq(id);
        let second = next_combat_seq(id);
        assert!(second.is_newer_than(first));
    }

    #[test]
    fn fresh_hit_accepts_with_snapshot() {
        let limits = CombatLimits::default();
        let decision = validate_hit_player(
            &hit_player_update(2, 40, 1000),
            Some(PlayerSeq(1)),
            TickStamp(41),
            snapshot(),
            &limits,
        );
        assert_eq!(decision, HitDecision::Accept { undo: snapshot() });
    }

    #[test]
    fn first_hit_accepts_any_seq() {
        let limits = CombatLimits::default();
        let decision = validate_hit_player(
            &hit_player_update(9000, 40, 1000),
            None,
            TickStamp(41),
            snapshot(),
            &limits,
        );
        assert_eq!(decision, HitDecision::Accept { undo: snapshot() });
    }

    #[test]
    fn self_hit_rejects() {
        let limits = CombatLimits::default();
        let update = capture_hit_player(
            gid(1, 1),
            PlayerSeq(2),
            TickStamp(40),
            CHUNK,
            gid(1, 1),
            CHUNK,
            1000,
        );
        assert_eq!(
            validate_hit_player(&update, None, TickStamp(41), snapshot(), &limits),
            HitDecision::Reject(CombatRejectReason::SelfHit)
        );
    }

    #[test]
    fn stale_seq_rejects() {
        let limits = CombatLimits::default();
        assert_eq!(
            validate_hit_player(
                &hit_player_update(7, 40, 1000),
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits,
            ),
            HitDecision::Reject(CombatRejectReason::StaleSequence)
        );
        assert_eq!(
            validate_hit_player(
                &hit_player_update(6, 40, 1000),
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits,
            ),
            HitDecision::Reject(CombatRejectReason::StaleSequence)
        );
    }

    #[test]
    fn stale_and_future_ticks_reject() {
        let limits = CombatLimits::default();
        assert_eq!(
            validate_hit_player(
                &hit_player_update(8, 10, 1000),
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits,
            ),
            HitDecision::Reject(CombatRejectReason::StaleTick)
        );
        assert_eq!(
            validate_hit_player(
                &hit_player_update(8, 42, 1000),
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits,
            ),
            HitDecision::Reject(CombatRejectReason::StaleTick)
        );
    }

    #[test]
    fn excessive_damage_rejects() {
        let limits = CombatLimits::default();
        assert_eq!(
            validate_hit_player(
                &hit_player_update(8, 40, limits.max_damage_milli + 1),
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits,
            ),
            HitDecision::Reject(CombatRejectReason::DamageOutOfRange)
        );
    }

    #[test]
    fn entity_hit_validates_like_player_hit() {
        let limits = CombatLimits::default();
        let target = EntityRef {
            origin: ServerId(2),
            local_id: 11,
            generation: 0,
            chunk: CHUNK,
        };
        let update = capture_hit_entity(gid(1, 1), PlayerSeq(8), TickStamp(40), CHUNK, target, 250);
        assert_eq!(
            validate_hit_entity(
                &update,
                Some(PlayerSeq(7)),
                TickStamp(41),
                snapshot(),
                &limits
            ),
            HitDecision::Accept { undo: snapshot() }
        );
        assert_eq!(
            validate_hit_entity(
                &update,
                Some(PlayerSeq(8)),
                TickStamp(41),
                snapshot(),
                &limits
            ),
            HitDecision::Reject(CombatRejectReason::StaleSequence)
        );
    }

    #[test]
    fn fire_accepts_fresh_shot() {
        let limits = CombatLimits::default();
        let update = capture_fire(
            gid(1, 1),
            PlayerSeq(8),
            TickStamp(40),
            CHUNK,
            0,
            900,
            [1.0, 0.0, 0.0],
        );
        assert_eq!(
            validate_fire(&update, Some(PlayerSeq(7)), TickStamp(41), &limits),
            FireDecision::Accept
        );
    }

    #[test]
    fn fire_rejects_bad_shots() {
        let limits = CombatLimits::default();
        let stale = capture_fire(
            gid(1, 1),
            PlayerSeq(7),
            TickStamp(40),
            CHUNK,
            0,
            900,
            [1.0, 0.0, 0.0],
        );
        assert_eq!(
            validate_fire(&stale, Some(PlayerSeq(7)), TickStamp(41), &limits),
            FireDecision::Reject(CombatRejectReason::StaleSequence)
        );
        let overcharged = capture_fire(
            gid(1, 1),
            PlayerSeq(8),
            TickStamp(40),
            CHUNK,
            0,
            limits.max_charge_milli + 1,
            [1.0, 0.0, 0.0],
        );
        assert_eq!(
            validate_fire(&overcharged, Some(PlayerSeq(7)), TickStamp(41), &limits),
            FireDecision::Reject(CombatRejectReason::ChargeOutOfRange)
        );
        for dir in [
            [f32::NAN, 0.0, 0.0],
            [0.0, 0.0, 0.0],
            [f32::INFINITY, 0.0, 1.0],
        ] {
            let wild = capture_fire(gid(1, 1), PlayerSeq(8), TickStamp(40), CHUNK, 0, 900, dir);
            assert_eq!(
                validate_fire(&wild, Some(PlayerSeq(7)), TickStamp(41), &limits),
                FireDecision::Reject(CombatRejectReason::BadDirection)
            );
        }
    }

    #[test]
    fn chunk_of_pos_handles_negatives() {
        assert_eq!(chunk_of_pos(WORLD, 0.5, 15.9), CHUNK);
        assert_eq!(chunk_of_pos(WORLD, 16.0, -0.5), ChunkKey::new(WORLD, 1, -1));
        assert_eq!(
            chunk_of_pos(WORLD, -16.0, -16.0),
            ChunkKey::new(WORLD, -1, -1)
        );
    }

    #[test]
    fn staging_roundtrip() {
        let _ = drain_combat_updates();
        stage_hit_player(hit_player_update(1, 2, 300));
        stage_hit_entity(capture_hit_entity(
            gid(2, 1),
            PlayerSeq(1),
            TickStamp(2),
            CHUNK,
            EntityRef {
                origin: ServerId(1),
                local_id: 9,
                generation: 0,
                chunk: CHUNK,
            },
            300,
        ));
        stage_fire(capture_fire(
            gid(2, 1),
            PlayerSeq(2),
            TickStamp(2),
            CHUNK,
            0,
            100,
            [0.0, 1.0, 0.0],
        ));
        let drained = drain_combat_updates();
        assert_eq!(drained.len(), 3);
        assert!(drain_combat_updates().is_empty());
    }
}
