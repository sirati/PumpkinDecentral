use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU16, Ordering};

use crate::banks::Bank;
use crate::identity::{ActionActor, GlobalPlayerId, PlayerSeq};
use crate::inventory::InventoryStack;
use crate::protocol::{
    CapturedAttack, CapturedAttackTargetOutcome, ChunkAddr, EntityMutationTarget, EntityMutationUpdate, EntityRef,
    FireProjectileUpdate, StatusEffectState,
};
use crate::time::TickStamp;

#[must_use]
pub fn capture_attack(attack: CapturedAttack) -> CapturedAttack {
    attack
}

#[must_use]
pub fn capture_fire(
    gid: GlobalPlayerId,
    seq: PlayerSeq,
    tick: TickStamp,
    chunk: ChunkAddr,
    kind: u8,
    charge_milli: u16,
    dir: [f32; 3],
) -> FireProjectileUpdate {
    FireProjectileUpdate {
        gid,
        seq,
        tick,
        chunk,
        kind,
        charge_milli,
        dir,
    }
}

#[must_use]
pub fn chunk_of_pos(x: f64, z: f64) -> ChunkAddr {
    ChunkAddr {
        x: (x.floor() as i32).div_euclid(16),
        z: (z.floor() as i32).div_euclid(16),
    }
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

const SEQ_STRIPES: usize = 256;

static COMBAT_SEQ: LazyLock<Box<[AtomicU16]>> =
    LazyLock::new(|| (0..SEQ_STRIPES).map(|_| AtomicU16::new(0)).collect());

fn stripe_for(gid: GlobalPlayerId) -> usize {
    let mixed = gid.server.0.rotate_left(5) ^ gid.player.0.wrapping_mul(0x9E37);
    (mixed as usize) % SEQ_STRIPES
}

pub fn next_combat_seq(gid: GlobalPlayerId) -> PlayerSeq {
    let stripe = stripe_for(gid);
    PlayerSeq(COMBAT_SEQ[stripe].fetch_add(1, Ordering::Relaxed))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombatTrackerEntryState {
    pub damage_type: u16,
    pub damage_milli: i32,
    pub fall_location: Option<u8>,
    pub fall_distance_milli: i32,
    pub tick: i64,
    pub source_id: Option<i32>,
    pub attacker_id: Option<i32>,
    pub attacker_name: Option<Vec<u8>>,
    pub attacker_item_name: Option<Vec<u8>>,
    pub attacker_is_living: bool,
    pub attacker_is_player: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombatTrackerState {
    pub entries: Vec<CombatTrackerEntryState>,
    pub last_damage_tick: i64,
    pub combat_start_tick: i64,
    pub combat_end_tick: i64,
    pub in_combat: bool,
    pub taking_damage: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombatStateSnapshot {
    pub health_milli: i32,
    pub absorption_milli: i32,
    pub hurt_cooldown: i32,
    pub last_damage_taken_milli: i32,
    pub dead: bool,
    pub death_time: u8,
    pub last_damage_type: Option<u16>,
    pub last_damage_tick: i64,
    pub last_attacker_id: i32,
    pub last_attacked_tick: i32,
    pub last_attacking_id: i32,
    pub last_attack_tick: i32,
    pub last_hurt_by_player_id: i32,
    pub last_hurt_by_player_tick: i64,
    pub last_hurt_by_mob_id: i32,
    pub last_hurt_by_mob_tick: i64,
    pub velocity_bits: [u64; 3],
    pub fire_ticks: u32,
    pub visual_fire: bool,
    pub active_effects: Vec<StatusEffectState>,
    pub combat_tracker: CombatTrackerState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapturedAttackTargetSnapshot {
    Living {
        target: EntityMutationTarget,
        state: CombatStateSnapshot,
    },
    NonLiving {
        target: EntityMutationTarget,
        outcome: crate::protocol::NonLivingAttackOutcome,
    },
}

impl CapturedAttackTargetSnapshot {
    #[must_use]
    pub const fn target(&self) -> EntityMutationTarget {
        match self {
            Self::Living { target, .. } | Self::NonLiving { target, .. } => *target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombatStatValue {
    pub kind: crate::protocol::CombatStatKind,
    pub item: u16,
    pub value: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CombatAdvancementValue {
    pub advancement: crate::protocol::CombatAdvancement,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedAttackActorSnapshot {
    pub combat: CombatStateSnapshot,
    pub cooldown: u32,
    pub velocity_bits: [u64; 3],
    pub fall_distance_bits: u32,
    pub exhaustion_bits: u32,
    pub held_item: Option<InventoryStack>,
    pub stats: Vec<CombatStatValue>,
    pub advancements: Vec<CombatAdvancementValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedAttackSnapshot {
    pub actor: CapturedAttackActorSnapshot,
    pub targets: Vec<CapturedAttackTargetSnapshot>,
}

impl CapturedAttack {
    #[must_use]
    pub fn attacker_matches_resolved_actor_entity(
        &self,
        actor: ActionActor,
        entity: EntityRef,
    ) -> bool {
        self.actor == actor && self.attacker.same_identity(entity)
    }

    #[must_use]
    pub fn same_identity(&self, other: &Self) -> bool {
        self.actor == other.actor
            && self.seq == other.seq
            && self.tick == other.tick
            && self.attacker.same_identity(other.attacker)
            && self.outcome == other.outcome
            && self.primary == other.primary
            && self.sweeping == other.sweeping
            && self.attacker_effects == other.attacker_effects
    }
}

pub trait CapturedAttackState {
    fn captured_attack_snapshot(&self, attack: &CapturedAttack) -> Option<CapturedAttackSnapshot>;
    fn apply_captured_attack(&mut self, attack: &CapturedAttack) -> bool;
    fn restore_captured_attack_snapshot(&mut self, snapshot: &CapturedAttackSnapshot) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CombatVerdict {
    Applied(CapturedAttackUndo),
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CombatLocalAcceptance {
    AlreadyOptimistic,
    Applied(CapturedAttackUndo),
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedAttackUndo {
    pub expected_current: CapturedAttackSnapshot,
    pub restore: CapturedAttackSnapshot,
}

#[must_use]
pub fn replay_captured_attack(
    state: &mut impl CapturedAttackState,
    attack: &CapturedAttack,
) -> CombatVerdict {
    if attack.is_noop() {
        return CombatVerdict::Rejected;
    }
    let Some(restore) = state.captured_attack_snapshot(attack) else {
        return CombatVerdict::Rejected;
    };
    if !state.apply_captured_attack(attack) {
        return CombatVerdict::Rejected;
    }
    let Some(expected_current) = state.captured_attack_snapshot(attack) else {
        return CombatVerdict::Rejected;
    };
    if expected_current == restore {
        return CombatVerdict::Rejected;
    }
    CombatVerdict::Applied(CapturedAttackUndo {
        expected_current,
        restore,
    })
}

#[must_use]
pub fn undo_captured_attack(
    state: &mut impl CapturedAttackState,
    attack: &CapturedAttack,
    undo: &CapturedAttackUndo,
) -> bool {
    if state
        .captured_attack_snapshot(attack)
        .as_ref()
        != Some(&undo.expected_current)
    {
        return false;
    }
    state.restore_captured_attack_snapshot(&undo.restore)
}

#[derive(Debug, Clone)]
struct LocalCapturedAttack {
    attack: CapturedAttack,
    undo: CapturedAttackUndo,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CombatGroundPromotion {
    pub ground_applied: usize,
    pub ground_rejected: usize,
    pub local_applied: usize,
    pub local_rejected: usize,
    pub local_undone: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CombatSingleTruthResolution {
    pub local_applied: usize,
    pub local_rejected: usize,
    pub local_undone: usize,
}

#[derive(Debug)]
pub struct CombatDualJournal {
    pending: BTreeMap<TickStamp, Vec<LocalCapturedAttack>>,
}

impl CombatDualJournal {
    #[must_use]
    pub fn new() -> Self {
        Self { pending: BTreeMap::new() }
    }

    #[must_use]
    pub fn stage_local_optimistic(
        &mut self,
        local: &mut impl CapturedAttackState,
        attack: CapturedAttack,
    ) -> CombatVerdict {
        let verdict = replay_captured_attack(local, &attack);
        if let CombatVerdict::Applied(undo) = &verdict {
            self.pending
                .entry(attack.tick)
                .or_default()
                .push(LocalCapturedAttack {
                    attack,
                    undo: undo.clone(),
                });
        }
        verdict
    }

    #[must_use]
    pub fn apply_accepted_local(
        &self,
        local: &mut impl CapturedAttackState,
        attack: CapturedAttack,
    ) -> CombatLocalAcceptance {
        if self
            .pending
            .get(&attack.tick)
            .is_some_and(|pending| pending.iter().any(|entry| entry.attack.same_identity(&attack)))
        {
            return CombatLocalAcceptance::AlreadyOptimistic;
        }
        match replay_captured_attack(local, &attack) {
            CombatVerdict::Applied(undo) => CombatLocalAcceptance::Applied(undo),
            CombatVerdict::Rejected => CombatLocalAcceptance::Rejected,
        }
    }

    #[must_use]
    pub fn undo_local_loser(&mut self, local: &mut impl CapturedAttackState, attack: CapturedAttack) -> bool {
        let index = self
            .pending
            .get(&attack.tick)
            .and_then(|pending| pending.iter().position(|entry| entry.attack.same_identity(&attack)));
        let Some(index) = index else {
            return false;
        };
        let Some(entry) = self
            .pending
            .get_mut(&attack.tick)
            .map(|pending| pending.remove(index))
        else {
            return false;
        };
        if undo_captured_attack(local, &entry.attack, &entry.undo) {
            if self.pending.get(&attack.tick).is_some_and(Vec::is_empty) {
                self.pending.remove(&attack.tick);
            }
            true
        } else {
            self.pending
                .entry(attack.tick)
                .or_default()
                .insert(index, entry);
            false
        }
    }

    #[must_use]
    pub fn take_pending_matching(
        &mut self,
        tick: TickStamp,
        mut matches: impl FnMut(&CapturedAttack) -> bool,
    ) -> Vec<(CapturedAttack, CapturedAttackUndo)> {
        let Some(pending) = self.pending.get_mut(&tick) else {
            return Vec::new();
        };
        let mut taken = Vec::new();
        let mut index = 0;
        while index < pending.len() {
            if matches(&pending[index].attack) {
                let entry = pending.remove(index);
                taken.push((entry.attack, entry.undo));
            } else {
                index += 1;
            }
        }
        if pending.is_empty() {
            self.pending.remove(&tick);
        }
        taken
    }

    #[must_use]
    pub fn pending_matching(
        &self,
        tick: TickStamp,
        mut matches: impl FnMut(&CapturedAttack) -> bool,
    ) -> Vec<CapturedAttack> {
        self.pending
            .get(&tick)
            .map(|pending| {
                pending
                    .iter()
                    .filter(|entry| matches(&entry.attack))
                    .map(|entry| entry.attack.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn promote_globally_accepted(
        &mut self,
        local: &mut impl CapturedAttackState,
        ground: &mut impl CapturedAttackState,
        cluster_seed: u64,
        tick: TickStamp,
        accepted: &[CapturedAttack],
    ) -> CombatGroundPromotion {
        let mut promotion = CombatGroundPromotion::default();
        let mut ordered: Vec<_> = accepted.iter().filter(|attack| attack.tick == tick).cloned().collect();
        sort_captured_attacks(cluster_seed, tick, &mut ordered);
        let pending = self.pending.remove(&tick).unwrap_or_default();
        for entry in pending.iter().filter(|entry| !ordered.iter().any(|attack| entry.attack.same_identity(attack))) {
            if undo_captured_attack(local, &entry.attack, &entry.undo) {
                promotion.local_undone += 1;
            }
        }
        for attack in ordered {
            let locally_optimistic = pending.iter().any(|entry| entry.attack.same_identity(&attack));
            match replay_captured_attack(ground, &attack) {
                CombatVerdict::Applied(_) => {
                    promotion.ground_applied += 1;
                    if !locally_optimistic {
                        match replay_captured_attack(local, &attack) {
                            CombatVerdict::Applied(_) => promotion.local_applied += 1,
                            CombatVerdict::Rejected => promotion.local_rejected += 1,
                        }
                    }
                }
                CombatVerdict::Rejected => {
                    promotion.ground_rejected += 1;
                    if let Some(entry) = pending.iter().find(|entry| entry.attack.same_identity(&attack))
                        && undo_captured_attack(local, &entry.attack, &entry.undo)
                    {
                        promotion.local_undone += 1;
                    }
                }
            }
        }
        promotion
    }

    #[must_use]
    pub fn resolve_globally_accepted_single_truth(
        &mut self,
        local: &mut impl CapturedAttackState,
        cluster_seed: u64,
        tick: TickStamp,
        accepted: &[CapturedAttack],
    ) -> CombatSingleTruthResolution {
        let mut resolution = CombatSingleTruthResolution::default();
        let mut ordered: Vec<_> = accepted.iter().filter(|attack| attack.tick == tick).cloned().collect();
        sort_captured_attacks(cluster_seed, tick, &mut ordered);
        let pending = self.pending.remove(&tick).unwrap_or_default();
        for entry in pending.iter().filter(|entry| {
            !ordered
                .iter()
                .any(|attack| entry.attack.same_identity(attack))
        }) {
            if undo_captured_attack(local, &entry.attack, &entry.undo) {
                resolution.local_undone += 1;
            }
        }
        for attack in ordered {
            if pending.iter().any(|entry| entry.attack.same_identity(&attack)) {
                continue;
            }
            match replay_captured_attack(local, &attack) {
                CombatVerdict::Applied(_) => resolution.local_applied += 1,
                CombatVerdict::Rejected => resolution.local_rejected += 1,
            }
        }
        resolution
    }
}

#[must_use]
pub fn combat_conflict_key(attack: &CapturedAttack) -> crate::accept::ConflictKey {
    match attack.primary.target {
        EntityMutationTarget::Entity(entity) => {
            crate::accept::ConflictKey::for_entity_origin(entity.origin, entity.local_id)
        }
        EntityMutationTarget::Player { gid, .. } => crate::accept::ConflictKey::for_player(gid),
    }
}

#[must_use]
pub fn order_captured_attacks(
    cluster_seed: u64,
    tick: TickStamp,
    left: &CapturedAttack,
    right: &CapturedAttack,
) -> std::cmp::Ordering {
    attack_conflict_keys(left)
        .cmp(&attack_conflict_keys(right))
        .then_with(|| {
            crate::order::order_action_actors(cluster_seed, tick, left.actor, right.actor).reverse()
        })
        .then_with(|| left.seq.cmp(&right.seq))
        .then_with(|| {
            attack_target_damage_milli(&left.primary.outcome)
                .cmp(&attack_target_damage_milli(&right.primary.outcome))
        })
}

#[must_use]
pub fn attack_conflict_keys(attack: &CapturedAttack) -> Vec<crate::accept::ConflictKey> {
    let mut keys: Vec<_> = attack
        .targets()
        .map(|target| match target.target {
            EntityMutationTarget::Entity(entity) => crate::accept::ConflictKey::for_entity_origin(entity.origin, entity.local_id),
            EntityMutationTarget::Player { gid, .. } => crate::accept::ConflictKey::for_player(gid),
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn attack_target_damage_milli(outcome: &CapturedAttackTargetOutcome) -> u32 {
    match outcome {
        CapturedAttackTargetOutcome::Living(outcome) => outcome.raw_damage_milli,
        CapturedAttackTargetOutcome::NonLiving(_) => 0,
    }
}

#[must_use]
pub fn living_damage_is_exact(outcome: &crate::protocol::LivingAttackOutcome) -> bool {
    outcome.effective_damage_milli <= outcome.raw_damage_milli
        && u64::from(outcome.absorption_damage_milli) + u64::from(outcome.health_damage_milli)
            == u64::from(outcome.cooldown_damage_delta_milli)
        && match outcome.hurt_cooldown_resolution {
            crate::protocol::LivingHurtCooldownResolution::NoDamage => {
                outcome.before.hurt_cooldown > 10
                    && i32::try_from(outcome.effective_damage_milli)
                        .is_ok_and(|damage| damage <= outcome.before.last_damage_taken_milli)
                    && outcome.cooldown_damage_delta_milli == 0
                    && outcome.absorption_damage_milli == 0
                    && outcome.health_damage_milli == 0
            }
            crate::protocol::LivingHurtCooldownResolution::Applied {
                last_damage_taken_after_milli,
            } => {
                i32::try_from(outcome.raw_damage_milli)
                    .is_ok_and(|damage| last_damage_taken_after_milli == damage)
                    && if outcome.before.hurt_cooldown > 10 {
                        outcome.before.last_damage_taken_milli >= 0
                            && i32::try_from(outcome.effective_damage_milli).is_ok_and(|damage| {
                                damage > outcome.before.last_damage_taken_milli
                                    && i32::try_from(outcome.cooldown_damage_delta_milli)
                                        .is_ok_and(|delta| {
                                            delta == damage - outcome.before.last_damage_taken_milli
                                        })
                            })
                    } else {
                        outcome.cooldown_damage_delta_milli == outcome.effective_damage_milli
                    }
            }
}

}

pub fn sort_captured_attacks(cluster_seed: u64, tick: TickStamp, attacks: &mut [CapturedAttack]) {
    attacks.sort_by(|left, right| order_captured_attacks(cluster_seed, tick, left, right));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CombatLimits {
    pub max_damage_milli: u32,
    pub max_charge_milli: u16,
    pub max_tick_age: u16,
}

impl CombatLimits {
    #[must_use]
    pub const fn default_limits() -> Self {
        Self {
            max_damage_milli: i32::MAX as u32,
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
    DamageInconsistent,
    MalformedTargetOutcome,
    ChargeOutOfRange,
    BadDirection,
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
pub fn validate_captured_attack(
    attack: &CapturedAttack,
    last_seq: Option<PlayerSeq>,
    now: TickStamp,
    limits: &CombatLimits,
) -> Result<(), CombatRejectReason> {
    if !is_seq_fresh(last_seq, attack.seq) {
        return Err(CombatRejectReason::StaleSequence);
    }
    if !is_tick_fresh(now, attack.tick, limits.max_tick_age) {
        return Err(CombatRejectReason::StaleTick);
    }
    for target in attack.targets() {
        if !target.outcome.is_well_formed() {
            return Err(CombatRejectReason::MalformedTargetOutcome);
        }
        if attack_target_damage_milli(&target.outcome) > limits.max_damage_milli {
            return Err(CombatRejectReason::DamageOutOfRange);
        }
        if let CapturedAttackTargetOutcome::Living(outcome) = &target.outcome
            && !living_damage_is_exact(outcome)
        {
            return Err(CombatRejectReason::DamageInconsistent);
        }
        if target.target.same_identity(EntityMutationTarget::Entity(attack.attacker)) {
            return Err(CombatRejectReason::SelfHit);
        }
    }
    Ok(())
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

pub fn stage_captured_attack(attack: CapturedAttack) {
    let mut staged = Some(attack);
    with_combat_bank(|bank| bank.attacks.push(staged.take().expect("combat bank invokes once")));
}

pub fn stage_fire(update: FireProjectileUpdate) {
    with_combat_bank(|bank| bank.fire.push(update));
}

pub fn stage_entity_mutation(update: EntityMutationUpdate) {
    with_combat_bank(|bank| bank.entity_mutations.push(update));
}

#[derive(Debug, Default)]
pub struct DrainedCombat {
    pub attacks: Vec<CapturedAttack>,
    pub fire: Vec<FireProjectileUpdate>,
    pub entity_mutations: Vec<EntityMutationUpdate>,
}

impl DrainedCombat {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.attacks.is_empty()
            && self.fire.is_empty()
            && self.entity_mutations.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.attacks.len()
            + self.fire.len()
            + self.entity_mutations.len()
    }
}

#[must_use]
pub fn drain_combat_updates() -> DrainedCombat {
    with_combat_bank(|bank| DrainedCombat {
        attacks: core::mem::take(&mut bank.attacks),
        fire: core::mem::take(&mut bank.fire),
        entity_mutations: core::mem::take(&mut bank.entity_mutations),
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
    sort_captured_attacks(cluster_seed, tick, &mut drained.attacks);
    sort_fire(cluster_seed, tick, &mut drained.fire);
    crate::entity_action::sort_entity_mutations(cluster_seed, tick, &mut drained.entity_mutations);
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

fn dir_bits(dir: [f32; 3]) -> [u32; 3] {
    [dir[0].to_bits(), dir[1].to_bits(), dir[2].to_bits()]
}

#[cfg(test)]
mod captured_attack_tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::inventory::{InvLoc, InventoryStack};
    use crate::protocol::{
        AttackCooldownTransition, AttackDamageType, AttackItemTransition, AttackKnockback,
        CapturedAttackActorEffects, CapturedAttackOutcome, CapturedAttackTarget, CombatAdvancement,
        CapturedAttackTargetOutcome, CombatAdvancementTransition, CombatAttributeSnapshot,
        CombatEquipmentSnapshot, CombatStatKind,
        CombatStatTransition, LivingAttackOutcome, LivingAttackPrecondition,
        LivingHurtCooldownResolution, AttackItemDrop, EntityNbtTransition, NonLivingAttackKind,
        NonLivingAttackLifecycle, NonLivingAttackOutcome,
    };

    fn entity(id: i32) -> EntityRef {
        EntityRef {
            origin: ServerId(1),
            owner: ServerId(1),
            local_id: id,
            chunk: ChunkAddr { x: 0, z: 0 },
        }
    }

    fn target(id: i32) -> EntityMutationTarget {
        EntityMutationTarget::Entity(entity(id))
    }

    fn combat_state(health_milli: i32) -> CombatStateSnapshot {
        CombatStateSnapshot {
            health_milli,
            absorption_milli: 0,
            hurt_cooldown: 0,
            last_damage_taken_milli: 0,
            dead: false,
            death_time: 0,
            last_damage_type: None,
            last_damage_tick: 0,
            last_attacker_id: 0,
            last_attacked_tick: 0,
            last_attacking_id: 0,
            last_attack_tick: 0,
            last_hurt_by_player_id: 0,
            last_hurt_by_player_tick: 0,
            last_hurt_by_mob_id: 0,
            last_hurt_by_mob_tick: 0,
            velocity_bits: [0; 3],
            fire_ticks: 0,
            visual_fire: false,
            active_effects: Vec::new(),
            combat_tracker: CombatTrackerState {
                entries: Vec::new(),
                last_damage_tick: 0,
                combat_start_tick: 0,
                combat_end_tick: 0,
                in_combat: false,
                taking_damage: false,
            },
        }
    }

    fn actor_snapshot() -> CapturedAttackActorSnapshot {
        CapturedAttackActorSnapshot {
            combat: combat_state(20_000),
            cooldown: 12,
            velocity_bits: [1, 2, 3],
            fall_distance_bits: 6.0f32.to_bits(),
            exhaustion_bits: 0.0f32.to_bits(),
            held_item: None,
            stats: Vec::new(),
            advancements: Vec::new(),
        }
    }

    fn living_precondition() -> LivingAttackPrecondition {
        LivingAttackPrecondition {
            health_milli: 20_000,
            absorption_milli: 0,
            hurt_cooldown: 0,
            last_damage_taken_milli: 0,
            fire_ticks: 0,
            visual_fire: false,
            velocity_bits: [0; 3],
            effects: Vec::new(),
            attributes: vec![CombatAttributeSnapshot { id: 7, value_bits: 9 }],
            equipment: vec![CombatEquipmentSnapshot {
                slot: 1,
                item: InventoryStack { item: 3, count: 1, nbt: vec![4] },
            }],
        }
    }

    fn attack() -> CapturedAttack {
        CapturedAttack {
            actor: ActionActor::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(3))),
            seq: PlayerSeq(7),
            tick: TickStamp(9),
            attacker: entity(3),
            outcome: CapturedAttackOutcome::Landed,
            primary: CapturedAttackTarget {
                target: target(4),
                outcome: CapturedAttackTargetOutcome::Living(LivingAttackOutcome {
                    before: living_precondition(),
                    raw_damage_milli: 4_000,
                    effective_damage_milli: 4_000,
                    cooldown_damage_delta_milli: 4_000,
                    absorption_damage_milli: 0,
                    health_damage_milli: 4_000,
                    hurt_cooldown_resolution: LivingHurtCooldownResolution::Applied {
                        last_damage_taken_after_milli: 4_000,
                    },
                    damage_type: AttackDamageType::MaceSmash,
                    critical: true,
                    fire_ticks_before: 0,
                    fire_ticks_after: 80,
                    visual_fire_before: false,
                    visual_fire_after: true,
                    knockback: None,
                }),
            },
            sweeping: vec![CapturedAttackTarget {
                target: target(5),
                outcome: CapturedAttackTargetOutcome::Living(LivingAttackOutcome {
                    before: living_precondition(),
                    raw_damage_milli: 1_000,
                    effective_damage_milli: 1_000,
                    cooldown_damage_delta_milli: 1_000,
                    absorption_damage_milli: 0,
                    health_damage_milli: 1_000,
                    hurt_cooldown_resolution: LivingHurtCooldownResolution::Applied {
                        last_damage_taken_after_milli: 1_000,
                    },
                    damage_type: AttackDamageType::PlayerAttack,
                    critical: false,
                    fire_ticks_before: 0,
                    fire_ticks_after: 0,
                    visual_fire_before: false,
                    visual_fire_after: false,
                    knockback: None,
                }),
            }],
            attacker_effects: CapturedAttackActorEffects {
                cooldown: AttackCooldownTransition { before: 12, after: 0 },
                velocity: crate::protocol::AttackVelocityTransition {
                    before_bits: [1, 2, 3],
                    after_bits: [4, 2, 6],
                },
                last_attacking_id_before: 0,
                last_attacking_id_after: 4,
                last_attack_tick_before: 0,
                last_attack_tick_after: 9,
                fall_distance_before_bits: 6.0f32.to_bits(),
                fall_distance_after_bits: 0.0f32.to_bits(),
                exhaustion_before_bits: 0.0f32.to_bits(),
                exhaustion_after_bits: 0.1f32.to_bits(),
                item: None,
                stats: Vec::new(),
                advancements: Vec::new(),
            },
        }
    }

    #[test]
    fn attack_conflict_keys_include_sweeping_targets() {
        let attack = attack();
        assert_eq!(attack_conflict_keys(&attack), vec![
            crate::accept::ConflictKey::for_entity(entity(4)),
            crate::accept::ConflictKey::for_entity(entity(5)),
        ]);
    }

    struct State {
        actor: CapturedAttackActorSnapshot,
        targets: Vec<CapturedAttackTargetSnapshot>,
    }

    impl State {
        fn new() -> Self {
            Self {
                actor: actor_snapshot(),
                targets: vec![
                    CapturedAttackTargetSnapshot::Living { target: target(4), state: combat_state(20_000) },
                    CapturedAttackTargetSnapshot::Living { target: target(5), state: combat_state(20_000) },
                ],
            }
        }
    }

    impl CapturedAttackState for State {
        fn captured_attack_snapshot(&self, _attack: &CapturedAttack) -> Option<CapturedAttackSnapshot> {
            Some(CapturedAttackSnapshot { actor: self.actor.clone(), targets: self.targets.clone() })
        }

        fn apply_captured_attack(&mut self, attack: &CapturedAttack) -> bool {
            self.actor.cooldown = attack.attacker_effects.cooldown.after;
            self.actor.velocity_bits = attack.attacker_effects.velocity.after_bits;
            self.actor.fall_distance_bits = attack.attacker_effects.fall_distance_after_bits;
            self.actor.exhaustion_bits = attack.attacker_effects.exhaustion_after_bits;
            if attack.outcome == CapturedAttackOutcome::Landed {
                for outcome in attack.targets() {
                    let Some(snapshot) = self.targets.iter_mut().find(|snapshot| snapshot.target() == outcome.target) else {
                        return false;
                    };
                    let CapturedAttackTargetOutcome::Living(outcome) = &outcome.outcome else {
                        return false;
                    };
                    let CapturedAttackTargetSnapshot::Living { state, .. } = snapshot else {
                        return false;
                    };
                    state.absorption_milli -= i32::try_from(outcome.absorption_damage_milli).unwrap();
                    state.health_milli -= i32::try_from(outcome.health_damage_milli).unwrap();
                    state.fire_ticks = outcome.fire_ticks_after;
                    state.visual_fire = outcome.visual_fire_after;
                }
            }
            true
        }

        fn restore_captured_attack_snapshot(&mut self, snapshot: &CapturedAttackSnapshot) -> bool {
            self.actor = snapshot.actor.clone();
            self.targets = snapshot.targets.clone();
            true
        }
    }

    #[test]
    fn journal_undo_restores_actor_primary_and_sweeping_state_atomically() {
        let attack = capture_attack(attack());
        let before = State::new();
        let mut state = State::new();
        let mut journal = CombatDualJournal::new();
        assert!(matches!(journal.stage_local_optimistic(&mut state, attack.clone()), CombatVerdict::Applied(_)));
        assert_ne!(state.actor, before.actor);
        assert_ne!(state.targets, before.targets);
        assert!(journal.undo_local_loser(&mut state, attack));
        assert_eq!(state.actor, before.actor);
        assert_eq!(state.targets, before.targets);
    }

    #[test]
    fn captured_attack_wire_round_trip_keeps_every_stateful_outcome() {
        let mut captured = attack();
        let CapturedAttackTargetOutcome::Living(primary) = &mut captured.primary.outcome else {
            unreachable!();
        };
        primary.knockback = Some(AttackKnockback {
            velocity_before_bits: [1, 2, 3],
            velocity_after_bits: [4, 5, 6],
        });
        captured.attacker_effects.item = Some(AttackItemTransition {
            slot: InvLoc::new(0, 2),
            before: InventoryStack { item: 7, count: 1, nbt: vec![1] },
            after: InventoryStack { item: 7, count: 1, nbt: vec![2] },
        });
        captured.attacker_effects.stats = vec![CombatStatTransition {
            kind: CombatStatKind::Used,
            item: 7,
            before: 10,
            after: 11,
        }];
        captured.attacker_effects.advancements = vec![CombatAdvancementTransition {
            advancement: CombatAdvancement::DealtOverkillDamage,
            before: false,
            after: true,
        }];
        let bytes = postcard::to_allocvec(&captured).unwrap();
        let decoded: CapturedAttack = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, captured);
    }

    #[test]
    fn valid_no_damage_preserves_actor_attempt_effects_without_target_mutation() {
        let mut no_damage = attack();
        no_damage.outcome = CapturedAttackOutcome::NoDamage;
        let CapturedAttackTargetOutcome::Living(primary) = &mut no_damage.primary.outcome else {
            unreachable!();
        };
        primary.before.hurt_cooldown = 20;
        primary.before.last_damage_taken_milli = 3_000;
        primary.raw_damage_milli = 2_000;
        primary.effective_damage_milli = 2_000;
        primary.cooldown_damage_delta_milli = 0;
        primary.absorption_damage_milli = 0;
        primary.health_damage_milli = 0;
        primary.hurt_cooldown_resolution = LivingHurtCooldownResolution::NoDamage;
        no_damage.sweeping.clear();
        no_damage.attacker_effects.velocity.after_bits = no_damage.attacker_effects.velocity.before_bits;
        no_damage.attacker_effects.last_attacking_id_after = no_damage.attacker_effects.last_attacking_id_before;
        no_damage.attacker_effects.last_attack_tick_after = no_damage.attacker_effects.last_attack_tick_before;
        no_damage.attacker_effects.fall_distance_after_bits = no_damage.attacker_effects.fall_distance_before_bits;
        no_damage.attacker_effects.exhaustion_after_bits = no_damage.attacker_effects.exhaustion_before_bits;
        assert_eq!(
            validate_captured_attack(
                &no_damage,
                None,
                no_damage.tick,
                &CombatLimits::default(),
            ),
            Ok(())
        );
        let before = State::new();
        let mut local = State::new();
        let mut journal = CombatDualJournal::new();
        assert!(matches!(journal.stage_local_optimistic(&mut local, no_damage.clone()), CombatVerdict::Applied(_)));
        assert_ne!(local.actor, before.actor);
        assert_eq!(local.targets, before.targets);
        assert!(journal.undo_local_loser(&mut local, no_damage));
        assert_eq!(local.actor, before.actor);
        assert_eq!(local.targets, before.targets);
    }

    #[test]
    fn raw_mitigated_absorption_and_health_damage_are_distinct_and_validated() {
        let mut captured = attack();
        {
            let CapturedAttackTargetOutcome::Living(primary) = &mut captured.primary.outcome else {
                unreachable!();
            };
            primary.raw_damage_milli = 5_000;
            primary.effective_damage_milli = 3_000;
            primary.cooldown_damage_delta_milli = 3_000;
            primary.absorption_damage_milli = 1_000;
            primary.health_damage_milli = 2_000;
            primary.hurt_cooldown_resolution = LivingHurtCooldownResolution::Applied {
                last_damage_taken_after_milli: 5_000,
            };
            assert!(living_damage_is_exact(primary));
        }
        assert_eq!(
            validate_captured_attack(
                &captured,
                None,
                captured.tick,
                &CombatLimits::default(),
            ),
            Ok(())
        );
        {
            let CapturedAttackTargetOutcome::Living(primary) = &mut captured.primary.outcome else {
                unreachable!();
            };
            primary.health_damage_milli = 2_001;
            assert!(!living_damage_is_exact(primary));
        }
        assert_eq!(
            validate_captured_attack(
                &captured,
                None,
                captured.tick,
                &CombatLimits::default(),
            ),
            Err(CombatRejectReason::DamageInconsistent)
        );
    }

    #[test]
    fn promotion_and_holder_reconciliation_preserve_complete_attack_state() {
        let attack = attack();
        let mut local = State::new();
        let mut ground = State::new();
        let mut journal = CombatDualJournal::new();
        assert!(matches!(journal.stage_local_optimistic(&mut local, attack.clone()), CombatVerdict::Applied(_)));
        let promotion = journal.promote_globally_accepted(
            &mut local,
            &mut ground,
            7,
            attack.tick,
            core::slice::from_ref(&attack),
        );
        assert_eq!(promotion.ground_applied, 1);
        assert_eq!(promotion.local_applied, 0);
        assert_eq!(local.actor, ground.actor);
        assert_eq!(local.targets, ground.targets);

        let mut holder = State::new();
        let mut holder_journal = CombatDualJournal::new();
        let resolution = holder_journal.resolve_globally_accepted_single_truth(
            &mut holder,
            7,
            attack.tick,
            core::slice::from_ref(&attack),
        );
        assert_eq!(resolution.local_applied, 1);
        assert_eq!(holder.actor, ground.actor);
        assert_eq!(holder.targets, ground.targets);
    }

    fn nonliving_attack() -> CapturedAttack {
        let mut attack = attack();
        attack.primary = CapturedAttackTarget {
            target: target(8),
            outcome: CapturedAttackTargetOutcome::NonLiving(NonLivingAttackOutcome::Destroy(
                EntityNbtTransition {
                    kind: NonLivingAttackKind::Painting,
                    before: vec![1],
                    after: vec![2],
                    removed_before: false,
                    removed_after: true,
                    drops: vec![AttackItemDrop {
                        entity: entity(90),
                        stack: InventoryStack { item: 4, count: 1, nbt: vec![10] },
                        entity_nbt: vec![10, 0],
                    }],
                    lifecycle: NonLivingAttackLifecycle {
                        present_before: true,
                        present_after: false,
                        spawned: vec![AttackItemDrop {
                            entity: entity(90),
                            stack: InventoryStack { item: 4, count: 1, nbt: vec![10] },
                            entity_nbt: vec![10, 0],
                        }],
                    },
                },
            )),
        };
        attack.sweeping.clear();
        attack
    }

    fn current_nonliving(outcome: &NonLivingAttackOutcome) -> NonLivingAttackOutcome {
        let NonLivingAttackOutcome::Destroy(mut outcome) = outcome.clone() else {
            unreachable!();
        };
        outcome.before = outcome.after.clone();
        outcome.removed_before = outcome.removed_after;
        outcome.lifecycle.present_before = outcome.lifecycle.present_after;
        NonLivingAttackOutcome::Destroy(outcome)
    }

    struct NonLivingState {
        actor: CapturedAttackActorSnapshot,
        target: NonLivingAttackOutcome,
    }

    impl NonLivingState {
        fn new(attack: &CapturedAttack) -> Self {
            let CapturedAttackTargetOutcome::NonLiving(target) = &attack.primary.outcome else {
                unreachable!();
            };
            let NonLivingAttackOutcome::Destroy(mut target) = target.clone() else {
                unreachable!();
            };
            target.after = target.before.clone();
            target.removed_after = target.removed_before;
            target.drops.clear();
            target.lifecycle.present_after = target.lifecycle.present_before;
            target.lifecycle.spawned.clear();
            Self {
                actor: actor_snapshot(),
                target: NonLivingAttackOutcome::Destroy(target),
            }
        }
    }

    impl CapturedAttackState for NonLivingState {
        fn captured_attack_snapshot(&self, attack: &CapturedAttack) -> Option<CapturedAttackSnapshot> {
            Some(CapturedAttackSnapshot {
                actor: self.actor.clone(),
                targets: vec![CapturedAttackTargetSnapshot::NonLiving {
                    target: attack.primary.target,
                    outcome: self.target.clone(),
                }],
            })
        }

        fn apply_captured_attack(&mut self, attack: &CapturedAttack) -> bool {
            let CapturedAttackTargetOutcome::NonLiving(outcome) = &attack.primary.outcome else {
                return false;
            };
            self.actor.cooldown = attack.attacker_effects.cooldown.after;
            self.actor.velocity_bits = attack.attacker_effects.velocity.after_bits;
            self.actor.fall_distance_bits = attack.attacker_effects.fall_distance_after_bits;
            self.actor.exhaustion_bits = attack.attacker_effects.exhaustion_after_bits;
            self.target = current_nonliving(outcome);
            true
        }

        fn restore_captured_attack_snapshot(&mut self, snapshot: &CapturedAttackSnapshot) -> bool {
            let Some(CapturedAttackTargetSnapshot::NonLiving { outcome, .. }) = snapshot.targets.first() else {
                return false;
            };
            self.actor = snapshot.actor.clone();
            self.target = outcome.clone();
            true
        }
    }

    #[test]
    fn nonliving_lifecycle_rollback_promotion_and_holder_preserve_exact_drop_refs() {
        let attack = nonliving_attack();
        let before = NonLivingState::new(&attack);
        let mut local = NonLivingState::new(&attack);
        let mut journal = CombatDualJournal::new();
        assert!(matches!(journal.stage_local_optimistic(&mut local, attack.clone()), CombatVerdict::Applied(_)));
        assert_ne!(local.target, before.target);
        assert!(journal.undo_local_loser(&mut local, attack.clone()));
        assert_eq!(local.target, before.target);

        assert!(matches!(journal.stage_local_optimistic(&mut local, attack.clone()), CombatVerdict::Applied(_)));
        let mut ground = NonLivingState::new(&attack);
        let promotion = journal.promote_globally_accepted(
            &mut local,
            &mut ground,
            7,
            attack.tick,
            core::slice::from_ref(&attack),
        );
        assert_eq!(promotion.ground_applied, 1);
        assert_eq!(local.target, ground.target);
        let NonLivingAttackOutcome::Destroy(ground_target) = &ground.target else {
            unreachable!();
        };
        assert_eq!(ground_target.lifecycle.spawned[0].entity, entity(90));

        let mut holder = NonLivingState::new(&attack);
        let mut holder_journal = CombatDualJournal::new();
        let resolution = holder_journal.resolve_globally_accepted_single_truth(
            &mut holder,
            7,
            attack.tick,
            core::slice::from_ref(&attack),
        );
        assert_eq!(resolution.local_applied, 1);
        assert_eq!(holder.target, ground.target);
    }
}
