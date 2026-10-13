use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use pumpkin_cluster::completion::ActionKey;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::time::TickStamp;
use pumpkin_nbt::compound::NbtCompound;
use rustc_hash::FxHashMap;

use crate::entity::player::statistics::Statistics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatisticSource {
    Infallible(TickStamp),
    LeaveGame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatisticTickEvidence {
    pub alive: bool,
    pub sleeping: bool,
    pub sneaking: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatisticMutation {
    Increment {
        category: i32,
        stat: i32,
        amount: i32,
    },
    Set {
        category: i32,
        stat: i32,
        value: i32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatisticIncrement {
    pub category: i32,
    pub stat: i32,
    pub amount: i32,
}

pub(crate) struct PreparedStatisticMutation {
    pub actor: GlobalPlayerId,
    pub tick: TickStamp,
    pub key: ActionKey,
    pub expected_local_revision: u64,
    pub increments: Vec<StatisticIncrement>,
    pub before: Vec<((i32, i32), i32)>,
    pub after: Vec<((i32, i32), i32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatisticPrepareError {
    UnknownActor,
    DuplicateAction,
    PendingFull,
    InvalidMutation,
    RevisionMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptedStatisticFailure {
    UnknownActor,
    RevisionMismatch { expected: u64, actual: u64 },
    InvalidBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcceptedStatisticError {
    pub tick: TickStamp,
    pub key: ActionKey,
    pub failure: AcceptedStatisticFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatisticDelta {
    pub actor: GlobalPlayerId,
    pub category: i32,
    pub stat: i32,
    pub amount: i32,
    pub source: StatisticSource,
}

#[derive(Clone)]
pub(crate) struct ActorStatisticsSnapshot {
    pub incarnation: u64,
    pub revision: u64,
    pub values: Arc<FxHashMap<(i32, i32), i32>>,
    pub ground_revision: u64,
    pub ground_values: Arc<FxHashMap<(i32, i32), i32>>,
    pub leave_game_recorded: bool,
}

impl ActorStatisticsSnapshot {
    pub fn write_nbt(&self, nbt: &mut NbtCompound) {
        Statistics {
            stats: self.ground_values.as_ref().clone(),
        }
        .write_nbt(nbt);
    }
}

pub(crate) type StatisticsPublication =
    Arc<ArcSwap<HashMap<GlobalPlayerId, Arc<ActorStatisticsSnapshot>>>>;

pub(crate) struct ActorStatisticsImport {
    pub actor: GlobalPlayerId,
    pub values: FxHashMap<(i32, i32), i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActorStatisticsRetire {
    pub actor: GlobalPlayerId,
    pub player_key: uuid::Uuid,
    pub incarnation: u64,
    pub install_generation: u64,
}

impl ActorStatisticsImport {
    pub fn from_nbt(actor: GlobalPlayerId, nbt: Option<&NbtCompound>) -> Self {
        let mut values = Statistics::default();
        if let Some(nbt) = nbt {
            values.read_nbt(nbt);
        }
        Self {
            actor,
            values: values.stats,
        }
    }
}

struct ActorStatistics {
    incarnation: u64,
    local_revision: u64,
    ground_revision: u64,
    local_values: FxHashMap<(i32, i32), i32>,
    ground_values: FxHashMap<(i32, i32), i32>,
    pending: HashMap<(TickStamp, ActionKey), Vec<StatisticIncrement>>,
    pending_total: FxHashMap<(i32, i32), i64>,
    leave_game_recorded: bool,
    last_infallible_tick: Option<TickStamp>,
}

impl ActorStatistics {
    fn refresh_local(&mut self, key: (i32, i32)) {
        let pending = self.pending_total.get(&key).copied().unwrap_or(0);
        match self.ground_values.get(&key).copied() {
            Some(ground) => {
                let value =
                    (i64::from(ground) + pending).clamp(i64::from(i32::MIN), i64::from(i32::MAX));
                self.local_values.insert(key, value as i32);
            }
            None if pending == 0 => {
                self.local_values.remove(&key);
            }
            None => {
                let value = pending.clamp(i64::from(i32::MIN), i64::from(i32::MAX));
                self.local_values.insert(key, value as i32);
            }
        }
    }

    fn ground_increment(&mut self, key: (i32, i32), amount: i32) {
        let value = self.ground_values.entry(key).or_default();
        *value = value.saturating_add(amount);
        self.refresh_local(key);
    }
}

#[derive(Default)]
pub(crate) struct OwnedActorStatistics {
    actors: HashMap<GlobalPlayerId, ActorStatistics>,
    dirty: HashSet<GlobalPlayerId>,
    published: StatisticsPublication,
    next_incarnation: u64,
}

impl OwnedActorStatistics {
    pub fn with_publication(published: StatisticsPublication) -> Self {
        Self {
            actors: HashMap::new(),
            dirty: HashSet::new(),
            published,
            next_incarnation: 0,
        }
    }

    pub(crate) fn can_import(&self, actor: GlobalPlayerId) -> bool {
        !self.actors.contains_key(&actor) && self.next_incarnation.checked_add(1).is_some()
    }

    pub fn import(&mut self, import: ActorStatisticsImport) -> bool {
        if !self.can_import(import.actor) {
            return false;
        }
        self.next_incarnation = self
            .next_incarnation
            .checked_add(1)
            .expect("preflighted actor incarnation");
        self.actors.insert(
            import.actor,
            ActorStatistics {
                incarnation: self.next_incarnation,
                local_revision: 0,
                ground_revision: 0,
                local_values: import.values.clone(),
                ground_values: import.values,
                pending: HashMap::new(),
                pending_total: FxHashMap::default(),
                leave_game_recorded: false,
                last_infallible_tick: None,
            },
        );
        self.dirty.insert(import.actor);
        true
    }

    pub fn import_nbt(&mut self, actor: GlobalPlayerId, nbt: &NbtCompound) -> bool {
        self.import(ActorStatisticsImport::from_nbt(actor, Some(nbt)))
    }

    pub fn preview_item_mutations(
        &self,
        actor: GlobalPlayerId,
        tick: TickStamp,
        key: ActionKey,
        mutations: &[StatisticMutation],
    ) -> Result<PreparedStatisticMutation, StatisticPrepareError> {
        let state = self
            .actors
            .get(&actor)
            .ok_or(StatisticPrepareError::UnknownActor)?;
        if state.pending.contains_key(&(tick, key)) {
            return Err(StatisticPrepareError::DuplicateAction);
        }
        if state.pending.len() >= 4096 {
            return Err(StatisticPrepareError::PendingFull);
        }
        if mutations.is_empty() || mutations.len() > 32 {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        let mut increments = Vec::with_capacity(mutations.len());
        let mut sums = FxHashMap::<(i32, i32), i64>::default();
        for mutation in mutations {
            let StatisticMutation::Increment {
                category,
                stat,
                amount,
            } = *mutation
            else {
                return Err(StatisticPrepareError::InvalidMutation);
            };
            if amount <= 0 {
                return Err(StatisticPrepareError::InvalidMutation);
            }
            increments.push(StatisticIncrement {
                category,
                stat,
                amount,
            });
            let sum = sums.entry((category, stat)).or_default();
            *sum = sum
                .checked_add(i64::from(amount))
                .ok_or(StatisticPrepareError::InvalidMutation)?;
        }
        let mut before = Vec::with_capacity(sums.len());
        let mut after = Vec::with_capacity(sums.len());
        for (key, amount) in sums {
            let current = state.local_values.get(&key).copied().unwrap_or(0);
            let next = (i64::from(current) + amount).clamp(i64::from(i32::MIN), i64::from(i32::MAX))
                as i32;
            before.push((key, current));
            after.push((key, next));
        }
        before.sort_unstable_by_key(|entry| entry.0);
        after.sort_unstable_by_key(|entry| entry.0);
        Ok(PreparedStatisticMutation {
            actor,
            tick,
            key,
            expected_local_revision: state.local_revision,
            increments,
            before,
            after,
        })
    }

    pub fn commit_prepared_item_mutation(
        &mut self,
        prepared: PreparedStatisticMutation,
    ) -> Result<u64, StatisticPrepareError> {
        let state = self
            .actors
            .get_mut(&prepared.actor)
            .ok_or(StatisticPrepareError::UnknownActor)?;
        if state.local_revision != prepared.expected_local_revision
            || prepared
                .before
                .iter()
                .any(|(key, value)| state.local_values.get(key).copied().unwrap_or(0) != *value)
        {
            return Err(StatisticPrepareError::RevisionMismatch);
        }
        if state.pending.contains_key(&(prepared.tick, prepared.key)) {
            return Err(StatisticPrepareError::DuplicateAction);
        }
        if state.pending.len() >= 4096 {
            return Err(StatisticPrepareError::PendingFull);
        }
        if prepared.increments.is_empty() || prepared.increments.len() > 32 {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        let mut additions = FxHashMap::<(i32, i32), i64>::default();
        for increment in &prepared.increments {
            if increment.amount <= 0 {
                return Err(StatisticPrepareError::InvalidMutation);
            }
            let key = (increment.category, increment.stat);
            let sum = additions.entry(key).or_default();
            *sum = sum
                .checked_add(i64::from(increment.amount))
                .ok_or(StatisticPrepareError::InvalidMutation)?;
        }
        if additions.len() != prepared.before.len() || additions.len() != prepared.after.len() {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        for (key, addition) in &additions {
            if !prepared
                .before
                .iter()
                .any(|(before_key, _)| before_key == key)
            {
                return Err(StatisticPrepareError::InvalidMutation);
            }
            let current = state.pending_total.get(key).copied().unwrap_or(0);
            let total = current
                .checked_add(*addition)
                .ok_or(StatisticPrepareError::InvalidMutation)?;
            let ground = state.ground_values.get(key).copied().unwrap_or(0);
            let next =
                (i64::from(ground) + total).clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
            if !prepared.after.contains(&(*key, next)) {
                return Err(StatisticPrepareError::RevisionMismatch);
            }
        }
        for (key, addition) in additions {
            let total = state.pending_total.entry(key).or_default();
            *total += addition;
            state.refresh_local(key);
        }
        state
            .pending
            .insert((prepared.tick, prepared.key), prepared.increments);
        state.local_revision = state.local_revision.wrapping_add(1);
        self.dirty.insert(prepared.actor);
        Ok(state.local_revision)
    }

    pub fn settle_item_mutation(
        &mut self,
        actor: GlobalPlayerId,
        tick: TickStamp,
        key: ActionKey,
        accepted: bool,
        increments: &[StatisticIncrement],
    ) -> Result<(u64, u64), StatisticPrepareError> {
        let state = self
            .actors
            .get_mut(&actor)
            .ok_or(StatisticPrepareError::UnknownActor)?;
        if increments.is_empty()
            || increments.len() > 32
            || increments.iter().any(|increment| increment.amount <= 0)
        {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        let pending = state.pending.get(&(tick, key));
        if pending.is_some_and(|pending| pending.as_slice() != increments) {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        if !accepted && pending.is_none() {
            return Err(StatisticPrepareError::InvalidMutation);
        }
        let mut removals = FxHashMap::<(i32, i32), i64>::default();
        if let Some(pending) = pending {
            for increment in pending {
                let sum = removals
                    .entry((increment.category, increment.stat))
                    .or_default();
                *sum = sum
                    .checked_add(i64::from(increment.amount))
                    .ok_or(StatisticPrepareError::InvalidMutation)?;
            }
        }
        let mut new_totals = Vec::with_capacity(removals.len());
        for (key, removal) in removals {
            let Some(total) = state.pending_total.get(&key).copied() else {
                return Err(StatisticPrepareError::InvalidMutation);
            };
            let Some(next) = total.checked_sub(removal) else {
                return Err(StatisticPrepareError::InvalidMutation);
            };
            if next < 0 {
                return Err(StatisticPrepareError::InvalidMutation);
            }
            new_totals.push((key, next));
        }
        state.pending.remove(&(tick, key));
        for (key, next) in new_totals {
            if next == 0 {
                state.pending_total.remove(&key);
            } else {
                state.pending_total.insert(key, next);
            }
        }
        let mut touched = HashSet::new();
        for increment in increments {
            let key = (increment.category, increment.stat);
            touched.insert(key);
            if accepted {
                state.ground_increment(key, increment.amount);
            }
        }
        for key in touched {
            state.refresh_local(key);
        }
        if accepted {
            state.ground_revision = state.ground_revision.wrapping_add(1);
        }
        state.local_revision = state.local_revision.wrapping_add(1);
        self.dirty.insert(actor);
        Ok((state.local_revision, state.ground_revision))
    }

    pub fn has_pending(&self, actor: GlobalPlayerId) -> bool {
        self.actors
            .get(&actor)
            .is_some_and(|state| !state.pending.is_empty())
    }

    pub fn apply_accepted(
        &mut self,
        actor: GlobalPlayerId,
        tick: TickStamp,
        key: ActionKey,
        expected_revision: u64,
        mutations: &[StatisticMutation],
    ) -> Result<u64, AcceptedStatisticError> {
        let state = self.actors.get_mut(&actor).ok_or(AcceptedStatisticError {
            tick,
            key,
            failure: AcceptedStatisticFailure::UnknownActor,
        })?;
        if state.ground_revision != expected_revision {
            return Err(AcceptedStatisticError {
                tick,
                key,
                failure: AcceptedStatisticFailure::RevisionMismatch {
                    expected: expected_revision,
                    actual: state.ground_revision,
                },
            });
        }
        if mutations.is_empty()
            || mutations.len() > 32
            || mutations.iter().any(|mutation| matches!(mutation, StatisticMutation::Increment { amount, .. } if *amount <= 0))
        {
            return Err(AcceptedStatisticError {
                tick,
                key,
                failure: AcceptedStatisticFailure::InvalidBatch,
            });
        }
        let mut touched = HashSet::new();
        for mutation in mutations {
            match *mutation {
                StatisticMutation::Increment {
                    category,
                    stat,
                    amount,
                } => {
                    let value = state.ground_values.entry((category, stat)).or_default();
                    *value = value.saturating_add(amount);
                    touched.insert((category, stat));
                }
                StatisticMutation::Set {
                    category,
                    stat,
                    value,
                } => {
                    state.ground_values.insert((category, stat), value);
                    touched.insert((category, stat));
                }
            }
        }
        for key in touched {
            state.refresh_local(key);
        }
        state.ground_revision = state.ground_revision.wrapping_add(1);
        state.local_revision = state.local_revision.wrapping_add(1);
        self.dirty.insert(actor);
        Ok(state.ground_revision)
    }

    pub fn apply_tick(
        &mut self,
        actor: GlobalPlayerId,
        tick: TickStamp,
        evidence: StatisticTickEvidence,
    ) -> bool {
        let Some(state) = self.actors.get_mut(&actor) else {
            return false;
        };
        if state
            .last_infallible_tick
            .is_some_and(|last| !tick.is_after(last))
        {
            return false;
        }
        let mut increment = |stat: pumpkin_data::statistic::CustomStatistic| {
            state.ground_increment(
                (
                    pumpkin_data::statistic::StatisticCategory::Custom as i32,
                    stat as i32,
                ),
                1,
            );
        };
        increment(pumpkin_data::statistic::CustomStatistic::PlayTime);
        increment(pumpkin_data::statistic::CustomStatistic::TotalWorldTime);
        if evidence.alive {
            increment(pumpkin_data::statistic::CustomStatistic::TimeSinceDeath);
        }
        if !evidence.sleeping {
            increment(pumpkin_data::statistic::CustomStatistic::TimeSinceRest);
        }
        if evidence.sneaking {
            increment(pumpkin_data::statistic::CustomStatistic::SneakTime);
        }
        state.last_infallible_tick = Some(tick);
        state.ground_revision = state.ground_revision.wrapping_add(1);
        state.local_revision = state.local_revision.wrapping_add(1);
        self.dirty.insert(actor);
        true
    }

    pub fn apply(&mut self, delta: StatisticDelta) -> bool {
        let Some(state) = self.actors.get_mut(&delta.actor) else {
            return false;
        };
        if delta.source == StatisticSource::LeaveGame {
            if delta.category != pumpkin_data::statistic::StatisticCategory::Custom as i32
                || delta.stat != pumpkin_data::statistic::CustomStatistic::LeaveGame as i32
                || delta.amount != 1
            {
                return false;
            }
            if state.leave_game_recorded {
                return false;
            }
            state.leave_game_recorded = true;
        }
        state.ground_increment((delta.category, delta.stat), delta.amount);
        state.ground_revision = state.ground_revision.wrapping_add(1);
        state.local_revision = state.local_revision.wrapping_add(1);
        self.dirty.insert(delta.actor);
        true
    }

    pub fn publish(&mut self) {
        if self.dirty.is_empty() {
            return;
        }
        let mut published = self.published.load().as_ref().clone();
        for actor in self.dirty.drain() {
            match self.actors.get(&actor) {
                Some(state) => {
                    published.insert(
                        actor,
                        Arc::new(ActorStatisticsSnapshot {
                            incarnation: state.incarnation,
                            revision: state.local_revision,
                            values: Arc::new(state.local_values.clone()),
                            ground_revision: state.ground_revision,
                            ground_values: Arc::new(state.ground_values.clone()),
                            leave_game_recorded: state.leave_game_recorded,
                        }),
                    );
                }
                None => {
                    published.remove(&actor);
                }
            }
        }
        self.published.store(Arc::new(published));
    }

    pub fn snapshot(&self, actor: GlobalPlayerId) -> Option<Arc<ActorStatisticsSnapshot>> {
        self.published.load().get(&actor).cloned()
    }

    pub fn current_revision(&self, actor: GlobalPlayerId) -> Option<u64> {
        self.actors.get(&actor).map(|state| state.ground_revision)
    }

    pub fn incarnation(&self, actor: GlobalPlayerId) -> Option<u64> {
        self.actors.get(&actor).map(|state| state.incarnation)
    }

    pub fn leave_game_status(&self, actor: GlobalPlayerId) -> Option<bool> {
        self.actors
            .get(&actor)
            .map(|state| state.leave_game_recorded)
    }

    pub fn write_nbt(&self, actor: GlobalPlayerId, nbt: &mut NbtCompound) -> bool {
        let Some(state) = self.actors.get(&actor) else {
            return false;
        };
        Statistics {
            stats: state.ground_values.clone(),
        }
        .write_nbt(nbt);
        true
    }

    pub fn remove(&mut self, actor: GlobalPlayerId) -> bool {
        if self.actors.remove(&actor).is_none() {
            return false;
        }
        self.dirty.insert(actor);
        true
    }

    pub fn retire(&mut self, retire: ActorStatisticsRetire) -> bool {
        if self
            .actors
            .get(&retire.actor)
            .is_none_or(|state| state.incarnation != retire.incarnation)
        {
            return false;
        }
        self.remove(retire.actor)
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};
    use pumpkin_data::statistic::{CustomStatistic, StatisticCategory};

    use super::*;

    #[test]
    fn losing_one_optimistic_increment_preserves_other_local_and_ground_changes() {
        let actor = GlobalPlayerId::new(ServerId(4), PlayerSlot(11));
        let key_a = ActionKey {
            incarnation: 0,
            actor: ActorId::Player(actor),
            sequence: 1,
            family: 4,
        };
        let key_b = ActionKey {
            incarnation: 0,
            actor: ActorId::Player(actor),
            sequence: 2,
            family: 4,
        };
        let key_remote = ActionKey {
            incarnation: 0,
            actor: ActorId::Peer(ServerId(5)),
            sequence: 3,
            family: 4,
        };
        let stat = CustomStatistic::PlayTime as i32;
        let category = StatisticCategory::Custom as i32;
        let mut initial = FxHashMap::default();
        initial.insert((category, stat), 5);
        let mut owner = OwnedActorStatistics::default();
        assert!(owner.import(ActorStatisticsImport {
            actor,
            values: initial
        }));
        let a = owner
            .preview_item_mutations(
                actor,
                TickStamp(50),
                key_a,
                &[StatisticMutation::Increment {
                    category,
                    stat,
                    amount: 3,
                }],
            )
            .unwrap();
        assert_eq!(a.before, vec![((category, stat), 5)]);
        assert_eq!(a.after, vec![((category, stat), 8)]);
        owner.commit_prepared_item_mutation(a).unwrap();
        let b = owner
            .preview_item_mutations(
                actor,
                TickStamp(50),
                key_b,
                &[StatisticMutation::Increment {
                    category,
                    stat,
                    amount: 4,
                }],
            )
            .unwrap();
        owner.commit_prepared_item_mutation(b).unwrap();
        owner.publish();
        let snapshot = owner.snapshot(actor).unwrap();
        assert_eq!(snapshot.values.get(&(category, stat)), Some(&12));
        assert_eq!(snapshot.ground_values.get(&(category, stat)), Some(&5));
        owner
            .settle_item_mutation(
                actor,
                TickStamp(50),
                key_remote,
                true,
                &[StatisticIncrement {
                    category,
                    stat,
                    amount: 2,
                }],
            )
            .unwrap();
        owner
            .settle_item_mutation(
                actor,
                TickStamp(50),
                key_a,
                false,
                &[StatisticIncrement {
                    category,
                    stat,
                    amount: 3,
                }],
            )
            .unwrap();
        owner
            .settle_item_mutation(
                actor,
                TickStamp(50),
                key_b,
                true,
                &[StatisticIncrement {
                    category,
                    stat,
                    amount: 4,
                }],
            )
            .unwrap();
        owner.publish();
        let snapshot = owner.snapshot(actor).unwrap();
        assert_eq!(snapshot.values.get(&(category, stat)), Some(&11));
        assert_eq!(snapshot.ground_values.get(&(category, stat)), Some(&11));
        assert!(!owner.has_pending(actor));
        let mut persisted = NbtCompound::new();
        snapshot.write_nbt(&mut persisted);
        let mut loaded = Statistics::default();
        loaded.read_nbt(&persisted);
        assert_eq!(loaded.get(StatisticCategory::Custom, stat), 11);
    }

    #[test]
    fn tick_statistics_wrap_once_and_follow_living_evidence() {
        let actor = GlobalPlayerId::new(ServerId(3), PlayerSlot(9));
        let mut owner = OwnedActorStatistics::default();
        assert!(owner.import(ActorStatisticsImport::from_nbt(actor, None)));
        assert!(owner.apply_tick(
            actor,
            TickStamp(u16::MAX),
            StatisticTickEvidence {
                alive: true,
                sleeping: false,
                sneaking: false,
            },
        ));
        assert!(owner.apply_tick(
            actor,
            TickStamp(0),
            StatisticTickEvidence {
                alive: false,
                sleeping: true,
                sneaking: true,
            },
        ));
        assert!(!owner.apply_tick(
            actor,
            TickStamp(0),
            StatisticTickEvidence {
                alive: true,
                sleeping: false,
                sneaking: true,
            },
        ));
        owner.publish();
        let values = &owner.snapshot(actor).unwrap().values;
        let get = |stat| {
            values
                .get(&(StatisticCategory::Custom as i32, stat as i32))
                .copied()
                .unwrap_or(0)
        };
        assert_eq!(get(CustomStatistic::PlayTime), 2);
        assert_eq!(get(CustomStatistic::TotalWorldTime), 2);
        assert_eq!(get(CustomStatistic::TimeSinceDeath), 1);
        assert_eq!(get(CustomStatistic::TimeSinceRest), 1);
        assert_eq!(get(CustomStatistic::SneakTime), 1);
    }

    #[test]
    fn accepted_statistics_apply_as_one_revision_and_reject_stale_replay() {
        let actor = GlobalPlayerId::new(ServerId(3), PlayerSlot(8));
        let key = ActionKey {
            incarnation: 0,
            actor: ActorId::Player(actor),
            sequence: 19,
            family: 1,
        };
        let mut owner = OwnedActorStatistics::default();
        assert!(owner.import(ActorStatisticsImport {
            actor,
            values: FxHashMap::default(),
        }));
        let mutations = [
            StatisticMutation::Increment {
                category: StatisticCategory::Custom as i32,
                stat: CustomStatistic::PlayTime as i32,
                amount: 3,
            },
            StatisticMutation::Set {
                category: StatisticCategory::Custom as i32,
                stat: CustomStatistic::TimeSinceDeath as i32,
                value: 0,
            },
        ];
        assert_eq!(
            owner.apply_accepted(actor, TickStamp(42), key, 0, &mutations),
            Ok(1)
        );
        assert_eq!(
            owner.apply_accepted(actor, TickStamp(42), key, 0, &mutations),
            Err(AcceptedStatisticError {
                tick: TickStamp(42),
                key,
                failure: AcceptedStatisticFailure::RevisionMismatch {
                    expected: 0,
                    actual: 1,
                },
            })
        );
        owner.publish();
        let snapshot = owner.snapshot(actor).unwrap();
        assert_eq!(snapshot.revision, 1);
        assert_eq!(
            snapshot.values.get(&(
                StatisticCategory::Custom as i32,
                CustomStatistic::PlayTime as i32
            )),
            Some(&3)
        );
        assert_eq!(
            snapshot.values.get(&(
                StatisticCategory::Custom as i32,
                CustomStatistic::TimeSinceDeath as i32
            )),
            Some(&0)
        );
    }

    #[test]
    fn logout_stat_survives_snapshot_and_is_recorded_once() {
        let actor = GlobalPlayerId::new(ServerId(3), PlayerSlot(7));
        let mut original = NbtCompound::new();
        let mut stats = Statistics::default();
        stats.increment_custom(CustomStatistic::LeaveGame, 4);
        stats.write_nbt(&mut original);

        let mut owner = OwnedActorStatistics::default();
        assert!(owner.import_nbt(actor, &original));
        owner.publish();
        let before = owner.snapshot(actor).unwrap();
        assert_eq!(before.revision, 0);
        assert!(owner.apply(StatisticDelta {
            actor,
            category: StatisticCategory::Custom as i32,
            stat: CustomStatistic::LeaveGame as i32,
            amount: 1,
            source: StatisticSource::LeaveGame,
        }));
        assert!(!owner.apply(StatisticDelta {
            actor,
            category: StatisticCategory::Custom as i32,
            stat: CustomStatistic::LeaveGame as i32,
            amount: 1,
            source: StatisticSource::LeaveGame,
        }));
        owner.publish();
        let after = owner.snapshot(actor).unwrap();
        assert_eq!(
            before.values.get(&(
                StatisticCategory::Custom as i32,
                CustomStatistic::LeaveGame as i32
            )),
            Some(&4)
        );
        assert_eq!(
            after.values.get(&(
                StatisticCategory::Custom as i32,
                CustomStatistic::LeaveGame as i32
            )),
            Some(&5)
        );
        assert_eq!(after.revision, 1);

        let mut persisted = NbtCompound::new();
        assert!(owner.write_nbt(actor, &mut persisted));
        let mut loaded = Statistics::default();
        loaded.read_nbt(&persisted);
        assert_eq!(
            loaded.get(StatisticCategory::Custom, CustomStatistic::LeaveGame as i32),
            5
        );
    }

    #[test]
    fn delayed_retire_cannot_remove_reused_player_slot() {
        let actor = GlobalPlayerId::new(ServerId(3), PlayerSlot(7));
        let mut owner = OwnedActorStatistics::default();
        assert!(owner.import(ActorStatisticsImport::from_nbt(actor, None)));
        owner.publish();
        let old = owner.snapshot(actor).unwrap().incarnation;
        assert!(owner.retire(ActorStatisticsRetire {
            actor,
            player_key: uuid::Uuid::nil(),
            incarnation: old,
            install_generation: 1,
        }));
        assert!(owner.import(ActorStatisticsImport::from_nbt(actor, None)));
        owner.publish();
        let newer = owner.snapshot(actor).unwrap().incarnation;
        assert_ne!(old, newer);
        assert!(!owner.retire(ActorStatisticsRetire {
            actor,
            player_key: uuid::Uuid::nil(),
            incarnation: old,
            install_generation: 1,
        }));
        assert_eq!(owner.snapshot(actor).unwrap().incarnation, newer);
    }
}
