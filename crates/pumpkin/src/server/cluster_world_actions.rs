use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use pumpkin_cluster::completion::{
    ActionKey, ChunkPendingTick, CompletionLedger, DropReport, FinishResult, RequirementResult,
    SourcedTickBatch, TickFinish,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::order::order_actors;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId, PlayerChunkTransition, TickBatch};
use pumpkin_cluster::time::TickStamp;

use super::cluster_world_apply::CLUSTER_SEED;

const MAX_PENDING_CANONICAL_ADMISSION: usize = 4096;

pub struct ReadyPromotion {
    pub tick: TickStamp,
    pub chunks: BTreeSet<ChunkKey>,
    pub actions: BTreeSet<ActionKey>,
    pub holders: BTreeSet<ServerId>,
    pub batches: Vec<SourcedTickBatch>,
}

#[derive(Clone, Default)]
pub struct WorldTickBuckets {
    pending: BTreeMap<TickStamp, Vec<SourcedTickBatch>>,
    completion: CompletionLedger,
    seen: HashSet<(TickStamp, ServerId, ActionKey)>,
    promoted: HashSet<(TickStamp, ActionKey)>,
    resolved: HashSet<TickStamp>,
    resolved_order: VecDeque<TickStamp>,
    departures: HashMap<GlobalPlayerId, PlayerChunkTransition>,
    invalid: HashSet<(ServerId, TickStamp)>,
    captured_holders: HashMap<(TickStamp, ChunkKey), Vec<ServerId>>,
    settlement_blocked: HashSet<(TickStamp, ActionKey)>,
    observer_chunks: HashSet<(TickStamp, ChunkKey)>,
    quarantined_chunks: HashSet<ChunkKey>,
    admitted_canonical: Vec<(ServerId, pumpkin_cluster::canonical_action::Action)>,
    authenticated_generations: HashMap<(TickStamp, ServerId), u64>,
    authenticated_finishes: HashMap<(TickStamp, ServerId), TickFinish>,
}

impl WorldTickBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_batch(
        &mut self,
        source: ServerId,
        local: ServerId,
        batch: TickBatch,
        holders: impl Fn(ChunkKey) -> Vec<u16> + Copy,
    ) {
        self.push_batch_with_entity_owners(source, local, batch, holders, |_| None);
    }

    pub fn push_authenticated_batch(
        &mut self,
        source: ServerId,
        generation: u64,
        local: ServerId,
        batch: TickBatch,
        holders: impl Fn(ChunkKey) -> Vec<u16> + Copy,
        current_owner: impl Fn(GlobalEntityId) -> Option<ServerId> + Copy,
    ) -> Vec<pumpkin_cluster::canonical_action::Action> {
        if !self.bind_generation(batch.tick, source, generation) {
            self.invalid.insert((source, batch.tick));
            return Vec::new();
        }
        if self
            .authenticated_finishes
            .get(&(batch.tick, source))
            .is_some_and(|finish| !batch_declared_in_finish(&batch, finish))
        {
            self.invalid.insert((source, batch.tick));
            return Vec::new();
        }
        self.push_batch_with_entity_owners(source, local, batch, holders, current_owner)
    }

    pub fn note_authenticated_finish(
        &mut self,
        source: ServerId,
        generation: u64,
        local: ServerId,
        finish: TickFinish,
        holders: impl Fn(ChunkKey) -> Vec<u16>,
        current_owner: impl Fn(GlobalEntityId) -> Option<ServerId>,
    ) -> bool {
        if !self.bind_generation(finish.tick, source, generation) {
            self.invalid.insert((source, finish.tick));
            return false;
        }
        match self.authenticated_finishes.get(&(finish.tick, source)) {
            Some(existing) if existing != &finish => {
                self.invalid.insert((source, finish.tick));
                return false;
            }
            Some(_) => {}
            None => {
                if self.pending.get(&finish.tick).is_some_and(|batches| {
                    batches
                        .iter()
                        .filter(|batch| batch.source == source)
                        .any(|batch| !batch_declared_in_finish(&batch.batch, &finish))
                }) {
                    self.invalid.insert((source, finish.tick));
                    return false;
                }
                self.authenticated_finishes
                    .insert((finish.tick, source), finish.clone());
            }
        }
        self.note_finish_with_entity_owners(source, local, finish, holders, current_owner);
        true
    }

    fn bind_generation(&mut self, tick: TickStamp, source: ServerId, generation: u64) -> bool {
        match self.authenticated_generations.entry((tick, source)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(generation);
                true
            }
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get() == generation,
        }
    }

    pub fn legacy_chunk_closed(
        &self,
        tick: TickStamp,
        chunk: ChunkKey,
        producers: &BTreeMap<ServerId, u64>,
    ) -> bool {
        if producers.is_empty() {
            return false;
        }
        let mut named = false;
        for (source, generation) in producers {
            if self.invalid.contains(&(*source, tick)) {
                return false;
            }
            if self.authenticated_generations.get(&(tick, *source)) != Some(generation) {
                return false;
            }
            let Some(finish) = self.authenticated_finishes.get(&(tick, *source)) else {
                return false;
            };
            named |= finish
                .chunks
                .iter()
                .any(|entry| entry.chunk == chunk && !entry.actions.is_empty());
        }
        !named || self.completion.chunk_complete(tick, chunk) || self.resolved.contains(&tick)
    }

    pub fn retire_authenticated_tick(&mut self, tick: TickStamp) {
        self.authenticated_generations
            .retain(|(entry, _), _| *entry != tick);
        self.authenticated_finishes
            .retain(|(entry, _), _| *entry != tick);
    }

    pub fn push_batch_with_entity_owners(
        &mut self,
        source: ServerId,
        local: ServerId,
        mut batch: TickBatch,
        holders: impl Fn(ChunkKey) -> Vec<u16> + Copy,
        current_owner: impl Fn(GlobalEntityId) -> Option<ServerId> + Copy,
    ) -> Vec<pumpkin_cluster::canonical_action::Action> {
        let tick = batch.tick;
        if self.resolved.contains(&tick) {
            return Vec::new();
        }
        for transition in batch.player_chunk_transition.clone() {
            self.relocate_pending(transition);
            self.departures.insert(transition.gid, transition);
        }
        for transition in self.departures.values().copied().collect::<Vec<_>>() {
            relocate_player_references(&mut batch, transition);
        }
        let Ok(manifest) = TickFinish::try_from_batch(&batch) else {
            self.invalid.insert((source, tick));
            return Vec::new();
        };
        let requirements = joint_requirements(&manifest, holders);
        let mut relevant = BTreeSet::new();
        for entry in &manifest.chunks {
            let required = self
                .captured_holders
                .get(&(tick, entry.chunk))
                .cloned()
                .or_else(|| requirements.get(&entry.chunk).cloned())
                .unwrap_or_default();
            if !required.contains(&local) && !self.observer_chunks.contains(&(tick, entry.chunk)) {
                continue;
            }
            if !required.contains(&source)
                || self.completion.require(tick, entry.chunk, &required)
                    == RequirementResult::RecordedWithUnexpectedSources
            {
                self.invalid.insert((source, tick));
                return Vec::new();
            }
            self.captured_holders
                .entry((tick, entry.chunk))
                .or_insert_with(|| required.clone());
            relevant.extend(entry.actions.iter().copied());
        }
        if relevant.is_empty() {
            return Vec::new();
        }
        retain_actions(&mut batch, |action| relevant.contains(&action));
        if self.admitted_canonical.len() + batch.canonical_actions.len()
            > MAX_PENDING_CANONICAL_ADMISSION
        {
            self.invalid.insert((source, tick));
            return Vec::new();
        }
        if self
            .completion
            .note_batch_with_entity_owners(source, &batch, current_owner)
            .is_err()
        {
            self.invalid.insert((source, tick));
            return Vec::new();
        }
        retain_actions(&mut batch, |action| {
            self.seen.insert((tick, source, action))
        });
        let accepted = batch.canonical_actions.clone();
        self.admitted_canonical
            .extend(accepted.iter().cloned().map(|action| (source, action)));
        if !batch.is_empty() {
            self.pending
                .entry(tick)
                .or_default()
                .push(SourcedTickBatch { source, batch });
        }
        accepted
    }

    pub fn drain_admitted_canonical_matching(
        &mut self,
        mut ready: impl FnMut(TickStamp) -> bool,
    ) -> Vec<(ServerId, pumpkin_cluster::canonical_action::Action)> {
        self.drain_admitted_canonical_actions_matching(|_, action| ready(action.tick))
    }

    pub fn drain_admitted_canonical_actions_matching(
        &mut self,
        mut ready: impl FnMut(ServerId, &pumpkin_cluster::canonical_action::Action) -> bool,
    ) -> Vec<(ServerId, pumpkin_cluster::canonical_action::Action)> {
        let mut selected = Vec::new();
        self.admitted_canonical.retain(|(source, action)| {
            if ready(*source, action) {
                selected.push((*source, action.clone()));
                false
            } else {
                true
            }
        });
        selected
    }

    pub fn can_admit_canonical(&self, count: usize) -> bool {
        count <= MAX_PENDING_CANONICAL_ADMISSION.saturating_sub(self.admitted_canonical.len())
    }

    pub fn pending_legacy_chunks(&self) -> HashSet<ChunkKey> {
        self.pending
            .values()
            .flatten()
            .flat_map(|sourced| {
                let mut legacy = sourced.batch.clone();
                legacy.canonical_actions.clear();
                if legacy.is_empty() {
                    return Vec::new();
                }
                TickFinish::try_from_batch(&legacy)
                    .map(|finish| finish.chunks.into_iter().map(|entry| entry.chunk).collect())
                    .unwrap_or_default()
            })
            .collect()
    }

    pub fn chunk_manifest_complete(&self, tick: TickStamp, chunk: ChunkKey) -> bool {
        self.completion.chunk_complete(tick, chunk) || self.resolved.contains(&tick)
    }

    pub fn note_finish(
        &mut self,
        source: ServerId,
        local: ServerId,
        finish: TickFinish,
        holders: impl Fn(ChunkKey) -> Vec<u16>,
    ) {
        self.note_finish_with_entity_owners(source, local, finish, holders, |_| None);
    }

    pub fn note_finish_with_entity_owners(
        &mut self,
        source: ServerId,
        local: ServerId,
        mut finish: TickFinish,
        holders: impl Fn(ChunkKey) -> Vec<u16>,
        current_owner: impl Fn(GlobalEntityId) -> Option<ServerId>,
    ) {
        if self.resolved.contains(&finish.tick) {
            return;
        }
        let requirements = joint_requirements(&finish, holders);
        finish.chunks.retain(|entry| {
            let required = self
                .captured_holders
                .get(&(finish.tick, entry.chunk))
                .cloned()
                .or_else(|| requirements.get(&entry.chunk).cloned())
                .unwrap_or_default();
            (required.contains(&local)
                || self.observer_chunks.contains(&(finish.tick, entry.chunk)))
                && required.contains(&source)
                && {
                    self.captured_holders
                        .entry((finish.tick, entry.chunk))
                        .or_insert_with(|| required.clone());
                    self.completion.require(finish.tick, entry.chunk, &required)
                        != RequirementResult::RecordedWithUnexpectedSources
                }
        });
        let tick = finish.tick;
        if self
            .completion
            .note_finish_with_entity_owners(source, finish, current_owner)
            == FinishResult::Conflicting
        {
            self.invalid.insert((source, tick));
        }
    }

    pub fn recover_drop(&mut self, report: DropReport) {
        let dropped = report.dropped;
        let tracked: BTreeSet<_> = self.completion.tracked_chunks().into_iter().collect();
        for mut sourced in report.retained {
            let tick = sourced.batch.tick;
            if self
                .completion
                .note_batch(sourced.source, &sourced.batch)
                .is_err()
            {
                self.invalid.insert((sourced.source, tick));
                continue;
            }
            let relevant: BTreeSet<_> = TickFinish::from_batch(&sourced.batch)
                .chunks
                .into_iter()
                .filter(|entry| tracked.contains(&(tick, entry.chunk)))
                .flat_map(|entry| entry.actions)
                .collect();
            retain_actions(&mut sourced.batch, |action| {
                relevant.contains(&action) && self.seen.insert((tick, sourced.source, action))
            });
            if !sourced.batch.is_empty() {
                self.pending.entry(tick).or_default().push(sourced);
            }
        }
        self.completion.drop_peer(dropped);
    }

    pub fn block_settlement(&mut self, tick: TickStamp, action: ActionKey) {
        self.settlement_blocked.insert((tick, action));
    }

    pub fn release_settlement(&mut self, tick: TickStamp, action: ActionKey) {
        self.settlement_blocked.remove(&(tick, action));
    }

    #[must_use]
    pub fn settlement_ticks(&self) -> Vec<TickStamp> {
        self.settlement_blocked
            .iter()
            .map(|(tick, _)| *tick)
            .collect()
    }

    #[must_use]
    pub fn settlement_actions(&self, tick: TickStamp) -> Vec<ActionKey> {
        self.settlement_blocked
            .iter()
            .filter_map(|(entry_tick, action)| (*entry_tick == tick).then_some(*action))
            .collect()
    }

    #[must_use]
    pub fn export_pending_chunk(&self, chunk: ChunkKey) -> Vec<ChunkPendingTick> {
        self.completion
            .tracked_chunks()
            .into_iter()
            .filter_map(|(tick, tracked)| {
                if tracked != chunk {
                    return None;
                }
                let batches = self
                    .pending
                    .get(&tick)
                    .into_iter()
                    .flatten()
                    .filter(|sourced| {
                        TickFinish::from_batch(&sourced.batch)
                            .chunks
                            .iter()
                            .any(|entry| entry.chunk == chunk)
                    })
                    .cloned()
                    .collect();
                Some(ChunkPendingTick {
                    tick,
                    chunk,
                    holders: self.completion.holders_for(tick, chunk),
                    finishes: self.completion.known_finishes_for(tick),
                    batches,
                    generations: self
                        .authenticated_generations
                        .iter()
                        .filter_map(|((entry_tick, source), generation)| {
                            (*entry_tick == tick).then_some((*source, *generation))
                        })
                        .collect::<BTreeMap<_, _>>()
                        .into_iter()
                        .collect(),
                })
            })
            .collect()
    }

    pub fn import_pending_chunk(
        &mut self,
        local: ServerId,
        states: Vec<ChunkPendingTick>,
        current_owner: impl Fn(GlobalEntityId) -> Option<ServerId> + Copy,
    ) -> Result<(), ()> {
        let mut next = self.clone();
        for state in states {
            if state.holders.is_empty()
                || state.holders.windows(2).any(|pair| pair[0] >= pair[1])
                || state
                    .generations
                    .windows(2)
                    .any(|pair| pair[0].0 >= pair[1].0)
                || state
                    .generations
                    .iter()
                    .any(|(_, generation)| *generation == 0)
                || state
                    .finishes
                    .iter()
                    .any(|(source, _)| !state.generations.iter().any(|(peer, _)| peer == source))
                || state.batches.iter().any(|batch| {
                    !state
                        .generations
                        .iter()
                        .any(|(peer, _)| *peer == batch.source)
                })
                || state
                    .batches
                    .iter()
                    .any(|batch| batch.batch.tick != state.tick)
                || state
                    .finishes
                    .iter()
                    .any(|(_, finish)| finish.tick != state.tick)
                || next
                    .captured_holders
                    .get(&(state.tick, state.chunk))
                    .is_some_and(|known| known != &state.holders)
            {
                return Err(());
            }
            next.captured_holders
                .insert((state.tick, state.chunk), state.holders.clone());
            if next
                .completion
                .require(state.tick, state.chunk, &state.holders)
                == RequirementResult::RecordedWithUnexpectedSources
            {
                return Err(());
            }
            next.observer_chunks.insert((state.tick, state.chunk));
            for (source, finish) in state.finishes {
                let generation = state
                    .generations
                    .iter()
                    .find_map(|(peer, generation)| (*peer == source).then_some(*generation))
                    .ok_or(())?;
                if !next.note_authenticated_finish(
                    source,
                    generation,
                    local,
                    finish,
                    |_| Vec::new(),
                    current_owner,
                ) {
                    return Err(());
                }
                next.completion.mark_imported_finish(state.tick, source);
            }
            for sourced in state.batches {
                let generation = state
                    .generations
                    .iter()
                    .find_map(|(peer, generation)| (*peer == sourced.source).then_some(*generation))
                    .ok_or(())?;
                next.push_authenticated_batch(
                    sourced.source,
                    generation,
                    local,
                    sourced.batch,
                    |_| Vec::new(),
                    current_owner,
                );
            }
            if next.invalid.len() != self.invalid.len() {
                return Err(());
            }
        }
        *self = next;
        Ok(())
    }

    pub fn quarantine_chunk(&mut self, chunk: ChunkKey) {
        self.quarantined_chunks.insert(chunk);
    }

    pub fn is_quarantined(&self, chunk: ChunkKey) -> bool {
        self.quarantined_chunks.contains(&chunk)
    }

    pub fn ready_chunk(&mut self, chunk: ChunkKey) {
        self.quarantined_chunks.remove(&chunk);
    }

    #[must_use]
    pub fn settlement_complete_except_barrier(&self, tick: TickStamp, action: ActionKey) -> bool {
        if !self.settlement_blocked.contains(&(tick, action))
            || !self.pending.get(&tick).is_some_and(|batches| {
                batches.iter().any(|sourced| {
                    TickFinish::from_batch(&sourced.batch)
                        .chunks
                        .iter()
                        .any(|entry| entry.actions.contains(&action))
                })
            })
        {
            return false;
        }
        let chunks = self.completion.action_chunks(tick, action);
        if chunks.is_empty()
            || chunks.iter().any(|chunk| {
                self.quarantined_chunks.contains(chunk)
                    || !self.completion.chunk_complete(tick, *chunk)
                    || self
                        .completion
                        .tracked_chunks()
                        .iter()
                        .any(|(prior, prior_chunk)| *prior < tick && *prior_chunk == *chunk)
            })
        {
            return false;
        }
        !self
            .completion
            .tracked_chunks()
            .into_iter()
            .filter(|(entry_tick, chunk)| *entry_tick == tick && chunks.contains(chunk))
            .flat_map(|(_, chunk)| self.completion.chunk_actions(tick, chunk))
            .filter(|other| *other != action)
            .any(|other| {
                (self.settlement_blocked.contains(&(tick, other))
                    || self
                        .completion
                        .action_chunks(tick, other)
                        .iter()
                        .any(|chunk| !self.completion.chunk_complete(tick, *chunk)))
                    && action_priority(tick, other, action).is_lt()
            })
    }

    #[must_use]
    pub fn actor_unresolved_through(&self, gid: GlobalPlayerId, through_tick: TickStamp) -> bool {
        let actor = pumpkin_cluster::actor::ActorId::Player(gid);
        let within = |tick: TickStamp| through_tick.distance_since(tick) < 32_768;
        self.pending.iter().any(|(tick, batches)| {
            within(*tick)
                && batches.iter().any(|sourced| {
                    TickFinish::from_batch(&sourced.batch)
                        .chunks
                        .iter()
                        .flat_map(|chunk| &chunk.actions)
                        .any(|action| action.actor == actor)
                })
        }) || self
            .settlement_blocked
            .iter()
            .any(|(tick, action)| within(*tick) && action.actor == actor)
            || self
                .completion
                .tracked_chunks()
                .iter()
                .any(|(tick, chunk)| {
                    within(*tick)
                        && self
                            .completion
                            .chunk_actions(*tick, *chunk)
                            .iter()
                            .any(|action| {
                                action.actor == actor && !self.promoted.contains(&(*tick, *action))
                            })
                })
    }

    pub fn canonical_admission_unresolved_through(
        &self,
        gid: GlobalPlayerId,
        lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
        through_tick: TickStamp,
    ) -> bool {
        self.admitted_canonical.iter().any(|(_, action)| {
            through_tick.distance_since(action.tick) < 32_768
                && (action.id.actor == pumpkin_cluster::actor::ActorId::Player(gid)
                    || lease.is_some_and(|lease| action.player_leases.contains(&lease)))
        })
    }

    pub fn commit_canonical_ground_applied(
        &mut self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
    ) -> bool {
        if self.resolved.contains(&reference.tick) {
            return true;
        }
        let key = ActionKey {
            actor: reference.id.actor,
            incarnation: reference.id.incarnation,
            sequence: reference.id.sequence,
            family: reference.id.family | 0x80,
        };
        if self.promoted.contains(&(reference.tick, key)) {
            return true;
        }
        let chunks = self.completion.action_chunks(reference.tick, key);
        if chunks.is_empty()
            || chunks
                .iter()
                .any(|chunk| !self.completion.chunk_complete(reference.tick, *chunk))
        {
            return false;
        }
        let batches = self
            .pending
            .get(&reference.tick)
            .into_iter()
            .flatten()
            .filter_map(|sourced| {
                let mut selected = sourced.clone();
                retain_actions(&mut selected.batch, |action| action == key);
                (!selected.batch.is_empty()).then_some(selected)
            })
            .collect::<Vec<_>>();
        if batches.is_empty() {
            return false;
        }
        let holders = chunks
            .iter()
            .flat_map(|chunk| {
                self.captured_holders
                    .get(&(reference.tick, *chunk))
                    .into_iter()
                    .flatten()
                    .copied()
            })
            .collect();
        self.commit_ready(ReadyPromotion {
            tick: reference.tick,
            chunks: chunks.into_iter().collect(),
            actions: BTreeSet::from([key]),
            holders,
            batches,
        });
        true
    }

    pub fn pop_ready(&self) -> Option<ReadyPromotion> {
        let tracked = self.completion.tracked_chunks();
        for tick in self.pending.keys().copied().collect::<Vec<_>>() {
            let tick_chunks: Vec<_> = tracked
                .iter()
                .filter_map(|(entry_tick, chunk)| (*entry_tick == tick).then_some(*chunk))
                .collect();
            let all_actions: BTreeSet<_> = tick_chunks
                .iter()
                .flat_map(|chunk| self.completion.chunk_actions(tick, *chunk))
                .collect();
            let unresolved: BTreeSet<_> = all_actions
                .iter()
                .copied()
                .filter(|action| {
                    (action.family & 0x80 != 0 && !self.promoted.contains(&(tick, *action)))
                        || self.settlement_blocked.contains(&(tick, *action))
                        || self
                            .completion
                            .action_chunks(tick, *action)
                            .iter()
                            .any(|chunk| !self.completion.chunk_complete(tick, *chunk))
                })
                .collect();
            let mut ready = BTreeSet::new();
            for action in &all_actions {
                if action.family & 0x80 != 0 {
                    continue;
                }
                if self.promoted.contains(&(tick, *action))
                    || unresolved.contains(action)
                    || self.settlement_blocked.contains(&(tick, *action))
                {
                    continue;
                }
                let chunks = self.completion.action_chunks(tick, *action);
                if chunks.iter().any(|chunk| {
                    self.quarantined_chunks.contains(chunk)
                        || tracked
                            .iter()
                            .any(|(prior, prior_chunk)| *prior < tick && prior_chunk == chunk)
                }) {
                    continue;
                }
                let blocked = unresolved.iter().any(|other| {
                    let other_chunks = self.completion.action_chunks(tick, *other);
                    chunks.iter().any(|chunk| other_chunks.contains(chunk))
                        && (other.family & 0x80 != 0
                            || action.family == 8
                            || action_priority(tick, *other, *action).is_lt())
                });
                if !blocked {
                    ready.insert(*action);
                }
            }
            if ready.is_empty() {
                continue;
            }
            let mut batches = Vec::new();
            for mut sourced in self.pending.get(&tick)?.clone() {
                retain_actions(&mut sourced.batch, |action| ready.contains(&action));
                if !sourced.batch.is_empty() {
                    batches.push(sourced);
                }
            }
            if batches.is_empty() {
                continue;
            }
            let chunks: BTreeSet<_> = ready
                .iter()
                .flat_map(|action| self.completion.action_chunks(tick, *action))
                .collect();
            let holders = chunks
                .iter()
                .flat_map(|chunk| {
                    self.captured_holders
                        .get(&(tick, *chunk))
                        .into_iter()
                        .flatten()
                        .copied()
                })
                .collect();
            return Some(ReadyPromotion {
                tick,
                chunks,
                actions: ready,
                holders,
                batches,
            });
        }
        None
    }

    pub fn commit_ready(&mut self, ready: ReadyPromotion) {
        let ReadyPromotion {
            tick,
            chunks,
            actions,
            batches,
            ..
        } = ready;
        if let Some(pending) = self.pending.get_mut(&tick) {
            for sourced in pending.iter_mut() {
                retain_actions(&mut sourced.batch, |action| !actions.contains(&action));
            }
            pending.retain(|sourced| !sourced.batch.is_empty());
        }
        self.promoted
            .extend(actions.iter().map(|action| (tick, *action)));
        let removable: BTreeSet<_> = chunks
            .iter()
            .copied()
            .filter(|chunk| {
                self.completion.chunk_complete(tick, *chunk)
                    && self
                        .completion
                        .chunk_actions(tick, *chunk)
                        .iter()
                        .all(|action| self.promoted.contains(&(tick, *action)))
            })
            .collect();
        self.completion.remove_chunks(tick, &removable);
        for chunk in &removable {
            self.captured_holders.remove(&(tick, *chunk));
            self.observer_chunks.remove(&(tick, *chunk));
        }
        for batch in &batches {
            for transition in &batch.batch.player_chunk_transition {
                self.departures.remove(&transition.gid);
            }
        }
        if !self
            .completion
            .tracked_chunks()
            .iter()
            .any(|(entry_tick, _)| *entry_tick == tick)
            && self.pending.get(&tick).is_none_or(Vec::is_empty)
        {
            self.pending.remove(&tick);
            self.seen.retain(|(entry_tick, _, _)| *entry_tick != tick);
            self.promoted.retain(|(entry_tick, _)| *entry_tick != tick);
            self.settlement_blocked
                .retain(|(entry_tick, _)| *entry_tick != tick);
            if self.resolved.insert(tick) {
                self.resolved_order.push_back(tick);
            }
            if self.resolved_order.len() > 32_768
                && let Some(retired) = self.resolved_order.pop_front()
            {
                self.resolved.remove(&retired);
            }
        }
    }

    #[must_use]
    pub fn pending_chunks(&self) -> Vec<pumpkin_cluster::completion::PendingChunk> {
        self.completion.pending_chunks()
    }

    fn relocate_pending(&mut self, transition: PlayerChunkTransition) {
        for (tick, batches) in &mut self.pending {
            if !tick_at_or_after(*tick, transition.tick) {
                continue;
            }
            for sourced in batches {
                let before = action_chunks(&sourced.batch);
                relocate_player_references(&mut sourced.batch, transition);
                let after = action_chunks(&sourced.batch);
                for (action, old) in before {
                    if let Some(new) = after.get(&action)
                        && old != *new
                    {
                        self.completion.reindex_action(*tick, action, &old, new);
                    }
                }
            }
        }
    }
}

fn joint_requirements(
    finish: &TickFinish,
    holders: impl Fn(ChunkKey) -> Vec<u16>,
) -> BTreeMap<ChunkKey, Vec<ServerId>> {
    let mut action_chunks = BTreeMap::<ActionKey, BTreeSet<ChunkKey>>::new();
    for entry in &finish.chunks {
        for action in &entry.actions {
            action_chunks
                .entry(*action)
                .or_default()
                .insert(entry.chunk);
        }
    }
    let mut requirements = BTreeMap::<ChunkKey, BTreeSet<ServerId>>::new();
    for chunks in action_chunks.values() {
        let required: BTreeSet<_> = chunks
            .iter()
            .flat_map(|chunk| holders(*chunk))
            .map(ServerId)
            .collect();
        for chunk in chunks {
            requirements
                .entry(*chunk)
                .or_default()
                .extend(required.iter().copied());
        }
    }
    requirements
        .into_iter()
        .map(|(chunk, required)| (chunk, required.into_iter().collect()))
        .collect()
}

fn action_priority(tick: TickStamp, left: ActionKey, right: ActionKey) -> Ordering {
    order_actors(CLUSTER_SEED, tick, left.actor, right.actor)
        .then_with(|| left.sequence.cmp(&right.sequence))
        .then_with(|| left.family.cmp(&right.family))
}

fn tick_at_or_after(tick: TickStamp, boundary: TickStamp) -> bool {
    tick.distance_since(boundary) < (u16::MAX / 2)
}

fn batch_declared_in_finish(batch: &TickBatch, finish: &TickFinish) -> bool {
    if batch.tick != finish.tick {
        return false;
    }
    let declared = finish
        .chunks
        .iter()
        .flat_map(|entry| {
            entry
                .actions
                .iter()
                .map(move |action| (*action, entry.chunk))
        })
        .collect::<BTreeSet<_>>();
    action_chunks(batch).into_iter().all(|(action, chunks)| {
        chunks
            .into_iter()
            .all(|chunk| declared.contains(&(action, chunk)))
    })
}

fn action_chunks(batch: &TickBatch) -> BTreeMap<ActionKey, BTreeSet<ChunkKey>> {
    let mut result = BTreeMap::new();
    for entry in TickFinish::from_batch(batch).chunks {
        for action in entry.actions {
            result
                .entry(action)
                .or_insert_with(BTreeSet::new)
                .insert(entry.chunk);
        }
    }
    result
}

fn relocate_player_references(batch: &mut TickBatch, transition: PlayerChunkTransition) {
    if !tick_at_or_after(batch.tick, transition.tick) {
        return;
    }
    for update in &mut batch.break_block {
        if update.gid == transition.gid && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
        }
    }
    for update in &mut batch.place_block {
        if update.gid == transition.gid && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
        }
    }
    for update in &mut batch.inv_ops {
        if update.actor == transition.gid.into() && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
            update.affected.retain(|chunk| *chunk != transition.from);
            update.affected.push(transition.to);
            update.affected.sort_unstable();
            update.affected.dedup();
        }
    }
    for update in &mut batch.hit_player {
        if update.gid == transition.gid && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
        }
        if update.target == transition.gid && update.target_chunk == transition.from {
            update.target_chunk = transition.to;
        }
    }
    for update in &mut batch.hit_entity {
        if update.gid == transition.gid && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
        }
    }
    for update in &mut batch.fire {
        if update.gid == transition.gid && update.actor_chunk == transition.from {
            update.actor_chunk = transition.to;
        }
    }
}

fn retain_actions(batch: &mut TickBatch, mut retain: impl FnMut(ActionKey) -> bool) {
    batch
        .canonical_actions
        .retain(|action| retain(ActionKey::canonical(action)));
    batch
        .break_block
        .retain(|update| retain(ActionKey::break_block(update)));
    batch
        .place_block
        .retain(|update| retain(ActionKey::place_block(update)));
    batch
        .inv_ops
        .retain(|update| retain(ActionKey::inventory(update)));
    batch
        .hit_player
        .retain(|update| retain(ActionKey::hit_player(update)));
    batch
        .hit_entity
        .retain(|update| retain(ActionKey::hit_entity(update)));
    batch
        .fire
        .retain(|update| retain(ActionKey::fire_projectile(update)));
    batch
        .administrative
        .retain(|update| retain(ActionKey::administrative(update)));
    batch
        .player_administrative
        .retain(|update| retain(ActionKey::player_administrative(update)));
    batch
        .player_chunk_transition
        .retain(|update| retain(ActionKey::player_chunk_transition(update)));
    batch
        .entity_chunk_transition
        .retain(|update| retain(ActionKey::entity_chunk_transition(update)));
    batch
        .item_action
        .retain(|update| retain(ActionKey::item_action(update)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::{BlockPos, WorldId};
    use pumpkin_cluster::world_delta::{AdministrativeDelta, BlockDelta};

    fn chunk() -> ChunkKey {
        ChunkKey::new(WorldId::new(0, 1), 2, 3)
    }

    fn action(source: u16, sequence: u16) -> TickBatch {
        let mut batch = TickBatch::new(TickStamp(17));
        batch.administrative.push(AdministrativeDelta {
            holder: ServerId(source),
            sequence,
            chunk: chunk(),
            tick: batch.tick,
            edits: vec![BlockDelta {
                pos: BlockPos {
                    x: 32,
                    y: 70,
                    z: 48,
                },
                old_state: sequence,
                new_state: sequence.wrapping_add(1),
            }],
        });
        batch
    }

    fn canonical_action(sequence: u16) -> pumpkin_cluster::canonical_action::Action {
        use pumpkin_cluster::canonical_action::{
            Action, ActionClass, ActionId, ActionKind, ClientOrigin, Condition, Effect, Expected,
            Stakeholder, Target, UndoProcedure, Value,
        };
        let target = Target::Chunk {
            key: chunk(),
            part: pumpkin_cluster::canonical_action::ChunkPart::Block(BlockPos {
                x: 32,
                y: 70,
                z: 48,
            }),
            inventory: None,
        };
        Action {
            edit_authority: None,
            id: ActionId {
                actor: pumpkin_cluster::actor::ActorId::Peer(ServerId(1)),
                incarnation: 0,
                sequence,
                family: 0,
            },
            tick: TickStamp(17),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![Stakeholder::Chunk(chunk())],
            dependencies: Vec::new(),
            conditions: vec![Condition {
                target,
                expected: Expected::BlockState(0),
            }],
            effects: vec![Effect::Put {
                target,
                value: Value::Block(sequence),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Block,
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        }
    }

    #[test]
    fn actor_drain_covers_canonical_actions_waiting_for_graph_admission() {
        let gid = GlobalPlayerId::new(ServerId(1), pumpkin_cluster::identity::PlayerSlot(4));
        let lease = pumpkin_cluster::canonical_action::LivePlayerLease {
            key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_u128(44)),
            gid,
            incarnation: 7,
        };
        let mut action = canonical_action(8);
        action.id.actor = pumpkin_cluster::actor::ActorId::Player(gid);
        action.id.incarnation = lease.incarnation;
        action.actor_chunk = Some(chunk());
        action.validate().unwrap();
        let tick = action.tick;
        let mut buckets = WorldTickBuckets::new();
        buckets.admitted_canonical.push((ServerId(1), action));
        assert!(!buckets.actor_unresolved_through(gid, tick));
        assert!(buckets.canonical_admission_unresolved_through(gid, Some(lease), tick));
        assert!(!buckets.canonical_admission_unresolved_through(
            gid,
            Some(lease),
            TickStamp(tick.0 - 1)
        ));
        buckets.drain_admitted_canonical_matching(|_| true);
        assert!(!buckets.canonical_admission_unresolved_through(gid, Some(lease), tick));
        let target = pumpkin_cluster::canonical_action::Target::Player {
            key: lease.key,
            lease: None,
            inventory: None,
        };
        let mut action = canonical_action(9);
        action.player_leases = vec![lease];
        action.stakeholders = vec![target.stakeholder()];
        action.conditions = vec![pumpkin_cluster::canonical_action::Condition {
            target,
            expected: pumpkin_cluster::canonical_action::Expected::ScalarAtLeast(1),
        }];
        action.effects = vec![pumpkin_cluster::canonical_action::Effect::Put {
            target,
            value: pumpkin_cluster::canonical_action::Value::Scalar(2),
        }];
        action.kind = pumpkin_cluster::canonical_action::ActionKind::PlayerAdministration;
        action.validate().unwrap();
        buckets.admitted_canonical.push((ServerId(1), action));
        assert!(buckets.canonical_admission_unresolved_through(gid, Some(lease), tick));
        assert!(!buckets.canonical_admission_unresolved_through(gid, None, tick));
    }

    #[test]
    fn empty_legacy_tick_closes_only_after_every_frozen_generation_finishes() {
        let mut buckets = WorldTickBuckets::new();
        let tick = TickStamp(17);
        let producers = BTreeMap::from([(ServerId(1), 10), (ServerId(2), 20)]);
        let empty = TickFinish {
            tick,
            chunks: Vec::new(),
        };
        assert!(!buckets.legacy_chunk_closed(tick, chunk(), &producers));
        assert!(buckets.note_authenticated_finish(
            ServerId(1),
            10,
            ServerId(1),
            empty.clone(),
            |_| vec![1, 2],
            |_| None,
        ));
        assert!(!buckets.legacy_chunk_closed(tick, chunk(), &producers));
        assert!(buckets.note_authenticated_finish(
            ServerId(2),
            20,
            ServerId(1),
            empty,
            |_| vec![1, 2],
            |_| None,
        ));
        assert!(buckets.legacy_chunk_closed(tick, chunk(), &producers));
        assert!(!buckets.legacy_chunk_closed(
            tick,
            chunk(),
            &BTreeMap::from([(ServerId(1), 10), (ServerId(2), 21)]),
        ));
    }

    #[test]
    fn named_legacy_action_keeps_chunk_open_until_its_batch_arrives() {
        let mut buckets = WorldTickBuckets::new();
        let tick = TickStamp(17);
        let producers = BTreeMap::from([(ServerId(1), 10), (ServerId(2), 20)]);
        let batch = action(2, 1);
        let named = TickFinish::from_batch(&batch);
        assert!(buckets.note_authenticated_finish(
            ServerId(1),
            10,
            ServerId(1),
            TickFinish {
                tick,
                chunks: Vec::new(),
            },
            |_| vec![1, 2],
            |_| None,
        ));
        assert!(buckets.note_authenticated_finish(
            ServerId(2),
            20,
            ServerId(1),
            named,
            |_| vec![1, 2],
            |_| None,
        ));
        assert!(!buckets.legacy_chunk_closed(tick, chunk(), &producers));
        buckets.push_authenticated_batch(
            ServerId(2),
            20,
            ServerId(1),
            batch,
            |_| vec![1, 2],
            |_| None,
        );
        assert!(buckets.legacy_chunk_closed(tick, chunk(), &producers));
    }

    #[test]
    fn undeclared_legacy_batch_cannot_close_as_an_empty_tick() {
        let mut buckets = WorldTickBuckets::new();
        let tick = TickStamp(17);
        let producers = BTreeMap::from([(ServerId(1), 10)]);
        buckets.push_authenticated_batch(
            ServerId(1),
            10,
            ServerId(1),
            action(1, 1),
            |_| vec![1],
            |_| None,
        );
        assert!(!buckets.note_authenticated_finish(
            ServerId(1),
            10,
            ServerId(1),
            TickFinish {
                tick,
                chunks: Vec::new(),
            },
            |_| vec![1],
            |_| None,
        ));
        assert!(!buckets.legacy_chunk_closed(tick, chunk(), &producers));

        let mut finish_first = WorldTickBuckets::new();
        assert!(finish_first.note_authenticated_finish(
            ServerId(1),
            10,
            ServerId(1),
            TickFinish {
                tick,
                chunks: Vec::new(),
            },
            |_| vec![1],
            |_| None,
        ));
        finish_first.push_authenticated_batch(
            ServerId(1),
            10,
            ServerId(1),
            action(1, 1),
            |_| vec![1],
            |_| None,
        );
        assert!(!finish_first.legacy_chunk_closed(tick, chunk(), &producers));
    }

    #[test]
    fn canonical_batch_retires_only_after_graph_ground_marker() {
        let canonical = canonical_action(6);
        let mut batch = TickBatch::new(canonical.tick);
        batch.canonical_actions.push(canonical.clone());
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut buckets = WorldTickBuckets::new();
        buckets.push_authenticated_batch(
            ServerId(1),
            10,
            ServerId(1),
            batch,
            |_| vec![1],
            |_| None,
        );
        assert!(buckets.note_authenticated_finish(
            ServerId(1),
            10,
            ServerId(1),
            finish,
            |_| vec![1],
            |_| None,
        ));
        assert!(buckets.pop_ready().is_none());
        assert!(buckets.commit_canonical_ground_applied(
            pumpkin_cluster::canonical_action::ActionRef {
                tick: canonical.tick,
                id: canonical.id,
            },
        ));
        assert!(buckets.pop_ready().is_none());
    }

    fn holders(_: ChunkKey) -> Vec<u16> {
        vec![1, 2, 3]
    }

    #[test]
    fn graph_intake_receives_only_actions_admitted_by_tick_buckets() {
        let canonical = canonical_action(5);
        let mut batch = TickBatch::new(canonical.tick);
        batch.canonical_actions.push(canonical.clone());
        let mut buckets = WorldTickBuckets::new();
        let accepted = buckets.push_batch_with_entity_owners(
            ServerId(1),
            ServerId(2),
            batch.clone(),
            holders,
            |_| None,
        );
        assert_eq!(accepted, vec![canonical.clone()]);
        assert!(
            buckets
                .push_batch_with_entity_owners(ServerId(1), ServerId(2), batch, holders, |_| None)
                .is_empty()
        );
        assert_eq!(
            buckets.drain_admitted_canonical_matching(|_| true),
            vec![(ServerId(1), canonical)]
        );
        assert!(
            buckets
                .drain_admitted_canonical_matching(|_| true)
                .is_empty()
        );
    }

    #[test]
    fn future_action_survives_until_its_owner_tick_scope_exists() {
        let canonical = canonical_action(7);
        let mut batch = TickBatch::new(canonical.tick);
        batch.canonical_actions.push(canonical.clone());
        let mut buckets = WorldTickBuckets::new();
        buckets.push_authenticated_batch(ServerId(1), 10, ServerId(2), batch, holders, |_| None);
        assert!(
            buckets
                .drain_admitted_canonical_matching(|tick| TickStamp(16).is_after(tick))
                .is_empty()
        );
        assert_eq!(
            buckets.drain_admitted_canonical_matching(|tick| tick == TickStamp(17)),
            vec![(ServerId(1), canonical)]
        );
    }

    #[test]
    fn actor_drain_waits_for_missing_holder_and_releases_after_drop_recovery() {
        let gid = pumpkin_cluster::identity::GlobalPlayerId::new(
            ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(4),
        );
        let tick = TickStamp(25);
        let mut batch = TickBatch::new(tick);
        batch
            .break_block
            .push(pumpkin_cluster::break_emit::capture_break(
                gid,
                pumpkin_cluster::identity::PlayerSeq(1),
                tick,
                BlockPos {
                    x: 32,
                    y: 70,
                    z: 48,
                },
                2,
                chunk(),
                chunk(),
            ));
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let holders = |_: ChunkKey| vec![1, 2];
        let mut buckets = WorldTickBuckets::new();
        buckets.push_batch(ServerId(1), ServerId(1), batch, holders);
        buckets.note_finish(ServerId(1), ServerId(1), finish, holders);
        assert!(buckets.actor_unresolved_through(gid, tick));
        assert!(buckets.pop_ready().is_none());
        buckets.recover_drop(DropReport {
            dropped: ServerId(2),
            gaps: Vec::new(),
            retained: Vec::new(),
            complete: true,
        });
        let promoted = buckets.pop_ready().unwrap();
        assert_eq!(promoted.actions.len(), 1);
        assert!(buckets.actor_unresolved_through(gid, tick));
        buckets.commit_ready(promoted);
        assert!(!buckets.actor_unresolved_through(gid, tick));
    }

    #[test]
    fn settlement_stays_blocked_until_every_holder_finished_and_action_arrived() {
        let batch = action(1, 9);
        let key = ActionKey::administrative(&batch.administrative[0]);
        let tick = batch.tick;
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut buckets = WorldTickBuckets::new();
        buckets.block_settlement(tick, key);
        buckets.note_finish(ServerId(1), ServerId(1), finish, holders);
        for peer in [2, 3] {
            buckets.note_finish(
                ServerId(peer),
                ServerId(1),
                TickFinish {
                    tick,
                    chunks: Vec::new(),
                },
                holders,
            );
        }
        assert!(!buckets.settlement_complete_except_barrier(tick, key));
        buckets.push_batch(ServerId(1), ServerId(1), batch, holders);
        assert!(buckets.settlement_complete_except_barrier(tick, key));
        assert!(buckets.pop_ready().is_none());
        buckets.release_settlement(tick, key);
        assert_eq!(buckets.pop_ready().unwrap().actions, BTreeSet::from([key]));
    }

    #[test]
    fn late_holder_imports_pending_tick_as_observer_without_joining_old_quorum() {
        let holders = |_: ChunkKey| vec![1, 2];
        let batch = action(1, 6);
        let tick = batch.tick;
        let key = ActionKey::administrative(&batch.administrative[0]);
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut provider = WorldTickBuckets::new();
        provider.push_authenticated_batch(ServerId(1), 11, ServerId(1), batch, holders, |_| None);
        assert!(provider.note_authenticated_finish(
            ServerId(1),
            11,
            ServerId(1),
            finish,
            holders,
            |_| None,
        ));
        let exported = provider.export_pending_chunk(chunk());
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].holders, vec![ServerId(1), ServerId(2)]);
        let mut stale_generation = WorldTickBuckets::new();
        assert!(stale_generation.note_authenticated_finish(
            ServerId(1),
            12,
            ServerId(3),
            TickFinish {
                tick,
                chunks: Vec::new(),
            },
            |_| vec![1, 2, 3],
            |_| None,
        ));
        assert!(
            stale_generation
                .import_pending_chunk(ServerId(3), exported.clone(), |_| None)
                .is_err()
        );
        let mut observer = WorldTickBuckets::new();
        observer
            .import_pending_chunk(ServerId(3), exported, |_| None)
            .unwrap();
        observer.quarantine_chunk(chunk());
        assert!(observer.pop_ready().is_none());
        observer.note_finish(
            ServerId(2),
            ServerId(3),
            TickFinish {
                tick,
                chunks: Vec::new(),
            },
            |_| vec![1, 2, 3],
        );
        assert!(observer.pop_ready().is_none());
        observer.ready_chunk(chunk());
        assert_eq!(observer.pop_ready().unwrap().actions, BTreeSet::from([key]));
    }

    #[test]
    fn finish_and_action_delivery_order_converge() {
        let source = ServerId(1);
        let batch = action(1, 4);
        let finish = TickFinish::try_from_batch(&batch).expect("valid action");
        let single = |_: ChunkKey| vec![1];

        let mut finish_first = WorldTickBuckets::new();
        finish_first.note_finish(source, source, finish.clone(), single);
        assert!(finish_first.pop_ready().is_none());
        finish_first.push_batch(source, source, batch.clone(), single);

        let mut action_first = WorldTickBuckets::new();
        action_first.push_batch(source, source, batch, single);
        assert!(action_first.pop_ready().is_none());
        action_first.note_finish(source, source, finish, single);

        assert_eq!(
            finish_first.pop_ready().unwrap().actions,
            action_first.pop_ready().unwrap().actions
        );
    }

    #[test]
    fn legacy_bucket_retires_canonical_entries_after_owner_commit() {
        let source = ServerId(1);
        let mut batch = TickBatch::new(TickStamp(17));
        batch.canonical_actions = vec![canonical_action(1), canonical_action(2)];
        let actions = batch.canonical_actions.clone();
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut buckets = WorldTickBuckets::new();
        buckets.push_batch(source, source, batch, |_| vec![1]);
        buckets.note_finish(source, source, finish, |_| vec![1]);

        assert!(buckets.pop_ready().is_none());
        for action in actions {
            assert!(buckets.commit_canonical_ground_applied(
                pumpkin_cluster::canonical_action::ActionRef {
                    tick: action.tick,
                    id: action.id,
                },
            ));
        }
        assert!(buckets.pop_ready().is_none());
    }

    #[test]
    fn legacy_bucket_separates_canonical_entries_from_legacy_actions() {
        let source = ServerId(1);
        let mut batch = action(1, 3);
        batch.canonical_actions.push(canonical_action(4));
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut buckets = WorldTickBuckets::new();
        buckets.push_batch(source, source, batch, |_| vec![1]);
        buckets.note_finish(source, source, finish, |_| vec![1]);

        assert!(buckets.pop_ready().is_none());
        assert!(buckets.commit_canonical_ground_applied(
            pumpkin_cluster::canonical_action::ActionRef {
                tick: TickStamp(17),
                id: canonical_action(4).id,
            },
        ));
        let ready = buckets.pop_ready().unwrap();
        assert!(ready.batches[0].batch.canonical_actions.is_empty());
        assert_eq!(ready.batches[0].batch.administrative.len(), 1);
    }

    #[test]
    fn new_actions_survive_wrapping_tick_identity() {
        let source = ServerId(1);
        let mut buckets = WorldTickBuckets::new();
        for raw in 0..=u16::MAX {
            let mut batch = action(1, raw);
            batch.tick = TickStamp(raw);
            batch.administrative[0].tick = batch.tick;
            let finish = TickFinish::try_from_batch(&batch).unwrap();
            buckets.push_batch(source, source, batch, |_| vec![1]);
            buckets.note_finish(source, source, finish, |_| vec![1]);
            let ready = buckets.pop_ready().unwrap();
            buckets.commit_ready(ready);
        }
        let mut wrapped = action(1, 0);
        wrapped.tick = TickStamp(0);
        wrapped.administrative[0].tick = wrapped.tick;
        let finish = TickFinish::try_from_batch(&wrapped).unwrap();
        buckets.push_batch(source, source, wrapped, |_| vec![1]);
        buckets.note_finish(source, source, finish, |_| vec![1]);
        assert!(buckets.pop_ready().is_some());
    }

    #[test]
    fn partial_three_peer_views_settle_to_the_same_actions() {
        let batches = [action(1, 1), action(2, 2), action(3, 3)];
        let mut views = [
            WorldTickBuckets::new(),
            WorldTickBuckets::new(),
            WorldTickBuckets::new(),
        ];
        let orders = [[0, 1, 2], [1, 2, 0], [2, 0, 1]];

        for (view, order) in views.iter_mut().zip(orders) {
            for index in order.into_iter().take(2) {
                let source = ServerId((index + 1) as u16);
                view.push_batch(source, ServerId(1), batches[index].clone(), holders);
                view.note_finish(
                    source,
                    ServerId(1),
                    TickFinish::try_from_batch(&batches[index]).unwrap(),
                    holders,
                );
            }
            assert!(view.pop_ready().is_none());
            let index = order[2];
            let source = ServerId((index + 1) as u16);
            view.note_finish(
                source,
                ServerId(1),
                TickFinish::try_from_batch(&batches[index]).unwrap(),
                holders,
            );
            view.push_batch(source, ServerId(1), batches[index].clone(), holders);
        }

        let promoted: Vec<_> = views
            .iter_mut()
            .map(|view| view.pop_ready().unwrap().actions)
            .collect();
        assert!(promoted.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(promoted[0].len(), 3);
    }

    fn cross_chunk_priority(d_sequence: u16, local_sequence: u16) -> Option<ReadyPromotion> {
        let source = ServerId(1);
        let mut batch = action(1, d_sequence);
        batch
            .administrative
            .extend(action(1, local_sequence).administrative);
        let finish = TickFinish::try_from_batch(&batch).unwrap();
        let mut buckets = WorldTickBuckets::new();
        let single = |_: ChunkKey| vec![1];
        buckets.push_batch(source, source, batch, single);
        buckets.note_finish(source, source, finish, single);
        let d = ActionKey::administrative(&action(1, d_sequence).administrative[0]);
        let other = ChunkKey::new(WorldId::new(0, 1), 9, 3);
        buckets.completion.reindex_action(
            TickStamp(17),
            d,
            &BTreeSet::from([chunk()]),
            &BTreeSet::from([chunk(), other]),
        );
        buckets.completion.require(TickStamp(17), other, &[source]);
        buckets.block_settlement(TickStamp(17), d);
        buckets.pop_ready()
    }

    #[test]
    fn lower_priority_cross_chunk_action_does_not_block_settled_local_action() {
        let promotion = cross_chunk_priority(2, 1).unwrap();
        assert_eq!(promotion.actions.len(), 1);
        assert_eq!(promotion.actions.iter().next().unwrap().sequence, 1);
    }

    #[test]
    fn higher_priority_cross_chunk_action_prevents_premature_promotion() {
        assert!(cross_chunk_priority(1, 2).is_none());
    }
}
