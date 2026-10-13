use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::identity::ServerId;
use crate::protocol::{ChunkKey, TickBatch};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionKey {
    pub actor: ActorId,
    pub incarnation: u64,
    pub sequence: u16,
    pub family: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkActions {
    pub chunk: ChunkKey,
    pub actions: Vec<ActionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickFinish {
    pub tick: TickStamp,
    pub chunks: Vec<ChunkActions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropGapState {
    MissingFinish {
        received: Vec<ActionKey>,
    },
    MissingActions {
        expected: Vec<ActionKey>,
        received: Vec<ActionKey>,
    },
    ConflictingFinish {
        received: Vec<ActionKey>,
    },
    UnexpectedSource {
        received: Vec<ActionKey>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropGap {
    pub tick: TickStamp,
    pub chunk: ChunkKey,
    pub peer: ServerId,
    pub state: DropGapState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChunk {
    pub tick: TickStamp,
    pub chunk: ChunkKey,
    pub holders: Vec<ServerId>,
    pub blockers: Vec<DropGap>,
}

impl ActionKey {
    #[must_use]
    pub const fn break_block(update: &crate::protocol::BreakBlockUpdate) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: 0,
            sequence: update.seq.0,
            family: 0,
        }
    }

    #[must_use]
    pub const fn place_block(update: &crate::protocol::PlaceBlockUpdate) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: 0,
            sequence: update.seq.0,
            family: 1,
        }
    }

    #[must_use]
    pub const fn inventory(update: &crate::inventory::InventoryOp) -> Self {
        Self {
            actor: update.actor,
            incarnation: 0,
            sequence: update.seq.0,
            family: 2,
        }
    }

    #[must_use]
    pub const fn hit_player(update: &crate::protocol::HitPlayerUpdate) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: 0,
            sequence: update.seq.0,
            family: 3,
        }
    }

    #[must_use]
    pub const fn hit_entity(update: &crate::protocol::HitEntityUpdate) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: 0,
            sequence: update.seq.0,
            family: 4,
        }
    }

    #[must_use]
    pub const fn fire_projectile(update: &crate::protocol::FireProjectileUpdate) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: 0,
            sequence: update.seq.0,
            family: 5,
        }
    }

    #[must_use]
    pub const fn administrative(update: &crate::world_delta::AdministrativeDelta) -> Self {
        Self {
            actor: ActorId::Peer(update.holder),
            incarnation: 0,
            sequence: update.sequence,
            family: 6,
        }
    }

    #[must_use]
    pub const fn player_administrative(
        update: &crate::player_admin::PlayerAdministrativeAction,
    ) -> Self {
        Self {
            actor: ActorId::Player(update.target),
            incarnation: 0,
            sequence: update.seq.0,
            family: 7,
        }
    }

    #[must_use]
    pub const fn player_chunk_transition(update: &crate::protocol::PlayerChunkTransition) -> Self {
        Self {
            actor: ActorId::Player(update.gid),
            incarnation: update.incarnation,
            sequence: update.seq.0,
            family: 8,
        }
    }

    #[must_use]
    pub const fn entity_chunk_transition(
        update: &crate::entity_transition::EntityChunkTransition,
    ) -> Self {
        Self {
            actor: ActorId::Entity(update.entity),
            incarnation: 0,
            sequence: update.sequence,
            family: 9,
        }
    }

    #[must_use]
    pub const fn item_action(update: &crate::item_action::ItemAction) -> Self {
        Self {
            actor: update.actor,
            incarnation: 0,
            sequence: update.sequence.0,
            family: 10,
        }
    }

    #[must_use]
    pub const fn canonical(action: &crate::canonical_action::Action) -> Self {
        Self::canonical_id(action.id)
    }

    #[must_use]
    pub const fn canonical_id(id: crate::canonical_action::ActionId) -> Self {
        Self {
            actor: id.actor,
            incarnation: id.incarnation,
            sequence: id.sequence,
            family: id.family | 0x80,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcedTickBatch {
    pub source: ServerId,
    pub batch: TickBatch,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkPendingTick {
    pub tick: TickStamp,
    pub chunk: ChunkKey,
    pub holders: Vec<ServerId>,
    pub generations: Vec<(ServerId, u64)>,
    pub finishes: Vec<(ServerId, TickFinish)>,
    pub batches: Vec<SourcedTickBatch>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropReport {
    pub dropped: ServerId,
    pub gaps: Vec<DropGap>,
    pub retained: Vec<SourcedTickBatch>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompletionMessage {
    Finish(TickFinish),
    RealmFinish(crate::realm_completion::HolisticFinish),
    DropReport(DropReport),
    Dependency(crate::settlement::DependencyNotice),
    Settlement(crate::settlement::SettlementPublication),
}

#[derive(Debug, Clone)]
pub struct LocalTickBatch {
    pub batch: TickBatch,
    pub actor_intents: Vec<crate::controller::ControllerActorIntent>,
    pub view_intents: Vec<crate::controller::ViewIntent>,
}

impl TickFinish {
    pub fn try_from_batch(batch: &TickBatch) -> Result<Self, ManifestError> {
        validate_manifest_batch(batch)?;
        Ok(Self::from_batch(batch))
    }

    #[must_use]
    pub fn from_batch(batch: &TickBatch) -> Self {
        let mut chunks: BTreeMap<ChunkKey, BTreeSet<ActionKey>> = BTreeMap::new();
        for action in &batch.canonical_actions {
            let key = ActionKey::canonical(action);
            for stakeholder in &action.stakeholders {
                let chunk = match stakeholder {
                    crate::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
                    crate::canonical_action::Stakeholder::Player(player) => action
                        .player_chunks
                        .iter()
                        .find_map(|(key, chunk)| (key == player).then_some(*chunk)),
                };
                if let Some(chunk) = chunk {
                    chunks.entry(chunk).or_default().insert(key);
                }
            }
        }
        for update in &batch.break_block {
            let key = ActionKey::break_block(update);
            chunks.entry(update.actor_chunk).or_default().insert(key);
            chunks.entry(update.chunk).or_default().insert(key);
        }
        for update in &batch.place_block {
            let key = ActionKey::place_block(update);
            chunks.entry(update.actor_chunk).or_default().insert(key);
            chunks.entry(update.chunk).or_default().insert(key);
        }
        for update in &batch.inv_ops {
            let key = ActionKey::inventory(update);
            for chunk in &update.affected {
                chunks.entry(*chunk).or_default().insert(key);
            }
        }
        for update in &batch.hit_player {
            let key = ActionKey::hit_player(update);
            chunks.entry(update.actor_chunk).or_default().insert(key);
            chunks.entry(update.target_chunk).or_default().insert(key);
        }
        for update in &batch.hit_entity {
            let key = ActionKey::hit_entity(update);
            chunks.entry(update.actor_chunk).or_default().insert(key);
            chunks.entry(update.target.chunk).or_default().insert(key);
        }
        for update in &batch.fire {
            let key = ActionKey::fire_projectile(update);
            chunks.entry(update.actor_chunk).or_default().insert(key);
        }
        for update in &batch.administrative {
            let key = ActionKey::administrative(update);
            chunks.entry(update.chunk).or_default().insert(key);
        }
        for update in &batch.player_administrative {
            let key = ActionKey::player_administrative(update);
            for chunk in &update.affected {
                chunks.entry(*chunk).or_default().insert(key);
            }
        }
        for update in &batch.player_chunk_transition {
            let key = ActionKey::player_chunk_transition(update);
            chunks.entry(update.from).or_default().insert(key);
            chunks.entry(update.to).or_default().insert(key);
        }
        for update in &batch.entity_chunk_transition {
            let key = ActionKey::entity_chunk_transition(update);
            chunks.entry(update.from).or_default().insert(key);
            chunks.entry(update.to).or_default().insert(key);
        }
        for update in &batch.item_action {
            let key = ActionKey::item_action(update);
            for chunk in &update.affected {
                chunks.entry(*chunk).or_default().insert(key);
            }
        }
        Self {
            tick: batch.tick,
            chunks: chunks
                .into_iter()
                .map(|(chunk, actions)| ChunkActions {
                    chunk,
                    actions: actions.into_iter().collect(),
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn actions_for(&self, chunk: ChunkKey) -> &[ActionKey] {
        self.chunks
            .binary_search_by_key(&chunk, |entry| entry.chunk)
            .ok()
            .map_or(&[], |index| self.chunks[index].actions.as_slice())
    }
}

#[derive(Debug, Clone, Default)]
struct TickState {
    holders: BTreeMap<ChunkKey, BTreeSet<ServerId>>,
    received: BTreeMap<ChunkKey, BTreeSet<(ServerId, ActionKey)>>,
    finishes: BTreeMap<ServerId, FinishState>,
    imported_finishes: BTreeSet<ServerId>,
    unexpected_sources: BTreeMap<ChunkKey, BTreeSet<ServerId>>,
}

#[derive(Debug, Clone)]
enum FinishState {
    Declared(TickFinish),
    Conflicting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishResult {
    Recorded,
    Duplicate,
    Conflicting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequirementResult {
    Recorded,
    RecordedWithUnexpectedSources,
    AlreadyCaptured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestError {
    EmptyAffectedChunks(ActionKey),
    TickMismatch(ActionKey),
    InvalidCanonicalAction(ActionKey),
    UnsupportedPlayerStakeholder(ActionKey),
    SourceNotHolder { action: ActionKey, chunk: ChunkKey },
    SourceNotActorOwner { action: ActionKey, source: ServerId },
}

pub fn validate_manifest_batch(batch: &TickBatch) -> Result<(), ManifestError> {
    for action in &batch.canonical_actions {
        let key = ActionKey::canonical(action);
        if action.tick != batch.tick {
            return Err(ManifestError::TickMismatch(key));
        }
        if action.validate().is_err() {
            return Err(ManifestError::InvalidCanonicalAction(key));
        }
        if action.stakeholders.iter().any(|stakeholder| {
            matches!(stakeholder, crate::canonical_action::Stakeholder::Player(player)
                if !action.player_chunks.iter().any(|(key, _)| key == player))
        }) {
            return Err(ManifestError::UnsupportedPlayerStakeholder(key));
        }
    }
    for update in &batch.break_block {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::break_block(update)));
        }
    }
    for update in &batch.place_block {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::place_block(update)));
        }
    }
    for update in &batch.inv_ops {
        let key = ActionKey::inventory(update);
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(key));
        }
        if update.affected.is_empty() {
            return Err(ManifestError::EmptyAffectedChunks(key));
        }
    }
    for update in &batch.hit_player {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::hit_player(update)));
        }
    }
    for update in &batch.administrative {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::administrative(
                update,
            )));
        }
    }
    for update in &batch.player_administrative {
        let key = ActionKey::player_administrative(update);
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(key));
        }
        if update.affected.is_empty() {
            return Err(ManifestError::EmptyAffectedChunks(key));
        }
    }
    for update in &batch.player_chunk_transition {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(
                ActionKey::player_chunk_transition(update),
            ));
        }
        if update.from == update.to {
            return Err(ManifestError::EmptyAffectedChunks(
                ActionKey::player_chunk_transition(update),
            ));
        }
    }
    for update in &batch.entity_chunk_transition {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(
                ActionKey::entity_chunk_transition(update),
            ));
        }
        if update.from == update.to {
            return Err(ManifestError::EmptyAffectedChunks(
                ActionKey::entity_chunk_transition(update),
            ));
        }
    }
    for update in &batch.item_action {
        let key = ActionKey::item_action(update);
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(key));
        }
        if update.affected.is_empty() {
            return Err(ManifestError::EmptyAffectedChunks(key));
        }
    }
    for update in &batch.hit_entity {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::hit_entity(update)));
        }
    }
    for update in &batch.fire {
        if update.tick != batch.tick {
            return Err(ManifestError::TickMismatch(ActionKey::fire_projectile(
                update,
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct CompletionLedger {
    ticks: HashMap<TickStamp, TickState>,
    holderless_order: VecDeque<TickStamp>,
}

const MAX_HOLDERLESS_TICKS: usize = 2_400;

impl CompletionLedger {
    pub fn mark_imported_finish(&mut self, tick: TickStamp, source: ServerId) {
        if let Some(state) = self.ticks.get_mut(&tick)
            && matches!(state.finishes.get(&source), Some(FinishState::Declared(_)))
        {
            state.imported_finishes.insert(source);
        }
    }
    #[must_use]
    pub fn holders_for(&self, tick: TickStamp, chunk: ChunkKey) -> Vec<ServerId> {
        self.ticks
            .get(&tick)
            .and_then(|state| state.holders.get(&chunk))
            .map_or_else(Vec::new, |holders| holders.iter().copied().collect())
    }

    #[must_use]
    pub fn known_finishes_for(&self, tick: TickStamp) -> Vec<(ServerId, TickFinish)> {
        self.ticks
            .get(&tick)
            .into_iter()
            .flat_map(|state| &state.finishes)
            .filter_map(|(source, state)| match state {
                FinishState::Declared(finish) => Some((*source, finish.clone())),
                FinishState::Conflicting => None,
            })
            .collect()
    }
    pub fn reindex_action(
        &mut self,
        tick: TickStamp,
        action: ActionKey,
        old_chunks: &BTreeSet<ChunkKey>,
        new_chunks: &BTreeSet<ChunkKey>,
    ) -> bool {
        let Some(state) = self.ticks.get_mut(&tick) else {
            return false;
        };
        let sources: BTreeSet<_> = old_chunks
            .iter()
            .flat_map(|chunk| state.received.get(chunk).into_iter().flatten())
            .filter_map(|(source, key)| (*key == action).then_some(*source))
            .collect();
        for chunk in old_chunks.difference(new_chunks) {
            if let Some(received) = state.received.get_mut(chunk) {
                received.retain(|(_, key)| *key != action);
            }
        }
        for chunk in new_chunks.difference(old_chunks) {
            state
                .received
                .entry(*chunk)
                .or_default()
                .extend(sources.iter().map(|source| (*source, action)));
        }
        for (source, finish) in &mut state.finishes {
            let FinishState::Declared(finish) = finish else {
                continue;
            };
            if !sources.contains(source) {
                continue;
            }
            for chunk in old_chunks.difference(new_chunks) {
                if let Ok(index) = finish
                    .chunks
                    .binary_search_by_key(chunk, |entry| entry.chunk)
                    && let Ok(action_index) = finish.chunks[index].actions.binary_search(&action)
                {
                    finish.chunks[index].actions.remove(action_index);
                }
            }
            for chunk in new_chunks.difference(old_chunks) {
                match finish
                    .chunks
                    .binary_search_by_key(chunk, |entry| entry.chunk)
                {
                    Ok(index) => {
                        let actions = &mut finish.chunks[index].actions;
                        if let Err(index) = actions.binary_search(&action) {
                            actions.insert(index, action);
                        }
                    }
                    Err(index) => finish.chunks.insert(
                        index,
                        ChunkActions {
                            chunk: *chunk,
                            actions: vec![action],
                        },
                    ),
                }
            }
        }
        !sources.is_empty()
    }

    pub fn require(
        &mut self,
        tick: TickStamp,
        chunk: ChunkKey,
        holders: &[ServerId],
    ) -> RequirementResult {
        let holders: BTreeSet<_> = holders.iter().copied().collect();
        let state = self.ticks.entry(tick).or_default();
        match state.holders.get(&chunk) {
            None => {
                state.holders.insert(chunk, holders);
                let captured = &state.holders[&chunk];
                let unexpected: BTreeSet<_> = state
                    .received
                    .get(&chunk)
                    .into_iter()
                    .flatten()
                    .map(|(source, _)| *source)
                    .chain(state.finishes.iter().filter_map(|(source, finish)| {
                        matches!(finish, FinishState::Declared(finish) if !finish.actions_for(chunk).is_empty())
                            .then_some(*source)
                    }))
                    .filter(|source| !captured.contains(source))
                    .collect();
                if !unexpected.is_empty() {
                    state.unexpected_sources.insert(chunk, unexpected);
                    RequirementResult::RecordedWithUnexpectedSources
                } else {
                    RequirementResult::Recorded
                }
            }
            Some(_) => RequirementResult::AlreadyCaptured,
        }
    }

    pub fn note_batch(&mut self, source: ServerId, batch: &TickBatch) -> Result<(), ManifestError> {
        self.note_batch_with_entity_owners(source, batch, |_| None)
    }

    pub fn note_batch_with_entity_owners(
        &mut self,
        source: ServerId,
        batch: &TickBatch,
        current_owner: impl Fn(crate::protocol::GlobalEntityId) -> Option<ServerId>,
    ) -> Result<(), ManifestError> {
        validate_manifest_batch(batch)?;
        let state = self.ticks.entry(batch.tick).or_default();
        let manifest = TickFinish::from_batch(batch);
        for entry in &manifest.chunks {
            if state
                .holders
                .get(&entry.chunk)
                .is_some_and(|holders| !holders.contains(&source))
            {
                return Err(ManifestError::SourceNotHolder {
                    action: entry.actions[0],
                    chunk: entry.chunk,
                });
            }
            if let Some(action) = entry.actions.iter().find(|action| match action.actor {
                ActorId::Player(player) => player.server != source,
                ActorId::Entity(entity) => {
                    action.family & 0x80 == 0 && current_owner(entity) != Some(source)
                }
                ActorId::Peer(peer) => peer != source,
            }) {
                return Err(ManifestError::SourceNotActorOwner {
                    action: *action,
                    source,
                });
            }
        }
        for entry in manifest.chunks {
            state
                .received
                .entry(entry.chunk)
                .or_default()
                .extend(entry.actions.into_iter().map(|action| (source, action)));
        }
        Ok(())
    }

    pub fn note_finish(&mut self, source: ServerId, finish: TickFinish) -> FinishResult {
        self.note_finish_with_entity_owners(source, finish, |_| None)
    }

    pub fn note_finish_with_entity_owners(
        &mut self,
        source: ServerId,
        finish: TickFinish,
        current_owner: impl Fn(crate::protocol::GlobalEntityId) -> Option<ServerId>,
    ) -> FinishResult {
        let mut merged: BTreeMap<ChunkKey, BTreeSet<ActionKey>> = BTreeMap::new();
        for entry in finish.chunks {
            merged.entry(entry.chunk).or_default().extend(entry.actions);
        }
        let finish = TickFinish {
            tick: finish.tick,
            chunks: merged
                .into_iter()
                .map(|(chunk, actions)| ChunkActions {
                    chunk,
                    actions: actions.into_iter().collect(),
                })
                .collect(),
        };
        let tick = finish.tick;
        let invalid_source = finish
            .chunks
            .iter()
            .flat_map(|entry| &entry.actions)
            .any(|action| match action.actor {
                ActorId::Player(player) => player.server != source,
                ActorId::Entity(entity) => {
                    action.family & 0x80 == 0 && current_owner(entity) != Some(source)
                }
                ActorId::Peer(peer) => peer != source,
            });
        if !self.ticks.contains_key(&tick) {
            self.holderless_order.push_back(tick);
        }
        let state = self.ticks.entry(tick).or_default();
        let mut unexpected_source = false;
        for entry in &finish.chunks {
            if state
                .holders
                .get(&entry.chunk)
                .is_some_and(|holders| !holders.contains(&source))
            {
                unexpected_source = true;
                state
                    .unexpected_sources
                    .entry(entry.chunk)
                    .or_default()
                    .insert(source);
            }
        }
        let result = if invalid_source || unexpected_source {
            state.finishes.insert(source, FinishState::Conflicting);
            FinishResult::Conflicting
        } else {
            match state.finishes.get(&source) {
                None => {
                    state.finishes.insert(source, FinishState::Declared(finish));
                    FinishResult::Recorded
                }
                Some(FinishState::Declared(recorded)) if recorded == &finish => {
                    state.imported_finishes.remove(&source);
                    FinishResult::Duplicate
                }
                Some(FinishState::Declared(recorded))
                    if state.imported_finishes.contains(&source)
                        && recorded.chunks.iter().all(|entry| {
                            finish.actions_for(entry.chunk) == entry.actions.as_slice()
                        }) =>
                {
                    state.finishes.insert(source, FinishState::Declared(finish));
                    state.imported_finishes.remove(&source);
                    FinishResult::Recorded
                }
                Some(FinishState::Declared(_)) => {
                    state.finishes.insert(source, FinishState::Conflicting);
                    state.imported_finishes.remove(&source);
                    FinishResult::Conflicting
                }
                Some(FinishState::Conflicting) => FinishResult::Conflicting,
            }
        };
        while self.holderless_order.len() > MAX_HOLDERLESS_TICKS {
            if let Some(oldest) = self.holderless_order.pop_front()
                && self
                    .ticks
                    .get(&oldest)
                    .is_some_and(|state| state.holders.is_empty())
            {
                self.ticks.remove(&oldest);
            }
        }
        result
    }

    #[must_use]
    pub fn chunk_complete(&self, tick: TickStamp, chunk: ChunkKey) -> bool {
        let Some(state) = self.ticks.get(&tick) else {
            return false;
        };
        let Some(holders) = state.holders.get(&chunk) else {
            return false;
        };
        if state
            .unexpected_sources
            .get(&chunk)
            .is_some_and(|sources| !sources.is_empty())
        {
            return false;
        }
        let received = state.received.get(&chunk);
        holders.iter().all(|holder| {
            state.finishes.get(holder).is_some_and(|finish| {
                let FinishState::Declared(finish) = finish else {
                    return false;
                };
                let expected = finish.actions_for(chunk);
                let received_count = received
                    .into_iter()
                    .flatten()
                    .filter(|(source, _)| source == holder)
                    .count();
                expected.len() == received_count
                    && expected.iter().all(|action| {
                        received.is_some_and(|received| received.contains(&(*holder, *action)))
                    })
            })
        })
    }

    #[must_use]
    pub fn tick_complete(&self, tick: TickStamp) -> bool {
        self.ticks.get(&tick).is_some_and(|state| {
            !state.holders.is_empty()
                && state
                    .holders
                    .keys()
                    .all(|chunk| self.chunk_complete(tick, *chunk))
        })
    }

    pub fn remove_tick(&mut self, tick: TickStamp) -> bool {
        self.ticks.remove(&tick).is_some()
    }

    #[must_use]
    pub fn tracked_chunks(&self) -> Vec<(TickStamp, ChunkKey)> {
        let mut chunks: Vec<_> = self
            .ticks
            .iter()
            .flat_map(|(tick, state)| state.holders.keys().map(|chunk| (*tick, *chunk)))
            .collect();
        chunks.sort_unstable();
        chunks
    }

    #[must_use]
    pub fn action_chunks(&self, tick: TickStamp, action: ActionKey) -> Vec<ChunkKey> {
        let Some(state) = self.ticks.get(&tick) else {
            return Vec::new();
        };
        let mut chunks: Vec<_> = state
            .finishes
            .values()
            .filter_map(|finish| match finish {
                FinishState::Declared(finish) => Some(finish),
                FinishState::Conflicting => None,
            })
            .flat_map(|finish| {
                finish
                    .chunks
                    .iter()
                    .filter(move |entry| entry.actions.contains(&action))
                    .map(|entry| entry.chunk)
            })
            .collect();
        chunks.sort_unstable();
        chunks.dedup();
        chunks
    }

    #[must_use]
    pub fn chunk_actions(&self, tick: TickStamp, chunk: ChunkKey) -> Vec<ActionKey> {
        let Some(state) = self.ticks.get(&tick) else {
            return Vec::new();
        };
        let mut actions: Vec<_> = state
            .finishes
            .values()
            .filter_map(|finish| match finish {
                FinishState::Declared(finish) => Some(finish.actions_for(chunk)),
                FinishState::Conflicting => None,
            })
            .flatten()
            .copied()
            .collect();
        actions.sort_unstable();
        actions.dedup();
        actions
    }

    pub fn remove_chunks(&mut self, tick: TickStamp, chunks: &BTreeSet<ChunkKey>) -> bool {
        let Some(state) = self.ticks.get_mut(&tick) else {
            return false;
        };
        let mut removed = false;
        for chunk in chunks {
            removed |= state.holders.remove(chunk).is_some();
            state.received.remove(chunk);
            state.unexpected_sources.remove(chunk);
        }
        if state.holders.is_empty() {
            self.ticks.remove(&tick);
            self.holderless_order.retain(|entry| *entry != tick);
        }
        removed
    }

    #[must_use]
    pub fn tracked_ticks(&self) -> usize {
        self.ticks.len()
    }

    #[must_use]
    pub fn pending_chunks(&self) -> Vec<PendingChunk> {
        let mut pending = Vec::new();
        for (tick, state) in &self.ticks {
            for (chunk, holders) in &state.holders {
                if self.chunk_complete(*tick, *chunk) {
                    continue;
                }
                let mut blockers = Vec::new();
                for peer in holders {
                    if let Some(gap) = gap_for(state, *tick, *chunk, *peer) {
                        blockers.push(gap);
                    }
                }
                if let Some(sources) = state.unexpected_sources.get(chunk) {
                    for peer in sources {
                        let received = received_actions(state, *chunk, *peer);
                        blockers.push(DropGap {
                            tick: *tick,
                            chunk: *chunk,
                            peer: *peer,
                            state: DropGapState::UnexpectedSource { received },
                        });
                    }
                }
                blockers.sort_by_key(|gap| gap.peer);
                pending.push(PendingChunk {
                    tick: *tick,
                    chunk: *chunk,
                    holders: holders.iter().copied().collect(),
                    blockers,
                });
            }
        }
        pending.sort_by_key(|entry| (entry.tick, entry.chunk));
        pending
    }

    #[must_use]
    pub fn gaps_for(&self, peer: ServerId) -> Vec<DropGap> {
        let mut gaps = Vec::new();
        for (tick, state) in &self.ticks {
            for (chunk, holders) in &state.holders {
                if !holders.contains(&peer) {
                    continue;
                }
                if let Some(gap) = gap_for(state, *tick, *chunk, peer) {
                    gaps.push(gap);
                }
            }
        }
        gaps.sort_by_key(|gap| (gap.tick, gap.chunk));
        gaps
    }

    pub fn drop_peer(&mut self, peer: ServerId) {
        for state in self.ticks.values_mut() {
            for holders in state.holders.values_mut() {
                holders.remove(&peer);
            }
            state.unexpected_sources.retain(|_, sources| {
                sources.remove(&peer);
                !sources.is_empty()
            });
        }
    }

    #[must_use]
    pub fn remaining_holders_for(&self, peer: ServerId) -> BTreeSet<ServerId> {
        let mut holders = BTreeSet::new();
        for state in self.ticks.values() {
            for chunk_holders in state.holders.values() {
                if chunk_holders.contains(&peer) {
                    holders.extend(
                        chunk_holders
                            .iter()
                            .copied()
                            .filter(|holder| *holder != peer),
                    );
                }
            }
        }
        holders
    }
}

fn received_actions(state: &TickState, chunk: ChunkKey, peer: ServerId) -> Vec<ActionKey> {
    let mut received: Vec<_> = state
        .received
        .get(&chunk)
        .into_iter()
        .flatten()
        .filter_map(|(source, action)| (*source == peer).then_some(*action))
        .collect();
    received.sort_unstable();
    received
}

fn gap_for(state: &TickState, tick: TickStamp, chunk: ChunkKey, peer: ServerId) -> Option<DropGap> {
    let received = received_actions(state, chunk, peer);
    let gap_state = match state.finishes.get(&peer) {
        None => DropGapState::MissingFinish { received },
        Some(FinishState::Conflicting) => DropGapState::ConflictingFinish { received },
        Some(FinishState::Declared(finish)) => {
            let expected = finish.actions_for(chunk).to_vec();
            if expected == received {
                return None;
            }
            DropGapState::MissingActions { expected, received }
        }
    };
    Some(DropGap {
        tick,
        chunk,
        peer,
        state: gap_state,
    })
}

const FINISH_PREFIX: [u8; 4] = *b"TFN1";
const REALM_FINISH_PREFIX: [u8; 4] = *b"RFN1";
const MAX_FINISH_BYTES: usize = 1024 * 1024;
const MAX_FINISH_CHUNKS: usize = 16_384;
const MAX_FINISH_ACTIONS: usize = 262_144;
const DROP_REPORT_PREFIX: [u8; 4] = *b"DRP1";
const MAX_DROP_REPORT_BYTES: usize = 8 * 1024 * 1024;
const MAX_DROP_GAPS: usize = 262_144;
const MAX_RETAINED_BATCHES: usize = 262_144;
const DEPENDENCY_PREFIX: [u8; 4] = *b"DEP1";
const SETTLEMENT_PREFIX: [u8; 4] = *b"STL1";

pub fn encode_finish(finish: &TickFinish) -> Result<Vec<u8>, postcard::Error> {
    if finish.chunks.len() > MAX_FINISH_CHUNKS
        || finish
            .chunks
            .iter()
            .try_fold(0_usize, |count, chunk| {
                count.checked_add(chunk.actions.len())
            })
            .is_none_or(|count| count > MAX_FINISH_ACTIONS)
    {
        return Err(postcard::Error::SerializeBufferFull);
    }
    let bytes = postcard::to_extend(finish, FINISH_PREFIX.to_vec())?;
    if bytes.len() > MAX_FINISH_BYTES {
        Err(postcard::Error::SerializeBufferFull)
    } else {
        Ok(bytes)
    }
}

pub fn decode_finish(bytes: &[u8]) -> Result<TickFinish, postcard::Error> {
    if bytes.len() > MAX_FINISH_BYTES {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    let payload = bytes
        .strip_prefix(&FINISH_PREFIX)
        .ok_or(postcard::Error::DeserializeBadEncoding)?;
    let finish: TickFinish = postcard::from_bytes(payload)?;
    if finish.chunks.len() > MAX_FINISH_CHUNKS
        || finish
            .chunks
            .iter()
            .try_fold(0_usize, |count, chunk| {
                count.checked_add(chunk.actions.len())
            })
            .is_none_or(|count| count > MAX_FINISH_ACTIONS)
    {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(finish)
}

pub fn encode_realm_finish(
    finish: &crate::realm_completion::HolisticFinish,
) -> Result<Vec<u8>, postcard::Error> {
    if finish.issued.len() > MAX_FINISH_CHUNKS
        || finish
            .issued
            .values()
            .try_fold(0_usize, |count, actions| count.checked_add(actions.len()))
            .is_none_or(|count| count > MAX_FINISH_ACTIONS)
    {
        return Err(postcard::Error::SerializeBufferFull);
    }
    let bytes = postcard::to_extend(finish, REALM_FINISH_PREFIX.to_vec())?;
    if bytes.len() > MAX_FINISH_BYTES {
        Err(postcard::Error::SerializeBufferFull)
    } else {
        Ok(bytes)
    }
}

pub fn decode_realm_finish(
    bytes: &[u8],
) -> Result<crate::realm_completion::HolisticFinish, postcard::Error> {
    if bytes.len() > MAX_FINISH_BYTES {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    let payload = bytes
        .strip_prefix(&REALM_FINISH_PREFIX)
        .ok_or(postcard::Error::DeserializeBadEncoding)?;
    let finish: crate::realm_completion::HolisticFinish = postcard::from_bytes(payload)?;
    if finish.issued.len() > MAX_FINISH_CHUNKS
        || finish
            .issued
            .values()
            .try_fold(0_usize, |count, actions| count.checked_add(actions.len()))
            .is_none_or(|count| count > MAX_FINISH_ACTIONS)
    {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(finish)
}

pub fn encode_drop_report(report: &DropReport) -> Result<Vec<u8>, postcard::Error> {
    if report.gaps.len() > MAX_DROP_GAPS || report.retained.len() > MAX_RETAINED_BATCHES {
        return Err(postcard::Error::SerializeBufferFull);
    }
    let bytes = postcard::to_extend(report, DROP_REPORT_PREFIX.to_vec())?;
    if bytes.len() > MAX_DROP_REPORT_BYTES {
        Err(postcard::Error::SerializeBufferFull)
    } else {
        Ok(bytes)
    }
}

pub fn encode_drop_report_parts(report: &DropReport) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let mut parts = Vec::new();
    let mut current = DropReport {
        dropped: report.dropped,
        gaps: report.gaps.clone(),
        retained: Vec::new(),
        complete: false,
    };
    for batch in &report.retained {
        current.retained.push(batch.clone());
        if encode_drop_report(&current).is_err() {
            current.retained.pop();
            if current.retained.is_empty() {
                return Err(postcard::Error::SerializeBufferFull);
            }
            parts.push(encode_drop_report(&current)?);
            current.gaps.clear();
            current.retained.push(batch.clone());
        }
    }
    current.complete = true;
    match encode_drop_report(&current) {
        Ok(bytes) => parts.push(bytes),
        Err(_) if !current.retained.is_empty() => {
            current.complete = false;
            parts.push(encode_drop_report(&current)?);
            parts.push(encode_drop_report(&DropReport {
                dropped: report.dropped,
                gaps: Vec::new(),
                retained: Vec::new(),
                complete: true,
            })?);
        }
        Err(error) => return Err(error),
    }
    Ok(parts)
}

pub fn decode_drop_report(bytes: &[u8]) -> Result<DropReport, postcard::Error> {
    if bytes.len() > MAX_DROP_REPORT_BYTES {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    let payload = bytes
        .strip_prefix(&DROP_REPORT_PREFIX)
        .ok_or(postcard::Error::DeserializeBadEncoding)?;
    let report: DropReport = postcard::from_bytes(payload)?;
    if report.gaps.len() > MAX_DROP_GAPS || report.retained.len() > MAX_RETAINED_BATCHES {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(report)
}

pub fn encode_dependency(
    notice: &crate::settlement::DependencyNotice,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_extend(notice, DEPENDENCY_PREFIX.to_vec())
}

pub fn encode_settlement(
    settlement: &crate::settlement::SettlementPublication,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_extend(settlement, SETTLEMENT_PREFIX.to_vec())
}

pub fn decode_message(bytes: &[u8]) -> Result<CompletionMessage, postcard::Error> {
    if bytes.starts_with(&FINISH_PREFIX) {
        decode_finish(bytes).map(CompletionMessage::Finish)
    } else if bytes.starts_with(&REALM_FINISH_PREFIX) {
        decode_realm_finish(bytes).map(CompletionMessage::RealmFinish)
    } else if bytes.starts_with(&DROP_REPORT_PREFIX) {
        decode_drop_report(bytes).map(CompletionMessage::DropReport)
    } else if let Some(payload) = bytes.strip_prefix(&DEPENDENCY_PREFIX) {
        postcard::from_bytes(payload).map(CompletionMessage::Dependency)
    } else if let Some(payload) = bytes.strip_prefix(&SETTLEMENT_PREFIX) {
        postcard::from_bytes(payload).map(CompletionMessage::Settlement)
    } else {
        Err(postcard::Error::DeserializeBadEncoding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot};
    use crate::inventory::{InvLoc, InvOpKind, capture_inv_op};
    use crate::protocol::{
        BlockPos, BreakBlockUpdate, ChunkAddr, EntityRef, FireProjectileUpdate, HitEntityUpdate,
        HitPlayerUpdate, WorldId,
    };

    #[test]
    fn canonical_entity_finish_does_not_read_live_owner() {
        let key = ActionKey {
            actor: ActorId::Entity(crate::protocol::GlobalEntityId {
                origin: ServerId(1),
                local_id: 4,
                generation: 7,
            }),
            incarnation: 7,
            sequence: 1,
            family: 0x88,
        };
        let finish = TickFinish {
            tick: TickStamp(7),
            chunks: vec![ChunkActions {
                chunk: chunk(0, 0),
                actions: vec![key],
            }],
        };
        let mut ledger = CompletionLedger::default();
        assert_eq!(
            ledger.note_finish_with_entity_owners(ServerId(2), finish.clone(), |_| {
                panic!("canonical authority belongs to frozen realm capture")
            }),
            FinishResult::Recorded
        );
        let mut legacy = finish;
        legacy.chunks[0].actions[0].family = 8;
        let mut ledger = CompletionLedger::default();
        assert_eq!(
            ledger.note_finish_with_entity_owners(ServerId(2), legacy, |_| Some(ServerId(1))),
            FinishResult::Conflicting
        );
    }

    fn gid(server: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(1))
    }

    fn chunk(x: i32, z: i32) -> ChunkKey {
        ChunkKey::from_position(WorldId::new(0, 17), ChunkAddr { x, z })
    }

    fn action(server: u16, sequence: u16, chunk: ChunkKey) -> TickBatch {
        let mut batch = TickBatch::new(TickStamp(7));
        batch.break_block.push(BreakBlockUpdate {
            gid: gid(server),
            seq: PlayerSeq(sequence),
            tick: TickStamp(7),
            pos: BlockPos { x: 0, y: 64, z: 0 },
            expected_old_state: 1,
            actor_chunk: chunk,
            chunk,
        });
        batch
    }

    #[test]
    fn three_peers_converge_only_after_missing_actions_arrive() {
        let chunk = chunk(4, 9);
        let batches = [
            action(1, 1, chunk),
            action(2, 1, chunk),
            action(3, 1, chunk),
        ];
        let holders = [ServerId(1), ServerId(2), ServerId(3)];
        let arrivals = [[0, 1], [1, 2], [2, 0]];

        for (receiver, initial) in arrivals.into_iter().enumerate() {
            let mut ledger = CompletionLedger::default();
            ledger.require(TickStamp(7), chunk, &holders);
            for batch in &batches {
                ledger.note_finish(
                    batch.break_block[0].gid.server,
                    TickFinish::from_batch(batch),
                );
            }
            for index in initial {
                ledger
                    .note_batch(batches[index].break_block[0].gid.server, &batches[index])
                    .expect("valid manifest batch");
            }
            assert!(
                !ledger.chunk_complete(TickStamp(7), chunk),
                "receiver {receiver}"
            );
            let missing = [2, 0, 1][receiver];
            ledger
                .note_batch(
                    batches[missing].break_block[0].gid.server,
                    &batches[missing],
                )
                .expect("valid manifest batch");
            assert!(
                ledger.chunk_complete(TickStamp(7), chunk),
                "receiver {receiver}"
            );
        }
    }

    #[test]
    fn multi_chunk_action_is_referenced_and_required_by_both_chunks() {
        let first = chunk(1, 0);
        let second = chunk(2, 0);
        let mut batch = action(1, 8, second);
        batch.break_block[0].actor_chunk = first;
        let finish = TickFinish::from_batch(&batch);
        assert_eq!(finish.chunks.len(), 2);

        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), first, &[ServerId(1)]);
        ledger.require(TickStamp(7), second, &[ServerId(1)]);
        ledger.note_finish(ServerId(1), finish);
        ledger
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        assert!(ledger.chunk_complete(TickStamp(7), first));
        assert!(ledger.chunk_complete(TickStamp(7), second));
    }

    #[test]
    fn inventory_action_is_declared_for_every_affected_chunk() {
        let first = chunk(1, 0);
        let second = chunk(2, 0);
        let mut batch = TickBatch::new(TickStamp(7));
        batch.inv_ops.push(capture_inv_op(
            gid(1),
            PlayerSeq(12),
            TickStamp(7),
            InvOpKind::Move,
            InvLoc { inv: 0, slot: 1 },
            InvLoc { inv: 0, slot: 2 },
            4,
            1,
            first,
            vec![first, second],
        ));
        let finish = TickFinish::from_batch(&batch);
        let key = ActionKey::inventory(&batch.inv_ops[0]);

        assert_eq!(finish.actions_for(first), &[key]);
        assert_eq!(finish.actions_for(second), &[key]);

        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), first, &[ServerId(1)]);
        ledger.require(TickStamp(7), second, &[ServerId(1)]);
        ledger.note_finish(ServerId(1), finish);
        assert!(!ledger.tick_complete(TickStamp(7)));
        ledger
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        assert!(ledger.tick_complete(TickStamp(7)));
    }

    #[test]
    fn combat_actions_are_declared_for_every_affected_chunk() {
        let actor = chunk(3, 4);
        let player_target = chunk(4, 4);
        let entity_target = chunk(5, 4);
        let mut batch = TickBatch::new(TickStamp(7));
        batch.hit_player.push(HitPlayerUpdate {
            gid: gid(1),
            seq: PlayerSeq(20),
            tick: TickStamp(7),
            actor_chunk: actor,
            target: gid(2),
            target_chunk: player_target,
            damage_milli: 1_000,
        });
        batch.hit_entity.push(HitEntityUpdate {
            gid: gid(1),
            seq: PlayerSeq(21),
            tick: TickStamp(7),
            actor_chunk: actor,
            target: EntityRef {
                origin: ServerId(2),
                local_id: 9,
                generation: 1,
                chunk: entity_target,
            },
            damage_milli: 1_000,
        });
        batch.fire.push(FireProjectileUpdate {
            gid: gid(1),
            seq: PlayerSeq(22),
            tick: TickStamp(7),
            actor_chunk: actor,
            kind: 0,
            charge_milli: 1_000,
            dir: [0.0, 0.0, 1.0],
        });

        let finish = TickFinish::try_from_batch(&batch).expect("valid manifest batch");
        assert_eq!(finish.actions_for(actor).len(), 3);
        assert_eq!(
            finish.actions_for(player_target),
            &[ActionKey::hit_player(&batch.hit_player[0])]
        );
        assert_eq!(
            finish.actions_for(entity_target),
            &[ActionKey::hit_entity(&batch.hit_entity[0])]
        );
    }

    #[test]
    fn inventory_without_an_affected_chunk_is_not_manifestable() {
        let mut batch = TickBatch::new(TickStamp(7));
        let actor_chunk = chunk(0, 0);
        let mut operation = capture_inv_op(
            gid(1),
            PlayerSeq(12),
            TickStamp(7),
            InvOpKind::Move,
            InvLoc { inv: 0, slot: 1 },
            InvLoc { inv: 0, slot: 2 },
            4,
            1,
            actor_chunk,
            vec![actor_chunk],
        );
        operation.affected.clear();
        batch.inv_ops.push(operation);

        assert_eq!(
            TickFinish::try_from_batch(&batch),
            Err(ManifestError::EmptyAffectedChunks(ActionKey::inventory(
                &batch.inv_ops[0]
            )))
        );
    }

    #[test]
    fn declaration_and_delivery_order_produce_the_same_completion() {
        let chunk = chunk(8, -3);
        let batch = action(1, 5, chunk);
        let finish = TickFinish::from_batch(&batch);
        let mut declaration_first = CompletionLedger::default();
        let mut delivery_first = CompletionLedger::default();
        for ledger in [&mut declaration_first, &mut delivery_first] {
            assert_eq!(
                ledger.require(TickStamp(7), chunk, &[ServerId(1)]),
                RequirementResult::Recorded
            );
        }

        declaration_first.note_finish(ServerId(1), finish.clone());
        assert!(!declaration_first.chunk_complete(TickStamp(7), chunk));
        declaration_first
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");

        delivery_first
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        assert!(!delivery_first.chunk_complete(TickStamp(7), chunk));
        delivery_first.note_finish(ServerId(1), finish);

        assert!(declaration_first.chunk_complete(TickStamp(7), chunk));
        assert!(delivery_first.chunk_complete(TickStamp(7), chunk));
    }

    #[test]
    fn manifest_only_declares_what_each_holder_will_deliver() {
        let chunk = chunk(3, 6);
        let holders = [ServerId(1), ServerId(2)];
        let batch = action(1, 5, chunk);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), chunk, &holders);

        assert_eq!(
            ledger.note_finish(ServerId(1), TickFinish::from_batch(&batch)),
            FinishResult::Recorded
        );
        assert!(!ledger.chunk_complete(TickStamp(7), chunk));

        assert_eq!(
            ledger.note_finish(
                ServerId(2),
                TickFinish {
                    tick: TickStamp(7),
                    chunks: Vec::new(),
                },
            ),
            FinishResult::Recorded
        );
        assert!(!ledger.chunk_complete(TickStamp(7), chunk));

        ledger
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        assert!(ledger.chunk_complete(TickStamp(7), chunk));
    }

    #[test]
    fn pending_details_distinguish_missing_manifest_and_missing_delivery() {
        let chunk = chunk(3, 6);
        let holders = [ServerId(1), ServerId(2)];
        let batch = action(1, 5, chunk);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), chunk, &holders);
        ledger.note_finish(ServerId(1), TickFinish::from_batch(&batch));

        let pending = ledger.pending_chunks();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].holders, holders);
        assert!(matches!(
            pending[0].blockers.as_slice(),
            [
                DropGap {
                    peer: ServerId(1),
                    state: DropGapState::MissingActions { .. },
                    ..
                },
                DropGap {
                    peer: ServerId(2),
                    state: DropGapState::MissingFinish { .. },
                    ..
                }
            ]
        ));

        ledger
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        ledger.note_finish(
            ServerId(2),
            TickFinish {
                tick: TickStamp(7),
                chunks: Vec::new(),
            },
        );
        assert!(ledger.pending_chunks().is_empty());
    }

    #[test]
    fn repeated_manifest_is_idempotent_but_cannot_be_changed() {
        let chunk = chunk(-2, 11);
        let first = action(1, 4, chunk);
        let changed = action(1, 5, chunk);
        let first_finish = TickFinish::from_batch(&first);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), chunk, &[ServerId(1)]);

        assert_eq!(
            ledger.note_finish(ServerId(1), first_finish.clone()),
            FinishResult::Recorded
        );
        assert_eq!(
            ledger.note_finish(ServerId(1), first_finish),
            FinishResult::Duplicate
        );
        ledger
            .note_batch(ServerId(1), &first)
            .expect("valid manifest batch");
        assert!(ledger.chunk_complete(TickStamp(7), chunk));

        assert_eq!(
            ledger.note_finish(ServerId(1), TickFinish::from_batch(&changed)),
            FinishResult::Conflicting
        );
        ledger
            .note_batch(ServerId(1), &changed)
            .expect("valid manifest batch");
        assert!(!ledger.chunk_complete(TickStamp(7), chunk));
        assert!(matches!(
            ledger.gaps_for(ServerId(1)).as_slice(),
            [DropGap {
                state: DropGapState::ConflictingFinish { .. },
                ..
            }]
        ));
    }

    #[test]
    fn later_holder_observations_do_not_change_the_captured_set() {
        let chunk = chunk(6, -8);
        let batch = action(1, 4, chunk);
        let mut ledger = CompletionLedger::default();
        assert_eq!(
            ledger.require(TickStamp(7), chunk, &[ServerId(2), ServerId(1)]),
            RequirementResult::Recorded
        );
        assert_eq!(
            ledger.require(TickStamp(7), chunk, &[ServerId(1), ServerId(2)]),
            RequirementResult::AlreadyCaptured
        );
        ledger.note_finish(ServerId(1), TickFinish::from_batch(&batch));
        ledger.note_finish(
            ServerId(2),
            TickFinish {
                tick: TickStamp(7),
                chunks: Vec::new(),
            },
        );
        ledger
            .note_batch(ServerId(1), &batch)
            .expect("valid manifest batch");
        assert!(ledger.chunk_complete(TickStamp(7), chunk));

        assert_eq!(
            ledger.require(TickStamp(7), chunk, &[ServerId(1)]),
            RequirementResult::AlreadyCaptured
        );
        assert!(ledger.chunk_complete(TickStamp(7), chunk));
    }

    #[test]
    fn action_source_must_belong_to_the_captured_holder_set_in_either_order() {
        let chunk = chunk(6, 3);
        let batch = action(2, 9, chunk);
        let finish = TickFinish::from_batch(&batch);
        for declaration_first in [false, true] {
            let mut ledger = CompletionLedger::default();
            if declaration_first {
                ledger.note_finish(ServerId(2), finish.clone());
            } else {
                ledger
                    .note_batch(ServerId(2), &batch)
                    .expect("valid manifest batch");
            }
            assert_eq!(
                ledger.require(TickStamp(7), chunk, &[ServerId(1)]),
                RequirementResult::RecordedWithUnexpectedSources
            );
            if declaration_first {
                assert!(matches!(
                    ledger.note_batch(ServerId(2), &batch),
                    Err(ManifestError::SourceNotHolder { .. })
                ));
            } else {
                ledger.note_finish(ServerId(2), finish.clone());
            }
            ledger.note_finish(
                ServerId(1),
                TickFinish {
                    tick: TickStamp(7),
                    chunks: Vec::new(),
                },
            );
            assert!(!ledger.chunk_complete(TickStamp(7), chunk));
            assert!(matches!(
                ledger.pending_chunks()[0].blockers.as_slice(),
                [DropGap {
                    peer: ServerId(2),
                    state: DropGapState::UnexpectedSource { .. },
                    ..
                }]
            ));
        }
    }

    #[test]
    fn peer_cannot_declare_another_peers_action_key() {
        let chunk = chunk(2, 7);
        let batch = action(1, 3, chunk);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), chunk, &[ServerId(1), ServerId(2)]);

        assert_eq!(
            ledger.note_finish(ServerId(2), TickFinish::from_batch(&batch)),
            FinishResult::Conflicting
        );
        assert!(!ledger.chunk_complete(TickStamp(7), chunk));
    }

    #[test]
    fn undeclared_received_action_prevents_completion() {
        let chunk = chunk(-7, 2);
        let declared = action(1, 3, chunk);
        let undeclared = action(1, 4, chunk);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), chunk, &[ServerId(1)]);
        ledger
            .note_batch(ServerId(1), &declared)
            .expect("valid manifest batch");
        ledger
            .note_batch(ServerId(1), &undeclared)
            .expect("valid manifest batch");
        ledger.note_finish(ServerId(1), TickFinish::from_batch(&declared));

        assert!(!ledger.chunk_complete(TickStamp(7), chunk));
        assert!(matches!(
            ledger.gaps_for(ServerId(1)).as_slice(),
            [DropGap {
                state: DropGapState::MissingActions { expected, received },
                ..
            }] if expected.len() == 1 && received.len() == 2
        ));
    }

    #[test]
    fn imported_finish_projection_accepts_later_full_manifest_once() {
        let first = chunk(4, 9);
        let second = chunk(5, 9);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), first, &[ServerId(1)]);
        let partial = TickFinish::from_batch(&action(1, 1, first));
        assert_eq!(
            ledger.note_finish(ServerId(1), partial.clone()),
            FinishResult::Recorded
        );
        ledger.mark_imported_finish(TickStamp(7), ServerId(1));
        let mut full = partial.clone();
        full.chunks
            .extend(TickFinish::from_batch(&action(1, 2, second)).chunks);
        assert_eq!(
            ledger.note_finish(ServerId(1), full.clone()),
            FinishResult::Recorded
        );
        assert_eq!(
            ledger.note_finish(ServerId(1), full.clone()),
            FinishResult::Duplicate
        );
        let mut conflicting = full;
        conflicting.chunks[0].actions.clear();
        assert_eq!(
            ledger.note_finish(ServerId(1), conflicting),
            FinishResult::Conflicting
        );
    }

    #[test]
    fn finish_wire_rejects_untagged_and_oversized_payloads() {
        let chunk = chunk(-4, 12);
        let finish = TickFinish::from_batch(&action(3, 19, chunk));
        let encoded = encode_finish(&finish).expect("encode finish");
        assert_eq!(decode_finish(&encoded).expect("decode finish"), finish);
        assert!(decode_finish(&encoded[FINISH_PREFIX.len()..]).is_err());
        assert!(decode_finish(&vec![0_u8; MAX_FINISH_BYTES + 1]).is_err());
    }

    #[test]
    fn realm_finish_wire_preserves_joint_action_identity_and_rejects_bad_framing() {
        use crate::canonical_action::{ActionId, Stakeholder};
        use crate::realm_completion::{HolderDomain, HolisticFinish, RequirementKey};

        let id = ActionId {
            actor: ActorId::Player(gid(1)),
            incarnation: 17,
            sequence: 19,
            family: 3,
        };
        let finish = HolisticFinish {
            full_source_slot: u64::from((TickStamp(7)).0),
            acquisition_prefix: None,
            tick: TickStamp(7),
            issued: BTreeMap::from([
                (
                    RequirementKey {
                        stakeholder: Stakeholder::Chunk(chunk(1, 1)),
                        domain: HolderDomain::Chunk { epoch: 9 },
                    },
                    BTreeSet::from([id]),
                ),
                (
                    RequirementKey {
                        stakeholder: Stakeholder::Chunk(chunk(2, 1)),
                        domain: HolderDomain::Chunk { epoch: 12 },
                    },
                    BTreeSet::from([id]),
                ),
            ]),
        };
        let encoded = encode_realm_finish(&finish).unwrap();
        assert_eq!(
            decode_message(&encoded).unwrap(),
            CompletionMessage::RealmFinish(finish)
        );
        assert!(decode_realm_finish(&encoded[REALM_FINISH_PREFIX.len()..]).is_err());
        assert!(decode_realm_finish(&vec![0_u8; MAX_FINISH_BYTES + 1]).is_err());
    }

    #[test]
    fn dropped_peer_report_distinguishes_missing_action_from_missing_finish() {
        let first = chunk(1, 1);
        let second = chunk(2, 2);
        let holders = [ServerId(1), ServerId(2)];
        let complete = action(1, 1, first);
        let missing_action = action(2, 2, first);
        let mut missing_finish = action(2, 3, second);
        missing_finish.tick = TickStamp(8);
        missing_finish.break_block[0].tick = TickStamp(8);
        let mut ledger = CompletionLedger::default();
        ledger.require(TickStamp(7), first, &holders);
        ledger.require(TickStamp(8), second, &holders);
        ledger
            .note_batch(ServerId(1), &complete)
            .expect("valid manifest batch");
        ledger.note_finish(ServerId(1), TickFinish::from_batch(&complete));
        ledger.note_finish(ServerId(2), TickFinish::from_batch(&missing_action));
        ledger
            .note_batch(ServerId(2), &missing_finish)
            .expect("valid manifest batch");

        let gaps = ledger.gaps_for(ServerId(2));
        assert_eq!(gaps.len(), 2);
        assert!(matches!(gaps[0].state, DropGapState::MissingActions { .. }));
        assert!(matches!(gaps[1].state, DropGapState::MissingFinish { .. }));

        ledger.drop_peer(ServerId(2));
        assert!(ledger.chunk_complete(TickStamp(7), first));
        assert!(!ledger.chunk_complete(TickStamp(8), second));
    }

    #[test]
    fn drop_report_wire_preserves_retained_source_batches() {
        let batch = action(2, 11, chunk(7, -3));
        let report = DropReport {
            dropped: ServerId(2),
            gaps: Vec::new(),
            retained: vec![SourcedTickBatch {
                source: ServerId(2),
                batch,
            }],
            complete: true,
        };
        let encoded = encode_drop_report(&report).expect("encode drop report");
        assert_eq!(
            decode_message(&encoded).expect("decode drop report"),
            CompletionMessage::DropReport(report)
        );
        assert!(decode_drop_report(&vec![0_u8; MAX_DROP_REPORT_BYTES + 1]).is_err());
    }

    #[test]
    fn zero_action_evidence_is_bounded_but_remains_usable_for_recent_ticks() {
        let peer = ServerId(2);
        let mut ledger = CompletionLedger::default();
        for tick in 0..=MAX_HOLDERLESS_TICKS {
            ledger.note_finish(
                peer,
                TickFinish {
                    tick: TickStamp(tick as u16),
                    chunks: Vec::new(),
                },
            );
        }
        assert_eq!(ledger.tracked_ticks(), MAX_HOLDERLESS_TICKS);
        let recent = TickStamp(MAX_HOLDERLESS_TICKS as u16);
        let chunk = chunk(2, 5);
        ledger.require(recent, chunk, &[peer]);
        assert!(ledger.chunk_complete(recent, chunk));
    }
}
