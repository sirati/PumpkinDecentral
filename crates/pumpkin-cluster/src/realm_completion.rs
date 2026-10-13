use crate::structural_map::StructuralMap;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hasher;

use serde::{Deserialize, Serialize};

use crate::action_graph::ActionRealm;
use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionId, ActionRef, LivePlayerLease, PlayerKey, Stakeholder,
};
use crate::identity::ServerId;
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerGeneration {
    pub server: ServerId,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum HolderDomain {
    Chunk {
        epoch: u64,
    },
    LiveInventory {
        actor_chunk: ChunkKey,
        owner: ServerId,
        epoch: u64,
    },
    PolicyAllPeers {
        membership_epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RequirementKey {
    pub stakeholder: Stakeholder,
    pub domain: HolderDomain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolisticFinish {
    pub full_source_slot: u64,
    pub tick: TickStamp,
    pub acquisition_prefix: Option<u64>,
    pub issued: BTreeMap<RequirementKey, BTreeSet<ActionId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionFailure {
    EmptyParticipants,
    ConflictingParticipants,
    ConflictingFinish,
    MissingRequirement,
    MissingPeerIncarnation,
    SourceNotParticipant,
    SourceNotActorOwner,
    InvalidActionRequirements,
    UndeclaredAction,
}

#[derive(Clone, Default)]
pub struct RealmCompletionLedger {
    requirements: StructuralMap<(TickStamp, RequirementKey), BTreeSet<PeerGeneration>>,
    requirement_domains: StructuralMap<(TickStamp, Stakeholder, u8), RequirementKey>,
    producers: StructuralMap<(TickStamp, RequirementKey), BTreeSet<PeerGeneration>>,
    closure_peers: StructuralMap<(TickStamp, RequirementKey), BTreeSet<PeerGeneration>>,
    actor_owners: StructuralMap<(TickStamp, ActorId), PeerGeneration>,
    action_issuers: StructuralMap<ActionRef, PeerGeneration>,
    actor_incarnations: StructuralMap<(TickStamp, ActorId), u64>,
    player_owners: StructuralMap<(TickStamp, PlayerKey), PeerGeneration>,
    player_leases: StructuralMap<(TickStamp, PlayerKey), LivePlayerLease>,
    player_chunks: StructuralMap<(TickStamp, PlayerKey), ChunkKey>,
    finishes: StructuralMap<(TickStamp, PeerGeneration), Option<HolisticFinish>>,
    received: StructuralMap<(TickStamp, RequirementKey, PeerGeneration), BTreeSet<ActionId>>,
    action_requirements: StructuralMap<ActionRef, BTreeSet<RequirementKey>>,
    actions: StructuralMap<ActionRef, (PeerGeneration, Action)>,
    virtual_actor_requirements: StructuralMap<ActionRef, BTreeSet<RequirementKey>>,
    realm_requirements: StructuralMap<(TickStamp, ActionRealm), BTreeSet<RequirementKey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenRequirementSnapshot {
    pub tick: TickStamp,
    pub key: RequirementKey,
    pub participants: BTreeSet<PeerGeneration>,
    pub producers: BTreeSet<PeerGeneration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenPlayerSnapshot {
    pub tick: TickStamp,
    pub lease: LivePlayerLease,
    pub owner: PeerGeneration,
    pub chunk: ChunkKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenActionSnapshot {
    pub reference: ActionRef,
    pub source: PeerGeneration,
    pub requirements: BTreeSet<RequirementKey>,
    pub virtual_actor_requirements: BTreeSet<RequirementKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmCompletionSnapshot {
    pub requirements: Vec<FrozenRequirementSnapshot>,
    pub players: Vec<FrozenPlayerSnapshot>,
    pub actions: Vec<FrozenActionSnapshot>,
    pub finishes: Vec<(PeerGeneration, HolisticFinish)>,
    pub conflicting_finishes: Vec<(TickStamp, PeerGeneration)>,
}

impl RequirementKey {
    #[must_use]
    pub fn chunk(chunk: ChunkKey, participants: &BTreeSet<PeerGeneration>) -> Self {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
        for peer in participants {
            hasher.write(&peer.server.0.to_be_bytes());
            hasher.write(&peer.generation.to_be_bytes());
        }
        Self {
            stakeholder: Stakeholder::Chunk(chunk),
            domain: HolderDomain::Chunk {
                epoch: hasher.finish(),
            },
        }
    }

    #[must_use]
    pub fn valid(self) -> bool {
        matches!(
            (self.stakeholder, self.domain),
            (Stakeholder::Chunk(_), HolderDomain::Chunk { .. })
                | (Stakeholder::Player(_), HolderDomain::LiveInventory { .. })
                | (Stakeholder::Player(_), HolderDomain::PolicyAllPeers { .. })
        )
    }
}

fn holder_domain_kind(domain: HolderDomain) -> u8 {
    match domain {
        HolderDomain::Chunk { .. } => 0,
        HolderDomain::LiveInventory { .. } => 1,
        HolderDomain::PolicyAllPeers { .. } => 2,
    }
}

impl RealmCompletionLedger {
    pub fn validate_capture_subset(&self, candidate: &Self) -> Result<(), CompletionFailure> {
        fn conflicts<K: Ord + Clone, V: PartialEq + Clone>(
            current: &StructuralMap<K, V>,
            candidate: &StructuralMap<K, V>,
        ) -> bool {
            candidate
                .iter()
                .any(|(key, value)| current.get(key).is_some_and(|old| old != value))
        }
        if conflicts(&self.requirements, &candidate.requirements)
            || conflicts(&self.producers, &candidate.producers)
            || conflicts(&self.actor_owners, &candidate.actor_owners)
            || conflicts(&self.action_issuers, &candidate.action_issuers)
            || conflicts(&self.actor_incarnations, &candidate.actor_incarnations)
            || conflicts(&self.player_owners, &candidate.player_owners)
            || conflicts(&self.player_leases, &candidate.player_leases)
            || conflicts(&self.player_chunks, &candidate.player_chunks)
            || conflicts(&self.action_requirements, &candidate.action_requirements)
            || conflicts(&self.actions, &candidate.actions)
        {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        let kind = |domain| match domain {
            HolderDomain::Chunk { .. } => 0,
            HolderDomain::LiveInventory { .. } => 1,
            HolderDomain::PolicyAllPeers { .. } => 2,
        };
        if candidate.requirements.keys().any(|(tick, key)| {
            self.requirements.keys().any(|(old_tick, old)| {
                tick == old_tick
                    && key.stakeholder == old.stakeholder
                    && kind(key.domain) == kind(old.domain)
                    && key != old
            })
        }) {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        for (reference, (source, _)) in &candidate.actions {
            if let Some(finish) = self.finishes.get(&(reference.tick, *source)) {
                let finish = finish
                    .as_ref()
                    .ok_or(CompletionFailure::ConflictingFinish)?;
                if candidate
                    .action_requirements
                    .get(reference)
                    .is_none_or(|requirements| {
                        requirements.iter().any(|key| {
                            finish
                                .issued
                                .get(key)
                                .is_none_or(|ids| !ids.contains(&reference.id))
                        })
                    })
                {
                    return Err(CompletionFailure::UndeclaredAction);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn snapshot_for_chunk(
        &self,
        chunk: ChunkKey,
        after: TickStamp,
        cut: TickStamp,
        references: &BTreeSet<ActionRef>,
    ) -> Option<RealmCompletionSnapshot> {
        let span = cut.distance_since(after);
        if span >= 32_768 {
            return None;
        }
        let mut snapshot = self.snapshot_for_actions(cut, references)?;
        let mut included = snapshot
            .requirements
            .iter()
            .map(|scope| (scope.tick, scope.key))
            .collect::<BTreeSet<_>>();
        let mut ticks = snapshot
            .requirements
            .iter()
            .map(|scope| scope.tick)
            .collect::<BTreeSet<_>>();
        for ((tick, key), participants) in &self.requirements {
            let distance = tick.distance_since(after);
            if key.stakeholder != Stakeholder::Chunk(chunk)
                || distance == 0
                || distance > span
                || !included.insert((*tick, *key))
            {
                continue;
            }
            snapshot.requirements.push(FrozenRequirementSnapshot {
                tick: *tick,
                key: *key,
                participants: participants.clone(),
                producers: self.producers.get(&(*tick, *key))?.clone(),
            });
            ticks.insert(*tick);
        }
        let mut finishes = snapshot
            .finishes
            .iter()
            .map(|(source, finish)| (finish.tick, *source))
            .collect::<BTreeSet<_>>();
        for ((tick, source), finish) in &self.finishes {
            if ticks.contains(tick) && finishes.insert((*tick, *source)) {
                if let Some(finish) = finish {
                    snapshot.finishes.push((*source, finish.clone()));
                }
            }
        }
        let mut conflicting_finishes = snapshot
            .conflicting_finishes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for ((tick, source), finish) in &self.finishes {
            if ticks.contains(tick) && finish.is_none() {
                conflicting_finishes.insert((*tick, *source));
            }
        }
        snapshot.conflicting_finishes = conflicting_finishes.into_iter().collect();
        snapshot
            .requirements
            .sort_by_key(|scope| (scope.tick, scope.key));
        snapshot
            .finishes
            .sort_by_key(|(source, finish)| (finish.tick, *source));
        if snapshot.requirements.len() > 32_768
            || snapshot.finishes.len() > 32_768
            || snapshot.conflicting_finishes.len() > 32_768
        {
            return None;
        }
        Some(snapshot)
    }

    #[must_use]
    pub fn snapshot_for_actions(
        &self,
        cut: TickStamp,
        references: &BTreeSet<ActionRef>,
    ) -> Option<RealmCompletionSnapshot> {
        if references.len() > 8_192
            || references
                .iter()
                .any(|reference| cut.distance_since(reference.tick) >= 32_768)
        {
            return None;
        }
        let mut keys = BTreeSet::new();
        let mut players = BTreeMap::new();
        let mut actions = Vec::with_capacity(references.len());
        let mut ticks = BTreeSet::new();
        for reference in references {
            let requirements = self.action_requirements.get(reference)?.clone();
            let (source, action) = self.actions.get(reference)?;
            if action.id != reference.id || action.tick != reference.tick {
                return None;
            }
            keys.extend(
                requirements
                    .iter()
                    .copied()
                    .map(|key| (reference.tick, key)),
            );
            ticks.insert(reference.tick);
            for lease in &action.player_leases {
                let owner = *self.player_owners.get(&(reference.tick, lease.key))?;
                let chunk = *self.player_chunks.get(&(reference.tick, lease.key))?;
                if self.player_leases.get(&(reference.tick, lease.key)) != Some(lease) {
                    return None;
                }
                players.insert(
                    (reference.tick, *lease),
                    FrozenPlayerSnapshot {
                        tick: reference.tick,
                        lease: *lease,
                        owner,
                        chunk,
                    },
                );
            }
            let virtual_actor_requirements = self
                .virtual_actor_requirements
                .get(reference)
                .cloned()
                .unwrap_or_default();
            keys.extend(
                virtual_actor_requirements
                    .iter()
                    .copied()
                    .map(|key| (reference.tick, key)),
            );
            actions.push(FrozenActionSnapshot {
                reference: *reference,
                source: *source,
                requirements,
                virtual_actor_requirements,
            });
        }
        let requirements = keys
            .into_iter()
            .map(|(tick, key)| {
                Some(FrozenRequirementSnapshot {
                    tick,
                    key,
                    participants: self.requirements.get(&(tick, key))?.clone(),
                    producers: self.producers.get(&(tick, key))?.clone(),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let finishes = self
            .finishes
            .iter()
            .filter_map(|((tick, source), finish)| {
                ticks
                    .contains(tick)
                    .then(|| finish.as_ref().map(|finish| (*source, finish.clone())))
                    .flatten()
            })
            .collect();
        let conflicting_finishes = self
            .finishes
            .iter()
            .filter_map(|((tick, source), finish)| {
                (ticks.contains(tick) && finish.is_none()).then_some((*tick, *source))
            })
            .collect();
        Some(RealmCompletionSnapshot {
            requirements,
            players: players.into_values().collect(),
            actions,
            finishes,
            conflicting_finishes,
        })
    }

    pub fn import_snapshot(
        &mut self,
        snapshot: &RealmCompletionSnapshot,
        actions: &BTreeMap<ActionRef, Action>,
    ) -> Result<(), CompletionFailure> {
        if snapshot.actions.len() > 8_192
            || snapshot.requirements.len() > 32_768
            || snapshot.players.len() > 8_192
            || snapshot.finishes.len() > 32_768
            || snapshot.conflicting_finishes.len() > 32_768
            || snapshot
                .actions
                .iter()
                .map(|entry| entry.reference)
                .collect::<BTreeSet<_>>()
                != actions.keys().copied().collect()
            || snapshot.finishes.iter().any(|(source, finish)| {
                snapshot
                    .conflicting_finishes
                    .contains(&(finish.tick, *source))
            })
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let mut candidate = self.clone();
        for player in &snapshot.players {
            candidate.capture_player_lease(player.tick, player.lease, player.owner)?;
            candidate.capture_player_chunk(player.tick, player.lease.key, player.chunk)?;
        }
        let mut requirements = snapshot.requirements.clone();
        requirements.sort_by_key(|scope| {
            (
                !matches!(scope.key.stakeholder, Stakeholder::Chunk(_)),
                scope.tick,
                scope.key,
            )
        });
        for scope in requirements {
            if let Stakeholder::Chunk(chunk) = scope.key.stakeholder
                && RequirementKey::chunk(chunk, &scope.participants) != scope.key
            {
                return Err(CompletionFailure::InvalidActionRequirements);
            }
            candidate.capture_with_producers(
                scope.tick,
                scope.key,
                scope.participants,
                scope.producers,
            )?;
        }
        for entry in &snapshot.actions {
            let action = actions
                .get(&entry.reference)
                .ok_or(CompletionFailure::InvalidActionRequirements)?;
            candidate.capture_action_issuer(action, entry.source)?;
            candidate.note_action(entry.source, action, entry.requirements.clone())?;
            if !entry.virtual_actor_requirements.is_empty() {
                candidate.capture_virtual_actor_requirements(
                    entry.reference,
                    entry.virtual_actor_requirements.clone(),
                )?;
            }
        }
        for (source, finish) in &snapshot.finishes {
            candidate.note_finish(*source, finish.clone())?;
        }
        for (tick, source) in &snapshot.conflicting_finishes {
            candidate.finishes.insert((*tick, *source), None);
        }
        *self = candidate;
        Ok(())
    }

    #[must_use]
    pub fn snapshot_finishes_through(
        &self,
        cut: TickStamp,
    ) -> Vec<(PeerGeneration, HolisticFinish)> {
        self.finishes
            .iter()
            .filter_map(|((tick, peer), finish)| {
                (cut.distance_since(*tick) < 32_768)
                    .then(|| finish.as_ref().map(|finish| (*peer, finish.clone())))
                    .flatten()
            })
            .collect()
    }

    pub fn capture_chunk_action(
        &mut self,
        source: PeerGeneration,
        action: &Action,
        scopes: Vec<(ChunkKey, BTreeSet<PeerGeneration>, BTreeSet<PeerGeneration>)>,
    ) -> Result<BTreeSet<RequirementKey>, CompletionFailure> {
        let had_actor_owner = self
            .actor_owners
            .contains_key(&(action.tick, action.id.actor));
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        let had_action_issuer = self.action_issuers.contains_key(&reference);
        self.capture_action_issuer(action, source)?;
        let mut inserted = Vec::new();
        let result = (|| {
            let mut requirements = BTreeSet::new();
            for (chunk, holders, producers) in scopes {
                let key = RequirementKey::chunk(chunk, &holders);
                let existed = self.requirements.contains_key(&(action.tick, key));
                self.capture_with_producers(action.tick, key, holders, producers)?;
                if !existed {
                    inserted.push(key);
                }
                requirements.insert(key);
            }
            self.note_action(source, action, requirements.clone())?;
            Ok(requirements)
        })();
        if result.is_err() {
            for key in inserted {
                self.requirements.remove(&(action.tick, key));
                self.requirement_domains.remove(&(action.tick, key.stakeholder, holder_domain_kind(key.domain)));
                self.producers.remove(&(action.tick, key));
                self.closure_peers.remove(&(action.tick, key));
            }
            if !had_actor_owner {
                self.actor_owners.remove(&(action.tick, action.id.actor));
            }
            if !had_action_issuer {
                self.action_issuers.remove(&reference);
            }
        }
        result
    }

    pub fn capture_chunk(
        &mut self,
        tick: TickStamp,
        chunk: ChunkKey,
        holders: BTreeSet<PeerGeneration>,
        producers: BTreeSet<PeerGeneration>,
    ) -> Result<RequirementKey, CompletionFailure> {
        let key = RequirementKey::chunk(chunk, &holders);
        self.capture_with_producers(tick, key, holders, producers)?;
        Ok(key)
    }

    pub fn capture_actor_owner(
        &mut self,
        tick: TickStamp,
        actor: ActorId,
        owner: PeerGeneration,
    ) -> Result<(), CompletionFailure> {
        match self.actor_owners.get(&(tick, actor)) {
            Some(existing) if *existing == owner => Ok(()),
            Some(_) => Err(CompletionFailure::ConflictingParticipants),
            None => {
                self.actor_owners.insert((tick, actor), owner);
                Ok(())
            }
        }
    }

    pub fn capture_action_issuer(
        &mut self,
        action: &Action,
        source: PeerGeneration,
    ) -> Result<(), CompletionFailure> {
        if matches!(action.id.actor, ActorId::Entity(_)) && action.conditions.iter().any(|condition| matches!(&condition.expected, crate::canonical_action::Expected::ActorControlled { issuer, .. } if *issuer == source)) {
            if action.validate().is_err() {
                return Err(CompletionFailure::InvalidActionRequirements);
            }
            let reference = ActionRef { tick: action.tick, id: action.id };
            match self.action_issuers.get(&reference) {
                Some(existing) if *existing != source => Err(CompletionFailure::ConflictingParticipants),
                _ => { self.action_issuers.insert(reference, source); Ok(()) }
            }
        } else {
            self.capture_actor_owner(action.tick, action.id.actor, source)
        }
    }

    pub fn capture_player_owner(
        &mut self,
        tick: TickStamp,
        player: PlayerKey,
        owner: PeerGeneration,
    ) -> Result<(), CompletionFailure> {
        match self.player_owners.get(&(tick, player)) {
            Some(existing) if *existing == owner => Ok(()),
            Some(_) => Err(CompletionFailure::ConflictingParticipants),
            None => {
                self.player_owners.insert((tick, player), owner);
                Ok(())
            }
        }
    }

    pub fn capture_player_lease(
        &mut self,
        tick: TickStamp,
        lease: LivePlayerLease,
        owner: PeerGeneration,
    ) -> Result<(), CompletionFailure> {
        if lease.gid.server != owner.server || lease.incarnation == 0 {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        if self
            .player_leases
            .get(&(tick, lease.key))
            .is_some_and(|existing| *existing != lease)
        {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        self.capture_player_owner(tick, lease.key, owner)?;
        self.player_leases.insert((tick, lease.key), lease);
        Ok(())
    }

    pub fn capture_player_chunk(
        &mut self,
        tick: TickStamp,
        player: PlayerKey,
        chunk: ChunkKey,
    ) -> Result<(), CompletionFailure> {
        match self.player_chunks.get(&(tick, player)) {
            Some(existing) if *existing == chunk => Ok(()),
            Some(_) => Err(CompletionFailure::ConflictingParticipants),
            None => {
                self.player_chunks.insert((tick, player), chunk);
                Ok(())
            }
        }
    }

    pub fn capture(
        &mut self,
        tick: TickStamp,
        key: RequirementKey,
        participants: BTreeSet<PeerGeneration>,
    ) -> Result<(), CompletionFailure> {
        self.capture_with_producers(tick, key, participants.clone(), participants)
    }

    pub fn capture_with_producers(
        &mut self,
        tick: TickStamp,
        key: RequirementKey,
        participants: BTreeSet<PeerGeneration>,
        producers: BTreeSet<PeerGeneration>,
    ) -> Result<(), CompletionFailure> {
        if participants.is_empty() {
            return Err(CompletionFailure::EmptyParticipants);
        }
        if !key.valid() {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let mut servers = BTreeSet::new();
        if participants
            .iter()
            .any(|participant| !servers.insert(participant.server))
        {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        let mut producer_servers = BTreeSet::new();
        if producers.is_empty()
            || producers
                .iter()
                .any(|producer| !producer_servers.insert(producer.server))
        {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        if participants.iter().any(|participant| {
            producers
                .iter()
                .any(|producer| producer.server == participant.server && producer != participant)
        }) {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        let domain = (tick, key.stakeholder, holder_domain_kind(key.domain));
        if self.requirement_domains.get(&domain).is_some_and(|existing| *existing != key) {
            return Err(CompletionFailure::ConflictingParticipants);
        }
        if let HolderDomain::LiveInventory {
            actor_chunk,
            owner,
            epoch,
        } = key.domain
        {
            let Stakeholder::Player(player) = key.stakeholder else {
                return Err(CompletionFailure::InvalidActionRequirements);
            };
            let Some(frozen_owner) = self.player_owners.get(&(tick, player)) else {
                return Err(CompletionFailure::InvalidActionRequirements);
            };
            if frozen_owner.server != owner
                || self.player_leases.get(&(tick, player)).is_none_or(|lease| {
                    lease.gid.server != owner
                        || lease.incarnation == 0
                        || lease.incarnation != epoch
                })
                || self.player_chunks.get(&(tick, player)) != Some(&actor_chunk)
                || !participants.contains(frozen_owner)
                || !producers.contains(frozen_owner)
                || !self.requirement_domains.contains_key(&(tick, Stakeholder::Chunk(actor_chunk), 0))
            {
                return Err(CompletionFailure::InvalidActionRequirements);
            }
        }
        match self.requirements.get(&(tick, key)) {
            Some(existing)
                if existing == &participants
                    && self.producers.get(&(tick, key)) == Some(&producers) =>
            {
                Ok(())
            }
            Some(_) => Err(CompletionFailure::ConflictingParticipants),
            None => {
                self.closure_peers.insert(
                    (tick, key),
                    participants.union(&producers).copied().collect(),
                );
                self.requirements.insert((tick, key), participants);
                self.requirement_domains.insert(domain, key);
                self.producers.insert((tick, key), producers);
                Ok(())
            }
        }
    }

    pub fn note_finish(
        &mut self,
        source: PeerGeneration,
        finish: HolisticFinish,
    ) -> Result<(), CompletionFailure> {
        if source.generation == 0 || finish.full_source_slot as u16 != finish.tick.0
            || self.actions.iter().any(|(reference, (issuer, action))| reference.tick == finish.tick && *issuer == source
                && (action.source_slot_witnesses().iter().any(|slot| *slot != finish.full_source_slot)
                    || action.morning_witnesses().iter().any(|(_, morning)| morning.slot > finish.full_source_slot))) {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let key = (finish.tick, source);
        match self.finishes.get(&key) {
            Some(Some(existing)) if existing == &finish => Ok(()),
            Some(_) => {
                self.finishes.insert(key, None);
                Err(CompletionFailure::ConflictingFinish)
            }
            None => {
                self.finishes.insert(key, Some(finish));
                Ok(())
            }
        }
    }

    pub fn retained_source_slots(&self) -> impl Iterator<Item = u64> + '_ {
        self.finishes.iter().filter_map(|((tick, source), finish)| {
            let finish = finish.as_ref()?;
            let complete = finish.issued.iter().all(|(requirement, expected)| {
                expected.is_empty() || self.received.get(&(*tick, *requirement, *source))
                    .is_some_and(|received| expected.is_subset(received))
            });
            (!complete).then_some(finish.full_source_slot)
        })
    }

    pub fn action_source_slot(&self, reference: ActionRef) -> Option<u64> {
        let source = self.action_issuers.get(&reference).copied().or_else(|| self.actions.get(&reference).map(|(source, _)| *source))?;
        self.finishes.get(&(reference.tick, source))?.as_ref().map(|finish| finish.full_source_slot)
    }

    pub fn note_action(
        &mut self,
        source: PeerGeneration,
        action: &Action,
        requirements: BTreeSet<RequirementKey>,
    ) -> Result<(), CompletionFailure> {
        if self.finishes.get(&(action.tick, source)).and_then(Option::as_ref).is_some_and(|finish|
            action.source_slot_witnesses().iter().any(|slot| *slot != finish.full_source_slot)
                || action.morning_witnesses().iter().any(|(_, morning)| morning.slot > finish.full_source_slot)) {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        if action.conditions.iter().any(|condition| {
            matches!(&condition.expected,
            crate::canonical_action::Expected::ActorControlled { issuer, .. } if *issuer != source)
        }) || action.effects.iter().any(|effect| {
            matches!(effect,
            crate::canonical_action::Effect::ChangeDroppedItem { issuer, .. } if *issuer != source)
        }) {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        if self.actor_owners.get(&(action.tick, action.id.actor)) != Some(&source)
            && self.action_issuers.get(&ActionRef {
                tick: action.tick,
                id: action.id,
            }) != Some(&source)
        {
            return Err(CompletionFailure::SourceNotActorOwner);
        }
        if action
            .player_leases
            .iter()
            .any(|lease| self.player_leases.get(&(action.tick, lease.key)) != Some(lease))
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        if self
            .actor_incarnations
            .get(&(action.tick, action.id.actor))
            .is_some_and(|incarnation| *incarnation != action.id.incarnation)
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let stakeholders = requirements
            .iter()
            .map(|key| key.stakeholder)
            .collect::<BTreeSet<_>>();
        if action.validate().is_err()
            || requirements.is_empty()
            || requirements.iter().any(|key| !key.valid())
            || stakeholders != action.stakeholders.iter().copied().collect()
            || action.player_chunks.iter().any(|(player, chunk)| {
                self.player_chunks.get(&(action.tick, *player)) != Some(chunk)
            })
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let targets = action
            .targets()
            .into_iter()
            .chain(action.counterfactual_conditions.iter().copied())
            .chain(action.counterfactual_effects.iter().copied())
            .collect::<Vec<_>>();
        for stakeholder in &action.stakeholders {
            let domains = requirements
                .iter()
                .filter(|key| &key.stakeholder == stakeholder)
                .map(|key| key.domain)
                .collect::<Vec<_>>();
            let valid = match stakeholder {
                Stakeholder::Chunk(_) => {
                    domains.len() == 1 && matches!(domains[0], HolderDomain::Chunk { .. })
                }
                Stakeholder::Player(player) => {
                    let inventory = targets.iter().any(|target| {
                        matches!(target, crate::canonical_action::Target::Player { key, inventory: Some(_), .. } if key == player)
                    });
                    let policy = targets.iter().any(|target| {
                        matches!(target, crate::canonical_action::Target::Player { key, inventory: None, .. } if key == player)
                    });
                    domains.len() == usize::from(inventory) + usize::from(policy)
                        && (!inventory
                            || domains
                                .iter()
                                .any(|domain| matches!(domain, HolderDomain::LiveInventory { .. })))
                        && (!policy
                            || domains.iter().any(|domain| {
                                matches!(domain, HolderDomain::PolicyAllPeers { .. })
                            }))
                        && domains.iter().all(|domain| match domain {
                            HolderDomain::LiveInventory {
                                actor_chunk, owner, ..
                            } => {
                                action
                                    .stakeholders
                                    .contains(&Stakeholder::Chunk(*actor_chunk))
                                    && self.player_chunks.get(&(action.tick, *player))
                                        == Some(actor_chunk)
                                    && self
                                        .player_owners
                                        .get(&(action.tick, *player))
                                        .is_some_and(|frozen| frozen.server == *owner)
                            }
                            HolderDomain::PolicyAllPeers { .. } => true,
                            HolderDomain::Chunk { .. } => false,
                        })
                }
            };
            if !valid {
                return Err(CompletionFailure::InvalidActionRequirements);
            }
        }
        for key in &requirements {
            if !self.requirements.contains_key(&(action.tick, *key)) {
                return Err(CompletionFailure::MissingRequirement);
            }
            let Some(producers) = self.producers.get(&(action.tick, *key)) else {
                return Err(CompletionFailure::MissingRequirement);
            };
            if !producers.contains(&source) {
                return Err(CompletionFailure::SourceNotParticipant);
            }
        }
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        if self
            .action_requirements
            .get(&reference)
            .is_some_and(|existing| existing != &requirements)
            || self
                .actions
                .get(&reference)
                .is_some_and(|existing| existing != &(source, action.clone()))
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        if self
            .finishes
            .get(&(action.tick, source))
            .and_then(Option::as_ref)
            .is_some_and(|finish| {
                requirements.iter().any(|key| {
                    !finish
                        .issued
                        .get(key)
                        .is_some_and(|actions| actions.contains(&action.id))
                })
            })
        {
            return Err(CompletionFailure::UndeclaredAction);
        }
        self.action_requirements
            .insert(reference, requirements.clone());
        self.actor_incarnations
            .insert((action.tick, action.id.actor), action.id.incarnation);
        self.actions.insert(reference, (source, action.clone()));
        for target in targets {
            self.realm_requirements
                .entry((action.tick, target.action_realm()))
                .or_default()
                .extend(
                    requirements
                        .iter()
                        .filter(|key| key.stakeholder == target.stakeholder())
                        .copied(),
                );
        }
        for key in &requirements {
            let realm = match key.stakeholder {
                Stakeholder::Chunk(chunk) => ActionRealm::Chunk(chunk),
                Stakeholder::Player(player) => ActionRealm::Player(player),
            };
            self.realm_requirements
                .entry((action.tick, realm))
                .or_default()
                .insert(*key);
        }
        for key in &requirements {
            self.received
                .entry((action.tick, *key, source))
                .or_default()
                .insert(action.id);
        }
        Ok(())
    }

    #[must_use]
    pub fn requirement_complete(&self, tick: TickStamp, key: RequirementKey) -> bool {
        let Some(closure_peers) = self.closure_peers.get(&(tick, key)) else {
            return false;
        };
        closure_peers.iter().all(|source| {
            let Some(Some(finish)) = self.finishes.get(&(tick, *source)) else {
                return false;
            };
            let expected = finish.issued.get(&key);
            let received = self.received.get(&(tick, key, *source));
            match (expected, received) {
                (None, None) => true,
                (Some(expected), Some(received)) => expected == received,
                (Some(expected), None) => expected.is_empty(),
                (None, Some(received)) => received.is_empty(),
            }
        })
    }

    #[must_use]
    pub fn chunk_complete(&self, tick: TickStamp, chunk: ChunkKey) -> bool {
        self.requirement_domains.get(&(tick, Stakeholder::Chunk(chunk), 0))
            .is_some_and(|key| self.requirement_complete(tick, *key))
    }

    #[must_use]
    pub fn chunk_producer_cut_complete(
        &self,
        tick: TickStamp,
        chunk: ChunkKey,
        holders: &BTreeSet<PeerGeneration>,
        producers: &BTreeSet<PeerGeneration>,
    ) -> bool {
        self.requirement_producer_cut_complete(
            tick,
            RequirementKey::chunk(chunk, holders),
            holders,
            producers,
        )
    }

    #[must_use]
    pub fn requirement_producer_cut_complete(
        &self,
        tick: TickStamp,
        key: RequirementKey,
        holders: &BTreeSet<PeerGeneration>,
        producers: &BTreeSet<PeerGeneration>,
    ) -> bool {
        if holders.is_empty() || producers.is_empty() || !holders.is_subset(producers) {
            return false;
        }
        if !key.valid() {
            return false;
        }
        if self.requirements.get(&(tick, key)) != Some(holders)
            || self.producers.get(&(tick, key)) != Some(producers)
        {
            return false;
        }
        if self
            .requirements
            .iter()
            .any(|((entry_tick, entry_key), participants)| {
                *entry_tick == tick
                    && entry_key.stakeholder == key.stakeholder
                    && (*entry_key != key || participants != holders)
            })
        {
            return false;
        }
        for source in producers {
            let Some(Some(finish)) = self.finishes.get(&(tick, *source)) else {
                return false;
            };
            if finish
                .issued
                .keys()
                .any(|entry| entry.stakeholder == key.stakeholder && *entry != key)
            {
                return false;
            }
            let expected = finish.issued.get(&key);
            let received = self.received.get(&(tick, key, *source));
            let matched = match (expected, received) {
                (None, None) => true,
                (Some(expected), Some(received)) => expected == received,
                (Some(expected), None) => expected.is_empty(),
                (None, Some(received)) => received.is_empty(),
            };
            if !matched {
                return false;
            }
            if expected.is_some_and(|ids| {
                ids.iter().any(|id| {
                    let reference = ActionRef { tick, id: *id };
                    self.actions
                        .get(&reference)
                        .is_none_or(|(issuer, _)| issuer != source)
                        || self
                            .action_requirements
                            .get(&reference)
                            .is_none_or(|requirements| !requirements.contains(&key))
                })
            }) {
                return false;
            }
        }
        !self.received.keys().any(|(entry_tick, entry_key, source)| {
            *entry_tick == tick && *entry_key == key && !producers.contains(source)
        })
    }

    #[must_use]
    pub fn all_producers_finished(
        &self,
        tick: TickStamp,
        producers: &BTreeSet<PeerGeneration>,
    ) -> bool {
        !producers.is_empty()
            && producers.iter().all(|source| {
                self.finishes
                    .get(&(tick, *source))
                    .is_some_and(Option::is_some)
            })
    }

    #[must_use]
    pub fn chunk_producer_range_complete(
        &self,
        after: TickStamp,
        through: TickStamp,
        chunk: ChunkKey,
        scopes: &BTreeMap<TickStamp, (BTreeSet<PeerGeneration>, BTreeSet<PeerGeneration>)>,
    ) -> bool {
        let span = through.distance_since(after);
        if span == 0 || span >= 32_768 {
            return false;
        }
        (1..=span).all(|offset| {
            let tick = TickStamp(after.0.wrapping_add(offset));
            scopes.get(&tick).is_some_and(|(holders, producers)| {
                self.chunk_producer_cut_complete(tick, chunk, holders, producers)
            })
        })
    }

    #[must_use]
    pub fn requirement_producer_range_complete(
        &self,
        after: TickStamp,
        through: TickStamp,
        scopes: &BTreeMap<
            TickStamp,
            (
                RequirementKey,
                BTreeSet<PeerGeneration>,
                BTreeSet<PeerGeneration>,
            ),
        >,
    ) -> bool {
        let span = through.distance_since(after);
        if span == 0 || span >= 32_768 {
            return false;
        }
        (1..=span).all(|offset| {
            let tick = TickStamp(after.0.wrapping_add(offset));
            scopes.get(&tick).is_some_and(|(key, holders, producers)| {
                self.requirement_producer_cut_complete(tick, *key, holders, producers)
            })
        })
    }

    #[must_use]
    pub fn action_complete(&self, reference: ActionRef) -> bool {
        self.action_requirements
            .get(&reference)
            .is_some_and(|requirements| {
                requirements
                    .iter()
                    .all(|key| self.requirement_complete(reference.tick, *key))
                    && self
                        .virtual_actor_requirements
                        .get(&reference)
                        .is_none_or(|virtual_keys| {
                            let mut coverage = requirements.clone();
                            coverage.extend(virtual_keys.iter().copied());
                            virtual_keys
                                .iter()
                                .all(|key| self.requirement_complete(reference.tick, *key))
                                && self.actor_manifests_complete(
                                    reference.tick,
                                    reference.id.actor,
                                    &coverage,
                                )
                        })
            })
    }

    pub fn capture_virtual_actor_requirements(
        &mut self,
        reference: ActionRef,
        requirements: BTreeSet<RequirementKey>,
    ) -> Result<(), CompletionFailure> {
        let (_, action) = self
            .actions
            .get(&reference)
            .ok_or(CompletionFailure::InvalidActionRequirements)?;
        let ActorId::Entity(entity) = action.id.actor else {
            return Err(CompletionFailure::InvalidActionRequirements);
        };
        if requirements.is_empty()
            || requirements.len() > 128
            || requirements.iter().any(|key| {
                !matches!(
                    (key.stakeholder, key.domain),
                    (Stakeholder::Chunk(_), HolderDomain::Chunk { .. })
                ) || !self.requirements.contains_key(&(reference.tick, *key))
                    || !self.producers.contains_key(&(reference.tick, *key))
            })
            || self
                .virtual_actor_requirements
                .get(&reference)
                .is_some_and(|existing| existing != &requirements)
        {
            return Err(CompletionFailure::InvalidActionRequirements);
        }
        let mut coverage = self
            .action_requirements
            .get(&reference)
            .ok_or(CompletionFailure::InvalidActionRequirements)?
            .clone();
        coverage.extend(requirements.iter().copied());
        for key in &requirements {
            let Stakeholder::Chunk(chunk) = key.stakeholder else {
                unreachable!()
            };
            self.realm_requirements
                .entry((reference.tick, ActionRealm::Entity(chunk, entity)))
                .or_default()
                .extend(coverage.iter().copied());
            self.realm_requirements
                .entry((reference.tick, ActionRealm::Chunk(chunk)))
                .or_default()
                .extend(coverage.iter().copied());
        }
        self.virtual_actor_requirements
            .insert(reference, requirements);
        Ok(())
    }

    pub fn actor_manifests_complete(
        &self,
        tick: TickStamp,
        actor: ActorId,
        requirements: &BTreeSet<RequirementKey>,
    ) -> bool {
        if requirements.is_empty() {
            return false;
        }
        let mut producers = BTreeSet::new();
        for key in requirements {
            if !self.requirements.contains_key(&(tick, *key)) {
                return false;
            }
            let Some(scope) = self.producers.get(&(tick, *key)) else {
                return false;
            };
            producers.extend(scope.iter().copied());
        }
        !producers.is_empty()
            && producers.iter().all(|source| {
                let Some(Some(finish)) = self.finishes.get(&(tick, *source)) else {
                    return false;
                };
                finish
                    .issued
                    .values()
                    .flatten()
                    .filter(|id| id.actor == actor)
                    .all(|id| {
                        self.actions
                            .get(&ActionRef { tick, id: *id })
                            .is_some_and(|(issuer, _)| issuer == source)
                    })
            })
    }

    #[must_use]
    pub fn realm_complete(&self, tick: TickStamp, realm: ActionRealm) -> bool {
        let parent = match realm {
            ActionRealm::Chunk(chunk) | ActionRealm::Entity(chunk, _) | ActionRealm::PlayerAvatar(chunk, _)
            | ActionRealm::Inventory(Stakeholder::Chunk(chunk), _) => Some(chunk),
            ActionRealm::Player(player) | ActionRealm::Inventory(Stakeholder::Player(player), _) => self.player_chunks.get(&(tick, player)).copied(),
        };
        let parent_complete = parent.map_or_else(
            || matches!(realm, ActionRealm::Player(_)) && self.realm_requirements.get(&(tick, realm))
                .is_some_and(|requirements| !requirements.is_empty() && requirements.iter().all(|key| matches!(key.domain, HolderDomain::PolicyAllPeers { .. }))),
            |chunk| self.chunk_complete(tick, chunk),
        );
        parent_complete && self.realm_requirements
            .get(&(tick, realm))
            .is_some_and(|requirements| {
                !requirements.is_empty()
                    && requirements
                        .iter()
                        .all(|key| self.requirement_complete(tick, *key))
                    && match realm {
                        ActionRealm::Entity(_, entity) => self.actor_manifests_complete(
                            tick,
                            ActorId::Entity(entity),
                            requirements,
                        ),
                        _ => true,
                    }
            })
    }

    #[must_use]
    pub fn requirements_for(&self, reference: ActionRef) -> Option<&BTreeSet<RequirementKey>> {
        self.action_requirements.get(&reference)
    }

    pub fn virtual_actor_requirements_for(
        &self,
        reference: ActionRef,
    ) -> Option<&BTreeSet<RequirementKey>> {
        self.virtual_actor_requirements.get(&reference)
    }

    pub fn retire_tick(&mut self, tick: TickStamp) {
        self.virtual_actor_requirements.remove_prefix(tick);
        self.requirements.remove_prefix(tick);
        self.requirement_domains.remove_prefix(tick);
        self.producers.remove_prefix(tick);
        self.closure_peers.remove_prefix(tick);
        self.actor_owners.remove_prefix(tick);
        self.action_issuers.remove_prefix(tick);
        self.actor_incarnations.remove_prefix(tick);
        self.player_owners.remove_prefix(tick);
        self.player_leases.remove_prefix(tick);
        self.player_chunks.remove_prefix(tick);
        self.finishes.remove_prefix(tick);
        self.received.remove_prefix(tick);
        self.action_requirements.remove_prefix(tick);
        self.actions.remove_prefix(tick);
        self.realm_requirements.remove_prefix(tick);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::{
        ActionClass, ActionKind, ChunkPart, ClientOrigin, Condition, Effect, Expected, PlayerKey,
        Target, UndoProcedure, Value,
    };
    use crate::identity::{GlobalPlayerId, PlayerSlot};
    use crate::protocol::{BlockPos, WorldId};
    use uuid::Uuid;

    fn joint_action() -> (Action, RequirementKey, RequirementKey) {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let player = PlayerKey(Uuid::from_u128(8));
        let chunk_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 80, z: 1 }),
            inventory: None,
        };
        let player_target = Target::Player {
            key: player,
            lease: None,
            inventory: None,
        };
        let mut stakeholders = vec![Stakeholder::Chunk(chunk), Stakeholder::Player(player)];
        stakeholders.sort();
        let action = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Peer(ServerId(1)),
                incarnation: 0,
                sequence: 5,
                family: 1,
            },
            tick: TickStamp(7),
            class: ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders,
            dependencies: Vec::new(),
            conditions: vec![
                Condition {
                    target: chunk_target,
                    expected: Expected::Missing,
                },
                Condition {
                    target: player_target,
                    expected: Expected::Missing,
                },
            ],
            effects: vec![
                Effect::Put {
                    target: chunk_target,
                    value: Value::Block(1),
                },
                Effect::Put {
                    target: player_target,
                    value: Value::Scalar(1),
                },
            ],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::World,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate().unwrap();
        let chunk_key = RequirementKey {
            stakeholder: Stakeholder::Chunk(chunk),
            domain: HolderDomain::Chunk { epoch: 3 },
        };
        let player_key = RequirementKey {
            stakeholder: Stakeholder::Player(player),
            domain: HolderDomain::PolicyAllPeers {
                membership_epoch: 4,
            },
        };
        (action, chunk_key, player_key)
    }

    fn peer(server: u16) -> PeerGeneration {
        PeerGeneration {
            server: ServerId(server),
            generation: 1,
        }
    }

    #[test]
    fn environment_finish_roots_release_only_after_all_declared_actions_arrive() {
        let (action, requirement, _) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger.note_finish(peer(1), HolisticFinish { tick: action.tick,
            full_source_slot: u64::from(action.tick.0), acquisition_prefix: None,
            issued: BTreeMap::from([(requirement, BTreeSet::from([action.id]))]) }).unwrap();
        assert_eq!(ledger.retained_source_slots().collect::<Vec<_>>(), vec![u64::from(action.tick.0)]);
        ledger.received.insert((action.tick, requirement, peer(1)), BTreeSet::from([action.id]));
        assert!(ledger.retained_source_slots().next().is_none());
    }

    #[test]
    fn authenticated_finish_retains_full_slot_and_rejects_wrap_alias() {
        let mut ledger = RealmCompletionLedger::default();
        let finish = HolisticFinish { tick: TickStamp(7), full_source_slot: 65_543,
            acquisition_prefix: None, issued: BTreeMap::new() };
        let mut malformed = finish.clone();
        malformed.full_source_slot += 1;
        assert_eq!(ledger.note_finish(peer(1), malformed), Err(CompletionFailure::InvalidActionRequirements));
        assert!(ledger.finishes.is_empty());
        ledger.note_finish(peer(1), finish.clone()).unwrap();
        assert_eq!(ledger.finishes.get(&(finish.tick, peer(1))).and_then(Option::as_ref), Some(&finish));
        let mut alias = finish;
        alias.full_source_slot = 7;
        assert_eq!(ledger.note_finish(peer(1), alias), Err(CompletionFailure::ConflictingFinish));
        assert_eq!(ledger.finishes.get(&(TickStamp(7), peer(1))), Some(&None));
    }

    #[test]
    fn tentative_capture_respects_closed_manifest_without_mutating_ledger() {
        let (action, chunk, player) = joint_action();
        let mut candidate = RealmCompletionLedger::default();
        candidate
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        candidate
            .capture(action.tick, chunk, BTreeSet::from([peer(1), peer(2)]))
            .unwrap();
        candidate
            .capture(
                action.tick,
                player,
                BTreeSet::from([peer(1), peer(2), peer(3)]),
            )
            .unwrap();
        candidate
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        let mut current = RealmCompletionLedger::default();
        assert_eq!(current.validate_capture_subset(&candidate), Ok(()));
        assert!(current.actions.is_empty());
        current
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert_eq!(
            current.validate_capture_subset(&candidate),
            Err(CompletionFailure::UndeclaredAction)
        );
        assert!(current.actions.is_empty());
        assert!(current.requirements.is_empty());
        let mut existing = candidate.clone();
        existing
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        assert_eq!(existing.validate_capture_subset(&candidate), Ok(()));
        let mut changed = candidate;
        let changed_reference = *changed.actions.keys().next().unwrap();
        changed.actions.get_mut(&changed_reference).unwrap().0 = peer(2);
        assert_eq!(
            existing.validate_capture_subset(&changed),
            Err(CompletionFailure::ConflictingParticipants)
        );
    }

    #[test]
    fn actor_manifest_closure_requires_named_actions_from_other_domains() {
        let (mut first, first_key, _) = joint_action();
        first.stakeholders = vec![first_key.stakeholder];
        first
            .conditions
            .retain(|condition| condition.target.stakeholder() == first_key.stakeholder);
        first.effects.retain(|effect| {
            effect
                .targets()
                .all(|target| target.stakeholder() == first_key.stakeholder)
        });
        let mut second = first.clone();
        second.id.sequence += 1;
        let chunk = ChunkKey::new(WorldId::new(0, 0), 1, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 17, y: 80, z: 1 }),
            inventory: None,
        };
        second.actor_chunk = Some(chunk);
        second.stakeholders = vec![Stakeholder::Chunk(chunk)];
        second.conditions[0].target = target;
        second.effects = vec![Effect::Put {
            target,
            value: Value::Block(1),
        }];
        let participants = BTreeSet::from([peer(1)]);
        let second_key = RequirementKey::chunk(chunk, &participants);
        let mut ledger = RealmCompletionLedger::default();
        for key in [first_key, second_key] {
            ledger
                .capture_with_producers(first.tick, key, participants.clone(), participants.clone())
                .unwrap();
        }
        ledger
            .capture_actor_owner(first.tick, first.id.actor, peer(1))
            .unwrap();
        ledger
            .note_action(peer(1), &first, BTreeSet::from([first_key]))
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((first.tick).0),
                    acquisition_prefix: None,
                    tick: first.tick,
                    issued: BTreeMap::from([
                        (first_key, BTreeSet::from([first.id])),
                        (second_key, BTreeSet::from([second.id])),
                    ]),
                },
            )
            .unwrap();
        assert!(ledger.requirement_complete(first.tick, first_key));
        assert!(!ledger.actor_manifests_complete(
            first.tick,
            first.id.actor,
            &BTreeSet::from([first_key])
        ));
        ledger
            .note_action(peer(1), &second, BTreeSet::from([second_key]))
            .unwrap();
        assert!(ledger.actor_manifests_complete(
            first.tick,
            first.id.actor,
            &BTreeSet::from([first_key])
        ));
        assert!(!ledger.actor_manifests_complete(first.tick, first.id.actor, &BTreeSet::new()));
    }

    #[test]
    fn virtual_actor_coverage_keeps_original_declaration_and_round_trips() {
        let (mut action, _, _) = joint_action();
        let origin = action.actor_chunk.unwrap();
        action.id.actor = ActorId::Entity(crate::protocol::GlobalEntityId {
            origin: ServerId(1),
            local_id: 9,
            generation: 1,
        });
        action.id.incarnation = 1;
        action.class = ActionClass::Infallible;
        action.conditions.clear();
        action.player_leases.clear();
        action.stakeholders = vec![Stakeholder::Chunk(origin)];
        action.effects.retain(|effect| {
            effect
                .targets()
                .all(|target| target.stakeholder() == Stakeholder::Chunk(origin))
        });
        action.validate().unwrap();
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        let destination = ChunkKey::new(origin.world, 1, 0);
        let source_holders = BTreeSet::from([peer(1)]);
        let destination_holders = BTreeSet::from([peer(2)]);
        let source_key = RequirementKey::chunk(origin, &source_holders);
        let destination_key = RequirementKey::chunk(destination, &destination_holders);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_chunk_action(
                peer(1),
                &action,
                vec![(origin, source_holders.clone(), source_holders)],
            )
            .unwrap();
        assert!(
            ledger
                .capture_virtual_actor_requirements(reference, BTreeSet::from([destination_key]))
                .is_err()
        );
        ledger
            .capture_with_producers(
                action.tick,
                destination_key,
                destination_holders.clone(),
                destination_holders,
            )
            .unwrap();
        ledger
            .capture_virtual_actor_requirements(reference, BTreeSet::from([destination_key]))
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([(source_key, BTreeSet::from([action.id]))]),
                },
            )
            .unwrap();
        assert!(!ledger.action_complete(reference));
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(ledger.action_complete(reference));
        assert_eq!(
            ledger.requirements_for(reference),
            Some(&BTreeSet::from([source_key]))
        );
        let snapshot = ledger
            .snapshot_for_actions(action.tick, &BTreeSet::from([reference]))
            .unwrap();
        assert_eq!(
            snapshot.actions[0].requirements,
            BTreeSet::from([source_key])
        );
        assert_eq!(
            snapshot.actions[0].virtual_actor_requirements,
            BTreeSet::from([destination_key])
        );
        let mut restored = RealmCompletionLedger::default();
        restored
            .import_snapshot(&snapshot, &BTreeMap::from([(reference, action)]))
            .unwrap();
        assert!(restored.action_complete(reference));
        assert!(restored.realm_complete(
            reference.tick,
            ActionRealm::Entity(
                destination,
                match reference.id.actor {
                    ActorId::Entity(id) => id,
                    _ => unreachable!(),
                }
            )
        ));
        restored.retire_tick(reference.tick);
        assert!(restored.virtual_actor_requirements.is_empty());
    }

    #[test]
    fn chunk_scope_epoch_changes_with_peer_incarnation() {
        let (action, _, _) = joint_action();
        let chunk = action.actor_chunk.unwrap();
        let first = BTreeSet::from([peer(1), peer(2)]);
        let second = BTreeSet::from([
            peer(1),
            PeerGeneration {
                server: ServerId(2),
                generation: 2,
            },
        ]);
        assert_ne!(
            RequirementKey::chunk(chunk, &first),
            RequirementKey::chunk(chunk, &second)
        );
    }

    #[test]
    fn reused_player_slot_cannot_issue_two_incarnations_into_one_tick() {
        let (mut first, chunk_key, _) = joint_action();
        let gid = GlobalPlayerId {
            server: ServerId(1),
            player: PlayerSlot(7),
        };
        first.id.actor = ActorId::Player(gid);
        first.id.incarnation = 10;
        first
            .stakeholders
            .retain(|stakeholder| *stakeholder == chunk_key.stakeholder);
        first
            .conditions
            .retain(|condition| condition.target.stakeholder() == chunk_key.stakeholder);
        first.effects.retain(|effect| {
            effect
                .targets()
                .all(|target| target.stakeholder() == chunk_key.stakeholder)
        });
        first.validate().unwrap();
        let mut second = first.clone();
        second.id.incarnation = 11;
        second.id.sequence = first.id.sequence + 1;
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(first.tick, first.id.actor, peer(1))
            .unwrap();
        ledger
            .capture_with_producers(
                first.tick,
                chunk_key,
                BTreeSet::from([peer(1)]),
                BTreeSet::from([peer(1)]),
            )
            .unwrap();
        let requirements = BTreeSet::from([chunk_key]);
        ledger
            .note_action(peer(1), &first, requirements.clone())
            .unwrap();
        assert_eq!(
            ledger.note_action(peer(1), &second, requirements),
            Err(CompletionFailure::InvalidActionRequirements)
        );
        assert_eq!(ledger.actions.len(), 1);
    }

    #[test]
    fn zero_action_chunk_cut_needs_every_frozen_producer_finish() {
        let (action, _, _) = joint_action();
        let chunk = action.actor_chunk.unwrap();
        let holders = BTreeSet::from([peer(1)]);
        let producers = BTreeSet::from([peer(1), peer(2)]);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_chunk(action.tick, chunk, holders.clone(), producers.clone())
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.chunk_producer_cut_complete(action.tick, chunk, &holders, &producers));
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(ledger.chunk_producer_cut_complete(action.tick, chunk, &holders, &producers));
        let snapshot = ledger
            .snapshot_for_chunk(
                chunk,
                TickStamp(action.tick.0.wrapping_sub(1)),
                action.tick,
                &BTreeSet::new(),
            )
            .unwrap();
        let mut imported = RealmCompletionLedger::default();
        imported
            .import_snapshot(&snapshot, &BTreeMap::new())
            .unwrap();
        assert!(imported.chunk_producer_cut_complete(action.tick, chunk, &holders, &producers));
    }

    #[test]
    fn accepted_frontier_cannot_skip_a_missing_middle_tick() {
        let (action, _, _) = joint_action();
        let chunk = action.actor_chunk.unwrap();
        let after = TickStamp(u16::MAX - 1);
        let ticks = [TickStamp(u16::MAX), TickStamp(0), TickStamp(1)];
        let holders = BTreeSet::from([peer(1)]);
        let producers = BTreeSet::from([peer(1), peer(2)]);
        let mut scopes = BTreeMap::new();
        let mut ledger = RealmCompletionLedger::default();
        for tick in ticks {
            ledger
                .capture_chunk(tick, chunk, holders.clone(), producers.clone())
                .unwrap();
            scopes.insert(tick, (holders.clone(), producers.clone()));
            for source in &producers {
                if tick == TickStamp(0) && *source == peer(2) {
                    continue;
                }
                ledger
                    .note_finish(
                        *source,
                        HolisticFinish {
                            full_source_slot: u64::from((tick).0),
                            acquisition_prefix: None,
                            tick,
                            issued: BTreeMap::new(),
                        },
                    )
                    .unwrap();
            }
        }
        assert!(!ledger.chunk_producer_range_complete(after, TickStamp(1), chunk, &scopes,));
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((TickStamp(0)).0),
                    acquisition_prefix: None,
                    tick: TickStamp(0),
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(ledger.chunk_producer_range_complete(after, TickStamp(1), chunk, &scopes,));
    }

    #[test]
    fn chunk_cut_cannot_shrink_uncaptured_producer_scope() {
        let (action, _, _) = joint_action();
        let chunk = action.actor_chunk.unwrap();
        let holders = BTreeSet::from([peer(1)]);
        let full_producers = BTreeSet::from([peer(1), peer(2)]);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_chunk(action.tick, chunk, holders.clone(), full_producers.clone())
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.chunk_producer_cut_complete(
            action.tick,
            chunk,
            &holders,
            &BTreeSet::from([peer(1)])
        ));
        assert!(!ledger.chunk_producer_cut_complete(action.tick, chunk, &holders, &full_producers));
    }

    #[test]
    fn chunk_cut_needs_each_declared_action_before_export() {
        let (mut action, _, _) = joint_action();
        action
            .stakeholders
            .retain(|key| matches!(key, Stakeholder::Chunk(_)));
        action
            .conditions
            .retain(|condition| matches!(condition.target, Target::Chunk { .. }));
        action.effects.retain(|effect| {
            effect
                .targets()
                .all(|target| matches!(target, Target::Chunk { .. }))
        });
        action.validate().unwrap();
        let chunk = action.actor_chunk.unwrap();
        let holders = BTreeSet::from([peer(1)]);
        let producers = BTreeSet::from([peer(1), peer(2)]);
        let key = RequirementKey::chunk(chunk, &holders);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_chunk(action.tick, chunk, holders.clone(), producers.clone())
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([(key, BTreeSet::from([action.id]))]),
                },
            )
            .unwrap();
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.chunk_producer_cut_complete(action.tick, chunk, &holders, &producers));
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .note_action(peer(1), &action, BTreeSet::from([key]))
            .unwrap();
        assert!(ledger.chunk_producer_cut_complete(action.tick, chunk, &holders, &producers));
    }

    #[test]
    fn failed_joint_capture_keeps_prior_and_rolls_back_new_scope() {
        let (action, _, _) = joint_action();
        let first_chunk = action.actor_chunk.unwrap();
        let second_chunk = ChunkKey::new(first_chunk.world, first_chunk.x() + 1, first_chunk.z());
        let first_holders = BTreeSet::from([peer(1), peer(2)]);
        let second_holders = BTreeSet::from([peer(1), peer(3)]);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_chunk(
                action.tick,
                second_chunk,
                second_holders.clone(),
                second_holders,
            )
            .unwrap();
        assert_eq!(
            ledger.capture_chunk_action(
                peer(1),
                &action,
                vec![
                    (first_chunk, first_holders.clone(), first_holders),
                    (
                        second_chunk,
                        BTreeSet::from([peer(1)]),
                        BTreeSet::from([peer(1)])
                    ),
                ],
            ),
            Err(CompletionFailure::ConflictingParticipants)
        );
        assert!(!ledger.chunk_complete(action.tick, first_chunk));
        assert!(
            ledger
                .requirements
                .keys()
                .all(|(_, key)| key.stakeholder != Stakeholder::Chunk(first_chunk))
        );
        assert!(
            !ledger
                .actor_owners
                .contains_key(&(action.tick, action.id.actor))
        );
        assert!(
            ledger
                .requirements
                .keys()
                .any(|(_, key)| key.stakeholder == Stakeholder::Chunk(second_chunk))
        );
    }

    #[test]
    fn action_admission_failure_rolls_back_new_scope() {
        let (action, _, _) = joint_action();
        let chunk = action.actor_chunk.unwrap();
        let holders = BTreeSet::from([peer(1)]);
        let mut ledger = RealmCompletionLedger::default();
        assert_eq!(
            ledger.capture_chunk_action(peer(1), &action, vec![(chunk, holders.clone(), holders)],),
            Err(CompletionFailure::InvalidActionRequirements)
        );
        assert!(ledger.requirements.is_empty());
        assert!(ledger.actor_owners.is_empty());
        assert!(ledger.actions.is_empty());
    }

    #[test]
    fn zero_action_chunk_tick_closes_only_after_every_frozen_producer_finishes() {
        let (action, chunk, _) = joint_action();
        let Stakeholder::Chunk(chunk_id) = chunk.stakeholder else {
            unreachable!()
        };
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_with_producers(
                action.tick,
                chunk,
                BTreeSet::from([peer(1)]),
                BTreeSet::from([peer(1), peer(2)]),
            )
            .unwrap();
        assert!(!ledger.chunk_complete(action.tick, chunk_id));
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.chunk_complete(action.tick, chunk_id));
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(ledger.chunk_complete(action.tick, chunk_id));
    }

    #[test]
    fn holistic_finish_before_action_completes_only_after_every_joint_leg() {
        let (action, chunk, player) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1), peer(2)]))
            .unwrap();
        ledger
            .capture(
                action.tick,
                player,
                BTreeSet::from([peer(1), peer(2), peer(3)]),
            )
            .unwrap();
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.requirement_complete(action.tick, chunk));
        assert_eq!(
            ledger.note_action(peer(2), &action, BTreeSet::from([chunk, player])),
            Err(CompletionFailure::SourceNotActorOwner),
        );
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        assert!(ledger.requirement_complete(action.tick, chunk));
        assert!(!ledger.action_complete(reference));
        let chunk_realm = action
            .targets()
            .into_iter()
            .find(|target| matches!(target.stakeholder(), Stakeholder::Chunk(_)))
            .unwrap()
            .action_realm();
        let player_realm = action
            .targets()
            .into_iter()
            .find(|target| matches!(target.stakeholder(), Stakeholder::Player(_)))
            .unwrap()
            .action_realm();
        assert!(ledger.realm_complete(action.tick, chunk_realm));
        assert!(!ledger.realm_complete(action.tick, player_realm));
        ledger
            .note_finish(
                peer(3),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(ledger.requirement_complete(action.tick, player));
        assert!(ledger.realm_complete(action.tick, player_realm));
        assert!(ledger.action_complete(reference));
    }

    #[test]
    fn frozen_completion_snapshot_round_trips_joint_scope_and_finishes() {
        let (action, _, player) = joint_action();
        let holders = BTreeSet::from([peer(1), peer(2)]);
        let chunk = RequirementKey::chunk(action.actor_chunk.unwrap(), &holders);
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        let mut source = RealmCompletionLedger::default();
        source
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        source
            .capture_with_producers(action.tick, chunk, holders.clone(), holders)
            .unwrap();
        source
            .capture(
                action.tick,
                player,
                BTreeSet::from([peer(1), peer(2), peer(3)]),
            )
            .unwrap();
        source
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        source
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        for peer in [peer(2), peer(3)] {
            source
                .note_finish(
                    peer,
                    HolisticFinish {
                        full_source_slot: u64::from((action.tick).0),
                        acquisition_prefix: None,
                        tick: action.tick,
                        issued: BTreeMap::new(),
                    },
                )
                .unwrap();
        }
        let snapshot = source
            .snapshot_for_actions(action.tick, &BTreeSet::from([reference]))
            .unwrap();
        let mut imported = RealmCompletionLedger::default();
        imported
            .import_snapshot(&snapshot, &BTreeMap::from([(reference, action.clone())]))
            .unwrap();
        assert!(imported.action_complete(reference));
        let mut altered = snapshot;
        let chunk_scope = altered
            .requirements
            .iter_mut()
            .find(|scope| matches!(scope.key.stakeholder, Stakeholder::Chunk(_)))
            .unwrap();
        chunk_scope.key.domain = HolderDomain::Chunk { epoch: 0 };
        let mut rejected = RealmCompletionLedger::default();
        assert_eq!(
            rejected.import_snapshot(&altered, &BTreeMap::from([(reference, action)])),
            Err(CompletionFailure::InvalidActionRequirements)
        );
        assert!(rejected.actions.is_empty());
    }

    #[test]
    fn conflicting_finish_cannot_make_a_requirement_complete() {
        let (action, _, _) = joint_action();
        let chunk = RequirementKey::chunk(action.actor_chunk.unwrap(), &BTreeSet::from([peer(1)]));
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1)]))
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert_eq!(
            ledger.note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([(chunk, BTreeSet::from([action.id]))]),
                }
            ),
            Err(CompletionFailure::ConflictingFinish)
        );
        assert!(!ledger.requirement_complete(action.tick, chunk));
        let snapshot = ledger
            .snapshot_for_chunk(
                action.actor_chunk.unwrap(),
                TickStamp(action.tick.0.wrapping_sub(1)),
                action.tick,
                &BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(snapshot.conflicting_finishes, vec![(action.tick, peer(1))]);
        let mut imported = RealmCompletionLedger::default();
        imported
            .import_snapshot(&snapshot, &BTreeMap::new())
            .unwrap();
        assert!(!imported.requirement_complete(action.tick, chunk));
    }

    #[test]
    fn replacement_peer_generation_cannot_complete_frozen_older_vote() {
        let (action, chunk, _) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1)]))
            .unwrap();
        ledger
            .note_finish(
                PeerGeneration {
                    server: ServerId(1),
                    generation: 2,
                },
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.requirement_complete(action.tick, chunk));
    }

    #[test]
    fn omitted_action_in_prior_finish_does_not_change_ledger() {
        let (action, chunk, player) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1)]))
            .unwrap();
        ledger
            .capture(action.tick, player, BTreeSet::from([peer(1)]))
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert_eq!(
            ledger.note_action(peer(1), &action, BTreeSet::from([chunk, player])),
            Err(CompletionFailure::UndeclaredAction)
        );
        let reference = ActionRef {
            tick: action.tick,
            id: action.id,
        };
        assert!(ledger.requirements_for(reference).is_none());
        assert!(ledger.requirement_complete(action.tick, chunk));
        assert!(ledger.requirement_complete(action.tick, player));
    }

    #[test]
    fn actor_chunk_without_target_has_completion_realm() {
        let (mut action, chunk, player) = joint_action();
        let actor_chunk = ChunkKey::new(WorldId::new(0, 0), 1, 0);
        let actor_key = RequirementKey {
            stakeholder: Stakeholder::Chunk(actor_chunk),
            domain: HolderDomain::Chunk { epoch: 3 },
        };
        action.actor_chunk = Some(actor_chunk);
        action.stakeholders.push(Stakeholder::Chunk(actor_chunk));
        action.stakeholders.sort();
        action.validate().unwrap();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        for key in [chunk, player, actor_key] {
            ledger
                .capture(action.tick, key, BTreeSet::from([peer(1)]))
                .unwrap();
        }
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player, actor_key]))
            .unwrap();
        assert!(!ledger.realm_complete(action.tick, ActionRealm::Chunk(actor_chunk)));
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                        (actor_key, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        assert!(ledger.realm_complete(action.tick, ActionRealm::Chunk(actor_chunk)));
    }

    #[test]
    fn duplicate_server_generations_cannot_be_frozen_together() {
        let (action, chunk, _) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        assert_eq!(
            ledger.capture(
                action.tick,
                chunk,
                BTreeSet::from([
                    peer(1),
                    PeerGeneration {
                        server: ServerId(1),
                        generation: 2
                    },
                ])
            ),
            Err(CompletionFailure::ConflictingParticipants)
        );
    }

    #[test]
    fn same_tick_chunk_cannot_have_two_holder_epochs() {
        let (action, chunk, _) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1)]))
            .unwrap();
        assert_eq!(
            ledger.capture(
                action.tick,
                RequirementKey {
                    stakeholder: chunk.stakeholder,
                    domain: HolderDomain::Chunk { epoch: 4 },
                },
                BTreeSet::from([peer(2)]),
            ),
            Err(CompletionFailure::ConflictingParticipants),
        );
    }

    #[test]
    fn frozen_chunk_scope_rejects_holder_change_inside_tick() {
        let (action, chunk, _) = joint_action();
        let Stakeholder::Chunk(chunk_id) = chunk.stakeholder else {
            unreachable!()
        };
        let mut ledger = RealmCompletionLedger::default();
        let first = ledger
            .capture_chunk(
                action.tick,
                chunk_id,
                BTreeSet::from([peer(1), peer(2)]),
                BTreeSet::from([peer(1), peer(2)]),
            )
            .unwrap();
        assert_eq!(
            ledger.capture_chunk(
                action.tick,
                chunk_id,
                BTreeSet::from([peer(1), peer(3)]),
                BTreeSet::from([peer(1), peer(3)]),
            ),
            Err(CompletionFailure::ConflictingParticipants),
        );
        assert_eq!(
            first,
            RequirementKey::chunk(chunk_id, &BTreeSet::from([peer(2), peer(1)]))
        );
        let replacement = BTreeSet::from([
            PeerGeneration {
                server: ServerId(1),
                generation: 2,
            },
            peer(2),
        ]);
        assert_ne!(first, RequirementKey::chunk(chunk_id, &replacement));
        assert_eq!(
            ledger.capture_chunk(action.tick, chunk_id, replacement.clone(), replacement),
            Err(CompletionFailure::ConflictingParticipants),
        );
    }

    #[test]
    fn late_action_uses_owner_frozen_for_its_tick() {
        let (action, chunk, player) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture_actor_owner(TickStamp(action.tick.0 + 1), action.id.actor, peer(2))
            .unwrap();
        for key in [chunk, player] {
            ledger
                .capture(action.tick, key, BTreeSet::from([peer(1)]))
                .unwrap();
        }
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
    }

    #[test]
    fn outside_producer_must_close_before_holder_ground_can_advance() {
        let (action, chunk, player) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture_with_producers(
                action.tick,
                chunk,
                BTreeSet::from([peer(2)]),
                BTreeSet::from([peer(1), peer(2)]),
            )
            .unwrap();
        ledger
            .capture_with_producers(
                action.tick,
                player,
                BTreeSet::from([peer(2)]),
                BTreeSet::from([peer(1), peer(2)]),
            )
            .unwrap();
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(!ledger.requirement_complete(action.tick, chunk));
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        assert!(!ledger.requirement_complete(action.tick, chunk));
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        assert!(ledger.requirement_complete(action.tick, chunk));
        assert!(ledger.requirement_complete(action.tick, player));
    }

    #[test]
    fn player_inventory_cannot_substitute_policy_domain() {
        use crate::canonical_action::InventoryId;
        use crate::inventory::{InvLoc, InventoryCell};

        let (mut action, chunk, policy) = joint_action();
        let Stakeholder::Player(player) = policy.stakeholder else {
            unreachable!()
        };
        let target = Target::Player {
            key: player,
            lease: Some(crate::canonical_action::LivePlayerLease {
                key: player,
                gid: GlobalPlayerId {
                    server: ServerId(1),
                    player: PlayerSlot(1),
                },
                incarnation: 1,
            }),
            inventory: Some(InventoryId::Player { inv: 0, slot: 1 }),
        };
        action.conditions[1].target = target;
        action.effects[1] = Effect::Put {
            target,
            value: Value::Inventory(InventoryCell::empty(InvLoc { inv: 0, slot: 1 })),
        };
        action.player_leases = vec![crate::canonical_action::LivePlayerLease {
            key: player,
            gid: GlobalPlayerId {
                server: ServerId(1),
                player: PlayerSlot(1),
            },
            incarnation: 1,
        }];
        action.player_chunks = vec![(player, action.actor_chunk.unwrap())];
        action.validate().unwrap();
        let live = RequirementKey {
            stakeholder: Stakeholder::Player(player),
            domain: HolderDomain::LiveInventory {
                actor_chunk: action.actor_chunk.unwrap(),
                owner: ServerId(1),
                epoch: 1,
            },
        };
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture_player_lease(action.tick, action.player_leases[0], peer(1))
            .unwrap();
        ledger
            .capture_player_chunk(action.tick, player, action.actor_chunk.unwrap())
            .unwrap();
        assert_eq!(
            ledger.capture(
                action.tick,
                RequirementKey {
                    stakeholder: Stakeholder::Player(player),
                    domain: HolderDomain::LiveInventory {
                        actor_chunk: action.actor_chunk.unwrap(),
                        owner: ServerId(1),
                        epoch: action.player_leases[0].incarnation + 1,
                    },
                },
                BTreeSet::from([peer(1)]),
            ),
            Err(CompletionFailure::InvalidActionRequirements),
        );
        for key in [chunk, policy, live] {
            ledger
                .capture(action.tick, key, BTreeSet::from([peer(1)]))
                .unwrap();
        }
        assert_eq!(
            ledger.note_action(peer(1), &action, BTreeSet::from([chunk, policy])),
            Err(CompletionFailure::InvalidActionRequirements),
        );
        let mut stale = action.clone();
        stale.id.sequence = stale.id.sequence.wrapping_add(1);
        stale.player_leases[0].incarnation += 1;
        assert_eq!(
            ledger.note_action(peer(1), &stale, BTreeSet::from([chunk, live])),
            Err(CompletionFailure::InvalidActionRequirements),
        );
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, live]))
            .unwrap();
    }

    #[test]
    fn inventory_domain_must_name_frozen_player_chunk_and_generation() {
        let (action, chunk, player_key) = joint_action();
        let Stakeholder::Player(player) = player_key.stakeholder else {
            unreachable!()
        };
        let wrong_chunk = ChunkKey::new(WorldId::new(0, 0), 1, 0);
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_player_owner(action.tick, player, peer(1))
            .unwrap();
        ledger
            .capture_player_chunk(action.tick, player, action.actor_chunk.unwrap())
            .unwrap();
        for key in [
            chunk,
            RequirementKey {
                stakeholder: Stakeholder::Chunk(wrong_chunk),
                domain: HolderDomain::Chunk { epoch: 1 },
            },
        ] {
            ledger
                .capture(action.tick, key, BTreeSet::from([peer(1)]))
                .unwrap();
        }
        assert_eq!(
            ledger.capture(
                action.tick,
                RequirementKey {
                    stakeholder: Stakeholder::Player(player),
                    domain: HolderDomain::LiveInventory {
                        actor_chunk: wrong_chunk,
                        owner: ServerId(1),
                        epoch: 1,
                    },
                },
                BTreeSet::from([peer(1)]),
            ),
            Err(CompletionFailure::InvalidActionRequirements),
        );
        let wrong_generation = PeerGeneration {
            server: ServerId(1),
            generation: 2,
        };
        assert_eq!(
            ledger.capture(
                action.tick,
                RequirementKey {
                    stakeholder: Stakeholder::Player(player),
                    domain: HolderDomain::LiveInventory {
                        actor_chunk: action.actor_chunk.unwrap(),
                        owner: ServerId(1),
                        epoch: 1,
                    },
                },
                BTreeSet::from([wrong_generation]),
            ),
            Err(CompletionFailure::InvalidActionRequirements),
        );
    }

    #[test]
    fn same_action_identity_with_different_effect_is_rejected() {
        let (action, chunk, player) = joint_action();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        for key in [chunk, player] {
            ledger
                .capture(action.tick, key, BTreeSet::from([peer(1)]))
                .unwrap();
        }
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        let mut equivocation = action.clone();
        equivocation.effects[0] = Effect::Put {
            target: action.conditions[0].target,
            value: Value::Block(2),
        };
        equivocation.validate().unwrap();
        assert_eq!(
            ledger.note_action(peer(1), &equivocation, BTreeSet::from([chunk, player]),),
            Err(CompletionFailure::InvalidActionRequirements),
        );
    }

    #[test]
    fn joint_graph_ground_waits_for_player_domain_after_chunk_domain() {
        use crate::action_graph::TickActionGraph;
        use crate::action_solver::{SolvedVariant, install_solved_component, solve_component};

        let (action, chunk, player) = joint_action();
        let mut graph = TickActionGraph::default();
        graph.insert(action.clone()).unwrap();
        let anchors = action
            .targets()
            .into_iter()
            .map(|target| (target, None))
            .collect();
        let solved = solve_component(&graph, &BTreeSet::from([action.id]), &anchors, 29, |_| {
            Some(true)
        })
        .unwrap();
        install_solved_component(&mut graph, &solved).unwrap();
        let SolvedVariant::Applied { result, .. } = &solved.variants[&action.id] else {
            panic!("joint action did not apply");
        };
        graph
            .mark_local_applied(action.id, 0, result.clone())
            .unwrap();
        graph.mark_stable(action.id).unwrap();
        let mut ledger = RealmCompletionLedger::default();
        ledger
            .capture_actor_owner(action.tick, action.id.actor, peer(1))
            .unwrap();
        ledger
            .capture(action.tick, chunk, BTreeSet::from([peer(1), peer(2)]))
            .unwrap();
        ledger
            .capture(
                action.tick,
                player,
                BTreeSet::from([peer(1), peer(2), peer(3)]),
            )
            .unwrap();
        ledger
            .note_action(peer(1), &action, BTreeSet::from([chunk, player]))
            .unwrap();
        ledger
            .note_finish(
                peer(1),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::from([
                        (chunk, BTreeSet::from([action.id])),
                        (player, BTreeSet::from([action.id])),
                    ]),
                },
            )
            .unwrap();
        ledger
            .note_finish(
                peer(2),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert!(
            graph
                .ground_ready_plan(
                    action.tick,
                    |realm| ledger.realm_complete(action.tick, realm),
                    |_| false
                )
                .components
                .is_empty()
        );
        ledger
            .note_finish(
                peer(3),
                HolisticFinish {
                    full_source_slot: u64::from((action.tick).0),
                    acquisition_prefix: None,
                    tick: action.tick,
                    issued: BTreeMap::new(),
                },
            )
            .unwrap();
        assert_eq!(
            graph
                .ground_ready_plan(
                    action.tick,
                    |realm| ledger.realm_complete(action.tick, realm),
                    |_| false
                )
                .components,
            vec![vec![ActionRef {
                tick: action.tick,
                id: action.id
            }]],
        );
    }
}
