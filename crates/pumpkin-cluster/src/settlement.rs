use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::chunks::Directory;
use crate::completion::ActionKey;
use crate::identity::ServerId;
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

pub const SETTLEMENT_RETAIN_MILLIS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SettlementId {
    pub tick: TickStamp,
    pub action: ActionKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyNotice {
    pub id: SettlementId,
    pub waiter: ServerId,
    pub chunks: Vec<ChunkKey>,
    pub missing: Vec<ChunkKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementPublication {
    pub id: SettlementId,
    pub owner: ServerId,
    pub chunks: Vec<ChunkKey>,
    pub recipients: Vec<ServerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ElectionRevision {
    pub term: u64,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryLoad {
    pub primary: ServerId,
    pub load: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryLoadSnapshot {
    pub revision: ElectionRevision,
    pub loads: Vec<PrimaryLoad>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkFetch {
    pub chunk: ChunkKey,
    pub source: ServerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyRouting {
    pub recipients: BTreeSet<ServerId>,
    pub complete_holder: Option<ServerId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryResolutionPlan {
    pub owner: ServerId,
    pub fetches: Vec<ChunkFetch>,
    pub election: PrimaryLoadSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementOutcome {
    pub publication: SettlementPublication,
    pub recipients: BTreeSet<ServerId>,
    pub released_primary_chunks: Vec<ChunkKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementError {
    TooFewChunks,
    SourceMismatch,
    WaiterHoldsNoAffectedChunk,
    IncorrectMissingSet,
    NoMissingDependency,
    ConflictingDependencySet,
    UnknownSettlement,
    NoCompleteHolder,
    NoEligiblePrimary,
    InvalidElectionSnapshot,
    StaleElectionSnapshot,
    ConflictingElectionSnapshot,
    MissingPrimarySource(ChunkKey),
    ConflictingOwner,
    WrongOwner,
    UnrelatedChunk,
    ChunkNotAcquired,
    NotGrounded,
    InvalidPublication,
}

#[derive(Debug, Default)]
struct PendingSettlement {
    chunks: BTreeSet<ChunkKey>,
    holders_by_chunk: BTreeMap<ChunkKey, BTreeSet<ServerId>>,
    required_holders: BTreeSet<ServerId>,
    missing_by_waiter: BTreeMap<ServerId, BTreeSet<ChunkKey>>,
    waiters: BTreeSet<ServerId>,
    owner: Option<ServerId>,
    eligible_owners: BTreeSet<ServerId>,
    election: Option<PrimaryLoadSnapshot>,
    last_election_revision: Option<ElectionRevision>,
    dropped: BTreeSet<ServerId>,
    acquired: BTreeSet<ChunkKey>,
    required_acquisitions: BTreeSet<ChunkKey>,
    grounded: BTreeSet<ChunkKey>,
}

#[derive(Debug, Default)]
pub struct SettlementCoordinator {
    pending: BTreeMap<SettlementId, PendingSettlement>,
    primary_locked: BTreeMap<SettlementId, BTreeSet<ChunkKey>>,
    retained_until: HashMap<ChunkKey, u64>,
}

impl DependencyNotice {
    pub fn from_directory(
        id: SettlementId,
        waiter: ServerId,
        chunks: impl IntoIterator<Item = ChunkKey>,
        directory: &Directory,
    ) -> Result<Self, SettlementError> {
        let chunks: BTreeSet<_> = chunks.into_iter().collect();
        if chunks.len() < 2 {
            return Err(SettlementError::TooFewChunks);
        }
        let missing = chunks
            .iter()
            .filter(|chunk| !directory.sorted_holders(chunk).contains(&waiter.0))
            .copied()
            .collect::<Vec<_>>();
        if missing.len() == chunks.len() {
            return Err(SettlementError::WaiterHoldsNoAffectedChunk);
        }
        if missing.is_empty() {
            return Err(SettlementError::NoMissingDependency);
        }
        Ok(Self {
            id,
            waiter,
            chunks: chunks.into_iter().collect(),
            missing,
        })
    }
}

impl PrimaryLoadSnapshot {
    #[must_use]
    pub fn new(revision: ElectionRevision, loads: impl IntoIterator<Item = PrimaryLoad>) -> Self {
        let mut loads: Vec<_> = loads.into_iter().collect();
        loads.sort_by_key(|entry| entry.primary);
        Self { revision, loads }
    }

    fn valid(&self) -> bool {
        !self.loads.is_empty()
            && self
                .loads
                .windows(2)
                .all(|pair| pair[0].primary < pair[1].primary)
    }
}

impl SettlementCoordinator {
    #[must_use]
    pub fn pending_ids(&self) -> Vec<SettlementId> {
        self.pending.keys().copied().collect()
    }

    #[must_use]
    pub fn owner(&self, id: SettlementId) -> Option<ServerId> {
        self.pending.get(&id).and_then(|pending| pending.owner)
    }

    #[must_use]
    pub fn missing_for(&self, id: SettlementId, waiter: ServerId) -> Vec<ChunkKey> {
        self.pending
            .get(&id)
            .and_then(|pending| pending.missing_by_waiter.get(&waiter))
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }

    #[must_use]
    pub fn required_holders(&self, id: SettlementId) -> Vec<ServerId> {
        self.pending
            .get(&id)
            .into_iter()
            .flat_map(|pending| &pending.required_holders)
            .copied()
            .collect()
    }

    #[must_use]
    pub fn frozen_holders(&self, id: SettlementId) -> Vec<(ChunkKey, Vec<ServerId>)> {
        self.pending
            .get(&id)
            .into_iter()
            .flat_map(|pending| &pending.holders_by_chunk)
            .map(|(chunk, holders)| (*chunk, holders.iter().copied().collect()))
            .collect()
    }

    #[must_use]
    pub fn pin_set(&self, id: SettlementId) -> Vec<ChunkKey> {
        self.primary_locked
            .get(&id)
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }

    pub fn note_dependency(
        &mut self,
        source: ServerId,
        notice: DependencyNotice,
        directory: &Directory,
    ) -> Result<DependencyRouting, SettlementError> {
        if source != notice.waiter {
            return Err(SettlementError::SourceMismatch);
        }
        let chunks: BTreeSet<_> = notice.chunks.iter().copied().collect();
        if chunks.len() < 2 || chunks.len() != notice.chunks.len() {
            return Err(SettlementError::TooFewChunks);
        }
        if let Some(pending) = self.pending.get(&notice.id)
            && pending.chunks != chunks
        {
            return Err(SettlementError::ConflictingDependencySet);
        }
        let (holders_by_chunk, required_holders) = self
            .pending
            .get(&notice.id)
            .map(|pending| {
                (
                    pending.holders_by_chunk.clone(),
                    pending.required_holders.clone(),
                )
            })
            .unwrap_or_else(|| {
                let holders_by_chunk: BTreeMap<_, _> = chunks
                    .iter()
                    .map(|chunk| {
                        (
                            *chunk,
                            directory
                                .sorted_holders(chunk)
                                .into_iter()
                                .map(ServerId)
                                .collect(),
                        )
                    })
                    .collect();
                let required_holders = holders_by_chunk.values().flatten().copied().collect();
                (holders_by_chunk, required_holders)
            });
        let held: BTreeSet<_> = holders_by_chunk
            .iter()
            .filter_map(|(chunk, holders)| holders.contains(&notice.waiter).then_some(*chunk))
            .collect();
        if held.is_empty() {
            return Err(SettlementError::WaiterHoldsNoAffectedChunk);
        }
        let expected_missing: BTreeSet<_> = chunks.difference(&held).copied().collect();
        let missing: BTreeSet<_> = notice.missing.iter().copied().collect();
        if expected_missing.is_empty() {
            return Err(SettlementError::NoMissingDependency);
        }
        if missing != expected_missing || missing.len() != notice.missing.len() {
            return Err(SettlementError::IncorrectMissingSet);
        }
        let pending = self
            .pending
            .entry(notice.id)
            .or_insert_with(|| PendingSettlement {
                chunks: chunks.clone(),
                holders_by_chunk,
                required_holders,
                ..PendingSettlement::default()
            });
        pending.waiters.insert(notice.waiter);
        pending
            .missing_by_waiter
            .insert(notice.waiter, expected_missing);
        let recipients = pending
            .required_holders
            .iter()
            .copied()
            .filter(|peer| *peer != notice.waiter)
            .collect();
        Ok(DependencyRouting {
            recipients,
            complete_holder: common_frozen_holder(pending),
        })
    }

    pub fn assign_complete_holder(
        &mut self,
        id: SettlementId,
    ) -> Result<ServerId, SettlementError> {
        let pending = self
            .pending
            .get_mut(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        let eligible = common_frozen_holders(pending);
        let owner = eligible
            .iter()
            .next()
            .copied()
            .ok_or(SettlementError::NoCompleteHolder)?;
        assign_owner(pending, owner)?;
        pending.eligible_owners = eligible;
        Ok(owner)
    }

    pub fn plan_primary_resolution(
        &mut self,
        id: SettlementId,
        election: PrimaryLoadSnapshot,
    ) -> Result<PrimaryResolutionPlan, SettlementError> {
        let pending = self
            .pending
            .get_mut(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        if common_frozen_holder(pending).is_some() {
            return Err(SettlementError::NoEligiblePrimary);
        }
        validate_election(pending, &election)?;
        let candidates: BTreeSet<_> = election
            .loads
            .iter()
            .map(|entry| entry.primary)
            .filter(|peer| {
                pending.required_holders.contains(peer) && !pending.dropped.contains(peer)
            })
            .collect();
        let owner = election
            .loads
            .iter()
            .filter(|entry| candidates.contains(&entry.primary))
            .min_by_key(|entry| (entry.load, entry.primary))
            .map(|entry| entry.primary)
            .ok_or(SettlementError::NoEligiblePrimary)?;
        let mut fetches = Vec::new();
        for chunk in &pending.chunks {
            let holders = &pending.holders_by_chunk[chunk];
            if holders.contains(&owner) {
                continue;
            }
            let source = holders
                .iter()
                .copied()
                .filter(|peer| *peer != owner && candidates.contains(peer))
                .min()
                .ok_or(SettlementError::MissingPrimarySource(*chunk))?;
            fetches.push(ChunkFetch {
                chunk: *chunk,
                source,
            });
        }
        assign_owner(pending, owner)?;
        pending.eligible_owners = candidates;
        pending.last_election_revision = Some(election.revision);
        pending.election = Some(election.clone());
        pending.required_acquisitions = fetches.iter().map(|fetch| fetch.chunk).collect();
        Ok(PrimaryResolutionPlan {
            owner,
            fetches,
            election,
        })
    }

    pub fn lock_primary_chunks(
        &mut self,
        id: SettlementId,
        primary: ServerId,
        chunks: impl IntoIterator<Item = ChunkKey>,
    ) -> Result<(), SettlementError> {
        let pending = self
            .pending
            .get(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        let chunks: BTreeSet<_> = chunks.into_iter().collect();
        if chunks.iter().any(|chunk| {
            !pending.chunks.contains(chunk) || !pending.holders_by_chunk[chunk].contains(&primary)
        }) {
            return Err(SettlementError::UnrelatedChunk);
        }
        self.primary_locked.entry(id).or_default().extend(chunks);
        Ok(())
    }

    pub fn note_acquired(
        &mut self,
        id: SettlementId,
        owner: ServerId,
        chunk: ChunkKey,
    ) -> Result<(), SettlementError> {
        let pending = self
            .pending
            .get_mut(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        if pending.owner != Some(owner) {
            return Err(SettlementError::WrongOwner);
        }
        if !pending.chunks.contains(&chunk) {
            return Err(SettlementError::UnrelatedChunk);
        }
        pending.acquired.insert(chunk);
        Ok(())
    }

    pub fn note_grounded(
        &mut self,
        id: SettlementId,
        owner: ServerId,
        chunk: ChunkKey,
    ) -> Result<(), SettlementError> {
        let pending = self
            .pending
            .get_mut(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        if pending.owner != Some(owner) {
            return Err(SettlementError::WrongOwner);
        }
        if !pending.chunks.contains(&chunk) {
            return Err(SettlementError::UnrelatedChunk);
        }
        if pending.required_acquisitions.contains(&chunk) && !pending.acquired.contains(&chunk) {
            return Err(SettlementError::ChunkNotAcquired);
        }
        pending.grounded.insert(chunk);
        Ok(())
    }

    pub fn settle(
        &mut self,
        id: SettlementId,
        owner: ServerId,
        now_millis: u64,
    ) -> Result<SettlementOutcome, SettlementError> {
        let pending = self
            .pending
            .get(&id)
            .ok_or(SettlementError::UnknownSettlement)?;
        if pending.owner != Some(owner) {
            return Err(SettlementError::WrongOwner);
        }
        if pending.grounded != pending.chunks {
            return Err(SettlementError::NotGrounded);
        }
        let Some(pending) = self.pending.remove(&id) else {
            return Err(SettlementError::UnknownSettlement);
        };
        let deadline = now_millis.saturating_add(SETTLEMENT_RETAIN_MILLIS);
        for chunk in &pending.acquired {
            self.retained_until
                .entry(*chunk)
                .and_modify(|current| *current = (*current).max(deadline))
                .or_insert(deadline);
        }
        let released_primary_chunks = self
            .primary_locked
            .remove(&id)
            .into_iter()
            .flatten()
            .collect();
        Ok(SettlementOutcome {
            publication: SettlementPublication {
                id,
                owner,
                chunks: pending.chunks.into_iter().collect(),
                recipients: pending.waiters.iter().copied().collect(),
            },
            recipients: pending.waiters,
            released_primary_chunks,
        })
    }

    pub fn observe_settlement(
        &mut self,
        source: ServerId,
        settlement: &SettlementPublication,
    ) -> Result<Vec<ChunkKey>, SettlementError> {
        let pending = self
            .pending
            .get(&settlement.id)
            .ok_or(SettlementError::UnknownSettlement)?;
        let chunks: BTreeSet<_> = settlement.chunks.iter().copied().collect();
        let recipients: BTreeSet<_> = settlement.recipients.iter().copied().collect();
        if settlement.owner != source
            || pending.owner != Some(source)
            || !pending.eligible_owners.contains(&source)
            || pending.chunks != chunks
            || chunks.len() != settlement.chunks.len()
            || pending.waiters != recipients
            || recipients.len() != settlement.recipients.len()
        {
            return Err(SettlementError::InvalidPublication);
        }
        self.pending.remove(&settlement.id);
        Ok(self
            .primary_locked
            .remove(&settlement.id)
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn peer_dropped(&mut self, peer: ServerId) -> Vec<SettlementId> {
        let mut replans = Vec::new();
        for (id, pending) in &mut self.pending {
            if !pending.required_holders.contains(&peer) {
                continue;
            }
            pending.dropped.insert(peer);
            let election_affected = pending
                .election
                .as_ref()
                .is_some_and(|election| election.loads.iter().any(|entry| entry.primary == peer));
            if pending.owner == Some(peer) || election_affected {
                pending.owner = None;
                pending.eligible_owners.clear();
                pending.election = None;
                pending.required_acquisitions.clear();
                pending.acquired.clear();
                replans.push(*id);
            }
        }
        replans
    }

    #[must_use]
    pub fn can_unload(&self, chunk: ChunkKey, primary: bool, now_millis: u64) -> bool {
        if !primary {
            return true;
        }
        if self
            .pending
            .values()
            .any(|pending| pending.chunks.contains(&chunk))
            || self
                .primary_locked
                .values()
                .any(|chunks| chunks.contains(&chunk))
        {
            return false;
        }
        self.retained_until
            .get(&chunk)
            .is_none_or(|deadline| now_millis >= *deadline)
    }

    pub fn expire_retention(&mut self, now_millis: u64) -> Vec<ChunkKey> {
        let mut expired: Vec<_> = self
            .retained_until
            .iter()
            .filter_map(|(chunk, deadline)| (now_millis >= *deadline).then_some(*chunk))
            .collect();
        expired.sort_unstable();
        for chunk in &expired {
            self.retained_until.remove(chunk);
        }
        expired
    }
}

fn common_frozen_holders(pending: &PendingSettlement) -> BTreeSet<ServerId> {
    let mut holders = pending.holders_by_chunk.values();
    let Some(first) = holders.next() else {
        return BTreeSet::new();
    };
    let mut common = first.clone();
    for holders in holders {
        common.retain(|peer| holders.contains(peer));
    }
    common.retain(|peer| !pending.dropped.contains(peer));
    common
}

fn common_frozen_holder(pending: &PendingSettlement) -> Option<ServerId> {
    common_frozen_holders(pending).into_iter().next()
}

fn validate_election(
    pending: &PendingSettlement,
    election: &PrimaryLoadSnapshot,
) -> Result<(), SettlementError> {
    if !election.valid() {
        return Err(SettlementError::InvalidElectionSnapshot);
    }
    if let Some(active) = &pending.election {
        if active == election {
            return Ok(());
        }
        if election.revision < active.revision {
            return Err(SettlementError::StaleElectionSnapshot);
        }
        return Err(SettlementError::ConflictingElectionSnapshot);
    }
    if pending
        .last_election_revision
        .is_some_and(|last| election.revision <= last)
    {
        return Err(SettlementError::StaleElectionSnapshot);
    }
    Ok(())
}

fn assign_owner(pending: &mut PendingSettlement, owner: ServerId) -> Result<(), SettlementError> {
    match pending.owner {
        Some(current) if current != owner => Err(SettlementError::ConflictingOwner),
        Some(_) => Ok(()),
        None => {
            pending.owner = Some(owner);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunks::{ChunkAdvert, ChunkDrop};
    use crate::identity::{GlobalPlayerId, PlayerSlot};
    use crate::protocol::WorldId;

    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(WorldId::new(0, 1), x, 0)
    }

    fn id() -> SettlementId {
        SettlementId {
            tick: TickStamp(9),
            action: ActionKey {
                incarnation: 0,
                actor: GlobalPlayerId::new(ServerId(2), PlayerSlot(4)).into(),
                sequence: 7,
                family: 3,
            },
        }
    }

    fn hold(directory: &mut Directory, holder: u16, chunks: impl IntoIterator<Item = ChunkKey>) {
        for chunk in chunks {
            directory.apply_advert(ChunkAdvert { holder, chunk });
        }
    }

    fn election(revision: u64, loads: &[(u16, u64)]) -> PrimaryLoadSnapshot {
        PrimaryLoadSnapshot::new(
            ElectionRevision { term: 4, revision },
            loads.iter().map(|(primary, load)| PrimaryLoad {
                primary: ServerId(*primary),
                load: *load,
            }),
        )
    }

    #[test]
    fn complete_holder_publishes_only_after_every_side_is_grounded() {
        let mut directory = Directory::new();
        hold(&mut directory, 2, [chunk(1)]);
        hold(&mut directory, 3, [chunk(2)]);
        hold(&mut directory, 4, [chunk(1), chunk(2)]);
        let mut coordinator = SettlementCoordinator::default();
        let first =
            DependencyNotice::from_directory(id(), ServerId(2), [chunk(1), chunk(2)], &directory)
                .unwrap();
        let second =
            DependencyNotice::from_directory(id(), ServerId(3), [chunk(1), chunk(2)], &directory)
                .unwrap();
        let routing = coordinator
            .note_dependency(ServerId(2), first, &directory)
            .unwrap();
        coordinator
            .note_dependency(ServerId(3), second, &directory)
            .unwrap();
        assert_eq!(routing.complete_holder, Some(ServerId(4)));
        assert_eq!(
            routing.recipients,
            BTreeSet::from([ServerId(3), ServerId(4)])
        );
        assert_eq!(coordinator.assign_complete_holder(id()), Ok(ServerId(4)));
        coordinator
            .note_grounded(id(), ServerId(4), chunk(1))
            .unwrap();
        assert_eq!(
            coordinator.settle(id(), ServerId(4), 100),
            Err(SettlementError::NotGrounded)
        );
        coordinator
            .note_grounded(id(), ServerId(4), chunk(2))
            .unwrap();
        let settled = coordinator.settle(id(), ServerId(4), 100).unwrap();
        assert_eq!(
            settled.recipients,
            BTreeSet::from([ServerId(2), ServerId(3)])
        );
        assert_eq!(settled.publication.chunks, vec![chunk(1), chunk(2)]);
    }

    #[test]
    fn split_primaries_elect_by_load_fetch_and_retain_acquired_side() {
        let mut directory = Directory::new();
        hold(&mut directory, 7, [chunk(1)]);
        hold(&mut directory, 8, [chunk(2)]);
        let mut coordinator = SettlementCoordinator::default();
        let notice =
            DependencyNotice::from_directory(id(), ServerId(7), [chunk(1), chunk(2)], &directory)
                .unwrap();
        coordinator
            .note_dependency(ServerId(7), notice, &directory)
            .unwrap();
        coordinator
            .lock_primary_chunks(id(), ServerId(7), [chunk(1)])
            .unwrap();
        assert_eq!(coordinator.pin_set(id()), vec![chunk(1)]);
        let plan = coordinator
            .plan_primary_resolution(id(), election(1, &[(7, 5), (8, 2)]))
            .unwrap();
        assert_eq!(plan.owner, ServerId(8));
        assert_eq!(
            plan.fetches,
            vec![ChunkFetch {
                chunk: chunk(1),
                source: ServerId(7)
            }]
        );
        assert_eq!(
            coordinator.note_grounded(id(), ServerId(8), chunk(1)),
            Err(SettlementError::ChunkNotAcquired)
        );
        coordinator
            .note_acquired(id(), ServerId(8), chunk(1))
            .unwrap();
        for target in [chunk(1), chunk(2)] {
            coordinator
                .note_grounded(id(), ServerId(8), target)
                .unwrap();
        }
        assert!(!coordinator.can_unload(chunk(1), true, 100));
        assert!(coordinator.can_unload(chunk(1), false, 100));
        let settled = coordinator.settle(id(), ServerId(8), 100).unwrap();
        assert_eq!(settled.released_primary_chunks, vec![chunk(1)]);
        assert!(!coordinator.can_unload(chunk(1), true, 5_099));
        assert!(coordinator.can_unload(chunk(1), true, 5_100));
        assert_eq!(coordinator.expire_retention(5_100), vec![chunk(1)]);
    }

    #[test]
    fn inconsistent_notice_and_forged_publication_leave_transaction_pending() {
        let mut directory = Directory::new();
        hold(&mut directory, 2, [chunk(1)]);
        hold(&mut directory, 3, [chunk(2), chunk(3)]);
        let mut coordinator = SettlementCoordinator::default();
        let notice =
            DependencyNotice::from_directory(id(), ServerId(2), [chunk(1), chunk(2)], &directory)
                .unwrap();
        assert_eq!(
            coordinator.note_dependency(ServerId(3), notice.clone(), &directory),
            Err(SettlementError::SourceMismatch)
        );
        coordinator
            .note_dependency(ServerId(2), notice, &directory)
            .unwrap();
        let inconsistent =
            DependencyNotice::from_directory(id(), ServerId(3), [chunk(1), chunk(3)], &directory)
                .unwrap();
        assert_eq!(
            coordinator.note_dependency(ServerId(3), inconsistent, &directory),
            Err(SettlementError::ConflictingDependencySet)
        );
        let plan = coordinator
            .plan_primary_resolution(id(), election(1, &[(2, 0), (3, 1)]))
            .unwrap();
        assert_eq!(plan.owner, ServerId(2));
        let forged = SettlementPublication {
            id: id(),
            owner: ServerId(2),
            chunks: vec![chunk(1), chunk(2)],
            recipients: vec![ServerId(2)],
        };
        assert_eq!(
            coordinator.observe_settlement(ServerId(3), &forged),
            Err(SettlementError::InvalidPublication)
        );
        let wrong_recipients = SettlementPublication {
            recipients: vec![ServerId(3)],
            ..forged
        };
        assert_eq!(
            coordinator.observe_settlement(ServerId(2), &wrong_recipients),
            Err(SettlementError::InvalidPublication)
        );
        assert_eq!(coordinator.pending_ids(), vec![id()]);
    }

    #[test]
    fn dependency_notice_rejects_waiter_that_holds_none_of_the_action() {
        let mut directory = Directory::new();
        hold(&mut directory, 3, [chunk(1), chunk(2)]);
        let mut coordinator = SettlementCoordinator::default();
        let notice = DependencyNotice {
            id: id(),
            waiter: ServerId(2),
            chunks: vec![chunk(1), chunk(2)],
            missing: vec![chunk(1), chunk(2)],
        };
        assert_eq!(
            coordinator.note_dependency(ServerId(2), notice, &directory),
            Err(SettlementError::WaiterHoldsNoAffectedChunk)
        );
        assert!(coordinator.pending_ids().is_empty());
    }

    #[test]
    fn holder_snapshot_stays_identical_after_directories_diverge() {
        let mut original = Directory::new();
        hold(&mut original, 2, [chunk(1)]);
        hold(&mut original, 3, [chunk(2)]);
        hold(&mut original, 4, [chunk(1), chunk(2)]);
        let first =
            DependencyNotice::from_directory(id(), ServerId(2), [chunk(1), chunk(2)], &original)
                .unwrap();
        let second =
            DependencyNotice::from_directory(id(), ServerId(3), [chunk(1), chunk(2)], &original)
                .unwrap();
        let mut left_directory = original.clone();
        let mut right_directory = original.clone();
        let mut left = SettlementCoordinator::default();
        let mut right = SettlementCoordinator::default();
        left.note_dependency(ServerId(2), first.clone(), &left_directory)
            .unwrap();
        right
            .note_dependency(ServerId(2), first, &right_directory)
            .unwrap();
        for target in [chunk(1), chunk(2)] {
            left_directory.apply_drop(ChunkDrop {
                holder: 4,
                chunk: target,
            });
            hold(&mut left_directory, 5, [target]);
        }
        right_directory.drop_holder(3);
        hold(&mut right_directory, 6, [chunk(2)]);
        left.note_dependency(ServerId(3), second.clone(), &left_directory)
            .unwrap();
        right
            .note_dependency(ServerId(3), second, &right_directory)
            .unwrap();
        assert_eq!(left.assign_complete_holder(id()), Ok(ServerId(4)));
        assert_eq!(right.assign_complete_holder(id()), Ok(ServerId(4)));
        assert_eq!(
            left.required_holders(id()),
            vec![ServerId(2), ServerId(3), ServerId(4)]
        );
        assert_eq!(left.frozen_holders(id()), right.frozen_holders(id()));
    }

    #[test]
    fn election_is_order_independent_and_rejects_stale_or_conflicting_views() {
        let mut directory = Directory::new();
        hold(&mut directory, 7, [chunk(1)]);
        hold(&mut directory, 8, [chunk(2)]);
        let notice =
            DependencyNotice::from_directory(id(), ServerId(7), [chunk(1), chunk(2)], &directory)
                .unwrap();
        let mut first = SettlementCoordinator::default();
        let mut second = SettlementCoordinator::default();
        first
            .note_dependency(ServerId(7), notice.clone(), &directory)
            .unwrap();
        second
            .note_dependency(ServerId(7), notice, &directory)
            .unwrap();
        let left = first
            .plan_primary_resolution(id(), election(2, &[(7, 9), (8, 3)]))
            .unwrap();
        let right = second
            .plan_primary_resolution(id(), election(2, &[(8, 3), (7, 9)]))
            .unwrap();
        assert_eq!(left, right);
        assert_eq!(left.owner, ServerId(8));
        assert_eq!(
            first.plan_primary_resolution(id(), election(1, &[(7, 0), (8, 1)])),
            Err(SettlementError::StaleElectionSnapshot)
        );
        assert_eq!(
            first.plan_primary_resolution(id(), election(2, &[(7, 0), (8, 1)])),
            Err(SettlementError::ConflictingElectionSnapshot)
        );
    }

    #[test]
    fn dropped_owner_replans_during_fetch_without_losing_transaction() {
        let mut directory = Directory::new();
        hold(&mut directory, 7, [chunk(1)]);
        hold(&mut directory, 8, [chunk(2)]);
        hold(&mut directory, 9, [chunk(2)]);
        let notice =
            DependencyNotice::from_directory(id(), ServerId(7), [chunk(1), chunk(2)], &directory)
                .unwrap();
        let mut coordinator = SettlementCoordinator::default();
        coordinator
            .note_dependency(ServerId(7), notice, &directory)
            .unwrap();
        let first = coordinator
            .plan_primary_resolution(id(), election(1, &[(7, 7), (8, 1), (9, 4)]))
            .unwrap();
        assert_eq!(first.owner, ServerId(8));
        assert_eq!(coordinator.peer_dropped(ServerId(8)), vec![id()]);
        let second = coordinator
            .plan_primary_resolution(id(), election(2, &[(7, 7), (9, 4)]))
            .unwrap();
        assert_eq!(second.owner, ServerId(9));
        assert_eq!(second.fetches[0].source, ServerId(7));
        coordinator
            .note_acquired(id(), ServerId(9), chunk(1))
            .unwrap();
        for target in [chunk(1), chunk(2)] {
            coordinator
                .note_grounded(id(), ServerId(9), target)
                .unwrap();
        }
        let settled = coordinator.settle(id(), ServerId(9), 10).unwrap();
        assert_eq!(settled.recipients, BTreeSet::from([ServerId(7)]));
    }

    #[test]
    fn grounded_side_survives_owner_drop_and_is_not_replayed() {
        let mut directory = Directory::new();
        hold(&mut directory, 7, [chunk(1)]);
        hold(&mut directory, 8, [chunk(2)]);
        hold(&mut directory, 9, [chunk(2)]);
        let notice =
            DependencyNotice::from_directory(id(), ServerId(7), [chunk(1), chunk(2)], &directory)
                .unwrap();
        let mut coordinator = SettlementCoordinator::default();
        coordinator
            .note_dependency(ServerId(7), notice, &directory)
            .unwrap();
        coordinator
            .plan_primary_resolution(id(), election(1, &[(7, 7), (8, 1), (9, 4)]))
            .unwrap();
        coordinator
            .note_grounded(id(), ServerId(8), chunk(2))
            .unwrap();
        coordinator.peer_dropped(ServerId(8));
        coordinator
            .plan_primary_resolution(id(), election(2, &[(7, 7), (9, 4)]))
            .unwrap();
        coordinator
            .note_acquired(id(), ServerId(9), chunk(1))
            .unwrap();
        coordinator
            .note_grounded(id(), ServerId(9), chunk(1))
            .unwrap();
        assert!(coordinator.settle(id(), ServerId(9), 10).is_ok());
    }
}
