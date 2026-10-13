use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admin_sync::{AdminAudit, AdminMutation, AdminSnapshot};
use crate::identity::ServerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AdminControlStamp {
    pub millis: u64,
    pub origin: ServerId,
    pub ordinal: u64,
    pub id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedAdminMutation {
    pub stamp: AdminControlStamp,
    pub audit: AdminAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedAdminSnapshot {
    pub baseline: AdminSnapshot,
    pub mutations: Vec<ReplicatedAdminMutation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdminControlTarget {
    Operator([u8; 16]),
    Ban([u8; 16]),
}

impl AdminControlTarget {
    pub fn of(mutation: &AdminMutation) -> Self {
        match mutation {
            AdminMutation::GrantOp(value) => Self::Operator(value.uuid),
            AdminMutation::RevokeOp(value) => Self::Operator(value.uuid),
            AdminMutation::AddBan(value) => Self::Ban(value.uuid),
            AdminMutation::RemoveBan(value) => Self::Ban(value.uuid),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminReplicaOutcome {
    Applied,
    Superseded,
    Duplicate,
    InvalidIdentity,
}

#[derive(Clone)]
pub struct AdminReplica {
    baseline: AdminSnapshot,
    latest: BTreeMap<AdminControlTarget, ReplicatedAdminMutation>,
    max_millis: u64,
    next_ordinal: u64,
}

impl AdminReplica {
    pub fn new(baseline: AdminSnapshot) -> Self {
        Self {
            baseline,
            latest: BTreeMap::new(),
            max_millis: 0,
            next_ordinal: 0,
        }
    }

    pub fn issue(
        &mut self,
        source: ServerId,
        millis: u64,
        id: [u8; 16],
        audit: AdminAudit,
    ) -> ReplicatedAdminMutation {
        self.max_millis = self
            .max_millis
            .checked_add(1)
            .expect("admin clock exhausted")
            .max(millis);
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .expect("admin ordinal exhausted");
        ReplicatedAdminMutation {
            stamp: AdminControlStamp {
                millis: self.max_millis,
                origin: source,
                ordinal: self.next_ordinal,
                id,
            },
            audit,
        }
    }

    pub fn receive(
        &mut self,
        authenticated_source: ServerId,
        mutation: ReplicatedAdminMutation,
    ) -> AdminReplicaOutcome {
        if mutation.stamp.origin != authenticated_source {
            return AdminReplicaOutcome::InvalidIdentity;
        }
        self.merge(mutation)
    }

    pub fn merge(&mut self, mutation: ReplicatedAdminMutation) -> AdminReplicaOutcome {
        if mutation.stamp.millis > i64::MAX as u64 || mutation.stamp.ordinal == 0 {
            return AdminReplicaOutcome::InvalidIdentity;
        }
        self.max_millis = self.max_millis.max(mutation.stamp.millis);
        let target = AdminControlTarget::of(&mutation.audit.mutation);
        if let Some(current) = self.latest.get(&target) {
            if current.stamp == mutation.stamp {
                return if current == &mutation {
                    AdminReplicaOutcome::Duplicate
                } else {
                    AdminReplicaOutcome::InvalidIdentity
                };
            }
            if current.stamp > mutation.stamp {
                return AdminReplicaOutcome::Superseded;
            }
            if current.stamp.id == mutation.stamp.id {
                return AdminReplicaOutcome::InvalidIdentity;
            }
        }
        self.latest.insert(target, mutation);
        AdminReplicaOutcome::Applied
    }

    pub fn reconcile(
        &mut self,
        snapshot: ReplicatedAdminSnapshot,
    ) -> Result<Vec<ReplicatedAdminMutation>, ()> {
        let mut candidate = self.clone();
        let mut changed = Vec::new();
        for mutation in snapshot.mutations {
            match candidate.merge(mutation.clone()) {
                AdminReplicaOutcome::Applied => changed.push(mutation),
                AdminReplicaOutcome::Superseded | AdminReplicaOutcome::Duplicate => {}
                AdminReplicaOutcome::InvalidIdentity => return Err(()),
            }
        }
        for operator in snapshot.baseline.operators {
            if let Some(existing) = candidate
                .baseline
                .operators
                .iter_mut()
                .find(|entry| entry.uuid == operator.uuid)
            {
                if (
                    operator.level,
                    &operator.name,
                    operator.bypasses_player_limit,
                ) < (
                    existing.level,
                    &existing.name,
                    existing.bypasses_player_limit,
                ) {
                    *existing = operator;
                }
            } else {
                candidate.baseline.operators.push(operator);
            }
        }
        for ban in snapshot.baseline.banned_players {
            if let Some(existing) = candidate
                .baseline
                .banned_players
                .iter_mut()
                .find(|entry| entry.uuid == ban.uuid)
            {
                if (
                    ban.created_epoch_secs,
                    &ban.reason,
                    &ban.source,
                    ban.expires_epoch_secs,
                    &ban.name,
                ) < (
                    existing.created_epoch_secs,
                    &existing.reason,
                    &existing.source,
                    existing.expires_epoch_secs,
                    &existing.name,
                ) {
                    *existing = ban;
                }
            } else {
                candidate.baseline.banned_players.push(ban);
            }
        }
        *self = candidate;
        Ok(changed)
    }

    pub fn export(&self) -> ReplicatedAdminSnapshot {
        ReplicatedAdminSnapshot {
            baseline: self.baseline.clone(),
            mutations: self.latest.values().cloned().collect(),
        }
    }

    pub fn view(&self) -> AdminSnapshot {
        let mut view = self.baseline.clone();
        for mutation in self.latest.values() {
            match &mutation.audit.mutation {
                AdminMutation::GrantOp(value) => {
                    view.operators.retain(|entry| entry.uuid != value.uuid);
                    view.operators.push(value.clone());
                }
                AdminMutation::RevokeOp(value) => {
                    view.operators.retain(|entry| entry.uuid != value.uuid)
                }
                AdminMutation::AddBan(value) => {
                    view.banned_players.retain(|entry| entry.uuid != value.uuid);
                    view.banned_players.push(value.clone());
                }
                AdminMutation::RemoveBan(value) => {
                    view.banned_players.retain(|entry| entry.uuid != value.uuid)
                }
            }
        }
        view.operators.sort_by_key(|entry| entry.uuid);
        view.banned_players.sort_by_key(|entry| entry.uuid);
        view
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicatedAdminControl {
    Mutation(ReplicatedAdminMutation),
    SnapshotPart {
        transfer: [u8; 16],
        index: u16,
        parts: u16,
        bytes: Vec<u8>,
    },
}

pub const ADMIN_REPLICA_PREFIX: &[u8; 8] = b"AdmCtrl2";
pub const ADMIN_REPLICA_PART_SIZE: usize = 900_000;
pub const ADMIN_REPLICA_PART_LIMIT: usize = 64;

pub fn encode_control(control: &ReplicatedAdminControl) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(control)?;
    let mut bytes = Vec::with_capacity(8 + payload.len());
    bytes.extend_from_slice(ADMIN_REPLICA_PREFIX);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_control(bytes: &[u8]) -> Option<ReplicatedAdminControl> {
    let bytes = bytes.strip_prefix(ADMIN_REPLICA_PREFIX)?;
    if bytes.len() > ADMIN_REPLICA_PART_SIZE + 4096 {
        return None;
    }
    let control = postcard::from_bytes::<ReplicatedAdminControl>(bytes).ok()?;
    if let ReplicatedAdminControl::SnapshotPart {
        index,
        parts,
        bytes,
        ..
    } = &control
    {
        if *parts == 0
            || usize::from(*parts) > ADMIN_REPLICA_PART_LIMIT
            || index >= parts
            || bytes.len() > ADMIN_REPLICA_PART_SIZE
        {
            return None;
        }
    }
    Some(control)
}

pub fn snapshot_frames(
    snapshot: &ReplicatedAdminSnapshot,
    transfer: [u8; 16],
) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let bytes = postcard::to_allocvec(snapshot)?;
    let parts = bytes.len().div_ceil(ADMIN_REPLICA_PART_SIZE);
    if parts == 0 || parts > ADMIN_REPLICA_PART_LIMIT {
        return Err(postcard::Error::SerializeBufferFull);
    }
    bytes
        .chunks(ADMIN_REPLICA_PART_SIZE)
        .enumerate()
        .map(|(index, bytes)| {
            encode_control(&ReplicatedAdminControl::SnapshotPart {
                transfer,
                index: index as u16,
                parts: parts as u16,
                bytes: bytes.to_vec(),
            })
        })
        .collect()
}

pub struct AdminReplicaAssembly {
    transfer: [u8; 16],
    parts: Vec<Option<Vec<u8>>>,
    remaining: usize,
}

impl AdminReplicaAssembly {
    pub fn new(transfer: [u8; 16], parts: u16) -> Option<Self> {
        if parts == 0 || usize::from(parts) > ADMIN_REPLICA_PART_LIMIT {
            return None;
        }
        Some(Self {
            transfer,
            parts: vec![None; usize::from(parts)],
            remaining: usize::from(parts),
        })
    }

    pub fn insert(
        &mut self,
        transfer: [u8; 16],
        index: u16,
        parts: u16,
        bytes: Vec<u8>,
    ) -> Result<Option<ReplicatedAdminSnapshot>, ()> {
        if transfer != self.transfer
            || usize::from(parts) != self.parts.len()
            || bytes.len() > ADMIN_REPLICA_PART_SIZE
        {
            return Err(());
        }
        let Some(slot) = self.parts.get_mut(usize::from(index)) else {
            return Err(());
        };
        if let Some(previous) = slot {
            return if previous == &bytes {
                Ok(None)
            } else {
                Err(())
            };
        }
        *slot = Some(bytes);
        self.remaining -= 1;
        if self.remaining != 0 {
            return Ok(None);
        }
        let total = self
            .parts
            .iter()
            .filter_map(Option::as_ref)
            .map(Vec::len)
            .sum();
        let mut bytes = Vec::with_capacity(total);
        for part in &self.parts {
            bytes.extend_from_slice(part.as_ref().ok_or(())?);
        }
        postcard::from_bytes(&bytes).map(Some).map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_sync::{OpGrant, OpRevoke};

    fn baseline() -> AdminSnapshot {
        AdminSnapshot {
            revision: 0,
            operators: Vec::new(),
            banned_players: Vec::new(),
        }
    }

    #[test]
    fn opposite_arrival_orders_converge_and_rejoin_retains_revoke() {
        let mut issuer = AdminReplica::new(baseline());
        let grant = issuer.issue(
            ServerId(2),
            100,
            [1; 16],
            AdminAudit::new(
                "Admin".to_owned(),
                AdminMutation::GrantOp(OpGrant::new([9; 16], "Alice".to_owned(), 4, false)),
            ),
        );
        issuer.receive(ServerId(2), grant.clone());
        let revoke = issuer.issue(
            ServerId(2),
            101,
            [2; 16],
            AdminAudit::new(
                "Admin".to_owned(),
                AdminMutation::RevokeOp(OpRevoke::new([9; 16], "Alice".to_owned())),
            ),
        );
        let mut first = AdminReplica::new(baseline());
        let mut second = AdminReplica::new(baseline());
        first.receive(ServerId(2), grant.clone());
        first.receive(ServerId(2), revoke.clone());
        second.receive(ServerId(2), revoke);
        second.receive(ServerId(2), grant.clone());
        assert_eq!(first.view(), second.view());
        assert!(first.view().operators.is_empty());
        let mut restarted = AdminReplica::new(baseline());
        restarted.reconcile(first.export()).unwrap();
        assert_eq!(
            restarted.receive(ServerId(2), grant),
            AdminReplicaOutcome::Superseded
        );
        assert!(restarted.view().operators.is_empty());
    }

    #[test]
    fn source_forgery_and_conflicting_same_stamp_are_rejected() {
        let mut replica = AdminReplica::new(baseline());
        let grant = replica.issue(
            ServerId(2),
            100,
            [1; 16],
            AdminAudit::new(
                "Admin".to_owned(),
                AdminMutation::GrantOp(OpGrant::new([9; 16], "Alice".to_owned(), 4, false)),
            ),
        );
        assert_eq!(
            replica.receive(ServerId(3), grant.clone()),
            AdminReplicaOutcome::InvalidIdentity
        );
        assert!(replica.view().operators.is_empty());
        assert_eq!(
            replica.receive(ServerId(2), grant.clone()),
            AdminReplicaOutcome::Applied
        );
        let mut forged = grant;
        forged.audit.mutation = AdminMutation::RevokeOp(OpRevoke::new([9; 16], "Alice".to_owned()));
        assert_eq!(
            replica.receive(ServerId(2), forged),
            AdminReplicaOutcome::InvalidIdentity
        );
        assert_eq!(replica.view().operators.len(), 1);
    }

    #[test]
    fn snapshot_larger_than_one_control_frame_reassembles_out_of_order() {
        let mut snapshot = ReplicatedAdminSnapshot {
            baseline: baseline(),
            mutations: Vec::new(),
        };
        for index in 0_u32..512 {
            let mut uuid = [0; 16];
            uuid[..4].copy_from_slice(&index.to_le_bytes());
            snapshot
                .baseline
                .banned_players
                .push(crate::admin_sync::BanEntry::new(
                    uuid,
                    format!("Player{index}"),
                    "Admin".to_owned(),
                    "x".repeat(4096),
                    100,
                    None,
                ));
        }
        let transfer = [7; 16];
        let frames = snapshot_frames(&snapshot, transfer).unwrap();
        assert!(frames.len() > 1);
        let mut assembly = AdminReplicaAssembly::new(transfer, frames.len() as u16).unwrap();
        let mut result = None;
        for bytes in frames.into_iter().rev() {
            let ReplicatedAdminControl::SnapshotPart {
                transfer,
                index,
                parts,
                bytes,
            } = decode_control(&bytes).unwrap()
            else {
                panic!();
            };
            result = assembly
                .insert(transfer, index, parts, bytes)
                .unwrap()
                .or(result);
        }
        assert_eq!(result.unwrap(), snapshot);
    }
}
