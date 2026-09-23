use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::actor::ActorId;
use crate::admin_sync::{AdminMutation, AdminSnapshot, BanEntry, OpGrant};
use crate::identity::{GlobalPlayerId, ServerId};
use crate::order::order_actors;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminIssuer {
    Player { gid: GlobalPlayerId, uuid: [u8; 16] },
    Console { server: ServerId },
}

impl AdminIssuer {
    #[must_use]
    pub const fn actor(self) -> ActorId {
        match self {
            Self::Player { gid, .. } => ActorId::Player(gid),
            Self::Console { server } => ActorId::Peer(server),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminTargetBefore {
    Operator(Option<OpGrant>),
    Ban(Option<BanEntry>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalAdminAction {
    pub issuer: AdminIssuer,
    pub source: ServerId,
    pub tick: TickStamp,
    pub sequence: u16,
    pub mutation_id: [u8; 16],
    pub permission_revision: u64,
    pub issuer_op_before: Option<OpGrant>,
    pub target_before: AdminTargetBefore,
    pub undo: AdminTargetBefore,
    pub mutation: AdminMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminRejection {
    SourceMismatch,
    IssuerMismatch,
    StalePermission,
    Unauthorized,
    StaleTarget,
}

pub fn capture_global_admin_action(
    source: ServerId,
    tick: TickStamp,
    sequence: u16,
    mutation_id: [u8; 16],
    issuer: AdminIssuer,
    mutation: AdminMutation,
    ground: &AdminSnapshot,
    local: &AdminSnapshot,
    issuer_bindings: &HashMap<GlobalPlayerId, [u8; 16]>,
) -> Result<GlobalAdminAction, AdminRejection> {
    if source != issuer.actor().origin() {
        return Err(AdminRejection::SourceMismatch);
    }
    if let AdminIssuer::Player { gid, uuid } = issuer {
        if issuer_bindings.get(&gid) != Some(&uuid) {
            return Err(AdminRejection::IssuerMismatch);
        }
        if issuer_op_before(ground, issuer)
            .as_ref()
            .is_none_or(|op| op.level < 3)
        {
            return Err(AdminRejection::Unauthorized);
        }
    }
    Ok(GlobalAdminAction {
        issuer,
        source,
        tick,
        sequence,
        mutation_id,
        permission_revision: permission_revision(ground),
        issuer_op_before: issuer_op_before(ground, issuer),
        target_before: target_before(ground, &mutation),
        undo: target_before(local, &mutation),
        mutation,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminDecision {
    pub action: GlobalAdminAction,
    pub rejection: Option<AdminRejection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminResolutionError {
    ConflictingSequence,
}

#[must_use]
pub fn target_before(snapshot: &AdminSnapshot, mutation: &AdminMutation) -> AdminTargetBefore {
    match mutation {
        AdminMutation::GrantOp(grant) => AdminTargetBefore::Operator(
            snapshot
                .operators
                .iter()
                .find(|entry| entry.uuid == grant.uuid)
                .cloned(),
        ),
        AdminMutation::RevokeOp(revoke) => AdminTargetBefore::Operator(
            snapshot
                .operators
                .iter()
                .find(|entry| entry.uuid == revoke.uuid)
                .cloned(),
        ),
        AdminMutation::AddBan(ban) => AdminTargetBefore::Ban(
            snapshot
                .banned_players
                .iter()
                .find(|entry| entry.uuid == ban.uuid)
                .cloned(),
        ),
        AdminMutation::RemoveBan(revoke) => AdminTargetBefore::Ban(
            snapshot
                .banned_players
                .iter()
                .find(|entry| entry.uuid == revoke.uuid)
                .cloned(),
        ),
    }
}

#[must_use]
pub fn issuer_op_before(snapshot: &AdminSnapshot, issuer: AdminIssuer) -> Option<OpGrant> {
    match issuer {
        AdminIssuer::Player { uuid, .. } => snapshot
            .operators
            .iter()
            .find(|entry| entry.uuid == uuid)
            .cloned(),
        AdminIssuer::Console { .. } => None,
    }
}

#[must_use]
pub fn permission_revision(snapshot: &AdminSnapshot) -> u64 {
    let mut grants: Vec<_> = snapshot.operators.iter().collect();
    grants.sort_unstable_by_key(|grant| grant.uuid);
    let mut fingerprint = 0;
    for grant in grants {
        fingerprint = xxhash_rust::xxh64::xxh64(&grant.uuid, fingerprint);
        fingerprint = xxhash_rust::xxh64::xxh64(&[grant.level], fingerprint);
    }
    fingerprint
}

pub fn apply_mutation(snapshot: &mut AdminSnapshot, mutation: &AdminMutation) {
    match mutation {
        AdminMutation::GrantOp(grant) => {
            snapshot.operators.retain(|entry| entry.uuid != grant.uuid);
            snapshot.operators.push(grant.clone());
            snapshot.operators.sort_by_key(|entry| entry.uuid);
        }
        AdminMutation::RevokeOp(revoke) => {
            snapshot.operators.retain(|entry| entry.uuid != revoke.uuid)
        }
        AdminMutation::AddBan(ban) => {
            snapshot
                .banned_players
                .retain(|entry| entry.uuid != ban.uuid);
            snapshot.banned_players.push(ban.clone());
            snapshot.banned_players.sort_by_key(|entry| entry.uuid);
        }
        AdminMutation::RemoveBan(revoke) => snapshot
            .banned_players
            .retain(|entry| entry.uuid != revoke.uuid),
    }
}

#[must_use]
pub fn target_uuid(mutation: &AdminMutation) -> [u8; 16] {
    match mutation {
        AdminMutation::GrantOp(grant) => grant.uuid,
        AdminMutation::RevokeOp(revoke) => revoke.uuid,
        AdminMutation::AddBan(ban) => ban.uuid,
        AdminMutation::RemoveBan(revoke) => revoke.uuid,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AdminTargetKey {
    Operator([u8; 16]),
    Ban([u8; 16]),
}

fn target_key(mutation: &AdminMutation) -> AdminTargetKey {
    match mutation {
        AdminMutation::GrantOp(grant) => AdminTargetKey::Operator(grant.uuid),
        AdminMutation::RevokeOp(revoke) => AdminTargetKey::Operator(revoke.uuid),
        AdminMutation::AddBan(ban) => AdminTargetKey::Ban(ban.uuid),
        AdminMutation::RemoveBan(revoke) => AdminTargetKey::Ban(revoke.uuid),
    }
}

pub fn undo_local(snapshot: &mut AdminSnapshot, action: &GlobalAdminAction) -> bool {
    let uuid = target_uuid(&action.mutation);
    match (&action.mutation, &action.undo) {
        (
            AdminMutation::GrantOp(_) | AdminMutation::RevokeOp(_),
            AdminTargetBefore::Operator(before),
        ) if before.as_ref().is_none_or(|entry| entry.uuid == uuid) => {
            snapshot.operators.retain(|entry| entry.uuid != uuid);
            if let Some(before) = before {
                snapshot.operators.push(before.clone());
                snapshot.operators.sort_by_key(|entry| entry.uuid);
            }
            true
        }
        (
            AdminMutation::AddBan(_) | AdminMutation::RemoveBan(_),
            AdminTargetBefore::Ban(before),
        ) if before.as_ref().is_none_or(|entry| entry.uuid == uuid) => {
            snapshot.banned_players.retain(|entry| entry.uuid != uuid);
            if let Some(before) = before {
                snapshot.banned_players.push(before.clone());
                snapshot.banned_players.sort_by_key(|entry| entry.uuid);
            }
            true
        }
        _ => false,
    }
}

#[must_use]
pub fn resolve_global_admin(
    cluster_seed: u64,
    tick: TickStamp,
    ground: &AdminSnapshot,
    issuer_bindings: &HashMap<GlobalPlayerId, [u8; 16]>,
    actions: &[GlobalAdminAction],
) -> Result<(AdminSnapshot, Vec<AdminDecision>), AdminResolutionError> {
    let mut ordered = actions.to_vec();
    ordered.sort_by(|left, right| {
        order_actors(
            cluster_seed,
            tick,
            left.issuer.actor(),
            right.issuer.actor(),
        )
        .then_with(|| left.sequence.cmp(&right.sequence))
    });
    if ordered
        .windows(2)
        .any(|pair| pair[0].id() == pair[1].id() && pair[0] != pair[1])
    {
        return Err(AdminResolutionError::ConflictingSequence);
    }
    ordered.dedup();
    let mut result = ground.clone();
    let mut decisions = Vec::with_capacity(ordered.len());
    for action in ordered {
        let rejection = if action.source != action.issuer.actor().origin() || action.tick != tick {
            Some(AdminRejection::SourceMismatch)
        } else if let AdminIssuer::Player { gid, uuid } = action.issuer {
            if issuer_bindings.get(&gid) != Some(&uuid) {
                Some(AdminRejection::IssuerMismatch)
            } else {
                None
            }
        } else {
            None
        };
        let rejection = if rejection.is_some() {
            rejection
        } else if action.permission_revision != permission_revision(ground) {
            Some(AdminRejection::StalePermission)
        } else if matches!(action.issuer, AdminIssuer::Player { .. })
            && (issuer_op_before(&result, action.issuer) != action.issuer_op_before
                || action
                    .issuer_op_before
                    .as_ref()
                    .is_none_or(|op| op.level < 3))
        {
            Some(AdminRejection::Unauthorized)
        } else if target_before(&result, &action.mutation) != action.target_before {
            Some(AdminRejection::StaleTarget)
        } else {
            None
        };
        if rejection.is_none() {
            apply_mutation(&mut result, &action.mutation);
        }
        decisions.push(AdminDecision { action, rejection });
    }
    Ok((result, decisions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_sync::OpRevoke;
    use crate::identity::PlayerSlot;

    fn actor(server: u16, uuid: u8) -> (AdminIssuer, OpGrant) {
        let gid = GlobalPlayerId::new(ServerId(server), PlayerSlot(1));
        let grant = OpGrant::new([uuid; 16], format!("Op{uuid}"), 4, false);
        (
            AdminIssuer::Player {
                gid,
                uuid: grant.uuid,
            },
            grant,
        )
    }

    #[test]
    fn three_conflicting_mutations_converge_after_missing_actions_arrive() {
        let actors = [actor(1, 1), actor(2, 2), actor(3, 3)];
        let target = OpGrant::new([9; 16], String::from("Target"), 2, false);
        let ground = AdminSnapshot {
            revision: 7,
            operators: actors
                .iter()
                .map(|(_, grant)| grant.clone())
                .chain([target.clone()])
                .collect(),
            banned_players: vec![],
        };
        let actions: Vec<_> = actors
            .iter()
            .enumerate()
            .map(|(index, (issuer, grant))| GlobalAdminAction {
                issuer: *issuer,
                source: issuer.actor().origin(),
                tick: TickStamp(100),
                sequence: 1,
                mutation_id: [1; 16],
                permission_revision: permission_revision(&ground),
                issuer_op_before: Some(grant.clone()),
                target_before: AdminTargetBefore::Operator(Some(target.clone())),
                undo: AdminTargetBefore::Operator(Some(target.clone())),
                mutation: if index == 2 {
                    AdminMutation::RevokeOp(OpRevoke::new(target.uuid, target.name.clone()))
                } else {
                    AdminMutation::GrantOp(OpGrant::new(
                        target.uuid,
                        target.name.clone(),
                        (index + 3) as u8,
                        false,
                    ))
                },
            })
            .collect();
        let bindings: HashMap<_, _> = actors
            .iter()
            .filter_map(|(issuer, _)| match issuer {
                AdminIssuer::Player { gid, uuid } => Some((*gid, *uuid)),
                AdminIssuer::Console { .. } => None,
            })
            .collect();
        let (expected_ground, expected_decisions) =
            resolve_global_admin(42, TickStamp(100), &ground, &bindings, &actions).unwrap();
        assert_eq!(
            expected_decisions
                .iter()
                .filter(|decision| decision.rejection.is_none())
                .count(),
            1
        );
        for reordered in [
            vec![actions[2].clone(), actions[0].clone(), actions[1].clone()],
            vec![actions[1].clone(), actions[2].clone(), actions[0].clone()],
        ] {
            let (actual_ground, actual_decisions) =
                resolve_global_admin(42, TickStamp(100), &ground, &bindings, &reordered).unwrap();
            assert_eq!(actual_ground, expected_ground);
            assert_eq!(actual_decisions, expected_decisions);
        }
        let partial_a = resolve_global_admin(42, TickStamp(100), &ground, &bindings, &actions[..2])
            .unwrap()
            .0;
        let partial_b = resolve_global_admin(42, TickStamp(100), &ground, &bindings, &actions[1..])
            .unwrap()
            .0;
        assert!(partial_a != expected_ground || partial_b != expected_ground);
    }

    #[test]
    fn revoked_issuer_and_stale_revision_cannot_mutate_ground() {
        let (issuer, grant) = actor(1, 1);
        let target = OpGrant::new([9; 16], String::from("Target"), 2, false);
        let ground = AdminSnapshot {
            revision: 8,
            operators: vec![target.clone()],
            banned_players: vec![],
        };
        let action = GlobalAdminAction {
            issuer,
            source: ServerId(1),
            tick: TickStamp(100),
            sequence: 1,
            mutation_id: [2; 16],
            permission_revision: permission_revision(&ground).wrapping_add(1),
            issuer_op_before: Some(grant.clone()),
            target_before: AdminTargetBefore::Operator(Some(target.clone())),
            undo: AdminTargetBefore::Operator(Some(target.clone())),
            mutation: AdminMutation::RevokeOp(OpRevoke::new(target.uuid, target.name)),
        };
        let bindings = HashMap::from([(
            match issuer {
                AdminIssuer::Player { gid, .. } => gid,
                AdminIssuer::Console { .. } => unreachable!(),
            },
            grant.uuid,
        )]);
        let (result, decision) =
            resolve_global_admin(42, TickStamp(100), &ground, &bindings, &[action.clone()])
                .unwrap();
        assert_eq!(result, ground);
        assert_eq!(decision[0].rejection, Some(AdminRejection::StalePermission));
        let mut current = action;
        current.permission_revision = permission_revision(&ground);
        let (result, decision) =
            resolve_global_admin(42, TickStamp(100), &ground, &bindings, &[current]).unwrap();
        assert_eq!(result, ground);
        assert_eq!(decision[0].rejection, Some(AdminRejection::Unauthorized));
    }

    #[test]
    fn authenticated_peer_cannot_borrow_another_uuid_grant() {
        let (issuer, grant) = actor(1, 1);
        let ground = AdminSnapshot {
            revision: 1,
            operators: vec![grant.clone()],
            banned_players: vec![],
        };
        let gid = match issuer {
            AdminIssuer::Player { gid, .. } => gid,
            AdminIssuer::Console { .. } => unreachable!(),
        };
        let bindings = HashMap::from([(gid, [7; 16])]);
        let mutation = AdminMutation::RevokeOp(OpRevoke::new(grant.uuid, grant.name));
        assert_eq!(
            capture_global_admin_action(
                ServerId(1),
                TickStamp(4),
                1,
                [7; 16],
                issuer,
                mutation,
                &ground,
                &ground,
                &bindings,
            ),
            Err(AdminRejection::IssuerMismatch)
        );
    }

    #[test]
    fn same_issuer_tick_sequence_with_different_payload_is_invariant_failure() {
        let (issuer, grant) = actor(1, 1);
        let ground = AdminSnapshot {
            revision: 1,
            operators: vec![grant],
            banned_players: vec![],
        };
        let gid = match issuer {
            AdminIssuer::Player { gid, .. } => gid,
            AdminIssuer::Console { .. } => unreachable!(),
        };
        let bindings = HashMap::from([(gid, [1; 16])]);
        let first = capture_global_admin_action(
            ServerId(1),
            TickStamp(3),
            9,
            [8; 16],
            issuer,
            AdminMutation::RevokeOp(OpRevoke::new([8; 16], String::from("A"))),
            &ground,
            &ground,
            &bindings,
        )
        .unwrap();
        let second = capture_global_admin_action(
            ServerId(1),
            TickStamp(3),
            9,
            [9; 16],
            issuer,
            AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("B"))),
            &ground,
            &ground,
            &bindings,
        )
        .unwrap();
        assert_eq!(
            resolve_global_admin(5, TickStamp(3), &ground, &bindings, &[first, second]),
            Err(AdminResolutionError::ConflictingSequence)
        );
    }

    #[test]
    fn losing_local_grant_restores_exact_prior_grant_before_remote_winner() {
        let (issuer, grant) = actor(1, 1);
        let prior = OpGrant::new([9; 16], String::from("OldName"), 2, true);
        let ground = AdminSnapshot {
            revision: 1,
            operators: vec![grant.clone(), prior.clone()],
            banned_players: vec![],
        };
        let local_grant = OpGrant::new(prior.uuid, String::from("NewName"), 4, false);
        let local_action = GlobalAdminAction {
            issuer,
            source: ServerId(1),
            tick: TickStamp(8),
            sequence: 1,
            mutation_id: [3; 16],
            permission_revision: permission_revision(&ground),
            issuer_op_before: Some(grant),
            target_before: AdminTargetBefore::Operator(Some(prior.clone())),
            undo: AdminTargetBefore::Operator(Some(prior)),
            mutation: AdminMutation::GrantOp(local_grant),
        };
        let mut local = ground.clone();
        apply_mutation(&mut local, &local_action.mutation);
        assert_ne!(local, ground);
        assert!(undo_local(&mut local, &local_action));
        assert_eq!(local, ground);
        let remote = AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("OldName")));
        apply_mutation(&mut local, &remote);
        let mut accepted_ground = ground;
        apply_mutation(&mut accepted_ground, &remote);
        assert_eq!(local, accepted_ground);
    }

    #[test]
    fn capture_separates_ground_precondition_from_optimistic_undo() {
        let (issuer, grant) = actor(1, 1);
        let prior = OpGrant::new([9; 16], String::from("Target"), 2, false);
        let ground = AdminSnapshot {
            revision: 1,
            operators: vec![grant, prior.clone()],
            banned_players: vec![],
        };
        let mut local = ground.clone();
        let intermediate = OpGrant::new(prior.uuid, prior.name.clone(), 3, false);
        apply_mutation(&mut local, &AdminMutation::GrantOp(intermediate.clone()));
        let mutation =
            AdminMutation::GrantOp(OpGrant::new(prior.uuid, prior.name.clone(), 4, false));
        let gid = match issuer {
            AdminIssuer::Player { gid, .. } => gid,
            AdminIssuer::Console { .. } => unreachable!(),
        };
        let bindings = HashMap::from([(gid, [1; 16])]);
        let action = capture_global_admin_action(
            ServerId(1),
            TickStamp(9),
            2,
            [10; 16],
            issuer,
            mutation,
            &ground,
            &local,
            &bindings,
        )
        .unwrap();
        assert_eq!(
            action.target_before,
            AdminTargetBefore::Operator(Some(prior))
        );
        assert_eq!(action.undo, AdminTargetBefore::Operator(Some(intermediate)));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GlobalAdminActionId {
    pub actor: ActorId,
    pub sequence: u16,
}

impl GlobalAdminAction {
    #[must_use]
    pub const fn id(&self) -> GlobalAdminActionId {
        GlobalAdminActionId {
            actor: self.issuer.actor(),
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalAdminTickFinish {
    pub tick: TickStamp,
    pub actions: Vec<GlobalAdminActionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminCompletionError {
    UnexpectedPeer,
    WrongTick,
    WrongSource,
    ConflictingFinish,
    ConflictingAction,
    TooManyActions,
    DuplicateAction,
    UndeclaredAction,
    UnexpectedFrame,
}

#[derive(Debug, Clone)]
pub struct GlobalAdminCompletion {
    tick: TickStamp,
    participants: std::collections::BTreeSet<ServerId>,
    finishes: std::collections::BTreeMap<ServerId, std::collections::BTreeSet<GlobalAdminActionId>>,
    actions: std::collections::BTreeMap<GlobalAdminActionId, GlobalAdminAction>,
}

impl GlobalAdminCompletion {
    #[must_use]
    pub fn new(tick: TickStamp, participants: impl IntoIterator<Item = ServerId>) -> Self {
        Self {
            tick,
            participants: participants.into_iter().collect(),
            finishes: std::collections::BTreeMap::new(),
            actions: std::collections::BTreeMap::new(),
        }
    }

    pub fn receive_finish(
        &mut self,
        peer: ServerId,
        finish: GlobalAdminTickFinish,
    ) -> Result<(), GlobalAdminCompletionError> {
        if !self.participants.contains(&peer) {
            return Err(GlobalAdminCompletionError::UnexpectedPeer);
        }
        if finish.tick != self.tick {
            return Err(GlobalAdminCompletionError::WrongTick);
        }
        if finish.actions.len() > GLOBAL_ADMIN_BATCH_LIMIT {
            return Err(GlobalAdminCompletionError::TooManyActions);
        }
        if finish.actions.iter().any(|id| id.actor.origin() != peer) {
            return Err(GlobalAdminCompletionError::WrongSource);
        }
        let action_count = finish.actions.len();
        let declared: std::collections::BTreeSet<_> = finish.actions.into_iter().collect();
        if declared.len() != action_count {
            return Err(GlobalAdminCompletionError::DuplicateAction);
        }
        if self
            .actions
            .keys()
            .any(|id| id.actor.origin() == peer && !declared.contains(id))
        {
            return Err(GlobalAdminCompletionError::UndeclaredAction);
        }
        match self.finishes.get(&peer) {
            Some(previous) if previous != &declared => {
                Err(GlobalAdminCompletionError::ConflictingFinish)
            }
            Some(_) => Ok(()),
            None => {
                self.finishes.insert(peer, declared);
                Ok(())
            }
        }
    }

    pub fn receive_action(
        &mut self,
        peer: ServerId,
        action: GlobalAdminAction,
    ) -> Result<(), GlobalAdminCompletionError> {
        if !self.participants.contains(&peer) {
            return Err(GlobalAdminCompletionError::UnexpectedPeer);
        }
        if action.tick != self.tick {
            return Err(GlobalAdminCompletionError::WrongTick);
        }
        if action.source != peer || action.id().actor.origin() != peer {
            return Err(GlobalAdminCompletionError::WrongSource);
        }
        if self
            .finishes
            .get(&peer)
            .is_some_and(|declared| !declared.contains(&action.id()))
        {
            return Err(GlobalAdminCompletionError::UndeclaredAction);
        }
        if self.actions.len() >= GLOBAL_ADMIN_BATCH_LIMIT.saturating_mul(self.participants.len())
            && !self.actions.contains_key(&action.id())
        {
            return Err(GlobalAdminCompletionError::TooManyActions);
        }
        match self.actions.get(&action.id()) {
            Some(previous) if previous != &action => {
                Err(GlobalAdminCompletionError::ConflictingAction)
            }
            Some(_) => Ok(()),
            None => {
                self.actions.insert(action.id(), action);
                Ok(())
            }
        }
    }

    pub fn declare_dropped_after_recovery(
        &mut self,
        dropped: ServerId,
        recovered: &[GlobalAdminAction],
    ) -> Result<Vec<GlobalAdminActionId>, GlobalAdminCompletionError> {
        if !self.participants.contains(&dropped) {
            return Err(GlobalAdminCompletionError::UnexpectedPeer);
        }
        let mut candidate = self.clone();
        let declared_before_drop = candidate.finishes.remove(&dropped);
        for action in recovered {
            candidate.receive_action(dropped, action.clone())?;
        }
        let received: std::collections::BTreeSet<_> = candidate
            .actions
            .keys()
            .filter(|id| id.actor.origin() == dropped)
            .copied()
            .collect();
        let missing = declared_before_drop
            .as_ref()
            .into_iter()
            .flat_map(|declared| declared.difference(&received))
            .copied()
            .collect();
        candidate.finishes.insert(dropped, received);
        *self = candidate;
        Ok(missing)
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.participants
            .iter()
            .all(|peer| self.finishes.contains_key(peer))
            && self.actions.keys().all(|id| {
                self.finishes
                    .get(&id.actor.origin())
                    .is_some_and(|declared| declared.contains(id))
            })
            && self
                .finishes
                .values()
                .all(|declared| declared.iter().all(|id| self.actions.contains_key(id)))
    }

    #[must_use]
    pub fn actions(&self) -> Option<Vec<GlobalAdminAction>> {
        self.is_complete()
            .then(|| self.actions.values().cloned().collect())
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use crate::admin_sync::OpRevoke;
    use crate::identity::PlayerSlot;

    #[test]
    fn finish_before_action_and_missing_peer_cannot_settle_early() {
        let tick = TickStamp(92);
        let peer_a = ServerId(1);
        let peer_b = ServerId(2);
        let issuer = AdminIssuer::Player {
            gid: GlobalPlayerId::new(peer_a, PlayerSlot(5)),
            uuid: [5; 16],
        };
        let action = GlobalAdminAction {
            issuer,
            source: peer_a,
            tick,
            sequence: 7,
            mutation_id: [4; 16],
            permission_revision: 4,
            issuer_op_before: None,
            target_before: AdminTargetBefore::Operator(None),
            undo: AdminTargetBefore::Operator(None),
            mutation: AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("Target"))),
        };
        let mut completion = GlobalAdminCompletion::new(tick, [peer_a, peer_b]);
        completion
            .receive_finish(
                peer_a,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![action.id()],
                },
            )
            .unwrap();
        assert!(!completion.is_complete());
        completion
            .receive_finish(
                peer_b,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![],
                },
            )
            .unwrap();
        assert!(!completion.is_complete());
        completion.receive_action(peer_a, action.clone()).unwrap();
        assert_eq!(completion.actions(), Some(vec![action]));
    }

    #[test]
    fn forged_source_or_conflicting_manifest_cannot_complete() {
        let tick = TickStamp(1);
        let a = ServerId(1);
        let b = ServerId(2);
        let id = GlobalAdminActionId {
            actor: ActorId::Peer(a),
            sequence: 1,
        };
        let mut completion = GlobalAdminCompletion::new(tick, [a, b]);
        assert_eq!(
            completion.receive_finish(
                b,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![id]
                }
            ),
            Err(GlobalAdminCompletionError::WrongSource)
        );
        completion
            .receive_finish(
                a,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![],
                },
            )
            .unwrap();
        assert_eq!(
            completion.receive_finish(
                a,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![id]
                }
            ),
            Err(GlobalAdminCompletionError::ConflictingFinish)
        );
        assert!(!completion.is_complete());
    }

    #[test]
    fn manifest_omitting_an_already_received_action_is_rejected() {
        let tick = TickStamp(2);
        let peer = ServerId(1);
        let action = GlobalAdminAction {
            issuer: AdminIssuer::Console { server: peer },
            source: peer,
            tick,
            sequence: 2,
            mutation_id: [6; 16],
            permission_revision: 0,
            issuer_op_before: None,
            target_before: AdminTargetBefore::Operator(None),
            undo: AdminTargetBefore::Operator(None),
            mutation: AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("Target"))),
        };
        let mut completion = GlobalAdminCompletion::new(tick, [peer]);
        completion.receive_action(peer, action.clone()).unwrap();
        assert_eq!(
            completion.receive_finish(
                peer,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![]
                }
            ),
            Err(GlobalAdminCompletionError::UndeclaredAction)
        );
        completion
            .receive_finish(
                peer,
                GlobalAdminTickFinish {
                    tick,
                    actions: vec![action.id()],
                },
            )
            .unwrap();
        assert!(completion.is_complete());
    }

    #[test]
    fn dropped_peer_recovery_converges_on_actions_seen_by_survivors() {
        let tick = TickStamp(4);
        let a = ServerId(1);
        let b = ServerId(2);
        let c = ServerId(3);
        let action = GlobalAdminAction {
            issuer: AdminIssuer::Console { server: c },
            source: c,
            tick,
            sequence: 1,
            mutation_id: [30; 16],
            permission_revision: 0,
            issuer_op_before: None,
            target_before: AdminTargetBefore::Operator(None),
            undo: AdminTargetBefore::Operator(None),
            mutation: AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("Target"))),
        };
        let lost = GlobalAdminActionId {
            actor: ActorId::Peer(c),
            sequence: 2,
        };
        let mut at_a = GlobalAdminCompletion::new(tick, [a, b, c]);
        let mut at_b = GlobalAdminCompletion::new(tick, [a, b, c]);
        for completion in [&mut at_a, &mut at_b] {
            completion
                .receive_finish(
                    a,
                    GlobalAdminTickFinish {
                        tick,
                        actions: vec![],
                    },
                )
                .unwrap();
            completion
                .receive_finish(
                    b,
                    GlobalAdminTickFinish {
                        tick,
                        actions: vec![],
                    },
                )
                .unwrap();
        }
        at_a.receive_action(c, action.clone()).unwrap();
        at_a.receive_finish(
            c,
            GlobalAdminTickFinish {
                tick,
                actions: vec![action.id(), lost],
            },
        )
        .unwrap();
        assert!(!at_a.is_complete());
        assert!(!at_b.is_complete());
        assert_eq!(at_a.declare_dropped_after_recovery(c, &[]), Ok(vec![lost]));
        assert_eq!(
            at_b.declare_dropped_after_recovery(c, &[action.clone()]),
            Ok(vec![])
        );
        assert_eq!(at_a.actions(), Some(vec![action.clone()]));
        assert_eq!(at_b.actions(), Some(vec![action]));
    }
}

pub const GLOBAL_ADMIN_FRAME_LIMIT: usize = 1_048_576;
pub const GLOBAL_ADMIN_BATCH_LIMIT: usize = 256;
pub const GLOBAL_ADMIN_MUTATION_LIMIT: usize = 60_000;
const GLOBAL_ADMIN_WIRE_PREFIX: &[u8; 8] = b"GAdmV001";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalAdminActionBatch {
    pub tick: TickStamp,
    pub actions: Vec<GlobalAdminAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalAdminFrame {
    ActionBatch(GlobalAdminActionBatch),
    TickFinish(GlobalAdminTickFinish),
    DropReport(crate::global_admin_drop::GlobalAdminDropReport),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminWireError {
    TooLarge,
    TooManyActions,
    Invalid,
}

pub fn encode_global_admin_frame(
    frame: &GlobalAdminFrame,
) -> Result<Vec<u8>, GlobalAdminWireError> {
    if matches!(frame, GlobalAdminFrame::ActionBatch(batch) if batch.actions.len() > GLOBAL_ADMIN_BATCH_LIMIT)
        || matches!(frame, GlobalAdminFrame::TickFinish(finish) if finish.actions.len() > GLOBAL_ADMIN_BATCH_LIMIT)
        || matches!(frame, GlobalAdminFrame::DropReport(report) if report.observed.len() > GLOBAL_ADMIN_BATCH_LIMIT || report.expected.len() > GLOBAL_ADMIN_BATCH_LIMIT)
    {
        return Err(GlobalAdminWireError::TooManyActions);
    }
    let payload = postcard::to_allocvec(frame).map_err(|_| GlobalAdminWireError::Invalid)?;
    if payload.len() > GLOBAL_ADMIN_FRAME_LIMIT - GLOBAL_ADMIN_WIRE_PREFIX.len() {
        return Err(GlobalAdminWireError::TooLarge);
    }
    let mut bytes = Vec::with_capacity(GLOBAL_ADMIN_WIRE_PREFIX.len() + payload.len());
    bytes.extend_from_slice(GLOBAL_ADMIN_WIRE_PREFIX);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_global_admin_frame(
    bytes: &[u8],
) -> Result<Option<GlobalAdminFrame>, GlobalAdminWireError> {
    let Some(payload) = bytes.strip_prefix(GLOBAL_ADMIN_WIRE_PREFIX) else {
        return Ok(None);
    };
    if bytes.len() > GLOBAL_ADMIN_FRAME_LIMIT {
        return Err(GlobalAdminWireError::TooLarge);
    }
    let frame: GlobalAdminFrame =
        postcard::from_bytes(payload).map_err(|_| GlobalAdminWireError::Invalid)?;
    match &frame {
        GlobalAdminFrame::ActionBatch(batch) if batch.actions.len() > GLOBAL_ADMIN_BATCH_LIMIT => {
            Err(GlobalAdminWireError::TooManyActions)
        }
        GlobalAdminFrame::ActionBatch(batch)
            if batch.actions.iter().any(|action| action.tick != batch.tick) =>
        {
            Err(GlobalAdminWireError::Invalid)
        }
        GlobalAdminFrame::TickFinish(finish) if finish.actions.len() > GLOBAL_ADMIN_BATCH_LIMIT => {
            Err(GlobalAdminWireError::TooManyActions)
        }
        GlobalAdminFrame::DropReport(report)
            if report.observed.len() > GLOBAL_ADMIN_BATCH_LIMIT
                || report.expected.len() > GLOBAL_ADMIN_BATCH_LIMIT =>
        {
            Err(GlobalAdminWireError::TooManyActions)
        }
        _ => Ok(Some(frame)),
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn grouped_batch_round_trip_and_size_guard() {
        let frame = GlobalAdminFrame::ActionBatch(GlobalAdminActionBatch {
            tick: TickStamp(7),
            actions: vec![],
        });
        let bytes = encode_global_admin_frame(&frame).unwrap();
        assert_eq!(decode_global_admin_frame(&bytes), Ok(Some(frame)));
        assert_eq!(decode_global_admin_frame(b"other-control-frame"), Ok(None));
        let mut oversized = bytes;
        oversized.resize(GLOBAL_ADMIN_FRAME_LIMIT + 1, 0);
        assert_eq!(
            decode_global_admin_frame(&oversized),
            Err(GlobalAdminWireError::TooLarge)
        );
    }
}

#[cfg(test)]
mod replica_tests {
    use super::*;
    use crate::identity::PlayerSlot;

    #[test]
    fn losing_optimistic_op_change_converges_on_remote_winner() {
        let local_id = GlobalPlayerId::new(ServerId(1), PlayerSlot(1));
        let remote_id = GlobalPlayerId::new(ServerId(2), PlayerSlot(1));
        let local_issuer = AdminIssuer::Player {
            gid: local_id,
            uuid: [1; 16],
        };
        let remote_issuer = AdminIssuer::Player {
            gid: remote_id,
            uuid: [2; 16],
        };
        let original = OpGrant::new([9; 16], String::from("Target"), 2, false);
        let ground = AdminSnapshot {
            revision: 1,
            operators: vec![
                OpGrant::new([1; 16], String::from("A"), 4, false),
                OpGrant::new([2; 16], String::from("B"), 4, false),
                original.clone(),
            ],
            banned_players: vec![],
        };
        let bindings = HashMap::from([(local_id, [1; 16]), (remote_id, [2; 16])]);
        let tick = TickStamp(7);
        let local = capture_global_admin_action(
            ServerId(1),
            tick,
            1,
            [11; 16],
            local_issuer,
            AdminMutation::GrantOp(OpGrant::new([9; 16], String::from("Target"), 3, false)),
            &ground,
            &ground,
            &bindings,
        )
        .unwrap();
        let remote = capture_global_admin_action(
            ServerId(2),
            tick,
            1,
            [12; 16],
            remote_issuer,
            AdminMutation::GrantOp(OpGrant::new([9; 16], String::from("Target"), 4, false)),
            &ground,
            &ground,
            &bindings,
        )
        .unwrap();
        let seed = (0..100)
            .find(|seed| {
                order_actors(*seed, tick, remote_issuer.actor(), local_issuer.actor()).is_lt()
            })
            .unwrap();
        let (_, decisions) =
            resolve_global_admin(seed, tick, &ground, &bindings, &[local.clone(), remote]).unwrap();
        let mut replica = GlobalAdminReplica::new(ground);
        replica.apply_local(local).unwrap();
        assert_eq!(
            target_before(&replica.local, &decisions[0].action.mutation),
            AdminTargetBefore::Operator(Some(OpGrant::new(
                [9; 16],
                String::from("Target"),
                3,
                false
            )))
        );
        replica.settle(tick, &decisions).unwrap();
        assert_eq!(replica.local, replica.ground);
        assert_eq!(
            target_before(&replica.ground, &decisions[0].action.mutation),
            AdminTargetBefore::Operator(Some(OpGrant::new(
                [9; 16],
                String::from("Target"),
                4,
                false
            )))
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminLocalError {
    TargetPending,
    InvalidUndo,
    TickPending,
    Capacity,
    Incomplete,
}

#[derive(Debug, Clone)]
pub struct GlobalAdminReplica {
    pub ground: AdminSnapshot,
    pub local: AdminSnapshot,
    pending: std::collections::BTreeMap<GlobalAdminActionId, GlobalAdminAction>,
    displaced: std::collections::BTreeSet<GlobalAdminActionId>,
}

impl GlobalAdminReplica {
    #[must_use]
    pub fn new(ground: AdminSnapshot) -> Self {
        Self {
            local: ground.clone(),
            ground,
            pending: std::collections::BTreeMap::new(),
            displaced: std::collections::BTreeSet::new(),
        }
    }

    pub fn apply_local(&mut self, action: GlobalAdminAction) -> Result<(), AdminLocalError> {
        if self.pending.len() >= GLOBAL_ADMIN_BATCH_LIMIT {
            return Err(AdminLocalError::Capacity);
        }
        let target = target_key(&action.mutation);
        if self
            .pending
            .values()
            .any(|pending| target_key(&pending.mutation) == target)
        {
            return Err(AdminLocalError::TargetPending);
        }
        if target_before(&self.local, &action.mutation) != action.undo {
            return Err(AdminLocalError::InvalidUndo);
        }
        apply_mutation(&mut self.local, &action.mutation);
        self.pending.insert(action.id(), action);
        Ok(())
    }

    pub fn settle(
        &mut self,
        tick: TickStamp,
        decisions: &[AdminDecision],
    ) -> Result<(), AdminLocalError> {
        if decisions
            .iter()
            .any(|decision| decision.action.tick != tick)
        {
            return Err(AdminLocalError::TickPending);
        }
        if self
            .pending
            .iter()
            .filter(|(_, action)| action.tick == tick)
            .any(|(id, action)| {
                !decisions
                    .iter()
                    .any(|decision| decision.action.id() == *id && decision.action == *action)
            })
        {
            return Err(AdminLocalError::Incomplete);
        }
        for decision in decisions {
            if decision.rejection.is_some()
                && let Some(action) = self.pending.get(&decision.action.id())
                && !self.displaced.contains(&decision.action.id())
                && !undo_local(&mut self.local, action)
            {
                return Err(AdminLocalError::InvalidUndo);
            }
        }
        let local_winners: std::collections::BTreeSet<_> = decisions
            .iter()
            .filter(|decision| {
                decision.rejection.is_none() && self.pending.contains_key(&decision.action.id())
            })
            .map(|decision| target_key(&decision.action.mutation))
            .collect();
        for decision in decisions {
            if decision.rejection.is_none() {
                let target = target_key(&decision.action.mutation);
                let later = self
                    .pending
                    .iter()
                    .find(|(_, pending)| {
                        pending.tick != tick && target_key(&pending.mutation) == target
                    })
                    .map(|(id, action)| (*id, action.clone()));
                if let Some((id, action)) = later
                    && !self.displaced.contains(&id)
                {
                    if !undo_local(&mut self.local, &action) {
                        return Err(AdminLocalError::InvalidUndo);
                    }
                    self.displaced.insert(id);
                }
                apply_mutation(&mut self.ground, &decision.action.mutation);
                if !local_winners.contains(&target)
                    || self.displaced.contains(&decision.action.id())
                {
                    apply_mutation(&mut self.local, &decision.action.mutation);
                }
            }
            self.pending.remove(&decision.action.id());
            self.displaced.remove(&decision.action.id());
        }
        Ok(())
    }
}

#[derive(Debug)]
struct OpenGlobalAdminTick {
    tick: TickStamp,
    bindings: HashMap<GlobalPlayerId, [u8; 16]>,
    completion: GlobalAdminCompletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminLedgerError {
    DuplicateTick,
    LateTick,
    Capacity,
    UnexpectedFrame,
    Completion(GlobalAdminCompletionError),
    Resolution(AdminResolutionError),
    Local(AdminLocalError),
}

#[derive(Debug)]
pub struct GlobalAdminLedger {
    cluster_seed: u64,
    pub replica: GlobalAdminReplica,
    open: std::collections::VecDeque<OpenGlobalAdminTick>,
    early: std::collections::BTreeMap<TickStamp, Vec<(ServerId, GlobalAdminFrame)>>,
    last_settled: Option<TickStamp>,
}

impl GlobalAdminLedger {
    #[must_use]
    pub fn new(cluster_seed: u64, ground: AdminSnapshot) -> Self {
        Self {
            cluster_seed,
            replica: GlobalAdminReplica::new(ground),
            open: std::collections::VecDeque::new(),
            early: std::collections::BTreeMap::new(),
            last_settled: None,
        }
    }

    pub fn open_tick(
        &mut self,
        tick: TickStamp,
        participants: impl IntoIterator<Item = ServerId>,
        bindings: HashMap<GlobalPlayerId, [u8; 16]>,
    ) -> Result<(), GlobalAdminLedgerError> {
        if self.last_settled.is_some_and(|last| !tick.is_after(last)) {
            return Err(GlobalAdminLedgerError::LateTick);
        }
        if self.open.iter().any(|open| open.tick == tick) {
            return Err(GlobalAdminLedgerError::DuplicateTick);
        }
        if self.open.len() >= 256 {
            return Err(GlobalAdminLedgerError::Capacity);
        }
        let mut open = OpenGlobalAdminTick {
            tick,
            bindings,
            completion: GlobalAdminCompletion::new(tick, participants),
        };
        if let Some(early) = self.early.get(&tick).cloned() {
            for (peer, frame) in early {
                Self::receive_open(&mut open, peer, frame)
                    .map_err(GlobalAdminLedgerError::Completion)?;
            }
        }
        self.early.remove(&tick);
        self.open.push_back(open);
        Ok(())
    }

    pub fn receive(
        &mut self,
        peer: ServerId,
        frame: GlobalAdminFrame,
    ) -> Result<(), GlobalAdminLedgerError> {
        let tick = match &frame {
            GlobalAdminFrame::ActionBatch(batch) => batch.tick,
            GlobalAdminFrame::TickFinish(finish) => finish.tick,
            GlobalAdminFrame::DropReport(_) => return Err(GlobalAdminLedgerError::UnexpectedFrame),
        };
        if self.last_settled.is_some_and(|last| !tick.is_after(last)) {
            return Err(GlobalAdminLedgerError::LateTick);
        }
        if let Some(open) = self.open.iter_mut().find(|open| open.tick == tick) {
            return Self::receive_open(open, peer, frame)
                .map_err(GlobalAdminLedgerError::Completion);
        }
        if self.early.values().map(Vec::len).sum::<usize>() >= 4096 {
            return Err(GlobalAdminLedgerError::Capacity);
        }
        self.early.entry(tick).or_default().push((peer, frame));
        Ok(())
    }

    fn receive_open(
        open: &mut OpenGlobalAdminTick,
        peer: ServerId,
        frame: GlobalAdminFrame,
    ) -> Result<(), GlobalAdminCompletionError> {
        let mut completion = open.completion.clone();
        match frame {
            GlobalAdminFrame::ActionBatch(batch) => {
                if batch.tick != open.tick {
                    return Err(GlobalAdminCompletionError::WrongTick);
                }
                for action in batch.actions {
                    completion.receive_action(peer, action)?;
                }
            }
            GlobalAdminFrame::TickFinish(finish) => completion.receive_finish(peer, finish)?,
            GlobalAdminFrame::DropReport(_) => {
                return Err(GlobalAdminCompletionError::UnexpectedFrame);
            }
        }
        open.completion = completion;
        Ok(())
    }

    pub fn settle_oldest(&mut self) -> Result<Option<Vec<AdminDecision>>, GlobalAdminLedgerError> {
        let Some(open) = self.open.front() else {
            return Ok(None);
        };
        let Some(actions) = open.completion.actions() else {
            return Ok(None);
        };
        let (_, decisions) = resolve_global_admin(
            self.cluster_seed,
            open.tick,
            &self.replica.ground,
            &open.bindings,
            &actions,
        )
        .map_err(GlobalAdminLedgerError::Resolution)?;
        self.replica
            .settle(open.tick, &decisions)
            .map_err(GlobalAdminLedgerError::Local)?;
        self.last_settled = Some(open.tick);
        self.open.pop_front();
        Ok(Some(decisions))
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::*;

    #[test]
    fn later_complete_tick_cannot_pass_earlier_unresolved_tick() {
        let a = ServerId(1);
        let b = ServerId(2);
        let mut ledger = GlobalAdminLedger::new(1, AdminSnapshot::default());
        ledger
            .open_tick(TickStamp(1), [a, b], HashMap::new())
            .unwrap();
        ledger
            .receive(
                b,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(2),
                    actions: vec![],
                }),
            )
            .unwrap();
        ledger
            .open_tick(TickStamp(2), [a, b], HashMap::new())
            .unwrap();
        ledger
            .receive(
                a,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(2),
                    actions: vec![],
                }),
            )
            .unwrap();
        assert_eq!(ledger.settle_oldest(), Ok(None));
        ledger
            .receive(
                a,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(1),
                    actions: vec![],
                }),
            )
            .unwrap();
        ledger
            .receive(
                b,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(1),
                    actions: vec![],
                }),
            )
            .unwrap();
        assert_eq!(ledger.settle_oldest(), Ok(Some(vec![])));
        assert_eq!(ledger.settle_oldest(), Ok(Some(vec![])));
        assert_eq!(
            ledger.receive(
                a,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(1),
                    actions: vec![]
                })
            ),
            Err(GlobalAdminLedgerError::LateTick)
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalAdminIntent {
    pub issuer: AdminIssuer,
    pub mutation: AdminMutation,
    pub mutation_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalAdminOutbound {
    pub peers: Vec<ServerId>,
    pub frame: GlobalAdminFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminOwnerError {
    NoOpenTick,
    TickPending,
    LateTick,
    Capacity,
    Capture(AdminRejection),
    Local(AdminLocalError),
    Ledger(GlobalAdminLedgerError),
    Wire(GlobalAdminWireError),
}

#[derive(Debug)]
pub struct GlobalAdminOwner {
    pub ledger: GlobalAdminLedger,
    local: ServerId,
    current_tick: Option<TickStamp>,
    current_peers: Vec<ServerId>,
    current_actions: Vec<GlobalAdminAction>,
    sequences: HashMap<ActorId, u16>,
    outbound: std::collections::VecDeque<GlobalAdminOutbound>,
}

impl GlobalAdminOwner {
    #[must_use]
    pub fn new(local: ServerId, cluster_seed: u64, ground: AdminSnapshot) -> Self {
        Self {
            ledger: GlobalAdminLedger::new(cluster_seed, ground),
            local,
            current_tick: None,
            current_peers: vec![],
            current_actions: vec![],
            sequences: HashMap::new(),
            outbound: std::collections::VecDeque::new(),
        }
    }

    pub fn begin_tick(
        &mut self,
        tick: TickStamp,
        peers: impl IntoIterator<Item = ServerId>,
        bindings: HashMap<GlobalPlayerId, [u8; 16]>,
    ) -> Result<(), GlobalAdminOwnerError> {
        if self.current_tick == Some(tick) {
            return Ok(());
        }
        if self
            .current_tick
            .is_some_and(|current| !tick.is_after(current))
            || self
                .ledger
                .open
                .back()
                .is_some_and(|last| !tick.is_after(last.tick))
        {
            return Err(GlobalAdminOwnerError::LateTick);
        }
        if self.current_tick.is_some() {
            self.seal_tick()?;
        }
        let mut participants: Vec<_> = peers.into_iter().collect();
        participants.push(self.local);
        participants.sort_unstable();
        participants.dedup();
        self.ledger
            .open_tick(tick, participants.iter().copied(), bindings)
            .map_err(GlobalAdminOwnerError::Ledger)?;
        self.current_tick = Some(tick);
        self.current_peers = participants
            .into_iter()
            .filter(|peer| *peer != self.local)
            .collect();
        Ok(())
    }

    pub fn submit(
        &mut self,
        intent: GlobalAdminIntent,
    ) -> Result<GlobalAdminActionId, GlobalAdminOwnerError> {
        let tick = self.current_tick.ok_or(GlobalAdminOwnerError::NoOpenTick)?;
        if self.ledger.open.back().is_none_or(|open| open.tick != tick) {
            return Err(GlobalAdminOwnerError::TickPending);
        }
        if self.current_actions.len() >= GLOBAL_ADMIN_BATCH_LIMIT {
            return Err(GlobalAdminOwnerError::Local(AdminLocalError::Capacity));
        }
        let actor = intent.issuer.actor();
        let next_sequence = self
            .sequences
            .get(&actor)
            .copied()
            .unwrap_or(0)
            .wrapping_add(1);
        let bindings = &self
            .ledger
            .open
            .back()
            .ok_or(GlobalAdminOwnerError::NoOpenTick)?
            .bindings;
        let action = capture_global_admin_action(
            self.local,
            tick,
            next_sequence,
            intent.mutation_id,
            intent.issuer,
            intent.mutation,
            &self.ledger.replica.ground,
            &self.ledger.replica.local,
            bindings,
        )
        .map_err(GlobalAdminOwnerError::Capture)?;
        if postcard::to_allocvec(&action.mutation)
            .map_err(|_| GlobalAdminOwnerError::Wire(GlobalAdminWireError::Invalid))?
            .len()
            > GLOBAL_ADMIN_MUTATION_LIMIT
        {
            return Err(GlobalAdminOwnerError::Wire(GlobalAdminWireError::TooLarge));
        }
        let mut candidate_actions = self.current_actions.clone();
        candidate_actions.push(action.clone());
        encode_global_admin_frame(&GlobalAdminFrame::ActionBatch(GlobalAdminActionBatch {
            tick,
            actions: candidate_actions,
        }))
        .map_err(GlobalAdminOwnerError::Wire)?;
        self.ledger
            .replica
            .apply_local(action.clone())
            .map_err(GlobalAdminOwnerError::Local)?;
        self.sequences.insert(actor, next_sequence);
        let id = action.id();
        self.current_actions.push(action);
        Ok(id)
    }

    pub fn seal_tick(&mut self) -> Result<(), GlobalAdminOwnerError> {
        let tick = self.current_tick.ok_or(GlobalAdminOwnerError::NoOpenTick)?;
        let needed = if self.current_peers.is_empty() {
            0
        } else if self.current_actions.is_empty() {
            1
        } else {
            2
        };
        if self.outbound.len() + needed > 4096 {
            return Err(GlobalAdminOwnerError::Capacity);
        }
        let actions = self.current_actions.clone();
        let finish = GlobalAdminTickFinish {
            tick,
            actions: actions.iter().map(GlobalAdminAction::id).collect(),
        };
        if !actions.is_empty() {
            let frame = GlobalAdminFrame::ActionBatch(GlobalAdminActionBatch { tick, actions });
            self.ledger
                .receive(self.local, frame.clone())
                .map_err(GlobalAdminOwnerError::Ledger)?;
            if !self.current_peers.is_empty() {
                self.outbound.push_back(GlobalAdminOutbound {
                    peers: self.current_peers.clone(),
                    frame,
                });
            }
        }
        let frame = GlobalAdminFrame::TickFinish(finish);
        self.ledger
            .receive(self.local, frame.clone())
            .map_err(GlobalAdminOwnerError::Ledger)?;
        self.current_tick = None;
        self.current_actions.clear();
        if !self.current_peers.is_empty() {
            self.outbound.push_back(GlobalAdminOutbound {
                peers: std::mem::take(&mut self.current_peers),
                frame,
            });
        } else {
            self.current_peers.clear();
        }
        Ok(())
    }

    pub fn receive(
        &mut self,
        peer: ServerId,
        frame: GlobalAdminFrame,
    ) -> Result<(), GlobalAdminOwnerError> {
        self.ledger
            .receive(peer, frame)
            .map_err(GlobalAdminOwnerError::Ledger)
    }

    pub fn settle_oldest(&mut self) -> Result<Option<Vec<AdminDecision>>, GlobalAdminOwnerError> {
        self.ledger
            .settle_oldest()
            .map_err(GlobalAdminOwnerError::Ledger)
    }

    #[must_use]
    pub fn next_outbound(&self) -> Option<&GlobalAdminOutbound> {
        self.outbound.front()
    }

    pub fn outbound_sent(&mut self) {
        self.outbound.pop_front();
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;

    #[test]
    fn two_peers_settle_console_grant_after_out_of_order_delivery() {
        let a = ServerId(1);
        let b = ServerId(2);
        let mut owner_a = GlobalAdminOwner::new(a, 77, AdminSnapshot::default());
        let mut owner_b = GlobalAdminOwner::new(b, 77, AdminSnapshot::default());
        owner_a
            .begin_tick(TickStamp(4), [b], HashMap::new())
            .unwrap();
        owner_b
            .begin_tick(TickStamp(4), [a], HashMap::new())
            .unwrap();
        owner_a
            .submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: a },
                mutation_id: [13; 16],
                mutation: AdminMutation::GrantOp(OpGrant::new(
                    [9; 16],
                    String::from("Target"),
                    4,
                    false,
                )),
            })
            .unwrap();
        owner_a.seal_tick().unwrap();
        owner_b.seal_tick().unwrap();
        let mut from_a = vec![];
        while let Some(outbound) = owner_a.next_outbound() {
            from_a.push(outbound.frame.clone());
            owner_a.outbound_sent();
        }
        let mut from_b = vec![];
        while let Some(outbound) = owner_b.next_outbound() {
            from_b.push(outbound.frame.clone());
            owner_b.outbound_sent();
        }
        owner_b.receive(a, from_a.pop().unwrap()).unwrap();
        assert_eq!(owner_b.settle_oldest(), Ok(None));
        owner_a.receive(b, from_b.pop().unwrap()).unwrap();
        owner_b.receive(a, from_a.pop().unwrap()).unwrap();
        let a_decisions = owner_a.settle_oldest().unwrap().unwrap();
        let b_decisions = owner_b.settle_oldest().unwrap().unwrap();
        assert_eq!(a_decisions, b_decisions);
        assert_eq!(owner_a.ledger.replica.ground, owner_b.ledger.replica.ground);
        assert_eq!(owner_a.ledger.replica.local, owner_b.ledger.replica.local);
        assert_eq!(owner_a.ledger.replica.ground.operators.len(), 1);
    }

    #[test]
    fn oversized_admin_intent_is_rejected_before_local_mutation() {
        let local = ServerId(1);
        let mut owner = GlobalAdminOwner::new(local, 77, AdminSnapshot::default());
        owner.begin_tick(TickStamp(4), [], HashMap::new()).unwrap();
        let mutation = AdminMutation::AddBan(BanEntry::new(
            [9; 16],
            String::from("Target"),
            String::from("Console"),
            "x".repeat(GLOBAL_ADMIN_FRAME_LIMIT),
            0,
            None,
        ));
        assert_eq!(
            owner.submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: local },
                mutation_id: [14; 16],
                mutation,
            }),
            Err(GlobalAdminOwnerError::Wire(GlobalAdminWireError::TooLarge))
        );
        assert!(owner.ledger.replica.local.banned_players.is_empty());
    }

    #[test]
    fn single_peer_ticks_settle_without_filling_network_outbox() {
        let local = ServerId(1);
        let mut owner = GlobalAdminOwner::new(local, 77, AdminSnapshot::default());
        for tick in 1..=260_u16 {
            owner
                .begin_tick(TickStamp(tick), [], HashMap::new())
                .unwrap();
            owner.seal_tick().unwrap();
            assert_eq!(owner.settle_oldest(), Ok(Some(vec![])));
        }
        assert!(owner.next_outbound().is_none());
    }

    #[test]
    fn later_admin_intent_captures_while_prior_tick_waits_for_peer() {
        let local = ServerId(1);
        let remote = ServerId(2);
        let mut owner = GlobalAdminOwner::new(local, 77, AdminSnapshot::default());
        owner
            .begin_tick(TickStamp(4), [remote], HashMap::new())
            .unwrap();
        owner
            .submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: local },
                mutation_id: [11; 16],
                mutation: AdminMutation::AddBan(BanEntry::new(
                    [1; 16],
                    String::from("First"),
                    String::from("Console"),
                    String::from("Test"),
                    0,
                    None,
                )),
            })
            .unwrap();
        owner.seal_tick().unwrap();
        owner
            .begin_tick(TickStamp(5), [remote], HashMap::new())
            .unwrap();
        owner
            .submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: local },
                mutation_id: [12; 16],
                mutation: AdminMutation::GrantOp(OpGrant::new(
                    [2; 16],
                    String::from("Second"),
                    4,
                    false,
                )),
            })
            .unwrap();
        owner.seal_tick().unwrap();
        assert_eq!(owner.ledger.replica.local.operators.len(), 1);
        assert_eq!(owner.ledger.replica.local.banned_players.len(), 1);
        assert!(owner.ledger.replica.ground.operators.is_empty());
        owner
            .receive(
                remote,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(5),
                    actions: vec![],
                }),
            )
            .unwrap();
        assert_eq!(owner.settle_oldest(), Ok(None));
        owner
            .receive(
                remote,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(4),
                    actions: vec![],
                }),
            )
            .unwrap();
        assert_eq!(owner.settle_oldest().unwrap().unwrap().len(), 1);
        assert_eq!(owner.settle_oldest().unwrap().unwrap().len(), 1);
        assert_eq!(owner.ledger.replica.ground.operators.len(), 1);
        assert_eq!(owner.ledger.replica.ground.banned_players.len(), 1);
    }

    #[test]
    fn prior_remote_winner_displaces_later_local_optimism() {
        let local = ServerId(1);
        let remote = ServerId(2);
        let mut owner = GlobalAdminOwner::new(local, 77, AdminSnapshot::default());
        let remote_grant = OpGrant::new([9; 16], String::from("Target"), 2, false);
        let local_grant = OpGrant::new([9; 16], String::from("Target"), 4, false);
        let remote_action = capture_global_admin_action(
            remote,
            TickStamp(4),
            1,
            [21; 16],
            AdminIssuer::Console { server: remote },
            AdminMutation::GrantOp(remote_grant.clone()),
            &AdminSnapshot::default(),
            &AdminSnapshot::default(),
            &HashMap::new(),
        )
        .unwrap();
        owner
            .begin_tick(TickStamp(4), [remote], HashMap::new())
            .unwrap();
        owner.seal_tick().unwrap();
        owner
            .begin_tick(TickStamp(5), [remote], HashMap::new())
            .unwrap();
        owner
            .submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: local },
                mutation_id: [22; 16],
                mutation: AdminMutation::GrantOp(local_grant),
            })
            .unwrap();
        owner.seal_tick().unwrap();
        owner
            .receive(
                remote,
                GlobalAdminFrame::ActionBatch(GlobalAdminActionBatch {
                    tick: TickStamp(4),
                    actions: vec![remote_action.clone()],
                }),
            )
            .unwrap();
        owner
            .receive(
                remote,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(4),
                    actions: vec![remote_action.id()],
                }),
            )
            .unwrap();
        assert_eq!(owner.settle_oldest().unwrap().unwrap().len(), 1);
        assert_eq!(
            owner.ledger.replica.local.operators,
            vec![remote_grant.clone()]
        );
        owner
            .receive(
                remote,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(5),
                    actions: vec![],
                }),
            )
            .unwrap();
        let decisions = owner.settle_oldest().unwrap().unwrap();
        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].rejection.is_some());
        assert_eq!(
            owner.ledger.replica.local.operators,
            vec![remote_grant.clone()]
        );
        assert_eq!(owner.ledger.replica.ground.operators, vec![remote_grant]);
    }

    #[test]
    fn operator_and_ban_mutations_for_one_uuid_remain_independent() {
        let local = ServerId(1);
        let remote = ServerId(2);
        let mut owner = GlobalAdminOwner::new(local, 77, AdminSnapshot::default());
        let grant = OpGrant::new([9; 16], String::from("Target"), 4, false);
        let ban = BanEntry::new(
            [9; 16],
            String::from("Target"),
            String::from("Console"),
            String::from("Test"),
            0,
            None,
        );
        let remote_action = capture_global_admin_action(
            remote,
            TickStamp(4),
            1,
            [31; 16],
            AdminIssuer::Console { server: remote },
            AdminMutation::AddBan(ban.clone()),
            &AdminSnapshot::default(),
            &AdminSnapshot::default(),
            &HashMap::new(),
        )
        .unwrap();
        owner
            .begin_tick(TickStamp(4), [remote], HashMap::new())
            .unwrap();
        owner
            .submit(GlobalAdminIntent {
                issuer: AdminIssuer::Console { server: local },
                mutation_id: [32; 16],
                mutation: AdminMutation::GrantOp(grant.clone()),
            })
            .unwrap();
        owner.seal_tick().unwrap();
        owner
            .receive(
                remote,
                GlobalAdminFrame::ActionBatch(GlobalAdminActionBatch {
                    tick: TickStamp(4),
                    actions: vec![remote_action.clone()],
                }),
            )
            .unwrap();
        owner
            .receive(
                remote,
                GlobalAdminFrame::TickFinish(GlobalAdminTickFinish {
                    tick: TickStamp(4),
                    actions: vec![remote_action.id()],
                }),
            )
            .unwrap();
        let decisions = owner.settle_oldest().unwrap().unwrap();
        assert_eq!(decisions.len(), 2);
        assert!(
            decisions
                .iter()
                .all(|decision| decision.rejection.is_none())
        );
        assert_eq!(owner.ledger.replica.local.operators, vec![grant.clone()]);
        assert_eq!(owner.ledger.replica.local.banned_players, vec![ban.clone()]);
        assert_eq!(owner.ledger.replica.ground.operators, vec![grant]);
        assert_eq!(owner.ledger.replica.ground.banned_players, vec![ban]);
    }
}
