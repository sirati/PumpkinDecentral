use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::global_admin::{GlobalAdminAction, GlobalAdminActionId};
use crate::identity::ServerId;
use crate::time::TickStamp;

pub const GLOBAL_ADMIN_DROP_MAX_PARTS: u16 = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalAdminDropReport {
    pub tick: TickStamp,
    pub dropped: ServerId,
    pub dropped_generation: u64,
    pub reporter: ServerId,
    pub reporter_generation: u64,
    pub part_index: u16,
    pub part_total: u16,
    pub complete: bool,
    pub observed: Vec<GlobalAdminAction>,
    pub expected: Vec<GlobalAdminActionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminDropError {
    WrongTick,
    WrongDroppedPeer,
    WrongGeneration,
    UnexpectedReporter,
    InvalidPart,
    WrongActionSource,
    ConflictingReport,
    ConflictingAction,
    Capacity,
}

#[derive(Debug, Clone)]
pub struct GlobalAdminDropCollector {
    tick: TickStamp,
    dropped: ServerId,
    dropped_generation: u64,
    survivors: BTreeMap<ServerId, u64>,
    parts: BTreeMap<ServerId, Vec<Option<GlobalAdminDropReport>>>,
}

impl GlobalAdminDropCollector {
    #[must_use]
    pub fn new(
        tick: TickStamp,
        dropped: ServerId,
        dropped_generation: u64,
        survivors: impl IntoIterator<Item = (ServerId, u64)>,
    ) -> Self {
        Self {
            tick,
            dropped,
            dropped_generation,
            survivors: survivors.into_iter().collect(),
            parts: BTreeMap::new(),
        }
    }

    pub fn receive(
        &mut self,
        authenticated_reporter: ServerId,
        authenticated_generation: u64,
        report: GlobalAdminDropReport,
    ) -> Result<(), GlobalAdminDropError> {
        if report.tick != self.tick {
            return Err(GlobalAdminDropError::WrongTick);
        }
        if report.dropped != self.dropped {
            return Err(GlobalAdminDropError::WrongDroppedPeer);
        }
        if report.dropped_generation != self.dropped_generation
            || report.reporter_generation != authenticated_generation
        {
            return Err(GlobalAdminDropError::WrongGeneration);
        }
        if report.reporter != authenticated_reporter
            || self.survivors.get(&report.reporter) != Some(&authenticated_generation)
        {
            return Err(GlobalAdminDropError::UnexpectedReporter);
        }
        if report.part_total == 0
            || report.part_total > GLOBAL_ADMIN_DROP_MAX_PARTS
            || report.part_index >= report.part_total
            || report.complete != (report.part_index + 1 == report.part_total)
        {
            return Err(GlobalAdminDropError::InvalidPart);
        }
        if report.observed.len() > 256 || report.expected.len() > 256 {
            return Err(GlobalAdminDropError::Capacity);
        }
        if report.observed.iter().any(|action| {
            action.tick != self.tick
                || action.source != self.dropped
                || action.id().actor.origin() != self.dropped
        }) || report
            .expected
            .iter()
            .any(|id| id.actor.origin() != self.dropped)
        {
            return Err(GlobalAdminDropError::WrongActionSource);
        }
        let parts = self
            .parts
            .entry(report.reporter)
            .or_insert_with(|| vec![None; usize::from(report.part_total)]);
        if parts.len() != usize::from(report.part_total) {
            return Err(GlobalAdminDropError::ConflictingReport);
        }
        let slot = &mut parts[usize::from(report.part_index)];
        if let Some(previous) = slot {
            if *previous != report {
                return Err(GlobalAdminDropError::ConflictingReport);
            }
            return Ok(());
        }
        *slot = Some(report);
        Ok(())
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.survivors.keys().all(|peer| {
            self.parts
                .get(peer)
                .is_some_and(|parts| parts.iter().all(Option::is_some))
        })
    }

    pub fn reconcile(
        &self,
    ) -> Result<Option<(Vec<GlobalAdminAction>, Vec<GlobalAdminActionId>)>, GlobalAdminDropError>
    {
        if !self.is_complete() {
            return Ok(None);
        }
        let mut actions = BTreeMap::new();
        let mut expected = BTreeSet::new();
        for parts in self.parts.values() {
            for report in parts.iter().flatten() {
                expected.extend(report.expected.iter().copied());
                for action in &report.observed {
                    match actions.get(&action.id()) {
                        Some(previous) if previous != action => {
                            return Err(GlobalAdminDropError::ConflictingAction);
                        }
                        Some(_) => {}
                        None => {
                            actions.insert(action.id(), action.clone());
                        }
                    }
                }
            }
        }
        if actions.len() > 256 {
            return Err(GlobalAdminDropError::Capacity);
        }
        let observed_ids: BTreeSet<_> = actions.keys().copied().collect();
        let lost = expected.difference(&observed_ids).copied().collect();
        Ok(Some((actions.into_values().collect(), lost)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_sync::{AdminMutation, OpRevoke};
    use crate::global_admin::{AdminIssuer, AdminTargetBefore};

    fn action() -> GlobalAdminAction {
        GlobalAdminAction {
            issuer: AdminIssuer::Console {
                server: ServerId(3),
            },
            source: ServerId(3),
            tick: TickStamp(4),
            sequence: 1,
            mutation_id: [4; 16],
            permission_revision: 0,
            issuer_op_before: None,
            target_before: AdminTargetBefore::Operator(None),
            undo: AdminTargetBefore::Operator(None),
            mutation: AdminMutation::RevokeOp(OpRevoke::new([9; 16], String::from("Target"))),
        }
    }

    fn report(
        reporter: ServerId,
        observed: Vec<GlobalAdminAction>,
        expected: Vec<GlobalAdminActionId>,
    ) -> GlobalAdminDropReport {
        GlobalAdminDropReport {
            tick: TickStamp(4),
            dropped: ServerId(3),
            dropped_generation: 8,
            reporter,
            reporter_generation: 5,
            part_index: 0,
            part_total: 1,
            complete: true,
            observed,
            expected,
        }
    }

    #[test]
    fn two_survivors_merge_observed_actions_and_detect_missing_declared_id() {
        let a = ServerId(1);
        let b = ServerId(2);
        let action = action();
        let missing = GlobalAdminActionId {
            actor: action.issuer.actor(),
            sequence: 2,
        };
        let mut collector =
            GlobalAdminDropCollector::new(TickStamp(4), ServerId(3), 8, [(a, 5), (b, 5)]);
        collector
            .receive(b, 5, report(b, vec![], vec![action.id(), missing]))
            .unwrap();
        assert_eq!(collector.reconcile(), Ok(None));
        collector
            .receive(a, 5, report(a, vec![action.clone()], vec![action.id()]))
            .unwrap();
        assert_eq!(
            collector.reconcile(),
            Ok(Some((vec![action], vec![missing])))
        );
    }

    #[test]
    fn stale_generation_and_conflicting_recovery_are_rejected() {
        let a = ServerId(1);
        let mut collector = GlobalAdminDropCollector::new(TickStamp(4), ServerId(3), 8, [(a, 5)]);
        assert_eq!(
            collector.receive(a, 4, report(a, vec![], vec![])),
            Err(GlobalAdminDropError::WrongGeneration)
        );
        let original = action();
        collector
            .receive(a, 5, report(a, vec![original.clone()], vec![original.id()]))
            .unwrap();
        let mut conflicting = original;
        conflicting.mutation_id = [7; 16];
        assert_eq!(
            collector.receive(a, 5, report(a, vec![conflicting], vec![])),
            Err(GlobalAdminDropError::ConflictingReport)
        );
    }
}
