use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admin_sync::AdminSnapshot;
use crate::identity::ServerId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalAdminAdmissionState {
    pub reporter: ServerId,
    pub generation: u64,
    pub settled_tick_full: u64,
    pub ground: AdminSnapshot,
    pub undurable_history_digest: [u8; 32],
    pub undurable_history_len: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminAdmissionError {
    UnexpectedReporter,
    WrongGeneration,
    ConflictingReport,
    HistoryCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalAdminAdmissionResult {
    NoSurvivors,
    Waiting,
    Diverged,
    Agreed {
        settled_tick_full: u64,
        ground: AdminSnapshot,
        undurable_history_digest: [u8; 32],
        undurable_history_len: u16,
    },
}

#[derive(Debug)]
pub struct GlobalAdminAdmissionReconciler {
    survivors: BTreeMap<ServerId, u64>,
    reports: BTreeMap<ServerId, GlobalAdminAdmissionState>,
}

impl GlobalAdminAdmissionReconciler {
    #[must_use]
    pub fn new(survivors: impl IntoIterator<Item = (ServerId, u64)>) -> Self {
        Self {
            survivors: survivors.into_iter().collect(),
            reports: BTreeMap::new(),
        }
    }

    pub fn receive(
        &mut self,
        authenticated_peer: ServerId,
        authenticated_generation: u64,
        report: GlobalAdminAdmissionState,
    ) -> Result<(), GlobalAdminAdmissionError> {
        if report.reporter != authenticated_peer
            || !self.survivors.contains_key(&authenticated_peer)
        {
            return Err(GlobalAdminAdmissionError::UnexpectedReporter);
        }
        if report.generation != authenticated_generation
            || self.survivors.get(&authenticated_peer) != Some(&authenticated_generation)
        {
            return Err(GlobalAdminAdmissionError::WrongGeneration);
        }
        if report.undurable_history_len > 4096 {
            return Err(GlobalAdminAdmissionError::HistoryCapacity);
        }
        match self.reports.get(&authenticated_peer) {
            Some(previous) if previous != &report => {
                Err(GlobalAdminAdmissionError::ConflictingReport)
            }
            Some(_) => Ok(()),
            None => {
                self.reports.insert(authenticated_peer, report);
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn result(&self) -> GlobalAdminAdmissionResult {
        if self.survivors.is_empty() {
            return GlobalAdminAdmissionResult::NoSurvivors;
        }
        if self.reports.len() != self.survivors.len() {
            return GlobalAdminAdmissionResult::Waiting;
        }
        let Some(first) = self.reports.values().next() else {
            return GlobalAdminAdmissionResult::Waiting;
        };
        let mut canonical = first.ground.clone();
        canonical.operators.sort_by_key(|entry| entry.uuid);
        canonical.banned_players.sort_by_key(|entry| entry.uuid);
        if self.reports.values().any(|report| {
            let mut ground = report.ground.clone();
            ground.operators.sort_by_key(|entry| entry.uuid);
            ground.banned_players.sort_by_key(|entry| entry.uuid);
            report.settled_tick_full != first.settled_tick_full
                || report.undurable_history_digest != first.undurable_history_digest
                || report.undurable_history_len != first.undurable_history_len
                || ground.operators != canonical.operators
                || ground.banned_players != canonical.banned_players
        }) {
            return GlobalAdminAdmissionResult::Diverged;
        }
        GlobalAdminAdmissionResult::Agreed {
            settled_tick_full: first.settled_tick_full,
            ground: canonical,
            undurable_history_digest: first.undurable_history_digest,
            undurable_history_len: first.undurable_history_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_sync::OpGrant;

    fn state(peer: ServerId, tick: u64, level: u8) -> GlobalAdminAdmissionState {
        GlobalAdminAdmissionState {
            reporter: peer,
            generation: 4,
            settled_tick_full: tick,
            ground: AdminSnapshot {
                revision: 8,
                operators: vec![OpGrant::new([9; 16], String::from("Target"), level, false)],
                banned_players: vec![],
            },
            undurable_history_digest: [7; 32],
            undurable_history_len: 1,
        }
    }

    #[test]
    fn rejoining_primary_waits_for_all_survivors_and_rejects_stale_truth() {
        let a = ServerId(1);
        let b = ServerId(2);
        let mut reconcile = GlobalAdminAdmissionReconciler::new([(a, 4), (b, 4)]);
        reconcile.receive(a, 4, state(a, 100, 4)).unwrap();
        assert_eq!(reconcile.result(), GlobalAdminAdmissionResult::Waiting);
        reconcile.receive(b, 4, state(b, 99, 3)).unwrap();
        assert_eq!(reconcile.result(), GlobalAdminAdmissionResult::Diverged);
    }

    #[test]
    fn matching_survivors_supply_ground_state_before_primary_seed() {
        assert_eq!(
            GlobalAdminAdmissionReconciler::new([]).result(),
            GlobalAdminAdmissionResult::NoSurvivors
        );
        let a = ServerId(1);
        let b = ServerId(2);
        let mut reconcile = GlobalAdminAdmissionReconciler::new([(a, 4), (b, 4)]);
        reconcile.receive(b, 4, state(b, 100, 4)).unwrap();
        reconcile.receive(a, 4, state(a, 100, 4)).unwrap();
        assert!(matches!(
            reconcile.result(),
            GlobalAdminAdmissionResult::Agreed {
                settled_tick_full: 100,
                ..
            }
        ));
        assert_eq!(
            reconcile.receive(a, 3, state(a, 100, 4)),
            Err(GlobalAdminAdmissionError::WrongGeneration)
        );
    }
}
