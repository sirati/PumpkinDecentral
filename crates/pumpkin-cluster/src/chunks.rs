use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::protocol::ChunkKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAdvert {
    pub holder: u16,
    pub chunk: ChunkKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDrop {
    pub holder: u16,
    pub chunk: ChunkKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFetch {
    pub chunk: ChunkKey,
    pub from: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkAnnounce {
    Acquire(ChunkAdvert),
    Release(ChunkDrop),
}

#[derive(Debug, Clone, Default)]
pub struct Directory {
    pub holders: HashMap<ChunkKey, HashSet<u16>>,
}

impl Directory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_advert(&mut self, advert: ChunkAdvert) {
        self.holders
            .entry(advert.chunk)
            .or_default()
            .insert(advert.holder);
    }

    pub fn apply_drop(&mut self, drop: ChunkDrop) {
        if let Some(holders) = self.holders.get_mut(&drop.chunk) {
            holders.remove(&drop.holder);
            if holders.is_empty() {
                self.holders.remove(&drop.chunk);
            }
        }
    }

    pub fn drop_holder(&mut self, holder: u16) -> Vec<ChunkKey> {
        let mut dropped = Vec::new();
        self.holders.retain(|chunk, holders| {
            if holders.remove(&holder) {
                dropped.push(*chunk);
            }
            !holders.is_empty()
        });
        dropped.sort_unstable();
        dropped
    }

    #[must_use]
    pub fn holders_of(&self, chunk: &ChunkKey) -> Option<&HashSet<u16>> {
        self.holders.get(chunk)
    }

    #[must_use]
    pub fn fetch_for(&self, chunk: &ChunkKey) -> Option<ChunkFetch> {
        let from = select_holder(&self.sorted_holders(chunk))?;
        Some(ChunkFetch {
            chunk: *chunk,
            from,
        })
    }

    #[must_use]
    pub fn sorted_holders(&self, chunk: &ChunkKey) -> Vec<u16> {
        match self.holders.get(chunk) {
            Some(holders) => {
                let mut out: Vec<u16> = holders.iter().copied().collect();
                out.sort_unstable();
                out
            }
            None => Vec::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.holders.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }
}

#[must_use]
pub fn select_holder(holders: &[u16]) -> Option<u16> {
    holders.iter().copied().min()
}

pub const PRIMARY_CLAIM_RETENTION_MILLIS: u64 = 30_000;

pub const STUCK_FETCH_MILLIS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionState {
    AwaitingRoute,
    Requested {
        peer: u16,
        since_millis: u64,
        diagnostic_emitted: bool,
    },
    Installed {
        revision: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkAcquisition {
    pub chunk: ChunkKey,
    pub state: AcquisitionState,
}

impl ChunkAcquisition {
    #[must_use]
    pub const fn new(chunk: ChunkKey) -> Self {
        Self {
            chunk,
            state: AcquisitionState::AwaitingRoute,
        }
    }

    #[must_use]
    pub fn request(
        &mut self,
        directory: &Directory,
        pings: &PingTracker,
        primary: Option<u16>,
        now_millis: u64,
    ) -> Option<ChunkFetch> {
        self.request_excluding(directory, pings, primary, now_millis, None)
    }

    #[must_use]
    pub fn request_excluding(
        &mut self,
        directory: &Directory,
        pings: &PingTracker,
        primary: Option<u16>,
        now_millis: u64,
        excluded: Option<u16>,
    ) -> Option<ChunkFetch> {
        if !matches!(self.state, AcquisitionState::AwaitingRoute) {
            return None;
        }
        let holders: Vec<u16> = directory
            .sorted_holders(&self.chunk)
            .into_iter()
            .filter(|peer| Some(*peer) != excluded)
            .collect();
        let peer = pings.best(&holders).or(primary)?;
        self.state = AcquisitionState::Requested {
            peer,
            since_millis: now_millis,
            diagnostic_emitted: false,
        };
        Some(ChunkFetch {
            chunk: self.chunk,
            from: peer,
        })
    }

    #[must_use]
    pub fn request_from(&mut self, peer: u16, now_millis: u64) -> Option<ChunkFetch> {
        if !matches!(self.state, AcquisitionState::AwaitingRoute) {
            return None;
        }
        self.state = AcquisitionState::Requested {
            peer,
            since_millis: now_millis,
            diagnostic_emitted: false,
        };
        Some(ChunkFetch {
            chunk: self.chunk,
            from: peer,
        })
    }

    pub fn unavailable(&mut self, peer: u16) -> bool {
        if matches!(self.state, AcquisitionState::Requested { peer: selected, .. } if selected == peer)
        {
            self.state = AcquisitionState::AwaitingRoute;
            true
        } else {
            false
        }
    }

    pub fn install(&mut self, peer: u16, revision: u64) -> bool {
        if matches!(self.state, AcquisitionState::Requested { peer: selected, .. } if selected == peer)
        {
            self.state = AcquisitionState::Installed { revision };
            true
        } else {
            false
        }
    }

    pub fn take_stuck_diagnostic(&mut self, now_millis: u64) -> Option<(u16, u64)> {
        let AcquisitionState::Requested {
            peer,
            since_millis,
            diagnostic_emitted,
        } = &mut self.state
        else {
            return None;
        };
        let waited = now_millis.saturating_sub(*since_millis);
        if *diagnostic_emitted || waited < STUCK_FETCH_MILLIS {
            return None;
        }
        *diagnostic_emitted = true;
        Some((*peer, waited))
    }
}

#[derive(Debug, Default)]
pub struct PrimaryClaims {
    last_requested: HashMap<ChunkKey, u64>,
}

impl PrimaryClaims {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn note_requested(&mut self, chunk: ChunkKey, now_millis: u64) {
        self.last_requested.insert(chunk, now_millis);
    }

    #[must_use]
    pub fn is_retained(&self, chunk: &ChunkKey, now_millis: u64) -> bool {
        self.last_requested
            .get(chunk)
            .is_some_and(|at| now_millis.saturating_sub(*at) < PRIMARY_CLAIM_RETENTION_MILLIS)
    }

    pub fn take_expired(&mut self, now_millis: u64) -> Vec<ChunkKey> {
        let mut expired = Vec::new();
        self.last_requested.retain(|chunk, at| {
            let live = now_millis.saturating_sub(*at) < PRIMARY_CLAIM_RETENTION_MILLIS;
            if !live {
                expired.push(*chunk);
            }
            live
        });
        expired
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.last_requested.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.last_requested.is_empty()
    }
}

#[must_use]
pub fn fetch_stuck_since(at_millis: u64, now_millis: u64) -> bool {
    now_millis.saturating_sub(at_millis) >= STUCK_FETCH_MILLIS
}

#[derive(Debug, Default)]
pub struct PingTracker {
    samples: HashMap<u16, u64>,
}

impl PingTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, peer: u16, rtt_millis: u64) {
        match self.samples.get(&peer) {
            Some(known) if *known <= rtt_millis => {}
            _ => {
                self.samples.insert(peer, rtt_millis);
            }
        }
    }

    #[must_use]
    pub fn best(&self, holders: &[u16]) -> Option<u16> {
        let mut out: Option<(u64, u16)> = None;
        for holder in holders {
            let ping = self.samples.get(holder).copied().unwrap_or(u64::MAX);
            let candidate = (ping, *holder);
            if out.is_none_or(|current| candidate < current) {
                out = Some(candidate);
            }
        }
        out.map(|(_, holder)| holder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(x: i32, z: i32) -> ChunkKey {
        ChunkKey::new(crate::protocol::WorldId::new(0, 7), x, z)
    }

    #[test]
    fn picks_lowest_holder() {
        assert_eq!(select_holder(&[3, 1, 2]), Some(1));
    }

    #[test]
    fn empty_directory_has_no_holder() {
        assert_eq!(select_holder(&[]), None);
    }

    #[test]
    fn directory_tracks_adverts_and_drops() {
        let mut directory = Directory::new();
        let first = chunk(0, 0);
        directory.apply_advert(ChunkAdvert {
            holder: 2,
            chunk: first,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 1,
            chunk: first,
        });
        assert_eq!(directory.sorted_holders(&first), vec![1, 2]);
        assert_eq!(select_holder(&directory.sorted_holders(&first)), Some(1));
        directory.apply_drop(ChunkDrop {
            holder: 1,
            chunk: first,
        });
        assert_eq!(directory.sorted_holders(&first), vec![2]);
        directory.apply_drop(ChunkDrop {
            holder: 2,
            chunk: first,
        });
        assert!(directory.is_empty());
    }

    #[test]
    fn fetch_for_picks_lowest_holder() {
        let mut directory = Directory::new();
        let target = chunk(1, 2);
        assert_eq!(directory.fetch_for(&target), None);
        directory.apply_advert(ChunkAdvert {
            holder: 3,
            chunk: target,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 1,
            chunk: target,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 2,
            chunk: target,
        });
        assert_eq!(
            directory.fetch_for(&target),
            Some(ChunkFetch {
                chunk: target,
                from: 1
            })
        );
        directory.apply_drop(ChunkDrop {
            holder: 1,
            chunk: target,
        });
        assert_eq!(
            directory.fetch_for(&target),
            Some(ChunkFetch {
                chunk: target,
                from: 2
            })
        );
    }

    #[test]
    fn ping_tracker_prefers_lowest_sample() {
        let mut tracker = PingTracker::new();
        assert_eq!(tracker.best(&[3, 1, 2]), Some(1));
        tracker.record(3, 40);
        tracker.record(1, 120);
        tracker.record(2, 80);
        assert_eq!(tracker.best(&[1, 2, 3]), Some(3));
        tracker.record(1, 10);
        assert_eq!(tracker.best(&[1, 2, 3]), Some(1));
    }

    #[test]
    fn drop_of_unknown_holder_is_noop() {
        let mut directory = Directory::new();
        directory.apply_drop(ChunkDrop {
            holder: 7,
            chunk: chunk(4, 4),
        });
        assert!(directory.is_empty());
    }

    #[test]
    fn disconnect_withdraws_only_that_peers_holds() {
        let mut directory = Directory::new();
        let shared = chunk(1, 1);
        let exclusive = chunk(2, 2);
        directory.apply_advert(ChunkAdvert {
            holder: 4,
            chunk: shared,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 8,
            chunk: shared,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 4,
            chunk: exclusive,
        });

        assert_eq!(directory.drop_holder(4), vec![shared, exclusive]);
        assert_eq!(directory.sorted_holders(&shared), vec![8]);
        assert!(directory.holders_of(&exclusive).is_none());
        assert!(directory.drop_holder(4).is_empty());
    }

    #[test]
    fn claims_retain_for_thirty_seconds() {
        let mut claims = PrimaryClaims::new();
        assert!(claims.is_empty());
        claims.note_requested(chunk(0, 0), 1_000);
        assert!(claims.is_retained(&chunk(0, 0), 1_000 + PRIMARY_CLAIM_RETENTION_MILLIS - 1));
        assert!(!claims.is_retained(&chunk(0, 0), 1_000 + PRIMARY_CLAIM_RETENTION_MILLIS));
    }

    #[test]
    fn claims_refresh_on_each_request() {
        let mut claims = PrimaryClaims::new();
        claims.note_requested(chunk(1, 2), 0);
        claims.note_requested(chunk(1, 2), 29_000);
        assert_eq!(claims.len(), 1);
        assert!(claims.is_retained(&chunk(1, 2), 58_999));
        assert!(!claims.is_retained(&chunk(1, 2), 59_000));
    }

    #[test]
    fn claims_release_only_expired() {
        let mut claims = PrimaryClaims::new();
        claims.note_requested(chunk(0, 0), 0);
        claims.note_requested(chunk(5, 5), 40_000);
        let mut expired = claims.take_expired(31_000);
        expired.sort_unstable();
        assert_eq!(expired, vec![chunk(0, 0)]);
        assert!(claims.is_retained(&chunk(5, 5), 41_000));
        assert!(!claims.is_empty());
    }

    #[test]
    fn stuck_fetch_fires_at_thirty_seconds() {
        assert!(!fetch_stuck_since(1_000, 1_000 + STUCK_FETCH_MILLIS - 1));
        assert!(fetch_stuck_since(1_000, 1_000 + STUCK_FETCH_MILLIS));
    }

    #[test]
    fn acquisition_changes_source_only_after_selected_source_becomes_unavailable() {
        let target = chunk(8, -3);
        let mut directory = Directory::new();
        directory.apply_advert(ChunkAdvert {
            holder: 4,
            chunk: target,
        });
        directory.apply_advert(ChunkAdvert {
            holder: 7,
            chunk: target,
        });
        let mut pings = PingTracker::new();
        pings.record(4, 90);
        pings.record(7, 20);
        let mut acquisition = ChunkAcquisition::new(target);

        assert_eq!(
            acquisition.request(&directory, &pings, Some(1), 1_000),
            Some(ChunkFetch {
                chunk: target,
                from: 7
            })
        );
        directory.apply_drop(ChunkDrop {
            holder: 4,
            chunk: target,
        });
        assert!(!acquisition.unavailable(4));
        assert_eq!(
            acquisition.request(&directory, &pings, Some(1), 2_000),
            None
        );

        directory.apply_drop(ChunkDrop {
            holder: 7,
            chunk: target,
        });
        assert!(acquisition.unavailable(7));
        assert_eq!(
            acquisition.request(&directory, &pings, Some(1), 2_000),
            Some(ChunkFetch {
                chunk: target,
                from: 1
            })
        );
        assert!(!acquisition.install(7, 9));
        assert!(acquisition.install(1, 9));
        assert_eq!(
            acquisition.state,
            AcquisitionState::Installed { revision: 9 }
        );
    }

    #[test]
    fn explicit_primary_drop_can_reselect_primary_generator() {
        let target = chunk(2, 5);
        let mut directory = Directory::new();
        directory.apply_advert(ChunkAdvert {
            holder: 1,
            chunk: target,
        });
        let pings = PingTracker::new();
        let mut acquisition = ChunkAcquisition::new(target);
        assert_eq!(
            acquisition.request(&directory, &pings, Some(1), 500),
            Some(ChunkFetch {
                chunk: target,
                from: 1,
            })
        );
        directory.apply_drop(ChunkDrop {
            holder: 1,
            chunk: target,
        });
        assert!(acquisition.unavailable(1));
        assert_eq!(
            acquisition.request_excluding(&directory, &pings, Some(1), 501, Some(1)),
            Some(ChunkFetch {
                chunk: target,
                from: 1,
            })
        );
    }

    #[test]
    fn acquisition_timeout_reports_once_without_changing_or_reissuing_request() {
        let target = chunk(2, 5);
        let mut acquisition = ChunkAcquisition::new(target);
        let directory = Directory::new();
        let pings = PingTracker::new();
        assert_eq!(
            acquisition.request(&directory, &pings, Some(12), 500),
            Some(ChunkFetch {
                chunk: target,
                from: 12
            })
        );
        assert_eq!(acquisition.take_stuck_diagnostic(30_499), None);
        assert_eq!(
            acquisition.take_stuck_diagnostic(30_500),
            Some((12, 30_000))
        );
        assert_eq!(acquisition.take_stuck_diagnostic(90_000), None);
        assert_eq!(
            acquisition.request(&directory, &pings, Some(12), 90_000),
            None
        );
        assert!(matches!(
            acquisition.state,
            AcquisitionState::Requested { peer: 12, .. }
        ));
    }
}
