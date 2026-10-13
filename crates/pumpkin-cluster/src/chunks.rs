use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::protocol::ChunkKey;
use crate::time::TickStamp;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSubscription {
    pub chunk: ChunkKey,
    pub subscriber: u16,
    pub incarnation: u64,
    pub subscription_id: u64,
    pub from_tick: TickStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSubscriptionRequest {
    pub chunk: ChunkKey,
    pub subscriber: u16,
    pub incarnation: u64,
    pub subscription_id: u64,
    pub from_tick: TickStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSubscriptionStart {
    pub subscription: ChunkSubscription,
    pub provider: u16,
    pub provider_incarnation: u64,
    pub snapshot_cut: TickStamp,
    pub snapshot_hash: [u8; 32],
}

impl ChunkSubscriptionStart {
    #[must_use]
    pub fn new(
        request: ChunkSubscriptionRequest,
        provider: u16,
        provider_incarnation: u64,
        snapshot_cut: TickStamp,
        snapshot_hash: [u8; 32],
    ) -> Self {
        Self {
            subscription: ChunkSubscription {
                chunk: request.chunk,
                subscriber: request.subscriber,
                incarnation: request.incarnation,
                subscription_id: request.subscription_id,
                from_tick: request.from_tick,
            },
            provider,
            provider_incarnation,
            snapshot_cut,
            snapshot_hash,
        }
    }

    #[must_use]
    pub fn has_contiguous_cut(&self) -> bool {
        self.subscription.from_tick.0 == self.snapshot_cut.0.wrapping_add(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkUnsubscription {
    pub chunk: ChunkKey,
    pub subscriber: u16,
    pub incarnation: u64,
    pub subscription_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkSubscriptionControl {
    Request(ChunkSubscriptionRequest),
    Start(ChunkSubscriptionStart),
    Unsubscribe(ChunkUnsubscription),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubscriptionSlot {
    incarnation: u64,
    subscription_id: u64,
    from_tick: TickStamp,
    active: bool,
}

pub const MAX_CHUNK_SUBSCRIPTION_SLOTS: usize = 65_536;
pub const MAX_CHUNK_SUBSCRIPTIONS_PER_PEER: usize = 8_192;

#[derive(Debug, Clone, Default)]
pub struct Directory {
    pub holders: crate::structural_map::StructuralMap<ChunkKey, HashSet<u16>>,
    subscriptions: crate::structural_map::StructuralMap<ChunkKey, crate::structural_map::StructuralMap<u16, SubscriptionSlot>>,
    subscription_slot_count: usize,
    subscriptions_per_peer: crate::structural_map::StructuralMap<u16, usize>,
    holder_chunks: crate::structural_map::StructuralMap<u16, crate::structural_map::StructuralMap<ChunkKey, ()>>,
    subscription_chunks: crate::structural_map::StructuralMap<u16, crate::structural_map::StructuralMap<ChunkKey, ()>>,
}

impl Directory {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    pub fn apply_advert(&mut self, advert: ChunkAdvert) {
        if self.holders.get_or_insert_default(advert.chunk).insert(advert.holder) {
            self.holder_chunks.get_or_insert_default(advert.holder).insert(advert.chunk, ());
        }
    }

    pub fn apply_drop(&mut self, drop: ChunkDrop) {
        if let Some(holders) = self.holders.get_mut(&drop.chunk) {
            holders.remove(&drop.holder);
            if holders.is_empty() { self.holders.remove(&drop.chunk); }
        }
        if let Some(chunks) = self.holder_chunks.get_mut(&drop.holder) {
            chunks.remove(&drop.chunk);
            if chunks.is_empty() { self.holder_chunks.remove(&drop.holder); }
        }
        if let Some(slot) = self.subscriptions.get_mut(&drop.chunk).and_then(|slots| slots.get_mut(&drop.holder)) {
            slot.active = false;
        }
    }

    fn install_subscription_slot(&mut self, chunk: ChunkKey, peer: u16, slot: SubscriptionSlot) {
        if self.subscriptions.get_or_insert_default(chunk).insert(peer, slot).is_none() {
            self.subscription_slot_count += 1;
            *self.subscriptions_per_peer.get_or_insert_default(peer) += 1;
            self.subscription_chunks.get_or_insert_default(peer).insert(chunk, ());
        }
    }

    pub fn subscribe(&mut self, request: ChunkSubscription) -> bool {
        let previous = self.subscriptions.get(&request.chunk).and_then(|slots| slots.get(&request.subscriber)).copied();
        if let Some(slot) = previous {
            if request.incarnation == slot.incarnation && request.subscription_id < slot.subscription_id { return false; }
            if request.incarnation == slot.incarnation && request.subscription_id == slot.subscription_id {
                return slot.active && slot.from_tick == request.from_tick;
            }
        } else if !self.can_add_subscription(request.subscriber) { return false; }
        self.install_subscription_slot(request.chunk, request.subscriber, SubscriptionSlot {
            incarnation: request.incarnation, subscription_id: request.subscription_id, from_tick: request.from_tick, active: true,
        });
        true
    }

    pub fn unsubscribe(&mut self, request: ChunkUnsubscription) -> bool {
        let previous = self.subscriptions.get(&request.chunk).and_then(|slots| slots.get(&request.subscriber)).copied();
        if let Some(mut slot) = previous {
            if request.incarnation == slot.incarnation && request.subscription_id < slot.subscription_id { return false; }
            if request.incarnation == slot.incarnation && request.subscription_id == slot.subscription_id {
                let changed = slot.active;
                slot.active = false;
                self.install_subscription_slot(request.chunk, request.subscriber, slot);
                return changed;
            }
        } else if !self.can_add_subscription(request.subscriber) { return false; }
        self.install_subscription_slot(request.chunk, request.subscriber, SubscriptionSlot {
            incarnation: request.incarnation, subscription_id: request.subscription_id, from_tick: TickStamp(0), active: false,
        });
        true
    }

    fn remove_source_subscriptions(&mut self, peer: u16) {
        let Some(chunks) = self.subscription_chunks.remove(&peer) else { return; };
        for chunk in chunks.keys() {
            if let Some(slots) = self.subscriptions.get_mut(chunk) {
                if slots.remove(&peer).is_some() { self.subscription_slot_count -= 1; }
                if slots.is_empty() { self.subscriptions.remove(chunk); }
            }
        }
        self.subscriptions_per_peer.remove(&peer);
    }

    pub fn replace_source_roster(&mut self, authenticated: crate::realm_completion::PeerGeneration,
        roster: &crate::acquisition_prefix::AcquisitionSourceRoster) -> Result<(), crate::canonical_action::ApplyFailure> {
        roster.validate(authenticated)?;
        let mut replacement = self.clone();
        replacement.remove_source_subscriptions(authenticated.server.0);
        for acquisition in &roster.active {
            let crate::acquisition_prefix::ChunkAcquisitionControl::Acquire { request, .. } = acquisition.control else {
                return Err(crate::canonical_action::ApplyFailure::InvalidAction);
            };
            if !replacement.subscribe(ChunkSubscription { chunk: request.chunk, subscriber: request.subscriber,
                incarnation: request.incarnation, subscription_id: request.subscription_id, from_tick: request.from_tick }) {
                return Err(crate::canonical_action::ApplyFailure::Capacity);
            }
        }
        *self = replacement;
        Ok(())
    }

    fn can_add_subscription(&self, peer: u16) -> bool {
        self.subscription_slot_count < MAX_CHUNK_SUBSCRIPTION_SLOTS
            && self.subscriptions_per_peer.get(&peer).copied().unwrap_or(0) < MAX_CHUNK_SUBSCRIPTIONS_PER_PEER
    }

    #[must_use]
    pub fn subscription_retired_or_superseded(&self, request: ChunkSubscriptionRequest) -> bool {
        self.subscriptions.get(&request.chunk).and_then(|slots| slots.get(&request.subscriber))
            .is_some_and(|slot| slot.incarnation == request.incarnation && (slot.subscription_id > request.subscription_id
                || slot.subscription_id == request.subscription_id && !slot.active))
    }

    pub fn matches_subscription(&self, request: ChunkSubscriptionRequest) -> bool {
        self.subscriptions.get(&request.chunk).and_then(|slots| slots.get(&request.subscriber))
            .is_some_and(|slot| slot.active && slot.incarnation == request.incarnation
                && slot.subscription_id == request.subscription_id && slot.from_tick == request.from_tick)
    }

    pub fn active_subscriptions(&self) -> Vec<ChunkSubscription> {
        self.subscriptions.iter().flat_map(|(chunk, slots)| slots.iter().filter_map(|(peer, slot)| slot.active.then_some(ChunkSubscription {
            chunk: *chunk, subscriber: *peer, incarnation: slot.incarnation, subscription_id: slot.subscription_id, from_tick: slot.from_tick,
        }))).collect()
    }

    pub fn active_subscriptions_for(&self, chunks: impl IntoIterator<Item = ChunkKey>) -> Vec<ChunkSubscription> {
        chunks.into_iter().flat_map(|chunk| self.subscriptions.get(&chunk).into_iter().flat_map(move |slots| {
            slots.iter().filter_map(move |(peer, slot)| slot.active.then_some(ChunkSubscription {
                chunk, subscriber: *peer, incarnation: slot.incarnation, subscription_id: slot.subscription_id, from_tick: slot.from_tick,
            }))
        })).collect()
    }

    pub fn subscriptions_at(&self, chunk: ChunkKey, tick: TickStamp) -> Vec<ChunkSubscription> {
        self.subscriptions.get(&chunk).into_iter().flat_map(|slots| slots.iter()).filter_map(|(peer, slot)| {
            (slot.active && tick.distance_since(slot.from_tick) < 32768).then_some(ChunkSubscription {
                chunk, subscriber: *peer, incarnation: slot.incarnation, subscription_id: slot.subscription_id, from_tick: slot.from_tick,
            })
        }).collect()
    }

    pub fn held_chunk_count(&self, holder: u16) -> usize {
        self.holder_chunks.get(&holder).map_or(0, crate::structural_map::StructuralMap::len)
    }

    pub fn drop_holder(&mut self, holder: u16) -> Vec<ChunkKey> {
        let dropped = self.holder_chunks.remove(&holder).map(|chunks| chunks.keys().copied().collect::<Vec<_>>()).unwrap_or_default();
        for chunk in &dropped {
            if let Some(holders) = self.holders.get_mut(chunk) {
                holders.remove(&holder);
                if holders.is_empty() { self.holders.remove(chunk); }
            }
        }
        self.remove_source_subscriptions(holder);
        dropped
    }

    #[must_use]
    pub fn holders_of(&self, chunk: &ChunkKey) -> Option<&HashSet<u16>> { self.holders.get(chunk) }

    #[must_use]
    pub fn fetch_for(&self, chunk: &ChunkKey) -> Option<ChunkFetch> {
        Some(ChunkFetch { chunk: *chunk, from: select_holder(&self.sorted_holders(chunk))? })
    }

    #[must_use]
    pub fn sorted_holders(&self, chunk: &ChunkKey) -> Vec<u16> {
        let mut holders = self.holders.get(chunk).map(|holders| holders.iter().copied().collect::<Vec<_>>()).unwrap_or_default();
        holders.sort_unstable();
        holders
    }

    #[must_use]
    pub fn len(&self) -> usize { self.holders.len() }
    #[must_use]
    pub fn is_empty(&self) -> bool { self.holders.is_empty() }
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
    expiry_order: BTreeSet<(u64, ChunkKey)>,
}

impl PrimaryClaims {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn note_requested(&mut self, chunk: ChunkKey, now_millis: u64) {
        if let Some(previous) = self.last_requested.insert(chunk, now_millis) {
            self.expiry_order.remove(&(previous, chunk));
        }
        self.expiry_order.insert((now_millis, chunk));
    }

    #[must_use]
    pub fn is_retained(&self, chunk: &ChunkKey, now_millis: u64) -> bool {
        self.last_requested
            .get(chunk)
            .is_some_and(|at| now_millis.saturating_sub(*at) < PRIMARY_CLAIM_RETENTION_MILLIS)
    }

    pub fn take_expired(&mut self, now_millis: u64) -> Vec<ChunkKey> {
        self.take_expired_limit(now_millis, usize::MAX)
    }

    pub fn take_expired_limit(&mut self, now_millis: u64, limit: usize) -> Vec<ChunkKey> {
        let mut expired = Vec::new();
        while expired.len() < limit {
            let Some((at, chunk)) = self.expiry_order.first().copied() else {
                break;
            };
            if now_millis.saturating_sub(at) < PRIMARY_CLAIM_RETENTION_MILLIS {
                break;
            }
            self.expiry_order.remove(&(at, chunk));
            self.last_requested.remove(&chunk);
            expired.push(chunk);
        }
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
    fn published_directory_keeps_exact_old_holders_and_subscriber_cut() {
        let key = chunk(2, 3);
        let mut directory = Directory::new();
        directory.apply_advert(ChunkAdvert { holder: 7, chunk: key });
        let request = ChunkSubscription { chunk: key, subscriber: 9, incarnation: 71, subscription_id: 5, from_tick: TickStamp(12) };
        assert!(directory.subscribe(request));
        let original = directory.clone();
        directory.apply_drop(ChunkDrop { holder: 7, chunk: key });
        assert!(directory.unsubscribe(ChunkUnsubscription { chunk: key, subscriber: 9, incarnation: 71, subscription_id: 5 }));
        assert!(directory.sorted_holders(&key).is_empty());
        assert!(directory.subscriptions_at(key, TickStamp(12)).is_empty());
        assert_eq!(original.sorted_holders(&key), vec![7]);
        assert_eq!(original.subscriptions_at(key, TickStamp(12)), vec![request]);
        assert_eq!(original.held_chunk_count(7), 1);
        assert_eq!(directory.held_chunk_count(7), 0);
    }

    #[test]
    #[ignore]
    fn thousand_views_with_hundred_thousand_network_chunks_metadata_cost() {
        use crate::acquisition_prefix::{AcquisitionSourceBank, ChunkAcquisitionControl};
        use crate::identity::ServerId;
        use crate::realm_completion::PeerGeneration;
        let mut directory = Directory::new();
        for index in 0..100_000 { directory.apply_advert(ChunkAdvert { holder: 10 + (index % 32) as u16, chunk: chunk(index % 400, index / 400) }); }
        let baseline = directory.clone();
        let source = PeerGeneration { server: ServerId(1), generation: 77 };
        let mut bank = AcquisitionSourceBank::new(source).unwrap();
        let mut pings = PingTracker::new();
        for peer in 10..42 { pings.record(peer, (peer - 9) as u64); }
        let mut needs = HashMap::<ChunkKey, usize>::new();
        let mut views = Vec::new();
        let mut snapshots = Vec::new();
        let mut initial_bytes = 0_usize;
        let started = std::time::Instant::now();
        for actor in 0..1000 {
            let center = (actor % 40, actor / 40);
            let mut view = HashSet::new();
            for dx in -4..=4 { for dz in -4..=4 {
                let key = chunk(center.0 + dx, center.1 + dz);
                if !needs.contains_key(&key) {
                    let mut acquisition = ChunkAcquisition::new(key);
                    let selected = acquisition.request(&directory, &pings, Some(99), 0).unwrap();
                    let request = ChunkSubscriptionRequest { chunk: key, subscriber: 1, incarnation: 77, subscription_id: 1, from_tick: TickStamp(5) };
                    let control = bank.issue(PeerGeneration { server: ServerId(selected.from), generation: 78 }, request).unwrap();
                    initial_bytes += crate::acquisition_prefix::encode(&control).unwrap().len();
                    assert!(directory.subscribe(ChunkSubscription { chunk: key, subscriber: 1, incarnation: 77, subscription_id: 1, from_tick: TickStamp(5) }));
                    directory.apply_advert(ChunkAdvert { holder: 1, chunk: key });
                }
                *needs.entry(key).or_default() += 1;
                view.insert(key);
            } }
            snapshots.push(directory.clone());
            views.push(view);
        }
        let initial = started.elapsed();
        let relevant = needs.len();
        let baseline_bytes = crate::acquisition_prefix::encode_roster(&bank.close_roster(TickStamp(5)).unwrap()).unwrap().len();
        let started = std::time::Instant::now();
        let mut changes = 0_usize;
        let mut refresh_bytes = 0_usize;
        for (actor, old) in views.iter_mut().enumerate() {
            let center = ((actor % 40) as i32 + 1, (actor / 40) as i32);
            let new = (-4..=4).flat_map(|dx| (-4..=4).map(move |dz| chunk(center.0 + dx, center.1 + dz))).collect::<HashSet<_>>();
            for key in old.difference(&new) {
                let count = needs.get_mut(key).unwrap();
                *count -= 1;
                if *count == 0 {
                    needs.remove(key);
                    let unsubscribe = ChunkUnsubscription { chunk: *key, subscriber: 1, incarnation: 77, subscription_id: 1 };
                    directory.unsubscribe(unsubscribe);
                    directory.apply_drop(ChunkDrop { holder: 1, chunk: *key });
                    let control = bank.retire(unsubscribe, TickStamp(6)).unwrap();
                    refresh_bytes += crate::acquisition_prefix::encode(&control).unwrap().len();
                    changes += 1;
                }
            }
            for key in new.difference(old) {
                if !needs.contains_key(key) {
                    let mut acquisition = ChunkAcquisition::new(*key);
                    let selected = acquisition.request(&directory, &pings, Some(99), 50).unwrap();
                    let request = ChunkSubscriptionRequest { chunk: *key, subscriber: 1, incarnation: 77, subscription_id: 2, from_tick: TickStamp(6) };
                    let control = bank.issue(PeerGeneration { server: ServerId(selected.from), generation: 78 }, request).unwrap();
                    assert!(matches!(control.control, ChunkAcquisitionControl::Acquire { .. }));
                    refresh_bytes += crate::acquisition_prefix::encode(&control).unwrap().len();
                    assert!(directory.subscribe(ChunkSubscription { chunk: *key, subscriber: 1, incarnation: 77, subscription_id: 2, from_tick: TickStamp(6) }));
                    directory.apply_advert(ChunkAdvert { holder: 1, chunk: *key });
                    changes += 1;
                }
                *needs.entry(*key).or_default() += 1;
            }
            *old = new;
        }
        let refreshed = started.elapsed();
        assert_eq!(baseline.len(), 100_000);
        assert_eq!(needs.values().sum::<usize>(), 81_000);
        assert_eq!(directory.active_subscriptions_for(needs.keys().copied()).len(), needs.len());
        println!("network_chunks=100000 views=1000 relevant_chunks={relevant} retained_chunk_refs=81000 phase_snapshot_handle_bytes={} initial_ms={} refresh_ms={} initial_control_bytes={initial_bytes} initial_roster_bytes={baseline_bytes} refresh_controls={changes} refresh_control_bytes={refresh_bytes}", snapshots.len() * std::mem::size_of::<Directory>(), initial.as_millis(), refreshed.as_millis());
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
    fn bounded_claim_expiration_preserves_refresh_and_unprocessed_claims() {
        let mut claims = PrimaryClaims::new();
        for index in 0..5 {
            claims.note_requested(chunk(index, 0), 0);
        }
        claims.note_requested(chunk(0, 0), 29_000);
        assert!(claims.take_expired_limit(31_000, 0).is_empty());
        assert_eq!(claims.len(), 5);
        assert_eq!(
            claims.take_expired_limit(31_000, 2),
            vec![chunk(1, 0), chunk(2, 0)]
        );
        assert_eq!(claims.len(), 3);
        assert_eq!(
            claims.take_expired_limit(31_000, 2),
            vec![chunk(3, 0), chunk(4, 0)]
        );
        assert!(claims.is_retained(&chunk(0, 0), 31_000));
        assert_eq!(claims.take_expired_limit(59_000, 2), vec![chunk(0, 0)]);
        assert!(claims.is_empty());
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

    #[test]
    fn subscription_scope_is_separate_from_holders_and_resists_reordered_close() {
        let target = chunk(2, 5);
        let mut directory = Directory::new();
        directory.apply_advert(ChunkAdvert {
            holder: 1,
            chunk: target,
        });
        let first = ChunkSubscription {
            chunk: target,
            subscriber: 7,
            incarnation: 3,
            subscription_id: 9,
            from_tick: TickStamp(65_535),
        };
        assert!(directory.subscribe(first));
        assert_eq!(
            directory.subscriptions_at(target, TickStamp(65_534)),
            Vec::<ChunkSubscription>::new()
        );
        assert_eq!(
            directory.subscriptions_at(target, TickStamp(0)),
            vec![first]
        );
        assert_eq!(directory.sorted_holders(&target), vec![1]);
        assert!(directory.unsubscribe(ChunkUnsubscription {
            chunk: target,
            subscriber: 7,
            incarnation: 3,
            subscription_id: 10,
        }));
        assert!(!directory.subscribe(first));
        assert!(directory.subscriptions_at(target, TickStamp(0)).is_empty());
        assert!(directory.subscribe(ChunkSubscription {
            incarnation: 4,
            subscription_id: 1,
            ..first
        }));
        assert_eq!(
            directory.subscriptions_at(target, TickStamp(0)),
            vec![ChunkSubscription {
                incarnation: 4,
                subscription_id: 1,
                ..first
            }]
        );
        directory.apply_drop(ChunkDrop {
            holder: 7,
            chunk: target,
        });
        assert!(directory.subscriptions_at(target, TickStamp(0)).is_empty());
        directory.drop_holder(7);
        assert!(directory.subscribe(ChunkSubscription {
            incarnation: 2,
            subscription_id: 1,
            ..first
        }));
        assert_eq!(
            directory.subscriptions_at(target, TickStamp(0)),
            vec![ChunkSubscription {
                incarnation: 2,
                subscription_id: 1,
                ..first
            }]
        );
    }

    #[test]
    fn subscription_slots_are_bounded_and_released_on_peer_drop() {
        let mut directory = Directory::new();
        for x in 0..MAX_CHUNK_SUBSCRIPTIONS_PER_PEER {
            assert!(directory.subscribe(ChunkSubscription {
                chunk: chunk(x as i32, 0),
                subscriber: 7,
                incarnation: 3,
                subscription_id: 1,
                from_tick: TickStamp(2),
            }));
        }
        let extra = ChunkSubscription {
            chunk: chunk(MAX_CHUNK_SUBSCRIPTIONS_PER_PEER as i32, 0),
            subscriber: 7,
            incarnation: 3,
            subscription_id: 1,
            from_tick: TickStamp(2),
        };
        assert!(!directory.subscribe(extra));
        assert!(!directory.unsubscribe(ChunkUnsubscription {
            chunk: extra.chunk,
            subscriber: extra.subscriber,
            incarnation: extra.incarnation,
            subscription_id: extra.subscription_id,
        }));
        directory.drop_holder(7);
        assert!(directory.subscribe(extra));
    }
}
