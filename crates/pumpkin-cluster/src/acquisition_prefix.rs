use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::canonical_action::ApplyFailure;
use crate::chunks::{ChunkSubscriptionRequest, ChunkUnsubscription};
use crate::realm_completion::PeerGeneration;
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalChunkRequest {
    pub provider: PeerGeneration,
    pub chunk: ChunkKey,
    pub lease: u64,
    pub full_source_slot: u64,
}

impl HistoricalChunkRequest {
    pub fn source_tick(self) -> TickStamp { TickStamp(self.full_source_slot as u16) }
    pub fn cut(self) -> TickStamp { TickStamp(self.source_tick().0.wrapping_sub(1)) }
    pub fn valid(self) -> bool { self.provider.generation != 0 && self.lease != 0 && self.full_source_slot != 0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkAcquisitionControl {
    Acquire { provider: PeerGeneration, request: ChunkSubscriptionRequest },
    Retire { request: ChunkUnsubscription, tick: TickStamp },
    HistoricalCut(HistoricalChunkRequest),
    ReleaseHistorical { chunk: ChunkKey, lease: u64, full_source_slot: u64, tick: TickStamp },
}

impl ChunkAcquisitionControl {
    pub fn from_tick(self) -> TickStamp {
        match self {
            Self::Acquire { request, .. } => request.from_tick,
            Self::Retire { tick, .. } | Self::ReleaseHistorical { tick, .. } => tick,
            Self::HistoricalCut(request) => request.source_tick(),
        }
    }

    fn valid_for(self, source: PeerGeneration) -> bool {
        match self {
            Self::Acquire { provider, request } => provider.generation != 0
                && request.subscriber == source.server.0
                && request.incarnation == source.generation
                && request.subscription_id != 0,
            Self::Retire { request, .. } => request.subscriber == source.server.0
                && request.incarnation == source.generation
                && request.subscription_id != 0,
            Self::HistoricalCut(request) => request.valid(),
            Self::ReleaseHistorical { lease, full_source_slot, .. } => lease != 0 && full_source_slot != 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumberedChunkAcquisition {
    pub source: PeerGeneration,
    pub sequence: u64,
    pub control: ChunkAcquisitionControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquisitionPrefix {
    pub source: PeerGeneration,
    pub tick: TickStamp,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquisitionSourceRoster {
    pub closed: AcquisitionPrefix,
    pub active: Vec<NumberedChunkAcquisition>,
    pub historical: Vec<NumberedChunkAcquisition>,
}

impl AcquisitionSourceRoster {
    pub fn validate(&self, authenticated: PeerGeneration) -> Result<(), ApplyFailure> {
        if self.closed.source != authenticated || authenticated.generation == 0 || self.historical.len() > 8192 {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut chunks = BTreeSet::new();
        let mut sequences = BTreeSet::new();
        for acquisition in &self.active {
            let ChunkAcquisitionControl::Acquire { request, .. } = acquisition.control else {
                return Err(ApplyFailure::InvalidAction);
            };
            if acquisition.source != authenticated || acquisition.sequence == 0
                || acquisition.sequence > self.closed.sequence
                || !acquisition.control.valid_for(authenticated)
                || !chunks.insert(request.chunk) || !sequences.insert(acquisition.sequence)
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut leases = BTreeSet::new();
        for acquisition in &self.historical {
            let ChunkAcquisitionControl::HistoricalCut(request) = acquisition.control else {
                return Err(ApplyFailure::InvalidAction);
            };
            if acquisition.source != authenticated || acquisition.sequence == 0
                || acquisition.sequence > self.closed.sequence || !request.valid()
                || !leases.insert((request.chunk, request.lease)) || !sequences.insert(acquisition.sequence)
            { return Err(ApplyFailure::InvalidAction); }
        }
        Ok(())
    }
}

pub fn encode(control: &NumberedChunkAcquisition) -> Result<Vec<u8>, ApplyFailure> {
    let payload = postcard::to_allocvec(control).map_err(|_| ApplyFailure::InvalidAction)?;
    if payload.len() > 256 {
        return Err(ApplyFailure::Capacity);
    }
    let mut bytes = Vec::with_capacity(4 + payload.len());
    bytes.extend_from_slice(b"CAQ2");
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode(bytes: &[u8]) -> Result<NumberedChunkAcquisition, ApplyFailure> {
    if bytes.len() > 260 || !bytes.starts_with(b"CAQ2") {
        return Err(ApplyFailure::InvalidAction);
    }
    let (control, remainder) = postcard::take_from_bytes(&bytes[4..]).map_err(|_| ApplyFailure::InvalidAction)?;
    if !remainder.is_empty() {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(control)
}

pub fn encode_roster(roster: &AcquisitionSourceRoster) -> Result<Vec<u8>, ApplyFailure> {
    roster.validate(roster.closed.source)?;
    let payload = postcard::to_allocvec(roster).map_err(|_| ApplyFailure::InvalidAction)?;
    if payload.len() > 64 * 1024 * 1024 {
        return Err(ApplyFailure::Capacity);
    }
    let mut bytes = Vec::with_capacity(4 + payload.len());
    bytes.extend_from_slice(b"CAR2");
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_roster(bytes: &[u8]) -> Result<AcquisitionSourceRoster, ApplyFailure> {
    if bytes.len() > 64 * 1024 * 1024 + 4 || !bytes.starts_with(b"CAR2") {
        return Err(ApplyFailure::InvalidAction);
    }
    let (roster, remainder): (AcquisitionSourceRoster, _) = postcard::take_from_bytes(&bytes[4..])
        .map_err(|_| ApplyFailure::InvalidAction)?;
    if !remainder.is_empty() {
        return Err(ApplyFailure::InvalidAction);
    }
    roster.validate(roster.closed.source)?;
    Ok(roster)
}

#[derive(Clone)]
pub struct AcquisitionSourceBank {
    source: PeerGeneration,
    next_sequence: u64,
    closed_through: Option<AcquisitionPrefix>,
    active: crate::structural_map::StructuralMap<ChunkKey, NumberedChunkAcquisition>,
    historical: crate::structural_map::StructuralMap<(ChunkKey, u64), NumberedChunkAcquisition>,
    closed_active: crate::structural_map::StructuralMap<ChunkKey,NumberedChunkAcquisition>,
    closed_historical: crate::structural_map::StructuralMap<(ChunkKey,u64),NumberedChunkAcquisition>,
}

impl AcquisitionSourceBank {
    pub fn new(source: PeerGeneration) -> Result<Self, ApplyFailure> {
        if source.generation == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(Self { source, next_sequence: 1, closed_through: None, active: Default::default(), historical: Default::default(), closed_active: Default::default(), closed_historical: Default::default() })
    }

    pub fn issue(
        &mut self,
        provider: PeerGeneration,
        request: ChunkSubscriptionRequest,
    ) -> Result<NumberedChunkAcquisition, ApplyFailure> {
        self.issue_control(ChunkAcquisitionControl::Acquire { provider, request })
    }

    pub fn retire(
        &mut self,
        request: ChunkUnsubscription,
        tick: TickStamp,
    ) -> Result<NumberedChunkAcquisition, ApplyFailure> {
        self.issue_control(ChunkAcquisitionControl::Retire { request, tick })
    }

    pub fn issue_control(
        &mut self,
        control: ChunkAcquisitionControl,
    ) -> Result<NumberedChunkAcquisition, ApplyFailure> {
        if !control.valid_for(self.source)
            || self.closed_through.is_some_and(|closed| {
                let distance = control.from_tick().distance_since(closed.tick);
                distance == 0 || distance >= 32768
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if let ChunkAcquisitionControl::Acquire { request, .. } = control {
            if self.active.get(&request.chunk).is_some_and(|previous| {
                matches!(previous.control, ChunkAcquisitionControl::Acquire { request: old, .. }
                    if request.subscription_id <= old.subscription_id)
            }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if let ChunkAcquisitionControl::HistoricalCut(request) = control {
            if self.historical.get(&(request.chunk, request.lease)).is_some_and(|previous| {
                !matches!(previous.control, ChunkAcquisitionControl::HistoricalCut(old)
                    if old.full_source_slot == request.full_source_slot && old.provider != request.provider)
            }) { return Err(ApplyFailure::InvalidAction); }
            if !self.historical.contains_key(&(request.chunk, request.lease)) && self.historical.len() >= 8192 { return Err(ApplyFailure::Capacity); }
        }
        if let ChunkAcquisitionControl::ReleaseHistorical { chunk, lease, full_source_slot, .. } = control {
            if self.historical.get(&(chunk, lease)).is_some_and(|previous| {
                !matches!(previous.control, ChunkAcquisitionControl::HistoricalCut(request) if request.full_source_slot == full_source_slot)
            }) { return Err(ApplyFailure::InvalidAction); }
        }
        let next = self.next_sequence.checked_add(1).ok_or(ApplyFailure::Capacity)?;
        let numbered = NumberedChunkAcquisition {
            source: self.source,
            sequence: self.next_sequence,
            control,
        };
        match control {
            ChunkAcquisitionControl::Acquire { request, .. } => {
                self.active.insert(request.chunk, numbered);
            }
            ChunkAcquisitionControl::Retire { request, .. } => {
                if self.active.get(&request.chunk).is_some_and(|previous| {
                    matches!(previous.control, ChunkAcquisitionControl::Acquire { request: old, .. }
                        if request.subscription_id >= old.subscription_id)
                }) {
                    self.active.remove(&request.chunk);
                }
            }
            ChunkAcquisitionControl::HistoricalCut(request) => {
                self.historical.insert((request.chunk, request.lease), numbered);
            }
            ChunkAcquisitionControl::ReleaseHistorical { chunk, lease, .. } => {
                self.historical.remove(&(chunk, lease));
            }
        }
        self.next_sequence = next;
        Ok(numbered)
    }

    pub fn close(&mut self, tick: TickStamp) -> Result<AcquisitionPrefix, ApplyFailure> {
        if let Some(previous) = self.closed_through {
            let distance = tick.distance_since(previous.tick);
            if distance == 0 { return Ok(previous); }
            if distance >= 32768 { return Err(ApplyFailure::InvalidAction); }
        }
        let prefix = AcquisitionPrefix { source: self.source, tick, sequence: self.next_sequence - 1 };
        self.closed_active = self.active.clone();
        self.closed_historical = self.historical.clone();
        self.closed_through = Some(prefix);
        Ok(prefix)
    }

    pub fn close_roster(&mut self, tick: TickStamp) -> Result<AcquisitionSourceRoster, ApplyFailure> {
        let closed = self.close(tick)?;
        Ok(AcquisitionSourceRoster {closed,active:self.closed_active.values().copied().collect(),historical:self.closed_historical.values().copied().collect()})
    }
}

#[derive(Clone, Default)]
struct SourceReceipt {
    processed_through: u64,
    pending: crate::structural_map::StructuralMap<u64, NumberedChunkAcquisition>,
    processed_after_prefix: crate::structural_map::StructuralMap<u64, TickStamp>,
    announced: Option<AcquisitionPrefix>,
    historical: crate::structural_map::StructuralMap<(ChunkKey, u64), HistoricalChunkRequest>,
    baseline: Option<(AcquisitionPrefix, [u8; 32])>,
}

#[derive(Clone, Default)]
pub struct AcquisitionReceiptLedger {
    sources: crate::structural_map::StructuralMap<PeerGeneration, SourceReceipt>,
    ready_sources: crate::structural_map::StructuralMap<PeerGeneration,()>,
    pending_count: usize,
    historical_count: usize,
}

impl AcquisitionReceiptLedger {
    pub fn admit_source(&mut self, source: PeerGeneration) -> Result<(), ApplyFailure> {
        if source.generation == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        if !self.sources.contains_key(&source) && self.sources.len() >= 8192 {
            return Err(ApplyFailure::Capacity);
        }
        self.sources.entry(source).or_default();
        Ok(())
    }

    pub fn seed_processed_roster(
        &mut self,
        authenticated: PeerGeneration,
        roster: &AcquisitionSourceRoster,
    ) -> Result<(), ApplyFailure> {
        use sha2::Digest;
        roster.validate(authenticated)?;
        let bytes = postcard::to_allocvec(roster).map_err(|_| ApplyFailure::InvalidAction)?;
        let hash: [u8; 32] = sha2::Sha256::digest(&bytes).into();
        let receipt = self.sources.get_mut(&authenticated).ok_or(ApplyFailure::InvalidAction)?;
        if receipt.processed_through > roster.closed.sequence
            || receipt.announced.is_some_and(|previous| {
                roster.closed.tick.distance_since(previous.tick) >= 32768
                    || roster.closed.sequence < previous.sequence
                    || roster.closed.tick == previous.tick && roster.closed != previous
            })
            || receipt.baseline.is_some_and(|(prefix, previous_hash)| prefix == roster.closed && previous_hash != hash)
            || receipt.pending.range((std::ops::Bound::Excluded(roster.closed.sequence), std::ops::Bound::Unbounded))
                .any(|(_, control)| roster.closed.tick.distance_since(control.control.from_tick()) < 32768)
        { return Err(ApplyFailure::InvalidAction); }
        let historical = roster.historical.iter().filter_map(|entry| {
            if let ChunkAcquisitionControl::HistoricalCut(request) = entry.control { Some(((request.chunk, request.lease), request)) } else { None }
        }).collect::<crate::structural_map::StructuralMap<_, _>>();
        let historical_count = self.historical_count - receipt.historical.len() + historical.len();
        if historical_count > 8192 { return Err(ApplyFailure::Capacity); }
        let previous = receipt.pending.len();
        receipt.pending.retain(|sequence, _| *sequence > roster.closed.sequence);
        self.pending_count -= previous - receipt.pending.len();
        let retired = receipt.processed_after_prefix.len();
        receipt.processed_after_prefix.retain(|sequence, _| *sequence > roster.closed.sequence);
        self.pending_count -= retired - receipt.processed_after_prefix.len();
        receipt.historical = historical;
        self.historical_count = historical_count;
        receipt.processed_through = roster.closed.sequence;
        receipt.announced = Some(roster.closed);
        receipt.baseline = Some((roster.closed, hash));
        if receipt.processed_through.checked_add(1).is_some_and(|next| receipt.pending.contains_key(&next)) {
            self.ready_sources.insert(authenticated,());
        } else { self.ready_sources.remove(&authenticated); }
        Ok(())
    }

    pub fn receive(
        &mut self,
        authenticated: PeerGeneration,
        control: NumberedChunkAcquisition,
    ) -> Result<(), ApplyFailure> {
        if control.source != authenticated
            || control.sequence == 0
            || !control.control.valid_for(authenticated)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let receipt = self.sources.get_mut(&authenticated).ok_or(ApplyFailure::InvalidAction)?;
        if receipt.announced.is_some_and(|prefix| {
            control.sequence > prefix.sequence
                && prefix.tick.distance_since(control.control.from_tick()) < 32768
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        if control.sequence <= receipt.processed_through {
            return Ok(());
        }
        if let Some(previous) = receipt.pending.get(&control.sequence) {
            return if *previous == control { Ok(()) } else { Err(ApplyFailure::InvalidAction) };
        }
        if self.pending_count >= 8192 {
            return Err(ApplyFailure::Capacity);
        }
        receipt.pending.insert(control.sequence, control);
        if receipt.processed_through.checked_add(1).is_some_and(|next| receipt.pending.contains_key(&next)) { self.ready_sources.insert(authenticated,()); }
        self.pending_count += 1;
        Ok(())
    }

    pub fn active_historical(&self) -> Vec<(PeerGeneration, HistoricalChunkRequest)> {
        self.sources.iter().flat_map(|(source, receipt)| receipt.historical.values().map(|request| (*source, *request))).collect()
    }

    pub fn pending_sources(&self) -> Vec<PeerGeneration> {
        self.ready_sources.keys().copied().collect()
    }

    pub fn next_pending(&self, source: PeerGeneration) -> Option<&NumberedChunkAcquisition> {
        let receipt = self.sources.get(&source)?;
        receipt.pending.get(&receipt.processed_through.checked_add(1)?)
    }

    pub fn mark_processed(&mut self, source: PeerGeneration, sequence: u64) -> Result<(), ApplyFailure> {
        let receipt = self.sources.get_mut(&source).ok_or(ApplyFailure::InvalidAction)?;
        if receipt.processed_through.checked_add(1) != Some(sequence)
            || !receipt.pending.contains_key(&sequence)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let pending = receipt.pending.get(&sequence).expect("contiguous acquisition");
        if let ChunkAcquisitionControl::HistoricalCut(request) = pending.control {
            if receipt.historical.get(&(request.chunk, request.lease)).is_some_and(|previous| {
                previous.full_source_slot != request.full_source_slot || previous.provider == request.provider
            }) { return Err(ApplyFailure::InvalidAction); }
            if !receipt.historical.contains_key(&(request.chunk, request.lease)) && self.historical_count >= 8192 { return Err(ApplyFailure::Capacity); }
        }
        if let ChunkAcquisitionControl::ReleaseHistorical { chunk, lease, full_source_slot, .. } = pending.control {
            if receipt.historical.get(&(chunk, lease)).is_some_and(|request| request.full_source_slot != full_source_slot) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let control = receipt.pending.remove(&sequence).expect("contiguous acquisition");
        match control.control {
            ChunkAcquisitionControl::HistoricalCut(request) => {
                if receipt.historical.insert((request.chunk, request.lease), request).is_none() { self.historical_count += 1; }
            }
            ChunkAcquisitionControl::ReleaseHistorical { chunk, lease, .. } => {
                if receipt.historical.remove(&(chunk, lease)).is_some() { self.historical_count -= 1; }
            }
            _ => {}
        }
        receipt.processed_through = sequence;
        if receipt.announced.is_some_and(|prefix| sequence <= prefix.sequence) {
            self.pending_count -= 1;
        } else {
            receipt.processed_after_prefix.insert(sequence, control.control.from_tick());
        }
        if sequence.checked_add(1).is_some_and(|next| receipt.pending.contains_key(&next)) { self.ready_sources.insert(source,()); }
        else { self.ready_sources.remove(&source); }
        Ok(())
    }

    pub fn announce(
        &mut self,
        authenticated: PeerGeneration,
        prefix: AcquisitionPrefix,
    ) -> Result<(), ApplyFailure> {
        if authenticated != prefix.source {
            return Err(ApplyFailure::InvalidAction);
        }
        let receipt = self.sources.get_mut(&authenticated).ok_or(ApplyFailure::InvalidAction)?;
        if let Some(previous) = receipt.announced {
            let distance = prefix.tick.distance_since(previous.tick);
            if distance == 0 {
                return if previous == prefix { Ok(()) } else { Err(ApplyFailure::InvalidAction) };
            }
            if distance >= 32768 {
                return if prefix.sequence <= previous.sequence { Ok(()) } else { Err(ApplyFailure::InvalidAction) };
            }
            if prefix.sequence < previous.sequence {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if receipt.pending.range((std::ops::Bound::Excluded(prefix.sequence), std::ops::Bound::Unbounded))
            .any(|(_, control)| prefix.tick.distance_since(control.control.from_tick()) < 32768)
            || receipt.processed_after_prefix.range((std::ops::Bound::Excluded(prefix.sequence), std::ops::Bound::Unbounded))
                .any(|(_, from_tick)| prefix.tick.distance_since(*from_tick) < 32768)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let retired = receipt.processed_after_prefix.keys().copied().take_while(|sequence| *sequence <= prefix.sequence).collect::<Vec<_>>();
        for sequence in retired {
            receipt.processed_after_prefix.remove(&sequence);
            self.pending_count -= 1;
        }
        receipt.announced = Some(prefix);
        Ok(())
    }

    pub fn cut_closed(&self, tick: TickStamp, frozen_sources: &BTreeSet<PeerGeneration>) -> bool {
        !frozen_sources.is_empty() && frozen_sources.iter().all(|source| {
            self.sources.get(source).is_some_and(|receipt| {
                receipt.announced.is_some_and(|prefix| {
                    prefix.tick.distance_since(tick) < 32768
                        && receipt.processed_through >= prefix.sequence
                })
            })
        })
    }

    pub fn retire_source(&mut self, source: PeerGeneration) {
        self.ready_sources.remove(&source);
        if let Some(receipt) = self.sources.remove(&source) {
            self.pending_count -= receipt.pending.len() + receipt.processed_after_prefix.len();
            self.historical_count -= receipt.historical.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ServerId;
    use crate::protocol::{ChunkKey, WorldId};

    fn source() -> PeerGeneration {
        PeerGeneration { server: ServerId(1), generation: 71 }
    }

    fn request(from_tick: u16, subscription_id: u64) -> ChunkSubscriptionRequest {
        ChunkSubscriptionRequest {
            chunk: ChunkKey::new(WorldId::new(0, 0), 0, 0),
            subscriber: 1,
            incarnation: 71,
            subscription_id,
            from_tick: TickStamp(from_tick),
        }
    }

    #[test]
    fn finish_before_controls_does_not_certify_a_missing_prefix() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let first = bank.issue(source(), request(8, 1)).unwrap();
        let second = bank.issue(source(), request(9, 2)).unwrap();
        let prefix = bank.close(TickStamp(9)).unwrap();
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.announce(source(), prefix).unwrap();
        receipts.receive(source(), second).unwrap();
        let frozen = BTreeSet::from([source()]);
        assert!(!receipts.cut_closed(TickStamp(9), &frozen));
        assert!(receipts.next_pending(source()).is_none());
        assert_eq!(receipts.mark_processed(source(), 2), Err(ApplyFailure::InvalidAction));
        receipts.receive(source(), first).unwrap();
        receipts.mark_processed(source(), 1).unwrap();
        assert!(!receipts.cut_closed(TickStamp(9), &frozen));
        receipts.mark_processed(source(), 2).unwrap();
        assert!(receipts.cut_closed(TickStamp(9), &frozen));
    }

    #[test]
    fn source_cannot_backdate_after_closure_or_change_a_closed_prefix() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let prefix = bank.close(TickStamp(65535)).unwrap();
        assert_eq!(bank.issue(source(), request(65535, 1)), Err(ApplyFailure::InvalidAction));
        assert_eq!(bank.issue(source(), request(65534, 1)), Err(ApplyFailure::InvalidAction));
        bank.issue(source(), request(0, 2)).unwrap();
        assert_eq!(bank.close(TickStamp(65535)).unwrap(), prefix);
        assert_eq!(bank.close(TickStamp(0)).unwrap().sequence, 1);
    }

    #[test]
    fn delivered_future_control_may_precede_an_older_finish() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let prefix = bank.close(TickStamp(8)).unwrap();
        let future = bank.issue(source(), request(9, 1)).unwrap();
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.receive(source(), future).unwrap();
        receipts.mark_processed(source(), 1).unwrap();
        receipts.announce(source(), prefix).unwrap();
        assert!(receipts.cut_closed(TickStamp(8), &BTreeSet::from([source()])));
    }

    #[test]
    fn receiver_rejects_a_false_closed_prefix_after_processing_backdated_control() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let old = bank.issue(source(), request(8, 1)).unwrap();
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.receive(source(), old).unwrap();
        receipts.mark_processed(source(), 1).unwrap();
        assert_eq!(receipts.announce(source(), AcquisitionPrefix {
            source: source(), tick: TickStamp(8), sequence: 0,
        }), Err(ApplyFailure::InvalidAction));
    }

    #[test]
    fn a_replacement_generation_cannot_close_the_previous_generation() {
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.announce(source(), AcquisitionPrefix { source: source(), tick: TickStamp(8), sequence: 0 }).unwrap();
        let replacement = PeerGeneration { generation: 72, ..source() };
        receipts.admit_source(replacement).unwrap();
        assert!(!receipts.cut_closed(TickStamp(8), &BTreeSet::from([replacement])));
        assert!(receipts.cut_closed(TickStamp(8), &BTreeSet::from([source()])));
    }
    #[test]
    fn acquisition_and_retirement_share_one_contiguous_prefix() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let first = bank.issue(source(), request(8, 1)).unwrap();
        let retire = bank.retire(ChunkUnsubscription {
            chunk: request(8, 1).chunk, subscriber: 1, incarnation: 71, subscription_id: 1,
        }, TickStamp(9)).unwrap();
        let prefix = bank.close(TickStamp(9)).unwrap();
        assert_eq!(retire.sequence, first.sequence + 1);
        assert_eq!(decode(&encode(&retire).unwrap()).unwrap(), retire);
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.announce(source(), prefix).unwrap();
        receipts.receive(source(), retire).unwrap();
        assert!(receipts.next_pending(source()).is_none());
        receipts.receive(source(), first).unwrap();
        receipts.mark_processed(source(), first.sequence).unwrap();
        assert!(!receipts.cut_closed(TickStamp(9), &BTreeSet::from([source()])));
        receipts.mark_processed(source(), retire.sequence).unwrap();
        assert!(receipts.cut_closed(TickStamp(9), &BTreeSet::from([source()])));
        assert_eq!(bank.retire(ChunkUnsubscription {
            chunk: request(8, 1).chunk, subscriber: 1, incarnation: 71, subscription_id: 1,
        }, TickStamp(9)), Err(ApplyFailure::InvalidAction));
    }

    #[test]
    fn closed_source_roster_preserves_more_than_eight_thousand_owned_chunks() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        for x in 0..9000 {
            let mut need = request(8, 1);
            need.chunk.position.x = x;
            bank.issue(source(), need).unwrap();
        }
        let frozen = bank.clone();
        let closed = bank.close_roster(TickStamp(8)).unwrap();
        assert_eq!(closed.active.len(), 9000);
        assert_eq!(decode_roster(&encode_roster(&closed).unwrap()).unwrap(), closed);
        assert_eq!(frozen.active.len(), 9000);
        bank.retire(ChunkUnsubscription {chunk: request(8, 1).chunk, subscriber: 1,
            incarnation: 71, subscription_id: 1}, TickStamp(9)).unwrap();
        assert_eq!(bank.close_roster(TickStamp(8)).unwrap(), closed);
        assert_eq!(bank.close_roster(TickStamp(9)).unwrap().active.len(), 8999);
        assert_eq!(frozen.active.len(), 9000);
    }

    #[test]
    fn whole_source_roster_is_frozen_at_the_closed_prefix() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let first = bank.issue(source(), request(8, 1)).unwrap();
        let closed = bank.close_roster(TickStamp(8)).unwrap();
        assert_eq!(closed.active, vec![first]);
        assert_eq!(decode_roster(&encode_roster(&closed).unwrap()).unwrap(), closed);
        bank.retire(ChunkUnsubscription {
            chunk: request(8, 1).chunk, subscriber: 1, incarnation: 71, subscription_id: 1,
        }, TickStamp(9)).unwrap();
        assert_eq!(bank.close_roster(TickStamp(8)).unwrap(), closed);
        let retired = bank.close_roster(TickStamp(9)).unwrap();
        assert!(retired.active.is_empty());
        assert_eq!(retired.closed.sequence, 2);
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.seed_processed_roster(source(), &retired).unwrap();
        assert!(receipts.cut_closed(TickStamp(9), &BTreeSet::from([source()])));
        assert_eq!(receipts.seed_processed_roster(source(), &closed), Err(ApplyFailure::InvalidAction));
        assert_eq!(retired.validate(PeerGeneration { generation: 72, ..source() }), Err(ApplyFailure::InvalidAction));
    }

    #[test]
    fn whole_source_roster_rejects_duplicate_slots_and_retire_entries() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let first = bank.issue(source(), request(8, 1)).unwrap();
        let mut roster = bank.close_roster(TickStamp(8)).unwrap();
        roster.active.push(first);
        assert_eq!(roster.validate(source()), Err(ApplyFailure::InvalidAction));
        roster.active = vec![bank.retire(ChunkUnsubscription {
            chunk: request(8, 1).chunk, subscriber: 1, incarnation: 71, subscription_id: 1,
        }, TickStamp(9)).unwrap()];
        roster.closed.sequence = 2;
        assert_eq!(roster.validate(source()), Err(ApplyFailure::InvalidAction));
    }

    #[test]
    fn historical_footprint_is_numbered_and_cannot_arrive_after_its_source_finish() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        bank.close(TickStamp(7)).unwrap();
        let request = HistoricalChunkRequest { provider: source(), chunk: request(8, 1).chunk, lease: 19, full_source_slot: 65544 };
        let control = bank.issue_control(ChunkAcquisitionControl::HistoricalCut(request)).unwrap();
        assert_eq!(control.control.from_tick(), TickStamp(8));
        let roster = bank.close_roster(TickStamp(8)).unwrap();
        assert_eq!(roster.historical, vec![control]);
        assert_eq!(decode_roster(&encode_roster(&roster).unwrap()).unwrap(), roster);
        assert_eq!(bank.issue_control(ChunkAcquisitionControl::HistoricalCut(HistoricalChunkRequest { lease: 20, ..request })), Err(ApplyFailure::InvalidAction));
        bank.issue_control(ChunkAcquisitionControl::ReleaseHistorical {
            chunk: request.chunk, lease: request.lease, full_source_slot: request.full_source_slot, tick: TickStamp(9),
        }).unwrap();
        assert!(bank.close_roster(TickStamp(9)).unwrap().historical.is_empty());
    }

    #[test]
    fn historical_provider_retarget_preserves_original_lease_and_cut() {
        let mut bank = AcquisitionSourceBank::new(source()).unwrap();
        let original = HistoricalChunkRequest { provider: source(), chunk: request(8, 1).chunk, lease: 19, full_source_slot: 65544 };
        let first = bank.issue_control(ChunkAcquisitionControl::HistoricalCut(original)).unwrap();
        let other = PeerGeneration { server: ServerId(2), generation: 72 };
        let next = HistoricalChunkRequest { provider: other, ..original };
        let second = bank.issue_control(ChunkAcquisitionControl::HistoricalCut(next)).unwrap();
        assert_eq!(second.sequence, first.sequence + 1);
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source()).unwrap();
        receipts.receive(source(), first).unwrap();
        receipts.mark_processed(source(), first.sequence).unwrap();
        receipts.receive(source(), second).unwrap();
        receipts.mark_processed(source(), second.sequence).unwrap();
        assert_eq!(receipts.active_historical(), vec![(source(), next)]);
        assert_eq!(bank.close_roster(TickStamp(8)).unwrap().historical, vec![second]);
        assert_eq!(bank.issue_control(ChunkAcquisitionControl::HistoricalCut(original)), Err(ApplyFailure::InvalidAction));
    }

}
