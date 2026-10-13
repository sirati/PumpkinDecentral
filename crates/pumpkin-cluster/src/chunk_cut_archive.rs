use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::acquisition_prefix::AcquisitionReceiptLedger;
use crate::canonical_action::ApplyFailure;
use crate::chunks::ChunkSubscription;
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Clone)]
struct RetainedCut<T> {
    value: Arc<T>,
    bytes: usize,
    sources: BTreeSet<PeerGeneration>,
}

#[derive(Clone)]
pub struct ChunkCutArchive<T> {
    cuts: BTreeMap<(ChunkKey, TickStamp), RetainedCut<T>>,
    pins: BTreeMap<(ChunkKey, PeerGeneration, u64), TickStamp>,
    historical_pins: BTreeMap<(ChunkKey, PeerGeneration, u64), TickStamp>,
    retained_bytes: usize,
    max_cuts: usize,
    max_bytes: usize,
}

impl<T> ChunkCutArchive<T> {
    pub fn new(max_cuts: usize, max_bytes: usize) -> Self {
        Self { cuts: BTreeMap::new(), pins: BTreeMap::new(), historical_pins: BTreeMap::new(), retained_bytes: 0, max_cuts, max_bytes }
    }

    pub fn retain(
        &mut self,
        chunk: ChunkKey,
        cut: TickStamp,
        value: Arc<T>,
        bytes: usize,
        sources: BTreeSet<PeerGeneration>,
    ) -> Result<(), ApplyFailure> {
        if sources.is_empty() || sources.iter().any(|source| source.generation == 0) {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(previous) = self.cuts.get(&(chunk, cut)) {
            return if Arc::ptr_eq(&previous.value, &value) && previous.sources == sources {
                Ok(())
            } else {
                Err(ApplyFailure::InvalidAction)
            };
        }
        let retained_bytes = self.retained_bytes.checked_add(bytes)
            .filter(|total| *total <= self.max_bytes).ok_or(ApplyFailure::Capacity)?;
        if self.cuts.len() >= self.max_cuts { return Err(ApplyFailure::Capacity); }
        self.cuts.insert((chunk, cut), RetainedCut { value, bytes, sources });
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    pub fn retained_values(&self) -> impl Iterator<Item = &Arc<T>> {
        self.cuts.values().map(|cut| &cut.value)
    }

    pub fn get(&self, chunk: ChunkKey, cut: TickStamp) -> Option<Arc<T>> {
        self.cuts.get(&(chunk, cut)).map(|retained| retained.value.clone())
    }

    pub fn pin(&mut self, subscription: ChunkSubscription) -> Result<(), ApplyFailure> {
        if subscription.incarnation == 0 || subscription.subscription_id == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let source = PeerGeneration { server: crate::identity::ServerId(subscription.subscriber), generation: subscription.incarnation };
        let key = (subscription.chunk, source, subscription.subscription_id);
        let cut = TickStamp(subscription.from_tick.0.wrapping_sub(1));
        if let Some(previous) = self.pins.get(&key) {
            return if *previous == cut { Ok(()) } else { Err(ApplyFailure::InvalidAction) };
        }
        if self.pins.len() >= 8192 { return Err(ApplyFailure::Capacity); }
        self.pins.insert(key, cut);
        Ok(())
    }

    pub fn release(&mut self, subscription: ChunkSubscription) {
        self.pins.remove(&(subscription.chunk,
            PeerGeneration { server: crate::identity::ServerId(subscription.subscriber), generation: subscription.incarnation },
            subscription.subscription_id));
    }

    pub fn pin_historical(
        &mut self,
        source: PeerGeneration,
        request: crate::acquisition_prefix::HistoricalChunkRequest,
    ) -> Result<(), ApplyFailure> {
        if source.generation == 0 || !request.valid() { return Err(ApplyFailure::InvalidAction); }
        let key = (request.chunk, source, request.lease);
        if let Some(previous) = self.historical_pins.get(&key) {
            return if *previous == request.cut() { Ok(()) } else { Err(ApplyFailure::InvalidAction) };
        }
        if self.historical_pins.len() >= 8192 { return Err(ApplyFailure::Capacity); }
        self.historical_pins.insert(key, request.cut());
        Ok(())
    }

    pub fn release_historical(&mut self, source: PeerGeneration, chunk: ChunkKey, lease: u64) {
        self.historical_pins.remove(&(chunk, source, lease));
    }

    pub fn prune(&mut self, receipts: &AcquisitionReceiptLedger) {
        let retired = self.cuts.iter().filter_map(|((chunk, cut), retained)| {
            let pinned = self.pins.iter().chain(self.historical_pins.iter()).any(|((pinned_chunk, _, _), pinned_cut)| pinned_chunk == chunk && pinned_cut == cut);
            (!pinned && receipts.cut_closed(TickStamp(cut.0.wrapping_add(1)), &retained.sources)).then_some((*chunk, *cut))
        }).collect::<Vec<_>>();
        for key in retired {
            let retained = self.cuts.remove(&key).expect("retired immutable cut");
            self.retained_bytes -= retained.bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acquisition_prefix::{AcquisitionPrefix, AcquisitionSourceBank};
    use crate::chunks::ChunkSubscriptionRequest;
    use crate::identity::ServerId;
    use crate::protocol::WorldId;

    #[test]
    fn exact_requested_cut_survives_prefix_closure_until_qualified_release() {
        let source = PeerGeneration { server: ServerId(1), generation: 7 };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 1, 2);
        let request = ChunkSubscriptionRequest { chunk, subscriber: 1, incarnation: 7, subscription_id: 3, from_tick: TickStamp(0) };
        let subscription = ChunkSubscription { chunk, subscriber: 1, incarnation: 7, subscription_id: 3, from_tick: TickStamp(0) };
        let mut source_bank = AcquisitionSourceBank::new(source).unwrap();
        let control = source_bank.issue(source, request).unwrap();
        let prefix = source_bank.close(TickStamp(0)).unwrap();
        let mut archive = ChunkCutArchive::new(1, 8);
        archive.retain(chunk, TickStamp(65535), Arc::new(17u64), 8, BTreeSet::from([source])).unwrap();
        archive.pin(subscription).unwrap();
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source).unwrap();
        receipts.announce(source, prefix).unwrap();
        archive.prune(&receipts);
        assert!(archive.get(chunk, TickStamp(65535)).is_some());
        assert!(archive.get(chunk, TickStamp(0)).is_none());
        receipts.receive(source, control).unwrap();
        receipts.mark_processed(source, control.sequence).unwrap();
        archive.prune(&receipts);
        assert!(archive.get(chunk, TickStamp(65535)).is_some());
        archive.release(ChunkSubscription { incarnation: 8, ..subscription });
        archive.prune(&receipts);
        assert!(archive.get(chunk, TickStamp(65535)).is_some());
        archive.release(subscription);
        archive.prune(&receipts);
        assert!(archive.get(chunk, TickStamp(65535)).is_none());
    }

    #[test]
    fn capacity_failure_keeps_live_proof_and_future_prefix_does_not_close_an_unknown_source() {
        let source = PeerGeneration { server: ServerId(1), generation: 7 };
        let other = PeerGeneration { server: ServerId(2), generation: 9 };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 1, 2);
        let mut archive = ChunkCutArchive::new(1, 8);
        archive.retain(chunk, TickStamp(3), Arc::new(17u64), 8, BTreeSet::from([source, other])).unwrap();
        assert_eq!(archive.retain(chunk, TickStamp(4), Arc::new(18u64), 8, BTreeSet::from([source])), Err(ApplyFailure::Capacity));
        let mut receipts = AcquisitionReceiptLedger::default();
        receipts.admit_source(source).unwrap();
        receipts.announce(source, AcquisitionPrefix { source, tick: TickStamp(4), sequence: 0 }).unwrap();
        archive.prune(&receipts);
        assert_eq!(*archive.get(chunk, TickStamp(3)).unwrap(), 17);
    }
}
