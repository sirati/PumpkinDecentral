use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::canonical_action::{ApplyFailure, Stakeholder};
use crate::chunks::ChunkSubscription;
use crate::entity_history::{EntityBindingProofContext, EntityHistoryEvidence};
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

#[derive(Clone, Default)]
pub struct ChunkDoneArchive {
    pins: BTreeMap<(ChunkKey, u16, u64, u64), TickStamp>,
    frames: BTreeMap<TickStamp, Arc<EntityHistoryEvidence>>,
    retained_nodes: usize,
    frame_bytes: BTreeMap<TickStamp, usize>,
    retained_bytes: usize,
    source_guards: BTreeMap<TickStamp, (TickStamp, BTreeSet<crate::realm_completion::PeerGeneration>)>,
    action_roots: BTreeSet<crate::canonical_action::ActionRef>,
}

impl ChunkDoneArchive {
    pub fn requires(&self, tick: TickStamp, chunks: &BTreeSet<ChunkKey>) -> bool {
        self.action_roots.iter().any(|reference| reference.tick == tick)
            || self.source_guards.contains_key(&tick) || self.pins
            .keys()
            .any(|(chunk, _, _, _)| chunks.contains(chunk))
    }

    pub fn set_action_roots(&mut self, roots: BTreeSet<crate::canonical_action::ActionRef>) -> Result<(), ApplyFailure> {
        crate::canonical_action::check_logical_budget(roots.len(), roots.len().saturating_mul(std::mem::size_of::<crate::canonical_action::ActionRef>()))?;
        self.action_roots = roots;
        self.prune();
        Ok(())
    }

    pub fn pin(&mut self, subscription: ChunkSubscription) -> Result<(), ApplyFailure> {
        let key = (
            subscription.chunk,
            subscription.subscriber,
            subscription.incarnation,
            subscription.subscription_id,
        );
        if subscription.incarnation == 0 || subscription.subscription_id == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(previous) = self.pins.get(&key) {
            return if *previous == subscription.from_tick {
                Ok(())
            } else {
                Err(ApplyFailure::InvalidAction)
            };
        }
        if self.pins.len() >= 8192 {
            return Err(ApplyFailure::Capacity);
        }
        self.pins.insert(key, subscription.from_tick);
        Ok(())
    }

    pub fn unpin(&mut self, subscription: ChunkSubscription) {
        self.pins.remove(&(
            subscription.chunk,
            subscription.subscriber,
            subscription.incarnation,
            subscription.subscription_id,
        ));
        self.prune();
    }

    fn chunks_for(tick: TickStamp, frame: &EntityHistoryEvidence) -> BTreeSet<ChunkKey> {
        frame
            .graphs
            .iter()
            .filter(|graph| graph.tick == tick)
            .flat_map(|graph| &graph.nodes)
            .flat_map(|node| {
                node.action
                    .stakeholders
                    .iter()
                    .filter_map(|stakeholder| match stakeholder {
                        Stakeholder::Chunk(chunk) => Some(*chunk),
                        Stakeholder::Player(_) => None,
                    })
                    .chain(node.action.player_chunks.iter().map(|(_, chunk)| *chunk))
                    .chain(node.actor_binding.iter().filter_map(|binding| {
                        match binding.resolved_source {
                            crate::canonical_action::Target::Chunk { key, .. } => Some(key),
                            _ => None,
                        }
                    }))
            })
            .collect()
    }

    fn needed(&self, tick: TickStamp, frame: &EntityHistoryEvidence) -> bool {
        let chunks = Self::chunks_for(tick, frame);
        frame.graphs.iter().filter(|graph| graph.tick == tick).flat_map(|graph| &graph.nodes)
            .any(|node| self.action_roots.contains(&crate::canonical_action::ActionRef { id: node.action.id, tick }))
            || self.source_guards.contains_key(&tick) || self.pins
            .keys()
            .any(|(chunk, _, _, _)| chunks.contains(chunk))
    }

    pub fn reserve_before_source_cut_drain(
        &mut self,
        tick: TickStamp,
        frame: Arc<EntityHistoryEvidence>,
        proofs: Option<&EntityBindingProofContext>,
        sources: BTreeSet<crate::realm_completion::PeerGeneration>,
        receipts: &crate::acquisition_prefix::AcquisitionReceiptLedger,
    ) -> Result<(), ApplyFailure> {
        if sources.is_empty() || sources.iter().any(|source| source.generation == 0) {
            return Err(ApplyFailure::InvalidAction);
        }
        let previous = self.source_guards.get(&tick).cloned();
        if !receipts.cut_closed(TickStamp(frame.snapshot_cut.0.wrapping_add(1)), &sources) {
            self.source_guards.insert(tick, (frame.snapshot_cut, sources));
        }
        let result = self.reserve_before_drain(tick, frame, proofs);
        if result.is_err() {
            if let Some(previous) = previous {
                self.source_guards.insert(tick, previous);
            } else {
                self.source_guards.remove(&tick);
            }
        }
        result
    }

    pub fn prune_closed_source_cuts(&mut self, receipts: &crate::acquisition_prefix::AcquisitionReceiptLedger) {
        self.source_guards.retain(|_, (cut, sources)| !receipts.cut_closed(TickStamp(cut.0.wrapping_add(1)), sources));
        self.prune();
    }

    pub fn reserve_before_drain(
        &mut self,
        tick: TickStamp,
        frame: Arc<EntityHistoryEvidence>,
        proofs: Option<&EntityBindingProofContext>,
    ) -> Result<(), ApplyFailure> {
        if !self.needed(tick, &frame) {
            return Ok(());
        }
        let graph = frame
            .graphs
            .iter()
            .find(|graph| graph.tick == tick)
            .ok_or(ApplyFailure::InvalidAction)?;
        let fixed = frame.fixed_dependency_outcomes(proofs)?;
        if graph.nodes.is_empty()
            || graph.nodes.iter().any(|node| {
                !fixed.contains_key(&crate::canonical_action::ActionRef {
                    tick,
                    id: node.action.id,
                })
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(previous) = self.frames.get(&tick) {
            return if **previous == *frame {
                Ok(())
            } else {
                Err(ApplyFailure::InvalidAction)
            };
        }
        let count = frame
            .graphs
            .iter()
            .try_fold(0usize, |count, graph| count.checked_add(graph.nodes.len()))
            .ok_or(ApplyFailure::Capacity)?;
        let retained = self
            .retained_nodes
            .checked_add(count)
            .filter(|count| *count <= 8192)
            .ok_or(ApplyFailure::Capacity)?;
        if self.frames.len() >= 128 {
            return Err(ApplyFailure::Capacity);
        }
        let bytes = postcard::experimental::serialized_size(frame.as_ref())
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let retained_bytes = self
            .retained_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= 64 * 1024 * 1024)
            .ok_or(ApplyFailure::Capacity)?;
        self.frames.insert(tick, frame);
        self.frame_bytes.insert(tick, bytes);
        self.retained_nodes = retained;
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    pub fn retained_frames(&self) -> impl Iterator<Item = &Arc<EntityHistoryEvidence>> {
        self.frames.values()
    }

    pub fn history_for_chunk_at_cut(&self, chunk: ChunkKey, cut: TickStamp) -> Vec<Arc<EntityHistoryEvidence>> {
        self.frames.iter().filter(|(tick, frame)| {
            !tick.is_after(cut) && !frame.snapshot_cut.is_after(cut)
                && Self::chunks_for(**tick, frame).contains(&chunk)
        }).map(|(_, frame)| frame.clone()).collect()
    }

    pub fn frames_for(
        &self,
        subscription: ChunkSubscription,
    ) -> Result<Vec<(TickStamp, Arc<EntityHistoryEvidence>)>, ApplyFailure> {
        let key = (
            subscription.chunk,
            subscription.subscriber,
            subscription.incarnation,
            subscription.subscription_id,
        );
        if self.pins.get(&key) != Some(&subscription.from_tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(self
            .frames
            .iter()
            .filter(|(tick, frame)| {
                tick.distance_since(subscription.from_tick) < 32768
                    && Self::chunks_for(**tick, frame).contains(&subscription.chunk)
            })
            .map(|(tick, frame)| (*tick, frame.clone()))
            .collect())
    }

    fn prune(&mut self) {
        let retired = self
            .frames
            .iter()
            .filter(|(tick, frame)| !self.needed(**tick, frame))
            .map(|(tick, _)| *tick)
            .collect::<Vec<_>>();
        for tick in retired {
            if let Some(frame) = self.frames.remove(&tick) {
                self.retained_bytes -= self.frame_bytes.remove(&tick).expect("archived frame size");
                self.retained_nodes -= frame
                    .graphs
                    .iter()
                    .map(|graph| graph.nodes.len())
                    .sum::<usize>();
            }
        }
    }
}
