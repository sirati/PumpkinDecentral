use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::acquisition_prefix::HistoricalChunkRequest;
use crate::canonical_action::{ActionRef, ApplyFailure, Cell, ChunkPart, LivePlayerLease, Target, Value};
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::item_spatial::ItemSpatialSample;
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::xfer::ChunkPayload;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorPhaseCutWitness {
    pub full_source_slot: u64,
    pub sources: BTreeSet<PeerGeneration>,
    pub primaries: BTreeSet<PeerGeneration>,
    pub holders: BTreeMap<ChunkKey, BTreeSet<PeerGeneration>>,
    pub players: BTreeMap<LivePlayerLease, (ChunkKey, PeerGeneration)>,
}

impl ActorPhaseCutWitness {
    pub fn valid(&self) -> bool {
        self.full_source_slot != 0 && !self.sources.is_empty()
            && self.sources.iter().all(|source| source.generation != 0)
            && self.primaries.is_subset(&self.sources)
            && self.holders.values().all(|holders| holders.is_subset(&self.sources))
            && self.players.iter().all(|(lease, (_, owner))| {
                lease.incarnation != 0 && lease.gid.server == owner.server
                    && self.sources.contains(owner)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorLocalPlayerFrame {
    pub lease: LivePlayerLease,
    pub association: ChunkKey,
    pub epoch: crate::canonical_action::SpatialEpoch,
    pub encoded: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorLocalChunkImage {
    pub requester: PeerGeneration,
    pub request: HistoricalChunkRequest,
    pub phase: ActorPhaseCutWitness,
    pub ground: ChunkPayload,
    pub local: BTreeMap<Target, Option<Cell>>,
    pub players: Vec<ContactObservation>,
    pub player_frames: Vec<ActorLocalPlayerFrame>,
    pub entities: Vec<ItemSpatialSample>,
}

impl ActorLocalChunkImage {
    pub fn validate_structural(&self, provider: PeerGeneration) -> Result<(), ApplyFailure> {
        if !self.request.valid() || self.request.provider != provider
            || !self.phase.valid() || self.phase.full_source_slot != self.request.full_source_slot
            || !self.phase.sources.contains(&self.requester)
            || !self.phase.holders.get(&self.request.chunk).is_some_and(|holders| holders.contains(&provider))
            || self.ground.chunk != self.request.chunk || self.ground.holder != provider.server.0
            || self.ground.snapshot_cut != self.request.cut()
            || self.ground.project_local()? != self.local {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut ledger = crate::realm_completion::RealmCompletionLedger::default();
        let actions = self.ground.graphs.iter().flat_map(|graph| graph.nodes.iter().map(move |node| (
            ActionRef { id: node.action.id, tick: graph.tick }, node.action.clone(),
        ))).collect();
        ledger.import_snapshot(self.ground.completion.as_ref().ok_or(ApplyFailure::InvalidAction)?, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        for (_, finish) in &self.ground.completion.as_ref().ok_or(ApplyFailure::InvalidAction)?.finishes {
            let distance = self.request.source_tick().distance_since(finish.tick);
            if distance == 0 || distance >= 32768
                || finish.full_source_slot.checked_add(u64::from(distance)) != Some(self.request.full_source_slot) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for graph in &self.ground.graphs {
            for node in &graph.nodes {
                if node.factual.result.is_some() && (!ledger.action_complete(ActionRef { id: node.action.id, tick: graph.tick })
                    || node.required_realms.iter().any(|realm| !ledger.realm_complete(graph.tick, *realm))) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        let mut players = BTreeSet::new();
        for observation in &self.players {
            let ContactIdentity::Player(lease) = observation.identity else { return Err(ApplyFailure::InvalidAction); };
            if !observation.valid() || !players.insert(lease)
                || observation.chunk != self.request.chunk || observation.simulation_tick != self.request.source_tick()
                || self.phase.players.get(&lease) != Some(&(observation.chunk, observation.owner))
                || !self.local.contains_key(&Target::Chunk { key: observation.chunk, part: ChunkPart::Player(lease.gid), inventory: None }) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut frame_players = BTreeSet::new();
        for frame in &self.player_frames {
            if frame.encoded.is_empty() || !frame_players.insert(frame.lease)
                || frame.association != self.request.chunk
                || !self.players.iter().any(|contact| contact.identity == ContactIdentity::Player(frame.lease)
                    && contact.chunk == frame.association && contact.epoch == frame.epoch) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut entities = BTreeSet::new();
        for sample in &self.entities {
            let target = Target::Chunk { key: self.request.chunk, part: ChunkPart::Entity(sample.id), inventory: None };
            let Some(Some(Cell { value: Value::Entity(body), .. })) = self.local.get(&target) else { return Err(ApplyFailure::InvalidAction); };
            let primary = self.phase.holders.get(&self.request.chunk).into_iter().flat_map(|holders| holders.intersection(&self.phase.primaries)).copied().next();
            if !entities.insert(sample.id) || !sample.matches_body(body)
                || sample.tick.is_after(self.request.source_tick())
                || sample.tick.distance_since(body.birth.action.tick) >= 32768
                || !matches!(sample.epoch, crate::canonical_action::SpatialEpoch::Action(reference) if sample.tick.distance_since(reference.tick) < 32768)
                || !sample.authorized_issuer(body, primary.unwrap_or(body.owner))
                || !self.phase.sources.contains(&sample.issuer) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let records = self.local.len().checked_add(self.players.len()).and_then(|count| count.checked_add(self.entities.len())).and_then(|count| count.checked_add(self.player_frames.len()))
            .ok_or(ApplyFailure::ResourceLimit)?;
        let bytes = postcard::experimental::serialized_size(self).map_err(|_| ApplyFailure::InvalidAction)?;
        crate::canonical_action::check_logical_budget(records, bytes)
    }
}

pub fn encode(image: &ActorLocalChunkImage) -> Result<Vec<u8>, ApplyFailure> {
    image.validate_structural(image.request.provider)?;
    let mut bytes = b"ALC2".to_vec();
    bytes.extend_from_slice(&crate::membership::CLUSTER_PROTOCOL_VERSION.to_le_bytes());
    bytes.extend(postcard::to_allocvec(image).map_err(|_| ApplyFailure::InvalidAction)?);
    if bytes.len() > crate::canonical_action::MAX_LOGICAL_ACTION_BYTES { return Err(ApplyFailure::ResourceLimit); }
    Ok(bytes)
}

pub fn decode(bytes: &[u8], provider: PeerGeneration) -> Result<ActorLocalChunkImage, ApplyFailure> {
    if bytes.len() > crate::canonical_action::MAX_LOGICAL_ACTION_BYTES || !bytes.starts_with(b"ALC2") { return Err(ApplyFailure::InvalidAction); }
    if bytes.len() < 6 || u16::from_le_bytes([bytes[4], bytes[5]]) != crate::membership::CLUSTER_PROTOCOL_VERSION { return Err(ApplyFailure::InvalidAction); }
    let image: ActorLocalChunkImage = postcard::from_bytes(&bytes[6..]).map_err(|_| ApplyFailure::InvalidAction)?;
    image.validate_structural(provider)?;
    Ok(image)
}
