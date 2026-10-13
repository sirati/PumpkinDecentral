use std::collections::BTreeMap;
use std::sync::Arc;

use pumpkin_cluster::actor_local_cut::{ActorLocalChunkImage, ActorPhaseCutWitness};
use pumpkin_cluster::canonical_action::{ApplyFailure, Cell, ChunkPart, Target, Value};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;

use super::cluster_chunk_state::{OwnedChunkBlocks, OwnedChunkStore};

#[derive(Clone)]
pub(super) struct ActorLocalChunkCut {
    image: Arc<ActorLocalChunkImage>,
    chunks: Arc<OwnedChunkStore>,
    player_frames: BTreeMap<pumpkin_cluster::identity::GlobalPlayerId, (pumpkin_cluster::canonical_action::LivePlayerLease, super::cluster_world_actor::ActorSpatialState, ChunkKey, pumpkin_cluster::canonical_action::SpatialEpoch)>,
}

impl ActorLocalChunkCut {
    pub(super) fn image_from_original(
        requester: PeerGeneration,
        request: pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest,
        export: super::cluster_world_owner::ChunkStateExport,
        original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    ) -> Result<ActorLocalChunkImage, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionState, TruthView};
        if original.original_source_slot() != Some(request.full_source_slot)
            || original.original_tick() != request.source_tick() || export.snapshot_cut != request.cut() {
            return Err(ApplyFailure::InvalidAction);
        }
        let ground = export.into_payload(request.chunk, request.provider.server.0);
        let local = ground.project_local()?;
        for (target, expected) in &local {
            if original.read(TruthView::Local, *target) != *expected {
                return Err(ApplyFailure::Condition(*target));
            }
        }
        let mut entities = Vec::new();
        for (target, cell) in &local {
            let Target::Chunk { key, part: ChunkPart::Entity(id), inventory: None } = target else { continue; };
            if *key != request.chunk { return Err(ApplyFailure::InvalidAction); }
            let Some(Cell { value: Value::Entity(body), .. }) = cell else { continue; };
            let sample = original.original_entity_sample(*id).ok_or(ApplyFailure::Condition(*target))?;
            if !sample.matches_body(body) || sample.tick.is_after(request.source_tick()) {
                return Err(ApplyFailure::Condition(*target));
            }
            entities.push(sample.clone());
        }
        let players = original.original_chunk_contacts(request.chunk)?;
        let player_frames = original.original_chunk_player_frames(request.chunk)?;
        let leases = player_frames.iter().map(|frame| frame.lease).collect::<Vec<_>>();
        let phase = original.original_chunk_phase(request.chunk, &leases)?;
        let image = ActorLocalChunkImage { requester, request, phase, ground, local,
            players, player_frames, entities };
        image.validate_structural(request.provider)?;
        Ok(image)
    }

    pub(super) fn from_validated_image(
        provider: PeerGeneration,
        requester: PeerGeneration,
        original_phase: &ActorPhaseCutWitness,
        image: ActorLocalChunkImage,
    ) -> Result<Self, ApplyFailure> {
        image.validate_structural(provider)?;
        if image.requester != requester || original_phase.full_source_slot != image.phase.full_source_slot
            || original_phase.sources != image.phase.sources || original_phase.primaries != image.phase.primaries
            || image.phase.holders.iter().any(|(key, holders)| original_phase.holders.get(key) != Some(holders))
            || image.phase.players.iter().any(|(lease, association)| original_phase.players.get(lease) != Some(association)) {
            return Err(ApplyFailure::InvalidAction);
        }
        super::cluster_checkpoint::validate_image(&image.ground).map_err(|_| ApplyFailure::InvalidAction)?;
        let key = image.request.chunk;
        let (_, min_y, palettes, biomes) = pumpkin_world::level::cluster_decode_snapshot_with_blocks(key.x(), key.z(), &image.ground.snapshot)
            .ok_or(ApplyFailure::InvalidAction)?;
        let blocks = OwnedChunkBlocks::from_palettes(min_y, palettes, biomes);
        let ground = image.ground.project_ground()?;
        let sections = blocks.section_cells();
        if sections.iter().any(|(y, expected)| {
            let target = Target::Chunk { key, part: ChunkPart::BlockSection(*y), inventory: None };
            ground.get(&target).and_then(|cell| cell.as_ref()).is_none_or(|cell| cell.value != expected.value)
        }) || ground.keys().filter(|target| matches!(target, Target::Chunk { key: chunk, part: ChunkPart::BlockSection(_), inventory: None } if *chunk == key)).count() != sections.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        let document = pumpkin_world::level::OwnedChunkImage::decode(&image.ground.snapshot).ok_or(ApplyFailure::InvalidAction)?;
        let lighting = document.light_sections().ok_or(ApplyFailure::InvalidAction)?;
        for (y, sky, block) in lighting {
            let target = Target::Chunk { key, part: ChunkPart::LightSection(y), inventory: None };
            if !matches!(ground.get(&target), Some(Some(Cell { value: Value::LightSection { sky: expected_sky, block: expected_block }, .. })) if *expected_sky == sky && *expected_block == block) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let queue = Target::Chunk { key, part: ChunkPart::Whole, inventory: None };
        let raw_ticks = pumpkin_world::level::cluster_snapshot_scheduled_ticks(&image.ground.snapshot).ok_or(ApplyFailure::InvalidAction)?;
        if !matches!(ground.get(&queue), Some(Some(Cell { value: Value::ScheduledQueue(ticks), .. }))
            if super::cluster_world_owner::scheduled_raw_image_matches(ticks, &raw_ticks, image.ground.snapshot_cut)) {
            return Err(ApplyFailure::InvalidAction);
        }
        let block_entities = pumpkin_world::level::cluster_snapshot_block_entities(&image.ground.snapshot).ok_or(ApplyFailure::InvalidAction)?;
        for (position, bytes) in block_entities {
            let target = Target::Chunk { key, part: ChunkPart::BlockEntity(pumpkin_cluster::protocol::BlockPos {
                x: position.0.x, y: position.0.y, z: position.0.z,
            }), inventory: None };
            if !matches!(ground.get(&target), Some(Some(Cell { value: Value::BlockEntity(expected), .. })) if *expected == bytes) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut chunks = OwnedChunkStore::from_actor_local_projection(key, blocks, &image.local, &ground)
            .ok_or(ApplyFailure::InvalidAction)?;
        if let Some(certificate) = &image.ground.genesis {
            chunks.install_genesis(certificate.clone()).map_err(|_|ApplyFailure::InvalidAction)?;
        }
        let mut player_frames = BTreeMap::new();
        for frame in &image.player_frames {
            let spatial: super::cluster_world_actor::ActorSpatialState = postcard::from_bytes(&frame.encoded).map_err(|_| ApplyFailure::InvalidAction)?;
            let contact = image.players.iter().find(|contact| contact.identity == pumpkin_cluster::item_contact::ContactIdentity::Player(frame.lease)).ok_or(ApplyFailure::InvalidAction)?;
            let target = Target::Chunk { key: frame.association, part: ChunkPart::Player(frame.lease.gid), inventory: None };
            if spatial.frame.effective.actor != frame.lease.gid || spatial.frame.claimed.actor != frame.lease.gid
                || spatial.world != frame.association.world || spatial.chunk.world != frame.association.world
                || spatial.frame.effective.position.map(f64::to_bits) != contact.position
                || spatial.frame.effective.source_seq != contact.observed_sequence
                || spatial.frame.effective.tick.is_after(image.request.source_tick())
                || spatial.frame.effective.velocity.iter().any(|value| !value.is_finite())
                || !spatial.frame.effective.yaw.is_finite() || !spatial.frame.effective.pitch.is_finite()
                || !spatial.head_yaw.is_finite() || !spatial.fall_distance.is_finite() || spatial.game_mode > 3
                || !matches!(image.local.get(&target), Some(Some(Cell { value: Value::Living(living), .. }))
                    if living.avatar.as_ref().is_some_and(|avatar| avatar.spatial_epoch == frame.epoch)) {
                return Err(ApplyFailure::InvalidAction);
            }
            player_frames.insert(frame.lease.gid, (frame.lease, spatial, frame.association, frame.epoch));
        }
        Ok(Self { image: Arc::new(image), chunks: Arc::new(chunks), player_frames })
    }

    pub(super) fn chunk(&self) -> ChunkKey { self.image.request.chunk }
    pub(super) fn full_source_slot(&self) -> u64 { self.image.request.full_source_slot }
    pub(super) fn provider(&self) -> PeerGeneration { self.image.request.provider }
    pub(super) fn phase(&self) -> &ActorPhaseCutWitness { &self.image.phase }
    pub(super) fn local_cells(&self) -> &BTreeMap<Target, Option<Cell>> { &self.image.local }
    pub(super) fn original_store(&self) -> &Arc<OwnedChunkStore> { &self.chunks }
    pub(super) fn player_projections(&self) -> impl Iterator<Item = &(pumpkin_cluster::canonical_action::LivePlayerLease, super::cluster_world_actor::ActorSpatialState, ChunkKey, pumpkin_cluster::canonical_action::SpatialEpoch)> {
        self.player_frames.values()
    }
    pub(super) fn player_contacts(&self) -> &[pumpkin_cluster::item_contact::ContactObservation] { &self.image.players }
    pub(super) fn entity_samples(&self) -> &[pumpkin_cluster::item_spatial::ItemSpatialSample] { &self.image.entities }
}
