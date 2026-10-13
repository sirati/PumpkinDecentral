use std::collections::{BTreeMap, HashMap, HashSet};

use pumpkin_cluster::canonical_action::{Cell, Value};
pub use pumpkin_cluster::entity_body::EntityGroundEvent;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::structural_map::StructuralMap;
use pumpkin_data::BlockStateId;
use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
use pumpkin_world::level::SyncChunk;

#[derive(Clone)]
pub struct OwnedChunkBlocks {
    min_y: i32,
    sections: std::sync::Arc<[BlockPalette]>,
    biomes: std::sync::Arc<[BiomePalette]>,
    revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockChange {
    pub chunk: ChunkKey,
    pub position: BlockPos,
    pub before: BlockStateId,
    pub after: BlockStateId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockTransactionError {
    MissingChunk,
    OutsideChunk,
    OutsideHeight,
    DuplicatePosition,
    StaleState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedRandomTickSample {
    pub position: BlockPos,
    pub tick_block: bool,
    pub tick_fluid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RandomTickSampleError {
    Budget { required: usize },
    SampleCountOverflow,
    OutsideWorld,
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Default)]
pub struct OwnedChunkStore {
    genesis: StructuralMap<ChunkKey, pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate>,
    chunks: StructuralMap<ChunkKey, OwnedChunkBlocks>,
    ground: StructuralMap<ChunkKey, OwnedChunkBlocks>,
    scheduled_local: StructuralMap<ChunkKey, Cell>,
    scheduled_ground: StructuralMap<ChunkKey, Cell>,
    occupancy_local: StructuralMap<ChunkKey, Cell>,
    occupancy_ground: StructuralMap<ChunkKey, Cell>,
    block_entities_local: StructuralMap<(ChunkKey, BlockPos), Cell>,
    block_entities_ground: StructuralMap<(ChunkKey, BlockPos), Cell>,
    light_sections_local: StructuralMap<(ChunkKey, i32), Cell>,
    light_sections_ground: StructuralMap<(ChunkKey, i32), Cell>,
    block_sections_local: StructuralMap<(ChunkKey, i32), Cell>,
    block_sections_ground: StructuralMap<(ChunkKey, i32), Cell>,
    entity_bodies_local: StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, (ChunkKey, Cell)>,
    entity_bodies_ground: StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, (ChunkKey, Cell)>,
    entities_local_by_chunk: StructuralMap<ChunkKey, StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, ()>>,
    entities_ground_by_chunk: StructuralMap<ChunkKey, StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, ()>>,
    entity_births: StructuralMap<
        pumpkin_cluster::protocol::GlobalEntityId,
        pumpkin_cluster::entity_body::EntityBirth,
    >,
    entity_birth_ids: StructuralMap<uuid::Uuid, pumpkin_cluster::protocol::GlobalEntityId>,
    entity_ground_history:
        StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, Vec<EntityGroundEvent>>,
    fixed_entity_ground_history:
        StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, Vec<EntityGroundEvent>>,
    entity_ground_evidence: StructuralMap<
        pumpkin_cluster::canonical_action::ActionRef,
        std::sync::Arc<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
    >,
    local_entity_revision: u64,
    entity_changes: StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, (Option<ChunkKey>, Option<ChunkKey>)>,
    unit_chunks: Option<std::collections::BTreeSet<ChunkKey>>,
    unit_births: std::collections::BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>,
    unit_evidence: std::collections::BTreeSet<pumpkin_cluster::canonical_action::ActionRef>,
}

fn move_shared_entries<K: Ord + Clone, V: Clone>(destination: &mut StructuralMap<K, V>, source: &mut StructuralMap<K, V>) {
    let keys = source.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        if let Some(value) = source.take_shared(&key) {
            destination.insert_shared(key, value);
        }
    }
}

pub struct PreparedBlockMutation {
    changes: Vec<BlockChange>,
    inverse: Vec<BlockChange>,
}

pub struct PreparedBlockReplacement {
    ground: bool,
    chunks: HashMap<ChunkKey, OwnedChunkBlocks>,
}

impl PreparedBlockMutation {
    pub fn chunks(&self) -> impl Iterator<Item = ChunkKey> + '_ {
        self.changes.iter().map(|change| change.chunk)
    }
}

impl OwnedChunkBlocks {
    pub fn from_palettes(
        min_y: i32,
        sections: Box<[BlockPalette]>,
        biomes: Box<[BiomePalette]>,
    ) -> Self {
        Self {
            min_y,
            sections: sections.into(),
            biomes: biomes.into(),
            revision: 0,
        }
    }

    pub fn from_chunk(chunk: &SyncChunk) -> Self {
        Self {
            min_y: chunk.section.min_y,
            sections: chunk
                .section
                .block_sections
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone().into(),
            biomes: chunk
                .section
                .biome_sections
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone().into(),
            revision: 0,
        }
    }

    fn indices(
        &self,
        chunk: ChunkKey,
        position: BlockPos,
    ) -> Result<(usize, usize, usize, usize), BlockTransactionError> {
        if position.x.div_euclid(16) != chunk.x() || position.z.div_euclid(16) != chunk.z() {
            return Err(BlockTransactionError::OutsideChunk);
        }
        let height = position
            .y
            .checked_sub(self.min_y)
            .ok_or(BlockTransactionError::OutsideHeight)?;
        let section = usize::try_from(height.div_euclid(16))
            .map_err(|_| BlockTransactionError::OutsideHeight)?;
        if section >= self.sections.len() {
            return Err(BlockTransactionError::OutsideHeight);
        }
        Ok((
            section,
            usize::try_from(position.x.rem_euclid(16)).unwrap_or(0),
            usize::try_from(height.rem_euclid(16)).unwrap_or(0),
            usize::try_from(position.z.rem_euclid(16)).unwrap_or(0),
        ))
    }

    pub fn block(
        &self,
        chunk: ChunkKey,
        position: BlockPos,
    ) -> Result<BlockStateId, BlockTransactionError> {
        let (section, x, y, z) = self.indices(chunk, position)?;
        Ok(self.sections[section].get(x, y, z))
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn min_y(&self) -> i32 {
        self.min_y
    }

    pub fn block_palettes(&self) -> &[BlockPalette] {
        &self.sections
    }

    pub fn section_cells(&self) -> Vec<(i32, Cell)> {
        self.sections
            .iter()
            .enumerate()
            .map(|(index, palette)| {
                let values = (0..4096)
                    .map(|offset| {
                        palette
                            .get(offset % 16, offset / 256, (offset / 16) % 16)
                            .as_u16()
                    })
                    .collect();
                (
                    self.min_y.div_euclid(16) + index as i32,
                    Cell {
                        revision: 0,
                        value: Value::BlockSection(values),
                    },
                )
            })
            .collect()
    }

    pub fn sample_random_ticks(
        &self,
        chunk: ChunkKey,
        tick: TickStamp,
        samples_per_section: usize,
        budget: usize,
    ) -> Result<Vec<OwnedRandomTickSample>, RandomTickSampleError> {
        let required = self
            .sections
            .len()
            .checked_mul(samples_per_section)
            .ok_or(RandomTickSampleError::SampleCountOverflow)?;
        if required > budget {
            return Err(RandomTickSampleError::Budget { required });
        }
        let base_x = chunk
            .x()
            .checked_mul(16)
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        let base_z = chunk
            .z()
            .checked_mul(16)
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        base_x
            .checked_add(15)
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        base_z
            .checked_add(15)
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        let height = self
            .sections
            .len()
            .checked_mul(16)
            .and_then(|height| i32::try_from(height).ok())
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        self.min_y
            .checked_add(height)
            .ok_or(RandomTickSampleError::OutsideWorld)?;
        let mut output = Vec::new();
        let mut random = super::cluster_world_apply::CLUSTER_SEED
            ^ u64::from(chunk.world.key)
            ^ (u64::from(chunk.world.dimension) << 16)
            ^ (u64::from(tick.0) << 32)
            ^ (chunk.x() as u32 as u64).rotate_left(13)
            ^ (chunk.z() as u32 as u64).rotate_left(41);
        for (section, palette) in self.sections.iter().enumerate() {
            for _ in 0..samples_per_section {
                let sample = next_random(&mut random);
                let x = (sample & 15) as usize;
                let y = ((sample >> 4) & 15) as usize;
                let z = ((sample >> 8) & 15) as usize;
                let state = palette.get(x, y, z);
                let tick_block = pumpkin_data::block_properties::has_random_ticks(state);
                let tick_fluid = pumpkin_world::chunk::palette::has_random_ticking_fluid(state);
                if tick_block || tick_fluid {
                    output.push(OwnedRandomTickSample {
                        position: BlockPos {
                            x: base_x + x as i32,
                            y: self.min_y + (section * 16 + y) as i32,
                            z: base_z + z as i32,
                        },
                        tick_block,
                        tick_fluid,
                    });
                }
            }
        }
        Ok(output)
    }

    pub fn biome_palettes(&self) -> &[BiomePalette] {
        &self.biomes
    }

    pub fn rough_biome_id(&self, position: BlockPos) -> Option<u8> {
        let height = position.y.checked_sub(self.min_y)?;
        let section = usize::try_from(height.div_euclid(16)).ok()?;
        self.biomes.get(section).map(|palette| {
            palette.get(
                usize::try_from(position.x.rem_euclid(16)).unwrap_or(0) >> 2,
                usize::try_from(height.rem_euclid(16)).unwrap_or(0) >> 2,
                usize::try_from(position.z.rem_euclid(16)).unwrap_or(0) >> 2,
            )
        })
    }
}

impl OwnedChunkStore {
    pub(super) fn from_actor_local_projection(
        chunk: ChunkKey,
        ground: OwnedChunkBlocks,
        local: &BTreeMap<pumpkin_cluster::canonical_action::Target, Option<Cell>>,
        projected_ground: &BTreeMap<pumpkin_cluster::canonical_action::Target, Option<Cell>>,
    ) -> Option<Self> {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target};
        let mut blocks = ground.clone();
        for (index, palette) in std::sync::Arc::make_mut(&mut blocks.sections).iter_mut().enumerate() {
            let y = blocks.min_y.div_euclid(16).checked_add(index.try_into().ok()?)?;
            let target = Target::Chunk { key: chunk, part: ChunkPart::BlockSection(y), inventory: None };
            let Some(Some(Cell { value: Value::BlockSection(values), .. })) = local.get(&target) else { return None; };
            if values.len() != 4096 { return None; }
            for (index, state) in values.iter().enumerate() {
                palette.set(index & 15, index >> 8, (index >> 4) & 15, BlockStateId::new(*state)?);
            }
        }
        let mut store = Self::default();
        store.ground.insert(chunk, ground);
        store.chunks.insert(chunk, blocks);
        for (is_ground, cells) in [(true, projected_ground), (false, local)] {
            for (target, cell) in cells {
                let Target::Chunk { key, part, inventory: None } = target else { continue; };
                if *key != chunk { return None; }
                match part {
                    ChunkPart::BlockSection(y) => store.install_block_section_phase(is_ground, chunk, *y, cell.clone()?),
                    ChunkPart::ActorOccupancy => if !store.install_occupancy_phase(is_ground, chunk, cell.clone()?) { return None; },
                    ChunkPart::Whole => {
                        if !matches!(cell, Some(Cell { value: Value::ScheduledQueue(_), .. })) { return None; }
                        store.install_scheduled_phase(is_ground, chunk, cell.clone()?);
                    }
                    ChunkPart::BlockEntity(position) => {
                        if let Some(Cell { value: Value::BlockEntity(bytes), .. }) = cell {
                            let state = if is_ground { store.ground_block(chunk, *position).ok()? } else { store.block(chunk, *position).ok()? };
                            if !pumpkin_world::level::cluster_block_entity_document_type_matches(state, bytes) { return None; }
                        } else if cell.is_some() { return None; }
                        store.install_block_entity_phase(is_ground, chunk, *position, cell.clone());
                    }
                    ChunkPart::LightSection(y) => {
                        if !matches!(cell, Some(Cell { value: Value::LightSection { sky, block }, .. }) if sky.len() == 2048 && block.len() == 2048) { return None; }
                        store.install_light_section_phase(is_ground, chunk, *y, cell.clone());
                    }
                    ChunkPart::Entity(id) => {
                        if cell.as_ref().is_some_and(|cell| !matches!(&cell.value, Value::Entity(body) if body.belongs_to(*id, chunk))) { return None; }
                        store.install_entity_body_phase(is_ground, chunk, *id, cell.clone());
                    }
                    _ => {}
                }
            }
        }
        Some(store)
    }

    pub(super) fn merge_missing_actor_chunk(&mut self, source: &Self, chunk: ChunkKey) -> bool {
        if self.contains(chunk) || !source.contains(chunk) { return false; }
        if source.genesis.get(&chunk).is_some_and(|certificate| self.genesis.get(&chunk).is_some_and(|old|old != certificate)) { return false; }
        let incoming_ids = source.chunk_unit_entity_ids(&std::collections::BTreeSet::from([chunk]));
        for id in &incoming_ids {
            let Some(birth) = source.entity_births.get(id) else { return false; };
            if self.entity_bodies_local.contains_key(id) || self.entity_bodies_ground.contains_key(id)
                || !self.entity_birth_matches(*id,birth) { return false; }
        }
        let mut selected = source.callback_subset(&std::collections::BTreeSet::from([chunk]));
        for id in incoming_ids {
            let birth = source.entity_births.get(&id).unwrap().clone();
            self.entity_birth_ids.insert(birth.uuid,id);
            self.entity_births.insert(id,birth);
        }
        move_shared_entries(&mut self.genesis, &mut selected.genesis);
        if let Some(blocks) = selected.chunks.take_shared(&chunk) {
            self.chunks.insert_shared(chunk, blocks);
        }
        if let Some(blocks) = selected.ground.take_shared(&chunk) {
            self.ground.insert_shared(chunk, blocks);
        }
        move_shared_entries(&mut self.occupancy_local, &mut selected.occupancy_local);
        move_shared_entries(&mut self.occupancy_ground, &mut selected.occupancy_ground);
        move_shared_entries(&mut self.scheduled_local, &mut selected.scheduled_local);
        move_shared_entries(&mut self.scheduled_ground, &mut selected.scheduled_ground);
        move_shared_entries(&mut self.block_entities_local, &mut selected.block_entities_local);
        move_shared_entries(&mut self.block_entities_ground, &mut selected.block_entities_ground);
        move_shared_entries(&mut self.light_sections_local, &mut selected.light_sections_local);
        move_shared_entries(&mut self.light_sections_ground, &mut selected.light_sections_ground);
        self.block_sections_local.extend(source.block_sections_local.iter().filter(|((key, _), _)| *key == chunk).map(|(key, value)| (*key, value.clone())));
        self.block_sections_ground.extend(source.block_sections_ground.iter().filter(|((key, _), _)| *key == chunk).map(|(key, value)| (*key, value.clone())));
        for (is_ground, bodies) in [(true, selected.entity_bodies_ground), (false, selected.entity_bodies_local)] {
            for (id, (key, cell)) in &bodies { self.install_entity_body_phase(is_ground, *key, *id, Some(cell.clone())); }
        }
        true
    }

    pub fn chunk_unit_entity_ids(&self, chunks: &std::collections::BTreeSet<ChunkKey>) -> std::collections::BTreeSet<pumpkin_cluster::protocol::GlobalEntityId> {
        chunks.iter().flat_map(|chunk| {
            self.entities_local_by_chunk.get(chunk).into_iter()
                .chain(self.entities_ground_by_chunk.get(chunk))
                .flat_map(|ids| ids.keys().copied())
        }).collect()
    }

    pub fn preflight_chunk_unit(&self, chunks: &std::collections::BTreeSet<ChunkKey>, entities: &[pumpkin_cluster::protocol::GlobalEntityId]) -> Result<(), BlockTransactionError> {
        if self.unit_chunks.is_some() || chunks.is_empty() || chunks.iter().any(|key| !self.chunks.contains_key(key) || !self.ground.contains_key(key)) {
            return Err(BlockTransactionError::MissingChunk);
        }
        let ids = entities.iter().copied().collect::<std::collections::BTreeSet<_>>();
        if ids.len() != entities.len() { return Err(BlockTransactionError::DuplicatePosition); }
        for index in [&self.entities_local_by_chunk, &self.entities_ground_by_chunk] {
            for chunk in chunks {
                if index.get(chunk).is_some_and(|held| held.keys().any(|id| !ids.contains(id))) {
                    return Err(BlockTransactionError::OutsideChunk);
                }
            }
        }
        for id in &ids {
            for bodies in [&self.entity_bodies_local, &self.entity_bodies_ground] {
                if bodies.get(id).is_some_and(|(key, _)| !chunks.contains(key)) {
                    return Err(BlockTransactionError::OutsideChunk);
                }
            }
        }
        Ok(())
    }

    pub fn take_chunk_unit(&mut self, chunks: &std::collections::BTreeSet<ChunkKey>, entities: &[pumpkin_cluster::protocol::GlobalEntityId]) -> Result<Self, BlockTransactionError> {
        self.preflight_chunk_unit(chunks, entities)?;
        let mut unit = Self::default();
        unit.unit_chunks = Some(chunks.clone());
        unit.entity_births = self.entity_births.clone();
        unit.entity_birth_ids = self.entity_birth_ids.clone();
        unit.entity_ground_evidence = self.entity_ground_evidence.clone();
        for key in chunks {
            if let Some(value) = self.genesis.take_shared(key) { unit.genesis.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.chunks.take_shared(key) { unit.chunks.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.ground.take_shared(key) { unit.ground.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.scheduled_local.take_shared(key) { unit.scheduled_local.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.scheduled_ground.take_shared(key) { unit.scheduled_ground.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.occupancy_local.take_shared(key) { unit.occupancy_local.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.occupancy_ground.take_shared(key) { unit.occupancy_ground.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.entities_local_by_chunk.take_shared(key) { unit.entities_local_by_chunk.insert_shared(*key, value); }
        }
        for key in chunks {
            if let Some(value) = self.entities_ground_by_chunk.take_shared(key) { unit.entities_ground_by_chunk.insert_shared(*key, value); }
        }
        for chunk in chunks {
            let keys = self.block_entities_local.range((*chunk, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(*chunk, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.block_entities_local.take_shared(&key) { unit.block_entities_local.insert_shared(key, value); } }
        }
        for chunk in chunks {
            let keys = self.block_entities_ground.range((*chunk, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(*chunk, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.block_entities_ground.take_shared(&key) { unit.block_entities_ground.insert_shared(key, value); } }
        }
        for chunk in chunks {
            let keys = self.light_sections_local.range((*chunk, i32::MIN)..=(*chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.light_sections_local.take_shared(&key) { unit.light_sections_local.insert_shared(key, value); } }
        }
        for chunk in chunks {
            let keys = self.light_sections_ground.range((*chunk, i32::MIN)..=(*chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.light_sections_ground.take_shared(&key) { unit.light_sections_ground.insert_shared(key, value); } }
        }
        for chunk in chunks {
            let keys = self.block_sections_local.range((*chunk, i32::MIN)..=(*chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.block_sections_local.take_shared(&key) { unit.block_sections_local.insert_shared(key, value); } }
        }
        for chunk in chunks {
            let keys = self.block_sections_ground.range((*chunk, i32::MIN)..=(*chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
            for key in keys { if let Some(value) = self.block_sections_ground.take_shared(&key) { unit.block_sections_ground.insert_shared(key, value); } }
        }
        for id in entities { if let Some(value) = self.entity_bodies_local.take_shared(id) { unit.entity_bodies_local.insert_shared(*id, value); } }
        for id in entities { if let Some(value) = self.entity_bodies_ground.take_shared(id) { unit.entity_bodies_ground.insert_shared(*id, value); } }
        for id in entities { if let Some(value) = self.entity_ground_history.take_shared(id) { unit.entity_ground_history.insert_shared(*id, value); } }
        for id in entities { if let Some(value) = self.fixed_entity_ground_history.take_shared(id) { unit.fixed_entity_ground_history.insert_shared(*id, value); } }
        Ok(unit)
    }

    pub fn can_return_chunk_unit(&self, unit: &Self) -> bool {
        let Some(chunks) = unit.unit_chunks.as_ref() else { return false; };
        !self.unit_chunks.is_some()
            && chunks.iter().all(|key| !self.chunks.contains_key(key) && !self.ground.contains_key(key) && unit.chunks.contains_key(key) && unit.ground.contains_key(key))
            && unit.chunks.keys().chain(unit.ground.keys()).all(|key| chunks.contains(key))
            && unit.genesis.iter().all(|(key,certificate)| chunks.contains(key) && certificate.chunk == *key && self.genesis.get(key).is_none_or(|old| old == certificate))
            && unit.scheduled_local.keys().chain(unit.scheduled_ground.keys()).chain(unit.occupancy_local.keys()).chain(unit.occupancy_ground.keys()).all(|key| chunks.contains(key))
            && unit.block_entities_local.keys().chain(unit.block_entities_ground.keys()).all(|(key,_)| chunks.contains(key))
            && unit.light_sections_local.keys().chain(unit.light_sections_ground.keys()).chain(unit.block_sections_local.keys()).chain(unit.block_sections_ground.keys()).all(|(key,_)| chunks.contains(key))
            && unit.entity_bodies_local.iter().chain(unit.entity_bodies_ground.iter()).all(|(id,(key,_))| chunks.contains(key) && !self.entity_bodies_local.contains_key(id) && !self.entity_bodies_ground.contains_key(id))
            && unit.unit_births.iter().all(|id| unit.entity_births.get(id).is_some_and(|birth| self.entity_birth_matches(*id,birth)))
    }

    pub fn return_chunk_unit(&mut self, mut unit: Self) -> Result<(), Self> {
        if !self.can_return_chunk_unit(&unit) { return Err(unit); }
        move_shared_entries(&mut self.genesis, &mut unit.genesis);
        move_shared_entries(&mut self.chunks, &mut unit.chunks);
        move_shared_entries(&mut self.ground, &mut unit.ground);
        move_shared_entries(&mut self.scheduled_local, &mut unit.scheduled_local);
        move_shared_entries(&mut self.scheduled_ground, &mut unit.scheduled_ground);
        move_shared_entries(&mut self.occupancy_local, &mut unit.occupancy_local);
        move_shared_entries(&mut self.occupancy_ground, &mut unit.occupancy_ground);
        move_shared_entries(&mut self.entities_local_by_chunk, &mut unit.entities_local_by_chunk);
        move_shared_entries(&mut self.entities_ground_by_chunk, &mut unit.entities_ground_by_chunk);
        move_shared_entries(&mut self.block_entities_local, &mut unit.block_entities_local);
        move_shared_entries(&mut self.block_entities_ground, &mut unit.block_entities_ground);
        move_shared_entries(&mut self.light_sections_local, &mut unit.light_sections_local);
        move_shared_entries(&mut self.light_sections_ground, &mut unit.light_sections_ground);
        move_shared_entries(&mut self.block_sections_local, &mut unit.block_sections_local);
        move_shared_entries(&mut self.block_sections_ground, &mut unit.block_sections_ground);
        move_shared_entries(&mut self.entity_bodies_local, &mut unit.entity_bodies_local);
        move_shared_entries(&mut self.entity_bodies_ground, &mut unit.entity_bodies_ground);
        move_shared_entries(&mut self.entity_ground_history, &mut unit.entity_ground_history);
        move_shared_entries(&mut self.fixed_entity_ground_history, &mut unit.fixed_entity_ground_history);
        for id in unit.unit_births {
            if let Some(birth) = unit.entity_births.get_shared(&id) {
                self.entity_birth_ids.insert(birth.uuid,id);
                self.entity_births.insert_shared(id,birth);
            }
        }
        for reference in unit.unit_evidence {
            if let Some(evidence) = unit.entity_ground_evidence.take_shared(&reference) { self.entity_ground_evidence.insert_shared(reference,evidence); }
        }
        for (id, (before, after)) in &unit.entity_changes {
            self.entity_changes.entry(*id).or_insert((*before,*after)).1 = *after;
        }
        self.local_entity_revision = self.local_entity_revision.wrapping_add(unit.local_entity_revision);
        Ok(())
    }

    pub fn callback_subset(&self, keys: &std::collections::BTreeSet<ChunkKey>) -> Self {
        let mut selected = Self::default();
        for key in keys {
            if let Some(value) = self.genesis.get_shared(key) { selected.genesis.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.chunks.get_shared(key) { selected.chunks.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.ground.get_shared(key) { selected.ground.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.scheduled_local.get_shared(key) { selected.scheduled_local.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.scheduled_ground.get_shared(key) { selected.scheduled_ground.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.occupancy_local.get_shared(key) { selected.occupancy_local.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.occupancy_ground.get_shared(key) { selected.occupancy_ground.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.entities_local_by_chunk.get_shared(key) { selected.entities_local_by_chunk.insert_shared(*key, value); }
        }
        for key in keys {
            if let Some(value) = self.entities_ground_by_chunk.get_shared(key) { selected.entities_ground_by_chunk.insert_shared(*key, value); }
        }
        for key in keys {
            for (target, _) in self.block_entities_local.range((*key, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(*key, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})) {
                selected.block_entities_local.insert_shared(*target, self.block_entities_local.get_shared(target).unwrap());
            }
        }
        for key in keys {
            for (target, _) in self.block_entities_ground.range((*key, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(*key, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})) {
                selected.block_entities_ground.insert_shared(*target, self.block_entities_ground.get_shared(target).unwrap());
            }
        }
        for key in keys {
            for (target, _) in self.light_sections_local.range((*key, i32::MIN)..=(*key, i32::MAX)) {
                selected.light_sections_local.insert_shared(*target, self.light_sections_local.get_shared(target).unwrap());
            }
        }
        for key in keys {
            for (target, _) in self.light_sections_ground.range((*key, i32::MIN)..=(*key, i32::MAX)) {
                selected.light_sections_ground.insert_shared(*target, self.light_sections_ground.get_shared(target).unwrap());
            }
        }
        for key in keys {
            if let Some(ids) = self.entities_local_by_chunk.get(key) {
                for id in ids.keys() {
                    if let Some(value) = self.entity_bodies_local.get_shared(id) { selected.entity_bodies_local.insert_shared(*id, value); }
                }
            }
        }
        for key in keys {
            if let Some(ids) = self.entities_ground_by_chunk.get(key) {
                for id in ids.keys() {
                    if let Some(value) = self.entity_bodies_ground.get_shared(id) { selected.entity_bodies_ground.insert_shared(*id, value); }
                }
            }
        }
        selected.entity_births = self.entity_births.clone();
        selected.entity_birth_ids = self.entity_birth_ids.clone();
        selected.local_entity_revision = self.local_entity_revision;
        selected
    }

    pub fn clone_subset(&self, keys: &std::collections::BTreeSet<ChunkKey>) -> Self {
        let history_references = self
            .entity_ground_history
            .values()
            .chain(self.fixed_entity_ground_history.values())
            .flat_map(|events| events.iter())
            .filter(|event| {
                event.before.is_some_and(|(chunk, _)| keys.contains(&chunk))
                    || event.after.is_some_and(|(chunk, _)| keys.contains(&chunk))
            })
            .map(|event| event.action)
            .collect::<std::collections::BTreeSet<_>>();
        Self {
            genesis: keys.iter().filter_map(|key| self.genesis.get_shared(key).map(|value| (*key,value))).fold(StructuralMap::default(), |mut map,(key,value)| {map.insert_shared(key,value);map}),
            unit_chunks: None,
            entity_changes: StructuralMap::default(),
            unit_births: Default::default(),
            unit_evidence: Default::default(),
            entities_local_by_chunk: self.callback_subset(keys).entities_local_by_chunk,
            entities_ground_by_chunk: self.callback_subset(keys).entities_ground_by_chunk,
            occupancy_local: self.occupancy_local.iter().filter(|(key, _)| keys.contains(key)).map(|(key, value)| (*key, value.clone())).collect(),
            occupancy_ground: self.occupancy_ground.iter().filter(|(key, _)| keys.contains(key)).map(|(key, value)| (*key, value.clone())).collect(),
            local_entity_revision: self.local_entity_revision,
            chunks: keys
                .iter()
                .filter_map(|key| self.chunks.get(key).cloned().map(|value| (*key, value)))
                .collect(),
            ground: keys
                .iter()
                .filter_map(|key| self.ground.get(key).cloned().map(|value| (*key, value)))
                .collect(),
            scheduled_local: keys
                .iter()
                .filter_map(|key| {
                    self.scheduled_local
                        .get(key)
                        .cloned()
                        .map(|value| (*key, value))
                })
                .collect(),
            scheduled_ground: keys
                .iter()
                .filter_map(|key| {
                    self.scheduled_ground
                        .get(key)
                        .cloned()
                        .map(|value| (*key, value))
                })
                .collect(),
            block_entities_local: self
                .block_entities_local
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            block_entities_ground: self
                .block_entities_ground
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            light_sections_local: self
                .light_sections_local
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            light_sections_ground: self
                .light_sections_ground
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            block_sections_local: self
                .block_sections_local
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            block_sections_ground: self
                .block_sections_ground
                .iter()
                .filter(|((chunk, _), _)| keys.contains(chunk))
                .map(|(key, value)| (*key, value.clone()))
                .collect(),
            entity_births: self.entity_births.clone(),
            entity_birth_ids: self.entity_birth_ids.clone(),
            entity_ground_evidence: history_references
                .iter()
                .filter_map(|reference| {
                    self.entity_ground_evidence
                        .get(reference)
                        .map(|frame| (*reference, frame.clone()))
                })
                .collect(),
            fixed_entity_ground_history: self
                .fixed_entity_ground_history
                .iter()
                .filter(|(_, events)| {
                    events
                        .iter()
                        .any(|event| history_references.contains(&event.action))
                })
                .map(|(id, events)| (*id, events.clone()))
                .collect(),
            entity_ground_history: self
                .entity_ground_history
                .iter()
                .filter(|(_, events)| {
                    events.iter().any(|event| {
                        event
                            .before
                            .into_iter()
                            .chain(event.after)
                            .any(|(chunk, _)| keys.contains(&chunk))
                    })
                })
                .map(|(id, events)| (*id, events.clone()))
                .collect(),
            entity_bodies_local: self
                .entity_bodies_local
                .iter()
                .filter(|(_, (chunk, _))| keys.contains(chunk))
                .map(|(id, value)| (*id, value.clone()))
                .collect(),
            entity_bodies_ground: self
                .entity_bodies_ground
                .iter()
                .filter(|(_, (chunk, _))| keys.contains(chunk))
                .map(|(id, value)| (*id, value.clone()))
                .collect(),
        }
    }

    pub fn prepared_ground_chunks(
        &self,
        keys: &std::collections::BTreeSet<ChunkKey>,
    ) -> Result<PreparedBlockReplacement, BlockTransactionError> {
        let chunks = keys
            .iter()
            .map(|key| {
                self.ground
                    .get(key)
                    .cloned()
                    .map(|blocks| (*key, blocks))
                    .ok_or(BlockTransactionError::MissingChunk)
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(PreparedBlockReplacement {
            ground: true,
            chunks,
        })
    }

    pub fn prepare_replacement(
        &self,
        ground: bool,
        changes: &[BlockChange],
    ) -> Result<PreparedBlockReplacement, BlockTransactionError> {
        if ground {
            self.validate_ground(changes)?;
        } else {
            self.validate(changes)?;
        }
        let source = if ground { &self.ground } else { &self.chunks };
        let mut chunks = HashMap::new();
        for change in changes {
            let chunk = match chunks.entry(change.chunk) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                    source
                        .get(&change.chunk)
                        .ok_or(BlockTransactionError::MissingChunk)?
                        .clone(),
                ),
            };
            let (section, x, y, z) = chunk.indices(change.chunk, change.position)?;
            std::sync::Arc::make_mut(&mut chunk.sections)[section].set(x, y, z, change.after);
            chunk.revision = chunk.revision.wrapping_add(1);
        }
        Ok(PreparedBlockReplacement { ground, chunks })
    }

    pub fn install_replacement(&mut self, prepared: PreparedBlockReplacement) {
        let destination = if prepared.ground {
            &mut self.ground
        } else {
            &mut self.chunks
        };
        destination.extend(prepared.chunks);
    }

    pub fn contains(&self, chunk: ChunkKey) -> bool {
        self.chunks.contains_key(&chunk)
    }

    pub(super) fn genesis(&self, chunk: ChunkKey) -> Option<&pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate> {
        self.genesis.get(&chunk)
    }

    pub(super) fn install_genesis(&mut self, certificate: pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate) -> Result<(), BlockTransactionError> {
        if certificate.validate_structural().is_err() || self.genesis.get(&certificate.chunk).is_some_and(|old| old != &certificate) {
            return Err(BlockTransactionError::StaleState);
        }
        self.genesis.insert(certificate.chunk,certificate);
        Ok(())
    }

    pub fn install(&mut self, chunk: ChunkKey, blocks: OwnedChunkBlocks) {
        self.ground.insert(chunk, blocks.clone());
        self.chunks.insert(chunk, blocks);
    }

    pub fn occupancy_root(&self, ground: bool, chunk: ChunkKey) -> Option<&Cell> {
        if ground { self.occupancy_ground.get(&chunk) } else { self.occupancy_local.get(&chunk) }
    }

    pub fn install_occupancy_phase(&mut self, ground: bool, chunk: ChunkKey, cell: Cell) -> bool {
        if !self.contains(chunk) || !matches!(&cell.value, Value::ActorOccupancy(root) if root.valid() && root.is_complete()) {
            return false;
        }
        if ground { self.occupancy_ground.insert(chunk, cell); } else { self.occupancy_local.insert(chunk, cell); }
        true
    }

    pub fn install_scheduled_queue(&mut self, chunk: ChunkKey, queue: Cell) -> bool {
        if !self.contains(chunk) || !matches!(queue.value, Value::ScheduledQueue(_)) {
            return false;
        }
        self.scheduled_local.insert(chunk, queue.clone());
        self.scheduled_ground.insert(chunk, queue);
        true
    }

    pub fn scheduled_queue(&self, ground: bool, chunk: ChunkKey) -> Option<&Cell> {
        if ground {
            self.scheduled_ground.get(&chunk)
        } else {
            self.scheduled_local.get(&chunk)
        }
    }

    pub fn block_entity(&self, ground: bool, chunk: ChunkKey, position: BlockPos) -> Option<&Cell> {
        if ground {
            self.block_entities_ground.get(&(chunk, position))
        } else {
            self.block_entities_local.get(&(chunk, position))
        }
    }

    pub fn minimum_y(&self, chunk: ChunkKey) -> Option<i32> {
        self.chunks.get(&chunk).map(OwnedChunkBlocks::min_y)
    }

    pub fn block_section(&self, ground: bool, chunk: ChunkKey, section_y: i32) -> Option<&Cell> {
        if ground {
            self.block_sections_ground.get(&(chunk, section_y))
        } else {
            self.block_sections_local.get(&(chunk, section_y))
        }
    }

    pub fn scheduled_dependency_roots(
        &self,
        chunk: Option<ChunkKey>,
    ) -> std::collections::BTreeSet<pumpkin_cluster::canonical_action::ActionRef> {
        self.scheduled_ground
            .iter()
            .filter(|(key, _)| chunk.is_none_or(|chunk| **key == chunk))
            .flat_map(|(_, cell)| {
                let Value::ScheduledQueue(facts) = &cell.value else {
                    return Vec::new();
                };
                facts
                    .iter()
                    .filter_map(|fact| match fact.origin {
                        pumpkin_cluster::canonical_action::ScheduledTickOrigin::Action {
                            cause,
                            ..
                        } => Some(cause),
                        pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary => {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn install_block_section_phase(
        &mut self,
        ground: bool,
        chunk: ChunkKey,
        section_y: i32,
        cell: Cell,
    ) {
        let source = if ground {
            &mut self.block_sections_ground
        } else {
            &mut self.block_sections_local
        };
        source.insert((chunk, section_y), cell);
    }

    pub fn initial_block_sections(&self, chunk: ChunkKey) -> Option<Vec<(i32, Cell)>> {
        let blocks = self.ground.get(&chunk)?;
        Some(blocks.section_cells())
    }

    pub fn light_section(&self, ground: bool, chunk: ChunkKey, section_y: i32) -> Option<&Cell> {
        if ground {
            self.light_sections_ground.get(&(chunk, section_y))
        } else {
            self.light_sections_local.get(&(chunk, section_y))
        }
    }

    pub fn light_sections_for_chunk(&self, ground: bool, chunk: ChunkKey) -> Vec<(i32, Cell)> {
        let source = if ground {
            &self.light_sections_ground
        } else {
            &self.light_sections_local
        };
        let mut sections = source
            .iter()
            .filter_map(|((key, y), cell)| (*key == chunk).then_some((*y, cell.clone())))
            .collect::<Vec<_>>();
        sections.sort_by_key(|(y, _)| *y);
        sections
    }

    pub fn install_light_section_phase(
        &mut self,
        ground: bool,
        chunk: ChunkKey,
        section_y: i32,
        cell: Option<Cell>,
    ) {
        let source = if ground {
            &mut self.light_sections_ground
        } else {
            &mut self.light_sections_local
        };
        if let Some(cell) = cell {
            source.insert((chunk, section_y), cell);
        } else {
            source.remove(&(chunk, section_y));
        }
    }

    pub fn ground_entity_bindings(
        &self,
    ) -> BTreeMap<
        pumpkin_cluster::protocol::GlobalEntityId,
        (ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration),
    > {
        self.entity_bodies_ground
            .iter()
            .filter_map(|(id, (chunk, cell))| {
                let Value::Entity(body) = &cell.value else {
                    return None;
                };
                Some((*id, (*chunk, body.owner)))
            })
            .collect()
    }

    pub fn entity_birth_matches(
        &self,
        id: pumpkin_cluster::protocol::GlobalEntityId,
        birth: &pumpkin_cluster::entity_body::EntityBirth,
    ) -> bool {
        self.entity_births
            .get(&id)
            .is_none_or(|known| known == birth)
            && self
                .entity_birth_ids
                .get(&birth.uuid)
                .is_none_or(|known| *known == id)
    }

    pub fn record_ground_entity_action(
        &mut self,
        action: pumpkin_cluster::canonical_action::ActionRef,
        writes: &[pumpkin_cluster::canonical_action::AppliedWrite],
    ) {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target};
        let mut events =
            BTreeMap::<pumpkin_cluster::protocol::GlobalEntityId, EntityGroundEvent>::new();
        for write in writes {
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } = write.target
            else {
                continue;
            };
            for (before, cell) in [(true, write.before.as_ref()), (false, write.after.as_ref())] {
                let Some(Cell {
                    value: Value::Entity(body),
                    ..
                }) = cell
                else {
                    continue;
                };
                let event = events.entry(id).or_insert_with(|| EntityGroundEvent {
                    action,
                    birth: body.birth.clone(),
                    before: None,
                    after: None,
                });
                assert_eq!(event.birth, body.birth);
                let slot = if before {
                    &mut event.before
                } else {
                    &mut event.after
                };
                assert!(slot.is_none_or(|previous| previous == (key, body.owner)));
                *slot = Some((key, body.owner));
            }
        }
        for (id, event) in events {
            if event.before == event.after {
                continue;
            }
            let history = self.entity_ground_history.entry(id).or_default();
            if let Some(previous) = history
                .iter_mut()
                .find(|previous| previous.action == action)
            {
                assert_eq!(previous.birth, event.birth);
                for (old, new) in [
                    (&mut previous.before, event.before),
                    (&mut previous.after, event.after),
                ] {
                    if let Some(new) = new {
                        assert!(old.is_none_or(|old| old == new));
                        *old = Some(new);
                    }
                }
            } else {
                history.push(event);
            }
        }
    }

    pub fn entity_ground_history(
        &self,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> &[EntityGroundEvent] {
        self.entity_ground_history
            .get(&id)
            .map_or(&[], Vec::as_slice)
    }

    fn entity_proof_frames<'a>(
        &'a self,
        frames: impl IntoIterator<Item = &'a pumpkin_cluster::entity_history::EntityHistoryEvidence>,
    ) -> Option<Vec<&'a pumpkin_cluster::entity_history::EntityHistoryEvidence>> {
        let mut result = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut references = std::collections::BTreeSet::new();
        let mut actions = 0usize;
        for frame in frames {
            if !seen.insert(frame as *const _ as usize) {
                continue;
            }
            actions = frame
                .graphs
                .iter()
                .try_fold(actions, |count, graph| count.checked_add(graph.nodes.len()))?;
            if result.len() >= 128 || actions > 8192 {
                return None;
            }
            references.extend(frame.references());
            result.push(frame);
        }
        let retained = references
            .iter()
            .filter_map(|reference| {
                self.entity_ground_evidence
                    .get(reference)
                    .map(|frame| frame.as_ref())
            })
            .collect::<Vec<_>>();
        for frame in retained {
            if !seen.insert(frame as *const _ as usize) {
                continue;
            }
            actions = frame
                .graphs
                .iter()
                .try_fold(actions, |count, graph| count.checked_add(graph.nodes.len()))?;
            if result.len() >= 128 || actions > 8192 {
                return None;
            }
            references.extend(frame.references());
            result.push(frame);
        }
        let mut index = 0;
        while index < result.len() {
            let frame = result[index];
            index += 1;
            for reference in frame
                .graphs
                .iter()
                .flat_map(|graph| &graph.nodes)
                .flat_map(|node| node.actor_binding.iter())
                .flat_map(|binding| &binding.transitions)
            {
                if references.contains(reference) {
                    continue;
                }
                let dependency = self.entity_ground_evidence.get(reference)?.as_ref();
                if !seen.insert(dependency as *const _ as usize) {
                    return None;
                }
                actions = dependency
                    .graphs
                    .iter()
                    .try_fold(actions, |count, graph| count.checked_add(graph.nodes.len()))?;
                if result.len() >= 128 || actions > 8192 {
                    return None;
                }
                references.extend(dependency.references());
                result.push(dependency);
            }
        }
        Some(result)
    }

    pub fn retain_entity_ground_evidence(
        &mut self,
        references: &[pumpkin_cluster::canonical_action::ActionRef],
        evidence: std::sync::Arc<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
    ) -> bool {
        if references
            .iter()
            .any(|reference| !evidence.references().contains(reference))
        {
            return false;
        }
        self.retain_entity_ground_frames(vec![evidence])
    }

    pub fn retain_entity_ground_frames(
        &mut self,
        frames: Vec<std::sync::Arc<pumpkin_cluster::entity_history::EntityHistoryEvidence>>,
    ) -> bool {
        let Some(proof_frames) =
            self.entity_proof_frames(frames.iter().map(|frame| frame.as_ref()))
        else {
            return false;
        };
        let Ok(proofs) =
            pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(proof_frames)
        else {
            return false;
        };
        let mut births = BTreeMap::new();
        let mut uuids = BTreeMap::new();
        let mut events = BTreeMap::new();
        for frame in &frames {
            let Ok(frame_births) = frame.births() else {
                return false;
            };
            for (id, birth) in frame_births {
                if !self.entity_birth_matches(id, &birth)
                    || births
                        .insert(id, birth.clone())
                        .is_some_and(|previous| previous != birth)
                    || uuids
                        .insert(birth.uuid, id)
                        .is_some_and(|previous| previous != id)
                {
                    return false;
                }
            }
            let Ok(frame_events) = frame.fixed_departures_with_context(Some(&proofs)) else {
                return false;
            };
            for (id, event) in frame_events {
                if self
                    .fixed_entity_ground_history
                    .get(&id)
                    .is_some_and(|history| {
                        history
                            .iter()
                            .any(|old| old.action == event.action && *old != event)
                    })
                    || events
                        .insert((id, event.action), event.clone())
                        .is_some_and(|old| old != event)
                {
                    return false;
                }
            }
        }
        for (id, birth) in births {
            if self.unit_chunks.is_some() { self.unit_births.insert(id); }
            self.entity_birth_ids.entry(birth.uuid).or_insert(id);
            self.entity_births.entry(id).or_insert(birth);
        }
        for ((id, _), event) in events {
            let history = self.fixed_entity_ground_history.entry(id).or_default();
            if !history
                .iter()
                .any(|previous| previous.action == event.action)
            {
                history.push(event);
            }
        }
        for frame in frames {
            for reference in frame.references() {
                self.entity_ground_evidence.insert(reference, frame.clone());
                if self.unit_chunks.is_some() { self.unit_evidence.insert(reference); }
            }
        }
        true
    }

    pub fn accepts_entity_history(
        &self,
        frames: &[pumpkin_cluster::entity_history::EntityHistoryEvidence],
        bodies: &[(pumpkin_cluster::canonical_action::Target, Option<Cell>)],
    ) -> bool {
        let Some(proof_frames) = self.entity_proof_frames(frames) else {
            return false;
        };
        let Ok(proofs) =
            pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(proof_frames)
        else {
            return false;
        };
        let mut incoming = BTreeMap::new();
        let mut births = BTreeMap::new();
        let mut uuids = BTreeMap::new();
        for frame in frames {
            let Ok(frame_births) = frame.births() else {
                return false;
            };
            for (id, birth) in frame_births {
                if !self.entity_birth_matches(id, &birth)
                    || births
                        .insert(id, birth.clone())
                        .is_some_and(|previous| previous != birth)
                    || uuids
                        .insert(birth.uuid, id)
                        .is_some_and(|previous| previous != id)
                {
                    return false;
                }
            }
            let Ok(events) = frame.fixed_departures_with_context(Some(&proofs)) else {
                return false;
            };
            for (id, event) in events {
                if self
                    .fixed_entity_ground_history
                    .get(&id)
                    .is_some_and(|history| {
                        history
                            .iter()
                            .any(|previous| previous.action == event.action && *previous != event)
                    })
                    || incoming
                        .insert((id, event.action), event.clone())
                        .is_some_and(|previous| previous != event)
                {
                    return false;
                }
            }
        }
        for (target, cell) in bodies {
            let Some(Cell {
                value: Value::Entity(body),
                ..
            }) = cell
            else {
                continue;
            };
            let pumpkin_cluster::canonical_action::Target::Chunk {
                key,
                part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id),
                inventory: None,
            } = *target
            else {
                return false;
            };
            if !body.belongs_to(id, key)
                || !self.entity_birth_matches(id, &body.birth)
                || births.get(&id).is_some_and(|birth| *birth != body.birth)
                || uuids
                    .get(&body.birth.uuid)
                    .is_some_and(|previous| *previous != id)
            {
                return false;
            }
        }
        true
    }

    pub fn historical_entity_authority(
        &self,
        action: &pumpkin_cluster::canonical_action::Action,
    ) -> Option<(ChunkKey, pumpkin_cluster::realm_completion::PeerGeneration)> {
        let pumpkin_cluster::actor::ActorId::Entity(id) = action.id.actor else {
            return None;
        };
        if let [
            pumpkin_cluster::canonical_action::Effect::MoveEntityBody {
                source:
                    pumpkin_cluster::canonical_action::Target::Chunk {
                        key,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(source_id),
                        inventory: None,
                    },
                authority: Some(witness),
                ..
            },
        ] = action.effects.as_slice()
        {
            if *source_id != id || action.actor_chunk != Some(*key) || action.validate().is_err() {
                return None;
            }
            return self.fixed_entity_ground_history.get(&id)?.iter().find_map(|event| {
                if event.birth != witness.birth || !action.tick.is_after(event.action.tick)
                    || event.before != Some((*key, witness.body_owner)) || event.after == event.before {
                    return None;
                }
                let frame = self.entity_ground_evidence.get(&event.action)?;
                let source = pumpkin_cluster::canonical_action::Target::Chunk {
                    key: *key, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id), inventory: None,
                };
                frame.graphs.iter().flat_map(|graph| &graph.nodes)
                    .filter_map(|node| node.factual.result.as_ref())
                    .flat_map(|applied| &applied.writes)
                    .filter(|write| write.target == source)
                    .filter_map(|write| write.before.as_ref())
                    .any(|cell| matches!(&cell.value, pumpkin_cluster::canonical_action::Value::Entity(body)
                        if body.birth == witness.birth && body.owner == witness.body_owner
                            && (witness.issuer == body.owner || matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Item(_)))))
                    .then_some((*key, witness.body_owner))
            });
        }
        if action.class != pumpkin_cluster::canonical_action::ActionClass::Infallible {
            for condition in &action.conditions {
                if let (
                    pumpkin_cluster::canonical_action::Target::Chunk {
                        key,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(entity),
                        inventory: None,
                    },
                    pumpkin_cluster::canonical_action::Expected::ActorControlled { birth, issuer },
                ) = (&condition.target, &condition.expected)
                {
                    if *entity == id
                        && action.actor_chunk == Some(*key)
                        && action.validate().is_ok()
                        && self
                            .fixed_entity_ground_history
                            .get(&id)
                            .is_some_and(|events| {
                                events.iter().any(|event| {
                                    &event.birth == birth
                                        && action.tick.is_after(event.action.tick)
                                        && event.before == Some((*key, *issuer))
                                        && event.after != event.before
                                })
                            })
                    {
                        return Some((*key, *issuer));
                    }
                }
            }
        }
        let [
            pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem {
                target:
                    pumpkin_cluster::canonical_action::Target::Chunk {
                        key,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(target_id),
                        inventory: None,
                    },
                body_owner: issuer,
                birth,
                change,
                ..
            },
        ] = action.effects.as_slice()
        else {
            return None;
        };
        let self_lifetime = *change
            == pumpkin_cluster::entity_body::DroppedItemChange::AdvanceLifetime
            && action.class == pumpkin_cluster::canonical_action::ActionClass::Infallible
            && action.conditions.is_empty();
        if *target_id != id
            || action.actor_chunk != Some(*key)
            || !self_lifetime
            || action.validate().is_err()
        {
            return None;
        }
        self.fixed_entity_ground_history
            .get(&id)?
            .iter()
            .filter(|event| {
                &event.birth == birth
                    && action.tick.is_after(event.action.tick)
                    && event.before == Some((*key, *issuer))
                    && event.after != event.before
            })
            .min_by_key(|event| action.tick.distance_since(event.action.tick))
            .map(|_| (*key, *issuer))
    }

    pub fn entity_ground_evidence(
        &self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
    ) -> Option<&pumpkin_cluster::entity_history::EntityHistoryEvidence> {
        self.entity_ground_evidence
            .get(&reference)
            .map(std::sync::Arc::as_ref)
    }

    pub fn entity_binding_proofs_for(
        &self,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Result<
        pumpkin_cluster::entity_history::EntityBindingProofContext,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        let roots = self
            .fixed_entity_ground_history
            .get(&id)
            .into_iter()
            .flatten()
            .map(|event| {
                self.entity_ground_evidence
                    .get(&event.action)
                    .map(std::sync::Arc::as_ref)
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
        let frames = self
            .entity_proof_frames(roots)
            .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
        pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(frames)
    }

    pub fn entity_history_for_chunk(
        &self,
        chunk: ChunkKey,
    ) -> Option<Vec<pumpkin_cluster::entity_history::EntityHistoryEvidence>> {
        let mut seen = std::collections::BTreeSet::new();
        let mut frames = Vec::new();
        let mut actions = 0usize;
        for evidence in self.entity_ground_evidence.values() {
            if !seen.insert(std::sync::Arc::as_ptr(evidence) as usize)
                || !evidence
                    .graphs
                    .iter()
                    .flat_map(|graph| &graph.nodes)
                    .any(|node| {
                        node.action.stakeholders.contains(
                            &pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
                        ) || node.actor_binding.as_ref().is_some_and(|binding|
                            matches!(binding.resolved_source,
                                pumpkin_cluster::canonical_action::Target::Chunk { key, .. } if key == chunk))
                    })
            {
                continue;
            }
            actions = evidence
                .graphs
                .iter()
                .try_fold(actions, |count, graph| count.checked_add(graph.nodes.len()))?;
            if frames.len() >= 128 || actions > 8192 {
                return None;
            }
            frames.push(evidence.as_ref().clone());
        }
        let complete = self.entity_proof_frames(frames.iter())?;
        pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(
            complete.iter().copied(),
        )
        .ok()?;
        Some(complete.into_iter().cloned().collect())
    }

    pub fn entity_body(
        &self,
        ground: bool,
        chunk: ChunkKey,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Option<&Cell> {
        let cells = if ground {
            &self.entity_bodies_ground
        } else {
            &self.entity_bodies_local
        };
        let (held, cell) = cells.get(&id)?;
        (*held == chunk).then_some(cell)
    }

    pub fn entity_body_shared(&self, ground: bool, id: pumpkin_cluster::protocol::GlobalEntityId) -> Option<std::sync::Arc<(ChunkKey,Cell)>> {
        if ground {self.entity_bodies_ground.get_shared(&id)} else {self.entity_bodies_local.get_shared(&id)}
    }

    pub fn entity_body_chunk(
        &self,
        ground: bool,
        id: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Option<ChunkKey> {
        let cells = if ground {
            &self.entity_bodies_ground
        } else {
            &self.entity_bodies_local
        };
        cells.get(&id).map(|(chunk, _)| *chunk)
    }

    pub fn local_entity_bodies(&self) -> impl Iterator<Item = (ChunkKey, &Cell)> {
        self.entity_bodies_local
            .values()
            .map(|(chunk, cell)| (*chunk, cell))
    }

    pub fn take_entity_changes(&mut self) -> StructuralMap<pumpkin_cluster::protocol::GlobalEntityId, (Option<ChunkKey>, Option<ChunkKey>)> {
        std::mem::take(&mut self.entity_changes)
    }

    pub fn local_entity_revision(&self) -> u64 {
        self.local_entity_revision
    }

    pub fn install_entity_body_phase(
        &mut self,
        ground: bool,
        chunk: ChunkKey,
        id: pumpkin_cluster::protocol::GlobalEntityId,
        cell: Option<Cell>,
    ) {
        if !ground {
            self.local_entity_revision = self.local_entity_revision.wrapping_add(1);
        }
        if let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = &cell
        {
            assert!(self.entity_birth_matches(id, &body.birth));
            if self.unit_chunks.is_some() { self.unit_births.insert(id); }
            self.entity_births
                .entry(id)
                .or_insert_with(|| body.birth.clone());
            self.entity_birth_ids.entry(body.birth.uuid).or_insert(id);
        }
        let (cells, index) = if ground {
            (&mut self.entity_bodies_ground, &mut self.entities_ground_by_chunk)
        } else {
            (&mut self.entity_bodies_local, &mut self.entities_local_by_chunk)
        };
        let previous = cells.get(&id).map(|(key, _)| *key);
        if !ground && (cell.is_some() || previous == Some(chunk)) {
            let after = cell.as_ref().map(|_| chunk);
            self.entity_changes.entry(id).or_insert((previous, after)).1 = after;
        }
        if let Some(cell) = cell {
            if let Some(previous) = previous.filter(|previous| *previous != chunk) {
                if let Some(ids) = index.get_mut(&previous) { ids.remove(&id); }
            }
            index.entry(chunk).or_default().insert(id, ());
            debug_assert!(matches!(&cell.value, Value::Entity(body) if body.belongs_to(id, chunk)));
            cells.insert(id, (chunk, cell));
        } else if cells.get(&id).is_some_and(|(held, _)| *held == chunk) {
            cells.remove(&id);
            if let Some(ids) = index.get_mut(&chunk) { ids.remove(&id); }
        }
    }

    pub fn block_entities_for_chunk(&self, ground: bool, chunk: ChunkKey) -> Vec<(BlockPos, Cell)> {
        let cells = if ground {
            &self.block_entities_ground
        } else {
            &self.block_entities_local
        };
        let mut out = cells
            .iter()
            .filter_map(|((key, position), cell)| {
                (*key == chunk).then_some((*position, cell.clone()))
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|(position, _)| (position.x, position.y, position.z));
        out
    }

    pub fn install_block_entity_phase(
        &mut self,
        ground: bool,
        chunk: ChunkKey,
        position: BlockPos,
        cell: Option<Cell>,
    ) {
        let cells = if ground {
            &mut self.block_entities_ground
        } else {
            &mut self.block_entities_local
        };
        if let Some(cell) = cell {
            cells.insert((chunk, position), cell);
        } else {
            cells.remove(&(chunk, position));
        }
    }

    pub fn install_scheduled_phase(&mut self, ground: bool, chunk: ChunkKey, cell: Cell) {
        debug_assert!(matches!(cell.value, Value::ScheduledQueue(_)));
        if ground {
            self.scheduled_ground.insert(chunk, cell);
        } else {
            self.scheduled_local.insert(chunk, cell);
        }
    }

    pub fn ground_block(
        &self,
        chunk: ChunkKey,
        position: BlockPos,
    ) -> Result<BlockStateId, BlockTransactionError> {
        self.ground
            .get(&chunk)
            .ok_or(BlockTransactionError::MissingChunk)?
            .block(chunk, position)
    }

    pub fn prepare_local(
        &self,
        changes: &[BlockChange],
    ) -> Result<PreparedBlockMutation, BlockTransactionError> {
        self.validate(changes)?;
        Ok(PreparedBlockMutation {
            changes: changes.to_vec(),
            inverse: changes
                .iter()
                .map(|change| BlockChange {
                    before: change.after,
                    after: change.before,
                    ..*change
                })
                .collect(),
        })
    }

    pub fn commit_local(
        &mut self,
        prepared: PreparedBlockMutation,
    ) -> Result<Vec<BlockChange>, BlockTransactionError> {
        self.apply(&prepared.changes)?;
        Ok(prepared.inverse)
    }

    pub fn promote_ground(&mut self, changes: &[BlockChange]) -> Result<(), BlockTransactionError> {
        self.validate_ground(changes)?;
        for change in changes {
            let blocks = self
                .ground
                .get_mut(&change.chunk)
                .ok_or(BlockTransactionError::MissingChunk)?;
            let (section, x, y, z) = blocks.indices(change.chunk, change.position)?;
            std::sync::Arc::make_mut(&mut blocks.sections)[section].set(x, y, z, change.after);
            blocks.revision = blocks.revision.wrapping_add(1);
        }
        Ok(())
    }

    pub fn validate_ground(&self, changes: &[BlockChange]) -> Result<(), BlockTransactionError> {
        let mut seen = HashSet::new();
        for change in changes {
            if !seen.insert((change.chunk, change.position)) {
                return Err(BlockTransactionError::DuplicatePosition);
            }
            if self.ground_block(change.chunk, change.position)? != change.before {
                return Err(BlockTransactionError::StaleState);
            }
        }
        Ok(())
    }

    pub fn snapshot(&self, chunk: ChunkKey) -> Option<std::sync::Arc<OwnedChunkBlocks>> {
        self.chunks.get_shared(&chunk)
    }

    pub fn owned_chunks(&self) -> impl Iterator<Item = ChunkKey> + '_ {
        self.chunks.keys().copied()
    }

    pub fn ground_snapshot(&self, chunk: ChunkKey) -> Option<std::sync::Arc<OwnedChunkBlocks>> {
        self.ground.get_shared(&chunk)
    }

    pub fn remove(&mut self, chunk: ChunkKey) {
        if self.entities_local_by_chunk.get(&chunk).is_some_and(|ids| !ids.is_empty()) {
            self.local_entity_revision = self.local_entity_revision.wrapping_add(1);
        }
        self.genesis.remove(&chunk);
        self.chunks.remove(&chunk);
        self.ground.remove(&chunk);
        self.scheduled_local.remove(&chunk);
        self.scheduled_ground.remove(&chunk);
        self.occupancy_local.remove(&chunk);
        self.occupancy_ground.remove(&chunk);
        let keys = self.block_entities_local.range((chunk, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(chunk, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.block_entities_local.remove(&key); }
        let keys = self.block_entities_ground.range((chunk, BlockPos {x:i32::MIN,y:i32::MIN,z:i32::MIN})..=(chunk, BlockPos {x:i32::MAX,y:i32::MAX,z:i32::MAX})).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.block_entities_ground.remove(&key); }
        let keys = self.light_sections_local.range((chunk, i32::MIN)..=(chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.light_sections_local.remove(&key); }
        let keys = self.light_sections_ground.range((chunk, i32::MIN)..=(chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.light_sections_ground.remove(&key); }
        let keys = self.block_sections_local.range((chunk, i32::MIN)..=(chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.block_sections_local.remove(&key); }
        let keys = self.block_sections_ground.range((chunk, i32::MIN)..=(chunk, i32::MAX)).map(|(key,_)| *key).collect::<Vec<_>>();
        for key in keys { self.block_sections_ground.remove(&key); }
        if let Some(ids) = self.entities_local_by_chunk.take_shared(&chunk) {
            for id in ids.keys() {
                self.entity_changes.entry(*id).or_insert((Some(chunk),None)).1 = None;
                self.entity_bodies_local.remove(id);
            }
        }
        if let Some(ids) = self.entities_ground_by_chunk.take_shared(&chunk) {
            for id in ids.keys() { self.entity_bodies_ground.remove(id); }
        }
    }

    pub fn block(
        &self,
        chunk: ChunkKey,
        position: BlockPos,
    ) -> Result<BlockStateId, BlockTransactionError> {
        self.chunks
            .get(&chunk)
            .ok_or(BlockTransactionError::MissingChunk)?
            .block(chunk, position)
    }

    pub fn rough_biome_id(&self, chunk: ChunkKey, position: BlockPos) -> Option<u8> {
        self.chunks.get(&chunk)?.rough_biome_id(position)
    }

    pub fn rough_ground_biome_id(&self, chunk: ChunkKey, position: BlockPos) -> Option<u8> {
        self.ground.get(&chunk)?.rough_biome_id(position)
    }

    pub fn validate(&self, changes: &[BlockChange]) -> Result<(), BlockTransactionError> {
        let mut seen = HashSet::new();
        for change in changes {
            if !seen.insert((change.chunk, change.position)) {
                return Err(BlockTransactionError::DuplicatePosition);
            }
            if self.block(change.chunk, change.position)? != change.before {
                return Err(BlockTransactionError::StaleState);
            }
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        changes: &[BlockChange],
    ) -> Result<Vec<BlockChange>, BlockTransactionError> {
        self.validate(changes)?;
        for change in changes {
            let chunk = self
                .chunks
                .get_mut(&change.chunk)
                .ok_or(BlockTransactionError::MissingChunk)?;
            let (section, x, y, z) = chunk.indices(change.chunk, change.position)?;
            std::sync::Arc::make_mut(&mut chunk.sections)[section].set(x, y, z, change.after);
            chunk.revision = chunk.revision.wrapping_add(1);
        }
        Ok(changes
            .iter()
            .map(|change| BlockChange {
                before: change.after,
                after: change.before,
                ..*change
            })
            .collect())
    }
}

pub(crate) fn published_block(
    server: &super::Server,
    chunk: ChunkKey,
    position: BlockPos,
) -> Result<BlockStateId, BlockTransactionError> {
    server
        .cluster_owned_chunk_blocks
        .load()
        .get(&chunk)
        .ok_or(BlockTransactionError::MissingChunk)?
        .block(chunk, position)
}

pub(crate) fn published_biome_id(
    server: &super::Server,
    chunk: ChunkKey,
    position: BlockPos,
) -> Option<u8> {
    server
        .cluster_owned_chunk_blocks
        .load()
        .get(&chunk)?
        .rough_biome_id(position)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::WorldId;

    #[test]
    fn entity_deletion_preserves_birth_and_uuid_reservation() {
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::canonical_action::{ActionId, ActionRef};
        use pumpkin_cluster::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial};
        use pumpkin_cluster::identity::ServerId;
        use pumpkin_cluster::realm_completion::PeerGeneration;
        let key = chunk(0);
        let id = pumpkin_cluster::protocol::GlobalEntityId {
            origin: ServerId(1),
            local_id: 42,
            generation: 3,
        };
        let peer = PeerGeneration {
            server: id.origin,
            generation: 12,
        };
        let birth = EntityBirth {
            source: peer,
            uuid: uuid::Uuid::from_u128(42),
            action: ActionRef {
                tick: TickStamp(4),
                id: ActionId {
                    actor: ActorId::Peer(id.origin),
                    incarnation: 12,
                    sequence: 1,
                    family: 8,
                },
            },
        };
        let cell = Cell {
            revision: 1,
            value: Value::Entity((EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 0,
                relation_versions: Default::default(),
                passengers: Default::default(),
                id,
                birth: birth.clone(),
                spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(
                    birth.action,
                ),
                owner: peer,
                kind: pumpkin_data::entity::EntityType::EXPERIENCE_ORB.id,
                spatial: EntitySpatial {
                    world: key.world,
                    position: [1.0_f64.to_bits(), 64.0_f64.to_bits(), 1.0_f64.to_bits()],
                    velocity_micro: [0; 3],
                    yaw: 0,
                    pitch: 0,
                    on_ground: false,
                },
                payload: EntityPayload::Experience(
                    pumpkin_cluster::entity_body::ExperienceOrbState {
                        amount: 3,
                        count: 1,
                        health_milli: 5_000,
                        age: 0,
                        last_lifetime_tick: None,
                    },
                ),
            }).into()),
        };
        let mut store = OwnedChunkStore::default();
        store.install(key, empty());
        store.install_entity_body_phase(false, key, id, Some(cell.clone()));
        assert!(store.ground_entity_bindings().is_empty());
        store.install_entity_body_phase(true, key, id, Some(cell.clone()));
        let chunks = std::collections::BTreeSet::from([key]);
        assert_eq!(store.preflight_chunk_unit(&chunks, &[]), Err(BlockTransactionError::OutsideChunk));
        assert!(store.contains(key));
        assert_eq!(store.chunk_unit_entity_ids(&chunks), std::collections::BTreeSet::from([id]));
        let unit = store.take_chunk_unit(&chunks, &[id]).unwrap();
        assert!(!store.contains(key));
        assert_eq!(unit.chunk_unit_entity_ids(&chunks), std::collections::BTreeSet::from([id]));
        assert!(store.return_chunk_unit(unit).is_ok());
        let target = pumpkin_cluster::canonical_action::Target::Chunk {
            key,
            part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id),
            inventory: None,
        };
        store.record_ground_entity_action(
            birth.action,
            &[pumpkin_cluster::canonical_action::AppliedWrite {
                target,
                before: None,
                after: Some(cell.clone()),
            }],
        );
        assert_eq!(store.ground_entity_bindings()[&id], (key, peer));
        store.install_entity_body_phase(false, key, id, None);
        store.install_entity_body_phase(true, key, id, None);
        let deleted = ActionRef {
            tick: TickStamp(5),
            id: ActionId {
                sequence: 2,
                ..birth.action.id
            },
        };
        let writes = [pumpkin_cluster::canonical_action::AppliedWrite {
            target,
            before: Some(cell),
            after: None,
        }];
        store.record_ground_entity_action(deleted, &writes);
        store.record_ground_entity_action(deleted, &writes);
        assert_eq!(store.entity_ground_history(id).len(), 2);
        assert_eq!(
            store.entity_ground_history(id)[1],
            EntityGroundEvent {
                action: deleted,
                birth: birth.clone(),
                before: Some((key, peer)),
                after: None,
            }
        );
        let mut lifetime = pumpkin_cluster::canonical_action::Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: u64::from(id.generation),
                sequence: 3,
                family: 8,
            },
            tick: TickStamp(6),
            actor_chunk: Some(key),
            chunk_transition: false,
            class: pumpkin_cluster::canonical_action::ActionClass::Infallible,
            kind: pumpkin_cluster::canonical_action::ActionKind::Item,
            stakeholders: vec![pumpkin_cluster::canonical_action::Stakeholder::Chunk(key)],
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            conditions: Vec::new(),
            effects: vec![
                pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem {
                    target,
                    issuer: peer,
                    body_owner: peer,
                    birth: birth.clone(),
                    change: pumpkin_cluster::entity_body::DroppedItemChange::AdvanceLifetime,
                },
            ],
            undo: vec![pumpkin_cluster::canonical_action::UndoProcedure::RestoreWrittenTargets],
            presentation: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            dependencies: Vec::new(),
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            client_origin: pumpkin_cluster::canonical_action::ClientOrigin::None,
        };
        assert_eq!(store.historical_entity_authority(&lifetime), None);
        let mut deletion = lifetime.clone();
        deletion.id = deleted.id;
        deletion.tick = deleted.tick;
        deletion.class = pumpkin_cluster::canonical_action::ActionClass::Undoable;
        deletion.kind = pumpkin_cluster::canonical_action::ActionKind::World;
        deletion.conditions = vec![pumpkin_cluster::canonical_action::Condition {
            target,
            expected: pumpkin_cluster::canonical_action::Expected::Exact(
                writes[0].before.clone().unwrap(),
            ),
        }];
        deletion.effects = vec![pumpkin_cluster::canonical_action::Effect::Delete { target }];
        let mut ledger = pumpkin_cluster::realm_completion::RealmCompletionLedger::default();
        let requirement = pumpkin_cluster::realm_completion::RequirementKey::chunk(
            key,
            &std::collections::BTreeSet::from([peer]),
        );
        ledger
            .capture(
                deleted.tick,
                requirement,
                std::collections::BTreeSet::from([peer]),
            )
            .unwrap();
        ledger
            .capture_actor_owner(deleted.tick, deleted.id.actor, peer)
            .unwrap();
        ledger
            .note_action(
                peer,
                &deletion,
                std::collections::BTreeSet::from([requirement]),
            )
            .unwrap();
        ledger
            .note_finish(
                peer,
                pumpkin_cluster::realm_completion::HolisticFinish {
                    full_source_slot: u64::from((deleted.tick).0),
                    acquisition_prefix: None,
                    tick: deleted.tick,
                    issued: BTreeMap::from([(
                        requirement,
                        std::collections::BTreeSet::from([deleted.id]),
                    )]),
                },
            )
            .unwrap();
        let mut graph = pumpkin_cluster::action_graph::TickActionGraph::default();
        graph.insert(deletion.clone()).unwrap();
        let evidence =
            std::sync::Arc::new(pumpkin_cluster::entity_history::EntityHistoryEvidence {
                snapshot_cut: deleted.tick,
                graphs: vec![pumpkin_cluster::action_graph::TickGraphSnapshot {
                    tick: deleted.tick,
                    nodes: vec![pumpkin_cluster::action_graph::GraphNodeSnapshot {
                        actor_binding: None,
                        action: deletion.clone(),
                        factual: pumpkin_cluster::action_graph::FactualVariant {
                            conditions: deletion.conditions.clone(),
                            effects: deletion.effects.clone(),
                            result: Some(pumpkin_cluster::canonical_action::AppliedAction {
                                id: deleted.id,
                                tick: deleted.tick,
                                view: pumpkin_cluster::canonical_action::TruthView::Local,
                                actor_chunk: deletion.actor_chunk,
                                writes: writes.to_vec(),
                            }),
                        },
                        wrapper: pumpkin_cluster::action_graph::SemanticWrapper::Applied,
                        semantic_order: Some(0),
                        required_realms: graph.actions[&deleted.id].realms.clone(),
                        applied_realms: graph.actions[&deleted.id].realms.clone(),
                    }],
                }],
                anchors: vec![(
                    target,
                    pumpkin_cluster::action_graph::GroundAnchor {
                        settled_through: TickStamp(4),
                        value: writes[0].before.clone(),
                    },
                )],
                completion: ledger
                    .snapshot_for_actions(
                        deleted.tick,
                        &std::collections::BTreeSet::from([deleted]),
                    )
                    .unwrap(),
            });
        let mut receiver = OwnedChunkStore::default();
        assert!(receiver.accepts_entity_history(&[evidence.as_ref().clone()], &[]));
        let unrelated = pumpkin_cluster::canonical_action::ActionRef {
            tick: TickStamp(deleted.tick.0.wrapping_add(1)),
            ..deleted
        };
        assert!(!receiver.retain_entity_ground_evidence(&[unrelated], evidence.clone()));
        assert_eq!(receiver.historical_entity_authority(&lifetime), None);
        assert!(receiver.retain_entity_ground_evidence(&[deleted], evidence.clone()));
        let mut contradictory = evidence.as_ref().clone();
        contradictory.graphs[0].nodes[0].action.client_origin =
            pumpkin_cluster::canonical_action::ClientOrigin::JavaBlockSequence(91);
        assert!(contradictory.validate().is_ok());
        assert!(!receiver.retain_entity_ground_frames(vec![std::sync::Arc::new(contradictory)]));
        assert_eq!(
            receiver.historical_entity_authority(&lifetime),
            Some((key, peer))
        );
        let mut guarded_world = lifetime.clone();
        guarded_world.class = pumpkin_cluster::canonical_action::ActionClass::Undoable;
        guarded_world.kind = pumpkin_cluster::canonical_action::ActionKind::World;
        let block_target = pumpkin_cluster::canonical_action::Target::Chunk {
            key,
            part: pumpkin_cluster::canonical_action::ChunkPart::Block(
                pumpkin_cluster::protocol::BlockPos { x: 1, y: 80, z: 1 },
            ),
            inventory: None,
        };
        guarded_world.conditions = vec![
            pumpkin_cluster::canonical_action::Condition {
                target,
                expected: pumpkin_cluster::canonical_action::Expected::ActorControlled {
                    birth: birth.clone(),
                    issuer: peer,
                },
            },
            pumpkin_cluster::canonical_action::Condition {
                target: block_target,
                expected: pumpkin_cluster::canonical_action::Expected::BlockState(0),
            },
        ];
        guarded_world.effects = vec![pumpkin_cluster::canonical_action::Effect::Put {
            target: block_target,
            value: pumpkin_cluster::canonical_action::Value::Block(1),
        }];
        let proof_context =
            pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames([
                evidence.as_ref(),
                evidence.as_ref(),
            ])
            .unwrap();
        assert_eq!(
            proof_context.binding_candidate(&guarded_world).unwrap(),
            None
        );
        assert!(
            pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(
                std::iter::repeat_n(evidence.as_ref(), 129),
            )
            .is_err()
        );
        assert_eq!(
            receiver.historical_entity_authority(&guarded_world),
            Some((key, peer))
        );
        guarded_world.conditions.remove(0);
        assert_eq!(receiver.historical_entity_authority(&guarded_world), None);
        let mut wrong_birth = birth.clone();
        wrong_birth.uuid = uuid::Uuid::from_u128(43);
        assert!(!receiver.entity_birth_matches(id, &wrong_birth));
        assert!(store.retain_entity_ground_evidence(&[deleted], evidence));
        assert_eq!(
            store.historical_entity_authority(&lifetime),
            Some((key, peer))
        );
        lifetime.tick = deleted.tick;
        assert_eq!(store.historical_entity_authority(&lifetime), None);
        lifetime.tick = TickStamp(6);
        let pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem { issuer, .. } =
            &mut lifetime.effects[0]
        else {
            unreachable!();
        };
        issuer.generation += 1;
        assert_eq!(store.historical_entity_authority(&lifetime), None);
        assert!(store.ground_entity_bindings().is_empty());
        assert!(store.entity_birth_matches(id, &birth));
        let mut changed = birth.clone();
        changed.uuid = uuid::Uuid::from_u128(43);
        assert!(!store.entity_birth_matches(id, &changed));
        let other_id = pumpkin_cluster::protocol::GlobalEntityId { local_id: 43, ..id };
        assert!(!store.entity_birth_matches(other_id, &birth));
        let copy = store.clone_subset(&std::collections::BTreeSet::from([key]));
        assert_eq!(
            copy.entity_ground_history(id),
            store.entity_ground_history(id)
        );
        assert!(!copy.entity_birth_matches(other_id, &birth));
        store.remove(key);
        assert!(!store.entity_birth_matches(id, &changed));
    }

    #[test]
    fn frozen_actor_palettes_survive_local_commit_and_ground_promotion() {
        let key = chunk(0);
        let mut store = OwnedChunkStore::default();
        store.install(key, empty());
        let position = BlockPos { x: 1, y: 64, z: 1 };
        let before = store.block(key, position).unwrap();
        let frozen = store.callback_subset(&std::collections::BTreeSet::from([key]));
        let snapshot = store.snapshot(key).unwrap();
        let frozen_palette = frozen.snapshot(key).unwrap();
        assert!(std::sync::Arc::ptr_eq(&snapshot.sections, &frozen_palette.sections));
        let change = BlockChange { chunk: key, position, before, after: pumpkin_data::Block::STONE.default_state.id };
        store.apply(&[change]).unwrap();
        store.promote_ground(&[change]).unwrap();
        assert_eq!(frozen.block(key, position).unwrap(), before);
        assert_eq!(store.block(key, position).unwrap(), change.after);
        assert_eq!(store.ground_block(key, position).unwrap(), change.after);
        assert_eq!(snapshot.block(key, position).unwrap(), before);
    }

    #[test]
    fn scheduled_queue_keeps_local_and_ground_phases_separate() {
        let key = chunk(0);
        let mut store = OwnedChunkStore::default();
        store.install(key, empty());
        let genesis = Cell {
            revision: 0,
            value: Value::ScheduledQueue(Vec::new()),
        };
        assert!(store.install_scheduled_queue(key, genesis.clone()));
        let mut local = genesis.clone();
        local.revision = 1;
        local.value =
            Value::ScheduledQueue(vec![pumpkin_cluster::canonical_action::ScheduledTickFact {
                origin: pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary,
                executor: pumpkin_cluster::realm_completion::PeerGeneration {
                    server: pumpkin_cluster::identity::ServerId(0),
                    generation: 1,
                },
                kind: pumpkin_cluster::canonical_action::ScheduledTickKind::Block,
                id: 1,
                position: BlockPos { x: 1, y: 64, z: 1 },
                due: pumpkin_cluster::time::TickStamp(12),
                priority: 0,
                source_order: 5,
            }]);
        store.install_scheduled_phase(false, key, local.clone());
        assert_eq!(store.scheduled_queue(false, key), Some(&local));
        assert_eq!(store.scheduled_queue(true, key), Some(&genesis));
        let subset = store.clone_subset(&std::collections::BTreeSet::from([key]));
        assert_eq!(subset.scheduled_queue(false, key), Some(&local));
        assert_eq!(subset.scheduled_queue(true, key), Some(&genesis));
        store.remove(key);
        assert!(store.scheduled_queue(false, key).is_none());
        assert!(store.scheduled_queue(true, key).is_none());
    }

    #[test]
    fn block_entity_cells_keep_local_and_ground_phases_separate() {
        let key = chunk(0);
        let position = BlockPos { x: 2, y: 64, z: 3 };
        let mut store = OwnedChunkStore::default();
        store.install(key, empty());
        let ground = Cell {
            revision: 3,
            value: Value::BlockEntity(vec![10, 0, 0, 0]),
        };
        let local = Cell {
            revision: 4,
            value: Value::BlockEntity(vec![10, 0, 0, 1]),
        };
        store.install_block_entity_phase(true, key, position, Some(ground.clone()));
        store.install_block_entity_phase(false, key, position, Some(local.clone()));
        assert_eq!(store.block_entity(true, key, position), Some(&ground));
        assert_eq!(store.block_entity(false, key, position), Some(&local));
        assert_eq!(
            store.block_entities_for_chunk(true, key),
            vec![(position, ground)]
        );
        let subset = store.clone_subset(&std::collections::BTreeSet::from([key]));
        assert_eq!(subset.block_entity(false, key, position), Some(&local));
        store.install_block_entity_phase(false, key, position, None);
        assert!(store.block_entity(false, key, position).is_none());
        assert!(store.block_entity(true, key, position).is_some());
        store.remove(key);
        assert!(store.block_entity(true, key, position).is_none());
    }

    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(WorldId::new(0, 7), x, 0)
    }

    fn empty() -> OwnedChunkBlocks {
        OwnedChunkBlocks {
            min_y: -64,
            sections: vec![BlockPalette::default(); 24].into(),
            biomes: vec![BiomePalette::default(); 24].into(),
            revision: 0,
        }
    }

    #[test]
    fn chunk_unit_moves_shared_backing_and_returns_exact_private_changes() {
        let a = chunk(0);
        let b = chunk(1);
        let mut store = OwnedChunkStore::default();
        store.install(a, empty());
        store.install(b, empty());
        let original = store.clone();
        let retained = original.snapshot(a).unwrap();
        let mut unit = store.take_chunk_unit(&std::collections::BTreeSet::from([a]), &[]).unwrap();
        assert!(!store.contains(a));
        assert!(store.contains(b));
        assert!(std::sync::Arc::ptr_eq(&retained, &unit.snapshot(a).unwrap()));
        let position = BlockPos {x:1,y:64,z:1};
        let before = unit.block(a,position).unwrap();
        let after = pumpkin_data::Block::STONE.default_state.id;
        let mutation = unit.prepare_replacement(false, &[BlockChange {chunk:a,position,before,after}]).unwrap();
        unit.install_replacement(mutation);
        assert_eq!(original.block(a,position),Ok(before));
        assert_eq!(store.block(a,position),Err(BlockTransactionError::MissingChunk));
        assert!(store.return_chunk_unit(unit).is_ok());
        assert_eq!(store.block(a,position),Ok(after));
        assert_eq!(store.ground_block(a,position),Ok(before));
        assert_eq!(original.block(a,position),Ok(before));
    }

    #[test]
    fn owned_random_sampling_is_deterministic_and_budgeted() {
        let mut blocks = empty();
        let leaves = pumpkin_data::Block::OAK_LEAVES.default_state.id;
        assert!(pumpkin_data::block_properties::has_random_ticks(leaves));
        for palette in std::sync::Arc::make_mut(&mut blocks.sections) {
            *palette = BlockPalette::Homogeneous(leaves);
        }
        let key = chunk(-1);
        let tick = TickStamp(u16::MAX);
        assert_eq!(
            blocks.sample_random_ticks(key, tick, 4, 95),
            Err(RandomTickSampleError::Budget { required: 96 })
        );
        assert_eq!(
            blocks.sample_random_ticks(key, tick, usize::MAX, usize::MAX),
            Err(RandomTickSampleError::SampleCountOverflow)
        );
        assert_eq!(
            blocks.sample_random_ticks(chunk(i32::MAX), tick, 1, 24),
            Err(RandomTickSampleError::OutsideWorld)
        );
        let mut too_high = blocks.clone();
        too_high.min_y = i32::MAX;
        assert_eq!(
            too_high.sample_random_ticks(key, tick, 1, 24),
            Err(RandomTickSampleError::OutsideWorld)
        );
        let first = blocks.sample_random_ticks(key, tick, 4, 96).unwrap();
        let second = blocks.sample_random_ticks(key, tick, 4, 96).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 96);
        assert!(first.iter().all(|sample| sample.tick_block));
    }

    #[test]
    fn joint_stale_preflight_leaves_all_chunks_unchanged() {
        let mut store = OwnedChunkStore::default();
        store.install(chunk(-1), empty());
        store.install(chunk(0), empty());
        let first = BlockPos { x: -1, y: 64, z: 0 };
        let second = BlockPos { x: 0, y: 64, z: 0 };
        let air = BlockStateId::AIR;
        let stone = pumpkin_data::Block::STONE.default_state.id;
        let changes = [
            BlockChange {
                chunk: chunk(-1),
                position: first,
                before: air,
                after: stone,
            },
            BlockChange {
                chunk: chunk(0),
                position: second,
                before: stone,
                after: air,
            },
        ];
        assert_eq!(
            store.apply(&changes),
            Err(BlockTransactionError::StaleState)
        );
        assert_eq!(store.block(chunk(-1), first), Ok(air));
        assert_eq!(store.block(chunk(0), second), Ok(air));
        let accepted = [
            changes[0],
            BlockChange {
                before: air,
                after: stone,
                ..changes[1]
            },
        ];
        let undo = store.apply(&accepted).expect("accepted joint transaction");
        assert_eq!(store.block(chunk(-1), first), Ok(stone));
        assert_eq!(store.block(chunk(0), second), Ok(stone));
        store.apply(&undo).expect("exact inverse transaction");
        assert_eq!(store.block(chunk(-1), first), Ok(air));
        assert_eq!(store.block(chunk(0), second), Ok(air));
    }

    #[test]
    fn prepared_replacement_installs_only_after_every_chunk_validates() {
        let mut store = OwnedChunkStore::default();
        store.install(chunk(-1), empty());
        store.install(chunk(0), empty());
        let first = BlockPos { x: -1, y: 64, z: 0 };
        let second = BlockPos { x: 0, y: 64, z: 0 };
        let air = BlockStateId::AIR;
        let stone = pumpkin_data::Block::STONE.default_state.id;
        let valid = BlockChange {
            chunk: chunk(-1),
            position: first,
            before: air,
            after: stone,
        };
        let stale = BlockChange {
            chunk: chunk(0),
            position: second,
            before: stone,
            after: air,
        };
        assert!(matches!(
            store.prepare_replacement(true, &[valid, stale]),
            Err(BlockTransactionError::StaleState)
        ));
        assert_eq!(store.ground_block(chunk(-1), first), Ok(air));
        let ground = store.prepare_replacement(true, &[valid]).unwrap();
        let local = store.prepare_replacement(false, &[valid]).unwrap();
        store.install_replacement(ground);
        store.install_replacement(local);
        assert_eq!(store.ground_block(chunk(-1), first), Ok(stone));
        assert_eq!(store.block(chunk(-1), first), Ok(stone));
    }
}
