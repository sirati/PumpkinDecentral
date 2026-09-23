use std::collections::{HashMap, HashSet};

use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_data::BlockStateId;
use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
use pumpkin_world::level::SyncChunk;

#[derive(Clone)]
pub struct OwnedChunkBlocks {
    min_y: i32,
    sections: Box<[BlockPalette]>,
    biomes: Box<[BiomePalette]>,
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

#[derive(Default)]
pub struct OwnedChunkStore {
    chunks: HashMap<ChunkKey, OwnedChunkBlocks>,
    ground: HashMap<ChunkKey, OwnedChunkBlocks>,
}

pub struct PreparedBlockMutation {
    changes: Vec<BlockChange>,
    inverse: Vec<BlockChange>,
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
            sections,
            biomes,
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
                .clone(),
            biomes: chunk
                .section
                .biome_sections
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
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
    pub fn contains(&self, chunk: ChunkKey) -> bool {
        self.chunks.contains_key(&chunk)
    }

    pub fn install(&mut self, chunk: ChunkKey, blocks: OwnedChunkBlocks) {
        self.ground.insert(chunk, blocks.clone());
        self.chunks.insert(chunk, blocks);
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
            blocks.sections[section].set(x, y, z, change.after);
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
        self.chunks.get(&chunk).cloned().map(std::sync::Arc::new)
    }

    pub fn ground_snapshot(&self, chunk: ChunkKey) -> Option<std::sync::Arc<OwnedChunkBlocks>> {
        self.ground.get(&chunk).cloned().map(std::sync::Arc::new)
    }

    pub fn remove(&mut self, chunk: ChunkKey) {
        self.chunks.remove(&chunk);
        self.ground.remove(&chunk);
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
            chunk.sections[section].set(x, y, z, change.after);
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

    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(WorldId::new(0, 7), x, 0)
    }

    fn empty() -> OwnedChunkBlocks {
        OwnedChunkBlocks {
            min_y: -64,
            sections: vec![BlockPalette::default(); 24].into_boxed_slice(),
            biomes: vec![BiomePalette::default(); 24].into_boxed_slice(),
            revision: 0,
        }
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
}
