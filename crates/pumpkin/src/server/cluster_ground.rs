use std::collections::HashMap;
use std::sync::Arc;

use pumpkin_cluster::protocol::{BlockPos, ChunkKey};

use super::Server;

#[derive(Debug, Clone, Default)]
pub struct GroundChunkSnapshot {
    pub revision: u64,
    pub blocks: HashMap<BlockPos, u16>,
}

pub fn publish_ground_blocks(
    server: &Server,
    chunk: ChunkKey,
    blocks: impl IntoIterator<Item = (BlockPos, u16)>,
) {
    let mut snapshots = (**server.cluster_ground_snapshots.load()).clone();
    let snapshot = snapshots.entry(chunk).or_default();
    snapshot.revision = snapshot.revision.wrapping_add(1);
    snapshot.blocks.extend(blocks);
    server.cluster_ground_snapshots.store(Arc::new(snapshots));
}

#[must_use]
pub fn accepted_ground_block(server: &Server, chunk: ChunkKey, position: BlockPos) -> Option<u16> {
    server
        .cluster_ground_snapshots
        .load()
        .get(&chunk)
        .and_then(|snapshot| snapshot.blocks.get(&position))
        .copied()
}

#[must_use]
pub fn accepted_ground_chunk(server: &Server, chunk: ChunkKey) -> Option<Arc<GroundChunkSnapshot>> {
    server
        .cluster_ground_snapshots
        .load()
        .get(&chunk)
        .cloned()
        .map(Arc::new)
}
