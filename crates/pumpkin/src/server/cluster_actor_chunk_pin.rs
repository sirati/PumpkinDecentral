use std::sync::Arc;

use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_util::math::vector2::Vector2;

pub(super) struct ActorChunkPins {
    family: u8,
    next: u64,
}

#[derive(Clone)]
pub(super) struct ActorChunkPin {
    lease: Arc<super::cluster::ChunkNeedLease>,
}

impl ActorChunkPins {
    pub(super) fn new(family: u8) -> Option<Self> {
        (family < 8).then_some(Self { family, next: 0 })
    }

    pub(super) fn acquire(
        &mut self,
        server: &super::Server,
        chunks: &[ChunkKey],
    ) -> Result<ActorChunkPin, ApplyFailure> {
        let first = chunks.first().ok_or(ApplyFailure::InvalidAction)?;
        if chunks.len() > 8192
            || chunks.iter().any(|chunk| chunk.world != first.world)
            || self.next >= 1_u64 << 56
        {
            return Err(ApplyFailure::Capacity);
        }
        let worlds = server.worlds.load();
        let world = worlds
            .iter()
            .find(|world| super::cluster_world_delta::world_id(world) == first.world)
            .ok_or(ApplyFailure::InvalidAction)?;
        let positions = chunks
            .iter()
            .map(|chunk| Vector2::new(chunk.x(), chunk.z()))
            .collect::<Vec<_>>();
        let session = (1_u64 << 59) | (u64::from(self.family) << 56) | self.next;
        let lease = super::cluster::try_require_lobby_chunks(server, world, &positions, session)
            .ok_or(ApplyFailure::Capacity)?;
        self.next += 1;
        Ok(ActorChunkPin {
            lease: Arc::new(lease),
        })
    }
}

impl ActorChunkPin {
    pub(super) fn chunks(&self) -> &[ChunkKey] {
        self.lease.chunks()
    }

    pub(super) fn available(&self, chunks: &super::cluster_chunk_state::OwnedChunkStore) -> bool {
        self.chunks().iter().all(|chunk| chunks.contains(*chunk))
    }
}

pub(super) fn section_targets(
    chunks: &[ChunkKey],
    minimum_y: i32,
    maximum_y: i32,
) -> Result<Vec<Target>, ApplyFailure> {
    if maximum_y <= minimum_y
        || chunks
            .len()
            .saturating_mul((maximum_y - minimum_y).div_euclid(16) as usize)
            > 16384
    {
        return Err(ApplyFailure::Capacity);
    }
    Ok(chunks
        .iter()
        .flat_map(|key| {
            (minimum_y.div_euclid(16)..(maximum_y - 1).div_euclid(16) + 1).map(|y| Target::Chunk {
                key: *key,
                part: ChunkPart::BlockSection(y),
                inventory: None,
            })
        })
        .collect())
}
