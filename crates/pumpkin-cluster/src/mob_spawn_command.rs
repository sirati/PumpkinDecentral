use serde::{Deserialize, Serialize};

use crate::entity_origin::EntityOriginIdentity;
use crate::protocol::{BlockPos, ChunkKey};
use crate::world_rules::WorldRulesWitness;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NaturalSpawnCause {
    Natural,
    Spawner(BlockPos),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NaturalMobSpawn {
    pub kind: u16,
    pub identity: EntityOriginIdentity,
    pub position: [u64; 3],
    pub seed: u64,
    pub cause: NaturalSpawnCause,
    pub rules: WorldRulesWitness,
}

impl NaturalMobSpawn {
    pub fn valid(&self) -> bool {
        self.identity.source.generation != 0
            && self.identity.id.origin == self.identity.source.server
            && self.identity.id.generation == self.identity.source.generation
            && !self.identity.uuid.is_nil()
            && self.rules.rules.issuer.generation != 0
            && self.rules.rules.epoch != 0
            && self.position.iter().all(|bits| {
                let coordinate = f64::from_bits(*bits);
                coordinate.is_finite() && coordinate.abs() <= 30_000_000.0
            })
            && match self.cause {
                NaturalSpawnCause::Natural => true,
                NaturalSpawnCause::Spawner(position) =>
                    [position.x, position.y, position.z].iter()
                        .all(|coordinate| coordinate.unsigned_abs() <= 30_000_000),
            }
    }

    pub fn chunk(&self) -> Option<ChunkKey> {
        self.valid().then(|| {
            let position = self.position.map(f64::from_bits);
            ChunkKey::new(self.rules.rules.world, (position[0].floor() as i32).div_euclid(16),
                (position[2].floor() as i32).div_euclid(16))
        })
    }

    pub fn block_position(&self) -> Option<BlockPos> {
        self.valid().then(|| {
            let position = self.position.map(f64::from_bits);
            BlockPos { x: position[0].floor() as i32, y: position[1].floor() as i32,
                z: position[2].floor() as i32 }
        })
    }
}
