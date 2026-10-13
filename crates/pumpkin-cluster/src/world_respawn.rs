use serde::{Deserialize, Serialize};

use crate::canonical_action::{ApplyFailure, ChunkPart, Target};
use crate::item_action::PlayerRespawnState;
use crate::protocol::{BlockPos, ChunkKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldRespawnPlan {
    pub point: PlayerRespawnState,
    pub border: crate::world_time::WorldBorderWitness,
    pub min_y: i32,
    pub max_y: i32,
    pub half_width: u64,
    pub height: u64,
    pub bed_works: bool,
    pub anchor_works: bool,
    pub config_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldRespawnResolution {
    pub position: [u64; 3],
    pub yaw: u32,
    pub anchor: Option<(Target, u16, u16)>,
}

impl WorldRespawnPlan {
    pub fn valid(self) -> bool {
        let width = f64::from_bits(self.half_width);
        let height = f64::from_bits(self.height);
        self.border.world == self.point.world
            && self.border.issuer.generation != 0
            && self.border.revision != 0
            && self
                .border
                .center
                .iter()
                .all(|value| f64::from_bits(*value).is_finite())
            && f64::from_bits(self.border.diameter).is_finite()
            && f64::from_bits(self.border.diameter) > 0.0
            && self.config_version != 0
            && f32::from_bits(self.point.yaw).is_finite()
            && self.min_y < self.max_y
            && self
                .max_y
                .checked_sub(self.min_y)
                .is_some_and(|height| height <= 4096)
            && (self.min_y..self.max_y).contains(&self.point.position.y)
            && width.is_finite()
            && width > 0.0
            && width <= 8.0
            && height.is_finite()
            && height > 0.0
            && height <= 32.0
            && self.point.position.x.abs_diff(0) <= 30_000_000
            && self.point.position.z.abs_diff(0) <= 30_000_000
    }

    pub fn contains_horizontal_box(self, x: f64, z: f64) -> bool {
        let center = self.border.center.map(f64::from_bits);
        let radius = f64::from_bits(self.border.diameter) / 2.0;
        let half_width = f64::from_bits(self.half_width);
        [x, z].into_iter().enumerate().all(|(axis, coordinate)| {
            coordinate.is_finite()
                && coordinate - half_width >= center[axis] - radius
                && coordinate + half_width <= center[axis] + radius
        })
    }

    pub fn target(self, position: BlockPos) -> Target {
        Target::Chunk {
            key: ChunkKey::new(
                self.point.world,
                position.x.div_euclid(16),
                position.z.div_euclid(16),
            ),
            part: ChunkPart::Block(position),
            inventory: None,
        }
    }

    pub fn targets(self) -> Result<Vec<Target>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let radius = 3 + f64::from_bits(self.half_width).ceil() as i32;
        let min_x = self
            .point
            .position
            .x
            .checked_sub(radius)
            .ok_or(ApplyFailure::InvalidAction)?
            .max(-30_000_000);
        let max_x = self
            .point
            .position
            .x
            .checked_add(radius)
            .ok_or(ApplyFailure::InvalidAction)?
            .min(30_000_000);
        let min_z = self
            .point
            .position
            .z
            .checked_sub(radius)
            .ok_or(ApplyFailure::InvalidAction)?
            .max(-30_000_000);
        let max_z = self
            .point
            .position
            .z
            .checked_add(radius)
            .ok_or(ApplyFailure::InvalidAction)?
            .min(30_000_000);
        let min_y = self
            .point
            .position
            .y
            .checked_sub(2)
            .ok_or(ApplyFailure::InvalidAction)?
            .max(self.min_y);
        let max_y = self
            .point
            .position
            .y
            .checked_add(f64::from_bits(self.height).ceil() as i32 + 3)
            .ok_or(ApplyFailure::InvalidAction)?
            .min(self.max_y - 1);
        let mut targets = Vec::new();
        for y in min_y..=max_y {
            for z in min_z..=max_z {
                for x in min_x..=max_x {
                    if targets.len() == 16_384 {
                        return Err(ApplyFailure::Capacity);
                    }
                    targets.push(self.target(BlockPos { x, y, z }));
                }
            }
        }
        Ok(targets)
    }
}
