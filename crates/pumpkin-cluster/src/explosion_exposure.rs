use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ActionState, ApplyFailure, Cell, ChunkPart, Target, Value};
use crate::item_contact::ContactObservation;
use crate::protocol::BlockPos;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplosionExposurePlan {
    pub center: [u64; 3],
    pub contact: ContactObservation,
}

struct ExposureRay {
    start: [f64; 3],
    end: [f64; 3],
    positions: Vec<BlockPos>,
}

pub fn block_ray_positions(start: [f64; 3], end: [f64; 3]) -> Result<Vec<BlockPos>, ApplyFailure> {
    if start
        .iter()
        .chain(end.iter())
        .any(|value| !value.is_finite() || value.abs() > 30_001_024.0)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    if start == end {
        return Ok(Vec::new());
    }
    let from: [f64; 3] =
        core::array::from_fn(|axis| start[axis] + (end[axis] - start[axis]) * -1.0e-7);
    let to: [f64; 3] = core::array::from_fn(|axis| end[axis] + (start[axis] - end[axis]) * -1.0e-7);
    let difference: [f64; 3] = core::array::from_fn(|axis| to[axis] - from[axis]);
    let mut position = from.map(|value| value.floor() as i32);
    let steps: [i32; 3] = core::array::from_fn(|axis| {
        if difference[axis] > 0.0 {
            1
        } else if difference[axis] < 0.0 {
            -1
        } else {
            0
        }
    });
    let delta: [f64; 3] = core::array::from_fn(|axis| {
        if steps[axis] == 0 {
            f64::INFINITY
        } else {
            f64::from(steps[axis]) / difference[axis]
        }
    });
    let mut next: [f64; 3] = core::array::from_fn(|axis| {
        if steps[axis] == 0 {
            f64::INFINITY
        } else {
            let boundary = f64::from(position[axis]) + if steps[axis] > 0 { 1.0 } else { 0.0 };
            (boundary - from[axis]) / difference[axis]
        }
    });
    let mut positions = Vec::new();
    loop {
        if positions.len() == 1024 {
            return Err(ApplyFailure::Capacity);
        }
        positions.push(BlockPos {
            x: position[0],
            y: position[1],
            z: position[2],
        });
        let axis = if next[0] <= next[1] && next[0] <= next[2] {
            0
        } else if next[1] <= next[2] {
            1
        } else {
            2
        };
        if next[axis] > 1.0 {
            break;
        }
        position[axis] = position[axis]
            .checked_add(steps[axis])
            .ok_or(ApplyFailure::InvalidAction)?;
        next[axis] += delta[axis];
    }
    Ok(positions)
}

impl ExplosionExposurePlan {
    pub fn valid(&self) -> bool {
        self.contact.valid()
            && self.center.iter().all(|bits| {
                let value = f64::from_bits(*bits);
                value.is_finite() && value.abs() < 30_000_000.0
            })
            && self
                .center
                .iter()
                .zip(self.contact.position)
                .all(|(center, position)| {
                    (f64::from_bits(*center) - f64::from_bits(position)).abs() <= 256.0
                })
            && f64::from_bits(self.contact.half_width) <= 16.0
            && f64::from_bits(self.contact.height) <= 16.0
    }
    fn rays(&self) -> Result<Vec<ExposureRay>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let position = self.contact.position.map(f64::from_bits);
        let width = f64::from_bits(self.contact.half_width);
        let height = f64::from_bits(self.contact.height);
        let lower = [position[0] - width, position[1], position[2] - width];
        let size = [width * 2.0, height, width * 2.0];
        let steps = size.map(|size| 1.0 / (size * 2.0 + 1.0));
        let offsets = [
            (1.0 - (1.0 / steps[0]).floor() * steps[0]) / 2.0,
            0.0,
            (1.0 - (1.0 / steps[2]).floor() * steps[2]) / 2.0,
        ];
        let end = self.center.map(f64::from_bits);
        let mut rays = Vec::new();
        let mut x = 0.0;
        while x <= 1.0 {
            let mut y = 0.0;
            while y <= 1.0 {
                let mut z = 0.0;
                while z <= 1.0 {
                    if rays.len() == 4096 {
                        return Err(ApplyFailure::Capacity);
                    }
                    let fractions = [x, y, z];
                    let start = core::array::from_fn(|axis| {
                        lower[axis] + size[axis] * fractions[axis] + offsets[axis]
                    });
                    rays.push(ExposureRay {
                        start,
                        end,
                        positions: block_ray_positions(start, end)?,
                    });
                    z += steps[2];
                }
                y += steps[1];
            }
            x += steps[0];
        }
        Ok(rays)
    }
    fn target(&self, position: BlockPos) -> Target {
        Target::Chunk {
            key: crate::protocol::ChunkKey::new(
                self.contact.world,
                position.x.div_euclid(16),
                position.z.div_euclid(16),
            ),
            part: ChunkPart::Block(position),
            inventory: None,
        }
    }
    pub fn targets(&self) -> Result<BTreeSet<Target>, ApplyFailure> {
        let mut targets = BTreeSet::new();
        for ray in self.rays()? {
            for position in ray.positions {
                targets.insert(self.target(position));
                if targets.len() > 16384 {
                    return Err(ApplyFailure::Capacity);
                }
            }
        }
        Ok(targets)
    }
    pub fn derive<S: ActionState>(
        &self,
        state: &S,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<f64, ApplyFailure> {
        let rays = self.rays()?;
        let total = rays.len();
        let mut visible = 0usize;
        for ray in rays {
            let mut blocked = false;
            for position in ray.positions {
                let target = self.target(position);
                let Some(Cell {
                    value: Value::Block(block),
                    ..
                }) = values.get(&target)
                else {
                    return Err(ApplyFailure::Condition(target));
                };
                if state.block_segment_obstructs(
                    *block,
                    position,
                    ray.start.map(f64::to_bits),
                    ray.end.map(f64::to_bits),
                )? {
                    blocked = true;
                    break;
                }
            }
            visible += usize::from(!blocked);
        }
        Ok(if total == 0 {
            0.0
        } else {
            visible as f64 / total as f64
        })
    }
}
