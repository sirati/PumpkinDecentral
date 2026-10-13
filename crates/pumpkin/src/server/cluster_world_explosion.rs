use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::canonical_action::{ActionState, Cell, ChunkPart, Target, TruthView, Value};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
use pumpkin_data::fluid::Fluid;
use pumpkin_data::{Block, BlockStateId};
use pumpkin_util::random::RandomImpl;

use super::cluster_world_callbacks::CallbackCaptureError;

pub(super) struct ExplosionBlockPlan {
    pub facts: BTreeMap<Target, Option<Cell>>,
    pub affected: Vec<BlockPos>,
}

pub(super) struct ExplosionGeometry {
    pub world: WorldId,
    pub center: [f64; 3],
    pub power: f32,
    pub seed: u64,
    pub min_y: i32,
    pub height: i32,
    pub preserve_rails: bool,
}

impl ExplosionGeometry {
    fn valid(&self) -> bool {
        self.center
            .iter()
            .all(|value| value.is_finite() && value.abs() < 30_000_000.0)
            && self.power.is_finite()
            && (0.0..=128.0).contains(&self.power)
            && self.height > 0
            && self.height <= 4096
            && self.min_y.checked_add(self.height).is_some()
    }
}

fn block<S: ActionState>(
    state: &S,
    world: WorldId,
    position: BlockPos,
    facts: &mut BTreeMap<Target, Option<Cell>>,
    capacity: usize,
) -> Result<BlockStateId, CallbackCaptureError> {
    let key = ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16));
    let target = Target::Chunk {
        key,
        part: ChunkPart::Block(position),
        inventory: None,
    };
    if let Some(Some(cell)) = facts.get(&target) {
        let Value::Block(id) = cell.value else {
            return Err(CallbackCaptureError::InvalidWrite(target));
        };
        return BlockStateId::new(id).ok_or(CallbackCaptureError::InvalidWrite(target));
    }
    if facts.len() >= capacity {
        return Err(CallbackCaptureError::Budget);
    }
    if !state.available(TruthView::Local, target) {
        return Err(CallbackCaptureError::InvalidWrite(target));
    }
    let cell = state
        .read(TruthView::Local, target)
        .ok_or(CallbackCaptureError::InvalidWrite(target))?;
    let Value::Block(id) = cell.value else {
        return Err(CallbackCaptureError::InvalidWrite(target));
    };
    let block = BlockStateId::new(id).ok_or(CallbackCaptureError::InvalidWrite(target))?;
    facts.insert(target, Some(cell));
    Ok(block)
}

fn rail(state: BlockStateId) -> bool {
    matches!(state.to_block(), block if block == &Block::RAIL || block == &Block::POWERED_RAIL || block == &Block::DETECTOR_RAIL || block == &Block::ACTIVATOR_RAIL)
}

pub(super) fn capture_block_rays<S: ActionState>(
    state: &S,
    geometry: &ExplosionGeometry,
    capacity: usize,
) -> Result<ExplosionBlockPlan, CallbackCaptureError> {
    if !geometry.valid() || capacity == 0 || capacity > 65_536 {
        return Err(CallbackCaptureError::Budget);
    }
    let mut facts = BTreeMap::new();
    let mut affected = BTreeSet::new();
    let mut random = pumpkin_util::random::RandomGenerator::Xoroshiro(
        pumpkin_util::random::xoroshiro128::Xoroshiro::from_seed(geometry.seed),
    );
    for x in 0..16 {
        for y in 0..16 {
            for z in 0..16 {
                if x > 0 && x < 15 && y > 0 && y < 15 && z > 0 && z < 15 {
                    continue;
                }
                let mut direction = [
                    f64::from(x) / 7.5 - 1.0,
                    f64::from(y) / 7.5 - 1.0,
                    f64::from(z) / 7.5 - 1.0,
                ];
                let length = direction
                    .iter()
                    .map(|value| value * value)
                    .sum::<f64>()
                    .sqrt();
                for component in &mut direction {
                    *component /= length;
                }
                let mut position = geometry.center;
                let mut strength = geometry.power * random.next_f32().mul_add(0.6, 0.7);
                while strength > 0.0 {
                    let point = BlockPos {
                        x: position[0].floor() as i32,
                        y: position[1].floor() as i32,
                        z: position[2].floor() as i32,
                    };
                    if point.y < geometry.min_y
                        || point.y >= geometry.min_y + geometry.height
                        || !(-30_000_000..30_000_000).contains(&point.x)
                        || !(-30_000_000..30_000_000).contains(&point.z)
                    {
                        break;
                    }
                    let current = block(state, geometry.world, point, &mut facts, capacity)?;
                    let block_type = current.to_block();
                    let fluid = Fluid::from_state_id(current)
                        .map(Fluid::to_flowing)
                        .or_else(|| {
                            block_type
                                .is_waterlogged(current)
                                .then_some(&Fluid::FLOWING_WATER)
                        });
                    let fluid_resistance =
                        fluid.map_or(0.0, |fluid| fluid.states[0].blast_resistance);
                    if !current.to_state().is_air() || fluid.is_some() {
                        let protected = geometry.preserve_rails
                            && (rail(current)
                                || rail(block(
                                    state,
                                    geometry.world,
                                    BlockPos {
                                        y: point
                                            .y
                                            .checked_add(1)
                                            .ok_or(CallbackCaptureError::Budget)?,
                                        ..point
                                    },
                                    &mut facts,
                                    capacity,
                                )?));
                        let resistance = if protected {
                            0.0
                        } else {
                            fluid_resistance.max(block_type.blast_resistance)
                        };
                        strength -= (resistance + 0.3) * 0.3;
                        if strength > 0.0 && !protected {
                            affected.insert(point);
                        }
                    }
                    for index in 0..3 {
                        position[index] += direction[index] * 0.3;
                    }
                    strength -= 0.225_000_01;
                }
            }
        }
    }
    Ok(ExplosionBlockPlan {
        facts,
        affected: affected.into_iter().collect(),
    })
}

pub(super) fn block_segment_obstructs(
    state: u16,
    position: BlockPos,
    start: [u64; 3],
    end: [u64; 3],
) -> Result<bool, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let state = BlockStateId::new(state).ok_or(ApplyFailure::InvalidAction)?;
    let start = start.map(f64::from_bits);
    let end = end.map(f64::from_bits);
    if start
        .iter()
        .chain(end.iter())
        .any(|value| !value.is_finite() || value.abs() > 30_000_016.0)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let point = pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
    for shape in state.to_state().get_block_collision_shapes_at(&point) {
        let lower = [
            shape.min.x + f64::from(position.x),
            shape.min.y + f64::from(position.y),
            shape.min.z + f64::from(position.z),
        ];
        let upper = [
            shape.max.x + f64::from(position.x),
            shape.max.y + f64::from(position.y),
            shape.max.z + f64::from(position.z),
        ];
        let mut first = 0.0_f64;
        let mut last = 1.0_f64;
        let mut misses = false;
        for axis in 0..3 {
            let delta = end[axis] - start[axis];
            if delta.abs() <= f64::EPSILON {
                if start[axis] < lower[axis] || start[axis] > upper[axis] {
                    misses = true;
                    break;
                }
                continue;
            }
            let a = (lower[axis] - start[axis]) / delta;
            let b = (upper[axis] - start[axis]) / delta;
            first = first.max(a.min(b));
            last = last.min(a.max(b));
            if first > last {
                misses = true;
                break;
            }
        }
        if !misses && first <= last {
            return Ok(true);
        }
    }
    Ok(false)
}
