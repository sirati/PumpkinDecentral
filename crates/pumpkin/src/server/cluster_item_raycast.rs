use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{
    ActionState, ApplyFailure, Cell, ChunkPart, Target, TruthView, Value,
};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
use pumpkin_data::{Block, BlockDirection, BlockStateId};
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FluidRayPolicy {
    None,
    Source,
    Any,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ItemRayHit {
    pub chunk: ChunkKey,
    pub position: BlockPos,
    pub state: BlockStateId,
    pub face: BlockDirection,
    pub point: [f64; 3],
}

pub(crate) struct ItemRayCapture {
    pub hit: Option<ItemRayHit>,
    pub facts: BTreeMap<Target, Cell>,
}

pub(crate) fn segment(
    position: [f64; 3],
    eye_height: f64,
    yaw: f32,
    pitch: f32,
    reach: f64,
) -> Result<([f64; 3], [f64; 3]), ApplyFailure> {
    if !eye_height.is_finite()
        || !(0.0..=32.0).contains(&eye_height)
        || !yaw.is_finite()
        || !pitch.is_finite()
        || !reach.is_finite()
        || !(0.0..=64.0).contains(&reach)
        || position.iter().any(|coordinate| !coordinate.is_finite())
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let start = [position[0], position[1] + eye_height, position[2]];
    let (sin_y, cos_y) = f64::from(yaw).to_radians().sin_cos();
    let (sin_p, cos_p) = f64::from(pitch).to_radians().sin_cos();
    let end = [
        start[0] - sin_y * cos_p * reach,
        start[1] - sin_p * reach,
        start[2] + cos_y * cos_p * reach,
    ];
    if start
        .iter()
        .chain(end.iter())
        .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 30_001_024.0)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok((start, end))
}

pub(crate) fn capture<S: ActionState>(
    view: &S,
    world: WorldId,
    start: [f64; 3],
    end: [f64; 3],
    fluids: FluidRayPolicy,
) -> Result<ItemRayCapture, ApplyFailure> {
    capture_bounded(view, world, start, end, fluids, None)
}

pub(crate) fn required_targets_with_rules(
    world: WorldId,
    start: [f64; 3],
    end: [f64; 3],
    fluids: FluidRayPolicy,
    rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
) -> Result<Vec<Target>, ApplyFailure> {
    if !rules.valid() || rules.rules.world != world {
        return Err(ApplyFailure::InvalidAction);
    }
    let distance_squared = start
        .iter()
        .zip(end.iter())
        .map(|(a, b)| (b - a).powi(2))
        .sum::<f64>();
    if !distance_squared.is_finite() || distance_squared > 64.0 * 64.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let maximum = rules
        .min_y
        .checked_add(rules.height as i32)
        .ok_or(ApplyFailure::InvalidAction)?;
    let mut targets = std::collections::BTreeSet::new();
    for position in pumpkin_cluster::explosion_exposure::block_ray_positions(start, end)? {
        for position in
            std::iter::once(position).chain((fluids != FluidRayPolicy::None).then(|| BlockPos {
                y: position.y.saturating_add(1),
                ..position
            }))
        {
            if position.y < rules.min_y || position.y >= maximum {
                continue;
            }
            targets.insert(Target::Chunk {
                key: ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16)),
                part: ChunkPart::Block(position),
                inventory: None,
            });
        }
    }
    Ok(targets.into_iter().collect())
}

pub(crate) fn source_readiness<S: ActionState>(
    view: &S,
    targets: &[Target],
) -> Result<(), ApplyFailure> {
    for target in targets {
        if !view.available(TruthView::Local, *target) {
            return Err(ApplyFailure::MissingStakeholder(target.stakeholder()));
        }
        let cell = view
            .read(TruthView::Local, *target)
            .ok_or(ApplyFailure::Condition(*target))?;
        if !matches!(cell.value, Value::Block(id) if BlockStateId::new(id).is_some()) {
            return Err(ApplyFailure::Type(*target));
        }
    }
    Ok(())
}

pub(crate) fn capture_with_rules<S: ActionState>(
    view: &S,
    world: WorldId,
    start: [f64; 3],
    end: [f64; 3],
    fluids: FluidRayPolicy,
    rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
) -> Result<ItemRayCapture, ApplyFailure> {
    if !rules.valid() || rules.rules.world != world {
        return Err(ApplyFailure::InvalidAction);
    }
    let max_y = rules
        .min_y
        .checked_add(rules.height as i32)
        .ok_or(ApplyFailure::InvalidAction)?;
    capture_bounded(view, world, start, end, fluids, Some((rules.min_y, max_y)))
}

fn capture_bounded<S: ActionState>(
    view: &S,
    world: WorldId,
    start: [f64; 3],
    end: [f64; 3],
    fluids: FluidRayPolicy,
    bounds: Option<(i32, i32)>,
) -> Result<ItemRayCapture, ApplyFailure> {
    let distance_squared = start
        .iter()
        .zip(end.iter())
        .map(|(a, b)| (b - a).powi(2))
        .sum::<f64>();
    if !distance_squared.is_finite() || distance_squared > 64.0 * 64.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let positions = pumpkin_cluster::explosion_exposure::block_ray_positions(start, end)?;
    let mut facts = BTreeMap::new();
    let mut states = Vec::with_capacity(positions.len());
    for position in positions {
        if bounds.is_some_and(|(min, max)| position.y < min || position.y >= max) {
            continue;
        }
        let chunk = ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16));
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(position),
            inventory: None,
        };
        let cell = view
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        let Value::Block(id) = cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        let state = BlockStateId::new(id).ok_or(ApplyFailure::Type(target))?;
        facts.insert(target, cell);
        states.push((chunk, position, state));
    }
    let from = Vector3::new(start[0], start[1], start[2]);
    let to = Vector3::new(end[0], end[1], end[2]);
    let mut closest = None::<(f64, ItemRayHit)>;
    for (chunk, position, state) in states {
        let data = state.to_state();
        let liquid = data.is_liquid();
        if liquid
            && (fluids == FluidRayPolicy::None
                || fluids == FluidRayPolicy::Source
                    && state != Block::WATER.default_state.id
                    && state != Block::LAVA.default_state.id)
        {
            continue;
        }
        let block_position =
            pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
        let offset = state.to_block().shape_offset_delta(&block_position);
        let mut shapes = data
            .outline_shapes
            .iter()
            .map(|id| pumpkin_data::block_properties::COLLISION_SHAPES[*id as usize].shift(offset))
            .collect::<Vec<_>>();
        if (liquid || data.is_waterlogged()) && fluids != FluidRayPolicy::None {
            let above = BlockPos {
                y: position
                    .y
                    .checked_add(1)
                    .ok_or(ApplyFailure::InvalidAction)?,
                ..position
            };
            let above_state = if bounds.is_some_and(|(min, max)| above.y < min || above.y >= max) {
                BlockStateId::AIR
            } else {
                let target = Target::Chunk {
                    key: chunk,
                    part: ChunkPart::Block(above),
                    inventory: None,
                };
                let cell = if let Some(cell) = facts.get(&target) {
                    cell.clone()
                } else {
                    view.read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?
                };
                let Value::Block(id) = cell.value else {
                    return Err(ApplyFailure::Type(target));
                };
                let above_state = BlockStateId::new(id).ok_or(ApplyFailure::Type(target))?;
                facts.insert(target, cell);
                above_state
            };
            let fluid = crate::world::World::get_fluid_from_state_id(state);
            let above_fluid = crate::world::World::get_fluid_from_state_id(above_state);
            let height = if above_fluid != &pumpkin_data::fluid::Fluid::EMPTY
                && above_fluid.matches_type(fluid)
            {
                1.0
            } else {
                pumpkin_data::fluid::Fluid::from_state_id(state)
                    .map_or(8.0 / 9.0, |fluid| f64::from(fluid.get_height(state)))
            };
            shapes.push(BoundingBox::new(
                Vector3::new(0.0, 0.0, 0.0),
                Vector3::new(1.0, height, 1.0),
            ));
        }
        for shape in shapes {
            let origin = Vector3::new(
                f64::from(position.x),
                f64::from(position.y),
                f64::from(position.z),
            );
            let min = shape.min.add(&origin);
            let max = shape.max.add(&origin);
            if let Some((distance, face, point)) =
                crate::world::World::intersects_aabb_with_hit(from, to, min, max)
                && closest.as_ref().is_none_or(|(old, _)| distance < *old)
            {
                closest = Some((
                    distance,
                    ItemRayHit {
                        chunk,
                        position,
                        state,
                        face,
                        point: [point.x, point.y, point.z],
                    },
                ));
            }
        }
    }
    Ok(ItemRayCapture {
        hit: closest.map(|(_, hit)| hit),
        facts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::canonical_action::OwnedStakeholderSet;

    fn put(
        state: &mut OwnedStakeholderSet,
        world: WorldId,
        position: BlockPos,
        block: BlockStateId,
    ) {
        state.local.insert(
            Target::Chunk {
                key: ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16)),
                part: ChunkPart::Block(position),
                inventory: None,
            },
            Cell {
                revision: 4,
                value: Value::Block(block.as_u16()),
            },
        );
    }

    #[test]
    fn hit_retains_cells_behind_it_and_missing_tail_requires_original_history() {
        let world = WorldId::new(0, 1);
        let mut state = OwnedStakeholderSet::default();
        for x in 0..=3 {
            put(
                &mut state,
                world,
                BlockPos { x, y: 64, z: 0 },
                if x == 1 {
                    Block::STONE.default_state.id
                } else {
                    BlockStateId::AIR
                },
            );
        }
        let result = capture(
            &state,
            world,
            [0.5, 64.5, 0.5],
            [3.5, 64.5, 0.5],
            FluidRayPolicy::None,
        )
        .unwrap();
        let hit = result.hit.unwrap();
        assert_eq!(hit.position, BlockPos { x: 1, y: 64, z: 0 });
        assert_eq!(hit.face, BlockDirection::West);
        assert_eq!(hit.point, [1.0, 64.5, 0.5]);
        let tail = Target::Chunk {
            key: ChunkKey::new(world, 0, 0),
            part: ChunkPart::Block(BlockPos { x: 3, y: 64, z: 0 }),
            inventory: None,
        };
        assert!(result.facts.contains_key(&tail));
        state.local.remove(&tail);
        assert!(
            matches!(capture(&state,world,[0.5,64.5,0.5],[3.5,64.5,0.5],FluidRayPolicy::None), Err(ApplyFailure::Condition(target)) if target == tail)
        );
    }

    #[test]
    fn original_dimension_bounds_exclude_only_outside_build_cells() {
        let world = WorldId::new(0, 1);
        let mut state = OwnedStakeholderSet::default();
        put(
            &mut state,
            world,
            BlockPos { x: 0, y: 15, z: 0 },
            Block::WATER.default_state.id,
        );
        let captured = capture_bounded(
            &state,
            world,
            [0.5, 17.5, 0.5],
            [0.5, 15.5, 0.5],
            FluidRayPolicy::Source,
            Some((0, 16)),
        )
        .unwrap();
        assert_eq!(captured.facts.len(), 1);
        let hit = captured.hit.unwrap();
        assert_eq!(hit.position, BlockPos { x: 0, y: 15, z: 0 });
        assert!((hit.point[1] - (15.0 + 8.0 / 9.0)).abs() < 1e-6);
        let missing = Target::Chunk {
            key: ChunkKey::new(world, 0, 0),
            part: ChunkPart::Block(BlockPos { x: 0, y: 14, z: 0 }),
            inventory: None,
        };
        assert!(
            matches!(capture_bounded(&state, world, [0.5, 17.5, 0.5], [0.5, 14.5, 0.5], FluidRayPolicy::Source, Some((0, 16))), Err(ApplyFailure::Condition(target)) if target == missing)
        );
    }

    #[test]
    fn source_fluid_raycast_uses_surface_height_and_ignore_policy_reaches_floor() {
        let world = WorldId::new(0, 1);
        let mut state = OwnedStakeholderSet::default();
        for y in 63..=66 {
            put(
                &mut state,
                world,
                BlockPos { x: 0, y, z: 0 },
                match y {
                    63 => Block::STONE.default_state.id,
                    64 => Block::WATER.default_state.id,
                    _ => BlockStateId::AIR,
                },
            );
        }
        let source = capture(
            &state,
            world,
            [0.5, 66.5, 0.5],
            [0.5, 63.5, 0.5],
            FluidRayPolicy::Source,
        )
        .unwrap();
        let water = source.hit.unwrap();
        assert_eq!(water.position, BlockPos { x: 0, y: 64, z: 0 });
        assert_eq!(water.face, BlockDirection::Up);
        assert!((water.point[1] - (64.0 + 8.0 / 9.0)).abs() < 1e-6);
        let floor = capture(
            &state,
            world,
            [0.5, 66.5, 0.5],
            [0.5, 63.5, 0.5],
            FluidRayPolicy::None,
        )
        .unwrap()
        .hit
        .unwrap();
        assert_eq!(floor.position, BlockPos { x: 0, y: 63, z: 0 });
        assert_eq!(floor.point[1], 64.0);
    }
}
