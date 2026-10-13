use pumpkin_data::{BlockDirection, BlockState};
use pumpkin_util::math::{boundingbox::BoundingBox, position::BlockPos, vector3::Vector3};

pub fn resolve_block_push_out(
    fractional_position: Vector3<f64>,
    velocity: Vector3<f64>,
    full_cube_neighbors: [bool; 6],
    amplitude: f64,
) -> Vector3<f64> {
    let mut distance = f64::MAX;
    let mut direction = BlockDirection::Up;
    for candidate in BlockDirection::all() {
        if candidate == BlockDirection::Down
            || full_cube_neighbors[usize::from(candidate.to_index())]
        {
            continue;
        }
        let component = fractional_position.get_axis(candidate.to_axis().into());
        let candidate_distance = if candidate.positive() {
            1.0 - component
        } else {
            component
        };
        if candidate_distance < distance {
            distance = candidate_distance;
            direction = candidate;
        }
    }
    let mut result = velocity * 0.75;
    result.set_axis(
        direction.to_axis().into(),
        if direction.positive() {
            amplitude
        } else {
            -amplitude
        },
    );
    result
}

#[derive(Debug, PartialEq, Eq)]
pub enum CollisionReadFailure<E> {
    InvalidBounds,
    Budget,
    Read { position: BlockPos, reason: E },
}

pub struct BlockCollisions {
    pub shapes: Vec<BoundingBox>,
    pub positions: Vec<(usize, BlockPos)>,
}

pub fn collect_block_collisions<E>(
    bounds: BoundingBox,
    budget: usize,
    mut read: impl FnMut(BlockPos) -> Result<&'static BlockState, E>,
    mut override_shape: impl FnMut(BlockPos, &'static BlockState) -> Option<Option<BoundingBox>>,
) -> Result<BlockCollisions, CollisionReadFailure<E>> {
    let lower = [bounds.min.x, bounds.min.y - 0.50001, bounds.min.z];
    let upper = [
        bounds.max.x - 1e-9,
        bounds.max.y - 1e-9,
        bounds.max.z - 1e-9,
    ];
    if lower.iter().zip(upper).any(|(low, high)| {
        !low.is_finite()
            || !high.is_finite()
            || *low > high
            || low.floor() < f64::from(i32::MIN)
            || high.floor() > f64::from(i32::MAX)
    }) {
        return Err(CollisionReadFailure::InvalidBounds);
    }
    let lower = lower.map(|value| value.floor() as i32);
    let upper = upper.map(|value| value.floor() as i32);
    let queries = lower
        .iter()
        .zip(upper)
        .try_fold(1usize, |count, (low, high)| {
            usize::try_from(i64::from(high) - i64::from(*low) + 1)
                .ok()
                .and_then(|width| count.checked_mul(width))
        });
    if queries.is_none_or(|queries| queries > budget) {
        return Err(CollisionReadFailure::Budget);
    }
    let mut result = BlockCollisions {
        shapes: Vec::new(),
        positions: Vec::new(),
    };
    for position in BlockPos::iterate(
        BlockPos::new(lower[0], lower[1], lower[2]),
        BlockPos::new(upper[0], upper[1], upper[2]),
    ) {
        let state =
            read(position).map_err(|reason| CollisionReadFailure::Read { position, reason })?;
        if state.is_air() {
            continue;
        }
        let start = result.shapes.len();
        match override_shape(position, state) {
            Some(Some(shape)) => {
                let shape = shape.at_pos(position);
                if shape.intersects(&bounds) {
                    result.shapes.push(shape);
                }
            }
            Some(None) => {}
            None => {
                result.shapes.extend(
                    state
                        .get_block_collision_shapes_at(&position)
                        .map(|shape| shape.at_pos(position))
                        .filter(|shape| shape.intersects(&bounds)),
                );
            }
        }
        if start != result.shapes.len() {
            result.positions.push((result.shapes.len(), position));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_data::Block;
    use pumpkin_util::math::{collision::resolve_block_movement, vector3::Vector3};

    #[test]
    fn push_out_uses_nearest_available_face_and_captured_impulse() {
        let mut neighbors = [true; 6];
        neighbors[usize::from(BlockDirection::West.to_index())] = false;
        neighbors[usize::from(BlockDirection::East.to_index())] = false;
        neighbors[usize::from(BlockDirection::Down.to_index())] = false;
        assert_eq!(
            resolve_block_push_out(
                Vector3::new(0.1, 0.01, 0.5),
                Vector3::new(1.0, 2.0, 3.0),
                neighbors,
                0.2,
            ),
            Vector3::new(-0.2, 1.5, 2.25)
        );
    }

    #[test]
    fn enclosed_push_out_preserves_upward_fallback() {
        assert_eq!(
            resolve_block_push_out(
                Vector3::new(0.5, 0.5, 0.5),
                Vector3::new(1.0, -2.0, 3.0),
                [true; 6],
                0.125,
            ),
            Vector3::new(0.75, 0.125, 2.25)
        );
    }

    #[test]
    fn immutable_generated_shapes_feed_the_shared_item_sweep() {
        let bounds = BoundingBox {
            min: Vector3::new(0.375, 0.5, 0.375),
            max: Vector3::new(0.625, 1.75, 0.625),
        };
        let collisions = collect_block_collisions::<()>(
            bounds,
            3,
            |position| {
                Ok(if position == BlockPos::new(0, 0, 0) {
                    Block::STONE.default_state
                } else {
                    Block::AIR.default_state
                })
            },
            |_, _| None,
        )
        .unwrap();
        assert_eq!(collisions.shapes.len(), 1);
        let entity = BoundingBox {
            min: Vector3::new(0.375, 1.5, 0.375),
            max: Vector3::new(0.625, 1.75, 0.625),
        };
        let resolved = resolve_block_movement(
            entity,
            Vector3::new(0.0, -1.0, 0.0),
            &collisions.shapes,
            &collisions.positions,
        );
        assert_eq!(resolved.movement.y, -0.5);
        assert_eq!(resolved.supporting_block, Some(BlockPos::new(0, 0, 0)));
    }

    #[test]
    fn missing_snapshot_is_not_treated_as_air_and_budget_precedes_reads() {
        let bounds = BoundingBox::full_block();
        let mut reads = 0;
        assert!(matches!(
            collect_block_collisions::<()>(
                bounds,
                0,
                |_| {
                    reads += 1;
                    Ok(Block::AIR.default_state)
                },
                |_, _| None
            ),
            Err(CollisionReadFailure::Budget)
        ));
        assert_eq!(reads, 0);
        assert!(matches!(
            collect_block_collisions(
                bounds,
                64,
                |position| Err::<&'static BlockState, _>(position),
                |_, _| None
            ),
            Err(CollisionReadFailure::Read { .. })
        ));
    }

    #[test]
    fn item_powder_snow_override_does_not_create_a_full_cube() {
        let result = collect_block_collisions::<()>(
            BoundingBox::full_block(),
            64,
            |_| Ok(Block::POWDER_SNOW.default_state),
            |_, _| Some(None),
        )
        .unwrap();
        assert!(result.shapes.is_empty());
        assert!(result.positions.is_empty());
    }

    #[test]
    fn exact_chunk_edge_does_not_read_the_adjacent_chunk() {
        let bounds = BoundingBox {
            min: Vector3::new(15.75, 1.0, 0.25),
            max: Vector3::new(16.0, 1.25, 0.5),
        };
        let result = collect_block_collisions(
            bounds,
            2,
            |position| {
                if position.0.x != 15 {
                    return Err(position);
                }
                Ok(Block::AIR.default_state)
            },
            |_, _| None,
        )
        .unwrap();
        assert!(result.shapes.is_empty());
    }
}
