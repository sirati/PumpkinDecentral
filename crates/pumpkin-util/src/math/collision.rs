use super::{
    boundingbox::BoundingBox,
    position::BlockPos,
    vector3::{Axis, Vector3},
};

#[derive(Debug, Clone, Copy)]
pub struct MovementCollision {
    pub movement: Vector3<f64>,
    pub supporting_block: Option<BlockPos>,
    pub horizontal_collision: bool,
}

pub fn resolve_block_movement(
    bounds: BoundingBox,
    movement: Vector3<f64>,
    collisions: &[BoundingBox],
    block_positions: &[(usize, BlockPos)],
) -> MovementCollision {
    let mut result = MovementCollision {
        movement,
        supporting_block: None,
        horizontal_collision: false,
    };
    // Y-Axis adjustment
    if movement.y != 0.0 {
        let mut max_time = 1.0;
        let mut positions = block_positions.iter().copied();
        if let Some((mut collisions_len, mut position)) = positions.next() {
            for (index, collider) in collisions.iter().enumerate() {
                if index == collisions_len {
                    let Some((next_len, next_pos)) = positions.next() else {
                        break;
                    };
                    collisions_len = next_len;
                    position = next_pos;
                }
                if let Some(time) =
                    bounds.calculate_collision_time(collider, result.movement, Axis::Y, max_time)
                {
                    max_time = time;
                    // If the entity is moving downwards and collides, set the supporting block position
                    if movement.y < 0.0 {
                        result.supporting_block = Some(position);
                    }
                }
            }
            result.movement.y *= max_time;
        }
    }
    for axis in Axis::horizontal() {
        if movement.get_axis(axis) == 0.0 {
            continue;
        }
        let mut max_time = 1.0;
        for collider in collisions {
            if let Some(time) =
                bounds.calculate_collision_time(collider, result.movement, axis, max_time)
            {
                max_time = time;
            }
        }
        if max_time != 1.0 {
            result
                .movement
                .set_axis(axis, result.movement.get_axis(axis) * max_time);
            result.horizontal_collision = true;
        }
    }
    result
}

pub fn resolve_step_movement<E>(
    bounds: BoundingBox,
    desired: Vector3<f64>,
    step_height: f64,
    on_ground: bool,
    mut collide: impl FnMut(BoundingBox, Vector3<f64>) -> Result<MovementCollision, E>,
) -> Result<MovementCollision, E> {
    let original = collide(bounds, desired)?;
    if !original.horizontal_collision || !step_height.is_finite() || step_height <= 0.0
        || !(on_ground || desired.y < 0.0 && original.movement.y != desired.y) {
        return Ok(original);
    }
    let shift = |bounds: BoundingBox, delta: Vector3<f64>| BoundingBox {
        min: bounds.min + delta, max: bounds.max + delta,
    };
    let distance = |delta: Vector3<f64>| delta.x * delta.x + delta.z * delta.z;
    let direct = collide(bounds, Vector3::new(desired.x, step_height, desired.z))?;
    let upward = collide(bounds.stretch(Vector3::new(desired.x, 0.0, desired.z)),
        Vector3::new(0.0, step_height, 0.0))?;
    let raised_bounds = shift(bounds, upward.movement);
    let horizontal = collide(raised_bounds, Vector3::new(desired.x, 0.0, desired.z))?;
    let separate = upward.movement + horizontal.movement;
    let candidate = if distance(separate) > distance(direct.movement) { separate } else { direct.movement };
    if distance(candidate) <= distance(original.movement) { return Ok(original); }
    let descent = collide(shift(bounds, candidate), Vector3::new(0.0, desired.y - candidate.y, 0.0))?;
    let movement = candidate + descent.movement;
    Ok(MovementCollision {
        movement,
        supporting_block: descent.supporting_block,
        horizontal_collision: movement.x != desired.x || movement.z != desired.z,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(min: [f64; 3], max: [f64; 3]) -> BoundingBox {
        BoundingBox {
            min: Vector3::new(min[0], min[1], min[2]),
            max: Vector3::new(max[0], max[1], max[2]),
        }
    }

    #[test]
    fn step_retry_crosses_slab_but_respects_ceiling_and_airborne_source() {
        let body = bounds([0.0, 0.0, 0.0], [0.6, 1.8, 0.6]);
        let slab = bounds([1.0, 0.0, 0.0], [2.0, 0.5, 1.0]);
        let ceiling = bounds([0.0, 1.9, 0.0], [3.0, 3.0, 1.0]);
        let desired = Vector3::new(1.2, -0.1, 0.0);
        let solve = |shapes: &[BoundingBox], grounded| resolve_step_movement(body, desired, 0.6, grounded,
            |at, delta| Ok::<_, ()>(resolve_block_movement(at, delta, shapes, &[(shapes.len(), BlockPos::new(1, 0, 0))]))).unwrap();
        let stepped = solve(&[slab], true);
        assert_eq!(stepped.movement.x, desired.x);
        assert_eq!(stepped.movement.y, 0.5);
        assert!(solve(&[slab, ceiling], true).movement.x < desired.x);
        let airborne = resolve_step_movement(body, Vector3::new(1.2, 0.0, 0.0), 0.6, false,
            |at, delta| Ok::<_, ()>(resolve_block_movement(at, delta, &[slab], &[(1, BlockPos::new(1, 0, 0))]))).unwrap();
        assert!(airborne.horizontal_collision);
        let mut reads = 0;
        assert!(resolve_step_movement(body, desired, 0.6, true, |at, delta| {
            reads += 1;
            if reads == 2 { return Err("missing original cut"); }
            Ok(resolve_block_movement(at, delta, &[slab], &[(1, BlockPos::new(1, 0, 0))]))
        }).is_err());
    }

    #[test]
    fn falling_item_stops_at_floor_and_keeps_support_identity() {
        let entity = bounds([0.375, 1.5, 0.375], [0.625, 1.75, 0.625]);
        let floor = bounds([0.0, 0.0, 0.0], [1.0, 1.0, 1.0]);
        let support = BlockPos::new(0, 0, 0);
        let collision = resolve_block_movement(
            entity,
            Vector3::new(0.0, -1.0, 0.0),
            &[floor],
            &[(1, support)],
        );
        assert_eq!(collision.movement.y, -0.5);
        assert_eq!(collision.supporting_block, Some(support));
        assert!(!collision.horizontal_collision);
    }

    #[test]
    fn closest_floor_wins_independent_of_shape_arrival() {
        let entity = bounds([0.375, 2.0, 0.375], [0.625, 2.25, 0.625]);
        let low = bounds([0.0, 0.0, 0.0], [1.0, 0.5, 1.0]);
        let high = bounds([0.0, 1.0, 0.0], [1.0, 1.5, 1.0]);
        for (shapes, positions) in [
            (
                [low, high],
                [(1, BlockPos::new(0, 0, 0)), (2, BlockPos::new(0, 1, 0))],
            ),
            (
                [high, low],
                [(1, BlockPos::new(0, 1, 0)), (2, BlockPos::new(0, 0, 0))],
            ),
        ] {
            let collision =
                resolve_block_movement(entity, Vector3::new(0.0, -2.0, 0.0), &shapes, &positions);
            assert_eq!(collision.movement.y, -0.5);
            assert_eq!(collision.supporting_block, Some(BlockPos::new(0, 1, 0)));
        }
    }

    #[test]
    fn wall_stops_horizontal_motion_without_creating_ground_support() {
        let entity = bounds([0.375, 1.0, 0.375], [0.625, 1.25, 0.625]);
        let wall = bounds([1.0, 0.0, 0.0], [2.0, 2.0, 1.0]);
        let collision = resolve_block_movement(
            entity,
            Vector3::new(1.0, 0.0, 0.0),
            &[wall],
            &[(1, BlockPos::new(1, 0, 0))],
        );
        assert_eq!(collision.movement.x, 0.375);
        assert!(collision.horizontal_collision);
        assert_eq!(collision.supporting_block, None);
    }
}
