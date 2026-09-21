use crate::entity::{EntityBase, player::Player};
use crate::world::World;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;

const STEP: f64 = 1.0 / 64.0;

#[derive(Clone, Copy)]
pub(crate) struct LocalMovementCorrection {
    pub position: Vector3<f64>,
    pub fall_distance_bits: u32,
}

pub(crate) enum LocalMovementBoundary {
    Unchanged,
    Corrected(LocalMovementCorrection),
    Rejected { fall_distance_bits: u32 },
}

enum CorrectionPosition {
    Unchanged,
    Corrected(Vector3<f64>),
    Rejected,
}

fn fully_loaded(world: &World, player: &Player, position: Vector3<f64>) -> bool {
    if !position.x.is_finite() || !position.y.is_finite() || !position.z.is_finite() {
        return false;
    }
    let dimensions = player.get_entity().entity_dimension.load();
    let bounds = BoundingBox::new_from_pos(position.x, position.y, position.z, &dimensions);
    let min_x = bounds.min.x.floor() as i32;
    let max_x = bounds.max.x.next_down().floor() as i32;
    let min_z = bounds.min.z.floor() as i32;
    let max_z = bounds.max.z.next_down().floor() as i32;
    for chunk_x in min_x.div_euclid(16)..=max_x.div_euclid(16) {
        for chunk_z in min_z.div_euclid(16)..=max_z.div_euclid(16) {
            if !world.level.is_cluster_full(&Vector2::new(chunk_x, chunk_z)) {
                return false;
            }
        }
    }
    true
}

fn safe(world: &World, player: &Player, position: Vector3<f64>) -> bool {
    let dimensions = player.get_entity().entity_dimension.load();
    world.is_space_empty(BoundingBox::new_from_pos(
        position.x, position.y, position.z, &dimensions,
    ))
}

fn correction_position(
    last: Vector3<f64>,
    attempted: Vector3<f64>,
    fully_loaded: impl Fn(Vector3<f64>) -> bool,
    safe: impl Fn(Vector3<f64>) -> bool,
) -> CorrectionPosition {
    if fully_loaded(attempted) {
        return CorrectionPosition::Unchanged;
    }
    let delta = attempted - last;
    let distance = delta.length();
    if !distance.is_finite() || distance <= f64::EPSILON {
        return if fully_loaded(last) && safe(last) {
            CorrectionPosition::Corrected(last)
        } else {
            CorrectionPosition::Rejected
        };
    }
    let limit = (distance * 2.0).min(2.0);
    let steps = (limit / STEP).ceil() as usize;
    for step in 1..=steps {
        let correction = (step as f64 * STEP).min(limit);
        let position = attempted - delta * (correction / distance);
        if fully_loaded(position) && safe(position) {
            return CorrectionPosition::Corrected(position);
        }
    }
    if distance <= limit && fully_loaded(last) && safe(last) {
        CorrectionPosition::Corrected(last)
    } else {
        CorrectionPosition::Rejected
    }
}

pub(crate) fn correct(
    player: &Player,
    attempted: Vector3<f64>,
) -> LocalMovementBoundary {
    if player.cluster_gid().is_none() {
        return LocalMovementBoundary::Unchanged;
    }
    let snapshot = player.cluster_last_tick_movement_snapshot();
    let world = player.world();
    match correction_position(
        snapshot.position,
        attempted,
        |position| fully_loaded(&world, player, position),
        |position| safe(&world, player, position),
    ) {
        CorrectionPosition::Unchanged => LocalMovementBoundary::Unchanged,
        CorrectionPosition::Corrected(position) => {
            LocalMovementBoundary::Corrected(LocalMovementCorrection {
                position,
                fall_distance_bits: snapshot.fall_distance_bits,
            })
        }
        CorrectionPosition::Rejected => LocalMovementBoundary::Rejected {
            fall_distance_bits: snapshot.fall_distance_bits,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(x: f64) -> Vector3<f64> {
        Vector3::new(x, 64.0, 0.0)
    }

    #[test]
    fn leaves_a_fully_loaded_destination_unchanged() {
        assert!(
            matches!(
                correction_position(point(0.0), point(1.0), |_| true, |_| true),
                CorrectionPosition::Unchanged
            )
        );
    }

    #[test]
    fn returns_to_the_loaded_side_within_twice_the_movement_distance() {
        let attempted = point(1.0);
        let corrected = match correction_position(
            point(0.0),
            attempted,
            |position| position.x <= 0.25,
            |_| true,
        ) {
            CorrectionPosition::Corrected(position) => position,
            _ => panic!(),
        };
        assert!(corrected.x <= 0.25);
        assert!(attempted.x - corrected.x <= 2.0);
        assert!(attempted.x - corrected.x <= 2.0 * (attempted.x - 0.0));
    }

    #[test]
    fn skips_solid_loaded_positions() {
        let corrected = match correction_position(
            point(0.0),
            point(1.0),
            |position| position.x <= 0.5,
            |position| position.x <= 0.25,
        ) {
            CorrectionPosition::Corrected(position) => position,
            _ => panic!(),
        };
        assert!(corrected.x <= 0.25);
    }

    #[test]
    fn falls_back_to_the_last_safe_loaded_position_within_the_cap() {
        let attempted = point(1.0);
        assert!(
            matches!(
                correction_position(
                    point(0.0),
                    attempted,
                    |position| position.x == 0.0,
                    |_| true,
                ),
                CorrectionPosition::Corrected(position) if position == point(0.0)
            )
        );
    }

    #[test]
    fn never_exceeds_the_two_block_cap() {
        assert!(
            matches!(
                correction_position(
                    point(0.0),
                    point(8.0),
                    |position| position.x == 0.0,
                    |_| true,
                ),
                CorrectionPosition::Rejected
            )
        );
    }

    #[test]
    fn rejects_when_no_capped_position_is_safe_and_fully_loaded() {
        assert!(matches!(
            correction_position(point(0.0), point(1.0), |_| false, |_| false),
            CorrectionPosition::Rejected
        ));
    }
}
