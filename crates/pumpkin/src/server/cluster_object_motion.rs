use pumpkin_cluster::{
    canonical_action::{ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value},
    entity_body::{EntityBody, EntityPayload, EntitySpatial},
    object_state::ObjectKind,
    protocol::ChunkKey,
};
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

#[derive(Clone)]
pub(super) struct ObjectMotion {
    pub spatial: EntitySpatial,
    pub destination: ChunkKey,
    pub landing: Option<pumpkin_util::math::position::BlockPos>,
    pub fall_distance: f32,
}

pub(super) fn simulate(
    state: &impl ActionState,
    body: &EntityBody,
    original: &EntitySpatial,
    minimum_y: i32,
    maximum_y: i32,
) -> Result<ObjectMotion, ApplyFailure> {
    let EntityPayload::Object(object) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let (width, height, prior_distance) = match &object.kind {
        ObjectKind::PrimedTnt(_) => (0.98, 0.98, 0.0),
        ObjectKind::FallingBlock(falling) => (0.98, 0.98, f32::from_bits(falling.fall_distance)),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if original.world != body.spatial.world {
        return Err(ApplyFailure::InvalidAction);
    }
    let coordinates = original.position.map(f64::from_bits);
    if coordinates.iter().any(|value| !value.is_finite()) {
        return Err(ApplyFailure::InvalidAction);
    }
    let position = Vector3::new(coordinates[0], coordinates[1], coordinates[2]);
    let bounds = BoundingBox::new(
        position.add_raw(-width / 2.0, 0.0, -width / 2.0),
        position.add_raw(width / 2.0, height, width / 2.0),
    );
    let mut velocity = Vector3::new(
        original.velocity_micro[0] as f64 / 1_000_000.0,
        original.velocity_micro[1] as f64 / 1_000_000.0 - 0.04,
        original.velocity_micro[2] as f64 / 1_000_000.0,
    );
    let read = |at: pumpkin_util::math::position::BlockPos| {
        if at.0.y < minimum_y || at.0.y >= maximum_y {
            return Ok(pumpkin_data::Block::AIR.default_state);
        }
        let target = Target::Chunk {
            key: ChunkKey::new(original.world, at.0.x.div_euclid(16), at.0.z.div_euclid(16)),
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos {
                x: at.0.x,
                y: at.0.y,
                z: at.0.z,
            }),
            inventory: None,
        };
        let cell = state
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        let Value::Block(id) = cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        pumpkin_data::BlockStateId::new(id)
            .map(|id| id.to_state())
            .ok_or(ApplyFailure::InvalidAction)
    };
    let collisions = pumpkin_world::collision::collect_block_collisions(
        bounds.stretch(velocity),
        4096,
        read,
        |_, _| None,
    )
    .map_err(|_| ApplyFailure::InvalidAction)?;
    let collision = pumpkin_util::math::collision::resolve_block_movement(
        bounds,
        velocity,
        &collisions.shapes,
        &collisions.positions,
    );
    let moved = position + collision.movement;
    if collision.movement.x != velocity.x {
        velocity.x = 0.0;
    }
    if collision.movement.z != velocity.z {
        velocity.z = 0.0;
    }
    let on_ground = collision.supporting_block.is_some();
    if on_ground {
        if matches!(object.kind, ObjectKind::PrimedTnt(_)) {
            velocity.x *= 0.7;
            velocity.z *= 0.7;
            velocity.y *= -0.5;
        } else {
            velocity.y = 0.0;
        }
    } else if collision.movement.y != velocity.y {
        velocity.y = 0.0;
    }
    velocity.x *= 0.98;
    velocity.y *= 0.98;
    velocity.z *= 0.98;
    let mut spatial = original.clone();
    spatial.position = [moved.x.to_bits(), moved.y.to_bits(), moved.z.to_bits()];
    spatial.velocity_micro =
        [velocity.x, velocity.y, velocity.z].map(|value| (value * 1_000_000.0).round() as i64);
    spatial.on_ground = on_ground;
    Ok(ObjectMotion {
        destination: ChunkKey::new(
            original.world,
            (moved.x.floor() as i32).div_euclid(16),
            (moved.z.floor() as i32).div_euclid(16),
        ),
        spatial,
        landing: collision.supporting_block,
        fall_distance: prior_distance + (-collision.movement.y).max(0.0) as f32,
    })
}
