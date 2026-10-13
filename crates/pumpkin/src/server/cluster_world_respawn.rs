use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{ApplyFailure, Cell, Target, Value};
use pumpkin_cluster::protocol::BlockPos;
use pumpkin_cluster::world_respawn::{WorldRespawnPlan, WorldRespawnResolution};
use pumpkin_data::tag::Taggable;
use pumpkin_data::{Block, BlockStateId};

fn state(
    plan: WorldRespawnPlan,
    values: &BTreeMap<Target, Cell>,
    position: BlockPos,
) -> Result<BlockStateId, ApplyFailure> {
    let target = plan.target(position);
    let Some(Cell {
        value: Value::Block(id),
        ..
    }) = values.get(&target)
    else {
        return Err(ApplyFailure::Condition(target));
    };
    BlockStateId::new(*id).ok_or(ApplyFailure::Type(target))
}

fn offset(position: BlockPos, x: i32, y: i32, z: i32) -> Result<BlockPos, ApplyFailure> {
    Ok(BlockPos {
        x: position
            .x
            .checked_add(x)
            .ok_or(ApplyFailure::InvalidAction)?,
        y: position
            .y
            .checked_add(y)
            .ok_or(ApplyFailure::InvalidAction)?,
        z: position
            .z
            .checked_add(z)
            .ok_or(ApplyFailure::InvalidAction)?,
    })
}

fn safe(
    plan: WorldRespawnPlan,
    values: &BTreeMap<Target, Cell>,
    position: BlockPos,
    forced: bool,
) -> Result<Option<[u64; 3]>, ApplyFailure> {
    let height = f64::from_bits(plan.height);
    let half_width = f64::from_bits(plan.half_width);
    if !plan.contains_horizontal_box(f64::from(position.x) + 0.5, f64::from(position.z) + 0.5)
        || position.y < plan.min_y
        || (!forced && position.y == plan.min_y)
        || f64::from(position.y) + height + 0.1 >= f64::from(plan.max_y)
        || [position.x, position.z].iter().any(|&coordinate| {
            f64::from(coordinate) + 0.5 - half_width < -30_000_000.0
                || f64::from(coordinate) + 0.5 + half_width >= 30_000_000.0
        })
    {
        return Ok(None);
    }
    let feet = state(plan, values, position)?;
    let head = state(plan, values, offset(position, 0, 1, 0)?)?;
    if forced {
        if (!feet.to_state().is_air() && feet.to_state().is_solid())
            || (!head.to_state().is_air() && head.to_state().is_solid())
        {
            return Ok(None);
        }
    } else {
        if feet
            .to_block()
            .has_tag(&pumpkin_data::tag::Block::MINECRAFT_INVALID_SPAWN_INSIDE)
            || head
                .to_block()
                .has_tag(&pumpkin_data::tag::Block::MINECRAFT_INVALID_SPAWN_INSIDE)
        {
            return Ok(None);
        }
        let floor = state(plan, values, offset(position, 0, -1, 0)?)?;
        if (!floor.to_state().is_solid() && !feet.to_state().is_solid())
            || (feet.to_state().is_solid() && !feet.to_state().is_air())
        {
            return Ok(None);
        }
    }
    let center = [
        f64::from(position.x) + 0.5,
        f64::from(position.y) + 0.1,
        f64::from(position.z) + 0.5,
    ];
    let min = [center[0] - half_width, center[1], center[2] - half_width];
    let max = [
        center[0] + half_width,
        center[1] + height,
        center[2] + half_width,
    ];
    for y in min[1].floor() as i32..=max[1].floor() as i32 {
        for z in min[2].floor() as i32..=max[2].floor() as i32 {
            for x in min[0].floor() as i32..=max[0].floor() as i32 {
                let position = BlockPos { x, y, z };
                let block = state(plan, values, position)?;
                let local = pumpkin_util::math::position::BlockPos::new(x, y, z);
                for shape in block.to_state().get_block_collision_shapes_at(&local) {
                    let origin = [f64::from(x), f64::from(y), f64::from(z)];
                    let shape_min = [shape.min.x, shape.min.y, shape.min.z];
                    let shape_max = [shape.max.x, shape.max.y, shape.max.z];
                    if (0..3).all(|axis| {
                        min[axis] < shape_max[axis] + origin[axis]
                            && max[axis] > shape_min[axis] + origin[axis]
                    }) {
                        return Ok(None);
                    }
                }
            }
        }
    }
    Ok(Some(center.map(f64::to_bits)))
}

pub(super) fn resolve(
    plan: WorldRespawnPlan,
    values: &BTreeMap<Target, Cell>,
) -> Result<Option<WorldRespawnResolution>, ApplyFailure> {
    for target in plan.targets()? {
        if !matches!(values.get(&target), Some(Cell { value: Value::Block(id), .. }) if BlockStateId::new(*id).is_some())
        {
            return Err(ApplyFailure::Condition(target));
        }
    }
    let position = plan.point.position;
    if plan.point.forced {
        return Ok(
            safe(plan, values, position, true)?.map(|position| WorldRespawnResolution {
                position,
                yaw: plan.point.yaw,
                anchor: None,
            }),
        );
    }
    let block = state(plan, values, position)?;
    let mut candidates = Vec::new();
    let mut anchor = None;
    if block
        .to_block()
        .has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
        && plan.bed_works
    {
        use pumpkin_data::block_properties::{HorizontalFacing, WhiteBedLikeProperties};
        let (fx, fz) = match WhiteBedLikeProperties::from_state_id(block).facing {
            HorizontalFacing::North => (0, -1),
            HorizontalFacing::South => (0, 1),
            HorizontalFacing::West => (-1, 0),
            HorizontalFacing::East => (1, 0),
        };
        let (rx, rz) = (-fz, fx);
        for (x, z) in [
            (rx, rz),
            (-rx, -rz),
            (rx - fx, rz - fz),
            (-rx - fx, -rz - fz),
            (-fx, -fz),
            (-fx * 2, -fz * 2),
            (rx + fx, rz + fz),
            (-rx + fx, -rz + fz),
            (fx, fz),
            (rx - fx * 2, rz - fz * 2),
        ] {
            candidates.push(offset(position, x, 0, z)?);
            candidates.push(offset(position, x, -1, z)?);
        }
        candidates.push(position);
    } else if block.to_block() == &Block::RESPAWN_ANCHOR && plan.anchor_works {
        let mut properties =
            pumpkin_data::block_properties::RespawnAnchorLikeProperties::from_state_id(block);
        if properties.charges == 0 {
            return Ok(None);
        }
        properties.charges -= 1;
        anchor = Some((
            plan.target(position),
            block.as_u16(),
            properties.to_state_id(&Block::RESPAWN_ANCHOR).as_u16(),
        ));
        for y in [0, -1, 1] {
            for (x, z) in [
                (0, -1),
                (-1, 0),
                (0, 1),
                (1, 0),
                (-1, -1),
                (1, -1),
                (-1, 1),
                (1, 1),
            ] {
                candidates.push(offset(position, x, y, z)?);
            }
        }
        candidates.push(offset(position, 0, 1, 0)?);
    } else {
        return Ok(None);
    }
    for candidate in candidates {
        if let Some(position) = safe(plan, values, candidate, false)? {
            return Ok(Some(WorldRespawnResolution {
                position,
                yaw: plan.point.yaw,
                anchor,
            }));
        }
    }
    Ok(None)
}

pub(super) fn respawn_light_plans(
    server: &super::Server,
    preferred: Option<WorldRespawnPlan>,
) -> Result<Vec<pumpkin_cluster::world_action::WorldLightPlan>, ApplyFailure> {
    let Some(plan) = preferred.filter(|plan| plan.anchor_works && !plan.point.forced) else {
        return Ok(Vec::new());
    };
    let world = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == plan.point.world)
        .cloned()
        .ok_or(ApplyFailure::InvalidAction)?;
    let mode = match world.level.lighting_config {
        pumpkin_config::lighting::LightingEngineConfig::Default => {
            pumpkin_cluster::world_action::WorldLightMode::Default
        }
        pumpkin_config::lighting::LightingEngineConfig::Full => {
            pumpkin_cluster::world_action::WorldLightMode::Full
        }
        pumpkin_config::lighting::LightingEngineConfig::Dark => {
            pumpkin_cluster::world_action::WorldLightMode::Dark
        }
    };
    let center_x = plan.point.position.x.div_euclid(16);
    let center_z = plan.point.position.z.div_euclid(16);
    let mut plans = Vec::with_capacity(9);
    for z in -1..=1 {
        for x in -1..=1 {
            plans.push(
                pumpkin_cluster::world_action::WorldLightPlan {
                    center: pumpkin_cluster::protocol::ChunkKey::new(
                        plan.point.world,
                        center_x.checked_add(x).ok_or(ApplyFailure::InvalidAction)?,
                        center_z.checked_add(z).ok_or(ApplyFailure::InvalidAction)?,
                    ),
                    min_section_y: world.dimension.min_y.div_euclid(16),
                    section_count: (world.dimension.height / 16)
                        .try_into()
                        .map_err(|_| ApplyFailure::InvalidAction)?,
                    has_skylight: world.dimension.has_skylight,
                    mode,
                    config_version: 1,
                }
                .with_config_fingerprint(),
            );
        }
    }
    Ok(plans)
}
