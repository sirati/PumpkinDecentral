use pumpkin_data::{
    Block, BlockDirection, BlockId, BlockStateId,
    block_properties::{WaterLikeProperties, blocks_movement},
    fluid::{Fluid, FluidState},
};
use pumpkin_util::math::{
    boundingbox::BoundingBox, item_motion::FluidContact, position::BlockPos, vector3::Vector3,
};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum FluidReadFailure<E> {
    InvalidPosition,
    Budget,
    Read { position: BlockPos, reason: E },
}

#[derive(Default)]
pub struct EntityFluidContacts {
    pub contacts: [FluidContact; 2],
    pub fluids: Vec<&'static Fluid>,
}

pub fn sample_entity_fluid_contacts<E>(
    entity_box: BoundingBox,
    sample_budget: usize,
    pushed: bool,
    mut read: impl FnMut(BlockPos) -> Result<BlockStateId, E>,
) -> Result<EntityFluidContacts, FluidReadFailure<E>> {
    let bounds = entity_box.expand(-0.001, -0.001, -0.001);
    let lower = [bounds.min.x, bounds.min.y, bounds.min.z];
    let upper = [bounds.max.x, bounds.max.y, bounds.max.z];
    if lower.iter().zip(upper).any(|(low, high)| {
        !low.is_finite()
            || !high.is_finite()
            || *low > high
            || low.floor() <= f64::from(i32::MIN)
            || high.floor() >= f64::from(i32::MAX)
    }) {
        return Err(FluidReadFailure::InvalidPosition);
    }
    let lower = lower.map(|value| value.floor() as i32);
    let upper = upper.map(|value| value.floor() as i32);
    let samples = lower
        .iter()
        .zip(upper)
        .try_fold(1usize, |count, (low, high)| {
            usize::try_from(i64::from(high) - i64::from(*low) + 1)
                .ok()
                .and_then(|width| count.checked_mul(width))
        });
    if samples.is_none_or(|samples| samples > sample_budget) {
        return Err(FluidReadFailure::Budget);
    }
    let mut contacts = [FluidContact::default(); 2];
    let mut fluids = BTreeMap::new();
    for x in lower[0]..=upper[0] {
        for y in lower[1]..=upper[1] {
            for z in lower[2]..=upper[2] {
                let position = BlockPos::new(x, y, z);
                let id =
                    read(position).map_err(|reason| FluidReadFailure::Read { position, reason })?;
                let (fluid, state) = fluid_state_from_block_state(id);
                if fluid.id == Fluid::EMPTY.id {
                    continue;
                }
                let surface =
                    f64::from(fluid_height(position, fluid, &state, &mut read)?) + f64::from(y);
                if surface < bounds.min.y {
                    continue;
                }
                let flow = if pushed {
                    Some(fluid_velocity(position, fluid, &state, &mut read)?)
                } else {
                    None
                };
                let index =
                    usize::from(fluid.id == Fluid::FLOWING_LAVA.id || fluid.id == Fluid::LAVA.id);
                contacts[index].observe(surface - entity_box.min.y, flow);
                fluids.insert(fluid.id, fluid);
            }
        }
    }
    Ok(EntityFluidContacts {
        contacts,
        fluids: fluids.into_values().collect(),
    })
}

pub fn fluid_height<E>(
    position: BlockPos,
    fluid: &Fluid,
    state: &FluidState,
    mut read: impl FnMut(BlockPos) -> Result<BlockStateId, E>,
) -> Result<f32, FluidReadFailure<E>> {
    if state.is_empty {
        return Ok(0.0);
    }
    if position.0.y == i32::MAX {
        return Err(FluidReadFailure::InvalidPosition);
    }
    let above = position.up();
    let id = read(above).map_err(|reason| FluidReadFailure::Read {
        position: above,
        reason,
    })?;
    Ok(if fluid.matches_type(fluid_from_block_state(id)) {
        1.0
    } else {
        state.height
    })
}

pub fn fluid_velocity<E>(
    position: BlockPos,
    fluid: &Fluid,
    state: &FluidState,
    mut read: impl FnMut(BlockPos) -> Result<BlockStateId, E>,
) -> Result<Vector3<f64>, FluidReadFailure<E>> {
    if [position.0.x, position.0.y, position.0.z]
        .iter()
        .any(|coordinate| *coordinate == i32::MIN || *coordinate == i32::MAX)
    {
        return Err(FluidReadFailure::InvalidPosition);
    }
    let mut read =
        |position| read(position).map_err(|reason| FluidReadFailure::Read { position, reason });
    let directions = [
        BlockDirection::North,
        BlockDirection::South,
        BlockDirection::West,
        BlockDirection::East,
    ];
    let mut velocity = Vector3::default();
    for direction in directions {
        let offset = direction.to_offset();
        let neighbor = position.offset(offset);
        let id = read(neighbor)?;
        let (neighbor_fluid, neighbor_state) = fluid_state_from_block_state(id);
        if neighbor_fluid.matches_type(fluid) {
            let mut neighbor_height = neighbor_state.height;
            let mut amplitude = 0.0;
            if neighbor_height == 0.0 {
                if !blocks_movement(id.to_state(), id.to_block_id()) {
                    let (down_fluid, down_state) =
                        fluid_state_from_block_state(read(neighbor.down())?);
                    if down_fluid.matches_type(fluid) {
                        neighbor_height = down_state.height;
                        if neighbor_height > 0.0 {
                            amplitude = f64::from(state.height)
                                - (f64::from(neighbor_height) - 0.888_888_9);
                        }
                    }
                }
            } else if neighbor_height > 0.0 {
                amplitude = f64::from(state.height) - f64::from(neighbor_height);
            }
            if amplitude != 0.0 {
                velocity.x += f64::from(offset.x) * amplitude;
                velocity.z += f64::from(offset.z) * amplitude;
            }
        }
    }
    if state.falling {
        for direction in directions {
            let neighbor = position.offset(direction.to_offset());
            if fluid_solid_face(fluid.id, read(neighbor)?, direction)
                || fluid_solid_face(fluid.id, read(neighbor.up())?, direction)
            {
                if velocity.length_squared() != 0.0 {
                    velocity = velocity.normalize();
                }
                velocity.y -= 6.0;
                break;
            }
        }
    }
    Ok(if velocity.length_squared() == 0.0 {
        velocity
    } else {
        velocity.normalize()
    })
}

// FlowingFluid.isSolidFace()
fn fluid_solid_face(fluid_id: u16, id: BlockStateId, direction: BlockDirection) -> bool {
    let fluid = Fluid::from_state_id(id).unwrap_or(&Fluid::EMPTY);
    if Fluid::same_fluid_type(fluid.id, fluid_id) {
        return false;
    }
    if direction == BlockDirection::Up {
        return true;
    }
    let block = Block::from_state_id(id);
    // Doesn't count blue ice or packed ice
    if block == &Block::ICE || block == &Block::FROSTED_ICE {
        return false;
    }
    id.to_state().is_side_solid(direction)
}

pub fn fluid_from_block_state(id: BlockStateId) -> &'static Fluid {
    if let Some(fluid) = Fluid::from_state_id(id) {
        return fluid.to_flowing();
    }
    // These blocks contain source water without a `waterlogged` property.
    if matches!(
        id.to_block_id(),
        BlockId::KELP
            | BlockId::KELP_PLANT
            | BlockId::SEAGRASS
            | BlockId::TALL_SEAGRASS
            | BlockId::BUBBLE_COLUMN
    ) || id.is_waterlogged()
    {
        &Fluid::FLOWING_WATER
    } else {
        &Fluid::EMPTY
    }
}

pub fn fluid_state_from_block_state(id: BlockStateId) -> (&'static Fluid, FluidState) {
    let fluid = fluid_from_block_state(id);
    let source = if fluid.matches_type(&Fluid::WATER) {
        &Fluid::WATER
    } else if fluid.matches_type(&Fluid::LAVA) {
        &Fluid::LAVA
    } else {
        &Fluid::EMPTY
    };
    let mut state = source.states[source.default_state_index as usize].clone();
    if matches!(id.to_block_id(), BlockId::WATER | BlockId::LAVA) {
        // LiquidBlock#getFluidState: source, amounts 7..1, then falling amount 8.
        // The fluid family's first state is not the state of the actual block.
        let level = WaterLikeProperties::from_state_id(id).level;
        let amount = if level == 0 || level >= 8 {
            8
        } else {
            8 - level
        };
        state.height = f32::from(amount) / 9.0;
        state.level = i16::from(amount);
        state.is_source = level == 0;
        state.is_still = state.is_source;
        state.falling = level >= 8;
        state.block_state_id = WaterLikeProperties {
            level: level.min(8),
        }
        .to_state_id(id.to_block());
    }
    // Keep the normalized family used by fluid callbacks, independently of source state.
    (fluid, state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_contact_sampling_retains_fluid_callbacks_and_depth_without_mutation() {
        let bounds = BoundingBox {
            min: Vector3::new(0.375, 80.0, 0.375),
            max: Vector3::new(0.625, 80.25, 0.625),
        };
        let sampled = sample_entity_fluid_contacts::<()>(bounds, 1, true, |_| {
            Ok(Block::WATER.default_state.id)
        })
        .unwrap();
        assert!(sampled.contacts[0].touching);
        assert_eq!(sampled.contacts[0].height, 1.0);
        assert_eq!(sampled.contacts[0].samples, 1);
        assert_eq!(sampled.contacts[0].push, Vector3::default());
        assert!(!sampled.contacts[1].touching);
        assert_eq!(sampled.fluids.len(), 1);
        assert_eq!(sampled.fluids[0].id, Fluid::FLOWING_WATER.id);
        assert!(matches!(
            sample_entity_fluid_contacts::<()>(bounds, 0, true, |_| {
                panic!("budget must be checked before reads")
            }),
            Err(FluidReadFailure::Budget)
        ));
    }

    #[test]
    fn column_height_requires_same_fluid_above_without_changing_own_flow_depth() {
        let (fluid, state) = fluid_state_from_block_state(
            WaterLikeProperties { level: 4 }.to_state_id(&Block::WATER),
        );
        let position = BlockPos::new(0, 80, 0);
        assert_eq!(
            fluid_height::<()>(position, fluid, &state, |_| Ok(Block::WATER
                .default_state
                .id)),
            Ok(1.0)
        );
        assert_eq!(
            fluid_height::<()>(position, fluid, &state, |_| Ok(Block::LAVA
                .default_state
                .id)),
            Ok(state.height)
        );
        assert_eq!(state.height, 4.0 / 9.0);
        assert_eq!(
            fluid_height(position, fluid, &state, |_| Err("missing")),
            Err(FluidReadFailure::Read {
                position: position.up(),
                reason: "missing",
            })
        );
    }

    #[test]
    fn source_water_flows_toward_lower_east_surface() {
        let position = BlockPos::new(0, 80, 0);
        let (fluid, state) = fluid_state_from_block_state(Block::WATER.default_state.id);
        let velocity = fluid_velocity::<()>(position, fluid, &state, |at| {
            Ok(if at == BlockPos::new(1, 80, 0) {
                WaterLikeProperties { level: 4 }.to_state_id(&Block::WATER)
            } else {
                Block::WATER.default_state.id
            })
        })
        .unwrap();
        assert_eq!(velocity, Vector3::new(1.0, 0.0, 0.0));
    }

    #[test]
    fn falling_water_gets_downward_wall_flow() {
        let position = BlockPos::new(0, 80, 0);
        let falling = WaterLikeProperties { level: 8 }.to_state_id(&Block::WATER);
        let (fluid, state) = fluid_state_from_block_state(falling);
        let velocity = fluid_velocity::<()>(position, fluid, &state, |at| {
            Ok(if at == BlockPos::new(0, 80, -1) {
                Block::STONE.default_state.id
            } else {
                falling
            })
        })
        .unwrap();
        assert_eq!(velocity, Vector3::new(0.0, -1.0, 0.0));
    }

    #[test]
    fn missing_neighbor_is_evidence_failure_instead_of_air() {
        let (fluid, state) = fluid_state_from_block_state(Block::WATER.default_state.id);
        assert_eq!(
            fluid_velocity(BlockPos::new(0, 80, 0), fluid, &state, |_| Err("missing")),
            Err(FluidReadFailure::Read {
                position: BlockPos::new(0, 80, -1),
                reason: "missing",
            })
        );
        assert_eq!(
            fluid_velocity(BlockPos::new(i32::MAX, 80, 0), fluid, &state, |_| {
                panic!("invalid coordinate must not be read")
            }),
            Err(FluidReadFailure::<()>::InvalidPosition)
        );
    }
}
