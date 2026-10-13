use pumpkin_data::block_properties::EndPortalFrameLikeProperties;
use pumpkin_data::{Block, BlockDirection, BlockStateId};
use pumpkin_util::math::{position::BlockPos, vector3::Vector3};

pub(super) fn completed_interior<E>(
    position: BlockPos,
    mut read: impl FnMut(BlockPos) -> Result<BlockStateId, E>,
) -> Result<Option<Vec<BlockPos>>, E> {
    let state = read(position)?;
    if state.to_block() != &Block::END_PORTAL_FRAME {
        return Ok(None);
    }
    let facing = EndPortalFrameLikeProperties::from_state_id(state).facing;
    let left = read(position.offset_dir(facing.rotate_clockwise().to_offset(), 1))?;
    let right = read(position.offset_dir(facing.rotate_counter_clockwise().to_offset(), 1))?;
    let offset = match (
        left.to_block() == &Block::END_PORTAL_FRAME,
        right.to_block() == &Block::END_PORTAL_FRAME,
    ) {
        (true, true) => 0,
        (true, false) => -1,
        (false, true) => 1,
        _ => return Ok(None),
    };
    let center = position
        .offset_dir(facing.to_offset(), 2)
        .offset_dir(facing.rotate_counter_clockwise().to_offset(), offset);
    for direction in BlockDirection::horizontal() {
        let middle = center.offset_dir(direction.to_offset(), 2);
        for position in [
            middle,
            middle.offset_dir(direction.rotate_clockwise().to_offset(), 1),
            middle.offset_dir(direction.rotate_counter_clockwise().to_offset(), 1),
        ] {
            let state = read(position)?;
            if state.to_block() != &Block::END_PORTAL_FRAME {
                return Ok(None);
            }
            let properties = EndPortalFrameLikeProperties::from_state_id(state);
            if !properties.eye || properties.facing != direction.to_facing().opposite() {
                return Ok(None);
            }
        }
    }
    Ok(Some(
        (-1..=1)
            .flat_map(|x| (-1..=1).map(move |z| center.offset(Vector3::new(x, 0, z))))
            .collect(),
    ))
}
