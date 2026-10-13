use pumpkin_cluster::canonical_action::{ActionState, ApplyFailure};
use pumpkin_cluster::canonical_block_inventory_tick::BlockInventoryTickRoot;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_data::block_properties::{ChestLikeProperties, ChestType};

pub(super) fn positions(state: pumpkin_data::BlockStateId, position: BlockPos) -> Result<Vec<BlockPos>, ApplyFailure> {
    if !matches!(state.to_block().name, "chest" | "trapped_chest") { return Ok(vec![position]); }
    let properties = ChestLikeProperties::from_state_id(state);
    let direction = match properties.r#type {
        ChestType::Single => return Ok(vec![position]),
        ChestType::Left => properties.facing.rotate_clockwise(),
        ChestType::Right => properties.facing.rotate_counter_clockwise(),
    };
    let offset = direction.to_offset();
    let other = BlockPos { x: position.x.checked_add(offset.x).ok_or(ApplyFailure::InvalidAction)?,
        y: position.y.checked_add(offset.y).ok_or(ApplyFailure::InvalidAction)?, z: position.z.checked_add(offset.z).ok_or(ApplyFailure::InvalidAction)? };
    Ok(if properties.r#type == ChestType::Right { vec![position, other] } else { vec![other, position] })
}

pub(super) fn valid_pair(right: pumpkin_data::BlockStateId, left: pumpkin_data::BlockStateId) -> bool {
    if right.to_block() != left.to_block() || !matches!(right.to_block().name, "chest" | "trapped_chest") { return false; }
    let right = ChestLikeProperties::from_state_id(right);
    let left = ChestLikeProperties::from_state_id(left);
    right.r#type == ChestType::Right && left.r#type == ChestType::Left && right.facing == left.facing
}

pub(super) fn capture<S: ActionState>(original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot, view: &S,
    chunk: ChunkKey, position: BlockPos) -> Result<Vec<BlockInventoryTickRoot>, ApplyFailure> {
    let first = super::cluster_inventory_block_tick::capture_block_inventory_root(original, view, chunk, position)?;
    let state = pumpkin_data::BlockStateId::new(first.state).ok_or(ApplyFailure::InvalidAction)?;
    let locations = positions(state, position)?;
    if locations.len() == 1 { return Ok(vec![first]); }
    let mut roots = Vec::with_capacity(2);
    for location in locations {
        let key = ChunkKey::new(chunk.world, location.x.div_euclid(16), location.z.div_euclid(16));
        let root = if location == position { first.clone() } else { super::cluster_inventory_block_tick::capture_block_inventory_root(original, view, key, location)? };
        roots.push(root);
    }
    if !valid_pair(pumpkin_data::BlockStateId::new(roots[0].state).ok_or(ApplyFailure::InvalidAction)?,
        pumpkin_data::BlockStateId::new(roots[1].state).ok_or(ApplyFailure::InvalidAction)?) { return Err(ApplyFailure::InvalidAction); }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_data::block_properties::HorizontalFacing;

    #[test]
    fn paired_chest_order_is_identical_from_either_leg_across_a_chunk_boundary() {
        let mut right = ChestLikeProperties::from_state_id(pumpkin_data::Block::CHEST.default_state.id);
        right.facing = HorizontalFacing::North; right.r#type = ChestType::Right;
        let mut left = right.clone(); left.r#type = ChestType::Left;
        let right_state = right.to_state_id(&pumpkin_data::Block::CHEST);
        let left_state = left.to_state_id(&pumpkin_data::Block::CHEST);
        let right_position = BlockPos { x: 16, y: 64, z: 0 };
        let left_position = BlockPos { x: 15, y: 64, z: 0 };
        let ordered = vec![right_position, left_position];
        assert_eq!(positions(right_state, right_position).unwrap(), ordered);
        assert_eq!(positions(left_state, left_position).unwrap(), ordered);
        assert!(valid_pair(right_state, left_state));
        left.facing = HorizontalFacing::East;
        assert!(!valid_pair(right_state, left.to_state_id(&pumpkin_data::Block::CHEST)));
        assert!(!valid_pair(right_state, right_state));
    }
}
