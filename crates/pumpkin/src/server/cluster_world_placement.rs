use pumpkin_data::{Block, BlockDirection, BlockStateId, block_properties::HorizontalFacing};
use pumpkin_util::math::position::BlockPos;

use crate::block::neighbor::PlacementRequest;
use crate::block::registry::BlockRegistry;
use crate::block::{BlockIsReplacing, CanUpdateIntentArgs, NeighborUpdateWorld};

/// Infers placement location and replacement policy from the current symbolic blocks.
/// The caller resolves the certified placement hook and the complete occupancy domain.
pub(crate) fn target<'a>(
    registry: &BlockRegistry,
    view: &dyn NeighborUpdateWorld,
    rules: &pumpkin_cluster::world_rules::WorldRulesSnapshot,
    world: pumpkin_cluster::protocol::WorldId,
    item: u16,
    clicked: BlockPos,
    face: BlockDirection,
    hit: [f32; 3],
    facing: HorizontalFacing,
) -> Option<PlacementRequest<'a>> {
    if !rules.valid() || rules.rules.world != world {
        return None;
    }
    let maximum = rules.min_y.checked_add(rules.height as i32)?;
    target_in_bounds(
        registry,
        view,
        (rules.min_y, maximum),
        item,
        clicked,
        face,
        hit,
        facing,
    )
}

fn target_in_bounds<'a>(
    registry: &BlockRegistry,
    view: &dyn NeighborUpdateWorld,
    bounds: (i32, i32),
    item: u16,
    clicked: BlockPos,
    face: BlockDirection,
    hit: [f32; 3],
    facing: HorizontalFacing,
) -> Option<PlacementRequest<'a>> {
    if bounds.0 >= bounds.1
        || hit
            .iter()
            .any(|value| !value.is_finite() || !(0.0..=1.0).contains(value))
    {
        return None;
    }
    if clicked.0.y < bounds.0 || clicked.0.y >= bounds.1 {
        return None;
    }
    let placed = Block::from_item_id(item)?;
    let classify = |position: BlockPos, direction: BlockDirection, clicked: bool| {
        let state = view.get_block_state_id(&position);
        let block = state.to_block();
        if block == placed {
            let replace = match registry.get_pumpkin_block(block.id) {
                Some(behavior) => behavior.can_update_intent(CanUpdateIntentArgs {
                    world: view,
                    block,
                    state_id: state,
                    position: &position,
                    direction,
                    hit,
                })?,
                None => false,
            };
            Some(replace.then_some(BlockIsReplacing::Itself(state)))
        } else if block == &Block::SCULK_VEIN || state.to_state().replaceable() {
            Some(Some(if block == &Block::WATER {
                BlockIsReplacing::Water(
                    pumpkin_data::block_properties::WaterLikeProperties::from_state_id(state).level,
                )
            } else if clicked {
                BlockIsReplacing::Other
            } else {
                BlockIsReplacing::None
            }))
        } else {
            Some(None)
        }
    };
    let (position, replacing) = if let Some(replacing) = classify(clicked, face, true)? {
        (clicked, replacing)
    } else {
        let delta = face.to_offset();
        let adjacent = BlockPos::new(
            clicked.0.x.checked_add(delta.x)?,
            clicked.0.y.checked_add(delta.y)?,
            clicked.0.z.checked_add(delta.z)?,
        );
        if adjacent.0.y < bounds.0 || adjacent.0.y >= bounds.1 {
            return None;
        }
        let replacing = classify(adjacent, face.opposite(), false)??;
        (adjacent, replacing)
    };
    if position.0.y < bounds.0 || position.0.y >= bounds.1 {
        return None;
    }
    Some(PlacementRequest {
        block: placed,
        position,
        direction: face.opposite(),
        facing,
        replacing,
        hit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::neighbor::{NeighborSnapshot, NeighborTranscript};
    use pumpkin_data::block_properties::{ResinBrickSlabLikeProperties, SlabType};

    #[test]
    fn solid_same_block_places_adjacent_but_matching_slab_stacks_at_clicked_location() {
        let registry = crate::block::registry::default_registry();
        let clicked = BlockPos::new(0, 8, 0);
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(clicked, Block::STONE.default_state.id);
        snapshot.blocks.insert(clicked.up(), BlockStateId::AIR);
        let view = NeighborTranscript::new(snapshot, 32).unwrap();
        let request = target_in_bounds(
            &registry,
            &view,
            (0, 16),
            Block::STONE.item_id,
            clicked,
            BlockDirection::Up,
            [0.5; 3],
            HorizontalFacing::North,
        )
        .unwrap();
        assert_eq!(request.position, clicked.up());
        assert_eq!(request.direction, BlockDirection::Down);
        assert!(view.finish().is_some());

        let mut properties = ResinBrickSlabLikeProperties::default(&Block::STONE_SLAB);
        properties.r#type = SlabType::Bottom;
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(clicked, properties.to_state_id(&Block::STONE_SLAB));
        let view = NeighborTranscript::new(snapshot, 32).unwrap();
        let request = target_in_bounds(
            &registry,
            &view,
            (0, 16),
            Block::STONE_SLAB.item_id,
            clicked,
            BlockDirection::Up,
            [0.5; 3],
            HorizontalFacing::North,
        )
        .unwrap();
        assert_eq!(request.position, clicked);
        assert!(matches!(request.replacing, BlockIsReplacing::Itself(_)));
        assert!(view.finish().is_some());
    }
}
