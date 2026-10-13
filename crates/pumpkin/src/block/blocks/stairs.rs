use crate::entity::EntityBase;
use pumpkin_data::block_properties::Half;
use pumpkin_data::block_properties::HorizontalFacing;
use pumpkin_data::block_properties::StairsShape;
use pumpkin_data::tag::Taggable;
use pumpkin_data::{Block, BlockDirection, BlockState, BlockStateId, Mirror, Rotation, tag};
use pumpkin_macros::pumpkin_block_from_tag;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::world::BlockFlags;

use crate::block::{BlockBehaviour, OnNeighborUpdateArgs, OnPlaceArgs, PathComputationType};
use pumpkin_world::world::BlockAccessor;

type StairsProperties = pumpkin_data::block_properties::OakStairsLikeProperties;

#[pumpkin_block_from_tag("minecraft:stairs")]
pub struct StairBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for StairBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = StairsProperties::default(args.block);
        properties.waterlogged = args.replacing.water_source();
        properties.facing = args.facing;
        properties.half = match args.direction {
            BlockDirection::Up => Half::Top,
            BlockDirection::Down => Half::Bottom,
            _ => {
                if args.hit[1] < 0.5 {
                    Half::Bottom
                } else {
                    Half::Top
                }
            }
        };
        properties.shape = compute_stair_shape(
            args.world,
            args.position,
            properties.facing,
            properties.half,
        );
        Some(properties.to_state_id(args.block))
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut stair_props = StairsProperties::default(args.block);
        stair_props.waterlogged = args.replacing.water_source();

        stair_props.facing = args.player.get_entity().get_horizontal_facing();
        stair_props.half = match args.direction {
            BlockDirection::Up => Half::Top,
            BlockDirection::Down => Half::Bottom,
            _ => match args.use_item_on.cursor_pos.y {
                0.0..0.5 => Half::Bottom,
                0.5..1.0 => Half::Top,

                // This cannot happen normally
                _ => Half::Bottom,
            },
        };

        stair_props.shape = compute_stair_shape(
            args.world,
            args.position,
            stair_props.facing,
            stair_props.half,
        );

        stair_props.to_state_id(args.block)
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        crate::block::neighbor::apply_legacy_neighbor_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        if !matches!(args.event, BlockCallbackEvent::Neighbor { .. }) {
            return Some(Vec::new());
        }
        let state = args.world.get_block_state_id(args.position);
        if state.to_block_id() != args.block.id {
            return Some(Vec::new());
        }
        let mut properties = StairsProperties::from_state_id(state);
        let shape = compute_stair_shape(
            args.world,
            args.position,
            properties.facing,
            properties.half,
        );
        if properties.shape == shape {
            return Some(Vec::new());
        }
        properties.shape = shape;
        Some(vec![BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: properties.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_ALL,
        }])
    }

    fn rotate(
        &self,
        block: &Block,
        state_id: BlockStateId,
        rotation: Rotation,
    ) -> &'static BlockState {
        let mut stair_props = StairsProperties::from_state_id(state_id);
        stair_props.facing = rotation.rotate_horizontal(stair_props.facing);
        BlockState::from_id(stair_props.to_state_id(block))
    }

    fn mirror(&self, block: &Block, state_id: BlockStateId, mirror: Mirror) -> &'static BlockState {
        let mut stair_props = StairsProperties::from_state_id(state_id);
        let direction = stair_props.facing;
        let shape = stair_props.shape;

        match mirror {
            Mirror::LeftRight => {
                if matches!(direction, HorizontalFacing::North | HorizontalFacing::South) {
                    stair_props.facing = stair_props.facing.opposite();
                    stair_props.shape = match shape {
                        StairsShape::Straight => StairsShape::Straight,
                        StairsShape::OuterLeft => StairsShape::OuterRight,
                        StairsShape::InnerRight => StairsShape::InnerLeft,
                        StairsShape::InnerLeft => StairsShape::InnerRight,
                        StairsShape::OuterRight => StairsShape::OuterLeft,
                    };
                    return BlockState::from_id(stair_props.to_state_id(block));
                }
            }
            Mirror::FrontBack => {
                if matches!(direction, HorizontalFacing::East | HorizontalFacing::West) {
                    stair_props.facing = stair_props.facing.opposite();
                    stair_props.shape = match shape {
                        StairsShape::Straight => StairsShape::Straight,
                        StairsShape::OuterLeft => StairsShape::OuterRight,
                        StairsShape::InnerRight => StairsShape::InnerRight,
                        StairsShape::InnerLeft => StairsShape::InnerLeft,
                        StairsShape::OuterRight => StairsShape::OuterLeft,
                    };
                    return BlockState::from_id(stair_props.to_state_id(block));
                }
            }
            Mirror::None => {}
        }

        BlockState::from_id(state_id)
    }

    fn is_pathfindable(&self, _state: &BlockState, _computation_type: PathComputationType) -> bool {
        false
    }
}

fn compute_stair_shape(
    world: &dyn BlockAccessor,
    block_pos: &BlockPos,
    facing: HorizontalFacing,
    half: Half,
) -> StairsShape {
    let right_locked = get_stair_properties_if_exists(
        world,
        &block_pos.offset(facing.rotate_clockwise().to_offset()),
    )
    .is_some_and(|other_stair_props| {
        other_stair_props.half == half && other_stair_props.facing == facing
    });

    let left_locked = get_stair_properties_if_exists(
        world,
        &block_pos.offset(facing.rotate_counter_clockwise().to_offset()),
    )
    .is_some_and(|other_stair_props| {
        other_stair_props.half == half && other_stair_props.facing == facing
    });

    if left_locked && right_locked {
        return StairsShape::Straight;
    }

    if let Some(other_stair_props) =
        get_stair_properties_if_exists(world, &block_pos.offset(facing.to_offset()))
        && other_stair_props.half == half
    {
        if !left_locked && other_stair_props.facing == facing.rotate_clockwise() {
            return StairsShape::OuterRight;
        } else if !right_locked && other_stair_props.facing == facing.rotate_counter_clockwise() {
            return StairsShape::OuterLeft;
        }
    }

    if let Some(other_stair_props) =
        get_stair_properties_if_exists(world, &block_pos.offset(facing.opposite().to_offset()))
        && other_stair_props.half == half
    {
        if !right_locked && other_stair_props.facing == facing.rotate_clockwise() {
            return StairsShape::InnerRight;
        } else if !left_locked && other_stair_props.facing == facing.rotate_counter_clockwise() {
            return StairsShape::InnerLeft;
        }
    }

    StairsShape::Straight
}

fn get_stair_properties_if_exists(
    world: &dyn BlockAccessor,
    block_pos: &BlockPos,
) -> Option<StairsProperties> {
    let block_state = world.get_block_state_id(block_pos);
    let block = block_state.to_block();
    block
        .has_tag(&tag::Block::MINECRAFT_STAIRS)
        .then(|| StairsProperties::from_state_id(block_state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::neighbor::{
        BlockCallbackEffect, BlockCallbackEvent, BlockCallbackIntentArgs, NeighborSnapshot,
        NeighborTranscript,
    };

    #[test]
    fn frozen_neighbor_turn_preserves_waterlogging_and_side_lock_suppresses_corner() {
        let position = BlockPos::new(0, 80, 0);
        let mut properties = StairsProperties::default(&Block::OAK_STAIRS);
        properties.facing = HorizontalFacing::North;
        properties.waterlogged = true;
        let before = properties.to_state_id(&Block::OAK_STAIRS);
        let mut front = properties;
        front.facing = HorizontalFacing::East;
        let mut snapshot = NeighborSnapshot::default();
        for x in -1..=1 {
            for z in -1..=1 {
                snapshot
                    .blocks
                    .insert(BlockPos::new(x, 80, z), BlockStateId::AIR);
            }
        }
        snapshot.blocks.insert(position, before);
        snapshot
            .blocks
            .insert(position.north(), front.to_state_id(&Block::OAK_STAIRS));
        let evaluate = |snapshot| {
            let view = NeighborTranscript::new(snapshot, 64).unwrap();
            let effects = StairBlock
                .callback_intent(BlockCallbackIntentArgs {
                    world: &view,
                    block: &Block::OAK_STAIRS,
                    position: &position,
                    event: BlockCallbackEvent::Neighbor {
                        source: Block::AIR.id,
                        notify: true,
                    },
                })
                .unwrap();
            assert!(view.finish().is_some());
            effects
        };
        let effects = evaluate(snapshot.clone());
        let [BlockCallbackEffect::SetBlock { state, .. }] = effects.as_slice() else {
            panic!("expected corner transition");
        };
        let after = StairsProperties::from_state_id(*state);
        assert_eq!(after.shape, StairsShape::OuterRight);
        assert!(after.waterlogged);
        assert_eq!(after.half, properties.half);
        assert_eq!(snapshot.blocks[&position], before);
        snapshot.blocks.insert(position.west(), before);
        assert!(evaluate(snapshot).is_empty());
    }
}
