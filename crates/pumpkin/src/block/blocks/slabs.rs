use pumpkin_data::block_properties::SlabType;
use pumpkin_data::{BlockDirection, BlockState, BlockStateId};
use pumpkin_macros::pumpkin_block_from_tag;

use crate::block::{
    BlockBehaviour, BlockIsReplacing, CanUpdateAtArgs, OnPlaceArgs, PathComputationType,
};

type SlabProperties = pumpkin_data::block_properties::ResinBrickSlabLikeProperties;

#[pumpkin_block_from_tag("minecraft:slabs")]
pub struct SlabBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for SlabBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = if let BlockIsReplacing::Itself(state) = args.replacing {
            SlabProperties::from_state_id(state)
        } else {
            SlabProperties::default(args.block)
        };
        if matches!(args.replacing, BlockIsReplacing::Itself(_)) {
            properties.r#type = SlabType::Double;
            properties.waterlogged = false;
        } else {
            properties.waterlogged = args.replacing.water_source();
            properties.r#type = match args.direction {
                BlockDirection::Up => SlabType::Top,
                BlockDirection::Down => SlabType::Bottom,
                _ => {
                    if args.hit[1] < 0.5 {
                        SlabType::Bottom
                    } else {
                        SlabType::Top
                    }
                }
            };
        }
        Some(properties.to_state_id(args.block))
    }

    fn can_update_intent(&self, args: crate::block::CanUpdateIntentArgs<'_>) -> Option<bool> {
        let properties = SlabProperties::from_state_id(args.state_id);
        Some(
            properties.r#type
                == match args.direction {
                    BlockDirection::Up => SlabType::Bottom,
                    BlockDirection::Down => SlabType::Top,
                    _ => {
                        if args.hit[1] < 0.5 {
                            SlabType::Top
                        } else {
                            SlabType::Bottom
                        }
                    }
                },
        )
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        if let BlockIsReplacing::Itself(state_id) = args.replacing {
            let mut slab_props = SlabProperties::from_state_id(state_id);
            slab_props.r#type = SlabType::Double;
            slab_props.waterlogged = false;
            return slab_props.to_state_id(args.block);
        }

        let mut slab_props = SlabProperties::default(args.block);
        slab_props.waterlogged = args.replacing.water_source();
        slab_props.r#type = match args.direction {
            BlockDirection::Up => SlabType::Top,
            BlockDirection::Down => SlabType::Bottom,
            _ => match args.use_item_on.cursor_pos.y {
                0.0..0.5 => SlabType::Bottom,
                _ => SlabType::Top,
            },
        };

        slab_props.to_state_id(args.block)
    }

    fn can_update_at(&self, args: CanUpdateAtArgs<'_>) -> bool {
        let slab_props = SlabProperties::from_state_id(args.state_id);

        slab_props.r#type
            == match args.direction {
                BlockDirection::Up => SlabType::Bottom,
                BlockDirection::Down => SlabType::Top,
                _ => match args.use_item_on.cursor_pos.y {
                    0.0..0.5 => SlabType::Top,
                    _ => SlabType::Bottom,
                },
            }
    }

    fn is_pathfindable(&self, state: &BlockState, computation_type: PathComputationType) -> bool {
        match computation_type {
            PathComputationType::Water => state.is_waterlogged(),
            PathComputationType::Land | PathComputationType::Air => false,
        }
    }
}
