use crate::block::{
    BlockBehaviour, CanPlaceAtArgs, GetStateForNeighborUpdateArgs, OnScheduledTickArgs,
};
use pumpkin_data::BlockStateId;
use pumpkin_data::block_properties::is_air;
use pumpkin_macros::{pumpkin_block, pumpkin_block_from_tag};
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::{BlockAccessor, BlockFlags};

#[pumpkin_block_from_tag("minecraft:wool_carpets")]
pub struct CarpetBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for CarpetBlock {
    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        can_place_at(args.block_accessor, args.position)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if !can_place_at(args.world, args.position) {
            args.world
                .schedule_block_tick(args.block, *args.position, 1, TickPriority::Normal);
        }
        args.state_id
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_callback_intent(self, args)
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_tick_intent(args, can_place_at)
    }
}

#[pumpkin_block("minecraft:moss_carpet")]
pub struct MossCarpetBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for MossCarpetBlock {
    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        can_place_at(args.block_accessor, args.position)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if !can_place_at(args.world, args.position) {
            args.world
                .schedule_block_tick(args.block, *args.position, 1, TickPriority::Normal);
        }
        args.state_id
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_callback_intent(self, args)
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_tick_intent(args, can_place_at)
    }
}

#[pumpkin_block("minecraft:pale_moss_carpet")]
pub struct PaleMossCarpetBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for PaleMossCarpetBlock {
    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        can_place_at(args.block_accessor, args.position)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if !can_place_at(args.world, args.position) {
            args.world
                .schedule_block_tick(args.block, *args.position, 1, TickPriority::Normal);
        }
        args.state_id
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_callback_intent(self, args)
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::neighbor::support_tick_intent(args, can_place_at)
    }
}

fn can_place_at(block_accessor: &dyn BlockAccessor, block_pos: &BlockPos) -> bool {
    !is_air(block_accessor.get_block_state_id(&block_pos.down()))
}
