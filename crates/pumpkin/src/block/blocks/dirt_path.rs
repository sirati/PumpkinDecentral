use crate::block::{
    BlockBehaviour, CanPlaceAtArgs, GetStateForNeighborUpdateArgs, OnPlaceArgs,
    OnScheduledTickArgs, PathComputationType, ScheduledTickIntentArgs,
};
use pumpkin_data::{Block, BlockDirection, BlockState, BlockStateId};
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockAccessor;
use pumpkin_world::world::BlockFlags;

#[pumpkin_block("minecraft:dirt_path")]
pub struct DirtPathBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for DirtPathBlock {
    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEvent;
        match args.event {
            BlockCallbackEvent::Scheduled => self.scheduled_tick_intent(ScheduledTickIntentArgs {
                world: args.world,
                block: args.block,
                position: args.position,
            }),
            // These legacy hooks are the BlockBehaviour no-op defaults.
            BlockCallbackEvent::StateReplaced { .. }
            | BlockCallbackEvent::Placed { .. }
            | BlockCallbackEvent::Prepare { .. }
            | BlockCallbackEvent::Neighbor { .. } => Some(Vec::new()),
        }
    }
    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn scheduled_tick_intent(
        &self,
        args: ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        Some(vec![
            crate::block::neighbor::BlockCallbackEffect::SetBlock {
                position: *args.position,
                state: Block::DIRT.default_state.id,
                flags: BlockFlags::NOTIFY_ALL,
            },
        ])
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        if !can_place_at(args.world, args.position) {
            return Block::DIRT.default_state.id;
        }

        args.block.default_state.id
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if args.direction == BlockDirection::Up && !can_place_at(args.world, args.position) {
            args.world
                .schedule_block_tick(args.block, *args.position, 1, TickPriority::Normal);
        }
        args.state_id
    }

    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        can_place_at(args.block_accessor, args.position)
    }

    fn is_pathfindable(&self, _state: &BlockState, _computation_type: PathComputationType) -> bool {
        false
    }
}

fn can_place_at(world: &dyn BlockAccessor, block_pos: &BlockPos) -> bool {
    let state = world.get_block_state(&block_pos.up());
    !state.is_solid() // TODO: add fence gate block
}
