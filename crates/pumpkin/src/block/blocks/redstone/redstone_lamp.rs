use crate::block::{OnNeighborUpdateArgs, OnPlaceArgs, OnScheduledTickArgs};
use pumpkin_data::BlockStateId;
use pumpkin_macros::pumpkin_block;
use pumpkin_world::{tick::TickPriority, world::BlockFlags};

use crate::block::BlockBehaviour;

use super::block_receives_redstone_power;

type RedstoneLampProperties = pumpkin_data::block_properties::RedstoneOreLikeProperties;

#[pumpkin_block("minecraft:redstone_lamp")]
pub struct RedstoneLamp;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for RedstoneLamp {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = RedstoneLampProperties::default(args.block);
        properties.lit = args.world.receives_redstone_power(args.position);
        Some(properties.to_state_id(args.block))
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = RedstoneLampProperties::default(args.block);
        props.lit = block_receives_redstone_power(args.world, args.position);
        props.to_state_id(args.block)
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        crate::block::neighbor::apply_legacy_neighbor_intent(self, args);
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let state = args.world.get_block_state_id(args.position);
        let mut props = RedstoneLampProperties::from_state_id(state);
        if props.lit && !args.world.receives_redstone_power(args.position) {
            props.lit = false;
            Some(vec![BlockCallbackEffect::SetBlock {
                position: *args.position,
                state: props.to_state_id(args.block),
                flags: BlockFlags::NOTIFY_LISTENERS,
            }])
        } else {
            Some(Vec::new())
        }
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        match args.event {
            BlockCallbackEvent::Scheduled => {
                self.scheduled_tick_intent(crate::block::ScheduledTickIntentArgs {
                    world: args.world,
                    block: args.block,
                    position: args.position,
                })
            }
            BlockCallbackEvent::Neighbor { .. } => {
                let state = args.world.get_block_state_id(args.position);
                let mut props = RedstoneLampProperties::from_state_id(state);
                let powered = args.world.receives_redstone_power(args.position);
                Some(if props.lit == powered {
                    Vec::new()
                } else if props.lit {
                    vec![BlockCallbackEffect::ScheduleBlock {
                        position: *args.position,
                        block: args.block.id,
                        delay: 4,
                        priority: TickPriority::Normal,
                    }]
                } else {
                    props.lit = true;
                    vec![BlockCallbackEffect::SetBlock {
                        position: *args.position,
                        state: props.to_state_id(args.block),
                        flags: BlockFlags::NOTIFY_LISTENERS,
                    }]
                })
            }
            BlockCallbackEvent::StateReplaced { .. }
            | BlockCallbackEvent::Placed { .. }
            | BlockCallbackEvent::Prepare { .. } => Some(Vec::new()),
        }
    }
}
