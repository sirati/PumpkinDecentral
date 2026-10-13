use crate::block::{
    EmitsRedstonePowerArgs, GetRedstonePowerArgs, GetStateForNeighborUpdateArgs, OnPlaceArgs,
    OnScheduledTickArgs, OnStateReplacedArgs,
};
use crate::entity::EntityBase;
use pumpkin_data::{Block, BlockStateId, FacingExt, block_properties::ObserverLikeProperties};
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::{tick::TickPriority, world::BlockFlags};

use crate::block::BlockBehaviour;

#[pumpkin_block("minecraft:observer")]
pub struct ObserverBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for ObserverBlock {
    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = ObserverLikeProperties::default(args.block);
        props.facing = args.player.get_entity().get_facing();
        props.to_state_id(args.block)
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let state = args.world.get_block_state(args.position);
        let mut props = ObserverLikeProperties::from_state_id(state.id);
        props.powered = !props.powered;
        let mut effects = vec![BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: props.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_LISTENERS,
        }];
        if props.powered {
            effects.push(BlockCallbackEffect::ScheduleBlock {
                position: *args.position,
                block: args.block.id,
                delay: 2,
                priority: TickPriority::Normal,
            });
        }
        let facing = props.facing.to_block_direction();
        let offset = facing.opposite().to_offset();
        let position = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        effects.push(BlockCallbackEffect::NotifyNeighbor {
            position,
            source: args.block.id,
        });
        effects.push(BlockCallbackEffect::NotifyNeighbors {
            position,
            except: Some(facing),
        });
        Some(effects)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        let props = ObserverLikeProperties::from_state_id(args.state_id);

        if props.facing.to_block_direction() == args.direction
            && !props.powered
            && !args
                .world
                .is_block_tick_scheduled(args.position, &Block::OBSERVER)
        {
            Self::schedule_tick(args.world, args.position);
        }

        args.state_id
    }

    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = ObserverLikeProperties::from_state_id(args.state.id);
        if props.facing.to_block_direction() == args.direction && props.powered {
            15
        } else {
            0
        }
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        self.get_weak_redstone_power(args)
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        crate::block::neighbor::apply_legacy_replaced_intent(self, args);
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
            BlockCallbackEvent::StateReplaced { old_state, moved } => {
                let properties = ObserverLikeProperties::from_state_id(old_state);
                if moved
                    || !properties.powered
                    || !args
                        .world
                        .is_block_tick_scheduled(args.position, &Block::OBSERVER)
                {
                    return Some(Vec::new());
                }
                let facing = properties.facing.to_block_direction();
                let offset = facing.opposite().to_offset();
                let position = BlockPos::new(
                    args.position.0.x.checked_add(offset.x)?,
                    args.position.0.y.checked_add(offset.y)?,
                    args.position.0.z.checked_add(offset.z)?,
                );
                Some(vec![
                    BlockCallbackEffect::NotifyNeighbor {
                        position,
                        source: args.block.id,
                    },
                    BlockCallbackEffect::NotifyNeighbors {
                        position,
                        except: Some(facing),
                    },
                ])
            }
            BlockCallbackEvent::Placed { .. }
            | BlockCallbackEvent::Prepare { .. }
            | BlockCallbackEvent::Neighbor { .. } => Some(Vec::new()),
        }
    }
}

impl ObserverBlock {
    fn schedule_tick(world: &dyn crate::block::NeighborUpdateWorld, block_pos: &BlockPos) {
        world.schedule_block_tick(&Block::OBSERVER, *block_pos, 2, TickPriority::Normal);
    }
}
