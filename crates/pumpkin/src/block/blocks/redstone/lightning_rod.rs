use std::sync::Arc;

use crate::block::{
    BlockBehaviour, EmitsRedstonePowerArgs, GetRedstonePowerArgs, OnPlaceArgs, OnScheduledTickArgs,
    OnStateReplacedArgs, PathComputationType,
};
use crate::world::World;
use pumpkin_data::block_properties::LightningRodLikeProperties;
use pumpkin_data::{BlockState, BlockStateId, FacingExt};
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockFlags;

#[pumpkin_block("minecraft:lightning_rod")]
pub struct LightningRodBlock;

impl LightningRodBlock {
    pub fn trigger(world: &Arc<World>, pos: &BlockPos) {
        let (block, state_id) = world.get_block_and_state_id(pos);
        let mut props = LightningRodLikeProperties::from_state_id(state_id);
        if !props.powered {
            props.powered = true;
            world.set_block_state(pos, props.to_state_id(block), BlockFlags::NOTIFY_ALL);

            Self::update_neighbors(world, pos, props);

            // In vanilla, it stays powered for 8 ticks (4 redstone ticks) before scheduled tick turns it off.
            world.schedule_block_tick(block, *pos, 8, TickPriority::Normal);
        }
    }

    fn neighbor_intents(
        position: &BlockPos,
        properties: LightningRodLikeProperties,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let delta = properties
            .facing
            .opposite()
            .to_block_direction()
            .to_offset();
        let attached = BlockPos::new(
            position.0.x.checked_add(delta.x)?,
            position.0.y.checked_add(delta.y)?,
            position.0.z.checked_add(delta.z)?,
        );
        Some(vec![
            BlockCallbackEffect::NotifyNeighbors {
                position: *position,
                except: None,
            },
            BlockCallbackEffect::NotifyNeighbors {
                position: attached,
                except: None,
            },
        ])
    }

    fn update_neighbors(
        world: &Arc<World>,
        position: &BlockPos,
        properties: LightningRodLikeProperties,
    ) {
        crate::block::neighbor::apply_legacy_callback_effects(
            world,
            Self::neighbor_intents(position, properties)
                .expect("lightning rod position is in world bounds"),
        )
        .expect("lightning rod notification effects are valid");
    }
}

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for LightningRodBlock {
    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = LightningRodLikeProperties::default(args.block);
        props.facing = args.direction.to_facing().opposite();
        props.to_state_id(args.block)
    }

    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = LightningRodLikeProperties::from_state_id(args.state.id);
        if props.powered { 15 } else { 0 }
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = LightningRodLikeProperties::from_state_id(args.state.id);
        // It emits strong power only in its facing direction (the direction pointing outward)
        if props.powered && props.facing.to_block_direction() == args.direction {
            15
        } else {
            0
        }
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        let state = args.world.get_block_state(args.position);
        let mut properties = LightningRodLikeProperties::from_state_id(state.id);
        if !properties.powered {
            return Some(Vec::new());
        }
        properties.powered = false;
        let mut effects = vec![crate::block::neighbor::BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: properties.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_ALL,
        }];
        effects.extend(Self::neighbor_intents(args.position, properties)?);
        Some(effects)
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        crate::block::neighbor::apply_legacy_replaced_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEvent;
        match args.event {
            BlockCallbackEvent::Scheduled => {
                self.scheduled_tick_intent(crate::block::ScheduledTickIntentArgs {
                    world: args.world,
                    block: args.block,
                    position: args.position,
                })
            }
            BlockCallbackEvent::StateReplaced { old_state, moved } => {
                let properties = LightningRodLikeProperties::from_state_id(old_state);
                if !moved && properties.powered {
                    Self::neighbor_intents(args.position, properties)
                } else {
                    Some(Vec::new())
                }
            }
            BlockCallbackEvent::Placed { .. }
            | BlockCallbackEvent::Prepare { .. }
            | BlockCallbackEvent::Neighbor { .. } => Some(Vec::new()),
        }
    }

    fn is_pathfindable(&self, _state: &BlockState, _computation_type: PathComputationType) -> bool {
        false
    }
}
