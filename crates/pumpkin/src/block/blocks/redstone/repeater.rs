use std::sync::Arc;

use pumpkin_data::{
    Block, BlockDirection, BlockState, BlockStateId, HorizontalFacingExt,
    block_properties::HorizontalFacing,
};
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockFlags;

use crate::{
    block::{
        BlockBehaviour, CanPlaceAtArgs, EmitsRedstonePowerArgs, GetRedstonePowerArgs,
        GetStateForNeighborUpdateArgs, NormalUseArgs, OnNeighborUpdateArgs, OnPlaceArgs,
        OnScheduledTickArgs, OnStateReplacedArgs, PlacedArgs, PlayerPlacedArgs,
        registry::BlockActionResult,
    },
    world::World,
};

use super::abstract_redstone_gate::{RedstoneGateBlock, RedstoneGateBlockProperties};

type RepeaterProperties = pumpkin_data::block_properties::RepeaterLikeProperties;

#[pumpkin_block("minecraft:repeater")]
pub struct RepeaterBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for RepeaterBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        if !RedstoneGateBlock::can_place_at(self, args.world, *args.position) {
            return None;
        }
        let mut properties = RepeaterProperties::default(args.block);
        properties.facing = args.facing.opposite();
        let state = properties.to_state_id(args.block);
        properties.locked = self.is_locked(args.world, *args.position, state, args.block);
        Some(properties.to_state_id(args.block))
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let state = args.world.get_block_state(args.position);
        if state.id.to_block() != args.block
            || self.is_locked(args.world, *args.position, state.id, args.block)
        {
            return Some(Vec::new());
        }
        let mut properties = RepeaterProperties::from_state_id(state.id);
        let powered = Self::input_power(args.world, *args.position, state.id)? > 0;
        if properties.powered && powered {
            return Some(Vec::new());
        }
        properties.powered = !properties.powered;
        let changed = properties.to_state_id(args.block);
        let mut effects = vec![BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: changed,
            flags: BlockFlags::NOTIFY_LISTENERS,
        }];
        if properties.powered && !powered {
            effects.push(BlockCallbackEffect::ScheduleBlock {
                position: *args.position,
                block: args.block.id,
                delay: properties.delay * 2,
                priority: TickPriority::VeryHigh,
            });
        }
        effects.extend(Self::target_effects(*args.position, changed, args.block)?);
        Some(effects)
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
                if moved || args.world.get_block(args.position) == args.block {
                    Some(Vec::new())
                } else {
                    Self::target_effects(*args.position, old_state, args.block)
                }
            }
            BlockCallbackEvent::Placed { .. } => Self::target_effects(
                *args.position,
                args.world.get_block_state_id(args.position),
                args.block,
            ),
            BlockCallbackEvent::Prepare { .. } => Some(Vec::new()),
            BlockCallbackEvent::Neighbor { .. } => {
                let state = args.world.get_block_state(args.position);
                if state.id.to_block() != args.block {
                    return Some(Vec::new());
                }
                if !RedstoneGateBlock::can_place_at(self, args.world, *args.position) {
                    return Some(vec![BlockCallbackEffect::BreakBlock {
                        position: *args.position,
                        flags: BlockFlags::NOTIFY_ALL,
                    }]);
                }
                if self.is_locked(args.world, *args.position, state.id, args.block) {
                    return Some(Vec::new());
                }
                let properties = RepeaterProperties::from_state_id(state.id);
                let powered = Self::input_power(args.world, *args.position, state.id)? > 0;
                if properties.powered == powered
                    || args
                        .world
                        .is_block_tick_scheduled(args.position, args.block)
                {
                    return Some(Vec::new());
                }
                let priority = if RedstoneGateBlock::is_target_not_aligned(
                    self,
                    args.world,
                    *args.position,
                    state,
                    args.block,
                ) {
                    TickPriority::ExtremelyHigh
                } else if properties.powered {
                    TickPriority::VeryHigh
                } else {
                    TickPriority::High
                };
                Some(vec![BlockCallbackEffect::ScheduleBlock {
                    position: *args.position,
                    block: args.block.id,
                    delay: properties.delay * 2,
                    priority,
                }])
            }
        }
    }

    fn normal_use_intent(
        &self,
        args: crate::block::NormalUseIntentArgs<'_>,
    ) -> Option<(
        BlockActionResult,
        Vec<crate::block::neighbor::BlockCallbackEffect>,
    )> {
        if !args.can_modify_world {
            return Some((BlockActionResult::Pass, Vec::new()));
        }
        let state = args.world.get_block_state_id(args.position);
        let mut properties = RepeaterProperties::from_state_id(state);
        properties.delay = if properties.delay == 4 {
            1
        } else {
            properties.delay + 1
        };
        Some((
            BlockActionResult::Success,
            vec![crate::block::neighbor::BlockCallbackEffect::SetBlock {
                position: *args.position,
                state: properties.to_state_id(args.block),
                flags: BlockFlags::NOTIFY_ALL,
            }],
        ))
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let state_id = RedstoneGateBlock::on_place(self, args.player, args.block);

        let mut props = RepeaterProperties::from_state_id(state_id);
        props.locked = self.is_locked(args.world, *args.position, state_id, args.block);

        props.to_state_id(args.block)
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        RedstoneGateBlock::on_neighbor_update(self, args);
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        let (block, state) = args.world.get_block_and_state(args.position);
        if self.is_locked(args.world, *args.position, state.id, block) {
            return;
        }
        let mut props = RepeaterProperties::from_state_id(state.id);

        let now_powered = props.powered;
        let should_be_powered = self.has_power(args.world, *args.position, state, block);

        // Vanilla's `DiodeBlock.tick` just does `setBlock(..., 2)`, which still calls the neighbor
        // in front. Pumpkin's `placed` only sets the block-type, so `update_target` below does it.
        if now_powered && !should_be_powered {
            props.powered = false;
            args.world.set_block_state(
                args.position,
                props.to_state_id(block),
                BlockFlags::NOTIFY_LISTENERS,
            );
            RedstoneGateBlock::update_target(
                self,
                args.world,
                *args.position,
                props.to_state_id(block),
                block,
            );
        } else if !now_powered {
            props.powered = true;
            args.world.set_block_state(
                args.position,
                props.to_state_id(block),
                BlockFlags::NOTIFY_LISTENERS,
            );
            if !should_be_powered {
                args.world.schedule_block_tick(
                    block,
                    *args.position,
                    RedstoneGateBlock::get_update_delay_internal(
                        self,
                        props.to_state_id(block),
                        block,
                    ),
                    TickPriority::VeryHigh,
                );
            }
            RedstoneGateBlock::update_target(
                self,
                args.world,
                *args.position,
                props.to_state_id(block),
                block,
            );
        }
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        let state = args.world.get_block_state(args.position);
        let props = RepeaterProperties::from_state_id(state.id);
        Self::on_use(props, args.world, *args.position, args.block);

        BlockActionResult::Success
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        RedstoneGateBlock::get_weak_redstone_power(self, args)
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        RedstoneGateBlock::get_strong_redstone_power(self, args)
    }

    fn emits_redstone_power(&self, args: EmitsRedstonePowerArgs<'_>) -> bool {
        // Vanilla's `isSignalSource` is always true; `shouldConnectTo` checks facing
        // and its opposite (in and out). Power stays facing-only.
        let repeater_props = RepeaterProperties::from_state_id(args.state.id);
        repeater_props.facing.to_block_direction() == args.direction
            || repeater_props.facing.to_block_direction() == args.direction.opposite()
    }

    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        RedstoneGateBlock::can_place_at(self, args.block_accessor, *args.position)
    }

    fn placed(&self, args: PlacedArgs<'_>) {
        RedstoneGateBlock::update_target(
            self,
            args.world,
            *args.position,
            args.state_id,
            args.block,
        );
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if args.direction == BlockDirection::Down
            && !RedstoneGateBlock::can_place_above(
                self,
                args.world,
                *args.neighbor_position,
                BlockState::from_id(args.neighbor_state_id),
            )
        {
            return Block::AIR.default_state.id;
        }
        let mut props = RepeaterProperties::from_state_id(args.state_id);
        if args.direction.to_axis() != props.facing.to_block_direction().to_axis() {
            props.locked = self.is_locked(args.world, *args.position, args.state_id, args.block);
            return props.to_state_id(args.block);
        }
        args.state_id
    }

    fn player_placed(&self, args: PlayerPlacedArgs<'_>) {
        RedstoneGateBlock::player_placed(self, args);
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        RedstoneGateBlock::on_state_replaced(self, args);
    }
}

impl RedstoneGateBlockProperties for RepeaterProperties {
    fn is_powered(&self) -> bool {
        self.powered
    }

    fn get_facing(&self) -> HorizontalFacing {
        self.facing
    }

    fn set_facing(&mut self, facing: HorizontalFacing) {
        self.facing = facing;
    }
}

impl RedstoneGateBlock<RepeaterProperties> for RepeaterBlock {
    fn get_output_level(
        &self,
        _world: &dyn crate::block::RedstonePowerWorld,
        _pos: BlockPos,
    ) -> u8 {
        15
    }

    fn update_powered(&self, world: &World, pos: BlockPos, state: &BlockState, block: &Block) {
        if self.is_locked(world, pos, state.id, block) {
            return;
        }
        let props = RepeaterProperties::from_state_id(state.id);
        let powered = props.powered;
        let has_power = RedstoneGateBlock::has_power(self, world, pos, state, block);

        if powered != has_power && !world.is_block_tick_scheduled(&pos, block) {
            let priority =
                if RedstoneGateBlock::is_target_not_aligned(self, world, pos, state, block) {
                    TickPriority::ExtremelyHigh
                } else if powered {
                    TickPriority::VeryHigh
                } else {
                    TickPriority::High
                };

            world.schedule_block_tick(
                block,
                pos,
                RedstoneGateBlock::get_update_delay_internal(self, state.id, block),
                priority,
            );
        }
    }

    fn get_update_delay_internal(&self, state_id: BlockStateId, _block: &Block) -> u8 {
        let props = RepeaterProperties::from_state_id(state_id);
        props.delay * 2
    }
}

impl RepeaterBlock {
    fn input_power(
        world: &dyn crate::block::NeighborUpdateWorld,
        position: BlockPos,
        state: BlockStateId,
    ) -> Option<u8> {
        let facing = RepeaterProperties::from_state_id(state)
            .facing
            .to_block_direction();
        let offset = facing.to_offset();
        let source = BlockPos::new(
            position.0.x.checked_add(offset.x)?,
            position.0.y.checked_add(offset.y)?,
            position.0.z.checked_add(offset.z)?,
        );
        let state = world.get_block_state(&source);
        let power = world.get_directional_power(&source, facing);
        Some(if state.id.to_block() == &Block::REDSTONE_WIRE {
            power.max(
                pumpkin_data::block_properties::RedstoneWireLikeProperties::from_state_id(state.id)
                    .power,
            )
        } else {
            power
        })
    }

    fn target_effects(
        position: BlockPos,
        state: BlockStateId,
        block: &Block,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let facing = RepeaterProperties::from_state_id(state).facing;
        let offset = facing.opposite().to_block_direction().to_offset();
        let front = BlockPos::new(
            position.0.x.checked_add(offset.x)?,
            position.0.y.checked_add(offset.y)?,
            position.0.z.checked_add(offset.z)?,
        );
        Some(vec![
            BlockCallbackEffect::NotifyNeighbor {
                position: front,
                source: block.id,
            },
            BlockCallbackEffect::NotifyNeighbors {
                position: front,
                except: Some(facing.to_block_direction()),
            },
        ])
    }

    fn on_use(props: RepeaterProperties, world: &Arc<World>, block_pos: BlockPos, block: &Block) {
        let mut props = props;
        props.delay = if props.delay == 4 { 1 } else { props.delay + 1 };
        let state = props.to_state_id(block);
        world.set_block_state(&block_pos, state, BlockFlags::NOTIFY_ALL);
    }

    fn is_locked(
        &self,
        world: &dyn crate::block::NeighborUpdateWorld,
        pos: BlockPos,
        state_id: BlockStateId,
        block: &Block,
    ) -> bool {
        Self::get_max_input_level_sides(self, world, pos, state_id, block, true) > 0
    }
}
