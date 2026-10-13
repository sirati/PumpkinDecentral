use std::sync::Arc;

use crate::block::{
    CanPlaceAtArgs, EmitsRedstonePowerArgs, GetRedstonePowerArgs, GetStateForNeighborUpdateArgs,
    OnPlaceArgs, OnStateReplacedArgs, blocks::abstract_wall_mounting::WallMountedBlock,
};
use pumpkin_data::{
    Block, BlockDirection, BlockStateId, HorizontalFacingExt,
    block_properties::{AttachFace, LeverLikeProperties},
    game_event::GameEvent,
    sound::{Sound, SoundCategory},
};
use pumpkin_macros::pumpkin_block;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::world::BlockFlags;

use crate::{
    block::{
        registry::BlockActionResult,
        {BlockBehaviour, NormalUseArgs},
    },
    world::World,
};

fn toggle_lever(world: &Arc<World>, block_pos: &BlockPos) {
    let (block, state) = world.get_block_and_state_id(block_pos);

    let mut lever_props = LeverLikeProperties::from_state_id(state);
    lever_props.powered = !lever_props.powered;
    let new_lever_state = lever_props.to_state_id(block);
    let replaced_lever_state =
        world.set_block_state(block_pos, new_lever_state, BlockFlags::NOTIFY_ALL);
    crate::server::cluster_world_delta::emit_redstone_write(
        world,
        block_pos,
        block_pos,
        replaced_lever_state.as_u16(),
        new_lever_state.as_u16(),
    );

    LeverBlock::update_neighbors(world, block_pos, lever_props);

    play_lever_sound(world, block_pos, lever_props.powered);

    let game_event = if lever_props.powered {
        GameEvent::BlockActivate
    } else {
        GameEvent::BlockDeactivate
    };
    world.emit_game_event(game_event.name(), block_pos.to_centered_f64());
}

fn play_lever_sound(world: &Arc<World>, block_pos: &BlockPos, powered: bool) {
    world.play_sound_fine(
        Sound::BlockLeverClick,
        SoundCategory::Blocks,
        &block_pos.to_centered_f64(),
        0.3,
        if powered { 0.6 } else { 0.5 },
    );
}

#[pumpkin_block("minecraft:lever")]
pub struct LeverBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for LeverBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = LeverLikeProperties::default(args.block);
        properties.face = match args.direction {
            BlockDirection::Up => AttachFace::Ceiling,
            BlockDirection::Down => AttachFace::Floor,
            _ => AttachFace::Wall,
        };
        properties.facing = if matches!(args.direction, BlockDirection::Up | BlockDirection::Down) {
            args.facing
        } else {
            args.direction.opposite().to_cardinal_direction()
        };
        let direction = properties.get_direction();
        let offset = direction.to_offset();
        let support = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        args.world
            .get_block_state(&support)
            .is_side_solid(direction.opposite())
            .then(|| properties.to_state_id(args.block))
    }

    fn normal_use_intent(
        &self,
        args: crate::block::NormalUseIntentArgs<'_>,
    ) -> Option<(
        BlockActionResult,
        Vec<crate::block::neighbor::BlockCallbackEffect>,
    )> {
        use crate::block::neighbor::BlockCallbackEffect;
        if !args.can_modify_world {
            return Some((BlockActionResult::Pass, Vec::new()));
        }
        let state = args.world.get_block_state_id(args.position);
        let mut properties = LeverLikeProperties::from_state_id(state);
        properties.powered = !properties.powered;
        let offset = properties.get_direction().opposite().to_offset();
        let attached = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        Some((
            BlockActionResult::Consume,
            vec![
                BlockCallbackEffect::SetBlock {
                    position: *args.position,
                    state: properties.to_state_id(args.block),
                    flags: BlockFlags::NOTIFY_ALL,
                },
                BlockCallbackEffect::NotifyNeighbors {
                    position: *args.position,
                    except: None,
                },
                BlockCallbackEffect::NotifyNeighbors {
                    position: attached,
                    except: None,
                },
                BlockCallbackEffect::Sound {
                    position: *args.position,
                    sound: Sound::BlockLeverClick,
                    volume_milli: 300,
                    pitch_milli: if properties.powered { 600 } else { 500 },
                },
                BlockCallbackEffect::GameEvent {
                    position: *args.position,
                    event: if properties.powered {
                        GameEvent::BlockActivate
                    } else {
                        GameEvent::BlockDeactivate
                    },
                },
            ],
        ))
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        toggle_lever(args.world, args.position);
        BlockActionResult::Consume
    }

    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = LeverLikeProperties::from_state_id(args.state.id);
        if props.powered { 15 } else { 0 }
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let props = LeverLikeProperties::from_state_id(args.state.id);
        if props.powered && props.get_direction() == args.direction {
            15
        } else {
            0
        }
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        crate::block::neighbor::apply_legacy_replaced_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        let BlockCallbackEvent::StateReplaced { old_state, .. } = args.event else {
            return Some(Vec::new());
        };
        let properties = LeverLikeProperties::from_state_id(old_state);
        if !properties.powered {
            return Some(Vec::new());
        }
        let offset = properties.get_direction().opposite().to_offset();
        let attached = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        Some(vec![
            BlockCallbackEffect::NotifyNeighbors {
                position: *args.position,
                except: None,
            },
            BlockCallbackEffect::NotifyNeighbors {
                position: attached,
                except: None,
            },
        ])
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = LeverLikeProperties::default(args.block);
        (props.face, props.facing) =
            WallMountedBlock::get_placement_face(self, args.player, args.direction);

        props.to_state_id(args.block)
    }

    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        // Use the provided direction, or fallback to the current state's direction if missing
        let direction = args
            .direction
            .unwrap_or_else(|| self.get_direction(args.state.id, args.block));

        WallMountedBlock::can_place_at(self, args.block_accessor, args.position, direction)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        WallMountedBlock::get_state_for_neighbor_update(self, args)
    }
}

impl WallMountedBlock for LeverBlock {
    fn get_direction(&self, state_id: BlockStateId, _block: &Block) -> BlockDirection {
        let props = LeverLikeProperties::from_state_id(state_id);
        match props.face {
            AttachFace::Floor => BlockDirection::Up,
            AttachFace::Ceiling => BlockDirection::Down,
            AttachFace::Wall => props.facing.to_block_direction(),
        }
    }
}

impl LeverBlock {
    fn update_neighbors(
        world: &Arc<World>,
        block_pos: &BlockPos,
        lever_props: LeverLikeProperties,
    ) {
        let direction = lever_props.get_direction().opposite();
        world.update_neighbors(block_pos, None);
        world.update_neighbors(&block_pos.offset(direction.to_offset()), None);
    }
}

pub trait LeverLikePropertiesExt {
    fn get_direction(&self) -> BlockDirection;
}

impl LeverLikePropertiesExt for LeverLikeProperties {
    fn get_direction(&self) -> BlockDirection {
        match self.face {
            AttachFace::Ceiling => BlockDirection::Down,
            AttachFace::Floor => BlockDirection::Up,
            AttachFace::Wall => self.facing.to_block_direction(),
        }
    }
}
