use std::sync::Arc;

use pumpkin_data::Block;
use pumpkin_data::BlockDirection;
use pumpkin_data::BlockStateId;
use pumpkin_data::HorizontalFacingExt;
use pumpkin_data::block_properties::AttachFace;
use pumpkin_data::sound::{Sound, SoundCategory};
use pumpkin_macros::pumpkin_block_from_tag;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockFlags;

type ButtonLikeProperties = pumpkin_data::block_properties::LeverLikeProperties;

use crate::block::CanPlaceAtArgs;
use crate::block::EmitsRedstonePowerArgs;
use crate::block::GetRedstonePowerArgs;
use crate::block::GetStateForNeighborUpdateArgs;
use crate::block::OnPlaceArgs;
use crate::block::OnScheduledTickArgs;
use crate::block::OnStateReplacedArgs;
use crate::block::blocks::abstract_wall_mounting::WallMountedBlock;
use crate::block::blocks::redstone::lever::LeverLikePropertiesExt;
use crate::block::registry::BlockActionResult;
use crate::block::{BlockBehaviour, NormalUseArgs};
use crate::world::World;

fn get_sound(block: &Block, on: bool) -> Sound {
    if block == &Block::STONE_BUTTON || block == &Block::POLISHED_BLACKSTONE_BUTTON {
        if on {
            Sound::BlockStoneButtonClickOn
        } else {
            Sound::BlockStoneButtonClickOff
        }
    } else if on {
        Sound::BlockWoodenButtonClickOn
    } else {
        Sound::BlockWoodenButtonClickOff
    }
}

/// Presses the button, unless it is already pressed. Returns whether it was
/// pressed, so callers can tell the two cases apart the way vanilla's
/// `ButtonBlock::useWithoutItem` does.
fn click_button(world: &Arc<World>, block_pos: &BlockPos) -> bool {
    let (block, state) = world.get_block_and_state_id(block_pos);

    let mut button_props = ButtonLikeProperties::from_state_id(state);
    if button_props.powered {
        return false;
    }

    button_props.powered = true;
    let new_button_state = button_props.to_state_id(block);
    let replaced_button_state =
        world.set_block_state(block_pos, new_button_state, BlockFlags::NOTIFY_ALL);
    crate::server::cluster_world_delta::emit_redstone_write(
        world,
        block_pos,
        block_pos,
        replaced_button_state.as_u16(),
        new_button_state.as_u16(),
    );
    let delay = if block == &Block::STONE_BUTTON {
        20
    } else {
        30
    };
    world.schedule_block_tick(block, *block_pos, delay, TickPriority::Normal);
    ButtonBlock::update_neighbors(world, block_pos, button_props);
    world.play_block_sound(get_sound(block, true), SoundCategory::Blocks, *block_pos);

    true
}

#[pumpkin_block_from_tag("minecraft:buttons")]
pub struct ButtonBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for ButtonBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = ButtonLikeProperties::default(args.block);
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
        let mut properties = ButtonLikeProperties::from_state_id(state);
        if properties.powered {
            return Some((BlockActionResult::Consume, Vec::new()));
        }
        properties.powered = true;
        let offset = properties.get_direction().opposite().to_offset();
        let attached = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        Some((
            BlockActionResult::Success,
            vec![
                BlockCallbackEffect::SetBlock {
                    position: *args.position,
                    state: properties.to_state_id(args.block),
                    flags: BlockFlags::NOTIFY_ALL,
                },
                BlockCallbackEffect::ScheduleBlock {
                    position: *args.position,
                    block: args.block.id,
                    delay: if args.block == &Block::STONE_BUTTON {
                        20
                    } else {
                        30
                    },
                    priority: TickPriority::Normal,
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
                    sound: get_sound(args.block, true),
                    volume_milli: 1000,
                    pitch_milli: 1000,
                },
            ],
        ))
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        if click_button(args.world, args.position) {
            BlockActionResult::Success
        } else {
            BlockActionResult::Consume
        }
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        let state = args.world.get_block_state_id(args.position);
        let mut properties = ButtonLikeProperties::from_state_id(state);
        if !properties.powered {
            return Some(Vec::new());
        }
        properties.powered = false;
        let offset = properties.get_direction().opposite().to_offset();
        let attached = BlockPos::new(
            args.position.0.x.checked_add(offset.x)?,
            args.position.0.y.checked_add(offset.y)?,
            args.position.0.z.checked_add(offset.z)?,
        );
        Some(vec![
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
                sound: get_sound(args.block, false),
                volume_milli: 1000,
                pitch_milli: 1000,
            },
        ])
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
            BlockCallbackEvent::StateReplaced {
                old_state,
                moved: false,
            } => {
                let properties = ButtonLikeProperties::from_state_id(old_state);
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
            _ => Some(Vec::new()),
        }
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let button_props = ButtonLikeProperties::from_state_id(args.state.id);
        if button_props.powered { 15 } else { 0 }
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        let button_props = ButtonLikeProperties::from_state_id(args.state.id);
        if button_props.powered && button_props.get_direction() == args.direction {
            15
        } else {
            0
        }
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        crate::block::neighbor::apply_legacy_replaced_intent(self, args);
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = ButtonLikeProperties::from_state_id(args.block.default_state.id);
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

impl WallMountedBlock for ButtonBlock {
    fn get_direction(&self, state_id: BlockStateId, _block: &Block) -> BlockDirection {
        let props = ButtonLikeProperties::from_state_id(state_id);
        match props.face {
            AttachFace::Floor => BlockDirection::Up,
            AttachFace::Ceiling => BlockDirection::Down,
            AttachFace::Wall => props.facing.to_block_direction(),
        }
    }
}

impl ButtonBlock {
    fn update_neighbors(world: &Arc<World>, block_pos: &BlockPos, props: ButtonLikeProperties) {
        let direction = props.get_direction().opposite();
        world.update_neighbors(block_pos, None);
        world.update_neighbors(&block_pos.offset(direction.to_offset()), None);
    }
}
