use crate::block::blocks::redstone::block_receives_redstone_power;
use crate::block::registry::BlockActionResult;
use crate::block::{
    BlockBehaviour, NormalUseArgs, OnNeighborUpdateArgs, OnPlaceArgs, PathComputationType,
};
use crate::entity::EntityBase;
use crate::entity::player::Player;
use crate::world::World;
use pumpkin_data::BlockDirection;
use pumpkin_data::block_properties::Half;
use pumpkin_data::sound::{Sound, SoundCategory};
use pumpkin_data::tag::Taggable;
use pumpkin_data::{Block, BlockState, BlockStateId, tag};
use pumpkin_macros::pumpkin_block_from_tag;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::world::BlockFlags;
use std::sync::Arc;

type TrapDoorProperties = pumpkin_data::block_properties::OakTrapdoorLikeProperties;

fn toggle_trapdoor(player: &Player, world: &Arc<World>, block_pos: &BlockPos) {
    let (block, block_state) = world.get_block_and_state_id(block_pos);
    let mut trapdoor_props = TrapDoorProperties::from_state_id(block_state);
    trapdoor_props.open = !trapdoor_props.open;

    world.play_block_sound_expect(
        player,
        get_sound(block, trapdoor_props.open),
        SoundCategory::Blocks,
        *block_pos,
    );

    world.set_block_state(
        block_pos,
        trapdoor_props.to_state_id(block),
        BlockFlags::NOTIFY_LISTENERS,
    );
}

fn can_open_trapdoor(block: &Block) -> bool {
    if block == &Block::IRON_TRAPDOOR {
        return false;
    }
    true
}

fn get_sound(block: &Block, open: bool) -> Sound {
    if open {
        if block.has_tag(&tag::Block::MINECRAFT_WOODEN_TRAPDOORS) {
            Sound::BlockWoodenTrapdoorOpen
        } else if block == &Block::IRON_TRAPDOOR {
            Sound::BlockIronTrapdoorOpen
        } else {
            Sound::BlockCopperTrapdoorOpen
        }
    } else if block.has_tag(&tag::Block::MINECRAFT_WOODEN_TRAPDOORS) {
        Sound::BlockWoodenTrapdoorClose
    } else if block == &Block::IRON_TRAPDOOR {
        Sound::BlockIronTrapdoorClose
    } else {
        Sound::BlockCopperTrapdoorClose
    }
}

#[pumpkin_block_from_tag("minecraft:trapdoors")]
pub struct TrapDoorBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for TrapDoorBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = TrapDoorProperties::default(args.block);
        properties.waterlogged = args.replacing.water_source();
        properties.facing = args.direction.to_horizontal_facing().unwrap_or(args.facing);
        properties.half = match args.direction {
            BlockDirection::Up => Half::Top,
            BlockDirection::Down => Half::Bottom,
            _ if args.hit[1] < 0.5 => Half::Bottom,
            _ => Half::Top,
        };
        let powered = args.world.receives_redstone_power(args.position);
        properties.powered = powered;
        properties.open = powered;
        Some(properties.to_state_id(args.block))
    }

    fn normal_use_intent(
        &self,
        args: crate::block::NormalUseIntentArgs<'_>,
    ) -> Option<(
        BlockActionResult,
        Vec<crate::block::neighbor::BlockCallbackEffect>,
    )> {
        use crate::block::neighbor::BlockCallbackEffect;
        if !can_open_trapdoor(args.block) || !args.can_modify_world {
            return Some((BlockActionResult::Pass, Vec::new()));
        }
        let mut props =
            TrapDoorProperties::from_state_id(args.world.get_block_state_id(args.position));
        props.open = !props.open;
        Some((
            BlockActionResult::Success,
            vec![
                BlockCallbackEffect::SetBlock {
                    position: *args.position,
                    state: props.to_state_id(args.block),
                    flags: BlockFlags::NOTIFY_LISTENERS,
                },
                BlockCallbackEffect::Sound {
                    position: *args.position,
                    sound: get_sound(args.block, props.open),
                    volume_milli: 1000,
                    pitch_milli: 1000,
                },
            ],
        ))
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::BlockCallbackEffect;
        if !matches!(
            args.event,
            crate::block::neighbor::BlockCallbackEvent::Neighbor { .. }
        ) {
            return Some(Vec::new());
        }
        let mut props =
            TrapDoorProperties::from_state_id(args.world.get_block_state_id(args.position));
        let powered = args.world.receives_redstone_power(args.position);
        let mut effects = Vec::new();
        if powered != props.powered {
            props.powered = powered;
            if powered != props.open {
                props.open = powered;
                effects.push(BlockCallbackEffect::Sound {
                    position: *args.position,
                    sound: get_sound(args.block, powered),
                    volume_milli: 1000,
                    pitch_milli: 1000,
                });
            }
        }
        effects.push(BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: props.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_LISTENERS,
        });
        Some(effects)
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        {
            if !can_open_trapdoor(args.block) {
                return BlockActionResult::Pass;
            }

            toggle_trapdoor(args.player, args.world, args.position);

            BlockActionResult::Success
        }
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut trapdoor_props = TrapDoorProperties::default(args.block);
        trapdoor_props.waterlogged = args.replacing.water_source();

        let powered = block_receives_redstone_power(args.world, args.position);

        let player_facing = args.player.get_entity().get_horizontal_facing();

        // Correct facing logic using Option unwrap
        let facing = args
            .direction
            .to_horizontal_facing()
            .unwrap_or(player_facing);

        trapdoor_props.facing = facing;

        trapdoor_props.half = match args.direction {
            BlockDirection::Up => Half::Top,
            BlockDirection::Down => Half::Bottom,
            _ => match args.use_item_on.cursor_pos.y {
                0.0..0.5 => Half::Bottom,
                _ => Half::Top,
            },
        };

        trapdoor_props.powered = powered;
        trapdoor_props.open = powered;

        trapdoor_props.to_state_id(args.block)
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        {
            let block_state = args.world.get_block_state(args.position);
            let mut trapdoor_props = TrapDoorProperties::from_state_id(block_state.id);
            let powered = block_receives_redstone_power(args.world, args.position);

            if powered != trapdoor_props.powered {
                trapdoor_props.powered = !trapdoor_props.powered;

                if powered != trapdoor_props.open {
                    trapdoor_props.open = trapdoor_props.powered;

                    args.world.play_block_sound(
                        get_sound(args.block, powered),
                        SoundCategory::Blocks,
                        *args.position,
                    );
                }
            }

            args.world.set_block_state(
                args.position,
                trapdoor_props.to_state_id(args.block),
                BlockFlags::NOTIFY_LISTENERS,
            );
        }
    }

    fn is_pathfindable(&self, state: &BlockState, computation_type: PathComputationType) -> bool {
        let props = TrapDoorProperties::from_state_id(state.id);
        match computation_type {
            PathComputationType::Land | PathComputationType::Air => props.open,
            PathComputationType::Water => props.waterlogged,
        }
    }
}
