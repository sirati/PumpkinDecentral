use std::sync::Arc;

use crate::block::blocks::redstone::block_receives_redstone_power;
use crate::block::registry::BlockActionResult;
use crate::block::{
    BlockBehaviour, GetStateForNeighborUpdateArgs, NormalUseArgs, OnNeighborUpdateArgs,
    OnPlaceArgs, PathComputationType,
};
use crate::entity::EntityBase;
use crate::entity::player::Player;
use crate::world::World;
use pumpkin_data::sound::{Sound, SoundCategory};
use pumpkin_data::tag;
use pumpkin_data::tag::Taggable;
use pumpkin_data::{Block, BlockState, BlockStateId};
use pumpkin_macros::pumpkin_block_from_tag;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::world::BlockFlags;

type FenceGateProperties = pumpkin_data::block_properties::OakFenceGateLikeProperties;

fn get_sound(block: &Block, open: bool) -> Sound {
    match (block, open) {
        (b, true) if b == &Block::BAMBOO_FENCE_GATE => Sound::BlockBambooWoodFenceGateOpen,
        (b, false) if b == &Block::BAMBOO_FENCE_GATE => Sound::BlockBambooWoodFenceGateClose,
        (b, true) if b == &Block::CHERRY_FENCE_GATE => Sound::BlockCherryWoodFenceGateOpen,
        (b, false) if b == &Block::CHERRY_FENCE_GATE => Sound::BlockCherryWoodFenceGateClose,
        (b, true) if b == &Block::CRIMSON_FENCE_GATE || b == &Block::WARPED_FENCE_GATE => {
            Sound::BlockNetherWoodFenceGateOpen
        }
        (b, false) if b == &Block::CRIMSON_FENCE_GATE || b == &Block::WARPED_FENCE_GATE => {
            Sound::BlockNetherWoodFenceGateClose
        }
        (_, true) => Sound::BlockFenceGateOpen,
        (_, false) => Sound::BlockFenceGateClose,
    }
}

pub fn toggle_fence_gate(
    world: &Arc<World>,
    block_pos: &BlockPos,
    player: &Player,
) -> BlockStateId {
    let (block, state) = world.get_block_and_state_id(block_pos);

    let mut fence_gate_props = FenceGateProperties::from_state_id(state);
    if fence_gate_props.open {
        fence_gate_props.open = false;
    } else {
        if fence_gate_props.facing
            == player
                .living_entity
                .entity
                .get_horizontal_facing()
                .opposite()
        {
            fence_gate_props.facing = player.get_entity().get_horizontal_facing();
        }
        fence_gate_props.open = true;
    }

    world.play_block_sound_expect(
        player,
        get_sound(block, fence_gate_props.open),
        SoundCategory::Blocks,
        *block_pos,
    );

    world.set_block_state(
        block_pos,
        fence_gate_props.to_state_id(block),
        BlockFlags::NOTIFY_LISTENERS,
    );
    fence_gate_props.to_state_id(block)
}

#[pumpkin_block_from_tag("minecraft:fence_gates")]
pub struct FenceGateBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for FenceGateBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = FenceGateProperties::default(args.block);
        properties.facing = args.facing;
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
        if !args.can_modify_world {
            return Some((BlockActionResult::Pass, Vec::new()));
        }
        let mut props =
            FenceGateProperties::from_state_id(args.world.get_block_state_id(args.position));
        if !props.open && props.facing == args.facing.opposite() {
            props.facing = args.facing;
        }
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
            FenceGateProperties::from_state_id(args.world.get_block_state_id(args.position));
        let powered = args.world.receives_redstone_power(args.position);
        if props.powered == powered {
            return Some(Vec::new());
        }
        props.powered = powered;
        let mut effects = Vec::new();
        if props.open != powered {
            props.open = powered;
            effects.push(BlockCallbackEffect::Sound {
                position: *args.position,
                sound: get_sound(args.block, powered),
                volume_milli: 1000,
                pitch_milli: 1000,
            });
        }
        effects.push(BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: props.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_LISTENERS,
        });
        Some(effects)
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut fence_gate_props = FenceGateProperties::default(args.block);
        fence_gate_props.facing = args.player.get_entity().get_horizontal_facing();

        let powered = block_receives_redstone_power(args.world, args.position);
        fence_gate_props.powered = powered;
        fence_gate_props.open = powered;

        fence_gate_props.to_state_id(args.block)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        let fence_props = is_in_wall(&args);
        fence_props.to_state_id(args.block)
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        {
            toggle_fence_gate(args.world, args.position, args.player);

            BlockActionResult::Success
        }
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        {
            let block_state = args.world.get_block_state(args.position);
            let mut fence_gate_props = FenceGateProperties::from_state_id(block_state.id);
            let powered = block_receives_redstone_power(args.world, args.position);

            if powered == fence_gate_props.powered {
                return;
            }

            fence_gate_props.powered = powered;

            if powered != fence_gate_props.open {
                fence_gate_props.open = powered;

                args.world.play_block_sound(
                    get_sound(args.block, powered),
                    SoundCategory::Blocks,
                    *args.position,
                );
            }

            args.world.set_block_state(
                args.position,
                fence_gate_props.to_state_id(args.block),
                BlockFlags::NOTIFY_LISTENERS,
            );
        }
    }

    fn is_pathfindable(&self, state: &BlockState, computation_type: PathComputationType) -> bool {
        match computation_type {
            PathComputationType::Land | PathComputationType::Air => {
                FenceGateProperties::from_state_id(state.id).open
            }
            PathComputationType::Water => false,
        }
    }
}

fn is_in_wall(args: &GetStateForNeighborUpdateArgs<'_>) -> FenceGateProperties {
    let mut fence_props = FenceGateProperties::from_state_id(args.state_id);

    let side_offset_left = args
        .position
        .offset(fence_props.facing.rotate_clockwise().to_offset());

    let side_offset_right = args
        .position
        .offset(fence_props.facing.rotate_counter_clockwise().to_offset());

    let neighbor_on_side =
        args.neighbor_position == &side_offset_left || args.neighbor_position == &side_offset_right;

    if neighbor_on_side {
        let neighbor_right = args.world.get_block(&side_offset_right);
        let neighbor_left = args.world.get_block(&side_offset_left);

        fence_props.in_wall = neighbor_left.has_tag(&tag::Block::MINECRAFT_WALLS)
            || neighbor_right.has_tag(&tag::Block::MINECRAFT_WALLS);
    }

    fence_props
}
