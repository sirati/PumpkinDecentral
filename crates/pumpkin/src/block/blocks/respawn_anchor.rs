use pumpkin_data::block_properties::RespawnAnchorLikeProperties;
use pumpkin_data::item::Item;
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::sound::{Sound, SoundCategory};
use pumpkin_data::{BlockState, translation};
use pumpkin_macros::pumpkin_block;
use pumpkin_world::world::BlockFlags;
use pumpkin_inventory::player::player_inventory::PlayerInventory;
use pumpkin_util::GameMode;

use crate::block::registry::BlockActionResult;
use crate::block::{
    BlockBehaviour, GetComparatorOutputArgs, NormalUseArgs, PathComputationType, UseWithItemArgs,
};
use crate::entity::EntityBase;

/// Vanilla `RespawnAnchorBlock.MAX_CHARGES`.
const MAX_CHARGES: u8 = 4;

#[pumpkin_block("minecraft:respawn_anchor")]
pub struct RespawnAnchorBlock;

impl BlockBehaviour for RespawnAnchorBlock {
    fn use_with_item(&self, args: UseWithItemArgs<'_>) -> BlockActionResult {
        if args.item_stack.item.id != Item::GLOWSTONE.id {
            return BlockActionResult::Pass;
        }

        let state_id = args.world.get_block_state_id(args.position);
        let mut props = RespawnAnchorLikeProperties::from_state_id(state_id);

        if props.charges >= 4 {
            return BlockActionResult::Pass;
        }

        let slot = match args.equipment_slot {
            EquipmentSlot::MainHand(_) => args.player.inventory().get_selected_slot() as usize,
            EquipmentSlot::OffHand(_) => PlayerInventory::OFF_HAND_SLOT,
            _ => return BlockActionResult::Pass,
        };
        let before = args.item_stack.clone();
        props.charges += 1;
        let new_state = props.to_state_id(args.block);
        args.world
            .set_block_state(args.position, new_state, BlockFlags::NOTIFY_ALL);

        crate::server::cluster_world_delta::emit_anchor_charge_action(
            args.player,
            args.position,
            state_id.as_u16(),
            new_state.as_u16(),
            slot,
            &before,
            args.player.gamemode.load() != GameMode::Creative,
        );

        args.item_stack
            .decrement_unless_creative(args.player.gamemode.load(), 1);

        args.world.play_sound(
            Sound::BlockRespawnAnchorCharge,
            SoundCategory::Blocks,
            &args.position.to_f64(),
        );

        BlockActionResult::Success
    }

    fn normal_use(&self, args: NormalUseArgs<'_>) -> BlockActionResult {
        let state_id = args.world.get_block_state_id(args.position);
        let props = RespawnAnchorLikeProperties::from_state_id(state_id);

        if !args.world.dimension.respawn_anchor_works {
            args.world
                .break_block(args.position, None, BlockFlags::SKIP_DROPS);
            let center_pos = args.position.to_centered_f64();
            args.world
                .explode(center_pos, 5.0, crate::world::ExplosionInteraction::Block);
            return BlockActionResult::SuccessServer;
        }

        if props.charges == 0 {
            args.player
                .send_system_message(&pumpkin_macros::translate_cross!(
                    translation::java::BLOCK_MINECRAFT_BED_NO_SLEEP,
                    translation::bedrock::TILE_BED_NOSLEEP
                ));
            return BlockActionResult::SuccessServer;
        }

        let player = args.player;
        let world = args.world;
        let pos = *args.position;
        if player.set_respawn_point(
            world.dimension.clone(),
            pos,
            player.get_entity().yaw.load(),
            player.get_entity().pitch.load(),
            false,
        ) {
            world.play_sound(
                Sound::BlockRespawnAnchorSetSpawn,
                SoundCategory::Blocks,
                &pos.to_f64(),
            );

            player.send_system_message(&pumpkin_macros::translate_cross!(
                translation::java::BLOCK_MINECRAFT_SET_SPAWN,
                translation::bedrock::TILE_BED_RESPAWNSET
            ));
        }

        BlockActionResult::SuccessServer
    }

    /// Charges scale over the full signal range, so each charge is worth 15 / 4.
    fn get_comparator_output(&self, args: GetComparatorOutputArgs<'_>) -> Option<u8> {
        let props = RespawnAnchorLikeProperties::from_state_id(args.state.id);
        Some(props.charges * 15 / MAX_CHARGES)
    }

    fn is_pathfindable(&self, _state: &BlockState, _computation_type: PathComputationType) -> bool {
        false
    }
}
