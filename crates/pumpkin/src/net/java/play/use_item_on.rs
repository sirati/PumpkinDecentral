#[allow(clippy::wildcard_imports)]
use super::*;
use crate::item::registry::should_try_block_placement;
use crossbeam::queue::SegQueue;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::place_emit::{capture_place, chunk_of_block, next_place_seq};
use pumpkin_cluster::protocol::PlaceBlockUpdate;
use pumpkin_cluster::time::TickStamp;

impl JavaClient {
    #[allow(clippy::too_many_lines)]
    pub fn handle_use_item_on(
        &self,
        player: &Arc<Player>,
        use_item_on: &SUseItemOn,
        server: &Arc<Server>,
    ) -> Result<(), BlockPlacingError> {
        if !player.has_client_loaded() {
            return Ok(());
        }
        player.update_last_action_time();
        self.update_sequence(use_item_on.sequence.0);

        let position = use_item_on.position;
        let cursor_pos = use_item_on.cursor_pos;

        let mut should_try_decrement = false;
        let mut placed: Option<(BlockPos, pumpkin_data::BlockStateId)> = None;

        if !player.can_interact_with_block_at(&position, 1.0) {
            // TODO: maybe log?
            return Err(BlockPlacingError::BlockOutOfReach);
        }

        let Ok(face) = BlockDirection::try_from(use_item_on.face.0) else {
            return Err(BlockPlacingError::InvalidBlockFace);
        };

        let Ok(hand) = Hand::from_packet_id(use_item_on.hand.0) else {
            return Err(BlockPlacingError::InvalidHand);
        };

        if player.gamemode.load() == GameMode::Spectator {
            let entity = &player.get_entity();
            let world = entity.world.load_full();
            let block = world.get_block(&position);

            let event = PlayerInteractEvent::new(
                player,
                InteractAction::RightClickBlock,
                block,
                Some(position),
            );

            send_cancellable_blocking! {{
                server;
                event;
                'cancelled: {
                    let state_id = world.get_block_state_id(&position);
                    player.try_send_client_packet(&CBlockUpdate::new(
                        position,
                        VarInt(i32::from(state_id.as_u16())),
                    ));
                    return Ok(());
                }
            }}

            if let Some(factory) = server
                .block_registry
                .get_screen_handler_factory(block, player, &position, server, &world)
            {
                player.open_handled_screen(factory.as_ref(), Some(position));
            }
            return Ok(());
        }

        let inventory = player.inventory();
        let held_item = inventory.held_item();
        let off_hand_item = inventory.off_hand_item();
        let held_item_empty = held_item.is_empty();
        let off_hand_item_empty = off_hand_item.is_empty();

        let mut item = inventory.get_stack_in_hand(hand);
        let item_id = item.item.id;
        player.increment_stat(StatisticCategory::Used, item_id as i32, 1);

        let entity = &player.get_entity();
        let world = entity.world.load_full();
        let block = world.get_block(&position);

        let event = PlayerInteractEvent::new(
            player,
            InteractAction::RightClickBlock,
            block,
            Some(position),
        );

        send_cancellable_blocking! {{
            server;
            event;
            'cancelled: {
                let state_id = world.get_block_state_id(&position);
                player.try_send_client_packet(&CBlockUpdate::new(
                    position,
                    VarInt(i32::from(state_id.as_u16())),
                ));
                return Ok(());
            }
        }}

        let equipment_slot = if matches!(hand, Hand::Right) {
            EquipmentSlot::MAIN_HAND
        } else {
            EquipmentSlot::OFF_HAND
        };

        let sneaking = player.get_entity().is_sneaking();

        // Code based on the java class ServerPlayerInteractionManager
        if !(sneaking && (!held_item_empty || !off_hand_item_empty)) {
            let result = Self::call_use_item_on(
                player,
                &position,
                &cursor_pos,
                face,
                &mut item,
                &equipment_slot,
                &world,
                block,
                server,
            );
            if result.consumes_action() {
                // TODO: Trigger ANY_BLOCK_USE Criteria

                if matches!(result, BlockActionResult::SuccessServer) {
                    player.swing_hand(hand, true);
                }
                return Ok(());
            }
        }

        let slot_index = if matches!(hand, Hand::Right) {
            inventory.get_selected_slot() as usize
        } else {
            PlayerInventory::OFF_HAND_SLOT
        };

        if item.is_empty() {
            // TODO item cool down
            // If the hand is empty we stop here
            return Ok(());
        }

        let before = item.clone();

        let item_result = server
            .item_registry
            .use_on_block(&mut item, player, position, face, cursor_pos, block, server);

        if should_try_block_placement(&item_result) {
            // Check if the item is a block, because not every item can be placed :D
            let item_id = item.item.id;
            if let Some(block) = Block::from_item_id(item_id) {
                placed =
                    Self::run_is_block_place(player, block, server, use_item_on, position, face)?;
                should_try_decrement = placed.is_some();
            }
        }

        if should_try_decrement {
            // TODO: Config
            // Decrease block count
            let consume = player.gamemode.load() != GameMode::Creative;
            let mut consumed_atomically = false;
            if let Some((final_pos, new_state)) = placed {
                if let Some(refreshed) =
                    Self::emit_cluster_place(player, server, hand, final_pos, new_state, consume)
                {
                    item = refreshed;
                    consumed_atomically = true;
                }
            }
            if !consumed_atomically && consume {
                item.decrement(1);
            }
        }

        let after = item.clone();

        if matches!(item_result, BlockActionResult::SuccessServer) {
            player.swing_hand(hand, true);
        }

        // Broadcast the break entity status before the slot sync; the client
        // needs the old item texture in the slot for break particles.
        if !before.is_empty() && after.is_empty() {
            let slot = if slot_index == player.inventory.get_selected_slot() as usize {
                &EquipmentSlot::MAIN_HAND
            } else {
                &EquipmentSlot::OFF_HAND
            };
            if before.is_damageable() {
                player.increment_stat(StatisticCategory::Broken, before.item.id as i32, 1);
            }
            player.world().send_entity_status(
                player.get_entity(),
                equipment_break_status(slot),
                None,
            );
        }

        if !after.are_equal(&before) {
            player.sync_hand_slot(slot_index, after.clone());
            inventory.set_stack_in_hand(hand, after);
        }

        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    fn call_use_item_on(
        player: &Arc<Player>,
        position: &BlockPos,
        cursor_pos: &Vector3<f32>,
        face: BlockDirection,
        held_item: &mut ItemStack,
        equipment_slot: &EquipmentSlot,
        world: &Arc<World>,
        block: &Block,
        server: &Arc<Server>,
    ) -> BlockActionResult {
        let result = server.block_registry.use_with_item(
            block,
            player,
            position,
            &BlockHitResult {
                face: &face,
                cursor_pos,
            },
            held_item,
            equipment_slot,
            server,
            world,
        );

        if result.consumes_action() {
            // TODO: Trigger ITEM_USED_ON_BLOCK Criteria
            return result;
        }

        if matches!(result, BlockActionResult::PassToDefaultBlockAction) {
            let result = server.block_registry.on_use(
                block,
                player,
                position,
                &BlockHitResult {
                    face: &face,
                    cursor_pos,
                },
                server,
                world,
            );

            if result.consumes_action() {
                // TODO: Trigger DEFAULT_BLOCK_USE Criteria
                return result;
            }
        }

        BlockActionResult::Pass
    }

    fn run_is_block_place(
        player: &Arc<Player>,
        block: &'static Block,
        server: &Arc<Server>,
        use_item_on: &SUseItemOn,
        location: BlockPos,
        face: BlockDirection,
    ) -> Result<Option<(BlockPos, pumpkin_data::BlockStateId)>, BlockPlacingError> {
        match server
            .block_registry
            .place_block(player, block, server, use_item_on, location, face)
        {
            Ok(Some((final_block_pos, new_state))) => {
                player.try_send_client_packet(&CBlockUpdate::new(
                    final_block_pos,
                    VarInt(i32::from(new_state.as_u16())),
                ));
                Ok(Some((final_block_pos, new_state)))
            }
            Ok(None) => Ok(None),
            Err(crate::block::registry::BlockPlacingError::InvalidGamemode) => {
                Err(BlockPlacingError::InvalidGamemode)
            }
            Err(crate::block::registry::BlockPlacingError::BlockOutOfWorld) => {
                Err(BlockPlacingError::BlockOutOfWorld)
            }
        }
    }
}

static PLACE_OUTBOX: SegQueue<PlaceBlockUpdate> = SegQueue::new();

impl JavaClient {
    pub fn drain_place_outbox() -> Vec<PlaceBlockUpdate> {
        let mut drained = Vec::new();
        while let Some(update) = PLACE_OUTBOX.pop() {
            drained.push(update);
        }
        drained
    }

    #[cfg(test)]
    pub fn stage_place_for_test(update: PlaceBlockUpdate) {
        PLACE_OUTBOX.push(update);
    }

    fn emit_cluster_place(
        player: &Player,
        server: &Server,
        hand: Hand,
        final_pos: BlockPos,
        new_state: pumpkin_data::BlockStateId,
        consume: bool,
    ) -> Option<ItemStack> {
        let cluster = &server.advanced_config.cluster;
        if !cluster.enabled {
            return None;
        }
        let gid = GlobalPlayerId::new(
            ServerId(cluster.server_id),
            PlayerSlot(Self::cluster_player_slot(player)),
        );
        let tick = Self::cluster_tick_stamp();
        let chunk = chunk_of_block(final_pos.0.x, final_pos.0.z);
        let update_pos = pumpkin_cluster::protocol::BlockPos {
            x: final_pos.0.x,
            y: final_pos.0.y,
            z: final_pos.0.z,
        };
        let new_state_id = new_state.as_u16();
        let seq = next_place_seq(gid);
        let inventory = player.inventory();
        let before = inventory.get_stack_in_hand(hand);
        let before_item = before.item.id;
        let before_count = before.item_count;
        let inv = match hand {
            Hand::Right => pumpkin_cluster::inventory::INV_MAIN,
            Hand::Left => pumpkin_cluster::inventory::INV_OFFHAND,
        };
        let emitted = inventory.consume_for_place(hand, consume, |slot, count_after| {
            let update = capture_place(
                gid,
                seq,
                tick,
                update_pos,
                new_state_id,
                inv,
                slot,
                before_item,
                before_count,
                count_after,
                chunk,
            );
            PLACE_OUTBOX.push(update);
        });
        if emitted {
            Some(inventory.get_stack_in_hand(hand))
        } else {
            None
        }
    }

    fn cluster_player_slot(player: &Player) -> u16 {
        (player.gameprofile.id.as_u128() & 0xFFFF) as u16
    }

    fn cluster_tick_stamp() -> TickStamp {
        TickStamp::now()
    }
}
