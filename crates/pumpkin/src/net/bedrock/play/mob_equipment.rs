#[allow(clippy::wildcard_imports)]
use super::*;

impl BedrockClient {
    pub fn handle_mob_equipment(
        &self,
        server: &Arc<Server>,
        player: &Arc<Player>,
        packet: &SMobEquipment,
    ) {
        player.update_last_action_time();
        let slot = packet.selected_slot;
        if slot >= 9 {
            return;
        }
        if server.advanced_config.cluster.enabled {
            crate::server::cluster_inventory_protocol::stage_selected_hotbar(player, slot, true);
            return;
        }
        let previous_slot = player.inventory.get_selected_slot();
        let mut event = PlayerItemHeldEvent::new(player.clone(), previous_slot, slot);
        server.plugin_manager.fire_blocking(server, &mut event);
        if event.cancelled {
            self.try_enqueue_client_packet(&CPlayerHotbar {
                selected_slot: VarUInt(previous_slot as u32),
                container_id: 0,
                should_select_slot: true,
            });
            return;
        }

        let inv = player.inventory();
        inv.set_selected_slot(slot);
        pumpkin_cluster::visual::emit_held(
            player.cluster_gid(),
            pumpkin_cluster::visual::tick_from_counter(player.tick_counter.load(Ordering::Relaxed)),
            slot,
        );
        let stack = inv.held_item();
        pumpkin_cluster::visual::emit_equipment(
            player.cluster_gid(),
            pumpkin_cluster::visual::tick_from_counter(player.tick_counter.load(Ordering::Relaxed)),
            0,
            if stack.is_empty() { 0 } else { stack.item.id },
            stack.item_count,
        );
        let equipment = &[(EquipmentSlot::MAIN_HAND, stack)];
        player.living_entity.send_equipment_changes(equipment);
    }
}
