#[allow(clippy::wildcard_imports)]
use super::*;

impl BedrockClient {
    pub fn handle_container_close(&self, player: &Arc<Player>, packet: &SContainerClose) {
        if player
            .world()
            .server
            .upgrade()
            .is_some_and(|server| server.advanced_config.cluster.enabled)
        {
            let session = player
                .cluster_current_inventory_session
                .load(Ordering::Acquire);
            let sync_id = player.screen_handler_sync_id.load(Ordering::Acquire);
            if (session == 0 && packet.container_id != 0 && packet.container_id != 0xff)
                || (session != 0 && packet.container_id != sync_id)
            {
                crate::server::cluster_inventory_protocol::resync_inventory_screen(player);
                return;
            }
            if player.try_on_handled_screen_closed().is_err() {
                crate::server::cluster_inventory_protocol::resync_inventory_screen(player);
                return;
            }
            if packet.container_id == 0 || packet.container_id == 0xff {
                self.inventory_opened.store(false, Ordering::Relaxed);
            }
            self.try_enqueue_client_packet(&SContainerClose {
                container_id: packet.container_id,
                container_type: packet.container_type,
                server_initiated_close: false,
            });
            return;
        }
        if packet.container_id == 0 || packet.container_id == 0xff {
            self.inventory_opened.store(false, Ordering::Relaxed);
        }
        player.on_handled_screen_closed();

        self.try_enqueue_client_packet(&SContainerClose {
            container_id: packet.container_id,
            container_type: packet.container_type,
            server_initiated_close: false,
        });

        // Sync the cursor (make it empty) to Bedrock client
        self.try_enqueue_client_packet(&CInventoryContent {
            container_id: VarUInt(59), // Cursor container ID
            slots: vec![NetworkItemStackDescriptor::default()],
            full_container_name: FullContainerName {
                container_name: ContainerName::Cursor,
                dynamic_id: None,
            },
            storage_item: NetworkItemStackDescriptor::default(),
        });

        // Sync the inventory content to Bedrock client
        let slots = player
            .inventory()
            .main_inventory
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(NetworkItemStackDescriptor::from)
            .collect();
        self.try_enqueue_client_packet(&CInventoryContent {
            container_id: VarUInt(0), // player inventory
            slots,
            full_container_name: FullContainerName {
                container_name: ContainerName::Inventory,
                dynamic_id: None,
            },
            storage_item: NetworkItemStackDescriptor::default(),
        });
    }
}
