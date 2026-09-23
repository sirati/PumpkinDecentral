#[allow(clippy::wildcard_imports)]
use super::*;
use pumpkin_protocol::java::server::play::SCloseContainer;

impl JavaClient {
    pub fn handle_close_container(&self, player: &Arc<Player>, packet: &SCloseContainer) {
        if player
            .world()
            .server
            .upgrade()
            .is_some_and(|server| server.advanced_config.cluster.enabled)
            && packet.window_id.0
                != i32::from(
                    if player
                        .cluster_current_inventory_session
                        .load(std::sync::atomic::Ordering::Acquire)
                        == 0
                    {
                        0
                    } else {
                        player
                            .screen_handler_sync_id
                            .load(std::sync::atomic::Ordering::Acquire)
                    },
                )
        {
            crate::server::cluster_inventory_protocol::resync_inventory_screen(player);
            return;
        }
        if player.try_on_handled_screen_closed().is_err() {
            crate::server::cluster_inventory_protocol::resync_inventory_screen(player);
        }
    }
}
