#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_player_ground(
        &self,
        player: &Player,
        ground: &SSetPlayerGround,
        server: &Server,
    ) {
        // A movement packet was received this tick — tracked for SClientTickEnd zeroing.
        self.received_movement_this_tick
            .store(true, Ordering::Relaxed);
        if server.advanced_config.cluster.enabled {
            if player.cluster_gid().is_none() {
                return;
            }
            crate::server::cluster_controller::submit_patch(
                server,
                player,
                crate::server::cluster_controller::ControllerPatch::Ground(ground.on_ground),
            );
            return;
        }
        player
            .living_entity
            .entity
            .on_ground
            .store(ground.on_ground, Ordering::Relaxed);
    }
}
