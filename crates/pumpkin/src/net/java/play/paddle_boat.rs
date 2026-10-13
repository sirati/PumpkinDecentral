#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_paddle_boat(&self, player: &Arc<Player>, packet: &SPaddleBoat) {
        if let Some(server) = player
            .world()
            .server
            .upgrade()
            .filter(|server| server.advanced_config.cluster.enabled)
        {
            crate::server::cluster_controller::submit_patch(
                &server,
                player,
                crate::server::cluster_controller::ControllerPatch::Paddles {
                    left: packet.left_paddle,
                    right: packet.right_paddle,
                },
            );
            return;
        }
        let vehicle = player
            .get_entity()
            .vehicle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(vehicle) = vehicle {
            vehicle.set_paddle_state(packet.left_paddle, packet.right_paddle);
        }
    }
}
