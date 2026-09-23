#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_paddle_boat(&self, player: &Arc<Player>, packet: &SPaddleBoat) {
        if player
            .world()
            .server
            .upgrade()
            .is_some_and(|server| server.advanced_config.cluster.enabled)
        {
            player.client.try_kick(
                crate::net::DisconnectReason::Unknown,
                &TextComponent::text("Vehicle input could not be verified"),
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
