#[allow(clippy::wildcard_imports)]
use super::*;
use pumpkin_protocol::java::server::play::SSetCommandMinecart;

impl JavaClient {
    pub fn handle_set_command_minecart(&self, player: &Player, packet: &SSetCommandMinecart<'_>) {
        if player.cluster_gid().is_some() {
            player.send_system_message(&TextComponent::text(
                "Command minecart editing is unavailable.",
            ));
            return;
        }

        if player.permission_lvl.load() < PermissionLvl::Two {
            return;
        }

        let world = player.world();
        if let Some(entity) = world.get_entity_by_id(packet.entity_id.0) {
            debug!(
                "Player {} updated command minecart {} command to: {}",
                player.gameprofile.name,
                entity.get_entity().entity_id,
                packet.command
            );
        }
    }
}
