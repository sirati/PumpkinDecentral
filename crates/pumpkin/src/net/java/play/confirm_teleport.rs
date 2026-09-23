#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_confirm_teleport(&self, player: &Player, confirm_teleport: &SConfirmTeleport) {
        if let Some(server) = player.world().server.upgrade()
            && server.advanced_config.cluster.enabled
        {
            if player.cluster_gid().is_none() {
                return;
            }
            crate::server::cluster_controller::submit_patch(
                &server,
                player,
                crate::server::cluster_controller::ControllerPatch::TeleportConfirm(
                    confirm_teleport.teleport_id,
                ),
            );
            return;
        }
        enum TeleportResult {
            Success,
            WrongId,
            NotTeleporting,
        }

        let result = {
            let mut awaiting_teleport = player
                .awaiting_teleport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((id, position)) = awaiting_teleport.as_ref() {
                if id == &confirm_teleport.teleport_id {
                    // We should set the position now to what we requested in the teleport packet.
                    // This may fix issues when the client sends the position while being teleported.
                    player.get_entity().set_pos(*position);
                    *awaiting_teleport = None;
                    TeleportResult::Success
                } else {
                    TeleportResult::WrongId
                }
            } else {
                TeleportResult::NotTeleporting
            }
        };

        match result {
            TeleportResult::Success => {
                if let Some(gid) = player.cluster_gid()
                    && let Some(server) = player.world().server.upgrade()
                {
                    if !server.cluster_controller.note_teleport(gid, None) {
                        self.try_kick(&TextComponent::text("Controller teleport queue saturated"));
                    }
                }
            }
            TeleportResult::WrongId => {
                self.try_kick(&TextComponent::text("Wrong teleport id"));
            }
            TeleportResult::NotTeleporting => {
                self.try_kick(&TextComponent::text(
                    "Send Teleport confirm, but we did not teleport",
                ));
            }
        }
    }
}
