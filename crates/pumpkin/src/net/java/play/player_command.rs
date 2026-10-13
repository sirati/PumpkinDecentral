#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_player_command(
        &self,
        player: &Arc<Player>,
        command: &SPlayerCommand,
        server: &Arc<Server>,
    ) {
        if command.entity_id != player.entity_id().into() {
            return;
        }
        if !player.has_client_loaded() {
            return;
        }
        if server.advanced_config.cluster.enabled {
            let patch = match command.action {
                Action::LeaveBed => Some(crate::server::cluster_controller::ControllerPatch::Wake),
                Action::StartSprinting => Some(
                    crate::server::cluster_controller::ControllerPatch::Sprint(true),
                ),
                Action::StopSprinting => Some(
                    crate::server::cluster_controller::ControllerPatch::Sprint(false),
                ),
                Action::StartFlyingElytra => {
                    Some(crate::server::cluster_controller::ControllerPatch::Pose {
                        swimming: None,
                        gliding: Some(true),
                    })
                }
                _ => None,
            };
            if let Some(patch) = patch {
                crate::server::cluster_controller::submit_patch(server, player, patch);
                return;
            }
        }
        player.update_last_action_time();

        let entity = &player.get_entity();
        match command.action {
            Action::StartSprinting => {
                if !entity.is_sprinting() {
                    send_cancellable_blocking! {{
                        server;
                        PlayerToggleSprintEvent::new(player.clone(), true);
                        'after: {
                            player.set_sprinting(event.is_sprinting);
                            player.update_player_pose();
                            pumpkin_cluster::visual::emit_sprint(
                                player.cluster_gid(),
                                pumpkin_cluster::visual::tick_from_counter(
                                    player.tick_counter.load(Ordering::Relaxed),
                                ),
                                event.is_sprinting,
                            );
                        }
                    }}
                }
            }
            Action::StopSprinting => {
                if entity.is_sprinting() {
                    send_cancellable_blocking! {{
                        server;
                        PlayerToggleSprintEvent::new(player.clone(), false);
                        'after: {
                            player.set_sprinting(event.is_sprinting);
                            player.update_player_pose();
                            pumpkin_cluster::visual::emit_sprint(
                                player.cluster_gid(),
                                pumpkin_cluster::visual::tick_from_counter(
                                    player.tick_counter.load(Ordering::Relaxed),
                                ),
                                event.is_sprinting,
                            );
                        }
                    }}
                }
            }
            Action::LeaveBed => player.wake_up(),

            Action::StartHorseJump | Action::StopHorseJump | Action::OpenVehicleInventory => {
                debug!("todo");
            }
            Action::StartFlyingElytra => {
                let fall_flying = entity.check_fall_flying();
                if entity.is_fall_flying() != fall_flying {
                    let mut event = crate::plugin::api::events::entity::entity_toggle_glide::EntityToggleGlideEvent::new(
                        entity.entity_id,
                        fall_flying,
                    );
                    server.plugin_manager.fire_blocking(server, &mut event);
                    if !event.cancelled {
                        entity.set_fall_flying(event.is_gliding);
                        pumpkin_cluster::visual::emit_pose(
                            player.cluster_gid(),
                            pumpkin_cluster::visual::tick_from_counter(
                                player.tick_counter.load(Ordering::Relaxed),
                            ),
                            entity.is_swimming(),
                            event.is_gliding,
                        );
                    }
                }
            }
            // <= 1.21.5
            Action::StartSneaking | Action::StopSneaking => {
                let want_sneaking = matches!(command.action, Action::StartSneaking);
                let was_sneaking = player.get_entity().is_sneaking();
                self.handle_player_input(
                    player,
                    &SPlayerInput {
                        input: SPlayerInput::SNEAK,
                    },
                    server,
                );
                let now_sneaking = player.get_entity().is_sneaking();
                if was_sneaking != want_sneaking && now_sneaking == want_sneaking {
                    pumpkin_cluster::visual::emit_sneak(
                        player.cluster_gid(),
                        pumpkin_cluster::visual::tick_from_counter(
                            player.tick_counter.load(Ordering::Relaxed),
                        ),
                        want_sneaking,
                    );
                }
            }
        }
    }
}
