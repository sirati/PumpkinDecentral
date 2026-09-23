#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub async fn handle_chat_command(
        &self,
        player: &Arc<Player>,
        server: &Arc<Server>,
        command: &SChatCommand<'_>,
    ) {
        player.update_last_action_time();
        if server.advanced_config.cluster.enabled {
            let command_str = command.command.strip_prefix('/').unwrap_or(command.command);
            if crate::server::cluster_session_ingress::stage(
                player,
                crate::server::cluster_session_ingress::StagedSessionInput::Command {
                    command: command_str.to_string(),
                    received_at: std::time::Instant::now(),
                },
            )
            .is_err()
            {
                player.send_system_message(&TextComponent::text("Cluster command queue is full"));
            }
            return;
        }
        if player.check_chat_spam(server, crate::entity::player::SpamType::Command) {
            return;
        }
        let command_str = command.command.strip_prefix('/').unwrap_or(command.command);
        send_cancellable! {{
            server;
            PlayerCommandSendEvent {
                player: player.clone(),
                command: command_str.to_string(),
                cancelled: false
            };

            'after: {
                let command = event.command;
                let dispatcher = server.command_dispatcher.load();
                dispatcher.handle_command(
                    &player.get_command_source(server),
                    &command,
                );

                if server.advanced_config.commands.log_console {
                    info!(
                        "Player ({}): executed command /{}",
                        player.gameprofile.name,
                        command
                    );
                }
            }
        }}
    }
}
