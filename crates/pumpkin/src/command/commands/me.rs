use pumpkin_data::world::EMOTE_COMMAND;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;
use tracing::info;

use crate::command::CommandSender;
use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};

const DESCRIPTION: &str = "Broadcasts a narrative message about yourself.";
const PERMISSION: &str = "minecraft:command.me";

struct MeExecutor;

impl CommandExecutor for MeExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let msg = StringArgumentType::get(context, "action")?;
        let sender = &context.source;
        let server = sender.server();

        let hidden_sender = context
            .source
            .player_or_none()
            .is_some_and(|player| crate::server::cluster_hide::is_hidden_player(server, &player))
            || matches!(&context.source.output, CommandSender::PreJoin(lobby) if crate::server::cluster_hide::is_hidden_gid(server, &lobby.gid));
        if hidden_sender {
            let body = TextComponent::text(msg.to_string());
            for viewer in server.get_all_players() {
                if viewer.gameprofile.id
                    != context
                        .source
                        .player_or_none()
                        .map(|player| player.gameprofile.id)
                        .unwrap_or_default()
                {
                    continue;
                }
                viewer.send_message(&body, EMOTE_COMMAND, &context.source.display_name, None);
            }
        } else {
            let message = TextComponent::text(msg.to_string());
            if server.advanced_config.cluster.enabled {
                let Ok(permit) = server.cluster_chat_presentation_tx.try_reserve() else {
                    context
                        .source
                        .send_error(TextComponent::text("Cluster chat queue is full"));
                    return Ok(0);
                };
                let sent = if let Some(player) = context.source.player_or_none() {
                    crate::server::cluster_chat_out::broadcast_emote_from_player(
                        server, &player, msg,
                    )
                } else {
                    crate::server::cluster_chat_out::broadcast_emote_from_console(
                        server,
                        &context.source.name,
                        msg,
                    )
                };
                if !sent {
                    context
                        .source
                        .send_error(TextComponent::text("Cluster chat queue is full"));
                    return Ok(0);
                }
                permit.send(crate::server::cluster_chat_presentation::ChatPresentation {
                    message,
                    sender: context.source.display_name.clone(),
                    chat_type: EMOTE_COMMAND,
                });
            } else {
                server.broadcast_message(
                    &message,
                    &context.source.display_name,
                    EMOTE_COMMAND,
                    None,
                );
            }
        }
        if let Some(player) = context.source.player_or_none() {
            info!("* {} {}", player.gameprofile.name, msg);
        } else {
            info!("* {} {}", context.source.name, msg);
        }

        Ok(1)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Zero),
    ));

    dispatcher.register(
        command("me", DESCRIPTION)
            .requires(PERMISSION)
            .then(argument("action", StringArgumentType::GreedyPhrase).executes(MeExecutor)),
    );
}
