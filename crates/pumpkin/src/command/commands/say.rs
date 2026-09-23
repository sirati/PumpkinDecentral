use pumpkin_data::world::SAY_COMMAND;
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use tracing::info;

const NAME: &str = "say";

const DESCRIPTION: &str = "Broadcast a message to all Players.";
const PERMISSION: &str = "minecraft:command.say";
const ARG_MESSAGE: &str = "message";

struct Executor;

impl CommandExecutor for Executor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let msg = context.get_argument::<String>(ARG_MESSAGE)?;

        let server = context.server();
        let message = TextComponent::text(msg.clone());
        if server.advanced_config.cluster.enabled {
            let Ok(permit) = server.cluster_chat_presentation_tx.try_reserve() else {
                context
                    .source
                    .send_error(TextComponent::text("Cluster chat queue is full"));
                return Ok(0);
            };
            let sent = if let Some(player) = context.source.player_or_none() {
                crate::server::cluster_chat_out::broadcast_say_from_player(server, &player, msg)
            } else {
                crate::server::cluster_chat_out::broadcast_say_from_console(
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
                chat_type: SAY_COMMAND,
            });
        } else {
            server.broadcast_message(&message, &context.source.display_name, SAY_COMMAND, None);
        }
        if let Some(player) = context.source.player_or_none() {
            info!("<chat> {}: {}", player.gameprofile.name, msg);
        } else {
            info!("<chat> {}: {}", context.source.name, msg);
        }

        Ok(1)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));
    dispatcher.register(
        command(NAME, DESCRIPTION)
            .requires(PERMISSION)
            .then(argument(ARG_MESSAGE, StringArgumentType::GreedyPhrase).executes(Executor)),
    );
}
