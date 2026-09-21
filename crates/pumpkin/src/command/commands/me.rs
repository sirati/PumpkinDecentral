use pumpkin_data::world::EMOTE_COMMAND;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;
use tracing::info;

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
            .is_some_and(|player| crate::server::cluster_hide::is_hidden_player(&player));
        if hidden_sender {
            let body = TextComponent::text(msg.to_string());
            for viewer in server.get_all_players() {
                if viewer.gameprofile.id != context.source.player_or_none().map(|player| player.gameprofile.id).unwrap_or_default()
                    && !viewer.has_permission(server, crate::server::cluster_hide::HIDE_PERMISSION)
                {
                    continue;
                }
                viewer.send_message(&body, EMOTE_COMMAND, &context.source.display_name, None);
            }
        } else {
            server.broadcast_message(
                &TextComponent::text(msg.to_string()),
                &context.source.display_name,
                EMOTE_COMMAND,
                None,
            );
        }
        if let Some(player) = context.source.player_or_none() {
            info!("* {} {}", player.gameprofile.name, msg);
            crate::server::cluster_chat_out::broadcast_emote_from_player(server, &player, msg);
        } else {
            info!("* {} {}", context.source.name, msg);
            crate::server::cluster_chat_out::broadcast_emote_from_console(
                server,
                &context.source.name,
                msg,
            );
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
