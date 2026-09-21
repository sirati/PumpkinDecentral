use pumpkin_data::world::{MSG_COMMAND_INCOMING, MSG_COMMAND_OUTGOING};
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::argument_types::entity_selector::EntitySelector;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::EntityBase;

const NAMES: [&str; 3] = ["msg", "tell", "w"];
const DESCRIPTION: &str = "Sends a private message to one or more players.";
const PERMISSION: &str = "minecraft:command.msg";

struct MsgExecutor;

impl CommandExecutor for MsgExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = match EntityArgumentType::get_players(context, "targets") {
            Ok(targets) => targets,
            Err(error) => {
                // No local player matched: retry as a cluster-wide private message to a
                // player online on another host before reporting the selector error.
                let selector = context.get_argument::<EntitySelector>("targets")?;
                if selector.uses_selector_variable || selector.entity_uuid.is_some() {
                    return Err(error);
                }
                let Some(target_name) = selector.player_name.clone() else {
                    return Err(error);
                };
                let message = StringArgumentType::get(context, "message")?.to_string();
                if crate::server::cluster_chat_pm::send_private_from_command(
                    context,
                    &target_name,
                    &message,
                )
                .is_none()
                {
                    return Err(error);
                }
                return Ok(1);
            }
        };
        let see_hidden =
            context
                .source
                .has_permission(crate::server::cluster_hide::HIDE_PERMISSION);
        let visible: Vec<_> = targets
            .into_iter()
            .filter(|target| see_hidden || !crate::server::cluster_hide::is_hidden_player(target))
            .collect();
        if visible.is_empty() {
            return Err(
                crate::command::argument_types::entity::NO_PLAYERS_ERROR_TYPE
                    .create_without_context(),
            );
        }
        let targets = visible;
        let msg = StringArgumentType::get(context, "message")?;

        let sender_name = &context.source.display_name;
        let msg_text = TextComponent::text(msg.to_string());

        if let Some(player) = context.source.player_or_none() {
            for target in &targets {
                player.send_message(
                    &msg_text,
                    MSG_COMMAND_OUTGOING,
                    &player.get_display_name(),
                    Some(&target.get_display_name()),
                );
                target.send_message(
                    &msg_text,
                    MSG_COMMAND_INCOMING,
                    &player.get_display_name(),
                    Some(&target.get_display_name()),
                );
            }
        } else {
            for target in &targets {
                target.send_message(
                    &msg_text,
                    MSG_COMMAND_INCOMING,
                    sender_name,
                    Some(&target.get_display_name()),
                );
            }
        }

        Ok(targets.len() as i32)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Zero),
    ));

    for name in NAMES {
        dispatcher.register(
            command(name, DESCRIPTION).requires(PERMISSION).then(
                argument("targets", EntityArgumentType::Players).then(
                    argument("message", StringArgumentType::GreedyPhrase).executes(MsgExecutor),
                ),
            ),
        );
    }
}
