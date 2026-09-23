use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, command};
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};

const DESCRIPTION: &str = "Hides you from player listings, or reveals you again.";
const PERMISSION: &str = "minecraft:command.hide";

struct HideExecutor;

impl CommandExecutor for HideExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let Some(server) = context.source.server.clone() else {
            return Ok(0);
        };
        let now_hidden = if let Some(player) = context.source.as_player() {
            crate::server::cluster_hide::toggle_hide_for(&server, &player)
        } else if let Some(lobby) = context.source.output.as_pre_join() {
            crate::server::cluster_hide::toggle_hide_lobby(&server, &lobby)
        } else {
            context
                .source
                .send_error(TextComponent::text("Only players can use /hide."));
            return Ok(0);
        };
        let Some(now_hidden) = now_hidden else {
            context.source.send_error(TextComponent::text(
                "Unable to update visibility right now.",
            ));
            return Ok(0);
        };
        if now_hidden {
            context
                .source
                .send_feedback(TextComponent::text("You are now hidden."), false);
        } else {
            context
                .source
                .send_feedback(TextComponent::text("You are now visible."), false);
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
        command("hide", DESCRIPTION)
            .requires(PERMISSION)
            .executes(HideExecutor),
    );
}
