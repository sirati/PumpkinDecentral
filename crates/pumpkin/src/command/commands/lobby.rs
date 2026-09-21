use std::sync::atomic::Ordering;

use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::argument_types::entity::EntityArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::server::cluster_lobby::{
    EXIT_LOBBY_OTHERS_PERMISSION, FORCED_LOBBY_PERMISSION, lobby_enter_manual, lobby_exit_manual,
};

const DESCRIPTION: &str = "Moves you into the lobby wait room.";
const FORCE_DESCRIPTION: &str = "Moves another player into the lobby wait room.";
const EXIT_DESCRIPTION: &str = "Leaves the lobby wait room and resumes logging in.";

struct LobbyExecutor;

impl CommandExecutor for LobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let Some(player) = context.source.as_player() else {
            context
                .source
                .send_error(TextComponent::text("Only players can use /lobby."));
            return Ok(0);
        };
        if lobby_enter_manual(&player) {
            context.source.send_feedback(
                TextComponent::text("You are now waiting in the lobby."),
                false,
            );
            Ok(1)
        } else {
            context
                .source
                .send_error(TextComponent::text("You are already waiting in the lobby."));
            Ok(0)
        }
    }
}

struct ForceLobbyExecutor;

impl CommandExecutor for ForceLobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let target = EntityArgumentType::get_player(context, "target")?;
        if target.is_in_cluster_lobby() {
            context.source.send_error(TextComponent::text(
                "That player is already waiting in the lobby.",
            ));
            return Ok(0);
        }
        if lobby_enter_manual(&target) {
            target.lobby_forced.store(true, Ordering::Relaxed);
            target.send_system_message(&TextComponent::text(
                "You were moved to the lobby wait room.",
            ));
            context.source.send_feedback(
                TextComponent::text(format!(
                    "Moved {} to the lobby.",
                    target.gameprofile.name
                )),
                true,
            );
            Ok(1)
        } else {
            context.source.send_error(TextComponent::text(
                "That player is already waiting in the lobby.",
            ));
            Ok(0)
        }
    }
}

struct ExitLobbyExecutor;

impl CommandExecutor for ExitLobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let Some(player) = context.source.as_player() else {
            context
                .source
                .send_error(TextComponent::text("Only players can use /exitlobby."));
            return Ok(0);
        };
        if player.lobby_forced.load(Ordering::Relaxed)
            && !context.source.has_permission(FORCED_LOBBY_PERMISSION)
        {
            context.source.send_error(TextComponent::text(
                "You were moved here by an operator and cannot leave on your own.",
            ));
            return Ok(0);
        }
        if lobby_exit_manual(&player) {
            context.source.send_feedback(
                TextComponent::text("You left the lobby wait room."),
                false,
            );
            Ok(1)
        } else {
            context
                .source
                .send_error(TextComponent::text("You are not waiting in the lobby."));
            Ok(0)
        }
    }
}

struct ExitLobbyOtherExecutor;

impl CommandExecutor for ExitLobbyOtherExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let name = StringArgumentType::get(context, "target")?;
        let Some(server) = context.source.server.clone() else {
            return Ok(0);
        };
        let Some(target) = server
            .get_all_players()
            .into_iter()
            .find(|candidate| candidate.gameprofile.name.eq_ignore_ascii_case(name))
        else {
            context
                .source
                .send_error(TextComponent::text("No such player is online."));
            return Ok(0);
        };
        if lobby_exit_manual(&target) {
            target.send_system_message(&TextComponent::text("You left the lobby wait room."));
            context.source.send_feedback(
                TextComponent::text(format!(
                    "Brought {} out of the lobby.",
                    target.gameprofile.name
                )),
                true,
            );
            Ok(1)
        } else {
            context.source.send_error(TextComponent::text(
                "That player is not waiting in the lobby.",
            ));
            Ok(0)
        }
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        FORCED_LOBBY_PERMISSION,
        FORCE_DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));
    registry.register_permission_or_panic(Permission::new(
        EXIT_LOBBY_OTHERS_PERMISSION,
        EXIT_DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Two),
    ));

    dispatcher.register(command("lobby", DESCRIPTION).executes(LobbyExecutor));
    dispatcher.register(
        command("forcelobby", FORCE_DESCRIPTION)
            .requires(FORCED_LOBBY_PERMISSION)
            .then(argument("target", EntityArgumentType::Player).executes(ForceLobbyExecutor)),
    );
    dispatcher.register(
        command("exitlobby", EXIT_DESCRIPTION)
            .executes(ExitLobbyExecutor)
            .then(
                argument("target", StringArgumentType::SingleWord)
                    .requires(EXIT_LOBBY_OTHERS_PERMISSION)
                    .executes(ExitLobbyOtherExecutor),
            ),
    );
}
