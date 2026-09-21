use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::server::cluster_lobby::{
    EXIT_LOBBY_OTHERS_PERMISSION, FORCED_LOBBY_PERMISSION,
};

fn resolve_lobby_target(
    server: &crate::server::Server,
    name: &str,
) -> Option<(pumpkin_cluster::identity::GlobalPlayerId, bool)> {
    if let Some(waiter) = server
        .lobby_waiters
        .load()
        .iter()
        .find(|waiter| waiter.profile.name.eq_ignore_ascii_case(name))
    {
        return Some((waiter.gid, true));
    }
    if let Some(player) = server
        .get_all_players()
        .into_iter()
        .find(|player| player.gameprofile.name.eq_ignore_ascii_case(name))
        && let Some(gid) = player.cluster_gid()
    {
        return Some((gid, false));
    }
    crate::server::cluster_presence::remote_presence_entries()
        .into_iter()
        .find(|(_, entry)| entry.name.eq_ignore_ascii_case(name))
        .map(|(gid, entry)| (gid, entry.in_lobby))
}

const DESCRIPTION: &str = "Moves you into the lobby wait room.";
const FORCE_DESCRIPTION: &str = "Moves another player into the lobby wait room.";
const EXIT_DESCRIPTION: &str = "Leaves the lobby wait room and resumes logging in.";

struct LobbyExecutor;

impl CommandExecutor for LobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        match &context.source.output {
            crate::command::CommandSender::Lobby(waiter) => {
                waiter.hold();
                context.source.send_feedback(TextComponent::text("You are held in the lobby."), false);
                Ok(1)
            }
            crate::command::CommandSender::Player(player) => {
                let Some(target) = player.cluster_gid() else {
                    context.source.send_error(TextComponent::text("The lobby is unavailable."));
                    return Ok(0);
                };
                if !crate::server::cluster_lobby_control::route_lobby_control(
                    context.source.server(),
                    pumpkin_cluster::lobby_control::LobbyControl::Hold { target },
                ) {
                    context.source.send_error(TextComponent::text("The lobby host is unavailable."));
                    return Ok(0);
                }
                context.source.send_feedback(TextComponent::text("Entering the lobby."), false);
                Ok(1)
            }
            _ => {
                context.source.send_error(TextComponent::text("This command requires a player."));
                Ok(0)
            }
        }
    }
}

struct ForceLobbyExecutor;

impl CommandExecutor for ForceLobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let name = StringArgumentType::get(context, "target")?;
        let server = context.source.server();
        let Some((target, _)) = resolve_lobby_target(server, name)
        else {
            context.source.send_error(TextComponent::text("No such player is online."));
            return Ok(0);
        };
        if !crate::server::cluster_lobby_control::route_lobby_control(
            server,
            pumpkin_cluster::lobby_control::LobbyControl::Hold { target },
        ) {
            context.source.send_error(TextComponent::text("The player lobby host is unavailable."));
            return Ok(0);
        }
        context.source.send_feedback(
            TextComponent::text(format!("Held {name} in the lobby.")),
            true,
        );
        Ok(1)
    }
}

struct ExitLobbyExecutor;

impl CommandExecutor for ExitLobbyExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let crate::command::CommandSender::Lobby(waiter) = &context.source.output else {
            context.source.send_error(TextComponent::text("You are not waiting in the lobby."));
            return Ok(0);
        };
        if waiter.forced.load(std::sync::atomic::Ordering::Relaxed)
            && !context.source.has_permission(FORCED_LOBBY_PERMISSION)
        {
            context.source.send_error(TextComponent::text(
                "You were moved here by an operator and cannot leave on your own.",
            ));
            return Ok(0);
        }
        waiter.release();
        context.source.send_feedback(
            TextComponent::text("You left the lobby wait room."),
            false,
        );
        Ok(1)
    }
}

struct ExitLobbyOtherExecutor;

impl CommandExecutor for ExitLobbyOtherExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let name = StringArgumentType::get(context, "target")?;
        let Some(server) = context.source.server.clone() else {
            return Ok(0);
        };
        let Some((target, in_lobby)) = resolve_lobby_target(&server, name)
        else {
            context
                .source
                .send_error(TextComponent::text("No such player is online."));
            return Ok(0);
        };
        if !in_lobby {
            context.source.send_error(TextComponent::text("That player is not waiting in the lobby."));
            return Ok(0);
        }
        if !crate::server::cluster_lobby_control::route_lobby_control(
            &server,
            pumpkin_cluster::lobby_control::LobbyControl::Release { target },
        ) {
            context.source.send_error(TextComponent::text("The player lobby host is unavailable."));
            return Ok(0);
        }
        context.source.send_feedback(
            TextComponent::text(format!("Brought {name} out of the lobby.")),
            true,
        );
        Ok(1)
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
            .then(argument("target", StringArgumentType::SingleWord).executes(ForceLobbyExecutor)),
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
