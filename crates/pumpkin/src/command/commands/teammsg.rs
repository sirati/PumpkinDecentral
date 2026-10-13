use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use pumpkin_data::translation;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

const DESCRIPTION: &str = "Sends a message to your team.";
const PERMISSION: &str = "minecraft:command.teammsg";
const ARG_MESSAGE: &str = "message";

const NO_TEAM_ERROR: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_TEAMMSG_FAILED_NOTEAM,
    translation::java::COMMANDS_TEAMMSG_FAILED_NOTEAM,
);

struct TeamMsgCommandExecutor;

impl CommandExecutor for TeamMsgCommandExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let server = context.source.server();
        let sender = match &context.source.output {
            crate::command::CommandSender::Player(player)
            | crate::command::CommandSender::Lobby(player) => player.cluster_gid(),
            crate::command::CommandSender::PreJoin(session) => Some(session.gid),
            _ => {
                context.source.player_or_err()?;
                None
            }
        };
        let sender_name = context.source.name.clone();
        let world = context.world();
        let message_text = StringArgumentType::get(context, ARG_MESSAGE)?;
        let teams = world.team_chat_snapshot.load();
        let team = teams
            .values()
            .find(|team| team.players.contains(&sender_name))
            .ok_or_else(|| NO_TEAM_ERROR.create_without_context())?;
        if server.advanced_config.cluster.enabled {
            let sender = sender.ok_or_else(|| {
                pumpkin_command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                    .create_without_context(TextComponent::text("Team chat sender is unavailable"))
            })?;
            let (uuid, session) = match &context.source.output {
                crate::command::CommandSender::Player(player)
                | crate::command::CommandSender::Lobby(player) => (
                    *player.gameprofile.id.as_bytes(),
                    player
                        .cluster_owner_install_generation
                        .load(std::sync::atomic::Ordering::Acquire),
                ),
                crate::command::CommandSender::PreJoin(session) => {
                    (*session.profile.id.as_bytes(), session.session_generation)
                }
                _ => {
                    return Err(
                        pumpkin_command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                            .create_without_context(TextComponent::text(
                                "Team chat sender is unavailable",
                            )),
                    );
                }
            };
            let broadcast = crate::server::cluster_chat_pm::capture_team_message(
                server,
                world,
                team,
                sender,
                uuid,
                session,
                sender_name.clone(),
                context.source.display_name.clone(),
                message_text,
            )
            .ok_or_else(|| {
                pumpkin_command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                    .create_without_context(TextComponent::text(
                        "Team chat sender or recipient capacity is unavailable",
                    ))
            })?;
            if !crate::server::cluster_chat_pm::broadcast_team(server, &broadcast) {
                return Err(
                    pumpkin_command::errors::error_types::DISPATCHER_PARSE_EXCEPTION
                        .create_without_context(TextComponent::text(
                            "Team chat capacity exhausted",
                        )),
                );
            }
            return Ok(crate::server::cluster_chat_pm::deliver_team_frozen(
                server, &broadcast,
            ));
        }
        let recipients = crate::server::cluster_chat_pm::deliver_team_local(
            server,
            world,
            team,
            &sender_name,
            context.source.display_name.clone(),
            message_text,
        );

        Ok(recipients)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Allow,
    ));

    // Register /teammsg <message>
    dispatcher.register(command("teammsg", DESCRIPTION).requires(PERMISSION).then(
        argument(ARG_MESSAGE, StringArgumentType::GreedyPhrase).executes(TeamMsgCommandExecutor),
    ));

    // Register alias /tm <message>
    dispatcher.register(command("tm", DESCRIPTION).requires(PERMISSION).then(
        argument(ARG_MESSAGE, StringArgumentType::GreedyPhrase).executes(TeamMsgCommandExecutor),
    ));
}
