use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::argument_types::entity::{EntityArgumentType, NO_PLAYERS_ERROR_TYPE};
use crate::command::argument_types::entity_selector::EntitySelector;
use crate::command::context::command_context::CommandContext;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::entity::EntityBase;
use crate::net::DisconnectReason;

const DESCRIPTION: &str = "Kicks the target player from the server.";
const PERMISSION: &str = "minecraft:command.kick";

struct KickExecutor {
    has_reason: bool,
}

impl CommandExecutor for KickExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = EntityArgumentType::get_optional_players(context, "targets")?;

        let reason = if self.has_reason {
            let custom_reason = StringArgumentType::get(context, "reason")?;
            TextComponent::text(custom_reason.to_string())
        } else {
            TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_KICKED,
                translation::bedrock::DISCONNECT_KICKED,
                [],
            )
        };

        let reason_text = if self.has_reason {
            StringArgumentType::get(context, "reason")?.to_string()
        } else {
            String::from("Kicked by an operator.")
        };
        let issuer = context.source.name.clone();
        let cluster_server = context.source.server().clone();
        if targets.is_empty() {
            let requested = context
                .get_argument::<EntitySelector>("targets")
                .ok()
                .and_then(|selector| selector.player_name.clone());
            let Some(requested) = requested else {
                return Err(NO_PLAYERS_ERROR_TYPE.create_without_context());
            };
            let directory =
                crate::server::cluster_presence::admin_directory_snapshot(&cluster_server);
            let Some(delivery) = pumpkin_cluster::admin_sync::resolve_kick_by_name(
                &directory,
                &requested,
                reason_text.clone(),
                issuer.clone(),
            ) else {
                return Err(NO_PLAYERS_ERROR_TYPE.create_without_context());
            };
            if !crate::server::cluster_admin_apply::route_kick_request(
                &cluster_server,
                &delivery.request,
            ) {
                return Err(NO_PLAYERS_ERROR_TYPE.create_without_context());
            }
            let feedback = TextComponent::translate_cross(
                translation::java::COMMANDS_KICK_SUCCESS,
                translation::bedrock::COMMANDS_KICK_SUCCESS,
                [
                    TextComponent::text(delivery.request.target_name.clone()),
                    reason.clone(),
                ],
            );
            context
                .source
                .send_feedback(feedback.color_named(NamedColor::Blue), true);
            return Ok(1);
        }
        for target in &targets {
            target.kick(DisconnectReason::Kicked, &reason);
            crate::server::cluster_admin_apply::broadcast_kick_for_local_player(
                &cluster_server,
                target.cluster_gid(),
                &target.gameprofile.name,
                &reason_text,
                &issuer,
            );

            let feedback = if self.has_reason {
                TextComponent::translate_cross(
                    translation::java::COMMANDS_KICK_SUCCESS,
                    translation::bedrock::COMMANDS_KICK_SUCCESS_REASON,
                    [target.as_ref().get_display_name(), reason.clone()],
                )
            } else {
                TextComponent::translate_cross(
                    translation::java::COMMANDS_KICK_SUCCESS,
                    translation::bedrock::COMMANDS_KICK_SUCCESS,
                    [target.as_ref().get_display_name(), reason.clone()],
                )
            };

            context
                .source
                .send_feedback(feedback.color_named(NamedColor::Blue), true);
        }

        Ok(targets.len() as i32)
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Three),
    ));

    dispatcher.register(
        command("kick", DESCRIPTION).requires(PERMISSION).then(
            argument("targets", EntityArgumentType::Players)
                .executes(KickExecutor { has_reason: false })
                .then(
                    argument("reason", StringArgumentType::GreedyPhrase)
                        .executes(KickExecutor { has_reason: true }),
                ),
        ),
    );
}
