use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::game_profile::GameProfileArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::command::suggestion::provider::{SuggestionProvider, SuggestionProviderResult};
use crate::command::suggestion::suggestions::SuggestionsBuilder;
use crate::data::SaveJSONConfiguration;

const DESCRIPTION: &str = "Revokes operator status from a player.";
const PERMISSION: &str = "minecraft:command.deop";

const ERROR_DEOP_FAILED: CommandErrorType<0> = CommandErrorType::new(
    pumpkin_data::translation::java::COMMANDS_DEOP_FAILED,
    pumpkin_data::translation::bedrock::COMMANDS_DEOP_FAILED,
);

struct DeopExecutor;

impl CommandExecutor for DeopExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = GameProfileArgumentType::get(context, "targets")?;
        let server = context.source.server();

        let mut revoked: Vec<(uuid::Uuid, String)> = Vec::new();
        {
            let mut config = server
                .data
                .operator_config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            for profile in &targets {
                if let Some(op_index) = config.ops.iter().position(|o| o.uuid == profile.id) {
                    config.ops.remove(op_index);
                    revoked.push((profile.id, profile.name.clone()));
                }
            }

            if !revoked.is_empty() {
                config.save();
            }
        }

        if revoked.is_empty() {
            return Err(ERROR_DEOP_FAILED.create_without_context());
        }

        for (id, name) in &revoked {
            crate::server::cluster_admin_apply::publish_op_revoke(
                server,
                *id,
                name,
                &context.source.name,
            );
        }

        for (id, name) in &revoked {
            if let Some(player) = server.get_player_by_uuid(*id)
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let command_dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(
                    &server_arc,
                    PermissionLvl::Zero,
                    &command_dispatcher,
                );
            }

            let msg = TextComponent::translate_cross(
                pumpkin_data::translation::java::COMMANDS_DEOP_SUCCESS,
                pumpkin_data::translation::bedrock::COMMANDS_DEOP_SUCCESS,
                [TextComponent::text(name.clone())],
            );
            context.source.send_feedback(msg, true);
        }

        crate::command::commands::whitelist::kick_non_whitelisted_players(server);

        Ok(revoked.len() as i32)
    }
}

struct DeopSuggestionProvider;

fn operator_completion_names(ops: &[pumpkin_config::op::Op]) -> Vec<String> {
    ops.iter().map(|op| op.name.clone()).collect()
}

impl SuggestionProvider for DeopSuggestionProvider {
    fn suggest(
        &self,
        context: &CommandContext,
        mut builder: SuggestionsBuilder,
    ) -> SuggestionProviderResult {
        let ops = context
            .server()
            .data
            .operator_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for name in operator_completion_names(&ops.ops) {
            // `ops.json` is cluster-replicated, so this is the complete
            // cluster-visible operator set, including operators hosted by a
            // different peer.  Filter here instead of relying on the client
            // to discard unrelated candidates.
            builder = builder.filter_and_suggest_one(name);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_config::op::Op;
    use pumpkin_util::PermissionLvl;
    use uuid::Uuid;

    use super::operator_completion_names;

    #[test]
    fn operator_completion_uses_the_replicated_operator_records() {
        let remote_op = Op::new(
            Uuid::from_u128(1),
            String::from("RemoteOperator"),
            PermissionLvl::Four,
            false,
        );
        assert_eq!(
            operator_completion_names(&[remote_op]),
            vec![String::from("RemoteOperator")]
        );
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Three),
    ));

    dispatcher.register(
        command("deop", DESCRIPTION).requires(PERMISSION).then(
            argument("targets", GameProfileArgumentType)
                .suggests(DeopSuggestionProvider)
                .executes(DeopExecutor),
        ),
    );
}
