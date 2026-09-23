use pumpkin_cluster::admin_sync::{AdminMutation, OpRevoke};
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
        if server.advanced_config.cluster.enabled {
            let snapshot = server.cluster_admin_view.load_full();
            let mut changed = 0;
            let mut busy = false;
            for profile in &targets {
                if !snapshot
                    .operators
                    .iter()
                    .any(|entry| entry.uuid == *profile.id.as_bytes())
                {
                    continue;
                }
                let mutation = AdminMutation::RevokeOp(OpRevoke::new(
                    *profile.id.as_bytes(),
                    profile.name.clone(),
                ));
                let Some(prepared) = crate::server::cluster_admin_apply::prepare_admin_mutation(
                    server,
                    mutation,
                    &context.source.name,
                ) else {
                    busy = true;
                    continue;
                };
                crate::server::cluster_admin_apply::commit_admin_mutation(server, prepared);
                context.source.send_feedback(
                    TextComponent::text(format!(
                        "Queued /deop for {}; pending primary durability.",
                        profile.name
                    )),
                    false,
                );
                changed += 1;
            }
            if changed == 0 {
                return if busy {
                    Err(crate::server::cluster_admin_apply::admin_busy_error())
                } else {
                    Err(ERROR_DEOP_FAILED.create_without_context())
                };
            }
            return Ok(changed);
        }

        let mut revoked: Vec<(uuid::Uuid, String)> = Vec::new();
        let mut prepared = Vec::new();
        let mut queue_full = false;
        {
            let mut config = server
                .data
                .operator_config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            for profile in &targets {
                if let Some(op_index) = config.ops.iter().position(|o| o.uuid == profile.id) {
                    let mutation = AdminMutation::RevokeOp(OpRevoke::new(
                        *profile.id.as_bytes(),
                        profile.name.clone(),
                    ));
                    let Some(reservation) =
                        crate::server::cluster_admin_apply::prepare_admin_mutation(
                            server,
                            mutation,
                            &context.source.name,
                        )
                    else {
                        queue_full = true;
                        continue;
                    };
                    prepared.push(reservation);
                    if !crate::server::cluster_admin_apply::defer_local_admin_apply(server) {
                        config.ops.remove(op_index);
                    }
                    revoked.push((profile.id, profile.name.clone()));
                }
            }

            if !revoked.is_empty()
                && crate::server::cluster_admin_apply::stores_authoritative_files(server)
                && !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
            {
                config.save();
            }
        }

        if revoked.is_empty() {
            if queue_full {
                return Err(crate::server::cluster_admin_apply::admin_busy_error());
            }
            return Err(ERROR_DEOP_FAILED.create_without_context());
        }

        if queue_full {
            context.source.send_feedback(
                TextComponent::text(
                    "Some cluster admin targets could not be queued; they were not changed.",
                ),
                false,
            );
        }

        for reservation in prepared {
            crate::server::cluster_admin_apply::commit_admin_mutation(server, reservation);
        }

        for (id, name) in &revoked {
            if !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
                && let Some(player) = server.get_player_by_uuid(*id)
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let command_dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(&server_arc, PermissionLvl::Zero, &command_dispatcher);
            }

            let msg = TextComponent::translate_cross(
                pumpkin_data::translation::java::COMMANDS_DEOP_SUCCESS,
                pumpkin_data::translation::bedrock::COMMANDS_DEOP_SUCCESS,
                [TextComponent::text(name.clone())],
            );
            if crate::server::cluster_admin_apply::mutation_pending_primary(server) {
                context.source.send_feedback(
                    TextComponent::text(format!(
                        "Queued /deop for {name}; pending primary durability."
                    )),
                    false,
                );
            } else {
                context.source.send_feedback(msg, true);
            }
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
        let names = if context.server().advanced_config.cluster.enabled {
            context
                .server()
                .cluster_admin_view
                .load()
                .operators
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>()
        } else {
            let ops = context
                .server()
                .data
                .operator_config
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            operator_completion_names(&ops.ops)
        };
        for name in names {
            if !crate::server::cluster_chat_pm::is_hidden_completion_name(context.server(), &name) {
                builder = builder.filter_and_suggest_one(name);
            }
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
