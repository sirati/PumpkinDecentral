use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::game_profile::GameProfileArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::command::suggestion::provider::{SuggestionProvider, SuggestionProviderResult};
use crate::command::suggestion::suggestions::SuggestionsBuilder;
use crate::data::SaveJSONConfiguration;
use pumpkin_cluster::admin_sync::{AdminMutation, OpGrant};
use pumpkin_config::op::Op;
use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

pub const ALREADY_OP_ERROR_TYPE: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_OP_FAILED,
    translation::bedrock::COMMANDS_OP_FAILED,
);

const DESCRIPTION: &str = "Grants operator status to a player.";
const PERMISSION: &str = "minecraft:command.op";
const ARG_TARGETS: &str = "targets";

struct OpCommandExecutor;

impl CommandExecutor for OpCommandExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let server = context.server();
        let profiles = GameProfileArgumentType::get(context, ARG_TARGETS)?;
        if server.advanced_config.cluster.enabled {
            let snapshot = server.cluster_admin_view.load_full();
            let level = server.basic_config.op_permission_level;
            let mut changed = 0;
            let mut busy = false;
            for profile in profiles {
                let existing = snapshot
                    .operators
                    .iter()
                    .find(|entry| entry.uuid == *profile.id.as_bytes());
                if existing.is_some_and(|entry| entry.level == level as u8) {
                    continue;
                }
                let mutation = AdminMutation::GrantOp(OpGrant::new(
                    *profile.id.as_bytes(),
                    profile.name.clone(),
                    level as u8,
                    existing.is_some_and(|entry| entry.bypasses_player_limit),
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
                        "Queued /op for {}; pending primary durability.",
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
                    Err(ALREADY_OP_ERROR_TYPE.create_without_context())
                };
            }
            return Ok(changed);
        }

        let new_level = server.basic_config.op_permission_level;
        let mut granted: Vec<(uuid::Uuid, String, pumpkin_util::PermissionLvl)> = Vec::new();
        let mut prepared = Vec::new();
        let mut queue_full = false;

        {
            let mut config = server
                .data
                .operator_config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            for profile in profiles {
                let maybe_existing_entry = config.ops.iter_mut().find(|o| o.uuid == profile.id);

                if let Some(op) = maybe_existing_entry {
                    if op.level == new_level {
                        continue;
                    }

                    let mutation = AdminMutation::GrantOp(OpGrant::new(
                        *profile.id.as_bytes(),
                        profile.name.clone(),
                        new_level as u8,
                        op.bypasses_player_limit,
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
                        op.level = new_level;
                        op.name.clone_from(&profile.name);
                    }
                } else {
                    let mutation = AdminMutation::GrantOp(OpGrant::new(
                        *profile.id.as_bytes(),
                        profile.name.clone(),
                        new_level as u8,
                        false,
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
                    let op_entry = Op::new(profile.id, profile.name.clone(), new_level, false);
                    if !crate::server::cluster_admin_apply::defer_local_admin_apply(server) {
                        config.ops.push(op_entry);
                    }
                }

                granted.push((profile.id, profile.name.clone(), new_level));
            }

            if !granted.is_empty()
                && crate::server::cluster_admin_apply::stores_authoritative_files(server)
                && !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
            {
                config.save();
            }
        }

        for reservation in prepared {
            crate::server::cluster_admin_apply::commit_admin_mutation(server, reservation);
        }

        if granted.is_empty() {
            if queue_full {
                return Err(crate::server::cluster_admin_apply::admin_busy_error());
            }
            return Err(ALREADY_OP_ERROR_TYPE.create_without_context());
        }

        if queue_full {
            context.source.send_feedback(
                TextComponent::text(
                    "Some cluster admin targets could not be queued; they were not changed.",
                ),
                false,
            );
        }

        for (id, name, level) in &granted {
            if !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
                && let Some(player) = server.get_player_by_uuid(*id)
            {
                let command_dispatcher = server.command_dispatcher.load();
                player.set_permission_lvl(server, *level, &command_dispatcher);
            }

            if crate::server::cluster_admin_apply::mutation_pending_primary(server) {
                context.source.send_feedback(
                    TextComponent::text(format!(
                        "Queued /op for {name}; pending primary durability."
                    )),
                    false,
                );
            } else {
                context.source.send_feedback(
                    TextComponent::translate_cross(
                        translation::java::COMMANDS_OP_SUCCESS,
                        translation::bedrock::COMMANDS_OP_SUCCESS,
                        [TextComponent::text(name.clone())],
                    ),
                    true,
                );
            }
        }

        Ok(granted.len() as i32)
    }
}

struct OpSuggestionProvider;

impl SuggestionProvider for OpSuggestionProvider {
    fn suggest(
        &self,
        context: &CommandContext,
        mut builder: SuggestionsBuilder,
    ) -> SuggestionProviderResult {
        for name in
            crate::server::cluster_chat_pm::completion_names_for_viewer(context.server(), "")
        {
            builder = builder.filter_and_suggest_one(name);
        }
        builder.build()
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Three),
    ));

    dispatcher.register(
        command("op", DESCRIPTION).requires(PERMISSION).then(
            argument(ARG_TARGETS, GameProfileArgumentType)
                .suggests(OpSuggestionProvider)
                .executes(OpCommandExecutor),
        ),
    );
}
