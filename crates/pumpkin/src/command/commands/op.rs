use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::game_profile::GameProfileArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::command::suggestion::provider::{SuggestionProvider, SuggestionProviderResult};
use crate::command::suggestion::suggestions::SuggestionsBuilder;
use crate::data::SaveJSONConfiguration;
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

        let new_level = server.basic_config.op_permission_level;
        let mut granted: Vec<(uuid::Uuid, String, pumpkin_util::PermissionLvl, bool)> = Vec::new();

        {
            let Ok(mut config) = server.data.operator_config.try_write() else {
                return Err(ALREADY_OP_ERROR_TYPE.create_without_context());
            };

            for profile in profiles {
                let maybe_existing_entry = config.ops.iter_mut().find(|o| o.uuid == profile.id);

                if let Some(op) = maybe_existing_entry {
                    if op.level == new_level {
                        continue;
                    }

                    op.level = new_level;
                    op.name.clone_from(&profile.name);
                } else {
                    let op_entry = Op::new(profile.id, profile.name.clone(), new_level, false);
                    config.ops.push(op_entry);
                }

                granted.push((profile.id, profile.name.clone(), new_level, false));
            }

            if !granted.is_empty()
                && crate::server::cluster_admin_apply::persists_admin_state(server)
            {
                config.save();
            }
        }

        for (id, name, level, bypass) in &granted {
            crate::server::cluster_admin_apply::publish_op_grant(
                server,
                *id,
                name,
                *level,
                *bypass,
                &context.source.name,
            );
        }

        if granted.is_empty() {
            return Err(ALREADY_OP_ERROR_TYPE.create_without_context());
        }

        for (id, name, level, _bypass) in &granted {
            if let Some(player) = server.get_player_by_uuid(*id) {
                let command_dispatcher = server.command_dispatcher.load();
                player.set_permission_lvl(server, *level, &command_dispatcher);
            }

            context.source.send_feedback(
                TextComponent::translate_cross(
                    translation::java::COMMANDS_OP_SUCCESS,
                    translation::bedrock::COMMANDS_OP_SUCCESS,
                    [TextComponent::text(name.clone())],
                ),
                true,
            );
        }

        Ok(granted.len() as i32)
    }
}

struct OpSuggestionProvider;

fn online_completion_names(
    local_names: impl IntoIterator<Item = String>,
    remote_names: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut names: Vec<String> = local_names.into_iter().collect();
    for name in remote_names {
        if name.is_empty() || names.iter().any(|existing| existing.eq_ignore_ascii_case(&name)) {
            continue;
        }
        names.push(name);
    }
    names
}

impl SuggestionProvider for OpSuggestionProvider {
    fn suggest(
        &self,
        context: &CommandContext,
        mut builder: SuggestionsBuilder,
    ) -> SuggestionProviderResult {
        // `/op` is an online-player command.  In particular, do not hide an
        // already-opped player here: doing so makes a player who is the only
        // one online disappear from completion altogether, and the executor
        // remains the authority for returning the normal "already op" error.
        let names = online_completion_names(
            context
                .source
                .server()
                .get_all_players()
                .into_iter()
                .map(|player| player.gameprofile.name.clone()),
            crate::server::cluster_presence::remote_presence_entries()
                .into_iter()
                .map(|(_, entry)| entry.name),
        );
        for name in names {
            // Keep the server suggestion result consistent with all other
            // player-name providers: completions honour the typed prefix.
            builder = builder.filter_and_suggest_one(name);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::online_completion_names;

    #[test]
    fn online_completion_keeps_already_opped_local_player_candidates() {
        // Operator status is intentionally not an input to this function:
        // `/op` must offer every online player, including existing operators.
        assert_eq!(
            online_completion_names(
                [String::from("LocalOperator")],
                [String::from("RemotePlayer")]
            ),
            vec![String::from("LocalOperator"), String::from("RemotePlayer")]
        );
    }

    #[test]
    fn online_completion_deduplicates_remote_presence_case_insensitively() {
        assert_eq!(
            online_completion_names(
                [String::from("LocalPlayer")],
                [String::from("localplayer"), String::new(), String::from("RemotePlayer")]
            ),
            vec![String::from("LocalPlayer"), String::from("RemotePlayer")]
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
        command("op", DESCRIPTION).requires(PERMISSION).then(
            argument(ARG_TARGETS, GameProfileArgumentType)
                .suggests(OpSuggestionProvider)
                .executes(OpCommandExecutor),
        ),
    );
}
