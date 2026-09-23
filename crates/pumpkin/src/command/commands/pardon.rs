use pumpkin_cluster::admin_sync::{AdminMutation, BanRevoke};
use pumpkin_data::translation;
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
use crate::data::banned_player::BannedPlayerList;

const DESCRIPTION: &str = "unbans a player";
const PERMISSION: &str = "minecraft:command.pardon";

const ERROR_PARDON_FAILED: CommandErrorType<1> = CommandErrorType::new(
    translation::java::COMMANDS_PARDON_FAILED,
    translation::bedrock::COMMANDS_UNBAN_FAILED,
);

struct PardonExecutor;

impl CommandExecutor for PardonExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = GameProfileArgumentType::get(context, "targets")?;
        let server = context.source.server();
        if server.advanced_config.cluster.enabled {
            let snapshot = server.cluster_admin_view.load_full();
            let mut changed = 0;
            let mut busy = false;
            for target in &targets {
                if !snapshot
                    .banned_players
                    .iter()
                    .any(|entry| entry.uuid == *target.id.as_bytes())
                {
                    continue;
                }
                let mutation = AdminMutation::RemoveBan(BanRevoke::new(
                    *target.id.as_bytes(),
                    target.name.clone(),
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
                        "Queued /pardon for {}; pending primary durability.",
                        target.name
                    )),
                    false,
                );
                changed += 1;
            }
            if changed == 0 {
                return if busy {
                    Err(crate::server::cluster_admin_apply::admin_busy_error())
                } else {
                    Err(
                        ERROR_PARDON_FAILED.create_without_context(TextComponent::text(
                            targets
                                .first()
                                .map_or_else(String::new, |target| target.name.clone()),
                        )),
                    )
                };
            }
            return Ok(changed);
        }
        let issuer = context.source.name.clone();
        let mut lock = server.data.banned_player_list.write().unwrap();
        let mut successes = 0;
        let mut prepared = Vec::new();
        let mut pardoned = Vec::new();
        let mut queue_full = false;

        for target in &targets {
            let idx = lock
                .banned_players
                .iter()
                .position(|entry| entry.uuid == target.id);

            if let Some(idx) = idx {
                let mutation = AdminMutation::RemoveBan(BanRevoke::new(
                    *target.id.as_bytes(),
                    target.name.clone(),
                ));
                let Some(reservation) = crate::server::cluster_admin_apply::prepare_admin_mutation(
                    server, mutation, &issuer,
                ) else {
                    queue_full = true;
                    continue;
                };
                prepared.push(reservation);
                if !crate::server::cluster_admin_apply::defer_local_admin_apply(server) {
                    lock.banned_players.remove(idx);
                }
                pardoned.push(target.name.clone());
                successes += 1;
            }
        }

        if successes > 0 {
            if crate::server::cluster_admin_apply::stores_authoritative_files(server)
                && !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
            {
                lock.save();
            }
            drop(lock);
            for reservation in prepared {
                crate::server::cluster_admin_apply::commit_admin_mutation(server, reservation);
            }
            for name in pardoned {
                if crate::server::cluster_admin_apply::mutation_pending_primary(server) {
                    context.source.send_feedback(
                        TextComponent::text(format!(
                            "Queued /pardon for {name}; pending primary durability."
                        )),
                        false,
                    );
                } else {
                    context.source.send_feedback(
                        TextComponent::translate_cross(
                            translation::java::COMMANDS_PARDON_SUCCESS,
                            translation::bedrock::COMMANDS_UNBAN_SUCCESS,
                            [TextComponent::text(name)],
                        ),
                        true,
                    );
                }
            }
            if queue_full {
                context.source.send_feedback(
                    TextComponent::text(
                        "Some cluster admin targets could not be queued; they were not changed.",
                    ),
                    false,
                );
            }
            Ok(successes)
        } else {
            if queue_full {
                return Err(crate::server::cluster_admin_apply::admin_busy_error());
            }
            let err_target = targets
                .first()
                .map_or_else(String::new, |first_target| first_target.name.clone());
            Err(ERROR_PARDON_FAILED.create_without_context(TextComponent::text(err_target)))
        }
    }
}

struct PardonSuggestionProvider;

fn suggest_banned_player_names(
    banned_players: &BannedPlayerList,
    hidden: impl Fn(&str) -> bool,
    builder: SuggestionsBuilder,
) -> SuggestionProviderResult {
    builder
        .filter_and_suggest_iter(
            banned_players
                .banned_players
                .iter()
                .filter(|entry| !hidden(&entry.name))
                .map(|entry| entry.name.clone()),
        )
        .build()
}

impl SuggestionProvider for PardonSuggestionProvider {
    fn suggest(
        &self,
        context: &CommandContext,
        builder: SuggestionsBuilder,
    ) -> SuggestionProviderResult {
        if context.server().advanced_config.cluster.enabled {
            return builder
                .filter_and_suggest_iter(
                    context
                        .server()
                        .cluster_admin_view
                        .load()
                        .banned_players
                        .iter()
                        .filter(|entry| {
                            !crate::server::cluster_chat_pm::is_hidden_completion_name(
                                context.server(),
                                &entry.name,
                            )
                        })
                        .map(|entry| entry.name.clone()),
                )
                .build();
        }
        let banned_players = context
            .server()
            .data
            .banned_player_list
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        suggest_banned_player_names(
            &banned_players,
            |name| {
                crate::server::cluster_chat_pm::is_hidden_completion_name(context.server(), name)
            },
            builder,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::banlist_serializer::BannedPlayerEntry;
    use pumpkin_command::suggestion::suggestions::SuggestionsBuilder;

    #[test]
    fn pardon_completes_replicated_ban_names() {
        let mut banned = BannedPlayerList::default();
        banned.banned_players.push(BannedPlayerEntry {
            uuid: uuid::Uuid::from_u128(1),
            name: String::from("RemoteBannedPlayer"),
            created: time::OffsetDateTime::now_utc(),
            source: String::from("RemoteOp"),
            expires: None,
            reason: String::from("test"),
        });

        let suggestions = suggest_banned_player_names(
            &banned,
            |_| false,
            SuggestionsBuilder::new("/pardon remote", 8),
        );
        assert_eq!(suggestions.suggestions.len(), 1);
        assert_eq!(
            suggestions.suggestions[0].text.cached_text(),
            "RemoteBannedPlayer"
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
        command("pardon", DESCRIPTION).requires(PERMISSION).then(
            argument("targets", GameProfileArgumentType)
                .suggests(PardonSuggestionProvider)
                .executes(PardonExecutor),
        ),
    );
}
