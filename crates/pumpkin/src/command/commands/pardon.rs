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
use crate::data::banned_player::BannedPlayerList;
use crate::data::SaveJSONConfiguration;

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
        let issuer = context.source.name.clone();
        let Ok(mut lock) = server.data.banned_player_list.try_write() else {
            return Err(ERROR_PARDON_FAILED.create_without_context(TextComponent::empty()));
        };
        let mut successes = 0;
        let mut pardoned: Vec<(uuid::Uuid, String)> = Vec::new();

        for target in &targets {
            let idx = lock
                .banned_players
                .iter()
                .position(|entry| entry.uuid == target.id);

            if let Some(idx) = idx {
                lock.banned_players.remove(idx);
                pardoned.push((target.id, target.name.clone()));
                context.source.send_feedback(
                    TextComponent::translate_cross(
                        translation::java::COMMANDS_PARDON_SUCCESS,
                        translation::bedrock::COMMANDS_UNBAN_SUCCESS,
                        [TextComponent::text(target.name.clone())],
                    ),
                    true,
                );
                successes += 1;
            }
        }

        if successes > 0 {
            if crate::server::cluster_admin_apply::persists_admin_state(server) {
                lock.save();
            }
            drop(lock);
            for (id, name) in &pardoned {
                crate::server::cluster_admin_apply::publish_ban_remove(server, *id, name, &issuer);
            }
            Ok(successes)
        } else {
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
    builder: SuggestionsBuilder,
) -> SuggestionProviderResult {
    builder
        .filter_and_suggest_iter(
            banned_players
                .banned_players
                .iter()
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
        // A pardon completion is backed by a very small local list.  Returning an
        // empty result merely because a concurrent ban update holds the write lock
        // makes the command look unreliable to the client, so take the normal read
        // lock and return the current replicated list.
        let Ok(banned_players) = context.server().data.banned_player_list.try_read() else {
            return builder.build();
        };
        suggest_banned_player_names(&banned_players, builder)
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
        command("pardon", DESCRIPTION)
            .requires(PERMISSION)
            .then(
                argument("targets", GameProfileArgumentType)
                    .suggests(PardonSuggestionProvider)
                    .executes(PardonExecutor),
            ),
    );
}
