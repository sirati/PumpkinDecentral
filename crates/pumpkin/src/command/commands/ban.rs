use pumpkin_cluster::admin_sync::{AdminMutation, BanEntry};
use pumpkin_data::translation;
use pumpkin_util::PermissionLvl;
use pumpkin_util::permission::{Permission, PermissionDefault, PermissionRegistry};
use pumpkin_util::text::TextComponent;

use crate::command::argument_builder::{ArgumentBuilder, argument, command};
use crate::command::argument_types::core::string::StringArgumentType;
use crate::command::argument_types::game_profile::GameProfileArgumentType;
use crate::command::context::command_context::CommandContext;
use crate::command::errors::error_types::CommandErrorType;
use crate::command::node::dispatcher::CommandDispatcher;
use crate::command::node::{CommandExecutor, CommandExecutorResult};
use crate::data::SaveJSONConfiguration;
use crate::data::banlist_serializer::BannedPlayerEntry;
use crate::net::{DisconnectReason, GameProfile};

const DESCRIPTION: &str = "bans a player";
const PERMISSION: &str = "minecraft:command.ban";

const ERROR_BAN_FAILED: CommandErrorType<0> = CommandErrorType::new(
    translation::java::COMMANDS_BAN_FAILED,
    translation::bedrock::COMMANDS_BAN_FAILED,
);

enum BanOutcome {
    Changed,
    Already,
    Busy,
}

fn ban_profile(
    context: &CommandContext,
    profile: &GameProfile,
    reason: Option<String>,
) -> BanOutcome {
    let server = context.source.server();
    if server.advanced_config.cluster.enabled {
        if server
            .cluster_admin_view
            .load()
            .banned_players
            .iter()
            .any(|entry| entry.uuid == *profile.id.as_bytes())
        {
            return BanOutcome::Already;
        }
        let reason = reason.unwrap_or_else(|| "Banned by an operator.".to_string());
        let created_epoch_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|age| age.as_secs() as i64)
            .unwrap_or(0);
        let mutation = AdminMutation::AddBan(BanEntry::new(
            *profile.id.as_bytes(),
            profile.name.clone(),
            context.source.name.clone(),
            reason,
            created_epoch_secs,
            None,
        ));
        let Some(prepared) = crate::server::cluster_admin_apply::prepare_admin_mutation(
            server,
            mutation,
            &context.source.name,
        ) else {
            return BanOutcome::Busy;
        };
        crate::server::cluster_admin_apply::commit_admin_mutation(server, prepared);
        context.source.send_feedback(
            TextComponent::text(format!(
                "Queued /ban for {}; pending primary durability.",
                profile.name
            )),
            false,
        );
        if let Some(player) = server.get_player_by_uuid(profile.id) {
            let kick_msg = TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_BANNED,
                translation::bedrock::DISCONNECTIONSCREEN_TITLE_BANNEDBYHOST,
                [],
            );
            player.kick(DisconnectReason::Kicked, &kick_msg);
        }
        return BanOutcome::Changed;
    }
    let mut banned_players = server.data.banned_player_list.write().unwrap();

    let reason = reason.unwrap_or_else(|| "Banned by an operator.".to_string());

    if let Some(entry) = banned_players
        .banned_players
        .iter_mut()
        .find(|entry| entry.uuid == profile.id)
    {
        if entry.name != profile.name
            && !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
        {
            entry.name.clone_from(&profile.name);
            if crate::server::cluster_admin_apply::stores_authoritative_files(server) {
                banned_players.save();
            }
        }
        return BanOutcome::Already;
    }

    let ban_source = context.source.name.clone();
    let created_epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_secs() as i64)
        .unwrap_or(0);
    let mutation = AdminMutation::AddBan(BanEntry::new(
        *profile.id.as_bytes(),
        profile.name.clone(),
        ban_source.clone(),
        reason.clone(),
        created_epoch_secs,
        None,
    ));
    let Some(reservation) = crate::server::cluster_admin_apply::prepare_admin_mutation(
        server,
        mutation,
        &context.source.name,
    ) else {
        return BanOutcome::Busy;
    };
    if !crate::server::cluster_admin_apply::defer_local_admin_apply(server) {
        banned_players.banned_players.push(BannedPlayerEntry::new(
            profile,
            ban_source.clone(),
            None,
            reason.clone(),
        ));
    }

    if crate::server::cluster_admin_apply::stores_authoritative_files(server)
        && !crate::server::cluster_admin_apply::defer_local_admin_apply(server)
    {
        banned_players.save();
    }
    drop(banned_players);
    crate::server::cluster_admin_apply::commit_admin_mutation(server, reservation);

    if crate::server::cluster_admin_apply::mutation_pending_primary(server) {
        context.source.send_feedback(
            TextComponent::text(format!(
                "Queued /ban for {}; pending primary durability.",
                profile.name
            )),
            false,
        );
    } else {
        context.source.send_feedback(
            TextComponent::translate_cross(
                translation::java::COMMANDS_BAN_SUCCESS,
                translation::bedrock::COMMANDS_BAN_SUCCESS,
                [
                    TextComponent::text(profile.name.clone()),
                    TextComponent::text(reason),
                ],
            ),
            true,
        );
    }

    if let Some(player) = server.get_player_by_uuid(profile.id) {
        let kick_msg = TextComponent::translate_cross(
            translation::java::MULTIPLAYER_DISCONNECT_BANNED,
            translation::bedrock::DISCONNECTIONSCREEN_TITLE_BANNEDBYHOST,
            [],
        );
        player.kick(DisconnectReason::Kicked, &kick_msg);
    }

    BanOutcome::Changed
}

struct BanExecutor {
    has_reason: bool,
}

impl CommandExecutor for BanExecutor {
    fn execute(&self, context: &CommandContext) -> CommandExecutorResult {
        let targets = GameProfileArgumentType::get(context, "targets")?;
        let reason = if self.has_reason {
            Some(StringArgumentType::get(context, "reason")?.to_string())
        } else {
            None
        };

        let mut count: usize = 0;
        let mut busy = false;
        for target in &targets {
            match ban_profile(context, target, reason.clone()) {
                BanOutcome::Changed => count += 1,
                BanOutcome::Already => {}
                BanOutcome::Busy => busy = true,
            }
        }

        if count == 0 {
            if busy {
                return Err(crate::server::cluster_admin_apply::admin_busy_error());
            }
            Err(ERROR_BAN_FAILED.create_without_context())
        } else {
            if busy {
                context.source.send_feedback(
                    TextComponent::text(
                        "Some cluster admin targets could not be queued; they were not changed.",
                    ),
                    false,
                );
            }
            Ok(count as i32)
        }
    }
}

pub fn register(dispatcher: &mut CommandDispatcher, registry: &PermissionRegistry) {
    registry.register_permission_or_panic(Permission::new(
        PERMISSION,
        DESCRIPTION,
        PermissionDefault::Op(PermissionLvl::Three),
    ));

    dispatcher.register(
        command("ban", DESCRIPTION).requires(PERMISSION).then(
            argument("targets", GameProfileArgumentType)
                .executes(BanExecutor { has_reason: false })
                .then(
                    argument("reason", StringArgumentType::GreedyPhrase)
                        .executes(BanExecutor { has_reason: true }),
                ),
        ),
    );
}
