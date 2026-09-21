use std::collections::HashSet;

use pumpkin_cluster::chat_sync::{
    ChatDirectorySnapshot, TeamChatBroadcast, canonical_chat_key, resolve_private_by_name,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::presence::{PresenceProperty, sanitize_presence_properties};
use pumpkin_data::world::MSG_COMMAND_OUTGOING;
use pumpkin_protocol::Property;
use pumpkin_protocol::java::client::play::CSystemChatMessage;
use pumpkin_util::text::TextComponent;
use tracing::debug;

use super::Server;
use super::cluster_chat_out;
use crate::command::context::command_context::CommandContext;
use crate::entity::EntityBase;
use crate::entity::player::Player;

#[must_use]
pub fn protocol_properties(properties: &[PresenceProperty]) -> Vec<Property> {
    sanitize_presence_properties(properties.to_vec())
        .into_iter()
        .map(|property| Property {
            name: property.name.into(),
            value: property.value.into(),
            signature: property.signature.map(Into::into),
        })
        .collect()
}

#[must_use]
pub fn directory_snapshot(server: &Server) -> ChatDirectorySnapshot {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let mut snapshot = ChatDirectorySnapshot::new(local);
    for player in server.get_all_players() {
        if let Some(gid) = player.cluster_gid() {
            snapshot.insert(gid, player.gameprofile.name.clone());
        }
    }
    for waiter in server.lobby_waiters.load().iter() {
        snapshot.insert(waiter.gid, waiter.profile.name.clone());
    }
    for (gid, entry) in super::cluster_presence::remote_presence_entries() {
        if snapshot.player_name(&gid).is_none()
            && snapshot.locate_by_name(entry.name.as_str()).is_none()
        {
            snapshot.insert(gid, entry.name);
        }
    }
    snapshot
}

#[must_use]
pub fn completion_names_for(server: &Server, prefix: &str) -> Vec<String> {
    completion_names_for_viewer(server, prefix, false)
}

#[must_use]
pub fn completion_names_for_viewer(
    server: &Server,
    prefix: &str,
    can_see_hidden: bool,
) -> Vec<String> {
    if can_see_hidden {
        return directory_snapshot(server).completion_names(prefix);
    }
    let hidden = hidden_completion_keys(server);
    directory_snapshot(server)
        .completion_names(prefix)
        .into_iter()
        .filter(|name| !hidden.contains(canonical_chat_key(name).as_str()))
        .collect()
}

fn hidden_completion_keys(server: &Server) -> HashSet<String> {
    let mut hidden = HashSet::new();
    for player in server.get_all_players() {
        if super::cluster_hide::is_hidden_player(&player) {
            hidden.insert(canonical_chat_key(&player.gameprofile.name));
        }
    }
    for (gid, entry) in super::cluster_presence::remote_presence_entries() {
        if super::cluster_hide::is_hidden_gid(&gid)
            || super::cluster_hide::is_hidden_uuid_bytes(&entry.uuid)
        {
            hidden.insert(canonical_chat_key(&entry.name));
        }
    }
    hidden
}

fn target_is_hidden(server: &Server, gid: &GlobalPlayerId, name: &str) -> bool {
    if super::cluster_hide::is_hidden_gid(gid) {
        return true;
    }
    if let Some(player) = server.get_player_by_name(name)
        && super::cluster_hide::is_hidden_player(&player)
    {
        return true;
    }
    super::cluster_presence::remote_presence_entries()
        .into_iter()
        .any(|(entry_gid, entry)| {
            entry_gid == *gid
                && (super::cluster_hide::is_hidden_gid(&entry_gid)
                    || super::cluster_hide::is_hidden_uuid_bytes(&entry.uuid))
        })
}

#[must_use]
pub fn extra_completion_names(server: &Server) -> Vec<String> {
    extra_completion_names_for_viewer(server, false)
}

#[must_use]
pub fn extra_completion_names_for_viewer(server: &Server, can_see_hidden: bool) -> Vec<String> {
    let local: HashSet<String> = server
        .get_all_players()
        .iter()
        .map(|player| canonical_chat_key(&player.gameprofile.name))
        .collect();
    let hidden = (!can_see_hidden).then(|| hidden_completion_keys(server));
    directory_snapshot(server)
        .completion_names("")
        .into_iter()
        .filter(|name| {
            !local.contains(canonical_chat_key(name).as_str())
                && hidden
                    .as_ref()
                    .is_none_or(|keys| !keys.contains(canonical_chat_key(name).as_str()))
        })
        .collect()
}

pub fn send_private_from_command(
    context: &CommandContext,
    target_name: &str,
    body: &str,
) -> Option<i32> {
    let server = context.source.server.as_ref()?;
    if server.get_player_by_name(target_name).is_some() {
        return None;
    }
    if let Some(waiter) = server
        .lobby_waiters
        .load()
        .iter()
        .find(|waiter| waiter.profile.name.eq_ignore_ascii_case(target_name))
    {
        if let Some(client) = waiter.client.java() {
            let message = TextComponent::text(format!(
                "[{} -> you] {}",
                context.source.name, body
            ));
            client.try_send_packet(&CSystemChatMessage::new(&message, false));
            context.source.send_message(TextComponent::text(format!(
                "[you -> {}] {}",
                waiter.profile.name, body
            )));
            return Some(1);
        }
    }
    let (from_name, from_gid) = match &context.source.output {
        crate::command::CommandSender::Player(player) => {
            (player.gameprofile.name.clone(), player.cluster_gid()?)
        }
        crate::command::CommandSender::Lobby(waiter) => {
            (waiter.profile.name.clone(), waiter.gid)
        }
        _ => return None,
    };
    let snapshot = directory_snapshot(server);
    let delivery =
        resolve_private_by_name(&snapshot, from_gid, from_name, target_name, body.to_string())?;
    if delivery.local
        || target_is_hidden(server, &delivery.target, &delivery.request.to_name)
            && !context
                .source
                .has_permission(super::cluster_hide::HIDE_PERMISSION)
    {
        return None;
    }
    if !cluster_chat_out::send_private_chat(&delivery.request) {
        debug!(target = target_name, "cluster private chat send failed");
        return None;
    }
    match &context.source.output {
        crate::command::CommandSender::Player(player) => {
            let message = TextComponent::text(body.to_string());
            let target_display = TextComponent::text(delivery.request.to_name.clone());
            player.send_message(
                &message,
                MSG_COMMAND_OUTGOING,
                &player.get_display_name(),
                Some(&target_display),
            );
        }
        crate::command::CommandSender::Lobby(waiter) => waiter.send_system_message(
            &TextComponent::text(format!(
                "[you -> {}] {}",
                delivery.request.to_name, body
            )),
        ),
        _ => {}
    }
    Some(1)
}

#[must_use]
pub fn broadcast_team_from_player(
    _server: &Server,
    player: &Player,
    team_name: &str,
    body: &str,
) -> u64 {
    if team_name.is_empty() {
        return 0;
    }
    let Some(sender) = player.cluster_gid() else {
        return 0;
    };
    let broadcast = TeamChatBroadcast::new(
        sender,
        player.gameprofile.name.clone(),
        team_name.to_string(),
        body.to_string(),
    );
    cluster_chat_out::broadcast_team_chat(&broadcast)
}
