//! Cluster-wide private messages, team messages, and remote player completion.
//!
//! This module is the pumpkin-side hub that wires the [`pumpkin_cluster::chat_sync`]
//! protocol into gameplay: it keeps a directory of player names learned from other
//! cluster hosts, routes `/msg`-style private messages to remote players, fans out
//! `/teammsg`-style team messages, and merges remote names into player tab-completion.

use std::collections::HashSet;

use pumpkin_cluster::chat_sync::{
    ChatDirectorySnapshot, TeamChatBroadcast, canonical_chat_key, resolve_private_by_name,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::presence::{PresenceProperty, sanitize_presence_properties};
use pumpkin_data::world::MSG_COMMAND_OUTGOING;
use pumpkin_protocol::Property;
use pumpkin_util::text::TextComponent;
use tracing::debug;

use super::Server;
use super::cluster_chat_out;
use crate::command::context::command_context::CommandContext;
use crate::entity::EntityBase;
use crate::entity::player::Player;

const UNASSIGNED_SENDER_SLOT: u16 = u16::MAX;

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
pub fn remote_player_properties(server: &Server, gid: &GlobalPlayerId) -> Vec<Property> {
    super::cluster_presence::remote_presence_entry(server, *gid)
        .map(|entry| protocol_properties(&entry.properties))
        .unwrap_or_default()
}

/// Builds a [`ChatDirectorySnapshot`] covering local players plus remembered remotes.
///
/// Local players keep their assigned cluster id when they have one and otherwise get a
/// per-call synthetic slot, which is sufficient because local targets are always
/// delivered by name.
#[must_use]
pub fn directory_snapshot(server: &Server) -> ChatDirectorySnapshot {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let mut snapshot = ChatDirectorySnapshot::new(local);
    for (index, player) in server.get_all_players().iter().enumerate() {
        let slot = u16::try_from(index).unwrap_or(u16::MAX);
        let gid = player
            .cluster_gid()
            .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(slot)));
        snapshot.insert(gid, player.gameprofile.name.clone());
    }
    for (gid, entry) in super::cluster_presence::all_presence_entries(server) {
        if snapshot.player_name(&gid).is_none()
            && snapshot.locate_by_name(entry.name.as_str()).is_none()
        {
            snapshot.insert(gid, entry.name);
        }
    }
    snapshot
}

/// Returns every known player name (local plus remote) matching `prefix`.
///
/// This backs player-name tab-completion so names from other cluster hosts complete the
/// same way local names do.
#[must_use]
pub fn completion_names_for(server: &Server, prefix: &str) -> Vec<String> {
    completion_names_for_viewer(server, prefix)
}

#[must_use]
pub fn completion_names_for_viewer(server: &Server, prefix: &str) -> Vec<String> {
    let hidden: HashSet<String> = hidden_completion_keys(server);
    directory_snapshot(server)
        .completion_names(prefix)
        .into_iter()
        .filter(|name| !hidden.contains(canonical_chat_key(name).as_str()))
        .collect()
}

fn hidden_completion_keys(server: &Server) -> HashSet<String> {
    let mut hidden = HashSet::new();
    for player in server.get_all_players() {
        if super::cluster_hide::is_hidden_player(server, &player) {
            hidden.insert(canonical_chat_key(&player.gameprofile.name));
        }
    }
    for (gid, entry) in super::cluster_presence::all_presence_entries(server) {
        if super::cluster_hide::is_hidden_gid(server, &gid)
            || super::cluster_hide::is_hidden_uuid_bytes(server, &entry.uuid)
        {
            hidden.insert(canonical_chat_key(&entry.name));
        }
    }
    hidden
}

#[must_use]
pub fn is_hidden_completion_name(server: &Server, name: &str) -> bool {
    hidden_completion_keys(server).contains(canonical_chat_key(name).as_str())
}

fn target_is_hidden(server: &Server, gid: &GlobalPlayerId, name: &str) -> bool {
    if super::cluster_hide::is_hidden_gid(server, gid) {
        return true;
    }
    if let Some(player) = server.get_player_by_name(name)
        && super::cluster_hide::is_hidden_player(server, &player)
    {
        return true;
    }
    super::cluster_presence::remote_presence_entries(server)
        .into_iter()
        .any(|(entry_gid, entry)| {
            entry_gid == *gid
                && (super::cluster_hide::is_hidden_gid(server, &entry_gid)
                    || super::cluster_hide::is_hidden_uuid_bytes(server, &entry.uuid))
        })
}

/// Returns remembered remote names that are not also online locally.
///
/// The entity-selector suggestion path already suggests local players, so merging just
/// these extras avoids duplicate entries.
#[must_use]
pub fn extra_completion_names(server: &Server) -> Vec<String> {
    extra_completion_names_for_viewer(server)
}

#[must_use]
pub fn extra_completion_names_for_viewer(server: &Server) -> Vec<String> {
    let local: HashSet<String> = server
        .get_all_players()
        .iter()
        .map(|player| canonical_chat_key(&player.gameprofile.name))
        .collect();
    let hidden = hidden_completion_keys(server);
    directory_snapshot(server)
        .completion_names("")
        .into_iter()
        .filter(|name| {
            !local.contains(canonical_chat_key(name).as_str())
                && !hidden.contains(canonical_chat_key(name).as_str())
        })
        .collect()
}

/// Sends a private message from a `/msg`-style command to a player on any host.
///
/// Returns `Some(1)` when a remote player accepted the message and `None` when the
/// target is not a known remote player or the cluster link is unavailable, in which
/// case the caller should fall back to its regular error handling.
pub fn send_private_from_command(
    context: &CommandContext,
    target_name: &str,
    body: &str,
) -> Option<i32> {
    let server = context.source.server.as_ref()?;
    if server.get_player_by_name(target_name).is_some() {
        return None;
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    let sender = match &context.source.output {
        crate::command::CommandSender::Player(player)
        | crate::command::CommandSender::Lobby(player) => Some(player.as_ref()),
        _ => context.source.player_or_none(),
    };
    let pre_join = context.source.output.as_pre_join();
    let (from_name, from_gid, echo_to) = match sender {
        Some(player) => (
            player.gameprofile.name.clone(),
            player.cluster_gid().unwrap_or(GlobalPlayerId::new(
                local,
                PlayerSlot(UNASSIGNED_SENDER_SLOT),
            )),
            Some(player),
        ),
        None => pre_join.as_ref().map_or_else(
            || {
                (
                    context.source.name.clone(),
                    GlobalPlayerId::new(local, PlayerSlot(0)),
                    None,
                )
            },
            |session| (session.profile.name.clone(), session.gid, None),
        ),
    };
    let snapshot = directory_snapshot(server);
    let delivery = resolve_private_by_name(
        &snapshot,
        from_gid,
        from_name,
        target_name,
        body.to_string(),
    )?;
    if target_is_hidden(server, &delivery.target, &delivery.request.to_name) {
        return None;
    }
    let delivered = if delivery.local {
        let message = TextComponent::text(format!(
            "From {}: {}",
            delivery.request.from_name, delivery.request.body
        ));
        server
            .cluster_lobby_directory
            .route_message(&delivery.request.to_name, &message)
    } else {
        cluster_chat_out::send_private_chat(server, &delivery.request)
    };
    if !delivered {
        debug!(target = target_name, "cluster private chat send failed");
        return None;
    }
    if let Some(player) = echo_to {
        let message = TextComponent::text(body.to_string());
        let target_display = TextComponent::text(delivery.request.to_name.clone());
        player.send_message(
            &message,
            MSG_COMMAND_OUTGOING,
            &player.get_display_name(),
            Some(&target_display),
        );
    } else if pre_join.is_some() {
        context.source.send_feedback(
            TextComponent::text(format!("To {}: {}", delivery.request.to_name, body)),
            false,
        );
    }
    debug!(
        from = delivery.request.from.server.0,
        to = delivery.request.to.server.0,
        "cluster private chat sent"
    );
    Some(1)
}

/// Fans out a team message from a `/teammsg`-style command to the rest of the cluster.
///
/// Returns the number of peer servers the message was queued for.
#[must_use]
pub fn broadcast_team_from_player(
    server: &Server,
    player: &Player,
    team_name: &str,
    body: &str,
) -> u64 {
    if team_name.is_empty() {
        return 0;
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    let sender = player
        .cluster_gid()
        .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(0)));
    let broadcast = TeamChatBroadcast::new(
        sender,
        player.gameprofile.name.clone(),
        team_name.to_string(),
        body.to_string(),
    );
    let sent = cluster_chat_out::broadcast_team_chat(server, &broadcast);
    if sent > 0 {
        debug!(team = team_name, peers = sent, "cluster team chat sent");
    }
    sent
}
