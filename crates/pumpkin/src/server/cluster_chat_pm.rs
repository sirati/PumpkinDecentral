//! Cluster-wide private messages, team messages, and remote player completion.
//!
//! This module is the pumpkin-side hub that wires the [`pumpkin_cluster::chat_sync`]
//! protocol into gameplay: it keeps a directory of player names learned from other
//! cluster hosts, routes `/msg`-style private messages to remote players, fans out
//! `/teammsg`-style team messages, and merges remote names into player tab-completion.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};

use pumpkin_cluster::chat_sync::{
    ChatDirectorySnapshot, TeamChatBroadcast, canonical_chat_key, is_valid_chat_name,
    resolve_private_by_name,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::presence::{PresenceProperty, sanitize_presence_properties};
use pumpkin_protocol::Property;
use pumpkin_data::world::MSG_COMMAND_OUTGOING;
use pumpkin_util::text::TextComponent;
use tracing::debug;

use super::Server;
use super::cluster_chat_out;
use crate::command::context::command_context::CommandContext;
use crate::entity::EntityBase;
use crate::entity::player::Player;

/// Upper bound for remembered remote players.
///
/// The directory is fed by inbound cluster chat traffic, so it is capped to keep a
/// misbehaving peer from growing it without bound.
const MAX_REMOTE_PLAYERS: usize = 1024;

struct RemoteIdentity {
    name: String,
    properties: Vec<Property>,
}

/// Player names learned from other cluster hosts, keyed by global player id.
static REMOTE_PLAYERS: LazyLock<RwLock<HashMap<GlobalPlayerId, RemoteIdentity>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Sender slot used when the local sender has no cluster id assigned yet.
const UNASSIGNED_SENDER_SLOT: u16 = u16::MAX;

/// Remembers a player hosted on another cluster server.
///
/// Names seen on inbound public, private, and team chat keep this directory fresh so
/// `/msg <name>` and tab-completion also cover players who are online elsewhere.
pub fn note_remote_player(gid: GlobalPlayerId, name: &str) {
    if !is_valid_chat_name(name) {
        return;
    }
    let mut remotes = REMOTE_PLAYERS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(identity) = remotes.get_mut(&gid) {
        identity.name = name.to_string();
        return;
    }
    if remotes.len() >= MAX_REMOTE_PLAYERS {
        if let Some(evicted) = remotes.keys().next().copied() {
            remotes.remove(&evicted);
        }
    }
    remotes.insert(
        gid,
        RemoteIdentity {
            name: name.to_string(),
            properties: Vec::new(),
        },
    );
}

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

pub fn note_remote_roster(
    gid: GlobalPlayerId,
    name: &str,
    properties: &[PresenceProperty],
) {
    if !is_valid_chat_name(name) {
        return;
    }
    let identity = RemoteIdentity {
        name: name.to_string(),
        properties: protocol_properties(properties),
    };
    let mut remotes = REMOTE_PLAYERS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if remotes.contains_key(&gid) {
        remotes.insert(gid, identity);
        return;
    }
    if remotes.len() >= MAX_REMOTE_PLAYERS {
        if let Some(evicted) = remotes.keys().next().copied() {
            remotes.remove(&evicted);
        }
    }
    remotes.insert(gid, identity);
}

#[must_use]
pub fn remote_player_properties(gid: &GlobalPlayerId) -> Vec<Property> {
    REMOTE_PLAYERS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(gid)
        .map(|identity| identity.properties.clone())
        .unwrap_or_default()
}

/// Forgets a remembered remote player. Returns `true` if an entry was removed.
#[must_use]
pub fn forget_remote_player(gid: &GlobalPlayerId) -> bool {
    let mut remotes = REMOTE_PLAYERS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    remotes.remove(gid).is_some()
}

/// Returns every remembered remote player name, sorted and deduplicated.
#[must_use]
pub fn remote_player_names() -> Vec<String> {
    let remotes = REMOTE_PLAYERS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut names: Vec<String> = remotes.values().map(|identity| identity.name.clone()).collect();
    names.sort();
    names.dedup();
    names
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
    let remotes = REMOTE_PLAYERS
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (gid, identity) in remotes.iter() {
        if snapshot.player_name(gid).is_none()
            && snapshot.locate_by_name(identity.name.as_str()).is_none()
        {
            snapshot.insert(*gid, identity.name.clone());
        }
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

/// Returns every known player name (local plus remote) matching `prefix`.
///
/// This backs player-name tab-completion so names from other cluster hosts complete the
/// same way local names do.
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

/// Returns remembered remote names that are not also online locally.
///
/// The entity-selector suggestion path already suggests local players, so merging just
/// these extras avoids duplicate entries.
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
    if can_see_hidden {
        return directory_snapshot(server)
            .completion_names("")
            .into_iter()
            .filter(|name| !local.contains(canonical_chat_key(name).as_str()))
            .collect();
    }
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
        crate::command::CommandSender::Player(player) => Some(player.as_ref()),
        _ => context.source.player_or_none(),
    };
    let (from_name, from_gid, echo_to) = match sender {
        Some(player) => (
            player.gameprofile.name.clone(),
            player.cluster_gid().unwrap_or(GlobalPlayerId::new(
                local,
                PlayerSlot(UNASSIGNED_SENDER_SLOT),
            )),
            Some(player),
        ),
        None => (
            context.source.name.clone(),
            GlobalPlayerId::new(local, PlayerSlot(0)),
            None,
        ),
    };
    let snapshot = directory_snapshot(server);
    let delivery =
        resolve_private_by_name(&snapshot, from_gid, from_name, target_name, body.to_string())?;
    if delivery.local {
        return None;
    }
    if target_is_hidden(server, &delivery.target, &delivery.request.to_name)
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
    if let Some(player) = echo_to {
        let message = TextComponent::text(body.to_string());
        let target_display = TextComponent::text(delivery.request.to_name.clone());
        player.send_message(
            &message,
            MSG_COMMAND_OUTGOING,
            &player.get_display_name(),
            Some(&target_display),
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
    let sent = cluster_chat_out::broadcast_team_chat(&broadcast);
    if sent > 0 {
        debug!(team = team_name, peers = sent, "cluster team chat sent");
    }
    sent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_and_forgets_remote_players() {
        let gid = GlobalPlayerId::new(ServerId(701), PlayerSlot(7));
        note_remote_player(gid, "PmtAlice");
        assert!(remote_player_names().contains(&String::from("PmtAlice")));
        assert!(forget_remote_player(&gid));
        assert!(!forget_remote_player(&gid));
        assert!(!remote_player_names().contains(&String::from("PmtAlice")));
    }

    #[test]
    fn rejects_invalid_remote_names() {
        let gid = GlobalPlayerId::new(ServerId(702), PlayerSlot(8));
        note_remote_player(gid, "");
        assert!(!forget_remote_player(&gid));
        note_remote_player(gid, "this-name-is-way-too-long");
        assert!(!forget_remote_player(&gid));
    }

    #[test]
    fn roster_notes_carry_skin_properties() {
        let gid = GlobalPlayerId::new(ServerId(704), PlayerSlot(10));
        note_remote_player(gid, "PmtCara");
        assert!(remote_player_properties(&gid).is_empty());
        let wire = vec![pumpkin_cluster::presence::PresenceProperty::new(
            String::from("textures"),
            String::from("dGV4dHVyZXM="),
            Some(String::from("c2lnbmF0dXJl")),
        )];
        note_remote_roster(gid, "PmtCara", &wire);
        let stored = remote_player_properties(&gid);
        assert_eq!(stored.len(), 1);
        assert_eq!(&*stored[0].name, "textures");
        note_remote_player(gid, "PmtCara");
        assert_eq!(remote_player_properties(&gid).len(), 1);
        assert!(forget_remote_player(&gid));
        assert!(remote_player_properties(&gid).is_empty());
    }

    #[test]
    fn refreshes_known_remote_names() {
        let gid = GlobalPlayerId::new(ServerId(703), PlayerSlot(9));
        note_remote_player(gid, "PmtBob");
        note_remote_player(gid, "PmtBobby");
        assert!(remote_player_names().contains(&String::from("PmtBobby")));
        assert!(forget_remote_player(&gid));
        assert!(!remote_player_names().contains(&String::from("PmtBobby")));
    }
}
