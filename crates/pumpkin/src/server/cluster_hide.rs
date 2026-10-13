use std::sync::Arc;

use pumpkin_cluster::hide::{HideUpdate, decode_hide, is_valid_hide_name};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::translation;
use pumpkin_protocol::bedrock::client::common::BuildPlatform;
use pumpkin_protocol::bedrock::client::player_list::{CPlayerList, PlayerListEntry, Skin};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::codec::var_long::VarLong;
use pumpkin_protocol::java::client::play::{
    CPlayerInfoUpdate, CRemovePlayerInfo, PlayerAction, PlayerInfoFlags,
};
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;
use tokio::sync::mpsc;
use tracing::debug;

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;

pub const HIDE_PERMISSION: &str = "minecraft:command.hide";

#[must_use]
pub fn hide_announced(server: &Server) -> u64 {
    server.cluster_presence.snapshot().announced
}

#[must_use]
pub fn is_hidden_gid(server: &Server, gid: &GlobalPlayerId) -> bool {
    server.cluster_presence.snapshot().hidden_gids.contains(gid)
}

#[must_use]
pub fn is_hidden_uuid_bytes(server: &Server, uuid: &[u8; 16]) -> bool {
    server
        .cluster_presence
        .snapshot()
        .hidden_uuids
        .contains(uuid)
}

#[must_use]
pub fn is_hidden_uuid(server: &Server, uuid: &uuid::Uuid) -> bool {
    is_hidden_uuid_bytes(server, uuid.as_bytes())
}

#[must_use]
pub fn is_hidden_player(server: &Server, player: &Player) -> bool {
    if is_hidden_uuid(server, &player.gameprofile.id) {
        return true;
    }
    match player.cluster_gid() {
        Some(gid) => is_hidden_gid(server, &gid),
        None => false,
    }
}

#[must_use]
pub fn hidden_snapshot(server: &Server) -> std::collections::HashSet<GlobalPlayerId> {
    server.cluster_presence.snapshot().hidden_gids.clone()
}

pub fn publish_hide_announce(
    server: &Server,
    gid: GlobalPlayerId,
    uuid: [u8; 16],
    name: &str,
    hidden: bool,
    session: u64,
) -> bool {
    if !is_valid_hide_name(name) {
        return false;
    }
    let update = HideUpdate::new(gid, uuid, name.to_string(), hidden);
    server
        .cluster_presence
        .try_submit(super::cluster_presence::PresenceCommand::LocalHide(
            update, session,
        ))
}

#[must_use]
pub fn hide_gid_for(server: &Server, player: &Player) -> GlobalPlayerId {
    if let Some(gid) = player.cluster_gid() {
        return gid;
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    let _ = super::cluster_presence::assign_login_gid(server, player);
    player
        .cluster_gid()
        .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(u16::MAX)))
}

fn fake_hide_message(name: &str, hidden: bool) -> TextComponent {
    if hidden {
        TextComponent::translate_cross(
            translation::java::MULTIPLAYER_PLAYER_LEFT,
            translation::bedrock::MULTIPLAYER_PLAYER_LEFT,
            [TextComponent::text(name.to_string())],
        )
        .color_named(NamedColor::Yellow)
    } else {
        TextComponent::translate_cross(
            translation::java::MULTIPLAYER_PLAYER_JOINED,
            translation::bedrock::MULTIPLAYER_PLAYER_JOINED,
            [TextComponent::text(name.to_string())],
        )
        .color_named(NamedColor::Yellow)
    }
}

fn broadcast_fake_hide(server: &Server, name: &str, hidden: bool, except: Option<uuid::Uuid>) {
    let message = fake_hide_message(name, hidden);
    for viewer in server.get_all_players() {
        if Some(viewer.gameprofile.id) == except {
            continue;
        }
        viewer.send_system_message(&message);
    }
    if server.advanced_config.cluster.enabled {
        let local = ServerId(server.advanced_config.cluster.server_id);
        for profile in server.cluster_presence.snapshot().entries.values() {
            if profile.gid.server == local
                && profile.in_lobby
                && except != Some(uuid::Uuid::from_bytes(profile.uuid))
            {
                let _ = server
                    .cluster_lobby_chat_tx
                    .send((Some(profile.name.clone()), message.clone()));
            }
        }
    }
}

fn remove_remote_from_viewer(viewer: &Player, uuid: uuid::Uuid, name: &str) {
    viewer.client.try_enqueue_packet_editioned(
        &CRemovePlayerInfo::new(&[uuid]),
        &CPlayerList {
            action: CPlayerList::ACTION_REMOVE,
            entries: vec![PlayerListEntry {
                uuid,
                entity_unique_id: VarLong(0),
                username: name.to_string(),
                xuid: String::new(),
                platform_chat_id: String::new(),
                build_platform: BuildPlatform::Unknown,
                skin: Skin::steve(),
                is_teacher: false,
                is_host: false,
                is_sub_client: false,
                player_color: [0, 0, 0, 0],
            }],
        },
    );
}

fn add_remote_to_viewer(
    server: &Server,
    viewer: &Player,
    uuid: uuid::Uuid,
    name: &str,
    properties: &[pumpkin_protocol::Property],
) {
    let skin = if matches!(
        viewer.client.as_ref(),
        crate::net::ClientPlatform::Bedrock(_)
    ) {
        let Some(skin) = super::cluster_profile_skins::get(server, uuid) else {
            return;
        };
        (*skin).clone()
    } else {
        Skin::steve()
    };
    let listed = server
        .cluster_presence
        .snapshot()
        .entries
        .values()
        .find(|entry| entry.uuid == *uuid.as_bytes())
        .is_some_and(|entry| entry.listed);
    let actions = [
        PlayerAction::AddPlayer { name, properties },
        PlayerAction::UpdateListed(listed),
        PlayerAction::UpdateLatency(VarInt(0)),
    ];
    let entry = [pumpkin_protocol::java::client::play::Player {
        uuid,
        actions: &actions,
    }];
    viewer.client.try_enqueue_packet_editioned(
        &CPlayerInfoUpdate::new(
            (PlayerInfoFlags::ADD_PLAYER
                | PlayerInfoFlags::UPDATE_LISTED
                | PlayerInfoFlags::UPDATE_LATENCY)
                .bits(),
            &entry,
        ),
        &CPlayerList {
            action: CPlayerList::ACTION_ADD,
            entries: vec![PlayerListEntry {
                uuid,
                entity_unique_id: VarLong(0),
                username: name.to_string(),
                xuid: String::new(),
                platform_chat_id: String::new(),
                build_platform: BuildPlatform::Unknown,
                skin,
                is_teacher: false,
                is_host: false,
                is_sub_client: false,
                player_color: [0, 0, 0, 0],
            }],
        },
    );
}

pub fn push_remote_login_to_viewers(
    server: &Server,
    gid: &GlobalPlayerId,
    uuid_bytes: &[u8; 16],
    name: &str,
    properties: &[pumpkin_protocol::Property],
) {
    if server.advanced_config.cluster.enabled { return; }
    if !is_valid_hide_name(name) {
        return;
    }
    let hidden = is_hidden_gid(server, gid) || is_hidden_uuid_bytes(server, uuid_bytes);
    let uuid = uuid::Uuid::from_bytes(*uuid_bytes);
    for viewer in server.get_all_players() {
        if hidden {
            continue;
        }
        add_remote_to_viewer(server, &viewer, uuid, name, properties);
    }
}

pub fn push_remote_logout_to_viewers(
    server: &Server,
    gid: &GlobalPlayerId,
    uuid_bytes: &[u8; 16],
    name: &str,
) {
    if server.advanced_config.cluster.enabled { return; }
    if !is_valid_hide_name(name) {
        return;
    }
    let hidden = is_hidden_gid(server, gid) || is_hidden_uuid_bytes(server, uuid_bytes);
    let uuid = uuid::Uuid::from_bytes(*uuid_bytes);
    for viewer in server.get_all_players() {
        if hidden {
            continue;
        }
        remove_remote_from_viewer(&viewer, uuid, name);
    }
}

fn hide_name_for(server: &Server, uuid: uuid::Uuid, fallback: Option<&str>) -> Option<String> {
    if let Some(entry) = super::cluster_presence::all_presence_entries(server)
        .into_iter()
        .find(|(_, entry)| entry.uuid == *uuid.as_bytes())
    {
        return Some(entry.1.name);
    }
    fallback
        .filter(|name| is_valid_hide_name(name))
        .map(str::to_string)
}

fn remove_hidden_from_viewer(viewer: &Player, uuid: uuid::Uuid, entity_id: i32, name: &str) {
    viewer.client.try_enqueue_packet_editioned(
        &CRemovePlayerInfo::new(&[uuid]),
        &CPlayerList {
            action: CPlayerList::ACTION_REMOVE,
            entries: vec![PlayerListEntry {
                uuid,
                entity_unique_id: VarLong(i64::from(entity_id)),
                username: name.to_string(),
                xuid: String::new(),
                platform_chat_id: String::new(),
                build_platform: BuildPlatform::Unknown,
                skin: Skin::steve(),
                is_teacher: false,
                is_host: false,
                is_sub_client: false,
                player_color: [0, 0, 0, 0],
            }],
        },
    );
}

fn add_hidden_to_viewer(viewer: &Player, hidden: &Player) {
    let gameprofile = &hidden.gameprofile;
    let entity_id = hidden.entity_id();
    let gamemode = hidden.gamemode.load();
    let properties = gameprofile.properties.load();
    let actions = [
        PlayerAction::AddPlayer {
            name: &gameprofile.name,
            properties: &properties,
        },
        PlayerAction::UpdateGameMode(VarInt(gamemode as i32)),
        PlayerAction::UpdateListed(true),
        PlayerAction::UpdateLatency(VarInt(0)),
        PlayerAction::UpdateListOrder(VarInt(0)),
        PlayerAction::UpdateHat(true),
    ];
    let entry = [pumpkin_protocol::java::client::play::Player {
        uuid: gameprofile.id,
        actions: &actions,
    }];
    viewer.client.try_enqueue_packet_editioned(
        &CPlayerInfoUpdate::new(
            (PlayerInfoFlags::ADD_PLAYER
                | PlayerInfoFlags::UPDATE_GAME_MODE
                | PlayerInfoFlags::UPDATE_LISTED
                | PlayerInfoFlags::UPDATE_LATENCY
                | PlayerInfoFlags::UPDATE_LIST_PRIORITY
                | PlayerInfoFlags::UPDATE_HAT)
                .bits(),
            &entry,
        ),
        &CPlayerList {
            action: CPlayerList::ACTION_ADD,
            entries: vec![PlayerListEntry {
                uuid: gameprofile.id,
                entity_unique_id: VarLong(i64::from(entity_id)),
                username: gameprofile.name.clone(),
                xuid: String::new(),
                platform_chat_id: String::new(),
                build_platform: BuildPlatform::Unknown,
                skin: (**hidden.bedrock_skin.load()).clone(),
                is_teacher: false,
                is_host: false,
                is_sub_client: false,
                player_color: [0, 0, 0, 0],
            }],
        },
    );
}

fn remove_entity_pairing(server: &Server, viewer: &Player, entity_id: i32) {
    for world in server.worlds.load().iter() {
        if let Some(tracked) = world.entity_tracker.get_tracked_entity(entity_id) {
            tracked.remove_player(viewer);
            return;
        }
    }
}

fn restore_entity_pairing(server: &Server, viewer: &Arc<Player>, entity_id: i32) {
    for world in server.worlds.load().iter() {
        if let Some(tracked) = world.entity_tracker.get_tracked_entity(entity_id) {
            tracked.update_player(viewer, world);
            return;
        }
    }
}

pub fn apply_hide_visibility(
    server: &Server,
    hidden_uuid: uuid::Uuid,
    fallback_name: Option<&str>,
    hidden: bool,
) {
    let viewers = server.get_all_players();
    if server.advanced_config.cluster.enabled {
        let snapshot = server.cluster_presence.snapshot();
        let Some(name) = snapshot.by_uuid(hidden_uuid.as_bytes()).map(|entry| entry.name.as_str()).or(fallback_name) else { super::cluster_status::refresh_cluster_status(server); return; };
        broadcast_fake_hide(server, name, hidden, Some(hidden_uuid));
        super::cluster_status::refresh_cluster_status(server);
        return;
    }
    if hidden {
        let local = viewers
            .iter()
            .find(|player| player.gameprofile.id == hidden_uuid)
            .map(|player| (player.entity_id(), player.gameprofile.name.clone()));
        let (entity_id, name) = match local {
            Some(found) => (Some(found.0), found.1),
            None => {
                let known = super::cluster_presence::all_presence_entries(server)
                    .into_iter()
                    .find(|(_, entry)| entry.uuid == *hidden_uuid.as_bytes());
                let Some((gid, entry)) = known else {
                    super::cluster_status::refresh_cluster_status(server);
                    return;
                };
                let entity_id = super::cluster_ghost::remote_player_replica(server, gid)
                    .map(|replica| replica.get_entity().entity_id);
                (entity_id, entry.name)
            }
        };
        for viewer in &viewers {
            if viewer.gameprofile.id == hidden_uuid {
                continue;
            }
            match entity_id {
                Some(entity_id) => {
                    remove_hidden_from_viewer(viewer, hidden_uuid, entity_id, &name);
                    remove_entity_pairing(server, viewer, entity_id);
                }
                _ => remove_remote_from_viewer(viewer, hidden_uuid, &name),
            }
        }
        broadcast_fake_hide(server, &name, true, Some(hidden_uuid));
    } else {
        let target = viewers
            .iter()
            .find(|player| player.gameprofile.id == hidden_uuid)
            .cloned();
        if let Some(target) = target {
            let name = target.gameprofile.name.clone();
            for viewer in &viewers {
                if viewer.gameprofile.id == hidden_uuid {
                    continue;
                }
                add_hidden_to_viewer(viewer, &target);
                restore_entity_pairing(server, viewer, target.entity_id());
            }
            broadcast_fake_hide(server, &name, false, Some(hidden_uuid));
        } else {
            let Some(name) = hide_name_for(server, hidden_uuid, fallback_name) else {
                super::cluster_status::refresh_cluster_status(server);
                return;
            };
            let remote = super::cluster_presence::all_presence_entries(server)
                .into_iter()
                .find(|(_, entry)| entry.uuid == *hidden_uuid.as_bytes());
            let entity_id = remote
                .as_ref()
                .and_then(|(gid, _)| super::cluster_ghost::remote_player_replica(server, *gid))
                .map(|replica| replica.get_entity().entity_id);
            for viewer in &viewers {
                if viewer.gameprofile.id == hidden_uuid {
                    continue;
                }
                if remote.is_some() {
                    let properties = remote
                        .as_ref()
                        .map(|(gid, _)| {
                            super::cluster_chat_pm::remote_player_properties(server, gid)
                        })
                        .unwrap_or_default();
                    add_remote_to_viewer(server, viewer, hidden_uuid, &name, &properties);
                }
                if let Some(entity_id) = entity_id {
                    restore_entity_pairing(server, viewer, entity_id);
                }
            }
            broadcast_fake_hide(server, &name, false, Some(hidden_uuid));
        }
    }
    super::cluster_status::refresh_cluster_status(server);
}

pub fn toggle_hide_for(server: &Arc<Server>, player: &Player) -> Option<bool> {
    let gid = hide_gid_for(server, player);
    let uuid = player.gameprofile.id;
    let uuid_bytes = *uuid.as_bytes();
    let now_hidden = !is_hidden_player(server, player);
    publish_hide_announce(
        server,
        gid,
        uuid_bytes,
        &player.gameprofile.name,
        now_hidden,
        player
            .cluster_owner_install_generation
            .load(std::sync::atomic::Ordering::Acquire),
    )
    .then_some(now_hidden)
}

pub fn toggle_hide_lobby(
    server: &Arc<Server>,
    lobby: &super::cluster_lobby::LobbyCommandSender,
) -> Option<bool> {
    let uuid = *lobby.profile.id.as_bytes();
    let now_hidden = !is_hidden_gid(server, &lobby.gid) && !is_hidden_uuid_bytes(server, &uuid);
    publish_hide_announce(
        server,
        lobby.gid,
        uuid,
        &lobby.profile.name,
        now_hidden,
        lobby.session_generation,
    )
    .then_some(now_hidden)
}

pub fn apply_hide_bytes(
    server: &Arc<Server>,
    peer: ServerId,
    generation: u64,
    bytes: &[u8],
) -> bool {
    let update = match decode_hide(bytes) {
        Ok(update) => update,
        Err(_) => return false,
    };
    if !super::cluster_presence::inbound_control_allowed(
        ServerId(server.advanced_config.cluster.server_id),
        peer,
        generation,
        update.gid.server,
        &server.cluster_peer_generations.load(),
        &server.cluster_primary_peers.load(),
    ) {
        return false;
    }
    server
        .cluster_presence
        .try_submit(super::cluster_presence::PresenceCommand::RemoteHide {
            peer,
            generation,
            update,
        })
}

pub fn handle_hide_parcel(server: &Arc<Server>, parcel: &InboundParcel) -> bool {
    if parcel.header.kind != StreamKind::Control {
        return false;
    }
    apply_hide_bytes(server, parcel.peer, parcel.generation, &parcel.bytes)
}

pub async fn hide_task(server: Arc<Server>, mut hide_rx: mpsc::Receiver<InboundParcel>) {
    let mut first = true;
    while let Some(parcel) = hide_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster hide stream started");
        }
        apply_hide_bytes(&server, parcel.peer, parcel.generation, &parcel.bytes);
    }
    debug!("cluster hide stream closed");
}

pub fn spawn_hide_apply(server: &Arc<Server>, hide_rx: mpsc::Receiver<InboundParcel>) {
    let task_server = Arc::clone(server);
    server.spawn_task(hide_task(task_server, hide_rx));
}

#[must_use]
pub fn is_hidden_remote_gid(server: &Server, gid: &GlobalPlayerId) -> bool {
    is_hidden_gid(server, gid)
}

pub fn refresh_profile_skin(server: &Server, uuid: uuid::Uuid) -> bool {
    let snapshot = server.cluster_presence.snapshot();
    let Some(entry) = snapshot.by_uuid(uuid.as_bytes()) else { return true; };
    super::cluster_roster::changed(server, entry.gid)
}
