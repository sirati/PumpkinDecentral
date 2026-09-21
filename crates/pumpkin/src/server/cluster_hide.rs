use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use pumpkin_cluster::hide::{HideUpdate, decode_hide, is_valid_hide_name};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_protocol::bedrock::client::add_player::CAddPlayer;
use pumpkin_protocol::bedrock::client::common::BuildPlatform;
use pumpkin_protocol::bedrock::client::player_list::{CPlayerList, PlayerListEntry, Skin};
use pumpkin_protocol::bedrock::client::remove_actor::CRemoveActor;
use pumpkin_protocol::bedrock::client::common::{SerializedAbilitiesData, SerializedAbilitiesDataSerializedLayer};
use pumpkin_protocol::bedrock::client::set_actor_data::PropertySyncData;
use pumpkin_protocol::codec::var_long::VarLong;
use pumpkin_protocol::codec::var_ulong::VarULong;
use pumpkin_protocol::java::client::play::{
    CPlayerInfoUpdate, CRemoveEntities, CRemovePlayerInfo, CSpawnEntity, PlayerAction,
    PlayerInfoFlags,
};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_util::math::{vector2::Vector2, vector3::Vector3};
use pumpkin_data::translation;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;
use tokio::sync::mpsc;
use tracing::debug;

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;

struct HideOutbox {
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static HIDE_OUTBOX: OnceLock<HideOutbox> = OnceLock::new();
static HIDE_ANNOUNCED: AtomicU64 = AtomicU64::new(0);
static HIDE_WRITE: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static HIDDEN_GIDS: LazyLock<ArcSwap<HashSet<GlobalPlayerId>>> =
    LazyLock::new(|| ArcSwap::from_pointee(HashSet::new()));
static HIDDEN_UUIDS: LazyLock<ArcSwap<HashSet<[u8; 16]>>> =
    LazyLock::new(|| ArcSwap::from_pointee(HashSet::new()));

pub const HIDE_PERMISSION: &str = "minecraft:command.hide";

#[must_use]
pub fn hide_announced() -> u64 {
    HIDE_ANNOUNCED.load(Ordering::Relaxed)
}

#[must_use]
pub fn is_hidden_gid(gid: &GlobalPlayerId) -> bool {
    HIDDEN_GIDS.load().contains(gid)
}

#[must_use]
pub fn is_hidden_uuid_bytes(uuid: &[u8; 16]) -> bool {
    HIDDEN_UUIDS.load().contains(uuid)
}

#[must_use]
pub fn is_hidden_uuid(uuid: &uuid::Uuid) -> bool {
    is_hidden_uuid_bytes(uuid.as_bytes())
}

#[must_use]
pub fn is_hidden_player(player: &Player) -> bool {
    if is_hidden_uuid(&player.gameprofile.id) {
        return true;
    }
    match player.cluster_gid() {
        Some(gid) => is_hidden_gid(&gid),
        None => false,
    }
}

#[must_use]
pub fn hidden_snapshot() -> HashSet<GlobalPlayerId> {
    HIDDEN_GIDS.load().as_ref().clone()
}

pub fn reset_hidden() {
    let _guard = HIDE_WRITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    HIDDEN_GIDS.store(Arc::new(HashSet::new()));
    HIDDEN_UUIDS.store(Arc::new(HashSet::new()));
}

pub fn set_hidden(gid: GlobalPlayerId, uuid: [u8; 16], hidden: bool) -> bool {
    let _guard = HIDE_WRITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut gids = HIDDEN_GIDS.load().as_ref().clone();
    let mut uuids = HIDDEN_UUIDS.load().as_ref().clone();
    let changed = if hidden {
        let fresh_gid = gids.insert(gid);
        let fresh_uuid = uuids.insert(uuid);
        fresh_gid || fresh_uuid
    } else {
        let had_gid = gids.remove(&gid);
        let had_uuid = uuids.remove(&uuid);
        had_gid || had_uuid
    };
    if changed {
        HIDDEN_GIDS.store(Arc::new(gids));
        HIDDEN_UUIDS.store(Arc::new(uuids));
    }
    changed
}

pub fn install_hide_outbox(
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = HIDE_OUTBOX.set(HideOutbox {
        local,
        peers,
        outbound,
    });
}

pub fn publish_hide_announce(gid: GlobalPlayerId, uuid: [u8; 16], name: &str, hidden: bool) {
    if !is_valid_hide_name(name) {
        return;
    }
    let Some(outbox) = HIDE_OUTBOX.get() else {
        return;
    };
    if gid.server != outbox.local {
        return;
    }
    let update = HideUpdate::new(gid, uuid, name.to_string(), hidden);
    let Ok(bytes) = pumpkin_cluster::hide::encode_hide(&update) else {
        return;
    };
    let mut sent = 0_u64;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        HIDE_ANNOUNCED.fetch_add(1, Ordering::Relaxed);
    }
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
        if viewer.has_permission(server, HIDE_PERMISSION) {
            continue;
        }
        viewer.send_system_message(&message);
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
    viewer: &Player,
    uuid: uuid::Uuid,
    name: &str,
    properties: &[pumpkin_protocol::Property],
) {
    let actions = [
        PlayerAction::AddPlayer { name, properties },
        PlayerAction::UpdateListed(true),
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
                skin: Skin::steve(),
                is_teacher: false,
                is_host: false,
                is_sub_client: false,
                player_color: [0, 0, 0, 0],
            }],
        },
    );
}

#[must_use]
pub fn hide_parcel_for_peer(update: &HideUpdate, peer: ServerId) -> Option<OutboundParcel> {
    let bytes = pumpkin_cluster::hide::encode_hide(update).ok()?;
    Some(OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes,
    })
}

pub fn publish_hidden_to_peer(server: &Server, peer: u16) {
    let Some(outbox) = HIDE_OUTBOX.get() else {
        return;
    };
    let target = ServerId(peer);
    for player in server.get_all_players() {
        if !is_hidden_player(&player) {
            continue;
        }
        let update = HideUpdate::new(
            hide_gid_for(server, &player),
            *player.gameprofile.id.as_bytes(),
            player.gameprofile.name.clone(),
            true,
        );
        if !is_valid_hide_name(update.name.as_str()) {
            continue;
        }
        if let Some(parcel) = hide_parcel_for_peer(&update, target) {
            let _ = outbox.outbound.try_send(parcel);
        }
    }
    for (gid, entry) in super::cluster_presence::remote_presence_entries() {
        if !is_hidden_gid(&gid) && !is_hidden_uuid_bytes(&entry.uuid) {
            continue;
        }
        let update = HideUpdate::new(gid, entry.uuid, entry.name.clone(), true);
        if let Some(parcel) = hide_parcel_for_peer(&update, target) {
            let _ = outbox.outbound.try_send(parcel);
        }
    }
}

pub fn push_remote_login_to_viewers(
    server: &Server,
    gid: &GlobalPlayerId,
    uuid_bytes: &[u8; 16],
    name: &str,
    properties: &[pumpkin_protocol::Property],
) {
    if !is_valid_hide_name(name) {
        return;
    }
    let hidden = is_hidden_gid(gid) || is_hidden_uuid_bytes(uuid_bytes);
    let uuid = uuid::Uuid::from_bytes(*uuid_bytes);
    for viewer in server.get_all_players() {
        if hidden && !viewer.has_permission(server, HIDE_PERMISSION) {
            continue;
        }
        add_remote_to_viewer(&viewer, uuid, name, properties);
    }
}

pub fn push_remote_logout_to_viewers(
    server: &Server,
    gid: &GlobalPlayerId,
    uuid_bytes: &[u8; 16],
    name: &str,
) {
    if !is_valid_hide_name(name) {
        return;
    }
    let hidden = is_hidden_gid(gid) || is_hidden_uuid_bytes(uuid_bytes);
    let uuid = uuid::Uuid::from_bytes(*uuid_bytes);
    for viewer in server.get_all_players() {
        if hidden && !viewer.has_permission(server, HIDE_PERMISSION) {
            continue;
        }
        remove_remote_from_viewer(&viewer, uuid, name);
    }
}

fn hide_name_for(uuid: uuid::Uuid, fallback: Option<&str>) -> Option<String> {
    if let Some(entry) = super::cluster_presence::remote_presence_entries()
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
    viewer.client.try_enqueue_packet_editioned(
        &CRemoveEntities::new(&[entity_id.into()]),
        &CRemoveActor::new(VarLong(i64::from(entity_id))),
    );
}

fn add_hidden_to_viewer(viewer: &Player, hidden: &Player) {
    let gameprofile = &hidden.gameprofile;
    let entity = hidden.get_entity();
    let pos = entity.pos.load();
    let velocity = entity.velocity.load();
    let pitch = entity.pitch.load();
    let yaw = entity.yaw.load();
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
    viewer.client.try_enqueue_packet_editioned(
        &CSpawnEntity::new(
            entity_id.into(),
            gameprofile.id,
            i32::from(pumpkin_data::entity::EntityType::PLAYER.id).into(),
            pos,
            pitch,
            yaw,
            yaw,
            0.into(),
            velocity,
        ),
        &CAddPlayer {
            uuid: gameprofile.id,
            player_name: gameprofile.name.clone(),
            target_runtime_id: VarULong(entity_id as u64),
            platform_chat_id: String::new(),
            position: Vector3::new(pos.x as f32, pos.y as f32, pos.z as f32),
            velocity: Vector3::new(velocity.x as f32, velocity.y as f32, velocity.z as f32),
            rotation: Vector2::new(pitch, yaw),
            y_head_rotation: entity.head_yaw.load(),
            carried_item: pumpkin_protocol::bedrock::network_item::NetworkItemStackDescriptor::default(),
            player_game_type: gamemode.into(),
            entity_data: entity.bedrock_metadata(),
            synced_properties: PropertySyncData::default(),
            abilities_data: SerializedAbilitiesData {
                target_player_raw_id: i64::from(entity_id),
                player_permissions:
                    pumpkin_protocol::bedrock::client::PlayerPermissionLevel::Visitor,
                command_permissions:
                    pumpkin_protocol::bedrock::client::CommandPermissionLevel::Any,
                layers: vec![SerializedAbilitiesDataSerializedLayer {
                    serialized_layer: 0,
                    abilities_set: 0,
                    ability_value: 0,
                    fly_speed: 0.05,
                    vertical_fly_speed: 0.05,
                    walk_speed: 0.1,
                }],
            },
            actor_links: Vec::new(),
            device_id: String::new(),
            build_platform: BuildPlatform::Unknown,
        },
    );
}

pub fn apply_hide_visibility(
    server: &Server,
    hidden_uuid: uuid::Uuid,
    fallback_name: Option<&str>,
    hidden: bool,
) {
    let viewers = server.get_all_players();
    if hidden {
        let local = viewers
            .iter()
            .find(|player| player.gameprofile.id == hidden_uuid)
            .map(|player| (player.entity_id(), player.gameprofile.name.clone()));
        let (entity_id, name) = match local {
            Some(found) => (Some(found.0), found.1),
            None => {
                let known = super::cluster_presence::remote_presence_entries()
                    .into_iter()
                    .any(|(_, entry)| entry.uuid == *hidden_uuid.as_bytes());
                if !known {
                    super::cluster_status::refresh_cluster_status(server);
                    return;
                }
                match hide_name_for(hidden_uuid, fallback_name) {
                    Some(name) => (None, name),
                    None => {
                        super::cluster_status::refresh_cluster_status(server);
                        return;
                    }
                }
            }
        };
        for viewer in &viewers {
            if viewer.gameprofile.id == hidden_uuid {
                continue;
            }
            if viewer.has_permission(server, HIDE_PERMISSION) {
                continue;
            }
            match entity_id {
                Some(entity_id) => remove_hidden_from_viewer(viewer, hidden_uuid, entity_id, &name),
                None => remove_remote_from_viewer(viewer, hidden_uuid, &name),
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
                if viewer.has_permission(server, HIDE_PERMISSION) {
                    continue;
                }
                add_hidden_to_viewer(viewer, &target);
            }
            broadcast_fake_hide(server, &name, false, Some(hidden_uuid));
        } else {
            let Some(name) = hide_name_for(hidden_uuid, fallback_name) else {
                super::cluster_status::refresh_cluster_status(server);
                return;
            };
            for viewer in &viewers {
                if viewer.gameprofile.id == hidden_uuid {
                    continue;
                }
                if viewer.has_permission(server, HIDE_PERMISSION) {
                    continue;
                }
                let properties = super::cluster_presence::remote_presence_entries()
                    .into_iter()
                    .find(|(_, entry)| entry.uuid == *hidden_uuid.as_bytes())
                    .map(|(gid, _)| super::cluster_chat_pm::remote_player_properties(&gid))
                    .unwrap_or_default();
                add_remote_to_viewer(viewer, hidden_uuid, &name, &properties);
            }
            broadcast_fake_hide(server, &name, false, Some(hidden_uuid));
        }
    }
    super::cluster_status::refresh_cluster_status(server);
}

pub fn toggle_hide_for(server: &Arc<Server>, player: &Player) -> bool {
    let gid = hide_gid_for(server, player);
    let uuid = player.gameprofile.id;
    let uuid_bytes = *uuid.as_bytes();
    let now_hidden = !is_hidden_player(player);
    set_hidden(gid, uuid_bytes, now_hidden);
    publish_hide_announce(gid, uuid_bytes, &player.gameprofile.name, now_hidden);
    apply_hide_visibility(server, uuid, Some(&player.gameprofile.name), now_hidden);
    now_hidden
}

pub fn apply_hide_bytes(server: &Arc<Server>, bytes: &[u8]) -> bool {
    let update = match decode_hide(bytes) {
        Ok(update) => update,
        Err(_) => return false,
    };
    if !set_hidden(update.gid, update.uuid, update.hidden) {
        return false;
    }
    let uuid = uuid::Uuid::from_bytes(update.uuid);
    apply_hide_visibility(server, uuid, Some(&update.name), update.hidden);
    debug!(
        server = update.gid.server.0,
        name = update.name.as_str(),
        hidden = update.hidden,
        "cluster hide applied"
    );
    true
}

pub fn handle_hide_parcel(server: &Arc<Server>, parcel: &InboundParcel) -> bool {
    if parcel.header.kind != StreamKind::Control {
        return false;
    }
    apply_hide_bytes(server, &parcel.bytes)
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
        apply_hide_bytes(&server, &parcel.bytes);
    }
    debug!("cluster hide stream closed");
}

pub fn spawn_hide_apply(server: &Arc<Server>, hide_rx: mpsc::Receiver<InboundParcel>) {
    let task_server = Arc::clone(server);
    server.spawn_task(hide_task(task_server, hide_rx));
}

#[must_use]
pub fn is_hidden_remote_gid(gid: &GlobalPlayerId) -> bool {
    is_hidden_gid(gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn hide_parcel_roundtrips_to_newcomer() {
        let update = HideUpdate::new(gid(5, 6), [6_u8; 16], String::from("SyncBob"), true);
        let parcel = hide_parcel_for_peer(&update, ServerId(9)).unwrap();
        assert_eq!(parcel.peer, ServerId(9));
        assert_eq!(decode_hide(&parcel.bytes).unwrap(), update);
    }

    #[test]
    fn toggles_gid_and_uuid_sets() {
        set_hidden(gid(3, 9), [9_u8; 16], false);
        assert!(!is_hidden_gid(&gid(3, 9)));
        assert!(!is_hidden_uuid_bytes(&[9_u8; 16]));
        assert!(set_hidden(gid(3, 9), [9_u8; 16], true));
        assert!(is_hidden_gid(&gid(3, 9)));
        assert!(is_hidden_uuid_bytes(&[9_u8; 16]));
        assert!(!set_hidden(gid(3, 9), [9_u8; 16], true));
        assert!(set_hidden(gid(3, 9), [9_u8; 16], false));
        assert!(!is_hidden_gid(&gid(3, 9)));
        assert!(!is_hidden_uuid_bytes(&[9_u8; 16]));
    }

    #[test]
    fn hide_update_roundtrips_through_store() {
        set_hidden(gid(4, 5), [5_u8; 16], false);
        let update = HideUpdate::new(gid(4, 5), [5_u8; 16], String::from("HiddenAlex"), true);
        let bytes = pumpkin_cluster::hide::encode_hide(&update).unwrap();
        let decoded = decode_hide(&bytes).unwrap();
        assert_eq!(decoded, update);
        assert!(set_hidden(decoded.gid, decoded.uuid, decoded.hidden));
        assert!(is_hidden_gid(&gid(4, 5)));
        assert!(is_hidden_uuid_bytes(&[5_u8; 16]));
        assert!(set_hidden(decoded.gid, decoded.uuid, false));
        assert!(!is_hidden_gid(&gid(4, 5)));
    }

    #[test]
    fn rejects_bad_hide_bytes() {
        assert!(decode_hide(&[]).is_err());
        assert!(!is_valid_hide_name(&String::new()));
    }
}
