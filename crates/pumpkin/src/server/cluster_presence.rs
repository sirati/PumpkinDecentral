use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use pumpkin_cluster::admin_sync::PlayerDirectorySnapshot;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::presence::{
    PRESENCE_MAX_ENTRIES, PresenceControl, PresenceLogin, PresenceLogout, PresenceProperty,
    RemotePlayerEntry, RemotePlayerProfile, decode_control, encode_control,
    is_valid_presence_name,
};
use pumpkin_cluster::protocol::{PlayerGameMode, StreamKind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::entity::player::Player;
use crate::net::GameProfile;
use pumpkin_util::GameMode;

struct PresenceOutbox {
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static PRESENCE_OUTBOX: OnceLock<PresenceOutbox> = OnceLock::new();
static NEXT_SLOT: AtomicU16 = AtomicU16::new(1);
static PRESENCE_ANNOUNCED: AtomicU64 = AtomicU64::new(0);
static REMOTE_PRESENCE: LazyLock<ArcSwap<Vec<RemotePlayerEntry>>> =
    LazyLock::new(|| ArcSwap::from_pointee(Vec::new()));

const LOGOUT_GRACE_MILLIS: u64 = 30_000;

#[must_use]
fn presence_gamemode(gamemode: GameMode) -> PlayerGameMode {
    match gamemode {
        GameMode::Survival => PlayerGameMode::Survival,
        GameMode::Creative => PlayerGameMode::Creative,
        GameMode::Adventure => PlayerGameMode::Adventure,
        GameMode::Spectator => PlayerGameMode::Spectator,
    }
}

#[must_use]
pub fn presence_properties_for(player: &Player) -> Vec<PresenceProperty> {
    player
        .gameprofile
        .properties
        .load()
        .iter()
        .map(|property| {
            PresenceProperty::new(
                property.name.to_string(),
                property.value.to_string(),
                property
                    .signature
                    .as_ref()
                    .map(|signature| signature.to_string()),
            )
        })
        .collect()
}

#[must_use]
pub fn login_parcel_for_peer(login: &PresenceLogin, peer: ServerId) -> Option<OutboundParcel> {
    let bytes = encode_control(&PresenceControl::Login(login.clone())).ok()?;
    Some(OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes,
    })
}

pub fn publish_roster_to_peer(server: &Server, peer: u16) {
    let Some(outbox) = PRESENCE_OUTBOX.get() else {
        return;
    };
    let target = ServerId(peer);
    for player in server.get_all_players() {
        let gid = assign_login_gid(server, &player);
        if gid.server != outbox.local {
            continue;
        }
        let name = player.gameprofile.name.clone();
        if !is_valid_presence_name(&name) {
            continue;
        }
        let login = PresenceLogin::new(
            gid,
            player.gameprofile.id.into_bytes(),
            name,
            presence_properties_for(&player),
            presence_gamemode(player.gamemode.load()),
            false,
        );
        if let Some(parcel) = login_parcel_for_peer(&login, target) {
            let _ = outbox.outbound.try_send(parcel);
        }
    }
    for waiter in server.lobby_waiters.load().iter() {
        if waiter.gid.server != outbox.local || !is_valid_presence_name(&waiter.profile.name) {
            continue;
        }
        let login = PresenceLogin::new(
            waiter.gid,
            waiter.profile.id.into_bytes(),
            waiter.profile.name.clone(),
            profile_properties(&waiter.profile),
            PlayerGameMode::Spectator,
            true,
        );
        if let Some(parcel) = login_parcel_for_peer(&login, target) {
            let _ = outbox.outbound.try_send(parcel);
        }
    }
}

#[must_use]
pub fn presence_announced() -> u64 {
    PRESENCE_ANNOUNCED.load(Ordering::Relaxed)
}

#[must_use]
pub fn remote_presence_count() -> usize {
    REMOTE_PRESENCE.load().len()
}

pub fn install_presence_outbox(
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = PRESENCE_OUTBOX.set(PresenceOutbox {
        local,
        peers,
        outbound,
    });
}

#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

fn local_server_id(server: &Server) -> ServerId {
    ServerId(server.advanced_config.cluster.server_id)
}

fn is_primary(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
        && matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
}

#[must_use]
pub fn assign_login_gid(server: &Server, player: &Player) -> GlobalPlayerId {
    if let Some(gid) = player.cluster_gid() {
        return gid;
    }
    let local = local_server_id(server);
    if !cluster_enabled(server) {
        return GlobalPlayerId::new(local, PlayerSlot(0));
    }
    let mut slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    if slot == u16::MAX {
        slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    }
    let gid = GlobalPlayerId::new(local, PlayerSlot(slot));
    player.set_cluster_gid(Some(gid));
    gid
}

#[must_use]
pub fn assign_lobby_gid(server: &Server) -> GlobalPlayerId {
    let local = local_server_id(server);
    let mut slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    if slot == u16::MAX {
        slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    }
    GlobalPlayerId::new(local, PlayerSlot(slot))
}

#[must_use]
fn profile_properties(profile: &GameProfile) -> Vec<PresenceProperty> {
    profile
        .properties
        .load()
        .iter()
        .map(|property| {
            PresenceProperty::new(
                property.name.to_string(),
                property.value.to_string(),
                property
                    .signature
                    .as_ref()
                    .map(|signature| signature.to_string()),
            )
        })
        .collect()
}

pub fn publish_login(server: &Server, player: &Player) {
    super::cluster_status::refresh_cluster_status(server);
    if !cluster_enabled(server) {
        return;
    }
    let gid = assign_login_gid(server, player);
    let name = player.gameprofile.name.clone();
    if !is_valid_presence_name(&name) {
        warn!(name = name.as_str(), "cluster presence login dropped: invalid name");
        return;
    }
    let login = PresenceLogin::new(
        gid,
        player.gameprofile.id.into_bytes(),
        name,
        presence_properties_for(player),
        presence_gamemode(player.gamemode.load()),
        false,
    );
    let Ok(bytes) = encode_control(&PresenceControl::Login(login)) else {
        warn!("cluster presence login encode failed");
        return;
    };
    let Some(outbox) = PRESENCE_OUTBOX.get() else {
        return;
    };
    if gid.server != outbox.local {
        return;
    }
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
        if !player.cluster_login_announced.swap(true, Ordering::AcqRel) {
            PRESENCE_ANNOUNCED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn publish_lobby_login(server: &Server, gid: GlobalPlayerId, profile: &GameProfile) {
    if !cluster_enabled(server) || !is_valid_presence_name(&profile.name) {
        return;
    }
    let Some(outbox) = PRESENCE_OUTBOX.get() else {
        return;
    };
    if gid.server != outbox.local {
        return;
    }
    let Ok(bytes) = encode_control(&PresenceControl::Login(PresenceLogin::new(
        gid,
        profile.id.into_bytes(),
        profile.name.clone(),
        profile_properties(profile),
        PlayerGameMode::Spectator,
        true,
    ))) else {
        warn!(uuid = %profile.id, "cluster lobby presence encode failed");
        return;
    };
    let mut sent = 0_u64;
    for peer in &outbox.peers {
        if outbox
            .outbound
            .try_send(OutboundParcel {
                peer: ServerId(*peer),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: bytes.clone(),
            })
            .is_ok()
        {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        PRESENCE_ANNOUNCED.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn publish_lobby_logout(server: &Server, gid: GlobalPlayerId) {
    if !cluster_enabled(server) {
        return;
    }
    let Some(outbox) = PRESENCE_OUTBOX.get() else {
        return;
    };
    if gid.server != outbox.local {
        return;
    }
    let Ok(bytes) = encode_control(&PresenceControl::Logout(PresenceLogout::new(gid))) else {
        return;
    };
    for peer in &outbox.peers {
        let _ = outbox.outbound.try_send(OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        });
    }
}

pub fn publish_logout(server: &Server, player: &Player) {
    super::cluster_status::refresh_cluster_status(server);
    if !cluster_enabled(server) {
        return;
    }
    let Some(gid) = player.cluster_gid() else {
        return;
    };
    let Ok(bytes) = encode_control(&PresenceControl::Logout(PresenceLogout::new(gid))) else {
        warn!("cluster presence logout encode failed");
        return;
    };
    let Some(outbox) = PRESENCE_OUTBOX.get() else {
        return;
    };
    if gid.server != outbox.local {
        return;
    }
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
        PRESENCE_ANNOUNCED.fetch_add(1, Ordering::Relaxed);
    }
    if pumpkin_world::level::is_cluster_secondary()
        && pumpkin_world::level::cluster_has_peers()
    {
        let watched: Vec<pumpkin_util::math::vector2::Vector2<i32>> =
            player.watched_section.load().all_chunks_within().collect();
        player.world().level.keep_cluster_chunks_for_relog(
            watched,
            now_millis().saturating_add(LOGOUT_GRACE_MILLIS),
        );
    }
}

pub fn note_remote_login(login: &PresenceLogin, stamp: u64) -> bool {
    if !is_valid_presence_name(login.name.as_str())
        || !login
            .properties
            .iter()
            .all(pumpkin_cluster::presence::is_valid_presence_property)
    {
        return false;
    }
    let current = REMOTE_PRESENCE.load();
    let mut next = (**current).clone();
    let entry = RemotePlayerEntry::new(
        login.gid,
        login.uuid,
        login.name.clone(),
        login.properties.clone(),
        login.gamemode,
        stamp,
        login.in_lobby,
    );
    if let Some(index) = next.iter().position(|known| known.gid == login.gid) {
        next[index] = entry;
    } else {
        if next.len() == PRESENCE_MAX_ENTRIES {
            next.remove(0);
        }
        next.push(entry);
    }
    REMOTE_PRESENCE.store(Arc::new(next));
    true
}

pub fn forget_remote_server(server: u16) -> usize {
    let current = REMOTE_PRESENCE.load();
    let mut next = (**current).clone();
    let before = next.len();
    next.retain(|entry| entry.gid.server.0 != server);
    let cleared = before.saturating_sub(next.len());
    if cleared != 0 {
        REMOTE_PRESENCE.store(Arc::new(next));
    }
    cleared
}

pub fn forget_remote_presence(gid: &GlobalPlayerId) -> Option<RemotePlayerEntry> {
    let current = REMOTE_PRESENCE.load();
    let mut next = (**current).clone();
    let index = next.iter().position(|entry| entry.gid == *gid)?;
    let removed = next.remove(index);
    REMOTE_PRESENCE.store(Arc::new(next));
    Some(removed)
}

#[must_use]
pub fn remote_presence_entries() -> Vec<(GlobalPlayerId, RemotePlayerEntry)> {
    REMOTE_PRESENCE
        .load()
        .iter()
        .cloned()
        .map(|entry| (entry.gid, entry))
        .collect()
}

#[must_use]
pub fn remote_presence_snapshot() -> Arc<Vec<RemotePlayerEntry>> {
    REMOTE_PRESENCE.load_full()
}

#[must_use]
pub fn remote_player_in_lobby(name: &str) -> bool {
    remote_presence_entries().into_iter().any(|(_, entry)| {
        entry.in_lobby && entry.name.eq_ignore_ascii_case(name)
    })
}

#[must_use]
pub fn remote_presence_entry(gid: GlobalPlayerId) -> Option<RemotePlayerEntry> {
    REMOTE_PRESENCE
        .load()
        .iter()
        .find(|entry| entry.gid == gid)
        .cloned()
}

#[must_use]
pub fn remote_replica_profile(gid: GlobalPlayerId) -> Option<RemotePlayerProfile> {
    REMOTE_PRESENCE
        .load()
        .iter()
        .find(|entry| entry.gid == gid)
        .and_then(RemotePlayerEntry::replica_profile)
}

pub fn apply_presence_bytes(
    server: &Arc<Server>,
    local: ServerId,
    source: ServerId,
    bytes: &[u8],
) -> bool {
    let control = match decode_control(bytes) {
        Ok(control) => control,
        Err(_) => return false,
    };
    match control {
        PresenceControl::Login(login) => {
            if source == local || login.gid.server != source {
                return false;
            }
            super::cluster_playerdata::note_presence(login.clone());
            let was_known = remote_presence_entry(login.gid).is_some();
            if !note_remote_login(&login, now_millis()) {
                debug!(
                    server = login.gid.server.0,
                    "cluster presence login dropped"
                );
                return false;
            }
            if was_known {
                super::cluster_status::refresh_cluster_status(server);
                return true;
            }
            super::cluster_hide::push_remote_login_to_viewers(
                server,
                &login.gid,
                &login.uuid,
                &login.name,
                &super::cluster_chat_pm::protocol_properties(&login.properties),
            );
            if !super::cluster_hide::is_hidden_gid(&login.gid)
                && !super::cluster_hide::is_hidden_uuid_bytes(&login.uuid)
            {
                let join_message = TextComponent::translate_cross(
                    pumpkin_data::translation::java::MULTIPLAYER_PLAYER_JOINED,
                    pumpkin_data::translation::bedrock::MULTIPLAYER_PLAYER_JOINED,
                    [TextComponent::text(login.name.clone())],
                )
                .color_named(NamedColor::Yellow);
                for world in server.worlds.load().iter() {
                    world.broadcast_system_message(&join_message, false);
                }
            }
            if is_primary(server) {
                info!(
                    name = login.name.as_str(),
                    server = login.gid.server.0,
                    slot = login.gid.player.0,
                    "cluster player joined"
                );
            } else {
                debug!(
                    name = login.name.as_str(),
                    server = login.gid.server.0,
                    "cluster presence login applied"
                );
            }
            super::cluster_status::refresh_cluster_status(server);
            true
        }
        PresenceControl::Logout(logout) => {
            if source == local || logout.gid.server != source {
                return false;
            }
            super::cluster_playerdata::note_presence_logout(logout.gid);
            let hidden = super::cluster_hide::is_hidden_gid(&logout.gid);
            let removed = forget_remote_presence(&logout.gid);
            let hidden = hidden
                || removed
                    .as_ref()
                    .is_some_and(|entry| super::cluster_hide::is_hidden_uuid_bytes(&entry.uuid));
            if let Some(entry) = removed.as_ref() {
                super::cluster_hide::push_remote_logout_to_viewers(
                    server,
                    &logout.gid,
                    &entry.uuid,
                    &entry.name,
                );
            }
            if !hidden {
                if let Some(entry) = removed.as_ref() {
                    let leave_message = TextComponent::translate_cross(
                        pumpkin_data::translation::java::MULTIPLAYER_PLAYER_LEFT,
                        pumpkin_data::translation::bedrock::MULTIPLAYER_PLAYER_LEFT,
                        [TextComponent::text(entry.name.clone())],
                    )
                    .color_named(NamedColor::Yellow);
                    for world in server.worlds.load().iter() {
                        world.broadcast_system_message(&leave_message, false);
                    }
                }
            }
            match removed {
                Some(entry) => {
                    if is_primary(server) {
                        info!(
                            name = entry.name.as_str(),
                            server = logout.gid.server.0,
                            slot = logout.gid.player.0,
                            "cluster player left"
                        );
                    } else {
                        debug!(
                            name = entry.name.as_str(),
                            server = logout.gid.server.0,
                            "cluster presence logout applied"
                        );
                    }
                }
                None => {
                    debug!(
                        server = logout.gid.server.0,
                        slot = logout.gid.player.0,
                        "cluster presence logout for unknown player"
                    );
                }
            }
            super::cluster_status::refresh_cluster_status(server);
            true
        }
    }
}

pub async fn presence_task(
    server: Arc<Server>,
    local: ServerId,
    mut presence_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut first = true;
    while let Some(parcel) = presence_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster presence stream started");
        }
        apply_presence_bytes(&server, local, parcel.peer, &parcel.bytes);
    }
    debug!("cluster presence stream closed");
}

pub fn spawn_presence_apply(
    server: &Arc<Server>,
    local: ServerId,
    presence_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(presence_task(task_server, local, presence_rx));
}

#[must_use]
pub fn admin_directory_snapshot(server: &Server) -> PlayerDirectorySnapshot {
    let local = local_server_id(server);
    let mut snapshot = PlayerDirectorySnapshot::new(local);
    for player in server.get_all_players() {
        if let Some(gid) = player.cluster_gid() {
            snapshot.apply_presence_login(gid, player.gameprofile.name.clone());
        }
    }
    for (gid, entry) in remote_presence_entries() {
        if snapshot.player_name(&gid).is_none()
            && snapshot.locate_by_name(entry.name.as_str()).is_none()
        {
            snapshot.apply_presence_login(gid, entry.name.clone());
        }
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn login(server: u16, player: u16, name: &str) -> PresenceLogin {
        PresenceLogin::new(
            GlobalPlayerId::new(ServerId(server), PlayerSlot(player)),
            [0xAB; 16],
            name.to_string(),
            Vec::new(),
            PlayerGameMode::Survival,
            false,
        )
    }

    #[test]
    fn roster_parcel_roundtrips_to_newcomer() {
        let announced = login(921, 4, "SyncAlice");
        let parcel = login_parcel_for_peer(&announced, ServerId(9)).unwrap();
        assert_eq!(parcel.peer, ServerId(9));
        assert_eq!(
            decode_control(&parcel.bytes).unwrap(),
            PresenceControl::Login(announced)
        );
    }

    #[test]
    fn notes_and_forgets_remote_login() {
        let announced = login(911, 1, "PresenceAlice");
        assert!(note_remote_login(&announced, 10));
        let found = remote_presence_entries()
            .into_iter()
            .find(|(gid, _)| *gid == announced.gid);
        assert_eq!(found.unwrap().1.name, String::from("PresenceAlice"));
        let removed = forget_remote_presence(&announced.gid);
        assert_eq!(removed.unwrap().name, String::from("PresenceAlice"));
        assert!(forget_remote_presence(&announced.gid).is_none());
    }

    #[test]
    fn rejects_invalid_login_names() {
        let announced = login(912, 1, "");
        assert!(!note_remote_login(&announced, 10));
        assert!(forget_remote_presence(&announced.gid).is_none());
    }

    #[test]
    fn relogin_overwrites_stale_entry() {
        let first = login(915, 1, "PresenceCarol");
        assert!(note_remote_login(&first, 10));
        let second = PresenceLogin::new(
            first.gid,
            [0xCD; 16],
            String::from("PresenceCarol"),
            Vec::new(),
            PlayerGameMode::Creative,
            false,
        );
        assert!(note_remote_login(&second, 20));
        let found = remote_presence_entries()
            .into_iter()
            .find(|(gid, _)| *gid == first.gid)
            .unwrap();
        assert_eq!(found.1.uuid, [0xCD; 16]);
        assert!(forget_remote_presence(&first.gid).is_some());
    }

    #[test]
    fn replica_profile_keeps_authenticated_login_fields() {
        let announced = PresenceLogin::new(
            GlobalPlayerId::new(ServerId(916), PlayerSlot(1)),
            [0xCE; 16],
            String::from("PresenceDana"),
            vec![PresenceProperty::new(
                String::from("textures"),
                String::from("signed-texture"),
                Some(String::from("signature")),
            )],
            PlayerGameMode::Adventure,
            false,
        );
        assert!(note_remote_login(&announced, 30));
        let profile = remote_replica_profile(announced.gid).unwrap();
        assert_eq!(profile.uuid, announced.uuid);
        assert_eq!(profile.name, announced.name);
        assert_eq!(profile.properties, announced.properties);
        assert_eq!(profile.gamemode, PlayerGameMode::Adventure);
        assert!(forget_remote_presence(&announced.gid).is_some());
    }

    #[test]
    fn lobby_presence_has_no_replica_profile() {
        let mut announced = login(917, 1, "PresenceEli");
        announced.in_lobby = true;
        assert!(note_remote_login(&announced, 40));
        assert!(remote_replica_profile(announced.gid).is_none());
        assert!(forget_remote_presence(&announced.gid).is_some());
    }

    #[test]
    fn entries_carry_host_server() {
        let announced = login(913, 2, "PresenceBob");
        assert!(note_remote_login(&announced, 77));
        let found = remote_presence_entries()
            .into_iter()
            .find(|(gid, _)| *gid == announced.gid);
        let (_, entry) = found.unwrap();
        assert_eq!(entry.host(), ServerId(913));
        assert_eq!(entry.joined_at_millis, 77);
        assert!(forget_remote_presence(&announced.gid).is_some());
    }

    #[test]
    fn remote_lobby_lookup_tracks_presence_updates() {
        let mut waiting = login(916, 3, "PresenceLobby");
        waiting.in_lobby = true;
        assert!(note_remote_login(&waiting, 10));
        assert!(remote_player_in_lobby("presencelobby"));

        let active = login(916, 3, "PresenceLobby");
        assert!(note_remote_login(&active, 20));
        assert!(!remote_player_in_lobby("PresenceLobby"));
        assert!(forget_remote_presence(&active.gid).is_some());
    }

    #[test]
    fn logout_announce_roundtrips() {
        let logout = PresenceLogout::new(GlobalPlayerId::new(ServerId(914), PlayerSlot(3)));
        let bytes = encode_control(&PresenceControl::Logout(logout)).unwrap();
        assert_eq!(
            decode_control(&bytes).unwrap(),
            PresenceControl::Logout(logout)
        );
        assert!(decode_control(&[]).is_err());
        assert!(now_millis() > 0);
    }
}
