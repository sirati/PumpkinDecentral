use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::admin_sync::PlayerDirectorySnapshot;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::presence::{
    PRESENCE_MAX_ENTRIES, PRESENCE_MAX_NAME_LEN, PresenceControl, PresenceLogin, PresenceLogout,
    PresenceProperty, RemotePlayerEntry, decode_control, encode_control, is_valid_presence_name,
};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::entity::player::Player;

struct PresenceOutbox {
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static PRESENCE_OUTBOX: OnceLock<PresenceOutbox> = OnceLock::new();
static NEXT_SLOT: AtomicU16 = AtomicU16::new(1);
static PRESENCE_ANNOUNCED: AtomicU64 = AtomicU64::new(0);
static PRESENCE_VICTIM: AtomicU64 = AtomicU64::new(0);
static REMOTE_SLOTS: LazyLock<[PresenceSlot; PRESENCE_MAX_ENTRIES]> =
    LazyLock::new(|| core::array::from_fn(|_| PresenceSlot::new()));

const PRESENCE_READ_RETRIES: u8 = 4;
const LOGOUT_GRACE_MILLIS: u64 = 30_000;

struct PresenceSlot {
    claimed: AtomicBool,
    live: AtomicBool,
    seq: AtomicU64,
    server: AtomicU16,
    player: AtomicU16,
    uuid_hi: AtomicU64,
    uuid_lo: AtomicU64,
    name_len: AtomicU8,
    name_first: AtomicU64,
    name_rest: AtomicU64,
    joined: AtomicU64,
    in_lobby: AtomicBool,
}

#[must_use]
fn pack_name(name: &str) -> (u64, u64, u8) {
    let bytes = name.as_bytes();
    let mut padded = [0_u8; PRESENCE_MAX_NAME_LEN];
    let end = bytes.len().min(PRESENCE_MAX_NAME_LEN);
    padded[..end].copy_from_slice(&bytes[..end]);
    let (first, rest) = padded.split_at(8);
    let len = u8::try_from(bytes.len().min(PRESENCE_MAX_NAME_LEN)).unwrap_or(u8::MAX);
    (
        u64::from_le_bytes(first.try_into().unwrap_or([0_u8; 8])),
        u64::from_le_bytes(rest.try_into().unwrap_or([0_u8; 8])),
        len,
    )
}

#[must_use]
fn unpack_name(first: u64, rest: u64, len: u8) -> String {
    let mut bytes = [0_u8; PRESENCE_MAX_NAME_LEN];
    bytes[..8].copy_from_slice(&first.to_le_bytes());
    bytes[8..].copy_from_slice(&rest.to_le_bytes());
    let end = usize::from(len).min(PRESENCE_MAX_NAME_LEN);
    String::from_utf8(bytes[..end].to_vec()).unwrap_or_default()
}

impl PresenceSlot {
    fn new() -> Self {
        Self {
            claimed: AtomicBool::new(false),
            live: AtomicBool::new(false),
            seq: AtomicU64::new(0),
            server: AtomicU16::new(0),
            player: AtomicU16::new(0),
            uuid_hi: AtomicU64::new(0),
            uuid_lo: AtomicU64::new(0),
            name_len: AtomicU8::new(0),
            name_first: AtomicU64::new(0),
            name_rest: AtomicU64::new(0),
            joined: AtomicU64::new(0),
            in_lobby: AtomicBool::new(false),
        }
    }

    fn matches(&self, gid: GlobalPlayerId) -> bool {
        self.claimed.load(Ordering::Acquire)
            && self.server.load(Ordering::Acquire) == gid.server.0
            && self.player.load(Ordering::Acquire) == gid.player.0
    }

    fn write(&self, login: &PresenceLogin, stamp: u64) {
        let (first, rest, len) = pack_name(login.name.as_str());
        self.seq.fetch_add(1, Ordering::AcqRel);
        self.server.store(login.gid.server.0, Ordering::Relaxed);
        self.player.store(login.gid.player.0, Ordering::Relaxed);
        self.uuid_hi.store(
            u64::from_le_bytes(login.uuid[..8].try_into().unwrap_or([0_u8; 8])),
            Ordering::Relaxed,
        );
        self.uuid_lo.store(
            u64::from_le_bytes(login.uuid[8..].try_into().unwrap_or([0_u8; 8])),
            Ordering::Relaxed,
        );
        self.name_first.store(first, Ordering::Relaxed);
        self.name_rest.store(rest, Ordering::Relaxed);
        self.name_len.store(len, Ordering::Relaxed);
        self.joined.store(stamp, Ordering::Relaxed);
        self.in_lobby.store(login.in_lobby, Ordering::Relaxed);
        self.live.store(true, Ordering::Release);
        self.seq.fetch_add(1, Ordering::AcqRel);
    }

    fn clear(&self) {
        self.seq.fetch_add(1, Ordering::AcqRel);
        self.live.store(false, Ordering::Release);
        self.claimed.store(false, Ordering::Release);
        self.seq.fetch_add(1, Ordering::AcqRel);
    }

    fn read(&self) -> Option<(GlobalPlayerId, RemotePlayerEntry)> {
        if !self.live.load(Ordering::Acquire) {
            return None;
        }
        let before = self.seq.load(Ordering::Acquire);
        if before & 1 == 1 {
            return None;
        }
        let gid = GlobalPlayerId::new(
            ServerId(self.server.load(Ordering::Acquire)),
            PlayerSlot(self.player.load(Ordering::Acquire)),
        );
        let mut uuid = [0_u8; 16];
        uuid[..8].copy_from_slice(&self.uuid_hi.load(Ordering::Acquire).to_le_bytes());
        uuid[8..].copy_from_slice(&self.uuid_lo.load(Ordering::Acquire).to_le_bytes());
        let name = unpack_name(
            self.name_first.load(Ordering::Acquire),
            self.name_rest.load(Ordering::Acquire),
            self.name_len.load(Ordering::Acquire),
        );
        let joined = self.joined.load(Ordering::Acquire);
        let in_lobby = self.in_lobby.load(Ordering::Acquire);
        if before != self.seq.load(Ordering::Acquire) || !self.live.load(Ordering::Acquire) {
            return None;
        }
        Some((
            gid,
            RemotePlayerEntry::new(gid, uuid, name, Vec::new(), joined, in_lobby),
        ))
    }
}

fn find_slot(gid: GlobalPlayerId) -> Option<usize> {
    REMOTE_SLOTS.iter().position(|slot| slot.matches(gid))
}

fn claim_slot() -> usize {
    for (index, slot) in REMOTE_SLOTS.iter().enumerate() {
        if slot
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return index;
        }
    }
    let victim = usize::try_from(PRESENCE_VICTIM.fetch_add(1, Ordering::Relaxed)).unwrap_or(0);
    victim % PRESENCE_MAX_ENTRIES
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
            player.is_in_cluster_lobby(),
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
    REMOTE_SLOTS
        .iter()
        .filter(|slot| slot.live.load(Ordering::Acquire))
        .count()
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

pub fn publish_login(server: &Server, player: &Player) {
    if !cluster_enabled(server) {
        return;
    }
    if player.cluster_login_announced.load(Ordering::Acquire) {
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
        player.is_in_cluster_lobby(),
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
        PRESENCE_ANNOUNCED.fetch_add(1, Ordering::Relaxed);
        player.cluster_login_announced.store(true, Ordering::Release);
    }
}

pub fn publish_lobby_state(server: &Server, player: &Player) {
    if !cluster_enabled(server) {
        return;
    }
    let gid = assign_login_gid(server, player);
    let name = player.gameprofile.name.clone();
    if !is_valid_presence_name(&name) {
        return;
    }
    let login = PresenceLogin::new(
        gid,
        player.gameprofile.id.into_bytes(),
        name,
        presence_properties_for(player),
        player.is_in_cluster_lobby(),
    );
    let Ok(bytes) = encode_control(&PresenceControl::Login(login)) else {
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
}

pub fn publish_logout(server: &Server, player: &Player) {
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
    if !is_valid_presence_name(login.name.as_str()) {
        return false;
    }
    let index = find_slot(login.gid).unwrap_or_else(claim_slot);
    REMOTE_SLOTS[index].write(login, stamp);
    super::cluster_chat_pm::note_remote_roster(login.gid, &login.name, &login.properties);
    true
}

pub fn forget_remote_server(server: u16) -> usize {
    let mut cleared = 0;
    for slot in REMOTE_SLOTS.iter() {
        if slot.claimed.load(Ordering::Acquire) && slot.server.load(Ordering::Acquire) == server
        {
            if let Some((gid, _)) = slot.read() {
                slot.clear();
                let _ = super::cluster_chat_pm::forget_remote_player(&gid);
                cleared += 1;
            }
        }
    }
    cleared
}

pub fn forget_remote_presence(gid: &GlobalPlayerId) -> Option<RemotePlayerEntry> {
    for _ in 0..PRESENCE_READ_RETRIES {
        let Some(index) = find_slot(*gid) else {
            return None;
        };
        if let Some((_, entry)) = REMOTE_SLOTS[index].read() {
            REMOTE_SLOTS[index].clear();
            let _ = super::cluster_chat_pm::forget_remote_player(gid);
            return Some(entry);
        }
    }
    None
}

#[must_use]
pub fn remote_presence_entries() -> Vec<(GlobalPlayerId, RemotePlayerEntry)> {
    let mut merged: HashMap<GlobalPlayerId, RemotePlayerEntry> = HashMap::new();
    for slot in REMOTE_SLOTS.iter() {
        if let Some((gid, entry)) = slot.read() {
            merged.insert(gid, entry);
        }
    }
    merged.into_iter().collect()
}

#[must_use]
pub fn remote_player_in_lobby(name: &str) -> bool {
    remote_presence_entries().into_iter().any(|(_, entry)| {
        entry.in_lobby && entry.name.eq_ignore_ascii_case(name)
    })
}

#[must_use]
pub fn remote_presence_entry(gid: GlobalPlayerId) -> Option<RemotePlayerEntry> {
    find_slot(gid).and_then(|index| REMOTE_SLOTS[index].read().map(|(_, entry)| entry))
}

pub fn apply_presence_bytes(server: &Arc<Server>, local: ServerId, bytes: &[u8]) -> bool {
    let control = match decode_control(bytes) {
        Ok(control) => control,
        Err(_) => return false,
    };
    match control {
        PresenceControl::Login(login) => {
            if login.gid.server == local {
                return false;
            }
            let was_known = find_slot(login.gid).is_some();
            if !note_remote_login(&login, now_millis()) {
                debug!(
                    server = login.gid.server.0,
                    "cluster presence login dropped"
                );
                return false;
            }
            if was_known {
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
            true
        }
        PresenceControl::Logout(logout) => {
            if logout.gid.server == local {
                return false;
            }
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
        apply_presence_bytes(&server, local, &parcel.bytes);
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
        let gid = player
            .cluster_gid()
            .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(u16::MAX)));
        snapshot.apply_presence_login(gid, player.gameprofile.name.clone());
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
