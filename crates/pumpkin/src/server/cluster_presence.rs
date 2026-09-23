use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use pumpkin_cluster::admin_sync::PlayerDirectorySnapshot;
use pumpkin_cluster::hide::HideUpdate;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::presence::{
    PRESENCE_MAX_ENTRIES, PresenceControl, PresenceLogin, PresenceLogout, PresenceProperty,
    RemotePlayerEntry, decode_control, encode_control, is_valid_presence_name,
};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_util::PermissionLvl;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::Server;
use crate::entity::player::Player;

const LOGOUT_GRACE_MILLIS: u64 = 30_000;
const PENDING_STATES_PER_PEER: usize = PRESENCE_MAX_ENTRIES * 4;
type PendingKey = (ServerId, u8, GlobalPlayerId);

fn parcel_key(parcel: &OutboundParcel) -> Option<PendingKey> {
    if let Ok(control) = decode_control(&parcel.bytes) {
        let gid = match control {
            PresenceControl::Login(login) => login.gid,
            PresenceControl::Logout(logout) => logout.gid,
        };
        return Some((parcel.peer, 0, gid));
    }
    pumpkin_cluster::hide::decode_hide(&parcel.bytes)
        .ok()
        .map(|update| (parcel.peer, 1, update.gid))
}

#[derive(Debug, Clone, Default)]
pub struct PresenceSnapshot {
    pub revision: u64,
    pub entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
    pub known_profiles: HashMap<[u8; 16], String>,
    pub failed_lookup_names: HashMap<String, std::time::Instant>,
    pub hidden_gids: HashSet<GlobalPlayerId>,
    pub hidden_uuids: HashSet<[u8; 16]>,
    pub permissions: HashMap<[u8; 16], PermissionLvl>,
    pub permission_revision: u64,
    pub announced: u64,
}

impl PresenceSnapshot {
    #[must_use]
    pub fn is_hidden(&self, gid: &GlobalPlayerId, uuid: &[u8; 16]) -> bool {
        self.hidden_gids.contains(gid) || self.hidden_uuids.contains(uuid)
    }

    #[must_use]
    pub fn permission_for_uuid(&self, uuid: &[u8; 16]) -> PermissionLvl {
        self.permissions
            .get(uuid)
            .copied()
            .unwrap_or(PermissionLvl::Zero)
    }

    #[must_use]
    pub fn is_visible_to(
        &self,
        gid: &GlobalPlayerId,
        uuid: &[u8; 16],
        viewer: Option<&[u8; 16]>,
    ) -> bool {
        viewer == Some(uuid) || !self.is_hidden(gid, uuid)
    }
}

pub struct PresenceRuntime {
    snapshot: Arc<ArcSwap<PresenceSnapshot>>,
    tx: mpsc::Sender<PresenceCommand>,
}

impl PresenceRuntime {
    #[must_use]
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<PresenceCommand>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                snapshot: Arc::new(ArcSwap::from_pointee(PresenceSnapshot::default())),
                tx,
            },
            rx,
        )
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<PresenceSnapshot> {
        self.snapshot.load_full()
    }

    pub fn try_submit(&self, command: PresenceCommand) -> bool {
        self.tx.try_send(command).is_ok()
    }

    pub fn try_reserve(&self) -> Option<mpsc::OwnedPermit<PresenceCommand>> {
        self.tx.clone().try_reserve_owned().ok()
    }
}

pub enum PresenceCommand {
    Configure {
        local: ServerId,
        peers: Vec<u16>,
        outbound: mpsc::Sender<OutboundParcel>,
    },
    LocalLogin(PresenceLogin),
    LocalLogout(GlobalPlayerId),
    RemoteControl {
        peer: ServerId,
        generation: u64,
        control: PresenceControl,
    },
    LocalHide(HideUpdate),
    RemoteHide {
        peer: ServerId,
        generation: u64,
        update: HideUpdate,
    },
    PeerDropped(ServerId),
    PublishToPeer(ServerId),
    Permission {
        uuid: [u8; 16],
        level: Option<PermissionLvl>,
    },
    ReplacePermissions(HashMap<[u8; 16], PermissionLvl>),
    LookupName(String),
    LookupResult(String, Option<([u8; 16], String)>),
}

struct PresenceOwner {
    local: ServerId,
    peers: Vec<u16>,
    outbound: Option<mpsc::Sender<OutboundParcel>>,
    entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
    known_profiles: HashMap<[u8; 16], String>,
    pending_lookups: HashSet<String>,
    failed_lookups: HashMap<String, std::time::Instant>,
    hidden_gids: HashSet<GlobalPlayerId>,
    hidden_uuids: HashSet<[u8; 16]>,
    permissions: HashMap<[u8; 16], PermissionLvl>,
    permission_revision: u64,
    pending: HashMap<PendingKey, OutboundParcel>,
    pending_order: VecDeque<PendingKey>,
    revision: u64,
    announced: u64,
}

impl PresenceOwner {
    fn new() -> Self {
        Self {
            local: ServerId(0),
            peers: Vec::new(),
            outbound: None,
            entries: HashMap::new(),
            known_profiles: HashMap::new(),
            pending_lookups: HashSet::new(),
            failed_lookups: HashMap::new(),
            hidden_gids: HashSet::new(),
            hidden_uuids: HashSet::new(),
            permissions: HashMap::new(),
            permission_revision: 0,
            pending: HashMap::new(),
            pending_order: VecDeque::new(),
            revision: 0,
            announced: 0,
        }
    }

    fn publish(&mut self, runtime: &PresenceRuntime) {
        self.revision = self.revision.wrapping_add(1);
        runtime.snapshot.store(Arc::new(PresenceSnapshot {
            revision: self.revision,
            entries: self.entries.clone(),
            known_profiles: self.known_profiles.clone(),
            failed_lookup_names: self.failed_lookups.clone(),
            hidden_gids: self.hidden_gids.clone(),
            hidden_uuids: self.hidden_uuids.clone(),
            permissions: self.permissions.clone(),
            permission_revision: self.permission_revision,
            announced: self.announced,
        }));
    }

    fn queue(&mut self, parcel: OutboundParcel) {
        let Some(key) = parcel_key(&parcel) else {
            return;
        };
        if self.pending.insert(key, parcel).is_none() {
            self.pending_order.push_back(key);
        }
        debug_assert!(self.pending.len() <= self.peers.len() * PENDING_STATES_PER_PEER);
    }

    fn broadcast(&mut self, bytes: Vec<u8>) {
        let peers = self.peers.clone();
        for peer in peers {
            self.queue(OutboundParcel {
                peer: ServerId(peer),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: bytes.clone(),
            });
        }
    }

    fn broadcast_except(&mut self, excluded: ServerId, bytes: Vec<u8>) {
        let peers = self.peers.clone();
        for peer in peers {
            if peer != excluded.0 {
                self.queue_to(ServerId(peer), bytes.clone());
            }
        }
    }

    fn queue_to(&mut self, peer: ServerId, bytes: Vec<u8>) {
        if !self.peers.contains(&peer.0) {
            return;
        }
        self.queue(OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::Control, None),
            bytes,
        });
    }

    fn purge_target(&mut self, peer: ServerId) {
        self.pending.retain(|(target, _, _), _| *target != peer);
        self.pending_order.retain(|(target, _, _)| *target != peer);
    }

    fn set_peers(&mut self, peers: Vec<u16>) {
        let removed: Vec<_> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| !peers.contains(peer))
            .collect();
        for peer in removed {
            self.purge_target(ServerId(peer));
        }
        let added: Vec<_> = peers
            .iter()
            .copied()
            .filter(|peer| !self.peers.contains(peer))
            .collect();
        self.peers = peers;
        for peer in added {
            self.publish_peer_state(ServerId(peer));
        }
    }

    fn flush(&mut self) {
        let Some(outbound) = self.outbound.as_ref() else {
            return;
        };
        while let Some(key) = self.pending_order.front().copied() {
            let Some(parcel) = self.pending.remove(&key) else {
                self.pending_order.pop_front();
                continue;
            };
            match outbound.try_send(parcel) {
                Ok(()) => {
                    self.pending_order.pop_front();
                    self.announced = self.announced.saturating_add(1);
                }
                Err(mpsc::error::TrySendError::Full(parcel)) => {
                    self.pending.insert(key, parcel);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(parcel)) => {
                    self.pending.insert(key, parcel);
                    break;
                }
            }
        }
    }

    fn insert(&mut self, login: &PresenceLogin) -> Option<RemotePlayerEntry> {
        if !is_valid_presence_name(&login.name) {
            return None;
        }
        let previous = self.entries.get(&login.gid).cloned();
        if previous.is_none() && self.entries.len() >= PRESENCE_MAX_ENTRIES {
            return None;
        }
        if self.known_profiles.len() < PRESENCE_MAX_ENTRIES
            || self.known_profiles.contains_key(&login.uuid)
        {
            self.known_profiles.insert(login.uuid, login.name.clone());
        }
        let joined = previous
            .as_ref()
            .map_or_else(now_millis, |entry| entry.joined_at_millis);
        self.entries.insert(
            login.gid,
            RemotePlayerEntry::new(
                login.gid,
                login.uuid,
                login.name.clone(),
                login.properties.clone(),
                joined,
                login.in_lobby,
                login.listed,
            ),
        );
        previous
    }

    fn can_insert(&self, login: &PresenceLogin) -> bool {
        is_valid_presence_name(&login.name)
            && (self.entries.contains_key(&login.gid) || self.entries.len() < PRESENCE_MAX_ENTRIES)
    }

    fn apply_hide(&mut self, update: &HideUpdate) -> bool {
        if !pumpkin_cluster::hide::is_valid_hide_name(&update.name) {
            return false;
        }
        if let Some(entry) = self.entries.get(&update.gid) {
            if entry.uuid != update.uuid || entry.name != update.name {
                return false;
            }
        } else if update.hidden
            && !self.hidden_gids.contains(&update.gid)
            && self.hidden_gids.len() >= PRESENCE_MAX_ENTRIES
        {
            return false;
        }
        if update.hidden {
            self.hidden_gids.insert(update.gid) | self.hidden_uuids.insert(update.uuid)
        } else {
            self.hidden_gids.remove(&update.gid) | self.hidden_uuids.remove(&update.uuid)
        }
    }

    fn publish_peer_state(&mut self, peer: ServerId) {
        let local_entries: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.gid.server != peer)
            .cloned()
            .collect();
        for entry in local_entries {
            let login = PresenceLogin::new(
                entry.gid,
                entry.uuid,
                entry.name,
                entry.properties,
                entry.in_lobby,
            )
            .with_listing(entry.listed);
            if let Ok(bytes) = encode_control(&PresenceControl::Login(login)) {
                self.queue_to(peer, bytes);
            }
        }
        let hidden: Vec<_> = self
            .entries
            .values()
            .filter(|entry| {
                entry.gid.server != peer
                    && (self.hidden_gids.contains(&entry.gid)
                        || self.hidden_uuids.contains(&entry.uuid))
            })
            .cloned()
            .collect();
        for entry in hidden {
            if let Ok(bytes) = pumpkin_cluster::hide::encode_hide(&HideUpdate::new(
                entry.gid, entry.uuid, entry.name, true,
            )) {
                self.queue_to(peer, bytes);
            }
        }
    }

    fn drop_peer(&mut self, peer: ServerId) -> Vec<RemotePlayerEntry> {
        self.purge_target(peer);
        self.peers.retain(|connected| *connected != peer.0);
        let removed: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.gid.server == peer)
            .cloned()
            .collect();
        for entry in &removed {
            self.entries.remove(&entry.gid);
            self.hidden_gids.remove(&entry.gid);
        }
        removed
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
                property.signature.as_ref().map(ToString::to_string),
            )
        })
        .collect()
}

#[must_use]
pub fn login_parcel_for_peer(login: &PresenceLogin, peer: ServerId) -> Option<OutboundParcel> {
    Some(OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes: encode_control(&PresenceControl::Login(login.clone())).ok()?,
    })
}

#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

fn local_server_id(server: &Server) -> ServerId {
    ServerId(server.advanced_config.cluster.server_id)
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

fn is_primary(server: &Server) -> bool {
    cluster_enabled(server) && matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
}

pub(crate) fn inbound_control_allowed(
    local: ServerId,
    peer: ServerId,
    generation: u64,
    claimed: ServerId,
    generations: &HashMap<ServerId, u64>,
    primary_peers: &HashSet<ServerId>,
) -> bool {
    peer != local
        && generations.get(&peer) == Some(&generation)
        && (claimed == peer || primary_peers.contains(&peer))
}

#[must_use]
pub fn assign_login_gid(server: &Server, player: &Player) -> GlobalPlayerId {
    if let Some(gid) = player.cluster_gid() {
        return gid;
    }
    let gid = reserve_login_gid(server);
    player.set_cluster_gid(Some(gid));
    gid
}

#[must_use]
pub fn reserve_login_gid(server: &Server) -> GlobalPlayerId {
    let local = local_server_id(server);
    if !cluster_enabled(server) {
        return GlobalPlayerId::new(local, PlayerSlot(0));
    }
    let slot = loop {
        let slot = server.cluster_presence_slot.fetch_add(1, Ordering::Relaxed);
        if slot != u16::MAX {
            break slot;
        }
    };
    GlobalPlayerId::new(local, PlayerSlot(slot))
}

fn submit(server: &Server, command: PresenceCommand) -> bool {
    server.cluster_presence.try_submit(command)
}

#[must_use]
pub fn permission_revision(server: &Server) -> u64 {
    server.cluster_presence.snapshot().permission_revision
}

fn permission_fingerprint(permissions: &HashMap<[u8; 16], PermissionLvl>) -> u64 {
    let mut grants: Vec<_> = permissions.iter().collect();
    grants.sort_unstable_by_key(|(uuid, _)| **uuid);
    let mut fingerprint = 0;
    for (uuid, level) in grants {
        fingerprint = xxhash_rust::xxh64::xxh64(uuid, fingerprint);
        fingerprint = xxhash_rust::xxh64::xxh64(&[*level as u8], fingerprint);
    }
    fingerprint
}

pub fn install_presence_outbox(
    server: &Server,
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = submit(
        server,
        PresenceCommand::Configure {
            local,
            peers,
            outbound,
        },
    );
}

pub fn publish_roster_to_peer(server: &Server, peer: u16) {
    let _ = submit(server, PresenceCommand::PublishToPeer(ServerId(peer)));
}

#[must_use]
pub fn presence_announced(server: &Server) -> u64 {
    server.cluster_presence.snapshot().announced
}

#[must_use]
pub fn remote_presence_count(server: &Server) -> usize {
    let local = local_server_id(server);
    server
        .cluster_presence
        .snapshot()
        .entries
        .values()
        .filter(|entry| entry.gid.server != local)
        .count()
}

pub fn publish_lobby_profile(
    server: &Server,
    gid: GlobalPlayerId,
    profile: &crate::net::GameProfile,
    listed: bool,
) {
    if !cluster_enabled(server) || !is_valid_presence_name(&profile.name) {
        return;
    }
    let properties = profile
        .properties
        .load()
        .iter()
        .map(|property| {
            PresenceProperty::new(
                property.name.to_string(),
                property.value.to_string(),
                property.signature.as_ref().map(ToString::to_string),
            )
        })
        .collect();
    let _ = submit(
        server,
        PresenceCommand::LocalLogin(
            PresenceLogin::new(
                gid,
                profile.id.into_bytes(),
                profile.name.clone(),
                properties,
                true,
            )
            .with_listing(listed),
        ),
    );
}

pub fn publish_logout_gid(server: &Server, gid: GlobalPlayerId) {
    let _ = submit(server, PresenceCommand::LocalLogout(gid));
}

pub fn publish_login(server: &Server, player: &Player) {
    if !cluster_enabled(server) || player.cluster_login_announced.load(Ordering::Acquire) {
        return;
    }
    let gid = assign_login_gid(server, player);
    let login = PresenceLogin::new(
        gid,
        player.gameprofile.id.into_bytes(),
        player.gameprofile.name.clone(),
        presence_properties_for(player),
        player.is_in_cluster_lobby(),
    )
    .with_listing(player.config.load().server_listing);
    if submit(server, PresenceCommand::LocalLogin(login)) {
        player
            .cluster_login_announced
            .store(true, Ordering::Release);
    }
}

pub fn publish_lobby_state(server: &Server, player: &Player) {
    if !cluster_enabled(server) {
        return;
    }
    let gid = assign_login_gid(server, player);
    let _ = submit(
        server,
        PresenceCommand::LocalLogin(
            PresenceLogin::new(
                gid,
                player.gameprofile.id.into_bytes(),
                player.gameprofile.name.clone(),
                presence_properties_for(player),
                player.is_in_cluster_lobby(),
            )
            .with_listing(player.config.load().server_listing),
        ),
    );
}

pub fn publish_logout(server: &Server, player: &Player) {
    if !cluster_enabled(server) {
        return;
    }
    let Some(gid) = player.cluster_gid() else {
        return;
    };
    let _ = submit(server, PresenceCommand::LocalLogout(gid));
    retain_player_handoff(server, player);
}

pub(crate) struct RetainedDeparture {
    watched: Vec<pumpkin_util::math::vector2::Vector2<i32>>,
    world_id: pumpkin_cluster::protocol::WorldId,
    retained_until: u64,
}

pub(crate) fn freeze_player_handoff(player: &Player) -> Option<RetainedDeparture> {
    if !pumpkin_world::level::is_cluster_secondary() {
        return None;
    }
    let watched: Vec<pumpkin_util::math::vector2::Vector2<i32>> =
        player.watched_section.load().all_chunks_within().collect();
    let world = player.world();
    let retained_until = now_millis().saturating_add(LOGOUT_GRACE_MILLIS);
    let world_id = super::cluster_world_delta::world_id(&world);
    world
        .level
        .keep_cluster_chunks_for_relog(watched.clone(), retained_until);
    Some(RetainedDeparture {
        watched,
        world_id,
        retained_until,
    })
}

pub(crate) fn commit_retained_departure(
    server: &Server,
    player: &Player,
    retained: &RetainedDeparture,
    logout: bool,
) -> Option<u64> {
    let watched: Vec<pumpkin_util::math::vector2::Vector2<i32>> =
        player.watched_section.load().all_chunks_within().collect();
    let world = player.world();
    if watched != retained.watched
        || super::cluster_world_delta::world_id(&world) != retained.world_id
    {
        world
            .level
            .keep_cluster_chunks_for_relog(watched.clone(), retained.retained_until);
    }
    super::cluster_handoff::commit_logout(server, player, &watched, retained.retained_until, logout)
}

pub fn retain_player_handoff(server: &Server, player: &Player) {
    if let Some(retained) = freeze_player_handoff(player) {
        let _ = commit_retained_departure(server, player, &retained, true);
    }
}

#[must_use]
pub fn all_presence_entries(server: &Server) -> Vec<(GlobalPlayerId, RemotePlayerEntry)> {
    server
        .cluster_presence
        .snapshot()
        .entries
        .iter()
        .map(|(gid, entry)| (*gid, entry.clone()))
        .collect()
}

#[must_use]
pub fn remote_presence_entries(server: &Server) -> Vec<(GlobalPlayerId, RemotePlayerEntry)> {
    let local = local_server_id(server);
    all_presence_entries(server)
        .into_iter()
        .filter(|(gid, _)| gid.server != local)
        .collect()
}

#[must_use]
pub fn remote_player_in_lobby(server: &Server, name: &str) -> bool {
    remote_presence_entries(server)
        .into_iter()
        .any(|(_, entry)| entry.in_lobby && entry.name.eq_ignore_ascii_case(name))
}

#[must_use]
pub fn remote_presence_entry(server: &Server, gid: GlobalPlayerId) -> Option<RemotePlayerEntry> {
    if gid.server == local_server_id(server) {
        return None;
    }
    server
        .cluster_presence
        .snapshot()
        .entries
        .get(&gid)
        .cloned()
}

pub fn forget_remote_server(server: &Server, peer: u16) {
    let _ = submit(server, PresenceCommand::PeerDropped(ServerId(peer)));
}

#[must_use]
pub fn permission_for_uuid(server: &Server, uuid: uuid::Uuid) -> PermissionLvl {
    server
        .cluster_presence
        .snapshot()
        .permission_for_uuid(uuid.as_bytes())
}

pub fn publish_permission(server: &Server, uuid: uuid::Uuid, level: Option<PermissionLvl>) {
    let _ = submit(
        server,
        PresenceCommand::Permission {
            uuid: *uuid.as_bytes(),
            level,
        },
    );
}

pub fn request_profile_name_lookup(server: &Server, name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }
    if server
        .cluster_presence
        .snapshot()
        .failed_lookup_names
        .get(&name.to_ascii_lowercase())
        .is_some_and(|last| last.elapsed() < Duration::from_secs(60))
    {
        return false;
    }
    submit(server, PresenceCommand::LookupName(name.to_string()))
}

pub fn apply_presence_bytes(
    server: &Arc<Server>,
    local: ServerId,
    peer: ServerId,
    generation: u64,
    bytes: &[u8],
) -> bool {
    let Ok(control) = decode_control(bytes) else {
        return false;
    };
    let claimed = match &control {
        PresenceControl::Login(login) => login.gid.server,
        PresenceControl::Logout(logout) => logout.gid.server,
    };
    if !inbound_control_allowed(
        local,
        peer,
        generation,
        claimed,
        &server.cluster_peer_generations.load(),
        &server.cluster_primary_peers.load(),
    ) {
        return false;
    }
    submit(
        server,
        PresenceCommand::RemoteControl {
            peer,
            generation,
            control,
        },
    )
}

pub async fn presence_task(
    server: Arc<Server>,
    local: ServerId,
    mut presence_rx: mpsc::Receiver<InboundParcel>,
) {
    while let Some(parcel) = presence_rx.recv().await {
        if parcel.header.kind == StreamKind::Control {
            let _ = apply_presence_bytes(
                &server,
                local,
                parcel.peer,
                parcel.generation,
                &parcel.bytes,
            );
        }
    }
}

pub fn spawn_presence_apply(
    server: &Arc<Server>,
    local: ServerId,
    presence_rx: mpsc::Receiver<InboundParcel>,
) {
    server.spawn_task(presence_task(Arc::clone(server), local, presence_rx));
}

fn join_message(name: &str) -> TextComponent {
    TextComponent::translate_cross(
        pumpkin_data::translation::java::MULTIPLAYER_PLAYER_JOINED,
        pumpkin_data::translation::bedrock::MULTIPLAYER_PLAYER_JOINED,
        [TextComponent::text(name.to_string())],
    )
    .color_named(NamedColor::Yellow)
}

fn leave_message(name: &str) -> TextComponent {
    TextComponent::translate_cross(
        pumpkin_data::translation::java::MULTIPLAYER_PLAYER_LEFT,
        pumpkin_data::translation::bedrock::MULTIPLAYER_PLAYER_LEFT,
        [TextComponent::text(name.to_string())],
    )
    .color_named(NamedColor::Yellow)
}

fn broadcast_message(server: &Server, message: &TextComponent) {
    for world in server.worlds.load().iter() {
        world.broadcast_system_message(message, false);
    }
    let _ = server.cluster_lobby_chat_tx.send((None, message.clone()));
}

async fn presence_owner(
    server: Arc<Server>,
    runtime: Arc<PresenceRuntime>,
    mut rx: mpsc::Receiver<PresenceCommand>,
) {
    let mut owner = PresenceOwner::new();
    let mut retry = tokio::time::interval(Duration::from_millis(10));
    loop {
        let command = tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                command
            }
            _ = retry.tick() => {
                owner.set_peers(
                    server
                        .cluster_connected_peers
                        .load()
                        .iter()
                        .map(|peer| peer.0)
                        .collect(),
                );
                let announced = owner.announced;
                owner.flush();
                if announced != owner.announced {
                    owner.publish(&runtime);
                }
                continue;
            }
        };
        owner.set_peers(
            server
                .cluster_connected_peers
                .load()
                .iter()
                .map(|peer| peer.0)
                .collect(),
        );
        let mut published_before_effects = false;
        match command {
            PresenceCommand::Configure {
                local,
                peers: _,
                outbound,
            } => {
                owner.local = local;
                owner.outbound = Some(outbound);
                let peers = owner.peers.clone();
                for peer in peers {
                    owner.publish_peer_state(ServerId(peer));
                }
            }
            PresenceCommand::LocalLogin(login) => {
                if login.gid.server == owner.local && owner.can_insert(&login) {
                    let previous = owner.insert(&login);
                    if login.in_lobby && previous.is_none() {
                        owner.publish(&runtime);
                        published_before_effects = true;
                        super::cluster_hide::push_remote_login_to_viewers(
                            &server,
                            &login.gid,
                            &login.uuid,
                            &login.name,
                            &super::cluster_chat_pm::protocol_properties(&login.properties),
                        );
                        if !owner.hidden_gids.contains(&login.gid)
                            && !owner.hidden_uuids.contains(&login.uuid)
                        {
                            broadcast_message(&server, &join_message(&login.name));
                        }
                    }
                    if let Ok(bytes) = encode_control(&PresenceControl::Login(login)) {
                        owner.broadcast(bytes);
                    }
                }
            }
            PresenceCommand::LocalLogout(gid) => {
                if gid.server == owner.local {
                    let removed = owner.entries.remove(&gid);
                    let hidden = removed.as_ref().is_some_and(|entry| {
                        owner.hidden_gids.contains(&gid) || owner.hidden_uuids.contains(&entry.uuid)
                    });
                    owner.hidden_gids.remove(&gid);
                    if let Some(entry) = removed.as_ref()
                        && entry.in_lobby
                    {
                        owner.publish(&runtime);
                        published_before_effects = true;
                        super::cluster_hide::push_remote_logout_to_viewers(
                            &server,
                            &gid,
                            &entry.uuid,
                            &entry.name,
                        );
                        if !hidden {
                            broadcast_message(&server, &leave_message(&entry.name));
                        }
                    }
                    if removed.is_some()
                        && let Ok(bytes) =
                            encode_control(&PresenceControl::Logout(PresenceLogout::new(gid)))
                    {
                        owner.broadcast(bytes);
                    }
                }
            }
            PresenceCommand::RemoteControl {
                peer,
                generation,
                control,
            } => match control {
                PresenceControl::Login(login)
                    if inbound_control_allowed(
                        owner.local,
                        peer,
                        generation,
                        login.gid.server,
                        &server.cluster_peer_generations.load(),
                        &server.cluster_primary_peers.load(),
                    ) =>
                {
                    if !owner.can_insert(&login) {
                        continue;
                    }
                    let direct = login.gid.server == peer;
                    let previous = owner.insert(&login);
                    let hidden = owner.hidden_gids.contains(&login.gid)
                        || owner.hidden_uuids.contains(&login.uuid);
                    if let Some(previous) = previous.as_ref() {
                        if previous.uuid != login.uuid
                            || previous.name != login.name
                            || previous.properties != login.properties
                            || previous.in_lobby != login.in_lobby
                        {
                            super::cluster_hide::push_remote_logout_to_viewers(
                                &server,
                                &login.gid,
                                &previous.uuid,
                                &previous.name,
                            );
                            super::cluster_hide::push_remote_login_to_viewers(
                                &server,
                                &login.gid,
                                &login.uuid,
                                &login.name,
                                &super::cluster_chat_pm::protocol_properties(&login.properties),
                            );
                        }
                    } else {
                        super::cluster_hide::push_remote_login_to_viewers(
                            &server,
                            &login.gid,
                            &login.uuid,
                            &login.name,
                            &super::cluster_chat_pm::protocol_properties(&login.properties),
                        );
                        if !hidden {
                            broadcast_message(&server, &join_message(&login.name));
                        }
                    }
                    if login.in_lobby {
                        super::cluster_ghost::remove_remote_player_ghost(&server, login.gid);
                    }
                    if is_primary(&server) && previous.is_none() {
                        info!(
                            name = login.name.as_str(),
                            server = peer.0,
                            "cluster player joined"
                        );
                    }
                    if is_primary(&server)
                        && direct
                        && let Ok(bytes) = encode_control(&PresenceControl::Login(login))
                    {
                        owner.broadcast_except(peer, bytes);
                    }
                }
                PresenceControl::Logout(logout)
                    if inbound_control_allowed(
                        owner.local,
                        peer,
                        generation,
                        logout.gid.server,
                        &server.cluster_peer_generations.load(),
                        &server.cluster_primary_peers.load(),
                    ) =>
                {
                    let direct = logout.gid.server == peer;
                    let removed = owner.entries.remove(&logout.gid);
                    if let Some(entry) = removed.as_ref() {
                        let hidden = owner.hidden_gids.remove(&logout.gid)
                            || owner.hidden_uuids.contains(&entry.uuid);
                        super::cluster_hide::push_remote_logout_to_viewers(
                            &server,
                            &logout.gid,
                            &entry.uuid,
                            &entry.name,
                        );
                        super::cluster_ghost::remove_remote_player_ghost(&server, logout.gid);
                        if !hidden {
                            broadcast_message(&server, &leave_message(&entry.name));
                        }
                    }
                    if is_primary(&server)
                        && direct
                        && removed.is_some()
                        && let Ok(bytes) = encode_control(&PresenceControl::Logout(logout))
                    {
                        owner.broadcast_except(peer, bytes);
                    }
                }
                PresenceControl::Login(_) | PresenceControl::Logout(_) => {}
            },
            PresenceCommand::LocalHide(update) => {
                if update.gid.server == owner.local && owner.apply_hide(&update) {
                    owner.publish(&runtime);
                    published_before_effects = true;
                    if let Ok(bytes) = pumpkin_cluster::hide::encode_hide(&update) {
                        owner.broadcast(bytes);
                    }
                    super::cluster_hide::apply_hide_visibility(
                        &server,
                        uuid::Uuid::from_bytes(update.uuid),
                        Some(&update.name),
                        update.hidden,
                    );
                }
            }
            PresenceCommand::RemoteHide {
                peer,
                generation,
                update,
            } => {
                let direct = update.gid.server == peer;
                if inbound_control_allowed(
                    owner.local,
                    peer,
                    generation,
                    update.gid.server,
                    &server.cluster_peer_generations.load(),
                    &server.cluster_primary_peers.load(),
                ) && owner.apply_hide(&update)
                {
                    owner.publish(&runtime);
                    published_before_effects = true;
                    super::cluster_hide::apply_hide_visibility(
                        &server,
                        uuid::Uuid::from_bytes(update.uuid),
                        Some(&update.name),
                        update.hidden,
                    );
                    if is_primary(&server)
                        && direct
                        && let Ok(bytes) = pumpkin_cluster::hide::encode_hide(&update)
                    {
                        owner.broadcast_except(peer, bytes);
                    }
                }
            }
            PresenceCommand::PeerDropped(peer) => {
                for entry in owner.drop_peer(peer) {
                    if is_primary(&server)
                        && let Ok(bytes) =
                            encode_control(&PresenceControl::Logout(PresenceLogout::new(entry.gid)))
                    {
                        owner.broadcast_except(peer, bytes);
                    }
                    super::cluster_hide::push_remote_logout_to_viewers(
                        &server,
                        &entry.gid,
                        &entry.uuid,
                        &entry.name,
                    );
                    super::cluster_ghost::remove_remote_player_ghost(&server, entry.gid);
                    if !owner.hidden_uuids.contains(&entry.uuid) {
                        broadcast_message(&server, &leave_message(&entry.name));
                    }
                }
            }
            PresenceCommand::PublishToPeer(peer) => owner.publish_peer_state(peer),
            PresenceCommand::Permission { uuid, level } => match level {
                Some(level) => {
                    owner.permissions.insert(uuid, level);
                    owner.permission_revision = permission_fingerprint(&owner.permissions);
                }
                None => {
                    owner.permissions.remove(&uuid);
                    owner.permission_revision = permission_fingerprint(&owner.permissions);
                }
            },
            PresenceCommand::ReplacePermissions(permissions) => {
                owner.permissions = permissions;
                owner.permission_revision = permission_fingerprint(&owner.permissions);
            }
            PresenceCommand::LookupName(name) => {
                let normalized = name.to_ascii_lowercase();
                owner
                    .failed_lookups
                    .retain(|_, last| last.elapsed() < Duration::from_secs(60));
                if owner.pending_lookups.len() < 64
                    && owner.failed_lookups.len() < 64
                    && !owner.failed_lookups.contains_key(&normalized)
                    && !owner
                        .known_profiles
                        .values()
                        .any(|known| known.eq_ignore_ascii_case(&name))
                    && owner.pending_lookups.insert(normalized.clone())
                {
                    let tx = runtime.tx.clone();
                    let auth = server
                        .advanced_config
                        .networking
                        .java
                        .authentication
                        .clone();
                    tokio::spawn(async move {
                        let resolved =
                            crate::net::authentication::lookup_profile_by_name(&name, &auth)
                                .await
                                .ok()
                                .flatten()
                                .map(|(uuid, name)| (*uuid.as_bytes(), name));
                        let _ = tx
                            .send(PresenceCommand::LookupResult(normalized, resolved))
                            .await;
                    });
                }
            }
            PresenceCommand::LookupResult(name, result) => {
                owner.pending_lookups.remove(&name);
                if let Some((uuid, resolved_name)) = result {
                    if owner.known_profiles.len() < PRESENCE_MAX_ENTRIES
                        || owner.known_profiles.contains_key(&uuid)
                    {
                        owner.known_profiles.insert(uuid, resolved_name);
                    }
                } else {
                    owner.failed_lookups.insert(name, std::time::Instant::now());
                }
            }
        }
        let announced_before_flush = owner.announced;
        owner.flush();
        if !published_before_effects || announced_before_flush != owner.announced {
            owner.publish(&runtime);
        }
        super::cluster_status::refresh_cluster_status(&server);
    }
    debug!("cluster presence owner closed");
}

pub fn spawn_presence_owner(server: &Arc<Server>, rx: mpsc::Receiver<PresenceCommand>) {
    let runtime = Arc::new(PresenceRuntime {
        snapshot: Arc::clone(&server.cluster_presence.snapshot),
        tx: server.cluster_presence.tx.clone(),
    });
    server.spawn_task(presence_owner(Arc::clone(server), runtime, rx));
}

#[must_use]
pub fn admin_directory_snapshot(server: &Server) -> PlayerDirectorySnapshot {
    let local = local_server_id(server);
    let mut snapshot = PlayerDirectorySnapshot::new(local);
    for (gid, entry) in all_presence_entries(server) {
        if snapshot.player_name(&gid).is_none() && snapshot.locate_by_name(&entry.name).is_none() {
            snapshot.apply_presence_login(gid, entry.name);
        }
    }
    for player in server.get_all_players() {
        let gid = player
            .cluster_gid()
            .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(u16::MAX)));
        if snapshot.player_name(&gid).is_none() {
            snapshot.apply_presence_login(gid, player.gameprofile.name.clone());
        }
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn permission_stamp_converges_across_arrival_order_and_detects_revoke() {
        let a = [1; 16];
        let b = [2; 16];
        let first = HashMap::from([(a, PermissionLvl::Four), (b, PermissionLvl::Two)]);
        let second = HashMap::from([(b, PermissionLvl::Two), (a, PermissionLvl::Four)]);
        assert_eq!(
            permission_fingerprint(&first),
            permission_fingerprint(&second)
        );
        let revoked = HashMap::from([(b, PermissionLvl::Two)]);
        assert_ne!(
            permission_fingerprint(&first),
            permission_fingerprint(&revoked)
        );
    }

    fn login(server: u16, player: u16, lobby: bool) -> PresenceLogin {
        PresenceLogin::new(
            gid(server, player),
            [player as u8; 16],
            format!("Player{player}"),
            vec![PresenceProperty::new(
                String::from("textures"),
                format!("texture-{player}"),
                Some(format!("signature-{player}")),
            )],
            lobby,
        )
    }

    #[test]
    fn owner_state_preserves_profiles_and_visibility_across_transitions() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(2);
        let waiting = login(2, 7, true).with_listing(false);
        assert!(owner.insert(&waiting).is_none());
        assert_eq!(owner.entries[&waiting.gid].properties, waiting.properties);
        assert!(!owner.entries[&waiting.gid].listed);
        let joined = login(2, 7, false);
        let previous = owner.insert(&joined).unwrap();
        assert!(previous.in_lobby);
        assert!(!previous.listed);
        assert!(!owner.entries[&joined.gid].in_lobby);
        assert!(owner.entries[&joined.gid].listed);
        let hide = HideUpdate::new(joined.gid, joined.uuid, joined.name.clone(), true);
        assert!(owner.apply_hide(&hide));
        assert!(owner.hidden_gids.contains(&joined.gid));
        assert!(owner.hidden_uuids.contains(&joined.uuid));
        let show = HideUpdate::new(joined.gid, joined.uuid, joined.name.clone(), false);
        assert!(owner.apply_hide(&show));
        assert!(!owner.hidden_gids.contains(&joined.gid));
        assert!(!owner.hidden_uuids.contains(&joined.uuid));
    }

    #[test]
    fn dropped_peer_removal_can_be_derived_without_touching_other_hosts() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(1);
        for entry in [login(2, 1, false), login(2, 2, true), login(3, 1, false)] {
            owner.insert(&entry);
        }
        let removed = owner.drop_peer(ServerId(2));
        assert_eq!(removed.len(), 2);
        assert_eq!(owner.entries.len(), 1);
        assert!(owner.entries.contains_key(&gid(3, 1)));
    }

    #[test]
    fn peer_connect_after_lobby_login_receives_full_profile_without_admission_event() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(1);
        let profile = login(1, 7, true);
        owner.insert(&profile);
        owner.set_peers(vec![2]);
        let sent = owner
            .pending
            .values()
            .find(|parcel| parcel.peer == ServerId(2))
            .unwrap();
        let PresenceControl::Login(received) = decode_control(&sent.bytes).unwrap() else {
            panic!("expected profile login");
        };
        assert_eq!(received.gid, profile.gid);
        assert_eq!(received.properties, profile.properties);
    }

    #[test]
    fn newcomer_receives_relayed_profiles_before_hidden_state() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(1);
        owner.peers = vec![3];
        let remote = login(2, 7, true);
        owner.insert(&remote);
        owner.apply_hide(&HideUpdate::new(
            remote.gid,
            remote.uuid,
            remote.name.clone(),
            true,
        ));
        owner.publish_peer_state(ServerId(3));
        assert_eq!(owner.pending.len(), 2);
        let (tx, mut rx) = mpsc::channel(2);
        owner.outbound = Some(tx);
        owner.flush();
        let profile = rx.try_recv().unwrap();
        let hidden = rx.try_recv().unwrap();
        assert_eq!(profile.peer, ServerId(3));
        assert_eq!(hidden.peer, ServerId(3));
        assert_eq!(
            decode_control(&profile.bytes).unwrap(),
            PresenceControl::Login(remote.clone())
        );
        assert_eq!(
            pumpkin_cluster::hide::decode_hide(&hidden.bytes).unwrap(),
            HideUpdate::new(remote.gid, remote.uuid, remote.name, true)
        );
    }

    #[test]
    fn saturated_transport_retains_distinct_states_and_coalesces_replacement() {
        let mut owner = PresenceOwner::new();
        owner.peers = vec![9];
        let (tx, mut rx) = mpsc::channel(1);
        owner.outbound = Some(tx);
        let entries: Vec<_> = (1..=256).map(|slot| login(1, slot, false)).collect();
        for entry in &entries {
            owner.queue_to(
                ServerId(9),
                encode_control(&PresenceControl::Login(entry.clone())).unwrap(),
            );
        }
        owner.flush();
        assert_eq!(owner.pending.len(), 255);
        let replacement = PresenceLogout::new(entries.last().unwrap().gid);
        owner.queue_to(
            ServerId(9),
            encode_control(&PresenceControl::Logout(replacement)).unwrap(),
        );
        assert_eq!(owner.pending.len(), 255);
        let mut delivered = vec![decode_control(&rx.try_recv().unwrap().bytes).unwrap()];
        while !owner.pending.is_empty() {
            owner.flush();
            delivered.push(decode_control(&rx.try_recv().unwrap().bytes).unwrap());
        }
        assert_eq!(delivered.len(), 256);
        assert_eq!(
            delivered.last(),
            Some(&PresenceControl::Logout(replacement))
        );
    }

    #[test]
    fn peer_drop_discards_saturated_state_before_replacement_publish() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(1);
        owner.peers = vec![2];
        let current = login(1, 7, false);
        owner.insert(&current);
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(OutboundParcel {
            peer: ServerId(99),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: vec![0],
        })
        .unwrap();
        owner.outbound = Some(tx);
        owner.queue_to(
            ServerId(2),
            encode_control(&PresenceControl::Login(current.clone())).unwrap(),
        );
        owner.queue_to(
            ServerId(2),
            pumpkin_cluster::hide::encode_hide(&HideUpdate::new(
                current.gid,
                current.uuid,
                current.name.clone(),
                true,
            ))
            .unwrap(),
        );
        owner.queue_to(
            ServerId(2),
            encode_control(&PresenceControl::Logout(PresenceLogout::new(current.gid))).unwrap(),
        );
        owner.flush();
        assert_eq!(owner.pending.len(), 2);
        owner.drop_peer(ServerId(2));
        assert!(owner.pending.is_empty());
        assert!(owner.pending_order.is_empty());
        rx.try_recv().unwrap();
        let replacement_current = current.clone().with_listing(false);
        owner.insert(&replacement_current);
        owner.peers.push(2);
        owner.publish_peer_state(ServerId(2));
        owner.flush();
        let replacement = rx.try_recv().unwrap();
        assert_eq!(
            decode_control(&replacement.bytes).unwrap(),
            PresenceControl::Login(replacement_current)
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn superseded_direct_and_primary_relay_generations_are_rejected() {
        let local = ServerId(1);
        let direct = ServerId(2);
        let primary = ServerId(3);
        let relayed_origin = ServerId(4);
        let generations = HashMap::from([(direct, 7), (primary, 11)]);
        let primaries = HashSet::from([primary]);
        assert!(inbound_control_allowed(
            local,
            direct,
            7,
            direct,
            &generations,
            &primaries
        ));
        assert!(!inbound_control_allowed(
            local,
            direct,
            6,
            direct,
            &generations,
            &primaries
        ));
        assert!(inbound_control_allowed(
            local,
            primary,
            11,
            relayed_origin,
            &generations,
            &primaries
        ));
        assert!(!inbound_control_allowed(
            local,
            primary,
            10,
            relayed_origin,
            &generations,
            &primaries
        ));
        assert!(!inbound_control_allowed(
            local,
            direct,
            7,
            relayed_origin,
            &generations,
            &primaries
        ));
    }

    #[test]
    fn hiding_removes_profile_from_other_rosters_and_restoration_returns_it() {
        let mut owner = PresenceOwner::new();
        let hidden = login(1, 1, false);
        let viewer = login(1, 2, false);
        owner.insert(&hidden);
        owner.insert(&viewer);
        let (runtime, _rx) = PresenceRuntime::new(1);
        owner.publish(&runtime);
        assert!(
            runtime
                .snapshot()
                .is_visible_to(&hidden.gid, &hidden.uuid, Some(&viewer.uuid))
        );
        owner.apply_hide(&HideUpdate::new(
            hidden.gid,
            hidden.uuid,
            hidden.name.clone(),
            true,
        ));
        owner.publish(&runtime);
        let during = runtime.snapshot();
        assert!(!during.is_visible_to(&hidden.gid, &hidden.uuid, Some(&viewer.uuid)));
        assert!(during.is_visible_to(&hidden.gid, &hidden.uuid, Some(&hidden.uuid)));
        owner.apply_hide(&HideUpdate::new(
            hidden.gid,
            hidden.uuid,
            hidden.name,
            false,
        ));
        owner.publish(&runtime);
        assert!(
            runtime
                .snapshot()
                .is_visible_to(&hidden.gid, &hidden.uuid, Some(&viewer.uuid))
        );
    }
}
