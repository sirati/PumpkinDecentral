use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use pumpkin_cluster::admin_sync::PlayerDirectorySnapshot;
use pumpkin_cluster::hide::HideUpdate;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::ordered_presence::{
    OrderedPresence, PresenceDisposition, PresenceOrdering, PresencePayload,
};
use pumpkin_cluster::presence::{
    PRESENCE_MAX_ENTRIES, PresenceLogin, PresenceLogout, PresenceProperty, RemotePlayerEntry,
    is_valid_presence_name,
};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::realm_completion::PeerGeneration;
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
    if let Some(envelope) = pumpkin_cluster::ordered_presence::decode(&parcel.bytes) {
        let facet = if matches!(envelope.payload, PresencePayload::Hide { .. }) {
            1
        } else {
            0
        };
        return Some((parcel.peer, facet, envelope.gid()));
    }
    None
}

#[derive(Debug, Clone, Default)]
pub struct PresenceSnapshot {
    pub names: HashMap<String, Option<GlobalPlayerId>>,
    pub uuids: HashMap<[u8; 16], Option<GlobalPlayerId>>,
    pub visible_uuids: HashSet<[u8; 16]>,
    pub revision: u64,
    pub roster_order: Arc<[GlobalPlayerId]>,
    pub roster_images: Arc<HashMap<GlobalPlayerId, Arc<RemotePlayerEntry>>>,
    pub entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
    pub retired_gids: HashSet<GlobalPlayerId>,
    pub retirement_sessions: HashMap<GlobalPlayerId, (PeerGeneration, u64)>,
    pub known_profiles: HashMap<[u8; 16], String>,
    pub failed_lookup_names: HashMap<String, std::time::Instant>,
    pub hidden_gids: HashSet<GlobalPlayerId>,
    pub hidden_uuids: HashSet<[u8; 16]>,
    pub permissions: HashMap<[u8; 16], PermissionLvl>,
    pub permission_revision: u64,
    pub announced: u64,
}

impl PresenceSnapshot {
    pub fn uuid_visible_to(&self, uuid: &[u8; 16], viewer: &[u8; 16]) -> bool {
        self.visible_uuids.contains(uuid) || uuid == viewer && self.uuids.contains_key(uuid)
    }

    pub fn by_uuid(&self, uuid: &[u8; 16]) -> Option<&RemotePlayerEntry> {
        self.uuids.get(uuid).copied().flatten().and_then(|gid| self.entries.get(&gid))
    }

    pub fn by_name(&self, name: &str) -> Option<GlobalPlayerId> {
        self.names.get(&name.to_ascii_lowercase()).copied().flatten()
    }

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
    LocalLogin(PresenceLogin, u64),
    LocalLogout(GlobalPlayerId, [u8; 16], u64),
    LocalDeparture {
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        session: u64,
        message: Option<TextComponent>,
    },
    LocalHide(HideUpdate, u64),
    RemoteOrdered {
        peer: ServerId,
        generation: u64,
        envelope: OrderedPresence,
    },
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
    transport_generations: HashMap<ServerId, u64>,
    outbound: Option<mpsc::Sender<OutboundParcel>>,
    entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
    retired_gids: HashSet<GlobalPlayerId>,
    ordering: PresenceOrdering,
    retirement_sessions: HashMap<GlobalPlayerId, (PeerGeneration, u64)>,
    deferred: HashMap<(PeerGeneration, GlobalPlayerId, u8), AuthenticatedPresence>,
    deferred_order: VecDeque<(PeerGeneration, GlobalPlayerId, u8)>,
    known_profiles: HashMap<[u8; 16], String>,
    pending_lookups: HashSet<String>,
    failed_lookups: HashMap<String, std::time::Instant>,
    hidden_gids: HashSet<GlobalPlayerId>,
    hidden_uuids: HashSet<[u8; 16]>,
    permissions: HashMap<[u8; 16], PermissionLvl>,
    permission_revision: u64,
    pending: HashMap<PendingKey, OutboundParcel>,
    pending_order: VecDeque<PendingKey>,
    pending_roster: HashSet<GlobalPlayerId>,
    roster_images: HashMap<GlobalPlayerId, Arc<RemotePlayerEntry>>,
    revision: u64,
    announced: u64,
}

impl PresenceOwner {
    fn new() -> Self {
        Self {
            local: ServerId(0),
            peers: Vec::new(),
            transport_generations: HashMap::new(),
            outbound: None,
            entries: HashMap::new(),
            retired_gids: HashSet::new(),
            ordering: PresenceOrdering::default(),
            retirement_sessions: HashMap::new(),
            deferred: HashMap::new(),
            deferred_order: VecDeque::new(),
            known_profiles: HashMap::new(),
            pending_lookups: HashSet::new(),
            failed_lookups: HashMap::new(),
            hidden_gids: HashSet::new(),
            hidden_uuids: HashSet::new(),
            permissions: HashMap::new(),
            permission_revision: 0,
            pending: HashMap::new(),
            pending_order: VecDeque::new(),
            pending_roster: HashSet::new(),
            roster_images: HashMap::new(),
            revision: 0,
            announced: 0,
        }
    }

    fn publish(&mut self, runtime: &PresenceRuntime) {
        self.roster_images.retain(|gid, _| self.entries.contains_key(gid));
        for (gid, entry) in &self.entries {
            if self.roster_images.get(gid).is_none_or(|old| old.as_ref() != entry) {
                self.roster_images.insert(*gid, Arc::new(entry.clone()));
            }
        }
        self.revision = self.revision.wrapping_add(1);
        runtime.snapshot.store(Arc::new(PresenceSnapshot {
            names: {
                let mut names = HashMap::with_capacity(self.entries.len());
                for (gid, entry) in &self.entries {
                    names.entry(entry.name.to_ascii_lowercase()).and_modify(|current| *current = None).or_insert(Some(*gid));
                }
                names
            },
            uuids: {
                let mut uuids = HashMap::with_capacity(self.entries.len());
                for (gid, entry) in &self.entries {
                    uuids.entry(entry.uuid).and_modify(|current| *current = None).or_insert(Some(*gid));
                }
                uuids
            },
            visible_uuids: self.entries.values().filter(|entry| !self.hidden_gids.contains(&entry.gid) && !self.hidden_uuids.contains(&entry.uuid)).map(|entry| entry.uuid).collect(),
            revision: self.revision,
            roster_order: self.entries.keys().copied().collect(),
            roster_images: Arc::new(self.roster_images.clone()),
            entries: self.entries.clone(),
            retired_gids: self.retired_gids.clone(),
            retirement_sessions: self.retirement_sessions.clone(),
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

    fn update_transport_generations(&mut self, generations: &HashMap<ServerId, u64>) {
        let changed: Vec<_> = self
            .peers
            .iter()
            .copied()
            .map(ServerId)
            .filter(|peer| {
                generations.get(peer).is_some_and(|generation| {
                    self.transport_generations.get(peer) != Some(generation)
                })
            })
            .collect();
        self.transport_generations = generations
            .iter()
            .filter(|(peer, _)| self.peers.contains(&peer.0))
            .map(|(peer, generation)| (*peer, *generation))
            .collect();
        for peer in changed {
            self.purge_target(peer);
            self.publish_peer_state(peer);
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
        self.retired_gids.remove(&login.gid);
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
            )
            .with_native_skin(login.native_skin),
        );
        previous
    }

    fn remove(&mut self, gid: GlobalPlayerId) -> Option<RemotePlayerEntry> {
        self.retired_gids.insert(gid);
        self.pending_roster.insert(gid);
        self.entries.remove(&gid)
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
        for envelope in self.ordering.snapshots() {
            if envelope.source.server == peer {
                continue;
            }
            if let Some(bytes) = pumpkin_cluster::ordered_presence::encode(&envelope) {
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
            self.remove(entry.gid);
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
pub fn login_parcel_for_peer(envelope: &OrderedPresence, peer: ServerId) -> Option<OutboundParcel> {
    if !matches!(envelope.payload, PresencePayload::Profile { .. }) {
        return None;
    }
    Some(OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes: pumpkin_cluster::ordered_presence::encode(envelope)?,
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
    native_skin: bool,
    session: u64,
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
            .with_listing(listed)
            .with_native_skin(native_skin),
            session,
        ),
    );
}

pub fn publish_logout_gid(server: &Server, gid: GlobalPlayerId, uuid: [u8; 16], session: u64) {
    let _ = submit(server, PresenceCommand::LocalLogout(gid, uuid, session));
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
    .with_listing(player.config.load().server_listing)
    .with_native_skin(matches!(
        player.client.as_ref(),
        crate::net::ClientPlatform::Bedrock(_)
    ));
    if submit(
        server,
        PresenceCommand::LocalLogin(
            login,
            player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
        ),
    ) {
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
            .with_listing(player.config.load().server_listing)
            .with_native_skin(matches!(
                player.client.as_ref(),
                crate::net::ClientPlatform::Bedrock(_)
            )),
            player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
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
    let _ = submit(
        server,
        PresenceCommand::LocalLogout(
            gid,
            *player.gameprofile.id.as_bytes(),
            player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
        ),
    );
}

pub(crate) struct RetainedDeparture {
    pub(crate) chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
    pub(crate) retained_until: u64,
}

pub(crate) fn freeze_player_handoff(server: &Server, player: &Player) -> Option<RetainedDeparture> {
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let watches = server.cluster_chunk_watches.load();
    let chunks = watches
        .get(&lease.gid)
        .filter(|watch| watch.session == lease.incarnation)
        .map(|watch| watch.delivered.iter().copied().collect())
        .unwrap_or_default();
    Some(RetainedDeparture {
        chunks,
        retained_until: now_millis().saturating_add(LOGOUT_GRACE_MILLIS),
    })
}

pub(crate) fn commit_retained_departure(
    server: &Server,
    player: &Player,
    retained: &RetainedDeparture,
    logout: bool,
    frozen: &super::cluster_controller::FrozenPlayerDeparture,
) -> Option<u64> {
    super::cluster_handoff::commit_logout(
        server,
        player,
        &retained.chunks,
        retained.retained_until,
        logout,
        frozen,
    )
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
    let Some(envelope) = pumpkin_cluster::ordered_presence::decode(bytes) else {
        return false;
    };
    let input = AuthenticatedPresence {
        peer,
        generation,
        envelope,
    };
    if authenticate_ordered(
        local,
        &input,
        &server.cluster_peer_generations.load(),
        &server.cluster_peer_incarnations.load(),
    ) == PresenceAuthentication::Rejected
    {
        return false;
    }
    submit(
        server,
        PresenceCommand::RemoteOrdered {
            peer,
            generation,
            envelope: input.envelope,
        },
    )
}

fn receive_presence_parcel(
    server: &Arc<Server>,
    local: ServerId,
    parcel: InboundParcel,
) -> Result<(), InboundParcel> {
    if parcel.header.kind != StreamKind::Control {
        return Ok(());
    }
    let Some(envelope) = pumpkin_cluster::ordered_presence::decode(&parcel.bytes) else {
        return Ok(());
    };
    let input = AuthenticatedPresence {
        peer: parcel.peer,
        generation: parcel.generation,
        envelope,
    };
    if authenticate_ordered(
        local,
        &input,
        &server.cluster_peer_generations.load(),
        &server.cluster_peer_incarnations.load(),
    ) == PresenceAuthentication::Rejected
    {
        return Ok(());
    }
    if submit(
        server,
        PresenceCommand::RemoteOrdered {
            peer: parcel.peer,
            generation: parcel.generation,
            envelope: input.envelope,
        },
    ) {
        Ok(())
    } else {
        Err(parcel)
    }
}

pub async fn presence_task(
    server: Arc<Server>,
    local: ServerId,
    mut presence_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut pending = None;
    let mut retry = tokio::time::interval(Duration::from_millis(10));
    loop {
        let parcel = tokio::select! {
            parcel = presence_rx.recv(), if pending.is_none() => { let Some(parcel) = parcel else { break; }; parcel },
            _ = retry.tick(), if pending.is_some() => pending.take().unwrap(),
        };
        pending = receive_presence_parcel(&server, local, parcel).err();
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

#[derive(Clone)]
struct AuthenticatedPresence {
    peer: ServerId,
    generation: u64,
    envelope: OrderedPresence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PresenceAuthentication {
    Accepted,
    Deferred,
    Rejected,
}

fn authenticate_ordered(
    local: ServerId,
    input: &AuthenticatedPresence,
    generations: &HashMap<ServerId, u64>,
    incarnations: &HashMap<ServerId, u64>,
) -> PresenceAuthentication {
    if input.peer == local || input.generation == 0 || !input.envelope.valid() {
        return PresenceAuthentication::Rejected;
    }
    match generations.get(&input.peer) {
        None => return PresenceAuthentication::Deferred,
        Some(generation) if *generation < input.generation => {
            return PresenceAuthentication::Deferred;
        }
        Some(generation) if *generation > input.generation => {
            return PresenceAuthentication::Rejected;
        }
        _ => {}
    }
    match incarnations.get(&input.envelope.source.server) {
        Some(incarnation) if *incarnation == input.envelope.source.generation => {
            PresenceAuthentication::Accepted
        }
        Some(_) => PresenceAuthentication::Rejected,
        None => PresenceAuthentication::Deferred,
    }
}

struct PresenceTransition {
    envelope: OrderedPresence,
    previous: Option<RemotePlayerEntry>,
    current: Option<RemotePlayerEntry>,
    previous_hidden: bool,
    hidden: bool,
    replaced_session: bool,
}

impl PresenceOwner {
    fn defer(&mut self, input: AuthenticatedPresence) -> Result<(), AuthenticatedPresence> {
        let facet = match input.envelope.payload {
            PresencePayload::Profile { .. } => 0,
            PresencePayload::Hide { .. } => 1,
            _ => 2,
        };
        let key = (input.envelope.source, input.envelope.gid(), facet);
        if let Some(old) = self.deferred.get_mut(&key) {
            if (input.envelope.session, input.envelope.revision)
                > (old.envelope.session, old.envelope.revision)
            {
                *old = input;
            }
            return Ok(());
        }
        if self.deferred.len() >= PENDING_STATES_PER_PEER {
            return Err(input);
        }
        self.deferred.insert(key, input);
        self.deferred_order.push_back(key);
        Ok(())
    }

    fn apply_ordered(&mut self, envelope: &OrderedPresence) -> Option<PresenceTransition> {
        let gid = envelope.gid();
        if self
            .retirement_sessions
            .get(&gid)
            .is_some_and(|(source, session)| {
                *source == envelope.source && envelope.session <= *session
            })
        {
            return None;
        }
        if let PresencePayload::Profile { login, .. } = &envelope.payload {
            if !self.can_insert(login) {
                return None;
            }
        }
        if let PresencePayload::Logout(PresenceLogout {
            announcement: pumpkin_cluster::presence::DepartureAnnouncement::CustomJson(json),
            ..
        }) = &envelope.payload
        {
            if serde_json::from_str::<TextComponent>(json).is_err() {
                return None;
            }
        }
        let previous = self.entries.get(&gid).cloned();
        let previous_hidden = previous.as_ref().is_some_and(|entry| {
            self.hidden_gids.contains(&gid) || self.hidden_uuids.contains(&entry.uuid)
        });
        let replaced_session = previous.as_ref().is_some_and(|entry| {
            entry.session != envelope.session || entry.source != Some(envelope.source)
        });
        let mut disposition = self.ordering.observe(envelope);
        if disposition == PresenceDisposition::Ignored
            && previous.is_none()
            && matches!(envelope.payload, PresencePayload::Profile { .. })
            && self.ordering.session(gid) == Some(envelope.session)
            && self.ordering.profile(gid) == Some(envelope)
        {
            disposition = PresenceDisposition::Profile { hidden: None };
        }
        if disposition == PresenceDisposition::Ignored {
            return None;
        }
        if replaced_session {
            self.entries.remove(&gid);
            self.hidden_gids.remove(&gid);
        }
        match (&envelope.payload, disposition) {
            (PresencePayload::Profile { login, .. }, PresenceDisposition::Profile { hidden }) => {
                self.insert(login);
                if let Some(entry) = self.entries.get_mut(&gid) {
                    entry.session = envelope.session;
                    entry.source = Some(envelope.source);
                }
                if let Some(hidden) = hidden {
                    if hidden {
                        self.hidden_gids.insert(gid);
                        self.hidden_uuids.insert(envelope.uuid);
                    } else {
                        self.hidden_gids.remove(&gid);
                        self.hidden_uuids.remove(&envelope.uuid);
                    }
                }
                self.retirement_sessions.remove(&gid);
            }
            (PresencePayload::Hide { name, hidden, .. }, PresenceDisposition::Hide) => {
                self.apply_hide(&HideUpdate::new(gid, envelope.uuid, name.clone(), *hidden));
                self.retired_gids.remove(&gid);
                self.retirement_sessions.remove(&gid);
            }
            (_, PresenceDisposition::Retired) => {
                self.remove(gid);
                self.hidden_gids.remove(&gid);
                self.retirement_sessions
                    .insert(gid, (envelope.source, envelope.session));
            }
            _ => return None,
        }
        let current = self.entries.get(&gid).cloned();
        let hidden = self.hidden_gids.contains(&gid) || self.hidden_uuids.contains(&envelope.uuid);
        self.pending_roster.insert(gid);
        Some(PresenceTransition {
            envelope: envelope.clone(),
            previous,
            current,
            previous_hidden,
            hidden,
            replaced_session,
        })
    }
}

fn present_presence(server: &Server, transition: &PresenceTransition) {
    let gid = transition.envelope.gid();
    match &transition.envelope.payload {
        PresencePayload::Profile { login, .. } => {
            if let Some(previous) = &transition.previous {
                if transition.replaced_session || transition.current.as_ref() != Some(previous) {
                    super::cluster_hide::push_remote_logout_to_viewers(
                        server,
                        &gid,
                        &previous.uuid,
                        &previous.name,
                    );
                    if transition.replaced_session && !transition.previous_hidden {
                        broadcast_message(server, &leave_message(&previous.name));
                    }
                    super::cluster_hide::push_remote_login_to_viewers(
                        server,
                        &gid,
                        &login.uuid,
                        &login.name,
                        &super::cluster_chat_pm::protocol_properties(&login.properties),
                    );
                }
            } else {
                super::cluster_hide::push_remote_login_to_viewers(
                    server,
                    &gid,
                    &login.uuid,
                    &login.name,
                    &super::cluster_chat_pm::protocol_properties(&login.properties),
                );
            }
            if (transition.previous.is_none() || transition.replaced_session) && !transition.hidden
            {
                broadcast_message(server, &join_message(&login.name));
            }
            if transition.previous.is_some()
                && !transition.replaced_session
                && transition.previous_hidden != transition.hidden
            {
                super::cluster_hide::apply_hide_visibility(
                    server,
                    uuid::Uuid::from_bytes(login.uuid),
                    Some(&login.name),
                    transition.hidden,
                );
            }
            if is_primary(server) && (transition.previous.is_none() || transition.replaced_session)
            {
                info!(
                    name = login.name.as_str(),
                    server = login.gid.server.0,
                    "cluster player joined"
                );
            }
        }
        PresencePayload::Hide { name, hidden, .. } => {
            if transition.replaced_session {
                if let Some(previous) = &transition.previous {
                    super::cluster_hide::push_remote_logout_to_viewers(
                        server,
                        &gid,
                        &previous.uuid,
                        &previous.name,
                    );
                    if !transition.previous_hidden {
                        broadcast_message(server, &leave_message(&previous.name));
                    }
                }
            } else if transition.current.is_some() && transition.previous_hidden != *hidden {
                super::cluster_hide::apply_hide_visibility(
                    server,
                    uuid::Uuid::from_bytes(transition.envelope.uuid),
                    Some(name),
                    *hidden,
                );
            }
        }
        PresencePayload::Logout(_) | PresencePayload::Retired { .. } => {
            if let Some(previous) = &transition.previous {
                super::cluster_hide::push_remote_logout_to_viewers(
                    server,
                    &gid,
                    &previous.uuid,
                    &previous.name,
                );
                if !transition.previous_hidden {
                    match &transition.envelope.payload {
                        PresencePayload::Logout(PresenceLogout {
                            announcement:
                                pumpkin_cluster::presence::DepartureAnnouncement::Suppressed,
                            ..
                        }) => {}
                        PresencePayload::Logout(PresenceLogout {
                            announcement:
                                pumpkin_cluster::presence::DepartureAnnouncement::CustomJson(json),
                            ..
                        }) => {
                            if let Ok(message) = serde_json::from_str::<TextComponent>(json) {
                                broadcast_message(server, &message);
                            }
                        }
                        _ => broadcast_message(server, &leave_message(&previous.name)),
                    }
                }
            }
        }
    }
}

async fn presence_owner(
    server: Arc<Server>,
    runtime: Arc<PresenceRuntime>,
    mut rx: mpsc::Receiver<PresenceCommand>,
) {
    let mut owner = PresenceOwner::new();
    owner.local = local_server_id(&server);
    let mut retry = tokio::time::interval(Duration::from_millis(10));
    let mut held_local = None;
    let mut held_remote = None;
    let mut warning = None::<std::time::Instant>;
    loop {
        let pending_roster = owner.pending_roster.iter().copied().take(256).collect::<Vec<_>>();
        for gid in pending_roster {
            if super::cluster_roster::changed(&server, gid) { owner.pending_roster.remove(&gid); } else { break; }
        }
        let local_source = server
            .cluster_peer_incarnations
            .load()
            .get(&owner.local)
            .copied()
            .filter(|boot| *boot != 0)
            .map(|generation| PeerGeneration {
                server: owner.local,
                generation,
            });
        let command = if held_local.is_some() && local_source.is_some() {
            held_local.take().unwrap()
        } else {
            tokio::select! {
                command = rx.recv(), if held_local.is_none() && held_remote.is_none() => { let Some(command) = command else { break; }; command }
                _ = retry.tick() => {
                    owner.set_peers(server.cluster_connected_peers.load().iter().map(|peer| peer.0).collect());
                    owner.update_transport_generations(&server.cluster_peer_generations.load());
                    for _ in 0..256 {
                        let Some(key) = owner.deferred_order.pop_front() else { break; };
                        let Some(input) = owner.deferred.remove(&key) else { continue; };
                        match authenticate_ordered(owner.local, &input, &server.cluster_peer_generations.load(), &server.cluster_peer_incarnations.load()) {
                            PresenceAuthentication::Accepted => {
                                if let Some(transition) = owner.apply_ordered(&input.envelope) { owner.publish(&runtime); present_presence(&server, &transition); }
                            }
                            PresenceAuthentication::Deferred => { owner.deferred.insert(key, input); owner.deferred_order.push_back(key); }
                            PresenceAuthentication::Rejected => {},
                        }
                    }
                    if let Some(input) = held_remote.take() { held_remote = owner.defer(input).err(); }
                    if held_remote.is_some() {
                        let now = std::time::Instant::now();
                        if warning.is_none_or(|last| now.duration_since(last).as_secs() >= 300) { warning = Some(now); tracing::warn!("ordered presence deferral capacity exhausted"); }
                    }
                    let announced = owner.announced;
                    owner.flush();
                    if announced != owner.announced { owner.publish(&runtime); }
                    super::cluster_status::refresh_cluster_status(&server);
                    continue;
                }
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
        owner.update_transport_generations(&server.cluster_peer_generations.load());
        if matches!(
            command,
            PresenceCommand::LocalLogin(..)
                | PresenceCommand::LocalLogout(..)
                | PresenceCommand::LocalDeparture { .. }
                | PresenceCommand::LocalHide(..)
        ) && local_source.is_none()
        {
            held_local = Some(command);
            continue;
        }
        match command {
            PresenceCommand::Configure {
                local,
                peers: _,
                outbound,
            } => {
                owner.local = local;
                owner.outbound = Some(outbound);
                for peer in owner.peers.clone() {
                    owner.publish_peer_state(ServerId(peer));
                }
            }
            local @ (PresenceCommand::LocalLogin(..)
            | PresenceCommand::LocalLogout(..)
            | PresenceCommand::LocalDeparture { .. }
            | PresenceCommand::LocalHide(..)) => {
                let (session, uuid, payload) = match local {
                    PresenceCommand::LocalLogin(login, session) => {
                        let hidden = owner.hidden_gids.contains(&login.gid)
                            || owner.hidden_uuids.contains(&login.uuid);
                        (
                            session,
                            login.uuid,
                            PresencePayload::Profile { login, hidden },
                        )
                    }
                    PresenceCommand::LocalHide(update, session) => (
                        session,
                        update.uuid,
                        PresencePayload::Hide {
                            gid: update.gid,
                            name: update.name,
                            hidden: update.hidden,
                        },
                    ),
                    PresenceCommand::LocalLogout(gid, uuid, session) => (
                        session,
                        uuid,
                        PresencePayload::Logout(PresenceLogout::new(gid)),
                    ),
                    PresenceCommand::LocalDeparture {
                        gid,
                        uuid,
                        session,
                        message,
                    } => {
                        let announcement = match message {
                            Some(message) => serde_json::to_string(&message)
                                .ok()
                                .filter(|json| json.len() <= 16384)
                                .map(pumpkin_cluster::presence::DepartureAnnouncement::CustomJson)
                                .unwrap_or(
                                    pumpkin_cluster::presence::DepartureAnnouncement::Standard,
                                ),
                            None => pumpkin_cluster::presence::DepartureAnnouncement::Suppressed,
                        };
                        (
                            session,
                            uuid,
                            PresencePayload::Logout(PresenceLogout { gid, announcement }),
                        )
                    }
                    _ => unreachable!(),
                };
                let source = local_source.unwrap();
                if let Some(envelope) = owner.ordering.issue(source, session, uuid, payload) {
                    if let Some(transition) = owner.apply_ordered(&envelope) {
                        owner.publish(&runtime);
                        present_presence(&server, &transition);
                        if let Some(bytes) = pumpkin_cluster::ordered_presence::encode(&envelope) {
                            owner.broadcast(bytes);
                        }
                    }
                }
            }
            PresenceCommand::RemoteOrdered {
                peer,
                generation,
                envelope,
            } => {
                let input = AuthenticatedPresence {
                    peer,
                    generation,
                    envelope,
                };
                match authenticate_ordered(
                    owner.local,
                    &input,
                    &server.cluster_peer_generations.load(),
                    &server.cluster_peer_incarnations.load(),
                ) {
                    PresenceAuthentication::Accepted => {
                        if let Some(transition) = owner.apply_ordered(&input.envelope) {
                            owner.publish(&runtime);
                            present_presence(&server, &transition);
                        }
                    }
                    PresenceAuthentication::Deferred => held_remote = owner.defer(input).err(),
                    PresenceAuthentication::Rejected => {}
                }
            }
            PresenceCommand::RemoteHide { .. } => {}
            PresenceCommand::PeerDropped(peer) => {
                for entry in owner.drop_peer(peer) {
                    owner.publish(&runtime);
                    super::cluster_hide::push_remote_logout_to_viewers(
                        &server,
                        &entry.gid,
                        &entry.uuid,
                        &entry.name,
                    );
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
        let _ = announced_before_flush;
        owner.publish(&runtime);
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

    #[test]
    fn uuid_visibility_survives_old_gid_retirement_and_tracks_new_profile_hide() {
        let mut owner = PresenceOwner::new();
        let old = login(1, 1, true);
        let mut successor = login(2, 2, true);
        successor.uuid = old.uuid;
        profile(&mut owner, old.clone(), 8, false);
        profile(&mut owner, successor.clone(), 9, false);
        issue(&mut owner, source(1), 8, old.uuid, PresencePayload::Logout(PresenceLogout::new(old.gid)));
        let (runtime, _) = PresenceRuntime::new(1);
        owner.publish(&runtime);
        let snapshot = runtime.snapshot();
        assert!(!snapshot.entries.contains_key(&old.gid));
        assert_eq!(snapshot.by_uuid(&old.uuid).unwrap().gid, successor.gid);
        assert!(snapshot.uuid_visible_to(&old.uuid, &[99; 16]));
        assert_eq!(snapshot.roster_order.len(), snapshot.entries.len());
        hide(&mut owner, &successor, 9, true);
        owner.publish(&runtime);
        let snapshot = runtime.snapshot();
        assert!(!snapshot.uuid_visible_to(&old.uuid, &[99; 16]));
        assert!(snapshot.uuid_visible_to(&old.uuid, &old.uuid));
    }

    fn gid(server: u16, slot: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(slot))
    }
    fn source(server: u16) -> PeerGeneration {
        PeerGeneration {
            server: ServerId(server),
            generation: 100 + u64::from(server),
        }
    }
    fn login(server: u16, slot: u16, lobby: bool) -> PresenceLogin {
        PresenceLogin::new(
            gid(server, slot),
            [slot as u8; 16],
            format!("Player{slot}"),
            vec![PresenceProperty::new(
                "textures".into(),
                format!("texture-{slot}"),
                Some(format!("signature-{slot}")),
            )],
            lobby,
        )
    }
    fn issue(
        owner: &mut PresenceOwner,
        source: PeerGeneration,
        session: u64,
        uuid: [u8; 16],
        payload: PresencePayload,
    ) -> OrderedPresence {
        let envelope = owner
            .ordering
            .issue(source, session, uuid, payload)
            .unwrap();
        assert!(owner.apply_ordered(&envelope).is_some());
        envelope
    }
    fn profile(
        owner: &mut PresenceOwner,
        login: PresenceLogin,
        session: u64,
        hidden: bool,
    ) -> OrderedPresence {
        issue(
            owner,
            source(login.gid.server.0),
            session,
            login.uuid,
            PresencePayload::Profile { login, hidden },
        )
    }
    fn hide(
        owner: &mut PresenceOwner,
        login: &PresenceLogin,
        session: u64,
        hidden: bool,
    ) -> OrderedPresence {
        issue(
            owner,
            source(login.gid.server.0),
            session,
            login.uuid,
            PresencePayload::Hide {
                gid: login.gid,
                name: login.name.clone(),
                hidden,
            },
        )
    }
    fn retire(owner: &mut PresenceOwner, login: &PresenceLogin, session: u64) -> OrderedPresence {
        issue(
            owner,
            source(login.gid.server.0),
            session,
            login.uuid,
            PresencePayload::Logout(PresenceLogout::new(login.gid)),
        )
    }

    #[test]
    fn original_profile_and_hide_facets_survive_reverse_arrival() {
        let mut issuer = PresenceOwner::new();
        let waiting = login(2, 7, true).with_native_skin(true).with_listing(false);
        let announced = profile(&mut issuer, waiting.clone(), 17, false);
        let hidden = hide(&mut issuer, &waiting, 17, true);
        let mut receiver = PresenceOwner::new();
        assert!(receiver.apply_ordered(&hidden).is_some());
        assert!(receiver.apply_ordered(&announced).is_some());
        let entry = &receiver.entries[&waiting.gid];
        assert!(entry.native_skin);
        assert!(entry.in_lobby);
        assert!(!entry.listed);
        assert_eq!(entry.properties, waiting.properties);
        assert_eq!(entry.session, 17);
        assert_eq!(entry.source, Some(source(2)));
        assert!(receiver.hidden_gids.contains(&waiting.gid));
        let shown = hide(&mut issuer, &waiting, 17, false);
        receiver.apply_ordered(&shown).unwrap();
        assert!(!receiver.hidden_gids.contains(&waiting.gid));
        assert!(receiver.apply_ordered(&hidden).is_none());
        assert!(!receiver.hidden_uuids.contains(&waiting.uuid));
    }

    #[test]
    fn logout_seed_retains_exact_boot_session_and_rejects_late_profile() {
        let mut issuer = PresenceOwner::new();
        let waiting = login(2, 7, true);
        let announced = profile(&mut issuer, waiting.clone(), 17, false);
        let logout = retire(&mut issuer, &waiting, 17);
        issuer.peers = vec![3];
        issuer.publish_peer_state(ServerId(3));
        let parcel = issuer.pending.values().next().unwrap();
        let tombstone = pumpkin_cluster::ordered_presence::decode(&parcel.bytes).unwrap();
        assert_eq!(tombstone.source, announced.source);
        assert_eq!(tombstone.session, 17);
        assert_eq!(tombstone.revision, logout.revision);
        assert!(matches!(tombstone.payload, PresencePayload::Retired { .. }));
        let mut receiver = PresenceOwner::new();
        receiver.apply_ordered(&tombstone).unwrap();
        assert!(receiver.apply_ordered(&announced).is_none());
        let mut invalid_revival = announced;
        invalid_revival.revision = logout.revision + 1;
        assert!(receiver.apply_ordered(&invalid_revival).is_none());
        let (runtime, _rx) = PresenceRuntime::new(1);
        receiver.publish(&runtime);
        assert_eq!(
            runtime.snapshot().retirement_sessions[&waiting.gid],
            (source(2), 17)
        );
        assert!(runtime.snapshot().entries.is_empty());
    }

    #[test]
    fn gid_reuse_rejects_old_session_even_when_uuid_is_reused() {
        for replacement_uuid in [[7; 16], [8; 16]] {
            let mut owner = PresenceOwner::new();
            let previous = login(2, 7, false);
            let stale_profile = profile(&mut owner, previous.clone(), 17, false);
            let stale_logout = retire(&mut owner, &previous, 17);
            let replacement = PresenceLogin::new(
                previous.gid,
                replacement_uuid,
                "Replacement".into(),
                vec![],
                true,
            );
            profile(&mut owner, replacement.clone(), 18, false);
            assert!(owner.apply_ordered(&stale_profile).is_none());
            assert!(owner.apply_ordered(&stale_logout).is_none());
            let current = &owner.entries[&previous.gid];
            assert_eq!(current.name, replacement.name);
            assert_eq!(current.uuid, replacement.uuid);
            assert_eq!(current.session, 18);
            assert!(!owner.retirement_sessions.contains_key(&previous.gid));
        }
    }

    #[test]
    fn relay_seed_keeps_original_envelopes_without_new_revisions() {
        let mut owner = PresenceOwner::new();
        owner.local = ServerId(1);
        let remote = login(2, 7, true);
        let original = profile(&mut owner, remote.clone(), 17, false);
        let hidden = hide(&mut owner, &remote, 17, true);
        owner.peers = vec![3];
        owner.publish_peer_state(ServerId(3));
        let (tx, mut rx) = mpsc::channel(2);
        owner.outbound = Some(tx);
        owner.flush();
        let received = [rx.try_recv().unwrap(), rx.try_recv().unwrap()]
            .map(|parcel| pumpkin_cluster::ordered_presence::decode(&parcel.bytes).unwrap());
        assert_eq!(received, [original, hidden]);
    }

    #[test]
    fn unknown_source_boot_is_deferred_and_exact_transport_and_boot_are_required() {
        let mut owner = PresenceOwner::new();
        let original = profile(&mut owner, login(2, 7, true), 17, false);
        let input = AuthenticatedPresence {
            peer: ServerId(3),
            generation: 11,
            envelope: original,
        };
        let transports = HashMap::from([(ServerId(3), 11)]);
        assert_eq!(
            authenticate_ordered(
                ServerId(1),
                &input,
                &HashMap::from([(ServerId(3), 10)]),
                &HashMap::from([(ServerId(2), 102)])
            ),
            PresenceAuthentication::Deferred
        );
        assert_eq!(
            authenticate_ordered(ServerId(1), &input, &transports, &HashMap::new()),
            PresenceAuthentication::Deferred
        );
        assert_eq!(
            authenticate_ordered(
                ServerId(1),
                &input,
                &transports,
                &HashMap::from([(ServerId(2), 101)])
            ),
            PresenceAuthentication::Rejected
        );
        assert_eq!(
            authenticate_ordered(
                ServerId(1),
                &input,
                &transports,
                &HashMap::from([(ServerId(2), 102)])
            ),
            PresenceAuthentication::Accepted
        );
        assert_eq!(
            authenticate_ordered(
                ServerId(1),
                &input,
                &HashMap::from([(ServerId(3), 12)]),
                &HashMap::from([(ServerId(2), 102)])
            ),
            PresenceAuthentication::Rejected
        );
        assert_eq!(
            authenticate_ordered(
                ServerId(3),
                &input,
                &transports,
                &HashMap::from([(ServerId(2), 102)])
            ),
            PresenceAuthentication::Rejected
        );
    }

    #[test]
    fn deferred_unknown_boot_keeps_independent_facets_and_latest_session() {
        let mut source_owner = PresenceOwner::new();
        let login = login(2, 7, true);
        let original = profile(&mut source_owner, login.clone(), 17, false);
        let hidden = hide(&mut source_owner, &login, 17, true);
        let replacement = profile(&mut source_owner, login.clone(), 18, false);
        let input = |envelope| AuthenticatedPresence {
            peer: ServerId(3),
            generation: 11,
            envelope,
        };
        let mut owner = PresenceOwner::new();
        assert!(owner.defer(input(original.clone())).is_ok());
        assert!(owner.defer(input(hidden)).is_ok());
        assert!(owner.defer(input(replacement.clone())).is_ok());
        assert!(owner.defer(input(original)).is_ok());
        assert_eq!(owner.deferred.len(), 2);
        while let Some(key) = owner.deferred_order.pop_front() {
            let envelope = owner.deferred.remove(&key).unwrap().envelope;
            owner.apply_ordered(&envelope);
        }
        assert_eq!(owner.entries[&login.gid].session, 18);
        assert!(!owner.hidden_gids.contains(&login.gid));
        assert_eq!(owner.ordering.profile(login.gid), Some(&replacement));
    }

    #[test]
    fn new_boot_replaces_old_projection_but_old_boot_cannot_return() {
        let mut owner = PresenceOwner::new();
        let old = profile(&mut owner, login(2, 7, false), 99, false);
        let mut replacement = old.clone();
        replacement.source.generation += 1;
        replacement.session = 1;
        replacement.revision = 1;
        if let PresencePayload::Profile { login, .. } = &mut replacement.payload {
            login.name = "Restarted".into();
        }
        let transports = HashMap::from([(ServerId(2), 12)]);
        let boots = HashMap::from([(ServerId(2), replacement.source.generation)]);
        let new_input = AuthenticatedPresence {
            peer: ServerId(2),
            generation: 12,
            envelope: replacement.clone(),
        };
        assert_eq!(
            authenticate_ordered(ServerId(1), &new_input, &transports, &boots),
            PresenceAuthentication::Accepted
        );
        owner.apply_ordered(&replacement).unwrap();
        let old_input = AuthenticatedPresence {
            peer: ServerId(2),
            generation: 12,
            envelope: old,
        };
        assert_eq!(
            authenticate_ordered(ServerId(1), &old_input, &transports, &boots),
            PresenceAuthentication::Rejected
        );
        assert_eq!(owner.entries[&replacement.gid()].name, "Restarted");
        assert_eq!(
            owner.entries[&replacement.gid()].source,
            Some(replacement.source)
        );
    }

    #[test]
    fn saturated_transport_retains_distinct_original_states_and_coalesces_retirement() {
        let mut owner = PresenceOwner::new();
        owner.peers = vec![9];
        let (tx, mut rx) = mpsc::channel(1);
        owner.outbound = Some(tx);
        for slot in 1..=256 {
            let envelope = profile(&mut owner, login(1, slot, false), 17, false);
            owner.queue_to(
                ServerId(9),
                pumpkin_cluster::ordered_presence::encode(&envelope).unwrap(),
            );
        }
        owner.flush();
        assert_eq!(owner.pending.len(), 255);
        let retired = retire(&mut owner, &login(1, 256, false), 17);
        owner.queue_to(
            ServerId(9),
            pumpkin_cluster::ordered_presence::encode(&retired).unwrap(),
        );
        assert_eq!(owner.pending.len(), 255);
        let mut last =
            pumpkin_cluster::ordered_presence::decode(&rx.try_recv().unwrap().bytes).unwrap();
        for _ in 0..255 {
            owner.flush();
            last =
                pumpkin_cluster::ordered_presence::decode(&rx.try_recv().unwrap().bytes).unwrap();
        }
        assert!(owner.pending.is_empty());
        assert_eq!(last, retired);
    }

    #[test]
    fn same_peer_transport_epoch_reseeds_original_envelopes() {
        let mut owner = PresenceOwner::new();
        let original = profile(&mut owner, login(1, 3, true), 17, false);
        let (tx, mut rx) = mpsc::channel(8);
        owner.outbound = Some(tx);
        owner.set_peers(vec![9]);
        owner.update_transport_generations(&HashMap::from([(ServerId(9), 10)]));
        owner.flush();
        assert_eq!(
            pumpkin_cluster::ordered_presence::decode(&rx.try_recv().unwrap().bytes),
            Some(original.clone())
        );
        assert!(rx.try_recv().is_err());
        owner.update_transport_generations(&HashMap::from([(ServerId(9), 10)]));
        owner.flush();
        assert!(rx.try_recv().is_err());
        owner.update_transport_generations(&HashMap::from([(ServerId(9), 11)]));
        owner.flush();
        assert_eq!(
            pumpkin_cluster::ordered_presence::decode(&rx.try_recv().unwrap().bytes),
            Some(original)
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn disconnected_projection_can_restore_only_the_retained_original_profile() {
        let mut owner = PresenceOwner::new();
        let original = profile(&mut owner, login(2, 7, true), 17, false);
        owner.drop_peer(ServerId(2));
        assert!(!owner.entries.contains_key(&original.gid()));
        assert!(owner.apply_ordered(&original).is_some());
        assert_eq!(owner.entries[&original.gid()].session, original.session);
        assert!(!owner.retired_gids.contains(&original.gid()));
        let mut stale = original.clone();
        stale.revision += 1;
        if let PresencePayload::Profile { login, .. } = &mut stale.payload {
            login.listed = false;
        }
        owner.apply_ordered(&stale).unwrap();
        owner.drop_peer(ServerId(2));
        assert!(owner.apply_ordered(&original).is_none());
        assert!(owner.apply_ordered(&stale).is_some());
        assert!(!owner.entries[&original.gid()].listed);
    }

    #[test]
    fn hidden_lobby_profile_updates_preserve_visibility_and_prior_snapshots() {
        let mut owner = PresenceOwner::new();
        let waiting = login(2, 7, true);
        profile(&mut owner, waiting.clone(), 17, false);
        let (runtime, _rx) = PresenceRuntime::new(1);
        owner.publish(&runtime);
        let before = runtime.snapshot();
        hide(&mut owner, &waiting, 17, true);
        profile(&mut owner, waiting.clone().with_listing(false), 17, true);
        owner.publish(&runtime);
        let after = runtime.snapshot();
        assert!(before.is_visible_to(&waiting.gid, &waiting.uuid, Some(&[9; 16])));
        assert!(!after.is_visible_to(&waiting.gid, &waiting.uuid, Some(&[9; 16])));
        assert!(after.is_visible_to(&waiting.gid, &waiting.uuid, Some(&waiting.uuid)));
        assert!(after.entries[&waiting.gid].in_lobby);
        assert!(!after.entries[&waiting.gid].listed);
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
        assert_ne!(
            permission_fingerprint(&first),
            permission_fingerprint(&HashMap::from([(b, PermissionLvl::Two)]))
        );
    }
}
