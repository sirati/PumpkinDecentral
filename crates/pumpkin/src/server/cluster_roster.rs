use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam::queue::ArrayQueue;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::presence::RemotePlayerEntry;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_protocol::bedrock::client::player_list::{CPlayerList, PlayerListEntry};
use pumpkin_protocol::bedrock::client::{Skin, common::BuildPlatform};
use pumpkin_protocol::java::client::play::{
    CPlayerInfoUpdate, CRemovePlayerInfo, PlayerAction, PlayerInfoFlags,
};
use uuid::Uuid;

use super::{Server, cluster_presence::PresenceSnapshot};
use crate::net::ClientPlatform;

struct Registration {
    client: Arc<ClientPlatform>,
    gid: GlobalPlayerId,
    uuid: [u8; 16],
    session: u64,
    ready: bool,
}

pub(crate) struct RosterRuntime {
    registrations: ArrayQueue<Registration>,
    changed: ArrayQueue<GlobalPlayerId>,
}

impl RosterRuntime {
    pub fn new() -> Self {
        Self {
            registrations: ArrayQueue::new(4096),
            changed: ArrayQueue::new(pumpkin_cluster::presence::PRESENCE_MAX_ENTRIES),
        }
    }
}

pub(crate) fn register(
    server: &Server,
    client: Arc<ClientPlatform>,
    gid: GlobalPlayerId,
    uuid: [u8; 16],
    session: u64,
    ready: bool,
) -> bool {
    session != 0
        && server
            .cluster_roster
            .registrations
            .push(Registration {
                client,
                gid,
                uuid,
                session,
                ready,
            })
            .is_ok()
}

pub(crate) fn changed(server: &Server, gid: GlobalPlayerId) -> bool {
    server.cluster_roster.changed.push(gid).is_ok()
}

struct Priority {
    cursor: Option<usize>,
    retry: bool,
}

struct Delivered {
    profile: Arc<RemotePlayerEntry>,
    skin: Option<Arc<Skin>>,
}

impl Delivered {
    fn identity_matches(&self, entry: &RemotePlayerEntry) -> bool {
        self.profile.uuid == entry.uuid && self.profile.source == entry.source && self.profile.session == entry.session
    }

    fn matches(&self, entry: &RemotePlayerEntry, skin: &Option<Arc<Skin>>) -> bool {
        self.profile.as_ref() == entry
            && match (&self.skin, skin) { (Some(a), Some(b)) => Arc::ptr_eq(a, b), (None, None) => true, _ => false }
    }
}

struct RosterBaseline {
    revision: u64,
    roster_order: Arc<[GlobalPlayerId]>,
    entries: Arc<HashMap<GlobalPlayerId, Arc<RemotePlayerEntry>>>,
}

impl From<&PresenceSnapshot> for RosterBaseline {
    fn from(snapshot: &PresenceSnapshot) -> Self {
        Self { revision: snapshot.revision, roster_order: snapshot.roster_order.clone(), entries: snapshot.roster_images.clone() }
    }
}

struct Viewer {
    registration: Registration,
    snapshot: Arc<RosterBaseline>,
    delivered: BTreeMap<GlobalPlayerId, Delivered>,
    removing: bool,
    removal_cursor: Option<GlobalPlayerId>,
    addition_cursor: usize,
    finished: bool,
    last_scan: std::time::Instant,
}

fn remove_packet(client: &ClientPlatform, uuid: [u8; 16]) -> Option<Bytes> {
    let uuid = Uuid::from_bytes(uuid);
    match client {
        ClientPlatform::Java(client) => client
            .serialize_packet(&CRemovePlayerInfo::new(&[uuid]))
            .ok(),
        ClientPlatform::Bedrock(client) => client
            .serialize_packet(&CPlayerList {
                action: CPlayerList::ACTION_REMOVE,
                entries: vec![PlayerListEntry {
                    uuid,
                    entity_unique_id: pumpkin_protocol::codec::var_long::VarLong(0),
                    username: String::new(),
                    xuid: String::new(),
                    platform_chat_id: String::new(),
                    build_platform: BuildPlatform::Unknown,
                    skin: Skin::steve(),
                    is_teacher: false,
                    is_host: false,
                    is_sub_client: false,
                    player_color: [0; 4],
                }],
            })
            .ok(),
    }
}

fn add_packet(
    server: &Server,
    client: &ClientPlatform,
    entry: &RemotePlayerEntry,
    skin: &Option<Arc<Skin>>,
) -> Option<Bytes> {
    let uuid = Uuid::from_bytes(entry.uuid);
    match client {
        ClientPlatform::Java(client) => {
            let properties = super::cluster_chat_pm::protocol_properties(&entry.properties);
            let views = server.cluster_secure_chat.views.load();
            let chat = views
                .get(&uuid)
                .and_then(super::cluster_secure_chat::ChatSessionView::init_chat);
            let mut flags = PlayerInfoFlags::ADD_PLAYER | PlayerInfoFlags::UPDATE_LISTED;
            let mut actions = vec![
                PlayerAction::AddPlayer {
                    name: &entry.name,
                    properties: &properties,
                },
                PlayerAction::UpdateListed(entry.listed),
            ];
            if chat.is_some() {
                flags |= PlayerInfoFlags::INITIALIZE_CHAT;
                actions.push(PlayerAction::InitializeChat(chat));
            }
            client
                .serialize_packet(&CPlayerInfoUpdate::new(
                    flags.bits(),
                    &[pumpkin_protocol::java::client::play::Player {
                        uuid,
                        actions: &actions,
                    }],
                ))
                .ok()
        }
        ClientPlatform::Bedrock(client) => client
            .serialize_packet(&CPlayerList {
                action: CPlayerList::ACTION_ADD,
                entries: vec![PlayerListEntry {
                    uuid,
                    entity_unique_id: pumpkin_protocol::codec::var_long::VarLong(0),
                    username: entry.name.clone(),
                    xuid: String::new(),
                    platform_chat_id: String::new(),
                    build_platform: BuildPlatform::Unknown,
                    skin: (**skin.as_ref()?).clone(),
                    is_teacher: false,
                    is_host: false,
                    is_sub_client: false,
                    player_color: [0; 4],
                }],
            })
            .ok(),
    }
}

fn baseline_entry_current(
    current: &PresenceSnapshot,
    entry: &RemotePlayerEntry,
    viewer: &[u8; 16],
) -> bool {
    current.entries.get(&entry.gid).is_some_and(|live| {
        live == entry && current.is_visible_to(&entry.gid, &entry.uuid, Some(viewer))
    })
}

impl Viewer {
    fn priority(&mut self, server: &Server, gid: GlobalPlayerId, bytes_left: &mut usize) -> bool {
        if !self.registration.ready { return false; }
        let current = server.cluster_presence.snapshot();
        let desired = current.entries.get(&gid).filter(|entry| current.is_visible_to(&gid, &entry.uuid, Some(&self.registration.uuid)));
        let Some(entry) = desired else {
            let Some(old) = self.delivered.get(&gid) else { return true; };
            if current.uuid_visible_to(&old.profile.uuid, &self.registration.uuid) { self.delivered.remove(&gid); return true; }
            let Some(packet) = remove_packet(&self.registration.client, old.profile.uuid) else { return false; };
            let size = packet.len();
            if size > *bytes_left { *bytes_left = 0; return false; }
            *bytes_left -= size;
            if self.registration.client.try_enqueue_packet_burst(vec![packet]).is_err() { return false; }
            self.delivered.remove(&gid);
            return true;
        };
        let skin = if matches!(self.registration.client.as_ref(), ClientPlatform::Bedrock(_)) {
            let skins = server.cluster_profile_skins.load();
            let Some(skin) = skins.get(&Uuid::from_bytes(entry.uuid)).filter(|skin| super::cluster_profile_skins::matches_profile(server, skin, entry)) else { return false; };
            Some(skin.skin.clone())
        } else { None };
        if self.delivered.get(&gid).is_some_and(|old| old.matches(entry, &skin)) { return true; }
        let Some(packet) = add_packet(server, &self.registration.client, entry, &skin) else { return false; };
        let size = packet.len();
        if size > *bytes_left { *bytes_left = 0; return false; }
        *bytes_left -= size;
        if self.registration.client.try_enqueue_packet_burst(vec![packet]).is_err() { return false; }
        self.delivered.insert(gid, Delivered { profile: current.roster_images.get(&gid).cloned().unwrap_or_else(|| Arc::new(entry.clone())), skin });
        true
    }

    fn step(&mut self, server: &Server, bytes_left: &mut usize) -> bool {
        if !self.registration.ready {
            return false;
        }
        let current = server.cluster_presence.snapshot();
        if self.finished {
            if current.revision == self.snapshot.revision
                && (matches!(self.registration.client.as_ref(), ClientPlatform::Java(_))
                    || self.last_scan.elapsed().as_millis() < 500)
            {
                return false;
            }
            self.snapshot = Arc::new(RosterBaseline::from(current.as_ref()));
            self.removing = true;
            self.removal_cursor = None;
            self.addition_cursor = 0;
            self.finished = false;
            self.last_scan = std::time::Instant::now();
        }
        if self.removing {
            let next = self
                .delivered
                .range((
                    self.removal_cursor
                        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    std::ops::Bound::Unbounded,
                ))
                .next();
            let Some((&gid, delivered)) = next else {
                self.removing = false;
                return true;
            };
            if current.uuid_visible_to(&delivered.profile.uuid, &self.registration.uuid) {
                let migrated = current.by_uuid(&delivered.profile.uuid).is_some_and(|entry| entry.gid != gid);
                self.removal_cursor = Some(gid);
                if migrated { self.delivered.remove(&gid); }
                return true;
            }
            let Some(packet) = remove_packet(&self.registration.client, delivered.profile.uuid) else {
                return false;
            };
            if packet.len() > *bytes_left {
                *bytes_left = 0;
                return false;
            }
            let len = packet.len();
            *bytes_left -= len;
            if self
                .registration
                .client
                .try_enqueue_packet_burst(vec![packet])
                .is_err()
            {
                return false;
            }
            self.delivered.remove(&gid);
            self.removal_cursor = Some(gid);
            return true;
        }
        let Some(&gid) = self.snapshot.roster_order.get(self.addition_cursor) else {
            self.finished = true;
            return false;
        };
        let Some(entry) = self.snapshot.entries.get(&gid) else {
            self.addition_cursor += 1;
            return true;
        };
        let live = baseline_entry_current(&current, entry, &self.registration.uuid);
        if !live {
            self.addition_cursor += 1;
            return true;
        }
        let skin = if matches!(
            self.registration.client.as_ref(),
            ClientPlatform::Bedrock(_)
        ) {
            let skins = server.cluster_profile_skins.load();
            let Some(skin) = skins
                .get(&Uuid::from_bytes(entry.uuid))
                .filter(|skin| super::cluster_profile_skins::matches_profile(server, skin, entry))
            else {
                self.addition_cursor += 1;
                return true;
            };
            Some(skin.skin.clone())
        } else {
            None
        };
        if self
            .delivered
            .get(&gid)
            .is_some_and(|delivered| delivered.matches(entry, &skin))
        {
            self.addition_cursor += 1;
            return true;
        }
        let Some(packet) = add_packet(server, &self.registration.client, entry, &skin) else {
            return false;
        };
        let mut packets = Vec::new();
        if let Some(delivered) = self.delivered.get(&gid)
            && delivered.profile.uuid != entry.uuid
            && !current.uuid_visible_to(&delivered.profile.uuid, &self.registration.uuid) {
            let Some(remove) = remove_packet(&self.registration.client, delivered.profile.uuid) else {
                return false;
            };
            packets.push(remove);
        }
        packets.push(packet);
        let len = packets.iter().map(Bytes::len).sum::<usize>();
        if len > *bytes_left {
            *bytes_left = 0;
            return false;
        }
        *bytes_left -= len;
        if self
            .registration
            .client
            .try_enqueue_packet_burst(packets)
            .is_err()
        {
            return false;
        }
        self.delivered.insert(
            gid,
            Delivered { profile: entry.clone(), skin },
        );
        self.addition_cursor += 1;
        true
    }
}

pub(crate) fn spawn(server: &Arc<Server>) {
    let weak = Arc::downgrade(server);
    server.spawn_task(async move {
        let mut viewers = BTreeMap::<usize, Viewer>::new();
        let mut priorities = HashMap::<GlobalPlayerId, Priority>::new();
        let mut priority_order = VecDeque::new();
        let mut order = VecDeque::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(server) = weak.upgrade() else {
                break;
            };
            for _ in 0..256 {
                let Some(registration) = server.cluster_roster.registrations.pop() else {
                    break;
                };
                let key = Arc::as_ptr(&registration.client) as usize;
                if let Some(viewer) = viewers.get_mut(&key) {
                    if viewer.registration.gid == registration.gid
                        && viewer.registration.uuid == registration.uuid
                        && viewer.registration.session == registration.session
                        && Arc::ptr_eq(&viewer.registration.client, &registration.client)
                    {
                        viewer.registration.ready = registration.ready;
                    }
                    continue;
                }
                if registration.client.closed() {
                    continue;
                }
                viewers.insert(
                    key,
                    Viewer {
                        registration,
                        snapshot: Arc::new(RosterBaseline::from(server.cluster_presence.snapshot().as_ref())),
                        delivered: BTreeMap::new(),
                        removing: true,
                        removal_cursor: None,
                        addition_cursor: 0,
                        finished: false,
                        last_scan: std::time::Instant::now(),
                    },
                );
                order.push_back(key);
            }
            let mut bytes_left = 32 * 1024 * 1024;
            while priorities.len() < pumpkin_cluster::presence::PRESENCE_MAX_ENTRIES {
                let Some(gid) = server.cluster_roster.changed.pop() else { break; };
                if let Some(priority) = priorities.get_mut(&gid) { priority.retry = true; }
                else { priorities.insert(gid, Priority { cursor: None, retry: false }); priority_order.push_back(gid); }
            }
            let mut retry_priorities = Vec::new();
            for _ in 0..4096 {
                let Some(gid) = priority_order.pop_front() else { break; };
                let priority = priorities.get_mut(&gid).unwrap();
                let next = viewers.range_mut((priority.cursor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded), std::ops::Bound::Unbounded)).next();
                if let Some((&key, viewer)) = next {
                    priority.retry |= !viewer.priority(&server, gid, &mut bytes_left);
                    priority.cursor = Some(key);
                    priority_order.push_back(gid);
                } else if priority.retry {
                    priority.cursor = None; priority.retry = false; retry_priorities.push(gid);
                } else { priorities.remove(&gid); }
                if bytes_left == 0 { break; }
            }
            priority_order.extend(retry_priorities);
            let turns = order.len().min(4096);
            let mut work_left = 4096;
            for _ in 0..turns {
                let key = order.pop_front().unwrap();
                let Some(viewer) = viewers.get_mut(&key) else {
                    continue;
                };
                if viewer.registration.client.closed() {
                    viewers.remove(&key);
                    continue;
                }
                for _ in 0..64.min(work_left) {
                    work_left -= 1;
                    if !viewer.step(&server, &mut bytes_left) {
                        break;
                    }
                }
                order.push_back(key);
                if work_left == 0 || bytes_left == 0 {
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};

    fn profile() -> RemotePlayerEntry {
        let mut entry = RemotePlayerEntry::new(
            GlobalPlayerId::new(ServerId(1), PlayerSlot(2)),
            [3; 16],
            "online".into(),
            Vec::new(),
            0,
            true,
            true,
        );
        entry.source = Some(PeerGeneration {
            server: ServerId(1),
            generation: 7,
        });
        entry.session = 8;
        entry
    }

    #[test]
    fn pinned_roster_baseline_cannot_resurrect_hidden_retired_or_reused_profile() {
        let entry = profile();
        let mut current = PresenceSnapshot::default();
        current.entries.insert(entry.gid, entry.clone());
        assert!(baseline_entry_current(&current, &entry, &[4; 16]));
        current.hidden_gids.insert(entry.gid);
        assert!(!baseline_entry_current(&current, &entry, &[4; 16]));
        current.hidden_gids.clear();
        current.entries.get_mut(&entry.gid).unwrap().session += 1;
        assert!(!baseline_entry_current(&current, &entry, &[4; 16]));
        current.entries.remove(&entry.gid);
        assert!(!baseline_entry_current(&current, &entry, &[4; 16]));
    }

    #[test]
    fn delivered_profile_requires_exact_session_and_actual_skin_identity() {
        let entry = profile();
        let skin = Arc::new(Skin::steve());
        let delivered = Delivered { profile: Arc::new(entry.clone()), skin: Some(skin.clone()) };
        assert!(delivered.matches(&entry, &Some(skin)));
        assert!(!delivered.matches(&entry, &Some(Arc::new(Skin::steve()))));
        let mut successor = entry;
        successor.session += 1;
        assert!(!delivered.identity_matches(&successor));
    }
}
