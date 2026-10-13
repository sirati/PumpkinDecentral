use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::data::LoadJSONConfiguration;
use crate::data::VanillaData;
use crate::data::banlist_serializer::BannedIpEntry;
use crate::data::banned_ip::BannedIpList;
use crate::data::whitelist::WhitelistConfig;

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WhitelistRecord {
    pub uuid: Uuid,
    pub name: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "BannedIpEntry", into = "BannedIpEntry")]
pub struct IpBanRecord {
    pub ip: IpAddr,
    pub created: time::OffsetDateTime,
    pub source: String,
    pub expires: Option<time::OffsetDateTime>,
    pub reason: String,
}

impl From<BannedIpEntry> for IpBanRecord {
    fn from(entry: BannedIpEntry) -> Self {
        Self {
            ip: entry.ip,
            created: entry.created,
            source: entry.source,
            expires: entry.expires,
            reason: entry.reason,
        }
    }
}

impl From<IpBanRecord> for BannedIpEntry {
    fn from(entry: IpBanRecord) -> Self {
        Self {
            ip: entry.ip,
            created: entry.created,
            source: entry.source,
            expires: entry.expires,
            reason: entry.reason,
        }
    }
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AccessPolicySnapshot {
    pub revision: u64,
    pub enabled: bool,
    pub whitelist: Vec<WhitelistRecord>,
    pub banned_ips: Vec<IpBanRecord>,
}

impl AccessPolicySnapshot {
    #[must_use]
    pub fn is_whitelisted(&self, uuid: Uuid) -> bool {
        self.whitelist.iter().any(|entry| entry.uuid == uuid)
    }

    #[must_use]
    pub fn active_ip_ban(&self, ip: IpAddr) -> Option<&IpBanRecord> {
        self.banned_ips.iter().find(|entry| {
            entry.ip == ip
                && entry
                    .expires
                    .is_none_or(|expires| expires >= time::OffsetDateTime::now_utc())
        })
    }
}

pub enum AccessPolicyCommand {
    SetEnabled(bool),
    AddWhitelist(WhitelistRecord),
    RemoveWhitelist(Uuid),
    BanIp(IpBanRecord),
    PardonIp(IpAddr),
    ReloadWhitelist,
    InstallWhitelist(Vec<WhitelistRecord>),
}

pub struct AccessPolicyRuntime {
    tx: mpsc::Sender<AccessPolicyCommand>,
    snapshot: Arc<ArcSwap<AccessPolicySnapshot>>,
}

impl AccessPolicyRuntime {
    pub fn new(data: &VanillaData, enabled: bool) -> (Self, mpsc::Receiver<AccessPolicyCommand>) {
        let whitelist = data
            .whitelist_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .whitelist
            .iter()
            .map(|entry| WhitelistRecord {
                uuid: entry.uuid,
                name: entry.name.clone(),
            })
            .collect();
        let banned_ips = data
            .banned_ip_list
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .banned_ips
            .iter()
            .map(|entry| IpBanRecord {
                ip: entry.ip,
                created: entry.created,
                source: entry.source.clone(),
                expires: entry.expires,
                reason: entry.reason.clone(),
            })
            .collect();
        let (tx, rx) = mpsc::channel(256);
        let fallback = AccessPolicySnapshot {
            revision: 0,
            enabled,
            whitelist,
            banned_ips,
        };
        let snapshot = fallback;
        (
            Self {
                tx,
                snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
            },
            rx,
        )
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<AccessPolicySnapshot> {
        self.snapshot.load_full()
    }

    pub fn try_submit(&self, command: AccessPolicyCommand) -> bool {
        let valid = match &command {
            AccessPolicyCommand::AddWhitelist(entry) => entry.name.len() <= 256,
            AccessPolicyCommand::BanIp(entry) => {
                entry.reason.len() <= 65536 && entry.source.len() <= 4096
            }
            AccessPolicyCommand::InstallWhitelist(entries) => {
                entries.len() <= 4096 && entries.iter().all(|entry| entry.name.len() <= 256)
            }
            _ => true,
        };
        valid && self.tx.try_send(command).is_ok()
    }
}

fn save_snapshot(snapshot: &AccessPolicySnapshot) -> std::io::Result<()> {
    let whitelist = WhitelistConfig {
        whitelist: snapshot
            .whitelist
            .iter()
            .map(|entry| {
                pumpkin_config::whitelist::WhitelistEntry::new(entry.uuid, entry.name.clone())
            })
            .collect(),
    };
    let banned_ips = BannedIpList {
        banned_ips: snapshot
            .banned_ips
            .iter()
            .map(|entry| BannedIpEntry {
                ip: entry.ip,
                created: entry.created,
                source: entry.source.clone(),
                expires: entry.expires,
                reason: entry.reason.clone(),
            })
            .collect(),
    };
    write_policy_file(WhitelistConfig::get_path(), &whitelist)?;
    write_policy_file(BannedIpList::get_path(), &banned_ips)?;
    write_policy_file(std::path::Path::new("cluster-access-policy.json"), snapshot)
}

fn write_policy_file(path: &std::path::Path, value: &impl serde::Serialize) -> std::io::Result<()> {
    use std::io::Write;
    let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    let directory = std::env::current_dir()?.join("data");
    std::fs::create_dir_all(&directory)?;
    let temporary = directory.join(format!(".access-policy-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, directory.join(path))?;
        std::fs::File::open(&directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn read_whitelist() -> std::io::Result<Vec<WhitelistRecord>> {
    let path = std::env::current_dir()?
        .join("data")
        .join(WhitelistConfig::get_path());
    let bytes = std::fs::read(path)?;
    let config: WhitelistConfig = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
    if config.whitelist.len() > 4096 || config.whitelist.iter().any(|entry| entry.name.len() > 256)
    {
        return Err(std::io::Error::other(
            "whitelist reload exceeds access policy limits",
        ));
    }
    Ok(config
        .whitelist
        .into_iter()
        .map(|entry| WhitelistRecord {
            uuid: entry.uuid,
            name: entry.name,
        })
        .collect())
}

pub(crate) fn kick_non_whitelisted(server: &super::Server, policy: &AccessPolicySnapshot) {
    if !server.basic_config.enforce_whitelist || !policy.enabled {
        return;
    }
    let admin = server.cluster_admin_view.load();
    for player in server.get_all_players() {
        if policy.is_whitelisted(player.gameprofile.id)
            || admin
                .operators
                .iter()
                .any(|op| op.uuid == *player.gameprofile.id.as_bytes())
        {
            continue;
        }
        player.kick(
            crate::net::DisconnectReason::Kicked,
            &pumpkin_macros::translate_cross!(
                pumpkin_data::translation::java::MULTIPLAYER_DISCONNECT_NOT_WHITELISTED,
                pumpkin_data::translation::bedrock::DISCONNECT_KICKED
            ),
        );
    }
    let local = pumpkin_cluster::identity::ServerId(server.advanced_config.cluster.server_id);
    for (gid, entry) in super::cluster_presence::all_presence_entries(server) {
        if gid.server != local
            || !entry.in_lobby
            || policy.is_whitelisted(Uuid::from_bytes(entry.uuid))
            || admin.operators.iter().any(|op| op.uuid == entry.uuid)
        {
            continue;
        }
        server.cluster_lobby_directory.disconnect_gid(
            gid,
            &pumpkin_macros::translate_cross!(
                pumpkin_data::translation::java::MULTIPLAYER_DISCONNECT_NOT_WHITELISTED,
                pumpkin_data::translation::bedrock::DISCONNECT_KICKED
            ),
        );
    }
}

pub const PREFIX: &[u8; 8] = b"PPolicy1";

pub fn receive(
    server: &super::Server,
    parcel: pumpkin_cluster::streams::InboundParcel,
) -> Result<(), pumpkin_cluster::streams::InboundParcel> {
    let Some(tx) = server.cluster_policy_inbound.load_full() else {
        return Err(parcel);
    };
    tx.try_send(parcel).map_err(|error| error.into_inner())
}

fn policy_updates(
    command: AccessPolicyCommand,
    state: &AccessPolicySnapshot,
) -> Vec<(String, Option<serde_json::Value>)> {
    match command {
        AccessPolicyCommand::SetEnabled(value) => vec![("enabled".into(), Some(value.into()))],
        AccessPolicyCommand::AddWhitelist(value) => vec![(
            format!("whitelist/{}", value.uuid),
            serde_json::to_value(value).ok(),
        )],
        AccessPolicyCommand::RemoveWhitelist(uuid) => vec![(format!("whitelist/{uuid}"), None)],
        AccessPolicyCommand::BanIp(value) => {
            vec![(format!("ip/{}", value.ip), serde_json::to_value(value).ok())]
        }
        AccessPolicyCommand::PardonIp(ip) => vec![(format!("ip/{ip}"), None)],
        AccessPolicyCommand::InstallWhitelist(entries) => {
            let mut updates: Vec<_> = state
                .whitelist
                .iter()
                .filter(|old| !entries.iter().any(|entry| entry.uuid == old.uuid))
                .map(|old| (format!("whitelist/{}", old.uuid), None))
                .collect();
            updates.extend(entries.into_iter().map(|entry| {
                (
                    format!("whitelist/{}", entry.uuid),
                    serde_json::to_value(entry).ok(),
                )
            }));
            updates
        }
        AccessPolicyCommand::ReloadWhitelist => Vec::new(),
    }
}

fn valid_register(entry: &super::cluster_control_registers::Register) -> bool {
    if entry.key.len() > 128 || entry.stamp.ordinal == 0 || entry.stamp.millis > i64::MAX as u64 {
        return false;
    }
    if entry.key == "enabled" {
        return entry
            .value
            .as_ref()
            .is_some_and(serde_json::Value::is_boolean);
    }
    if let Some(id) = entry.key.strip_prefix("whitelist/") {
        let Ok(uuid) = id.parse::<Uuid>() else {
            return false;
        };
        if entry.key != format!("whitelist/{uuid}") {
            return false;
        }
        return entry.value.as_ref().is_none_or(|value| {
            serde_json::from_value::<WhitelistRecord>(value.clone())
                .is_ok_and(|entry| entry.uuid == uuid && entry.name.len() <= 256)
        });
    }
    if let Some(ip) = entry.key.strip_prefix("ip/") {
        let Ok(ip) = ip.parse::<IpAddr>() else {
            return false;
        };
        if entry.key != format!("ip/{ip}") {
            return false;
        }
        return entry.value.as_ref().is_none_or(|value| {
            serde_json::from_value::<IpBanRecord>(value.clone()).is_ok_and(|entry| {
                entry.ip == ip && entry.reason.len() <= 65536 && entry.source.len() <= 4096
            })
        });
    }
    false
}

fn policy_view(
    registers: &super::cluster_control_registers::Registers,
    revision: u64,
) -> AccessPolicySnapshot {
    let mut state = AccessPolicySnapshot {
        revision,
        ..Default::default()
    };
    for entry in registers.entries() {
        let Some(value) = &entry.value else {
            continue;
        };
        if entry.key == "enabled" {
            state.enabled = value.as_bool().unwrap_or(false);
        } else if entry.key.starts_with("whitelist/") {
            if let Ok(entry) = serde_json::from_value(value.clone()) {
                state.whitelist.push(entry);
            }
        } else if entry.key.starts_with("ip/") {
            if let Ok(entry) = serde_json::from_value(value.clone()) {
                state.banned_ips.push(entry);
            }
        }
    }
    state
}

pub fn spawn(server: &Arc<super::Server>, mut rx: mpsc::Receiver<AccessPolicyCommand>) {
    use super::cluster_control_registers::{Frame, Registers};
    use std::collections::{HashMap, VecDeque};
    let snapshot = Arc::clone(&server.cluster_access_policy.snapshot);
    let weak_server = Arc::downgrade(server);
    let persist = !server.advanced_config.cluster.enabled
        || matches!(
            server.advanced_config.cluster.role,
            pumpkin_config::ClusterRole::Primary
        );
    let local = server.advanced_config.cluster.server_id;
    let path = std::path::PathBuf::from("data/cluster-policy-registers.json");
    let mut registers = Registers::default();
    if persist {
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Registers>(&bytes) {
                Ok(saved) if saved.entries().all(valid_register) => registers = saved,
                _ => {
                    tracing::error!("invalid persisted global access policy");
                    return;
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let initial = match std::fs::read("data/cluster-access-policy.json") {
                    Ok(bytes) => match serde_json::from_slice::<AccessPolicySnapshot>(&bytes) {
                        Ok(state) => Arc::new(state),
                        Err(error) => {
                            tracing::error!(%error, "invalid legacy access policy snapshot");
                            return;
                        }
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        snapshot.load_full()
                    }
                    Err(error) => {
                        tracing::error!(%error, "legacy access policy could not be loaded");
                        return;
                    }
                };
                let mut updates =
                    policy_updates(AccessPolicyCommand::SetEnabled(initial.enabled), &initial);
                updates.extend(initial.whitelist.iter().cloned().flat_map(|entry| {
                    policy_updates(AccessPolicyCommand::AddWhitelist(entry), &initial)
                }));
                updates.extend(
                    initial.banned_ips.iter().cloned().flat_map(|entry| {
                        policy_updates(AccessPolicyCommand::BanIp(entry), &initial)
                    }),
                );
                for (key, value) in updates {
                    let entry = super::cluster_control_registers::Register {
                        key,
                        value,
                        stamp: super::cluster_control_registers::Stamp {
                            millis: 0,
                            origin: local,
                            incarnation: 0,
                            ordinal: 1,
                        },
                    };
                    registers.merge(entry);
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to load global access policy");
                return;
            }
        }
    }
    let (inbound, mut inbound_rx) = mpsc::channel::<pumpkin_cluster::streams::InboundParcel>(256);
    server.cluster_policy_inbound.store(Some(Arc::new(inbound)));
    let (save_tx, mut save_rx) = mpsc::channel::<(Registers, AccessPolicySnapshot)>(1);
    if persist {
        server.spawn_task(async move {
            while let Some((registers, state)) = save_rx.recv().await {
                let path = path.clone();
                match tokio::task::spawn_blocking(move || {
                    super::cluster_control_registers::save(&path, &registers)?;
                    save_snapshot(&state)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    result => tracing::error!(?result, "global access policy persistence failed"),
                }
            }
        });
    }
    server.spawn_task(async move {
        let mut pending_save = None;
        let mut outgoing =
            VecDeque::<(Option<pumpkin_cluster::identity::ServerId>, Vec<u8>)>::new();
        let mut peers = HashMap::new();
        let mut seeds = VecDeque::<pumpkin_cluster::identity::ServerId>::new();
        let mut assemblies = super::cluster_control_registers::SeedAssemblies::default();
        let mut seed_backlog =
            VecDeque::<([u8; 16], pumpkin_cluster::streams::InboundParcel)>::new();
        let (reload_done, mut reload_results) =
            mpsc::channel::<(u64, std::io::Result<Vec<WhitelistRecord>>)>(1);
        let mut diagnostics = super::cluster_control_registers::Diagnostics::default();
        let mut pending_inbound: Option<pumpkin_cluster::streams::InboundParcel> = None;
        let mut deferred_inbound = VecDeque::<(pumpkin_cluster::streams::InboundParcel, std::time::Instant)>::new();
        let mut shutdown_started = false;
        let mut dirty = true;
        let mut drained = false;
        let mut reload_pending = false;
        let mut primary_reload_pending = false;
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            let Some(server) = weak_server.upgrade() else {
                break;
            };
            let shutting_down = server
                .cluster_global_shutdown
                .load(std::sync::atomic::Ordering::Acquire);
            let local_boot_ready = !server.advanced_config.cluster.enabled
                || server
                    .cluster_peer_incarnations
                    .load()
                    .get(&pumpkin_cluster::identity::ServerId(local))
                    .is_some_and(|boot| *boot != 0);
            let inbound_ready = pending_inbound.as_ref().is_none_or(|parcel| super::cluster_control_registers::control_publication_ready(parcel.generation, server.cluster_peer_generations.load().get(&parcel.peer).copied(), server.cluster_peer_incarnations.load().get(&parcel.peer).copied()));
            if !inbound_ready { deferred_inbound.push_back((pending_inbound.take().unwrap(), std::time::Instant::now())); }
            if shutting_down && !shutdown_started {
                shutdown_started = true;
                rx.close();
                inbound_rx.close();
                seeds.clear();
                outgoing.retain(|(_, bytes)| {
                    !matches!(
                        super::cluster_control_registers::decode(PREFIX, bytes),
                        Some(Frame::Seed { .. })
                    )
                });
            }
            let command = tokio::select! {
                command = rx.recv(), if !drained && (shutting_down || local_boot_ready && outgoing.len() < 4096 && !dirty) => { if command.is_none() { drained = true; } command },
                result = reload_results.recv(), if (shutting_down || local_boot_ready) && outgoing.len() < 4096 && !dirty => {
                    reload_pending = false;
                    match result {
                        Some((revision, Ok(entries))) if revision == snapshot.load().revision => Some(AccessPolicyCommand::InstallWhitelist(entries)),
                        Some((_, Ok(_))) => { tracing::warn!("whitelist reload superseded by newer policy mutation"); None },
                        Some((_, Err(error))) => { tracing::error!(%error, "global whitelist reload failed"); None },
                        None => break,
                    }
                }
                parcel = async {
                    if let Some(parcel) = pending_inbound.take() { Some(parcel) } else { inbound_rx.recv().await }
                }, if !shutting_down && (pending_inbound.is_some() || seed_backlog.len() < 256 && deferred_inbound.len() < 256) && outgoing.len() < 4096 && !dirty => {
                    let Some(parcel) = parcel else { break; };
                    let generation = server.cluster_peer_generations.load().get(&parcel.peer).copied();
                    if generation.is_none_or(|generation| generation < parcel.generation) { deferred_inbound.push_back((parcel, std::time::Instant::now())); continue; }
                    if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation) { diagnostics.rejected("access_policy", parcel.peer.0, "stale transport"); continue; }
                    let Some(remote_incarnation) = server.cluster_peer_incarnations.load().get(&parcel.peer).copied().filter(|boot| *boot != 0) else { deferred_inbound.push_back((parcel, std::time::Instant::now())); continue; };
                    match super::cluster_control_registers::decode(PREFIX, &parcel.bytes) {
                        Some(Frame::Mutation(entries)) => {
                            if let Some(changed) = registers.merge_authenticated(parcel.peer.0, remote_incarnation, entries, valid_register) { dirty |= changed; } else { diagnostics.rejected("access_policy", parcel.peer.0, "invalid mutation"); }
                        }
                        Some(Frame::Seed { transfer, index, total, entries }) if total > 0 && total <= 64 && index < total && entries.iter().all(valid_register) => {
                            match assemblies.push(parcel.peer.0, parcel.generation, transfer, index, total, entries) {
                                super::cluster_control_registers::SeedResult::Pending => continue,
                                super::cluster_control_registers::SeedResult::Full => { seed_backlog.push_back((transfer, parcel)); continue; }
                                super::cluster_control_registers::SeedResult::Rejected(reason) => { diagnostics.rejected("access_policy", parcel.peer.0, reason); continue; }
                                super::cluster_control_registers::SeedResult::Complete(entries) => {
                                let Some(changed) = registers.merge_reconciled(entries, valid_register) else { diagnostics.rejected("access_policy", parcel.peer.0, "contradictory seed"); continue; };
                                dirty |= changed;
                                if changed {
                                    for peer in server.cluster_connected_peers.load().iter() {
                                        if *peer != parcel.peer && seeds.len() < 256 && !seeds.contains(peer) { seeds.push_back(*peer); }
                                    }
                                }
                                }
                            }
                        }
                        Some(Frame::Reload) if persist => {
                            if let Ok(permit) = reload_done.clone().try_reserve_owned() { reload_pending = true; let revision = snapshot.load().revision; tokio::task::spawn_blocking(move || permit.send((revision, read_whitelist()))); }
                        }
                        _ => { diagnostics.rejected("access_policy", parcel.peer.0, "malformed frame"); },
                    }
                    None
                }
                _ = flush.tick() => {
                    if dirty {
                        let state = policy_view(&registers, snapshot.load().revision.wrapping_add(1));
                        snapshot.store(Arc::new(state.clone()));
                        kick_non_whitelisted(&server, &state);
                        for player in server.get_all_players() {
                            if let Some(ban) = state.active_ip_ban(player.client.address().ip()) {
                                player.kick(crate::net::DisconnectReason::Kicked, &pumpkin_util::text::TextComponent::text(ban.reason.clone()));
                            }
                        }
                        for ban in &state.banned_ips {
                            if state.active_ip_ban(ban.ip).is_some() { server.cluster_lobby_directory.disconnect_ip(ban.ip, &pumpkin_util::text::TextComponent::text(ban.reason.clone())); }
                        }
                        if persist { pending_save = Some((registers.clone(), state)); }
                        dirty = false;
                    }
                    if let Some(save) = pending_save.take() { if let Err(error) = save_tx.try_send(save) { pending_save = Some(error.into_inner()); } }
                    if shutting_down && drained && !reload_pending && !primary_reload_pending && pending_save.is_none() && outgoing.is_empty() { break; }
                    if primary_reload_pending {
                        let primaries = server.cluster_primary_peers.load();
                        if !primaries.is_empty() {
                            if let Some(bytes) = super::cluster_control_registers::encode(PREFIX, &Frame::Reload) {
                                for peer in primaries.iter() { outgoing.push_back((Some(*peer), bytes.clone())); }
                                primary_reload_pending = false;
                            }
                        }
                    }
                    let connected = server.cluster_connected_peers.load();
                    let generations = server.cluster_peer_generations.load();
                    for peer in connected.iter() {
                        let generation = generations.get(peer).copied();
                        if peers.get(peer) != Some(&generation) && seeds.len() < 256 {
                            if !seeds.contains(peer) { seeds.push_back(*peer); }
                            peers.insert(*peer, generation);
                        }
                    }
                    peers.retain(|peer, _| connected.contains(peer));
                    assemblies.retain(|peer, generation| generations.get(&pumpkin_cluster::identity::ServerId(peer)) == Some(&generation));
                    for (parcel, since) in &deferred_inbound {
                        if since.elapsed().as_secs() >= 10 { diagnostics.deferred("access_policy", parcel.peer.0, "authenticated source publication unavailable"); }
                    }
                    if pending_inbound.is_none() {
                        let boots = server.cluster_peer_incarnations.load();
                        pending_inbound = super::cluster_control_registers::take_ready_control(&mut deferred_inbound, |(parcel, _)| super::cluster_control_registers::control_publication_ready(parcel.generation, generations.get(&parcel.peer).copied(), boots.get(&parcel.peer).copied())).map(|(parcel, _)| parcel);
                    }
                    if pending_inbound.is_none() && deferred_inbound.len() < 256 {
                        if let Some(index) = seed_backlog.iter().position(|(transfer, parcel)|
                            generations.get(&parcel.peer).is_some_and(|generation| *generation > parcel.generation)
                                || assemblies.can_accept(parcel.peer.0, parcel.generation, *transfer)) {
                            pending_inbound = Some(seed_backlog.remove(index).unwrap().1);
                        }
                    }
                    if !shutting_down && outgoing.len() < 4032 {
                        if let Some(peer) = seeds.pop_front() {
                            if connected.contains(&peer) {
                                if let Some(frames) = super::cluster_control_registers::seed_frames(PREFIX, &registers) { outgoing.extend(frames.into_iter().map(|bytes| (Some(peer), bytes))); }
                            }
                        }
                    }
                    for _ in 0..16 {
                        let Some((peer, bytes)) = outgoing.front() else { break; };
                        let targets = match peer { Some(peer) if connected.contains(peer) => vec![*peer], Some(_) => { outgoing.pop_front(); continue; }, None => connected.iter().copied().collect() };
                        if targets.is_empty() || server.cluster_global_outbound.try_event(&server, targets, bytes.clone()) { outgoing.pop_front(); } else { break; }
                    }
                    None
                }
            };
            if let Some(command) = command {
                if matches!(command, AccessPolicyCommand::ReloadWhitelist) {
                    if persist {
                        if let Ok(permit) = reload_done.clone().try_reserve_owned() {
                            reload_pending = true;
                            let revision = snapshot.load().revision;
                            tokio::task::spawn_blocking(move || {
                                permit.send((revision, read_whitelist()))
                            });
                        }
                    } else {
                        primary_reload_pending = true;
                    }
                    continue;
                }
                let state = policy_view(&registers, snapshot.load().revision);
                let mut entries = Vec::new();
                for (key, value) in policy_updates(command, &state) {
                    let incarnation = server
                        .cluster_peer_incarnations
                        .load()
                        .get(&pumpkin_cluster::identity::ServerId(local))
                        .copied()
                        .unwrap_or(0);
                    let entry = registers.issue(local, incarnation, key, value);
                    if valid_register(&entry) {
                        registers.merge(entry.clone());
                        entries.push(entry);
                    }
                }
                if !entries.is_empty() {
                    for entry in entries {
                        if let Some(bytes) = super::cluster_control_registers::encode(
                            PREFIX,
                            &Frame::Mutation(vec![entry]),
                        ) {
                            outgoing.push_back((None, bytes));
                        }
                    }
                    dirty = true;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::cluster_control_registers::{self as control, Frame, Registers};

    fn apply(
        issuer: &mut Registers,
        replica: &mut Registers,
        command: AccessPolicyCommand,
    ) -> Vec<u8> {
        let before = policy_view(issuer, 0);
        let entries = policy_updates(command, &before)
            .into_iter()
            .map(|(key, value)| issuer.issue(2, 11, key, value))
            .collect::<Vec<_>>();
        for entry in &entries {
            issuer.merge(entry.clone());
        }
        let bytes = control::encode(PREFIX, &Frame::Mutation(entries)).unwrap();
        let Some(Frame::Mutation(entries)) = control::decode(PREFIX, &bytes) else {
            panic!("invalid policy frame")
        };
        assert_eq!(
            replica.merge_authenticated(2, 11, entries, valid_register),
            Some(true)
        );
        bytes
    }

    #[test]
    fn command_to_wire_to_snapshot_enforces_uuid_and_ip_tombstones() {
        let uuid = Uuid::from_u128(1);
        let ip = "192.0.2.7".parse().unwrap();
        let mut issuer = Registers::default();
        let mut replica = Registers::default();
        apply(
            &mut issuer,
            &mut replica,
            AccessPolicyCommand::SetEnabled(true),
        );
        let grant = apply(
            &mut issuer,
            &mut replica,
            AccessPolicyCommand::AddWhitelist(WhitelistRecord {
                uuid,
                name: "Alice".into(),
            }),
        );
        let ban = apply(
            &mut issuer,
            &mut replica,
            AccessPolicyCommand::BanIp(IpBanRecord {
                ip,
                created: time::OffsetDateTime::UNIX_EPOCH,
                expires: None,
                source: "console".into(),
                reason: "test".into(),
            }),
        );
        let before = policy_view(&replica, 4);
        assert!(before.enabled);
        assert!(before.is_whitelisted(uuid));
        assert!(before.active_ip_ban(ip).is_some());
        apply(
            &mut issuer,
            &mut replica,
            AccessPolicyCommand::RemoveWhitelist(uuid),
        );
        apply(&mut issuer, &mut replica, AccessPolicyCommand::PardonIp(ip));
        let bytes = serde_json::to_vec(&replica).unwrap();
        let mut restarted: Registers = serde_json::from_slice(&bytes).unwrap();
        for delayed in [grant, ban] {
            let Some(Frame::Mutation(entries)) = control::decode(PREFIX, &delayed) else {
                panic!("invalid delayed frame")
            };
            assert_eq!(
                restarted.merge_authenticated(2, 11, entries, valid_register),
                Some(false)
            );
        }
        let after = policy_view(&restarted, 6);
        assert!(!after.is_whitelisted(uuid));
        assert!(after.active_ip_ban(ip).is_none());
        assert!(before.is_whitelisted(uuid));
        assert!(before.active_ip_ban(ip).is_some());
    }
}
