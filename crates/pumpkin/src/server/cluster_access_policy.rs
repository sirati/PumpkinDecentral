use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::data::VanillaData;
use crate::data::banlist_serializer::BannedIpEntry;
use crate::data::banned_ip::BannedIpList;
use crate::data::whitelist::WhitelistConfig;
use crate::data::{LoadJSONConfiguration, SaveJSONConfiguration};

#[derive(Clone, PartialEq, Eq)]
pub struct WhitelistRecord {
    pub uuid: Uuid,
    pub name: String,
}

#[derive(Clone)]
pub struct IpBanRecord {
    pub ip: IpAddr,
    pub created: time::OffsetDateTime,
    pub source: String,
    pub expires: Option<time::OffsetDateTime>,
    pub reason: String,
}

#[derive(Clone, Default)]
pub struct AccessPolicySnapshot {
    pub revision: u64,
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
    pub fn new(data: &VanillaData) -> (Self, mpsc::Receiver<AccessPolicyCommand>) {
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
        (
            Self {
                tx,
                snapshot: Arc::new(ArcSwap::from_pointee(AccessPolicySnapshot {
                    revision: 0,
                    whitelist,
                    banned_ips,
                })),
            },
            rx,
        )
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<AccessPolicySnapshot> {
        self.snapshot.load_full()
    }

    pub fn try_submit(&self, command: AccessPolicyCommand) -> bool {
        self.tx.try_send(command).is_ok()
    }
}

fn save_snapshot(snapshot: &AccessPolicySnapshot) {
    WhitelistConfig {
        whitelist: snapshot
            .whitelist
            .iter()
            .map(|entry| {
                pumpkin_config::whitelist::WhitelistEntry::new(entry.uuid, entry.name.clone())
            })
            .collect(),
    }
    .save();
    BannedIpList {
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
    }
    .save();
}

pub(crate) fn kick_non_whitelisted(server: &super::Server, policy: &AccessPolicySnapshot) {
    if !server.basic_config.enforce_whitelist || !server.white_list.load(Ordering::Relaxed) {
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
}

pub fn spawn(server: &Arc<super::Server>, mut rx: mpsc::Receiver<AccessPolicyCommand>) {
    let snapshot = Arc::clone(&server.cluster_access_policy.snapshot);
    let weak_server = Arc::downgrade(server);
    let persist = server.advanced_config.cluster.enabled
        && matches!(
            server.advanced_config.cluster.role,
            pumpkin_config::ClusterRole::Primary
        );
    let (save_tx, mut save_rx) = mpsc::channel::<AccessPolicySnapshot>(1);
    if persist {
        tokio::spawn(async move {
            while let Some(state) = save_rx.recv().await {
                let _ = tokio::task::spawn_blocking(move || save_snapshot(&state)).await;
            }
        });
    }
    tokio::spawn(async move {
        let mut state = snapshot.load().as_ref().clone();
        let mut pending_save = None;
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            let command = tokio::select! {
                command = rx.recv() => command,
                _ = flush.tick() => None,
            };
            if let Some(command) = command {
                let recheck_players = matches!(
                    command,
                    AccessPolicyCommand::RemoveWhitelist(_)
                        | AccessPolicyCommand::InstallWhitelist(_)
                );
                let changed = match command {
                    AccessPolicyCommand::AddWhitelist(entry) => {
                        if let Some(old) = state
                            .whitelist
                            .iter_mut()
                            .find(|old| old.uuid == entry.uuid)
                        {
                            if *old == entry {
                                false
                            } else {
                                *old = entry;
                                true
                            }
                        } else {
                            state.whitelist.push(entry);
                            true
                        }
                    }
                    AccessPolicyCommand::RemoveWhitelist(uuid) => {
                        let before = state.whitelist.len();
                        state.whitelist.retain(|entry| entry.uuid != uuid);
                        before != state.whitelist.len()
                    }
                    AccessPolicyCommand::BanIp(entry) => {
                        state.banned_ips.retain(|old| old.ip != entry.ip);
                        state.banned_ips.push(entry);
                        true
                    }
                    AccessPolicyCommand::PardonIp(ip) => {
                        let before = state.banned_ips.len();
                        state.banned_ips.retain(|entry| entry.ip != ip);
                        before != state.banned_ips.len()
                    }
                    AccessPolicyCommand::ReloadWhitelist => {
                        if let Some(server) = weak_server.upgrade() {
                            let tx = server.cluster_access_policy.tx.clone();
                            tokio::spawn(async move {
                                if let Ok(entries) = tokio::task::spawn_blocking(|| {
                                    WhitelistConfig::load()
                                        .whitelist
                                        .into_iter()
                                        .map(|entry| WhitelistRecord {
                                            uuid: entry.uuid,
                                            name: entry.name,
                                        })
                                        .collect()
                                })
                                .await
                                {
                                    let _ = tx
                                        .send(AccessPolicyCommand::InstallWhitelist(entries))
                                        .await;
                                }
                            });
                        }
                        false
                    }
                    AccessPolicyCommand::InstallWhitelist(entries) => {
                        state.whitelist = entries;
                        true
                    }
                };
                if changed {
                    state.revision = state.revision.wrapping_add(1);
                    snapshot.store(Arc::new(state.clone()));
                    if recheck_players && let Some(server) = weak_server.upgrade() {
                        kick_non_whitelisted(&server, &state);
                    }
                    if persist {
                        pending_save = Some(state.clone());
                    }
                }
            } else if rx.is_closed() {
                break;
            }
            if let Some(save) = pending_save.take() {
                if let Err(mpsc::error::TrySendError::Full(save)) = save_tx.try_send(save) {
                    pending_save = Some(save);
                }
            }
        }
    });
}
