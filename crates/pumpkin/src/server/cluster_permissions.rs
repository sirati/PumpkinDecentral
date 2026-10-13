use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::command::CommandSource;
use crate::plugin::player::player_permission_check::PlayerPermissionCheckEvent;

use super::Server;

pub fn uuid_level(server: &Server, uuid: uuid::Uuid) -> pumpkin_util::PermissionLvl {
    match server
        .cluster_admin_view
        .load()
        .operators
        .iter()
        .find(|entry| entry.uuid == *uuid.as_bytes())
        .map_or(0, |entry| entry.level)
    {
        1 => pumpkin_util::PermissionLvl::One,
        2 => pumpkin_util::PermissionLvl::Two,
        3 => pumpkin_util::PermissionLvl::Three,
        4 => pumpkin_util::PermissionLvl::Four,
        _ => pumpkin_util::PermissionLvl::Zero,
    }
}

tokio::task_local! {
    static DECISIONS: (uuid::Uuid, HashMap<String, bool>);
}

pub fn decision(uuid: uuid::Uuid, node: &str) -> Option<bool> {
    DECISIONS
        .try_with(|(issuer, values)| {
            if *issuer == uuid {
                values.get(node).copied()
            } else {
                None
            }
        })
        .ok()
        .flatten()
}

pub async fn with_source<F: Future>(
    server: &Arc<Server>,
    source: &CommandSource,
    future: F,
) -> F::Output {
    let Some(player) = source.output.as_player() else {
        return future.await;
    };
    if !server
        .plugin_manager
        .has_handlers::<PlayerPermissionCheckEvent>()
    {
        return future.await;
    }
    let Some(policy) = freeze_permission_policy(server) else {
        return DECISIONS
            .scope((player.gameprofile.id, HashMap::new()), future)
            .await;
    };
    let version = policy.version(player.gameprofile.id);
    let mut decisions = HashMap::new();
    let mut nodes = policy.permissions.nodes();
    nodes.sort_unstable();
    let requested_nodes = nodes.clone();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for node in nodes {
            let initial = policy.permissions.has_permission(
                &player.gameprofile.id,
                &node,
                policy.level(player.gameprofile.id),
            );
            let mut event = PlayerPermissionCheckEvent::new(player.clone(), node.clone(), initial);
            server.plugin_manager.fire(server, &mut event).await;
            decisions.insert(node, event.result);
        }
    })
    .await
    .is_ok();
    if !completed {
        decisions = requested_nodes
            .into_iter()
            .map(|node| (node, false))
            .collect();
        let _ = server
            .cluster_permission_failures
            .try_send(player.gameprofile.id);
    }
    let current_policy = freeze_permission_policy(server);
    let current = current_policy
        .as_ref()
        .and_then(|policy| policy.version(player.gameprofile.id));
    let same_handlers = current_policy
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(&current.handler_snapshot, &policy.handler_snapshot));
    if current != version || !same_handlers {
        decisions.clear();
    }
    if current == version && same_handlers {
        if let (Some(lease), Some(version)) = (
            super::cluster_world_owner::player_action_lease(server, &player),
            version,
        ) {
            let prepared = PreparedPlayerPermissions {
                lease,
                version,
                values: Arc::new(decisions.clone()),
                handler_snapshot: Arc::clone(&policy.handler_snapshot),
            };
            server.cluster_prepared_permissions.rcu(|current| {
                let mut next = (**current).clone();
                if next.len() < 4096 || next.contains_key(&lease.key.0) {
                    next.insert(lease.key.0, prepared.clone());
                }
                next
            });
        }
    }
    DECISIONS
        .scope((player.gameprofile.id, decisions), future)
        .await
}

#[derive(Clone)]
pub struct PreparedPlayerPermissions {
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub version: u64,
    pub values: Arc<HashMap<String, bool>>,
    pub handler_snapshot: Arc<dyn Send + Sync>,
}

#[derive(Clone)]
pub struct FrozenPermissionPolicy {
    pub issuer: pumpkin_cluster::realm_completion::PeerGeneration,
    permissions: pumpkin_util::permission::PermissionPolicySnapshot,
    administration: Arc<pumpkin_cluster::admin_sync::AdminSnapshot>,
    plugins: Arc<HashMap<uuid::Uuid, PreparedPlayerPermissions>>,
    plugin_required: bool,
    handler_snapshot: Arc<dyn Send + Sync>,
}

impl FrozenPermissionPolicy {
    fn level(&self, uuid: uuid::Uuid) -> pumpkin_util::PermissionLvl {
        match self
            .administration
            .operators
            .iter()
            .find(|grant| grant.uuid == *uuid.as_bytes())
            .map_or(0, |grant| grant.level)
        {
            1 => pumpkin_util::PermissionLvl::One,
            2 => pumpkin_util::PermissionLvl::Two,
            3 => pumpkin_util::PermissionLvl::Three,
            4 => pumpkin_util::PermissionLvl::Four,
            _ => pumpkin_util::PermissionLvl::Zero,
        }
    }

    pub fn version(&self, uuid: uuid::Uuid) -> Option<u64> {
        let bytes = self.permissions.fingerprint_bytes(&uuid).ok()?;
        let mut version = xxhash_rust::xxh64::xxh64(&bytes, 0);
        let mut grants = self
            .administration
            .operators
            .iter()
            .map(|entry| (entry.uuid, entry.level))
            .collect::<Vec<_>>();
        grants.sort_unstable();
        for (uuid, level) in grants {
            version = xxhash_rust::xxh64::xxh64(&uuid, version);
            version = xxhash_rust::xxh64::xxh64(&[level], version);
        }
        Some(version)
    }
}

pub fn freeze_permission_policy(server: &Server) -> Option<FrozenPermissionPolicy> {
    let local = pumpkin_cluster::identity::ServerId(server.advanced_config.cluster.server_id);
    let generation = server
        .cluster_peer_incarnations
        .load()
        .get(&local)
        .copied()?;
    Some(FrozenPermissionPolicy {
        issuer: pumpkin_cluster::realm_completion::PeerGeneration {
            server: local,
            generation,
        },
        permissions: server.permission_manager.snapshot(),
        administration: server.cluster_admin_view.load_full(),
        plugins: server.cluster_prepared_permissions.load_full(),
        plugin_required: server
            .plugin_manager
            .has_handlers::<PlayerPermissionCheckEvent>(),
        handler_snapshot: server.plugin_manager.handler_snapshot(),
    })
}

pub fn capture_permission_for_action(
    viewer: pumpkin_cluster::canonical_action::LivePlayerLease,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    tick: pumpkin_cluster::time::TickStamp,
    node: &str,
    policy: &FrozenPermissionPolicy,
) -> Option<super::cluster_player_commands::CapturedPermission> {
    if source != policy.issuer || viewer.gid.server != source.server {
        return None;
    }
    let version = policy.version(viewer.key.0)?;
    let allowed = if policy.plugin_required {
        let entry = policy.plugins.get(&viewer.key.0)?;
        if entry.lease != viewer
            || entry.version != version
            || !Arc::ptr_eq(&entry.handler_snapshot, &policy.handler_snapshot)
        {
            return None;
        }
        *entry.values.get(node)?
    } else {
        policy
            .permissions
            .has_permission(&viewer.key.0, node, policy.level(viewer.key.0))
    };
    Some(super::cluster_player_commands::CapturedPermission {
        issuer: source,
        viewer,
        tick,
        policy_revision: policy.administration.revision,
        decision_version: version,
        node: node.to_owned(),
        allowed,
    })
}

pub fn prepare_for_action(server: &Server, player: &Arc<crate::entity::player::Player>) -> bool {
    server
        .cluster_permission_prepare
        .try_send(Arc::downgrade(player))
        .is_ok()
}

pub fn spawn_preparation(
    server: &Arc<Server>,
    mut requests: tokio::sync::mpsc::Receiver<std::sync::Weak<crate::entity::player::Player>>,
) {
    let weak_server = Arc::downgrade(server);
    server.spawn_task(async move {
        while let Some(player) = requests.recv().await {
            let Some(server) = weak_server.upgrade() else {
                break;
            };
            let Some(player) = player.upgrade() else {
                continue;
            };
            let Some(lease) = super::cluster_world_owner::player_action_lease(&server, &player)
            else {
                continue;
            };
            let Some(policy) = freeze_permission_policy(&server) else {
                continue;
            };
            if policy.plugins.get(&lease.key.0).is_some_and(|entry| {
                entry.lease == lease
                    && Arc::ptr_eq(&entry.handler_snapshot, &policy.handler_snapshot)
                    && Some(entry.version) == policy.version(lease.key.0)
            }) {
                continue;
            }
            let source = player.get_command_source(&server);
            with_source(&server, &source, async {}).await;
        }
    });
}

pub fn spawn_diagnostics(
    server: &Arc<Server>,
    mut failures: tokio::sync::mpsc::Receiver<uuid::Uuid>,
) {
    server.spawn_task(async move {
        let mut last = HashMap::<uuid::Uuid, std::time::Instant>::new();
        let mut overflow = None::<std::time::Instant>;
        while let Some(uuid) = failures.recv().await {
            let now = std::time::Instant::now();
            last.retain(|_, previous| now.duration_since(*previous).as_secs() < 300);
            if last.contains_key(&uuid) { continue; }
            if last.len() < 4096 {
                last.insert(uuid, now);
                tracing::warn!(%uuid, "cluster permission callback deadline exceeded; dispatch denied");
            } else if overflow.is_none_or(|previous| now.duration_since(previous).as_secs() >= 300) {
                overflow = Some(now);
                tracing::warn!("cluster permission callback failures exceed diagnostic capacity");
            }
        }
    });
}

#[derive(Clone)]
pub(crate) struct FrozenCommandPermissions {
    pub principal: pumpkin_cluster::player_command_control::CommandPrincipal,
    pub permission: pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot,
    pub origin: pumpkin_cluster::realm_completion::PeerGeneration,
    pub source_session: u64,
    policy: FrozenPermissionPolicy,
    decisions: Option<Arc<HashMap<String, bool>>>,
}

tokio::task_local! {
    static COMMAND_PERMISSIONS: FrozenCommandPermissions;
}

impl FrozenCommandPermissions {
    pub(crate) fn matches_source(&self, source: &CommandSource) -> bool {
        use crate::command::CommandSender;
        use pumpkin_cluster::player_command_control::CommandPrincipal;
        match (&self.principal, &source.output) {
            (CommandPrincipal::Console, CommandSender::Console | CommandSender::Rcon(_)) => true,
            (
                CommandPrincipal::Player { gid, uuid, .. },
                CommandSender::Player(player) | CommandSender::Lobby(player),
            ) => {
                player.cluster_gid() == Some(*gid)
                    && *player.gameprofile.id.as_bytes() == *uuid
                    && player
                        .cluster_owner_install_generation
                        .load(std::sync::atomic::Ordering::Acquire)
                        == self.source_session
            }
            (CommandPrincipal::Player { gid, uuid, .. }, CommandSender::PreJoin(player)) => {
                player.gid == *gid
                    && *player.profile.id.as_bytes() == *uuid
                    && player.session_generation == self.source_session
            }
            _ => false,
        }
    }

    pub(crate) fn allows(&self, node: &str) -> bool {
        match &self.principal {
            pumpkin_cluster::player_command_control::CommandPrincipal::Console => true,
            pumpkin_cluster::player_command_control::CommandPrincipal::Player { uuid, .. } => {
                self.decision(uuid::Uuid::from_bytes(*uuid), node) == Some(true)
            }
        }
    }

    fn decision(&self, uuid: uuid::Uuid, node: &str) -> Option<bool> {
        let pumpkin_cluster::player_command_control::CommandPrincipal::Player {
            uuid: subject, ..
        } = &self.principal
        else {
            return None;
        };
        if uuid.as_bytes() != subject {
            return None;
        }
        Some(self.decisions.as_ref().map_or_else(
            || {
                self.policy
                    .permissions
                    .has_permission(&uuid, node, self.policy.level(uuid))
            },
            |decisions| decisions.get(node).copied().unwrap_or(false),
        ))
    }
}

pub(crate) fn frozen_command_decision(uuid: uuid::Uuid, node: &str) -> Option<bool> {
    COMMAND_PERMISSIONS
        .try_with(|permissions| permissions.decision(uuid, node))
        .ok()
        .flatten()
}

pub(crate) fn active_command_permissions(
    source: &CommandSource,
) -> Option<FrozenCommandPermissions> {
    COMMAND_PERMISSIONS
        .try_with(|permissions| {
            permissions
                .matches_source(source)
                .then(|| permissions.clone())
        })
        .ok()
        .flatten()
}

pub(crate) fn with_command_permissions_sync<R>(
    permissions: FrozenCommandPermissions,
    execute: impl FnOnce() -> R,
) -> R {
    COMMAND_PERMISSIONS.sync_scope(permissions, execute)
}

pub(crate) fn capture_command_permissions(
    server: &Server,
    source: &CommandSource,
) -> Option<FrozenCommandPermissions> {
    use crate::command::CommandSender;
    use pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot;
    use pumpkin_cluster::player_command_control::CommandPrincipal;
    if let Some(permissions) = active_command_permissions(source) {
        return Some(permissions);
    }
    let policy = freeze_permission_policy(server)?;
    let (identity, source_session, player_callbacks) = match &source.output {
        CommandSender::Console | CommandSender::Rcon(_) => (None, 0, false),
        CommandSender::Player(player) | CommandSender::Lobby(player) => (
            Some((player.cluster_gid()?, player.gameprofile.id)),
            player
                .cluster_owner_install_generation
                .load(std::sync::atomic::Ordering::Acquire),
            policy.plugin_required,
        ),
        CommandSender::PreJoin(player) => (
            Some((player.gid, player.profile.id)),
            player.session_generation,
            false,
        ),
        _ => return None,
    };
    let (principal, permission, decisions) = match identity {
        None => (
            CommandPrincipal::Console,
            CommandPermissionSnapshot {
                revision: 0,
                level: 4,
            },
            None,
        ),
        Some((gid, uuid)) => {
            if gid.server != policy.issuer.server || source_session == 0 {
                return None;
            }
            let level = policy
                .administration
                .operators
                .iter()
                .find(|grant| grant.uuid == *uuid.as_bytes())
                .map_or(0, |grant| grant.level);
            let revision = super::cluster_player_commands::permission_revision_snapshot(
                &policy.administration,
            );
            let principal = CommandPrincipal::Player {
                gid,
                uuid: *uuid.as_bytes(),
                permission_revision: revision,
            };
            let permission = CommandPermissionSnapshot { revision, level };
            if !permission.valid_for(&principal) {
                return None;
            }
            let decisions = if player_callbacks {
                Some(Arc::new(
                    DECISIONS
                        .try_with(|(subject, decisions)| {
                            (*subject == uuid).then(|| decisions.clone())
                        })
                        .ok()
                        .flatten()?,
                ))
            } else {
                None
            };
            (principal, permission, decisions)
        }
    };
    Some(FrozenCommandPermissions {
        principal,
        permission,
        origin: policy.issuer,
        source_session,
        policy,
        decisions,
    })
}

#[cfg(test)]
mod command_tests {
    use super::*;
    use pumpkin_cluster::admin_sync::{AdminSnapshot, OpGrant};
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot;
    use pumpkin_cluster::player_command_control::CommandPrincipal;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use pumpkin_util::permission::{Permission, PermissionDefault, PermissionManager};

    fn fixture() -> (
        PermissionManager,
        arc_swap::ArcSwap<AdminSnapshot>,
        FrozenCommandPermissions,
        uuid::Uuid,
    ) {
        let uuid = uuid::Uuid::from_bytes([3; 16]);
        let manager = PermissionManager::new();
        manager
            .register_permission(Permission::new(
                "minecraft:command.kill",
                "test",
                PermissionDefault::Op(pumpkin_util::PermissionLvl::Two),
            ))
            .unwrap();
        let administration = arc_swap::ArcSwap::from_pointee(AdminSnapshot {
            revision: 1,
            operators: vec![OpGrant::new(*uuid.as_bytes(), "issuer".into(), 2, false)],
            banned_players: Vec::new(),
        });
        let origin = PeerGeneration {
            server: ServerId(2),
            generation: 9,
        };
        let policy = FrozenPermissionPolicy {
            issuer: origin,
            permissions: manager.snapshot(),
            administration: administration.load_full(),
            plugins: Arc::new(HashMap::new()),
            plugin_required: false,
            handler_snapshot: Arc::new(()),
        };
        let revision = super::super::cluster_player_commands::permission_revision_snapshot(
            &policy.administration,
        );
        let permissions = FrozenCommandPermissions {
            principal: CommandPrincipal::Player {
                gid: GlobalPlayerId::new(origin.server, PlayerSlot(1)),
                uuid: *uuid.as_bytes(),
                permission_revision: revision,
            },
            permission: CommandPermissionSnapshot { revision, level: 2 },
            origin,
            source_session: 7,
            policy,
            decisions: None,
        };
        (manager, administration, permissions, uuid)
    }

    #[test]
    fn accepted_chain_keeps_original_permissions_after_deop() {
        let (manager, administration, permissions, uuid) = fixture();
        administration.store(Arc::new(AdminSnapshot {
            revision: 2,
            operators: Vec::new(),
            banned_players: Vec::new(),
        }));
        manager.set_permission(uuid, "minecraft:command.kill", false);
        assert!(!manager.has_permission(
            &uuid,
            "minecraft:command.kill",
            pumpkin_util::PermissionLvl::Zero
        ));
        assert_eq!(
            frozen_command_decision(uuid, "minecraft:command.kill"),
            None
        );
        with_command_permissions_sync(permissions, || {
            assert_eq!(
                frozen_command_decision(uuid, "minecraft:command.kill"),
                Some(true)
            );
            assert_eq!(
                frozen_command_decision(uuid::Uuid::from_bytes([4; 16]), "minecraft:command.kill"),
                None
            );
        });
        assert_eq!(
            frozen_command_decision(uuid, "minecraft:command.kill"),
            None
        );
    }

    #[test]
    fn captured_plugin_allow_preserves_actual_nonoperator_level_and_exact_node() {
        let (_, _, mut permissions, _) = fixture();
        permissions.permission.level = 0;
        permissions.decisions = Some(Arc::new(HashMap::from([(
            "minecraft:command.summon".into(),
            true,
        )])));
        assert!(permissions.permission.valid_for(&permissions.principal));
        assert!(permissions.allows("minecraft:command.summon"));
        assert!(!permissions.allows("minecraft:command.kill"));
        permissions.decisions = Some(Arc::new(HashMap::from([(
            "minecraft:command.summon".into(),
            false,
        )])));
        assert!(!permissions.allows("minecraft:command.summon"));
    }

    #[test]
    fn captured_plugin_denial_is_not_replaced_by_new_policy() {
        let (manager, _, mut permissions, uuid) = fixture();
        permissions.decisions = Some(Arc::new(HashMap::from([(
            "minecraft:command.kill".into(),
            false,
        )])));
        manager.set_permission(uuid, "minecraft:command.kill", true);
        with_command_permissions_sync(permissions, || {
            assert_eq!(
                frozen_command_decision(uuid, "minecraft:command.kill"),
                Some(false)
            );
        });
    }
}
