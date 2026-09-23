//! Cluster-wide admin enforcement.
//!
//! [`pumpkin_cluster::admin_sync`] already defines the wire parcels for
//! op/ban mutations as well as kick and spectate requests. This module hooks
//! those parcels up to the [`Server`] stores so mutations propagate across
//! peers, persist on the primary, and kicks/spectates reach players no matter
//! which node hosts them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::admin_sync::{
    AdminAudit, AdminControlMessage, AdminMutation, AdminSnapshot, BanEntry, BanRevoke,
    KickRequest, OpGrant, OpRevoke, SpectateRequest, encode_admin_snapshot_frames,
    encode_control_message, is_valid_op_level, should_deliver_kick_locally, submit_admin_mutation,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_config::ClusterRole;
use pumpkin_util::GameMode;
use pumpkin_util::PermissionLvl;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::{Color, NamedColor};
use tracing::{debug, info, warn};

use super::Server;
use super::cluster_global_outbound::EventReservation;
use crate::data::SaveJSONConfiguration;
use crate::entity::EntityBase;
use crate::net::DisconnectReason;

static ADMIN_MUTATION_APPLIED: AtomicU64 = AtomicU64::new(0);
static ADMIN_MUTATION_SENT: AtomicU64 = AtomicU64::new(0);
static ADMIN_MUTATION_UNSENT: AtomicU64 = AtomicU64::new(0);
static ADMIN_BROADCAST_WARN_LAST_MILLIS: AtomicU64 = AtomicU64::new(0);
static ADMIN_KICK_DELIVERED: AtomicU64 = AtomicU64::new(0);
static ADMIN_SPECTATE_DELIVERED: AtomicU64 = AtomicU64::new(0);

/// Number of inbound admin mutations applied to the local [`Server`] stores.
#[must_use]
pub fn admin_mutations_applied() -> u64 {
    ADMIN_MUTATION_APPLIED.load(Ordering::Relaxed)
}

#[must_use]
pub fn admin_mutations_sent() -> u64 {
    ADMIN_MUTATION_SENT.load(Ordering::Relaxed)
}

#[must_use]
pub fn admin_mutations_unsent() -> u64 {
    ADMIN_MUTATION_UNSENT.load(Ordering::Relaxed)
}

pub(crate) fn admin_broadcast_warn_cooldown_elapsed() -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0);
    let last = ADMIN_BROADCAST_WARN_LAST_MILLIS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000 {
        return false;
    }
    ADMIN_BROADCAST_WARN_LAST_MILLIS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// Number of inbound cluster kicks delivered locally.
#[must_use]
pub fn kicks_delivered() -> u64 {
    ADMIN_KICK_DELIVERED.load(Ordering::Relaxed)
}

/// Number of inbound cluster spectate requests delivered locally.
#[must_use]
pub fn spectates_delivered() -> u64 {
    ADMIN_SPECTATE_DELIVERED.load(Ordering::Relaxed)
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

pub fn mutation_pending_primary(server: &Server) -> bool {
    cluster_enabled(server)
}

pub fn defer_local_admin_apply(server: &Server) -> bool {
    cluster_enabled(server) && matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
}

pub(crate) fn notify_durable_admin_ack(server: &Server, issuer: &str) {
    let message = TextComponent::text("Cluster admin change is durable on the primary.");
    if server
        .cluster_lobby_directory
        .route_message(issuer, &message)
    {
        return;
    }
    if let Some(player) = server.get_player_by_name(issuer) {
        player.send_system_message(&message);
    } else {
        info!(
            issuer,
            "cluster admin change durable on primary; issuer is offline"
        );
    }
}

fn local_server_id(server: &Server) -> ServerId {
    ServerId(server.advanced_config.cluster.server_id)
}

pub(crate) fn stores_authoritative_files(server: &Server) -> bool {
    !cluster_enabled(server) || matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
}

fn persist_on_primary(server: &Server, mutation: &AdminMutation) -> bool {
    let Some(handle) = server.primary_save.as_ref() else {
        return false;
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0);
    if submit_admin_mutation(
        handle,
        super::cluster::disciplined_tick_stamp(millis),
        mutation,
    )
    .is_err()
    {
        if admin_broadcast_warn_cooldown_elapsed() {
            warn!("cluster admin mutation persist queue full");
        }
        return false;
    }
    true
}

pub struct PreparedAdminMutation {
    mutation: AdminMutation,
    outbound: Option<PreparedAdminRoute>,
}

pub(crate) struct LocalAdminMutation {
    pub id: [u8; 16],
    pub audit: AdminAudit,
}

enum PreparedAdminRoute {
    Primary(
        tokio::sync::mpsc::OwnedPermit<LocalAdminMutation>,
        LocalAdminMutation,
    ),
    Secondary(EventReservation, Vec<u8>),
}

pub fn admin_busy_error() -> crate::command::errors::command_syntax_error::CommandSyntaxError {
    use crate::command::errors::command_syntax_error::CommandSyntaxError;
    use crate::command::errors::error_types::CommandErrorType;

    static CLUSTER_ADMIN_BUSY: CommandErrorType<0> =
        CommandErrorType::new("cluster.admin.busy", "cluster.admin.busy");
    CommandSyntaxError::create_without_context(
        &CLUSTER_ADMIN_BUSY,
        TextComponent::text("Cluster admin operation could not be queued; try again."),
    )
}

pub fn prepare_admin_mutation(
    server: &Server,
    mutation: AdminMutation,
    issuer: &str,
) -> Option<PreparedAdminMutation> {
    if !cluster_enabled(server) {
        return Some(PreparedAdminMutation {
            mutation,
            outbound: None,
        });
    }
    let audit = AdminAudit::new(issuer.to_string(), mutation.clone());
    let is_primary = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let id = *uuid::Uuid::new_v4().as_bytes();
    let outbound = if is_primary {
        let route = server.cluster_admin_local_route.load_full()?;
        let reservation = route.as_ref().clone().try_reserve_owned().ok()?;
        PreparedAdminRoute::Primary(reservation, LocalAdminMutation { id, audit })
    } else {
        let bytes =
            encode_control_message(&AdminControlMessage::SequencedMutation { id, audit }).ok()?;
        let reservation = server
            .cluster_global_outbound
            .try_reserve_primary_event(id)?;
        PreparedAdminRoute::Secondary(reservation, bytes)
    };
    Some(PreparedAdminMutation {
        mutation,
        outbound: Some(outbound),
    })
}

pub fn commit_admin_mutation(server: &Server, prepared: PreparedAdminMutation) {
    let PreparedAdminMutation { mutation, outbound } = prepared;
    let outbound = match outbound {
        Some(PreparedAdminRoute::Primary(reservation, command)) => {
            reservation.send(command);
            ADMIN_MUTATION_SENT.fetch_add(1, Ordering::Relaxed);
            return;
        }
        other => other,
    };
    match &mutation {
        AdminMutation::GrantOp(grant) => {
            if let Some(level) = permission_level_from_u8(grant.level) {
                super::cluster_presence::publish_permission(
                    server,
                    uuid::Uuid::from_bytes(grant.uuid),
                    Some(level),
                );
            }
        }
        AdminMutation::RevokeOp(revoke) => {
            super::cluster_presence::publish_permission(
                server,
                uuid::Uuid::from_bytes(revoke.uuid),
                None,
            );
        }
        AdminMutation::AddBan(_) | AdminMutation::RemoveBan(_) => {}
    }
    let revision = server.cluster_admin_view.load().revision;
    apply_mutation_to_view(server, revision, &mutation);
    if !cluster_enabled(server) {
        return;
    }
    let _ = persist_on_primary(server, &mutation);
    if let Some(PreparedAdminRoute::Secondary(reservation, bytes)) = outbound {
        reservation.commit(bytes);
        ADMIN_MUTATION_SENT.fetch_add(1, Ordering::Relaxed);
    }
}

fn try_broadcast(server: &Server, message: &AdminControlMessage) -> usize {
    let peers = server.cluster_connected_peers.load();
    if peers.is_empty() {
        ADMIN_MUTATION_UNSENT.fetch_add(1, Ordering::Relaxed);
        if admin_broadcast_warn_cooldown_elapsed() {
            warn!("cluster admin broadcast dropped: no peers");
        }
        return 0;
    }
    let Ok(bytes) = encode_control_message(message) else {
        warn!("cluster admin control encode failed");
        return 0;
    };
    let accepted =
        server
            .cluster_global_outbound
            .try_event(server, peers.iter().copied().collect(), bytes);
    let sent = if accepted { peers.len() } else { 0 };
    ADMIN_MUTATION_SENT.fetch_add(sent as u64, Ordering::Relaxed);
    let unsent = peers.len().saturating_sub(sent);
    if unsent > 0 {
        ADMIN_MUTATION_UNSENT.fetch_add(unsent as u64, Ordering::Relaxed);
        if admin_broadcast_warn_cooldown_elapsed() {
            warn!(
                sent = sent,
                peers = peers.len(),
                "cluster admin broadcast partially dropped: mesh queue full"
            );
        }
    }
    sent
}

fn try_send_to_host(server: &Server, host: u16, message: &AdminControlMessage) -> bool {
    let Ok(bytes) = encode_control_message(message) else {
        warn!("cluster admin control encode failed");
        return false;
    };
    server
        .cluster_global_outbound
        .try_event(server, vec![ServerId(host)], bytes)
}

/// The primary is the mesh fan-out point for inbound administrative changes.
///
/// A secondary commonly has only the primary as a pinned peer.  Its local
/// `/ban` or `/pardon` therefore reaches the primary first; the primary must
/// immediately send that same idempotent mutation to every secondary rather
/// than waiting for a later reconnect/seed pass.
const fn should_relay_inbound_mutation(role: ClusterRole) -> bool {
    matches!(role, ClusterRole::Primary)
}

pub(crate) fn relay_inbound_mutation_from_primary(
    server: &Server,
    message: &AdminControlMessage,
) -> bool {
    if !should_relay_inbound_mutation(server.advanced_config.cluster.role) {
        return true;
    }
    let peers = server.cluster_connected_peers.load();
    if peers.is_empty() {
        return true;
    }
    let Ok(bytes) = encode_control_message(message) else {
        return false;
    };
    server
        .cluster_global_outbound
        .try_event(server, peers.iter().copied().collect(), bytes)
}

/// Propagates a locally-applied admin mutation to peers and persists it on the primary.
pub fn propagate_admin_mutation(server: &Server, mutation: &AdminMutation, issuer: &str) {
    if !cluster_enabled(server) {
        return;
    }
    let _ = persist_on_primary(server, mutation);
    let sent = try_broadcast(
        server,
        &AdminControlMessage::MutationWithAudit(AdminAudit::new(
            issuer.to_string(),
            mutation.clone(),
        )),
    );
    if sent > 0 {
        debug!(peers = sent, "cluster admin mutation broadcast");
    }
}

pub fn publish_admin_snapshot_to_peer(server: &Server, peer: u16) -> bool {
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
        let Ok(bytes) = encode_control_message(&AdminControlMessage::SnapshotRequest) else {
            return false;
        };
        return server
            .cluster_global_outbound
            .try_event(server, vec![ServerId(peer)], bytes);
    }
    server
        .cluster_admin_seed_route
        .load_full()
        .is_some_and(|route| route.try_send(ServerId(peer)).is_ok())
}

pub(crate) fn snapshot_from_stores(server: &Server, revision: u64) -> AdminSnapshot {
    let mut grants: Vec<OpGrant> = {
        let guard = server
            .data
            .operator_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .ops
            .iter()
            .map(|op| {
                OpGrant::new(
                    *op.uuid.as_bytes(),
                    op.name.clone(),
                    op.level as u8,
                    op.bypasses_player_limit,
                )
            })
            .collect()
    };
    grants.sort_unstable_by_key(|grant| grant.uuid);
    let mut bans: Vec<BanEntry> = {
        let guard = server
            .data
            .banned_player_list
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .banned_players
            .iter()
            .map(|entry| {
                BanEntry::new(
                    *entry.uuid.as_bytes(),
                    entry.name.clone(),
                    entry.source.clone(),
                    entry.reason.clone(),
                    entry.created.unix_timestamp(),
                    entry.expires.map(|expires| expires.unix_timestamp()),
                )
            })
            .collect()
    };
    bans.sort_unstable_by_key(|ban| ban.uuid);
    AdminSnapshot {
        revision,
        operators: grants,
        banned_players: bans,
    }
}

pub(crate) fn snapshot_with_revision(server: &Server, revision: u64) -> AdminSnapshot {
    let mut snapshot = server.cluster_admin_view.load().as_ref().clone();
    snapshot.revision = revision;
    snapshot
}

fn apply_mutation_to_view(server: &Server, revision: u64, mutation: &AdminMutation) {
    let mut snapshot = snapshot_with_revision(server, revision);
    match mutation {
        AdminMutation::GrantOp(grant) => {
            snapshot.operators.retain(|entry| entry.uuid != grant.uuid);
            snapshot.operators.push(grant.clone());
            snapshot.operators.sort_unstable_by_key(|entry| entry.uuid);
        }
        AdminMutation::RevokeOp(revoke) => {
            snapshot.operators.retain(|entry| entry.uuid != revoke.uuid);
        }
        AdminMutation::AddBan(ban) => {
            snapshot
                .banned_players
                .retain(|entry| entry.uuid != ban.uuid);
            snapshot.banned_players.push(ban.clone());
            snapshot
                .banned_players
                .sort_unstable_by_key(|entry| entry.uuid);
        }
        AdminMutation::RemoveBan(revoke) => {
            snapshot
                .banned_players
                .retain(|entry| entry.uuid != revoke.uuid);
        }
    }
    server.cluster_admin_view.store(Arc::new(snapshot));
}

pub(crate) fn enqueue_admin_snapshot(server: &Server, peer: ServerId, revision: u64) -> bool {
    let Some(reservation) = server
        .cluster_global_outbound
        .try_reserve_snapshot(server, peer)
    else {
        return false;
    };
    let snapshot = snapshot_with_revision(server, revision);
    let frames = match encode_admin_snapshot_frames(&snapshot) {
        Ok(frames) => frames,
        Err(error) => {
            if admin_broadcast_warn_cooldown_elapsed() {
                tracing::error!(%error, "cluster admin snapshot encoding failed");
            }
            return false;
        }
    };
    reservation.commit(frames);
    true
}

pub(crate) fn apply_admin_snapshot(server: &Server, snapshot: &AdminSnapshot) -> bool {
    if snapshot.operators.len() > 4096 || snapshot.banned_players.len() > 4096 {
        return false;
    }
    let mut seen_ops = std::collections::HashSet::new();
    let mut permissions = std::collections::HashMap::new();
    for grant in &snapshot.operators {
        let Some(level) = permission_level_from_u8(grant.level) else {
            return false;
        };
        if grant.name.is_empty() || grant.name.len() > 256 || !seen_ops.insert(grant.uuid) {
            return false;
        }
        permissions.insert(grant.uuid, level);
    }
    let mut seen_bans = std::collections::HashSet::new();
    for ban in &snapshot.banned_players {
        let Some(created) = epoch_to_datetime(ban.created_epoch_secs) else {
            return false;
        };
        let expires = match ban.expires_epoch_secs {
            Some(epoch) => match epoch_to_datetime(epoch) {
                Some(expires) => Some(expires),
                None => return false,
            },
            None => None,
        };
        if ban.name.is_empty()
            || ban.name.len() > 256
            || ban.source.len() > 4096
            || ban.reason.len() > 4096
            || !seen_bans.insert(ban.uuid)
        {
            return false;
        }
        let _ = (created, expires);
    }
    let Some(presence_permit) = server.cluster_presence.try_reserve() else {
        return false;
    };
    let old_ops = server.cluster_admin_view.load().operators.clone();
    server.cluster_admin_view.store(Arc::new(snapshot.clone()));
    presence_permit.send(super::cluster_presence::PresenceCommand::ReplacePermissions(permissions));
    for old in old_ops {
        if !seen_ops.contains(&old.uuid) {
            if let Some(player) = server.get_player_by_uuid(uuid::Uuid::from_bytes(old.uuid))
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(&server_arc, PermissionLvl::Zero, &dispatcher);
            }
        }
    }
    for grant in &snapshot.operators {
        if let Some(level) = permission_level_from_u8(grant.level) {
            let uuid = uuid::Uuid::from_bytes(grant.uuid);
            if let Some(player) = server.get_player_by_uuid(uuid)
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(&server_arc, level, &dispatcher);
            }
        }
    }
    let local_server = local_server_id(server);
    for ban in &snapshot.banned_players {
        if ban
            .expires_epoch_secs
            .is_some_and(|expires| expires < time::OffsetDateTime::now_utc().unix_timestamp())
        {
            continue;
        }
        let uuid = uuid::Uuid::from_bytes(ban.uuid);
        if let Some(player) = server.get_player_by_uuid(uuid) {
            player.kick(
                DisconnectReason::Kicked,
                &TextComponent::text(ban.reason.clone()),
            );
        } else if let Some((_, entry)) = super::cluster_presence::all_presence_entries(server)
            .into_iter()
            .find(|(gid, entry)| gid.server == local_server && entry.uuid == ban.uuid)
        {
            server
                .cluster_lobby_directory
                .disconnect(&entry.name, &TextComponent::text(ban.reason.clone()));
        }
    }
    true
}

/// Publishes a locally-applied op grant to peers and persists it on the primary.
pub fn publish_op_grant(
    server: &Server,
    uuid: uuid::Uuid,
    name: &str,
    level: PermissionLvl,
    bypasses_player_limit: bool,
    issuer: &str,
) {
    super::cluster_presence::publish_permission(server, uuid, Some(level));
    let grant = OpGrant::new(
        *uuid.as_bytes(),
        name.to_string(),
        level as u8,
        bypasses_player_limit,
    );
    propagate_admin_mutation(server, &AdminMutation::GrantOp(grant), issuer);
}

/// Publishes a locally-applied op revoke to peers and persists it on the primary.
pub fn publish_op_revoke(server: &Server, uuid: uuid::Uuid, name: &str, issuer: &str) {
    super::cluster_presence::publish_permission(server, uuid, None);
    let revoke = OpRevoke::new(*uuid.as_bytes(), name.to_string());
    propagate_admin_mutation(server, &AdminMutation::RevokeOp(revoke), issuer);
}

/// Publishes a locally-applied player ban to peers and persists it on the primary.
pub fn publish_ban_add(
    server: &Server,
    uuid: uuid::Uuid,
    name: &str,
    source: &str,
    reason: &str,
    created_epoch_secs: i64,
    expires_epoch_secs: Option<i64>,
    issuer: &str,
) {
    let entry = BanEntry::new(
        *uuid.as_bytes(),
        name.to_string(),
        source.to_string(),
        reason.to_string(),
        created_epoch_secs,
        expires_epoch_secs,
    );
    propagate_admin_mutation(server, &AdminMutation::AddBan(entry), issuer);
}

/// Publishes a locally-applied player pardon to peers and persists it on the primary.
pub fn publish_ban_remove(server: &Server, uuid: uuid::Uuid, name: &str, issuer: &str) {
    let revoke = BanRevoke::new(*uuid.as_bytes(), name.to_string());
    propagate_admin_mutation(server, &AdminMutation::RemoveBan(revoke), issuer);
}

fn permission_level_from_u8(level: u8) -> Option<PermissionLvl> {
    match level {
        0 => Some(PermissionLvl::Zero),
        1 => Some(PermissionLvl::One),
        2 => Some(PermissionLvl::Two),
        3 => Some(PermissionLvl::Three),
        4 => Some(PermissionLvl::Four),
        _ => None,
    }
}

fn epoch_to_datetime(value: i64) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp(value).ok()
}

fn apply_grant_to_server(server: &Server, grant: &OpGrant) -> bool {
    if !is_valid_op_level(grant.level) {
        return false;
    }
    let Some(level) = permission_level_from_u8(grant.level) else {
        return false;
    };
    let uuid = uuid::Uuid::from_bytes(grant.uuid);
    if cluster_enabled(server) {
        let changed = !server
            .cluster_admin_view
            .load()
            .operators
            .iter()
            .any(|entry| entry == grant);
        if changed {
            if let Some(player) = server.get_player_by_uuid(uuid)
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(&server_arc, level, &dispatcher);
            }
        }
        return changed;
    }
    let mut config = server
        .data
        .operator_config
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match config.ops.iter_mut().find(|entry| entry.uuid == uuid) {
        Some(existing) => {
            if existing.level == level
                && existing.name == grant.name
                && existing.bypasses_player_limit == grant.bypasses_player_limit
            {
                return false;
            }
            existing.level = level;
            existing.name.clone_from(&grant.name);
            existing.bypasses_player_limit = grant.bypasses_player_limit;
        }
        None => {
            config.ops.push(pumpkin_config::op::Op::new(
                uuid,
                grant.name.clone(),
                level,
                grant.bypasses_player_limit,
            ));
        }
    }
    if stores_authoritative_files(server) {
        config.save();
    }
    drop(config);
    if let Some(player) = server.get_player_by_uuid(uuid) {
        if let Some(server_arc) = player.world().server.upgrade() {
            let dispatcher = server_arc.command_dispatcher.load();
            player.set_permission_lvl(&server_arc, level, &dispatcher);
        }
    }
    true
}

fn apply_revoke_to_server(server: &Server, revoke: &OpRevoke) -> bool {
    let uuid = uuid::Uuid::from_bytes(revoke.uuid);
    if cluster_enabled(server) {
        let changed = server
            .cluster_admin_view
            .load()
            .operators
            .iter()
            .any(|entry| entry.uuid == revoke.uuid);
        if changed {
            if let Some(player) = server.get_player_by_uuid(uuid)
                && let Some(server_arc) = player.world().server.upgrade()
            {
                let dispatcher = server_arc.command_dispatcher.load();
                player.set_permission_lvl(&server_arc, PermissionLvl::Zero, &dispatcher);
            }
        }
        return changed;
    }
    let mut config = server
        .data
        .operator_config
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(index) = config.ops.iter().position(|entry| entry.uuid == uuid) else {
        return false;
    };
    config.ops.remove(index);
    if stores_authoritative_files(server) {
        config.save();
    }
    drop(config);
    if let Some(player) = server.get_player_by_uuid(uuid) {
        if let Some(server_arc) = player.world().server.upgrade() {
            let dispatcher = server_arc.command_dispatcher.load();
            player.set_permission_lvl(&server_arc, PermissionLvl::Zero, &dispatcher);
        }
    }
    true
}

fn apply_ban_add_to_server(server: &Server, entry: &BanEntry) -> bool {
    let uuid = uuid::Uuid::from_bytes(entry.uuid);
    if cluster_enabled(server) {
        let changed = !server
            .cluster_admin_view
            .load()
            .banned_players
            .iter()
            .any(|current| current == entry);
        if changed && let Some(player) = server.get_player_by_uuid(uuid) {
            player.kick(
                DisconnectReason::Kicked,
                &TextComponent::text(entry.reason.clone()),
            );
        }
        return changed;
    }
    let created =
        epoch_to_datetime(entry.created_epoch_secs).unwrap_or_else(time::OffsetDateTime::now_utc);
    let expires = entry.expires_epoch_secs.and_then(epoch_to_datetime);
    let mut list = server
        .data
        .banned_player_list
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = list
        .banned_players
        .iter()
        .find(|current| current.uuid == uuid)
        && existing.name == entry.name
        && existing.source == entry.source
        && existing.reason == entry.reason
        && existing.created == created
        && existing.expires == expires
    {
        return false;
    }
    if let Some(existing) = list
        .banned_players
        .iter_mut()
        .find(|current| current.uuid == uuid)
    {
        existing.name.clone_from(&entry.name);
        existing.source.clone_from(&entry.source);
        existing.reason.clone_from(&entry.reason);
        existing.created = created;
        existing.expires = expires;
    } else {
        list.banned_players
            .push(crate::data::banlist_serializer::BannedPlayerEntry {
                uuid,
                name: entry.name.clone(),
                created,
                source: entry.source.clone(),
                expires,
                reason: entry.reason.clone(),
            });
    }
    if stores_authoritative_files(server) {
        list.save();
    }
    drop(list);
    if let Some(player) = server.get_player_by_uuid(uuid) {
        player.kick(
            DisconnectReason::Kicked,
            &TextComponent::text(entry.reason.clone()),
        );
    }
    true
}

fn apply_ban_remove_to_server(server: &Server, revoke: &BanRevoke) -> bool {
    let uuid = uuid::Uuid::from_bytes(revoke.uuid);
    if cluster_enabled(server) {
        return server
            .cluster_admin_view
            .load()
            .banned_players
            .iter()
            .any(|entry| entry.uuid == revoke.uuid);
    }
    let mut list = server
        .data
        .banned_player_list
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(index) = list
        .banned_players
        .iter()
        .position(|current| current.uuid == uuid)
    else {
        return false;
    };
    list.banned_players.remove(index);
    if stores_authoritative_files(server) {
        list.save();
    }
    true
}

fn audit_status_message(mutation: &AdminMutation, target_name: &str) -> TextComponent {
    match mutation {
        AdminMutation::GrantOp(_) => TextComponent::translate_cross(
            pumpkin_data::translation::java::COMMANDS_OP_SUCCESS,
            pumpkin_data::translation::bedrock::COMMANDS_OP_SUCCESS,
            [TextComponent::text(target_name.to_string())],
        ),
        AdminMutation::RevokeOp(_) => TextComponent::translate_cross(
            pumpkin_data::translation::java::COMMANDS_DEOP_SUCCESS,
            pumpkin_data::translation::bedrock::COMMANDS_DEOP_SUCCESS,
            [TextComponent::text(target_name.to_string())],
        ),
        AdminMutation::AddBan(entry) => TextComponent::translate_cross(
            pumpkin_data::translation::java::COMMANDS_BAN_SUCCESS,
            pumpkin_data::translation::bedrock::COMMANDS_BAN_SUCCESS,
            [
                TextComponent::text(target_name.to_string()),
                TextComponent::text(entry.reason.clone()),
            ],
        ),
        AdminMutation::RemoveBan(_) => TextComponent::translate_cross(
            pumpkin_data::translation::java::COMMANDS_PARDON_SUCCESS,
            pumpkin_data::translation::bedrock::COMMANDS_UNBAN_SUCCESS,
            [TextComponent::text(target_name.to_string())],
        ),
    }
}

fn audit_target_uuid(mutation: &AdminMutation) -> uuid::Uuid {
    match mutation {
        AdminMutation::GrantOp(grant) => uuid::Uuid::from_bytes(grant.uuid),
        AdminMutation::RevokeOp(revoke) => uuid::Uuid::from_bytes(revoke.uuid),
        AdminMutation::AddBan(entry) => uuid::Uuid::from_bytes(entry.uuid),
        AdminMutation::RemoveBan(revoke) => uuid::Uuid::from_bytes(revoke.uuid),
    }
}

fn audit_target_name(mutation: &AdminMutation) -> String {
    match mutation {
        AdminMutation::GrantOp(grant) => grant.name.clone(),
        AdminMutation::AddBan(entry) => entry.name.clone(),
        AdminMutation::RemoveBan(revoke) => revoke.name.clone(),
        AdminMutation::RevokeOp(revoke) => revoke.name.clone(),
    }
}

/// Renders a successful action from another peer locally. The command has
/// already changed the stores when this runs, so a newly-opped target is in
/// the local operator audience; a deopped target receives a direct copy after
/// dropping out of that audience.
fn announce_inbound_admin_audit(server: &Server, audit: &AdminAudit) {
    let target_name = audit_target_name(&audit.mutation);
    let status = audit_status_message(&audit.mutation, &target_name);
    let issuer = if audit.issuer.is_empty() {
        "Server"
    } else {
        audit.issuer.as_str()
    };
    let announcement = TextComponent::translate_cross(
        "chat.type.admin",
        "chat.type.admin",
        [TextComponent::text(issuer.to_string()), status],
    )
    .color(Color::Named(NamedColor::Gray))
    .italic();

    // Remote actions have no local CommandSource. Preserve its two observable
    // effects explicitly: terminal output and a structured node log event.
    println!("{}", announcement.clone().to_pretty_console());
    info!(
        issuer,
        target = target_name.as_str(),
        mutation = ?audit.mutation,
        "cluster admin action applied"
    );

    let target_uuid = audit_target_uuid(&audit.mutation);
    let mut target_notified = false;
    for player in server.get_all_players() {
        if player.permission_lvl.load() >= server.basic_config.op_permission_level {
            target_notified |= player.gameprofile.id == target_uuid;
            player.send_system_message(&announcement);
        }
    }
    if !target_notified && let Some(player) = server.get_player_by_uuid(target_uuid) {
        player.send_system_message(&announcement);
    }
}

/// Applies an inbound admin mutation to the local [`Server`] stores.
pub fn apply_inbound_mutation_to_server(server: &Server, mutation: &AdminMutation) -> bool {
    apply_inbound_mutation_to_server_with_save(server, mutation, true, false)
}

pub(crate) fn apply_sequenced_mutation_to_server(server: &Server, audit: &AdminAudit) {
    if apply_inbound_mutation_to_server_with_save(server, &audit.mutation, false, false) {
        announce_inbound_admin_audit(server, audit);
    }
}

pub(crate) fn apply_replayed_mutation_to_server(server: &Server, mutation: &AdminMutation) {
    let _ = apply_inbound_mutation_to_server_with_save(server, mutation, false, true);
}

fn apply_inbound_mutation_to_server_with_save(
    server: &Server,
    mutation: &AdminMutation,
    save_to_primary_queue: bool,
    replayed: bool,
) -> bool {
    let changed = match mutation {
        AdminMutation::GrantOp(grant) => apply_grant_to_server(server, grant),
        AdminMutation::RevokeOp(revoke) => apply_revoke_to_server(server, revoke),
        AdminMutation::AddBan(entry) => apply_ban_add_to_server(server, entry),
        AdminMutation::RemoveBan(revoke) => apply_ban_remove_to_server(server, revoke),
    };
    if changed {
        let revision = server.cluster_admin_view.load().revision;
        apply_mutation_to_view(server, revision, mutation);
        if save_to_primary_queue && stores_authoritative_files(server) {
            let _ = persist_on_primary(server, mutation);
        }
        match mutation {
            AdminMutation::GrantOp(grant) => {
                if let Some(level) = permission_level_from_u8(grant.level) {
                    super::cluster_presence::publish_permission(
                        server,
                        uuid::Uuid::from_bytes(grant.uuid),
                        Some(level),
                    );
                }
            }
            AdminMutation::RevokeOp(revoke) => {
                super::cluster_presence::publish_permission(
                    server,
                    uuid::Uuid::from_bytes(revoke.uuid),
                    None,
                );
            }
            AdminMutation::AddBan(_) | AdminMutation::RemoveBan(_) => {}
        }
        if !replayed {
            ADMIN_MUTATION_APPLIED.fetch_add(1, Ordering::Relaxed);
            info!("cluster admin mutation applied to server stores");
        }
    }
    changed
}

fn find_player_by_gid(
    server: &Server,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
) -> Option<Arc<crate::entity::player::Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.cluster_gid() == Some(gid) {
                return Some(player.clone());
            }
        }
    }
    None
}

/// Delivers an inbound cluster kick to a locally-hosted player.
pub fn deliver_remote_kick(server: &Server, request: &KickRequest) -> bool {
    if !should_deliver_kick_locally(request, local_server_id(server)) {
        return false;
    }
    if server
        .cluster_lobby_directory
        .disconnect_gid(request.target, &TextComponent::text(request.reason.clone()))
    {
        ADMIN_KICK_DELIVERED.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    let target = find_player_by_gid(server, request.target);
    let Some(target) = target else {
        return false;
    };
    target.kick(
        DisconnectReason::Kicked,
        &TextComponent::text(request.reason.clone()),
    );
    ADMIN_KICK_DELIVERED.fetch_add(1, Ordering::Relaxed);
    info!(
        name = request.target_name.as_str(),
        issuer = request.issuer.as_str(),
        "cluster kick delivered"
    );
    true
}

pub(crate) fn authorized_remote_kick(
    server: &Server,
    source: ServerId,
    request: &KickRequest,
) -> bool {
    let Some(identity) = request.issuer_identity.as_ref() else {
        return server.cluster_primary_peers.load().contains(&source);
    };
    let presence = server.cluster_presence.snapshot();
    authorized_player_kick(&presence, identity, request)
}

fn authorized_player_kick(
    presence: &super::cluster_presence::PresenceSnapshot,
    identity: &pumpkin_cluster::admin_sync::KickIssuerIdentity,
    request: &KickRequest,
) -> bool {
    presence.permission_revision == identity.permission_revision
        && presence
            .entries
            .get(&identity.gid)
            .is_some_and(|entry| entry.uuid == identity.uuid && entry.name == request.issuer)
        && presence.permission_for_uuid(&identity.uuid) >= PermissionLvl::Three
}

/// Delivers an inbound cluster spectate request to locally-hosted players.
pub fn deliver_remote_spectate(server: &Server, request: &SpectateRequest) -> bool {
    let target = find_player_by_gid(server, request.target)
        .or_else(|| server.get_player_by_name(&request.target_name));
    let Some(target) = target else {
        return false;
    };
    let Some(viewer) = find_player_by_gid(server, request.viewer) else {
        return false;
    };
    if viewer.gamemode.load() != GameMode::Spectator {
        debug!("cluster spectate dropped: viewer is not a spectator");
        return false;
    }
    if viewer.entity_id() == target.entity_id() {
        return false;
    }
    let viewer_world = viewer.world();
    let target_world = target.world();
    if !Arc::ptr_eq(&viewer_world, &target_world) {
        debug!("cluster spectate dropped: viewer and target are on different worlds");
        return false;
    }
    let target_entity = target.get_entity();
    let target_id = target_entity.entity_id;
    viewer.camera_target_id.store(Some(target_id));
    viewer.try_send_client_packet(&pumpkin_protocol::java::client::play::CSetCamera::new(
        target_id.into(),
    ));
    viewer.teleport(
        target_entity.pos.load(),
        Some(target_entity.yaw.load()),
        Some(target_entity.pitch.load()),
        viewer_world,
    );
    ADMIN_SPECTATE_DELIVERED.fetch_add(1, Ordering::Relaxed);
    info!(
        target = request.target_name.as_str(),
        "cluster spectate delivered"
    );
    true
}

/// Handles an inbound admin control message for the local [`Server`].
///
/// Returns `true` when the message was an admin mutation, kick, or spectate
/// request handled here. [`Server`] mutation stores are kept in sync and
/// kick/spectate requests are delivered to locally-hosted players.
pub fn handle_admin_message_for_server(
    server: &Arc<Server>,
    message: &AdminControlMessage,
) -> bool {
    match message {
        AdminControlMessage::Snapshot(_)
        | AdminControlMessage::SnapshotPart { .. }
        | AdminControlMessage::SnapshotRequest => false,
        AdminControlMessage::Mutation(mutation) => {
            apply_inbound_mutation_to_server(server, mutation);
            true
        }
        AdminControlMessage::MutationWithAudit(audit) => {
            if apply_inbound_mutation_to_server(server, &audit.mutation) {
                announce_inbound_admin_audit(server, audit);
            }
            true
        }
        AdminControlMessage::VersionedMutationWithAudit { audit, .. } => {
            if apply_inbound_mutation_to_server(server, &audit.mutation) {
                announce_inbound_admin_audit(server, audit);
            }
            true
        }
        AdminControlMessage::SequencedMutation { audit, .. } => {
            apply_sequenced_mutation_to_server(server, audit);
            true
        }
        AdminControlMessage::MutationApplied { .. } => true,
        AdminControlMessage::Kick(request) => deliver_remote_kick(server, request),
        AdminControlMessage::Spectate(request) => deliver_remote_spectate(server, request),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_primary_relays_inbound_admin_mutations() {
        assert!(should_relay_inbound_mutation(ClusterRole::Primary));
        assert!(!should_relay_inbound_mutation(ClusterRole::Secondary));
    }

    #[test]
    fn remote_kick_requires_current_issuer_presence_and_permission() {
        use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot};
        use pumpkin_cluster::presence::RemotePlayerEntry;

        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(9));
        let uuid = [5; 16];
        let mut presence = super::super::cluster_presence::PresenceSnapshot::default();
        presence.entries.insert(
            gid,
            RemotePlayerEntry::new(gid, uuid, String::from("Alice"), Vec::new(), 0, true, true),
        );
        presence.permissions.insert(uuid, PermissionLvl::Three);
        presence.permission_revision = 7;
        let request = KickRequest::new(
            GlobalPlayerId::new(ServerId(3), PlayerSlot(4)),
            String::from("Bob"),
            String::from("reason"),
            String::from("Alice"),
        );
        let identity = pumpkin_cluster::admin_sync::KickIssuerIdentity {
            gid,
            uuid,
            permission_revision: 7,
        };
        assert!(authorized_player_kick(&presence, &identity, &request));
        presence.permissions.remove(&uuid);
        assert!(!authorized_player_kick(&presence, &identity, &request));
        presence.permissions.insert(uuid, PermissionLvl::Three);
        presence.permission_revision = 8;
        assert!(!authorized_player_kick(&presence, &identity, &request));
        presence.permission_revision = 7;
        presence.entries.remove(&gid);
        assert!(!authorized_player_kick(&presence, &identity, &request));
    }
}

pub fn route_kick_request(server: &Server, request: &KickRequest) -> bool {
    if !cluster_enabled(server) {
        return deliver_remote_kick(server, request);
    }
    let local = local_server_id(server);
    if request.target.server == local {
        return deliver_remote_kick(server, request);
    }
    if request.issuer_identity.is_none()
        && !matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
    {
        return false;
    }
    try_send_to_host(
        server,
        request.target.server.0,
        &AdminControlMessage::Kick(request.clone()),
    )
}

/// Broadcasts a spectate request toward the host of the spectated player.
pub fn broadcast_spectate_for_players(
    server: &Server,
    viewer_gid: Option<pumpkin_cluster::identity::GlobalPlayerId>,
    target_gid: Option<pumpkin_cluster::identity::GlobalPlayerId>,
    target_name: &str,
) {
    if !cluster_enabled(server) {
        return;
    }
    let local = local_server_id(server);
    let (Some(viewer), Some(target)) = (viewer_gid, target_gid) else {
        return;
    };
    if viewer == target {
        return;
    }
    let request = SpectateRequest::new(viewer, target, target_name.to_string());
    if target.server == local {
        return;
    }
    try_send_to_host(
        server,
        target.server.0,
        &AdminControlMessage::Spectate(request),
    );
}
