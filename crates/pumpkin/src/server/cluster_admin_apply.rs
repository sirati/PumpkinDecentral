//! Cluster-wide admin enforcement.
//!
//! [`pumpkin_cluster::admin_sync`] already defines the wire parcels for
//! op/ban mutations as well as kick and spectate requests. This module hooks
//! those parcels up to the [`Server`] stores so mutations propagate across
//! peers, persist on the primary, and kicks/spectates reach players no matter
//! which node hosts them.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::admin_sync::{
    AdminAudit, AdminControlMessage, AdminMutation, BanEntry, BanRevoke, KickRequest, OpGrant,
    OpRevoke, encode_control_message, is_valid_op_level,
    should_deliver_kick_locally, submit_admin_mutation,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use pumpkin_util::PermissionLvl;
use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::{Color, NamedColor};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::data::SaveJSONConfiguration;
use crate::net::DisconnectReason;

struct AdminOutbox {
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static ADMIN_OUTBOX: OnceLock<AdminOutbox> = OnceLock::new();
static ADMIN_MUTATION_APPLIED: AtomicU64 = AtomicU64::new(0);
static ADMIN_MUTATION_SENT: AtomicU64 = AtomicU64::new(0);
static ADMIN_MUTATION_UNSENT: AtomicU64 = AtomicU64::new(0);
static ADMIN_BROADCAST_WARN_LAST_MILLIS: AtomicU64 = AtomicU64::new(0);
static ADMIN_KICK_DELIVERED: AtomicU64 = AtomicU64::new(0);

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

fn admin_broadcast_warn_cooldown_elapsed() -> bool {
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

/// Installs the outbound admin channel used to propagate mutations and kick/spectate requests.
pub fn install_admin_outbox(peers: Vec<u16>, outbound: mpsc::Sender<OutboundParcel>) {
    let _ = ADMIN_OUTBOX.set(AdminOutbox { peers, outbound });
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

#[must_use]
pub fn persists_admin_state(server: &Server) -> bool {
    !cluster_enabled(server)
        || matches!(server.advanced_config.cluster.role, ClusterRole::Primary)
}

fn local_server_id(server: &Server) -> ServerId {
    ServerId(server.advanced_config.cluster.server_id)
}

fn persist_on_primary(server: &Server, mutation: &AdminMutation) {
    let Some(handle) = server.primary_save.as_ref() else {
        return;
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
        warn!("cluster admin mutation persist queue full");
    }
}

fn try_broadcast(message: &AdminControlMessage) -> usize {
    let Some(outbox) = ADMIN_OUTBOX.get() else {
        ADMIN_MUTATION_UNSENT.fetch_add(1, Ordering::Relaxed);
        if admin_broadcast_warn_cooldown_elapsed() {
            warn!("cluster admin broadcast dropped: outbox not installed");
        }
        return 0;
    };
    if outbox.peers.is_empty() {
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
    let mut sent = 0_usize;
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
    ADMIN_MUTATION_SENT.fetch_add(sent as u64, Ordering::Relaxed);
    let unsent = outbox.peers.len().saturating_sub(sent);
    if unsent > 0 {
        ADMIN_MUTATION_UNSENT.fetch_add(unsent as u64, Ordering::Relaxed);
        if admin_broadcast_warn_cooldown_elapsed() {
            warn!(
                sent = sent,
                peers = outbox.peers.len(),
                "cluster admin broadcast partially dropped: mesh queue full"
            );
        }
    }
    sent
}

fn try_send_to_host(host: u16, message: &AdminControlMessage) -> bool {
    let Some(outbox) = ADMIN_OUTBOX.get() else {
        return false;
    };
    let Ok(bytes) = encode_control_message(message) else {
        warn!("cluster admin control encode failed");
        return false;
    };
    let parcel = OutboundParcel {
        peer: ServerId(host),
        header: StreamHeader::new(StreamKind::Control, None),
        bytes,
    };
    outbox.outbound.try_send(parcel).is_ok()
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

fn relay_inbound_mutation_from_primary(server: &Server, message: &AdminControlMessage) -> usize {
    if !should_relay_inbound_mutation(server.advanced_config.cluster.role) {
        return 0;
    }
    try_broadcast(message)
}

/// Propagates a locally-applied admin mutation to peers and persists it on the primary.
pub fn propagate_admin_mutation(server: &Server, mutation: &AdminMutation, issuer: &str) {
    if !cluster_enabled(server) {
        return;
    }
    persist_on_primary(server, mutation);
    // One control parcel carries both the state transition and its operator
    // feedback. This preserves ordering and prevents a remote peer from
    // displaying a success for a transition it did not receive.
    let sent = try_broadcast(&AdminControlMessage::MutationWithAudit(AdminAudit::new(
        issuer.to_string(),
        mutation.clone(),
    )));
    if sent > 0 {
        debug!(peers = sent, "cluster admin mutation broadcast");
    }
}

pub fn publish_admin_state_to_peer(server: &Server, peer: u16) {
    if !matches!(server.advanced_config.cluster.role, ClusterRole::Primary) {
        return;
    }
    let Some(outbox) = ADMIN_OUTBOX.get() else {
        return;
    };
    let target = ServerId(peer);
    let mut mutations = Vec::new();
    {
        let Ok(guard) = server.data.operator_config.try_read() else {
            warn!("cluster admin seed skipped: operator state busy");
            return;
        };
        mutations.extend(guard.ops.iter().map(|op| {
            AdminMutation::GrantOp(OpGrant::new(
                    *op.uuid.as_bytes(),
                    op.name.clone(),
                    op.level as u8,
                    op.bypasses_player_limit,
                ))
        }));
    }
    {
        let Ok(guard) = server.data.banned_player_list.try_read() else {
            warn!("cluster admin seed skipped: ban state busy");
            return;
        };
        mutations.extend(guard.banned_players.iter().map(|entry| {
            AdminMutation::AddBan(BanEntry::new(
                *entry.uuid.as_bytes(),
                entry.name.clone(),
                entry.source.clone(),
                entry.reason.clone(),
                entry.created.unix_timestamp(),
                entry.expires.map(|expires| expires.unix_timestamp()),
            ))
        }));
    }
    for mutation in mutations {
        let Ok(bytes) = encode_control_message(&AdminControlMessage::Mutation(mutation)) else {
            continue;
        };
        let parcel = OutboundParcel {
            peer: target,
            header: StreamHeader::new(StreamKind::Control, None),
            bytes,
        };
        let _ = outbox.outbound.try_send(parcel);
    }
}

pub fn publish_ops_to_peer(server: &Server, peer: u16) {
    publish_admin_state_to_peer(server, peer);
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
    let Ok(mut config) = server.data.operator_config.try_write() else {
        warn!("cluster op grant dropped: operator state busy");
        return false;
    };
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
    if persists_admin_state(server) {
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
    let Ok(mut config) = server.data.operator_config.try_write() else {
        warn!("cluster op revoke dropped: operator state busy");
        return false;
    };
    let Some(index) = config.ops.iter().position(|entry| entry.uuid == uuid) else {
        return false;
    };
    config.ops.remove(index);
    if persists_admin_state(server) {
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
    let created = epoch_to_datetime(entry.created_epoch_secs)
        .unwrap_or_else(time::OffsetDateTime::now_utc);
    let expires = entry
        .expires_epoch_secs
        .and_then(epoch_to_datetime);
    let Ok(mut list) = server.data.banned_player_list.try_write() else {
        warn!("cluster ban add dropped: ban state busy");
        return false;
    };
    if let Some(existing) = list
        .banned_players
        .iter()
        .find(|current| current.uuid == uuid)
    {
        if existing.name == entry.name {
            return false;
        }
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
    if persists_admin_state(server) {
        list.save();
    }
    drop(list);
    if let Some(player) = server.get_player_by_uuid(uuid) {
        player.kick(
            DisconnectReason::Kicked,
            &TextComponent::text(entry.reason.clone()),
        );
    }
    for waiter in server
        .lobby_waiters
        .load()
        .iter()
        .filter(|waiter| waiter.profile.id == uuid)
    {
        waiter.client.try_kick(
            DisconnectReason::Kicked,
            &TextComponent::text(entry.reason.clone()),
        );
    }
    true
}

fn apply_ban_remove_to_server(server: &Server, revoke: &BanRevoke) -> bool {
    let uuid = uuid::Uuid::from_bytes(revoke.uuid);
    let Ok(mut list) = server.data.banned_player_list.try_write() else {
        warn!("cluster ban remove dropped: ban state busy");
        return false;
    };
    let Some(index) = list
        .banned_players
        .iter()
        .position(|current| current.uuid == uuid)
    else {
        return false;
    };
    list.banned_players.remove(index);
    if persists_admin_state(server) {
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
    if !target_notified
        && let Some(player) = server.get_player_by_uuid(target_uuid)
    {
        player.send_system_message(&announcement);
    }
}

/// Applies an inbound admin mutation to the local [`Server`] stores.
pub fn apply_inbound_mutation_to_server(server: &Server, mutation: &AdminMutation) -> bool {
    let changed = match mutation {
        AdminMutation::GrantOp(grant) => apply_grant_to_server(server, grant),
        AdminMutation::RevokeOp(revoke) => apply_revoke_to_server(server, revoke),
        AdminMutation::AddBan(entry) => apply_ban_add_to_server(server, entry),
        AdminMutation::RemoveBan(revoke) => apply_ban_remove_to_server(server, revoke),
    };
    if changed {
        ADMIN_MUTATION_APPLIED.fetch_add(1, Ordering::Relaxed);
        info!("cluster admin mutation applied to server stores");
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

fn find_player_including_lobby(
    server: &Server,
    name: &str,
) -> Option<Arc<crate::entity::player::Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.gameprofile.name.eq_ignore_ascii_case(name) {
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
    if let Some(waiter) = server.lobby_waiters.load().iter().find(|waiter| {
        waiter.gid == request.target
            || waiter
                .profile
                .name
                .eq_ignore_ascii_case(&request.target_name)
    }) {
        waiter.client.try_kick(
            DisconnectReason::Kicked,
            &TextComponent::text(request.reason.clone()),
        );
        ADMIN_KICK_DELIVERED.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    let target = find_player_by_gid(server, request.target)
        .or_else(|| find_player_including_lobby(server, &request.target_name));
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

/// Handles an inbound admin control message for the local [`Server`].
///
/// Returns `true` when the message was an admin mutation, kick, or spectate
/// request handled here. [`Server`] mutation stores are kept in sync and
/// kick/spectate requests are delivered to locally-hosted players.
pub fn handle_admin_message_for_server(server: &Arc<Server>, message: &AdminControlMessage) -> bool {
    match message {
        AdminControlMessage::Mutation(mutation) => {
            apply_inbound_mutation_to_server(server, mutation);
            let relayed = relay_inbound_mutation_from_primary(server, message);
            if relayed > 0 {
                debug!(peers = relayed, "cluster admin mutation relayed by primary");
            }
            true
        }
        AdminControlMessage::MutationWithAudit(audit) => {
            if apply_inbound_mutation_to_server(server, &audit.mutation) {
                announce_inbound_admin_audit(server, audit);
            }
            let relayed = relay_inbound_mutation_from_primary(server, message);
            if relayed > 0 {
                debug!(peers = relayed, "cluster admin mutation relayed by primary");
            }
            true
        }
        AdminControlMessage::Kick(request) => deliver_remote_kick(server, request),
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
}

/// Broadcasts a kick for a locally-kicked player so every peer enforces it.
pub fn broadcast_kick_for_local_player(
    server: &Server,
    target_gid: Option<pumpkin_cluster::identity::GlobalPlayerId>,
    target_name: &str,
    reason: &str,
    issuer: &str,
) {
    if !cluster_enabled(server) {
        return;
    }
    let Some(target) = target_gid else {
        return;
    };
    let local = local_server_id(server);
    let request = KickRequest::new(
        target,
        target_name.to_string(),
        reason.to_string(),
        issuer.to_string(),
    );
    try_broadcast(&AdminControlMessage::Kick(request.clone()));
    if target.server != local {
        try_send_to_host(target.server.0, &AdminControlMessage::Kick(request));
    }
}

pub fn route_kick_request(server: &Server, request: &KickRequest) -> bool {
    if !cluster_enabled(server) {
        return deliver_remote_kick(server, request);
    }
    let local = local_server_id(server);
    let mut sent = try_broadcast(&AdminControlMessage::Kick(request.clone()));
    if request.target.server == local {
        if deliver_remote_kick(server, request) {
            return true;
        }
    } else if try_send_to_host(
        request.target.server.0,
        &AdminControlMessage::Kick(request.clone()),
    ) {
        sent = sent.saturating_add(1);
    }
    sent > 0
}
