use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::admin_sync::{
    AdminControlMessage, KickRequest, SpectateRequest, admin_control_kind,
    decode_control_message, kick_parcel_for_host, should_deliver_kick_locally,
    should_deliver_spectate_locally, spectate_parcel_for_host,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_util::GameMode;
use pumpkin_util::text::TextComponent;
use tokio::sync::mpsc;

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;
use crate::net::DisconnectReason;

struct ModerationOutbox {
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
}

static MODERATION_OUTBOX: OnceLock<ModerationOutbox> = OnceLock::new();
static MODERATION_KICKS_DELIVERED: AtomicU64 = AtomicU64::new(0);
static MODERATION_SPECTATES_SERVED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn moderation_kicks_delivered() -> u64 {
    MODERATION_KICKS_DELIVERED.load(Ordering::Relaxed)
}

#[must_use]
pub fn moderation_spectates_served() -> u64 {
    MODERATION_SPECTATES_SERVED.load(Ordering::Relaxed)
}

pub fn install_moderation_outbox(local: ServerId, outbound: mpsc::Sender<OutboundParcel>) {
    let _ = MODERATION_OUTBOX.set(ModerationOutbox { local, outbound });
}

fn moderation_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

fn moderation_local_id(server: &Server) -> ServerId {
    ServerId(server.advanced_config.cluster.server_id)
}

fn find_moderation_player(server: &Server, gid: GlobalPlayerId) -> Option<Arc<Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.cluster_gid() == Some(gid) {
                return Some(player.clone());
            }
        }
    }
    None
}

fn find_moderation_player_by_name(server: &Server, name: &str) -> Option<Arc<Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.gameprofile.name.eq_ignore_ascii_case(name) {
                return Some(player.clone());
            }
        }
    }
    None
}

fn send_moderation_parcel(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> bool {
    let Some(outbox) = MODERATION_OUTBOX.get() else {
        return false;
    };
    let parcel = OutboundParcel {
        peer: ServerId(peer),
        header: StreamHeader::new(kind, None),
        bytes,
    };
    outbox.outbound.try_send(parcel).is_ok()
}

pub fn deliver_moderation_kick(server: &Server, request: &KickRequest) -> bool {
    let local = moderation_local_id(server);
    if !should_deliver_kick_locally(request, local) {
        return false;
    }
    let target = find_moderation_player(server, request.target)
        .or_else(|| find_moderation_player_by_name(server, &request.target_name));
    let Some(target) = target else {
        return false;
    };
    target.kick(
        DisconnectReason::Kicked,
        &TextComponent::text(request.reason.clone()),
    );
    MODERATION_KICKS_DELIVERED.fetch_add(1, Ordering::Relaxed);
    true
}

pub fn kick_player_by_id(
    server: &Server,
    target: GlobalPlayerId,
    reason: &str,
    issuer: &str,
) -> bool {
    if !moderation_enabled(server) {
        return false;
    }
    let local = moderation_local_id(server);
    let target_name = find_moderation_player(server, target)
        .map(|player| player.gameprofile.name.clone())
        .unwrap_or_default();
    let request = KickRequest::new(target, target_name, reason.to_string(), issuer.to_string());
    if should_deliver_kick_locally(&request, local) {
        return deliver_moderation_kick(server, &request);
    }
    let Ok(parcel) = kick_parcel_for_host(&request) else {
        return false;
    };
    send_moderation_parcel(parcel.peer, parcel.kind, parcel.bytes)
}

pub fn serve_spectate_request(server: &Server, request: &SpectateRequest) -> bool {
    let local = moderation_local_id(server);
    if !should_deliver_spectate_locally(request, local) {
        return false;
    }
    let target = find_moderation_player(server, request.target)
        .or_else(|| server.get_player_by_name(&request.target_name));
    let Some(target) = target else {
        return false;
    };
    let Some(viewer) = find_moderation_player(server, request.viewer) else {
        MODERATION_SPECTATES_SERVED.fetch_add(1, Ordering::Relaxed);
        return true;
    };
    if viewer.gamemode.load() != GameMode::Spectator {
        return false;
    }
    if viewer.entity_id() == target.entity_id() {
        return false;
    }
    let viewer_world = viewer.world();
    let target_world = target.world();
    if !Arc::ptr_eq(&viewer_world, &target_world) {
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
    MODERATION_SPECTATES_SERVED.fetch_add(1, Ordering::Relaxed);
    true
}

pub fn request_spectate_by_id(
    server: &Server,
    viewer: GlobalPlayerId,
    target: GlobalPlayerId,
    target_name: &str,
) -> bool {
    if !moderation_enabled(server) {
        return false;
    }
    if viewer == target {
        return false;
    }
    let local = moderation_local_id(server);
    let canonical = find_moderation_player(server, target)
        .map(|player| player.gameprofile.name.clone())
        .unwrap_or_else(|| target_name.to_string());
    let request = SpectateRequest::new(viewer, target, canonical);
    if should_deliver_spectate_locally(&request, local) {
        return serve_spectate_request(server, &request);
    }
    let Ok(parcel) = spectate_parcel_for_host(&request) else {
        return false;
    };
    send_moderation_parcel(parcel.peer, parcel.kind, parcel.bytes)
}

pub fn handle_moderation_parcel(server: &Server, parcel: &InboundParcel) -> bool {
    if parcel.header.kind != admin_control_kind() {
        return false;
    }
    let Ok(message) = decode_control_message(&parcel.bytes) else {
        return false;
    };
    match message {
        AdminControlMessage::Kick(request) => deliver_moderation_kick(server, &request),
        AdminControlMessage::Spectate(request) => serve_spectate_request(server, &request),
        AdminControlMessage::Mutation(_) | AdminControlMessage::MutationWithAudit(_) => false,
    }
}

pub fn spawn_moderation_apply(
    server: &Arc<Server>,
    mut inbound: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        while let Some(parcel) = inbound.recv().await {
            handle_moderation_parcel(&task_server, &parcel);
        }
    });
}

#[must_use]
pub fn moderation_outbox_local() -> Option<ServerId> {
    MODERATION_OUTBOX.get().map(|outbox| outbox.local)
}
