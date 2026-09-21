use std::sync::Arc;
use std::sync::OnceLock;

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::lobby_control::{LobbyControl, decode_control, encode_control, lobby_control_kind};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use tokio::sync::mpsc;
use tracing::warn;

use super::Server;

struct LobbyControlOutbox {
    local: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
}

static LOBBY_CONTROL_OUTBOX: OnceLock<LobbyControlOutbox> = OnceLock::new();

pub fn install_lobby_control_outbox(local: ServerId, outbound: mpsc::Sender<OutboundParcel>) {
    let _ = LOBBY_CONTROL_OUTBOX.set(LobbyControlOutbox { local, outbound });
}

pub fn route_lobby_control(server: &Server, control: LobbyControl) -> bool {
    let Some(outbox) = LOBBY_CONTROL_OUTBOX.get() else {
        warn!(target = control.target().server.0, "cluster lobby control dropped: outbox unavailable");
        return false;
    };
    if control.host() == outbox.local {
        return super::cluster_lobby::apply_remote_lobby_control(server, control);
    }
    let Ok(bytes) = encode_control(&control) else {
        warn!(target = control.target().server.0, "cluster lobby control encode failed");
        return false;
    };
    outbox
        .outbound
        .try_send(OutboundParcel {
            peer: control.host(),
            header: StreamHeader::new(lobby_control_kind(), None),
            bytes,
        })
        .is_ok()
}

pub fn apply_lobby_control_bytes(server: &Server, local: ServerId, bytes: &[u8]) -> bool {
    let Ok(control) = decode_control(bytes) else {
        return false;
    };
    if control.host() != local {
        warn!(
            target = control.target().server.0,
            local = local.0,
            "cluster lobby control dropped: wrong host"
        );
        return false;
    }
    super::cluster_lobby::apply_remote_lobby_control(server, control)
}

pub async fn lobby_control_task(
    server: Arc<Server>,
    local: ServerId,
    mut inbound: mpsc::Receiver<InboundParcel>,
) {
    while let Some(parcel) = inbound.recv().await {
        if parcel.header.kind != lobby_control_kind() {
            continue;
        }
        apply_lobby_control_bytes(&server, local, &parcel.bytes);
    }
}

pub fn spawn_lobby_control_apply(
    server: &Arc<Server>,
    local: ServerId,
    inbound: mpsc::Receiver<InboundParcel>,
) {
    server.spawn_task(lobby_control_task(Arc::clone(server), local, inbound));
}
