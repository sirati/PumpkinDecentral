use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::chat_sync::{
    ChatControlMessage, EmoteChatBroadcast, PrivateChatRequest, PublicChatBroadcast,
    SayChatBroadcast, TeamChatBroadcast, chat_control_kind, emote_parcels_for_peers,
    encode_control_message, is_valid_chat_body, is_valid_chat_name, private_parcel_for_host,
    say_parcels_for_peers, team_parcels_for_peers,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use tokio::sync::mpsc;

use super::Server;
use crate::entity::player::Player;

struct ChatOutbox {
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static CHAT_OUTBOX: OnceLock<ChatOutbox> = OnceLock::new();
static CHAT_BROADCAST: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn chat_broadcast() -> u64 {
    CHAT_BROADCAST.load(Ordering::Relaxed)
}

pub fn install_chat_outbox(
    local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = CHAT_OUTBOX.set(ChatOutbox {
        local,
        peers,
        outbound,
    });
}

pub fn broadcast_public_chat(sender: GlobalPlayerId, sender_name: &str, body: &str) {
    let Some(outbox) = CHAT_OUTBOX.get() else {
        return;
    };
    if outbox.peers.is_empty() {
        return;
    }
    if sender.server != outbox.local {
        return;
    }
    if !is_valid_chat_body(body) || !is_valid_chat_name(sender_name) {
        return;
    }
    let broadcast = PublicChatBroadcast::new(sender, sender_name.to_string(), body.to_string());
    let Ok(bytes) = encode_control_message(&ChatControlMessage::Public(broadcast)) else {
        return;
    };
    let mut sent = 0_u64;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(chat_control_kind(), None),
            bytes: bytes.clone(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn broadcast_public_chat_from_player(_server: &Server, player: &Player, body: &str) {
    let Some(sender) = player.cluster_gid() else {
        return;
    };
    broadcast_public_chat(sender, &player.gameprofile.name, body);
}

pub fn broadcast_emote_chat(broadcast: &EmoteChatBroadcast) -> u64 {
    let Some(outbox) = CHAT_OUTBOX.get() else {
        return 0;
    };
    if outbox.peers.is_empty() || broadcast.sender.server != outbox.local {
        return 0;
    }
    if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
        return 0;
    }
    let Ok(parcels) = emote_parcels_for_peers(broadcast, &outbox.peers) else {
        return 0;
    };
    let mut sent = 0_u64;
    for parcel in parcels {
        let outbound_parcel = OutboundParcel {
            peer: ServerId(parcel.peer),
            header: StreamHeader::new(parcel.kind, None),
            bytes: parcel.bytes,
        };
        if outbox.outbound.try_send(outbound_parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn broadcast_emote_from_player(_server: &Server, player: &Player, body: &str) {
    let Some(sender) = player.cluster_gid() else {
        return;
    };
    let broadcast = EmoteChatBroadcast::new(sender, player.gameprofile.name.clone(), body.to_string());
    broadcast_emote_chat(&broadcast);
}

pub fn broadcast_emote_from_console(server: &Server, sender_name: &str, body: &str) {
    let _ = (server, sender_name, body);
}

pub fn broadcast_say_chat(broadcast: &SayChatBroadcast) -> u64 {
    let Some(outbox) = CHAT_OUTBOX.get() else {
        return 0;
    };
    if outbox.peers.is_empty() || broadcast.sender.server != outbox.local {
        return 0;
    }
    if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
        return 0;
    }
    let Ok(parcels) = say_parcels_for_peers(broadcast, &outbox.peers) else {
        return 0;
    };
    let mut sent = 0_u64;
    for parcel in parcels {
        let outbound_parcel = OutboundParcel {
            peer: ServerId(parcel.peer),
            header: StreamHeader::new(parcel.kind, None),
            bytes: parcel.bytes,
        };
        if outbox.outbound.try_send(outbound_parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn broadcast_say_from_player(_server: &Server, player: &Player, body: &str) {
    let Some(sender) = player.cluster_gid() else {
        return;
    };
    let broadcast = SayChatBroadcast::new(sender, player.gameprofile.name.clone(), body.to_string());
    broadcast_say_chat(&broadcast);
}

pub fn broadcast_say_from_console(server: &Server, sender_name: &str, body: &str) {
    let _ = (server, sender_name, body);
}

#[must_use]
pub fn send_private_chat(request: &PrivateChatRequest) -> bool {
    let Some(outbox) = CHAT_OUTBOX.get() else {
        return false;
    };
    if request.from.server != outbox.local {
        return false;
    }
    if !is_valid_chat_body(&request.body)
        || !is_valid_chat_name(&request.from_name)
        || !is_valid_chat_name(&request.to_name)
    {
        return false;
    }
    let Ok(parcel) = private_parcel_for_host(request) else {
        return false;
    };
    let outbound_parcel = OutboundParcel {
        peer: ServerId(parcel.peer),
        header: StreamHeader::new(parcel.kind, None),
        bytes: parcel.bytes,
    };
    if outbox.outbound.try_send(outbound_parcel).is_ok() {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
        true
    } else {
        false
    }
}

#[must_use]
pub fn broadcast_team_chat(broadcast: &TeamChatBroadcast) -> u64 {
    let Some(outbox) = CHAT_OUTBOX.get() else {
        return 0;
    };
    if outbox.peers.is_empty() || broadcast.sender.server != outbox.local {
        return 0;
    }
    if !is_valid_chat_body(&broadcast.body)
        || !is_valid_chat_name(&broadcast.sender_name)
        || broadcast.team.is_empty()
    {
        return 0;
    }
    let Ok(parcels) = team_parcels_for_peers(broadcast, &outbox.peers) else {
        return 0;
    };
    let mut sent = 0_u64;
    for parcel in parcels {
        let outbound_parcel = OutboundParcel {
            peer: ServerId(parcel.peer),
            header: StreamHeader::new(parcel.kind, None),
            bytes: parcel.bytes,
        };
        if outbox.outbound.try_send(outbound_parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    if sent > 0 {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::PlayerSlot;

    #[test]
    fn broadcast_without_outbox_is_noop() {
        broadcast_public_chat(
            GlobalPlayerId::new(ServerId(1), PlayerSlot(0)),
            "steve",
            "hello",
        );
        assert_eq!(chat_broadcast(), 0);
    }
}
