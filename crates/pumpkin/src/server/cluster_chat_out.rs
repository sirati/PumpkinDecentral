use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::chat_sync::{
    ChatControlMessage, EmoteChatBroadcast, PrivateChatRequest, PublicChatBroadcast,
    SayChatBroadcast, TeamChatBroadcast, encode_control_message, is_valid_chat_body,
    is_valid_chat_name, private_parcel_for_host,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};

use super::Server;
use crate::entity::player::Player;

static CHAT_BROADCAST: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn chat_broadcast() -> u64 {
    CHAT_BROADCAST.load(Ordering::Relaxed)
}

pub fn broadcast_public_chat(
    server: &Server,
    sender: GlobalPlayerId,
    sender_name: &str,
    body: &str,
) -> bool {
    if !server.advanced_config.cluster.enabled {
        return true;
    }
    let peers = server.cluster_connected_peers.load();
    if sender.server != ServerId(server.advanced_config.cluster.server_id) {
        return false;
    }
    if !is_valid_chat_body(body) || !is_valid_chat_name(sender_name) {
        return false;
    }
    if peers.is_empty() {
        return true;
    }
    let broadcast = PublicChatBroadcast::new(sender, sender_name.to_string(), body.to_string());
    let Ok(bytes) = encode_control_message(&ChatControlMessage::Public(broadcast)) else {
        return false;
    };
    let sent =
        server
            .cluster_global_outbound
            .try_event(server, peers.iter().copied().collect(), bytes);
    if sent {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn broadcast_public_chat_from_player(server: &Server, player: &Player, body: &str) -> bool {
    if !server.advanced_config.cluster.enabled {
        return true;
    }
    let Some(sender) = player.cluster_gid() else {
        return false;
    };
    if !broadcast_public_chat(server, sender, &player.gameprofile.name, body) {
        return false;
    }
    let decorated = pumpkin_util::text::TextComponent::chat_decorated(
        &server.advanced_config.chat.format,
        &player.gameprofile.name,
        body,
    );
    let _ = server.cluster_lobby_chat_tx.send((None, decorated));
    true
}

pub fn broadcast_emote_chat(server: &Server, broadcast: &EmoteChatBroadcast) -> bool {
    let peers = server.cluster_connected_peers.load();
    if broadcast.sender.server != ServerId(server.advanced_config.cluster.server_id) {
        return false;
    }
    if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
        return false;
    }
    if peers.is_empty() {
        return true;
    }
    let Ok(bytes) = encode_control_message(&ChatControlMessage::Emote(broadcast.clone())) else {
        return false;
    };
    let sent =
        server
            .cluster_global_outbound
            .try_event(server, peers.iter().copied().collect(), bytes);
    if sent {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn broadcast_emote_from_player(server: &Server, player: &Player, body: &str) -> bool {
    let Some(sender) = player.cluster_gid() else {
        return false;
    };
    let broadcast =
        EmoteChatBroadcast::new(sender, player.gameprofile.name.clone(), body.to_string());
    broadcast_emote_chat(server, &broadcast)
}

pub fn broadcast_emote_from_console(server: &Server, sender_name: &str, body: &str) -> bool {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let broadcast = EmoteChatBroadcast::new(
        GlobalPlayerId::new(local, PlayerSlot(0)),
        sender_name.to_string(),
        body.to_string(),
    );
    broadcast_emote_chat(server, &broadcast)
}

pub fn broadcast_say_chat(server: &Server, broadcast: &SayChatBroadcast) -> bool {
    let peers = server.cluster_connected_peers.load();
    if broadcast.sender.server != ServerId(server.advanced_config.cluster.server_id) {
        return false;
    }
    if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
        return false;
    }
    if peers.is_empty() {
        return true;
    }
    let Ok(bytes) = encode_control_message(&ChatControlMessage::Say(broadcast.clone())) else {
        return false;
    };
    let sent =
        server
            .cluster_global_outbound
            .try_event(server, peers.iter().copied().collect(), bytes);
    if sent {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}

pub fn broadcast_say_from_player(server: &Server, player: &Player, body: &str) -> bool {
    let Some(sender) = player.cluster_gid() else {
        return false;
    };
    let broadcast =
        SayChatBroadcast::new(sender, player.gameprofile.name.clone(), body.to_string());
    broadcast_say_chat(server, &broadcast)
}

pub fn broadcast_say_from_console(server: &Server, sender_name: &str, body: &str) -> bool {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let broadcast = SayChatBroadcast::new(
        GlobalPlayerId::new(local, PlayerSlot(0)),
        sender_name.to_string(),
        body.to_string(),
    );
    broadcast_say_chat(server, &broadcast)
}

#[must_use]
pub fn send_private_chat(server: &Server, request: &PrivateChatRequest) -> bool {
    if request.from.server != ServerId(server.advanced_config.cluster.server_id) {
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
    if !server
        .cluster_connected_peers
        .load()
        .contains(&request.to.server)
    {
        return false;
    }
    if server
        .cluster_global_outbound
        .try_event(server, vec![ServerId(parcel.peer)], parcel.bytes)
    {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
        true
    } else {
        false
    }
}

#[must_use]
pub fn broadcast_team_chat(server: &Server, broadcast: &TeamChatBroadcast) -> u64 {
    let peers = server.cluster_connected_peers.load();
    if peers.is_empty()
        || broadcast.sender.server != ServerId(server.advanced_config.cluster.server_id)
    {
        return 0;
    }
    if !is_valid_chat_body(&broadcast.body)
        || !is_valid_chat_name(&broadcast.sender_name)
        || broadcast.team.is_empty()
    {
        return 0;
    }
    let Ok(bytes) = encode_control_message(&ChatControlMessage::Team(broadcast.clone())) else {
        return 0;
    };
    let sent =
        if server
            .cluster_global_outbound
            .try_event(server, peers.iter().copied().collect(), bytes)
        {
            peers.len() as u64
        } else {
            0
        };
    if sent > 0 {
        CHAT_BROADCAST.fetch_add(1, Ordering::Relaxed);
    }
    sent
}
