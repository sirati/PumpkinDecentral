use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::chat_sync::{
    ChatInboundEffect, handle_chat_bytes, is_valid_chat_body, is_valid_chat_name,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::translation;
use pumpkin_data::world::{EMOTE_COMMAND, MSG_COMMAND_INCOMING, RAW, SAY_COMMAND};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::CSystemChatMessage;
use pumpkin_util::text::TextComponent;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::entity::EntityBase;
use crate::entity::player::Player;

static CHAT_DELIVERED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn chat_delivered() -> u64 {
    CHAT_DELIVERED.load(Ordering::Relaxed)
}

fn send_lobby_system_message(server: &Server, message: &TextComponent) {
    let packet = CSystemChatMessage::new(message, false);
    for waiter in server.lobby_waiters.load().iter() {
        if let Some(client) = waiter.client.java() {
            client.try_send_packet(&packet);
        }
    }
}

fn is_announced_remote_player(
    source: ServerId,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    name: &str,
) -> bool {
    gid.server == source
        && super::cluster_presence::remote_presence_entry(gid)
        .is_some_and(|entry| entry.name.eq_ignore_ascii_case(name))
}

pub fn apply_chat_bytes(
    server: &Arc<Server>,
    local: ServerId,
    source: ServerId,
    bytes: &[u8],
) -> bool {
    let effect = match handle_chat_bytes(local, bytes) {
        Ok(effect) => effect,
        Err(error) => {
            debug!(%error, "cluster chat parcel decode failed");
            return false;
        }
    };
    match effect {
        ChatInboundEffect::PublicDelivery(broadcast) => {
            if !is_valid_chat_body(&broadcast.body)
                || !is_valid_chat_name(&broadcast.sender_name)
                || !is_announced_remote_player(source, broadcast.sender, &broadcast.sender_name)
            {
                debug!("cluster public chat dropped: invalid body or name");
                return false;
            }
            let decorated = TextComponent::chat_decorated(
                &server.advanced_config.chat.format,
                &broadcast.sender_name,
                &broadcast.body,
            );
            let outgoing = crate::net::chat::PlayerChatMessage::system(broadcast.body.clone())
                .with_unsigned_content(decorated);
            send_lobby_system_message(server, &TextComponent::text(format!(
                "<{}> {}",
                broadcast.sender_name, broadcast.body
            )));
            let chat_type: VarInt = (RAW + 1).into();
            server.broadcast_chat_message(
                &outgoing,
                Player::is_text_filtering_enabled,
                None,
                chat_type,
                &TextComponent::empty(),
                None,
            );
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            info!(
                "<chat> {}: {}",
                broadcast.sender_name.as_str(),
                broadcast.body.as_str()
            );
            true
        }
        ChatInboundEffect::PrivateDelivery(request) => {
            if !is_valid_chat_body(&request.body)
                || !is_valid_chat_name(&request.from_name)
                || !is_valid_chat_name(&request.to_name)
                || !is_announced_remote_player(source, request.from, &request.from_name)
            {
                debug!("cluster private chat dropped: invalid body or name");
                return false;
            }
            let target = server.get_player_by_name(&request.to_name).or_else(|| {
                server
                    .get_all_players()
                    .into_iter()
                    .find(|player| player.gameprofile.name.eq_ignore_ascii_case(&request.to_name))
            });
            let Some(target) = target else {
                if let Some(waiter) = server.lobby_waiters.load().iter().find(|waiter| {
                    waiter.profile.name.eq_ignore_ascii_case(&request.to_name)
                }) {
                    if let Some(client) = waiter.client.java() {
                        let message = TextComponent::text(format!(
                            "[{} -> you] {}",
                            request.from_name, request.body
                        ));
                        client.try_send_packet(&CSystemChatMessage::new(&message, false));
                        CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                }
                debug!(
                    to = request.to_name.as_str(),
                    "cluster private chat dropped: target not local"
                );
                return false;
            };
            let from_display = TextComponent::text(request.from_name.clone());
            let target_display = target.get_display_name();
            let msg_text = TextComponent::text(request.body.clone());
            target.send_message(
                &msg_text,
                MSG_COMMAND_INCOMING,
                &from_display,
                Some(&target_display),
            );
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            debug!(
                from = request.from.server.0,
                to = request.to.server.0,
                "cluster private chat delivered"
            );
            true
        }
        ChatInboundEffect::TeamDelivery(broadcast) => {
            if !is_valid_chat_body(&broadcast.body)
                || !is_valid_chat_name(&broadcast.sender_name)
                || broadcast.team.is_empty()
                || !is_announced_remote_player(source, broadcast.sender, &broadcast.sender_name)
            {
                debug!("cluster team chat dropped: invalid body, name, or team");
                return false;
            }
            let sender_display = TextComponent::text(broadcast.sender_name.clone());
            let msg_component = TextComponent::text(broadcast.body.clone());
            for world in server.worlds.load().iter() {
                let team = world.team_snapshot().get(&broadcast.team).cloned();
                let Some(team) = team else {
                    continue;
                };
                let team_display = team.display_name.clone().color_named(team.color);
                for player in world.players.load().iter() {
                    if !team.players.contains(&player.gameprofile.name) {
                        continue;
                    }
                    let msg = TextComponent::translate_cross(
                        translation::java::CHAT_TYPE_TEAM_TEXT,
                        translation::java::CHAT_TYPE_TEAM_TEXT,
                        [
                            team_display.clone(),
                            sender_display.clone(),
                            msg_component.clone(),
                        ],
                    );
                    player.send_system_message(&msg);
                }
            }
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            debug!(
                from = broadcast.sender.server.0,
                team = broadcast.team.as_str(),
                "cluster team chat delivered"
            );
            true
        }
        ChatInboundEffect::EmoteDelivery(broadcast) => {
            if !is_valid_chat_body(&broadcast.body)
                || !is_valid_chat_name(&broadcast.sender_name)
                || !is_announced_remote_player(source, broadcast.sender, &broadcast.sender_name)
            {
                debug!("cluster emote dropped: invalid body or name");
                return false;
            }
            let message = TextComponent::text(broadcast.body.clone());
            let sender = TextComponent::text(broadcast.sender_name.clone());
            send_lobby_system_message(
                server,
                &TextComponent::text(format!("* {} {}", broadcast.sender_name, broadcast.body)),
            );
            if super::cluster_hide::is_hidden_gid(&broadcast.sender) {
                for viewer in server.get_all_players() {
                    if viewer.has_permission(server, super::cluster_hide::HIDE_PERMISSION) {
                        viewer.send_message(&message, EMOTE_COMMAND, &sender, None);
                    }
                }
            } else {
                server.broadcast_message(&message, &sender, EMOTE_COMMAND, None);
            }
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            info!(
                "* {} {}",
                broadcast.sender_name.as_str(),
                broadcast.body.as_str()
            );
            debug!(
                from = broadcast.sender.server.0,
                name = broadcast.sender_name.as_str(),
                "cluster emote delivered"
            );
            true
        }
        ChatInboundEffect::SayDelivery(broadcast) => {
            if !is_valid_chat_body(&broadcast.body)
                || !is_valid_chat_name(&broadcast.sender_name)
                || !is_announced_remote_player(source, broadcast.sender, &broadcast.sender_name)
            {
                debug!("cluster say dropped: invalid body or name");
                return false;
            }
            let message = TextComponent::text(broadcast.body.clone());
            let sender = TextComponent::text(broadcast.sender_name.clone());
            send_lobby_system_message(
                server,
                &TextComponent::text(format!("<{}> {}", broadcast.sender_name, broadcast.body)),
            );
            server.broadcast_message(&message, &sender, SAY_COMMAND, None);
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            info!(
                "<chat> {}: {}",
                broadcast.sender_name.as_str(),
                broadcast.body.as_str()
            );
            debug!(
                from = broadcast.sender.server.0,
                name = broadcast.sender_name.as_str(),
                "cluster say delivered"
            );
            true
        }
        ChatInboundEffect::Ignored => {
            debug!("cluster chat parcel ignored for this server");
            false
        }
    }
}

pub async fn chat_delivery_task(
    server: Arc<Server>,
    local: ServerId,
    mut chat_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut first = true;
    while let Some(parcel) = chat_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster chat stream started");
        }
        apply_chat_bytes(&server, local, parcel.peer, &parcel.bytes);
    }
    debug!("cluster chat stream closed");
}

pub fn spawn_chat_delivery(
    server: &Arc<Server>,
    local: ServerId,
    chat_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(chat_delivery_task(task_server, local, chat_rx));
}

pub async fn control_fanout_task(
    mut source: mpsc::Receiver<InboundParcel>,
    admin_tx: mpsc::Sender<InboundParcel>,
    chat_tx: mpsc::Sender<InboundParcel>,
    presence_tx: mpsc::Sender<InboundParcel>,
    hide_tx: mpsc::Sender<InboundParcel>,
    world_time_tx: mpsc::Sender<InboundParcel>,
    playerdata_tx: mpsc::Sender<InboundParcel>,
    lobby_control_tx: mpsc::Sender<InboundParcel>,
    boundary_tx: mpsc::Sender<InboundParcel>,
) {
    while let Some(parcel) = source.recv().await {
        let admin_result = admin_tx.try_send(parcel.clone());
        let chat_result = chat_tx.try_send(parcel.clone());
        let presence_result = presence_tx.try_send(parcel.clone());
        let hide_result = hide_tx.try_send(parcel.clone());
        let world_time_result = world_time_tx.try_send(parcel.clone());
        let playerdata_result = playerdata_tx.try_send(parcel.clone());
        let lobby_control_result = lobby_control_tx.try_send(parcel.clone());
        let boundary_result = boundary_tx.try_send(parcel);
        if admin_result.is_err()
            && chat_result.is_err()
            && presence_result.is_err()
            && hide_result.is_err()
            && world_time_result.is_err()
            && playerdata_result.is_err()
            && lobby_control_result.is_err()
            && boundary_result.is_err()
        {
            warn!("cluster control fanout closed, stopping");
            break;
        }
    }
    debug!("cluster control fanout closed");
}

pub fn spawn_control_fanout(
    server: &Arc<Server>,
    source: mpsc::Receiver<InboundParcel>,
    admin_tx: mpsc::Sender<InboundParcel>,
    chat_tx: mpsc::Sender<InboundParcel>,
    presence_tx: mpsc::Sender<InboundParcel>,
    hide_tx: mpsc::Sender<InboundParcel>,
    world_time_tx: mpsc::Sender<InboundParcel>,
    playerdata_tx: mpsc::Sender<InboundParcel>,
    lobby_control_tx: mpsc::Sender<InboundParcel>,
    boundary_tx: mpsc::Sender<InboundParcel>,
) {
    server.spawn_task(control_fanout_task(
        source,
        admin_tx,
        chat_tx,
        presence_tx,
        hide_tx,
        world_time_tx,
        playerdata_tx,
        lobby_control_tx,
        boundary_tx,
    ));
}
