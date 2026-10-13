use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::chat_sync::{
    ChatInboundEffect, authorized_chat_message, decode_control_message, handle_chat_bytes,
    is_valid_chat_body, is_valid_chat_name,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_data::world::{EMOTE_COMMAND, MSG_COMMAND_INCOMING, RAW, SAY_COMMAND};
use pumpkin_protocol::codec::var_int::VarInt;
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

pub fn apply_chat_effect(
    server: &Arc<Server>,
    effect: ChatInboundEffect,
    permit: Option<mpsc::OwnedPermit<super::cluster_chat_presentation::ChatPresentation>>,
) -> bool {
    match effect {
        ChatInboundEffect::PublicDelivery(broadcast) => {
            if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
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
            let _ = server.cluster_lobby_chat_tx.send((
                None,
                outgoing
                    .unsigned_content
                    .clone()
                    .unwrap_or_else(TextComponent::empty),
            ));
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
            {
                debug!("cluster private chat dropped: invalid body or name");
                return false;
            }
            let target = server.get_player_by_name(&request.to_name).or_else(|| {
                server.get_all_players().into_iter().find(|player| {
                    player
                        .gameprofile
                        .name
                        .eq_ignore_ascii_case(&request.to_name)
                })
            });
            let Some(target) = target else {
                let message =
                    TextComponent::text(format!("From {}: {}", request.from_name, request.body));
                let delivered = server
                    .cluster_lobby_chat_tx
                    .send((Some(request.to_name.clone()), message))
                    .is_ok();
                if delivered {
                    CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
                }
                return delivered;
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
            if !broadcast.valid()
                || serde_json::from_str::<TextComponent>(&broadcast.team_display_json).is_err()
                || serde_json::from_str::<TextComponent>(&broadcast.sender_display_json).is_err()
            {
                return false;
            }
            super::cluster_chat_pm::deliver_team_frozen(server, &broadcast);
            CHAT_DELIVERED.fetch_add(1, Ordering::Relaxed);
            true
        }
        ChatInboundEffect::EmoteDelivery(broadcast) => {
            let Some(permit) = permit else { return false };
            if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
                debug!("cluster emote dropped: invalid body or name");
                return false;
            }
            let message = TextComponent::text(broadcast.body.clone());
            let sender = TextComponent::text(broadcast.sender_name.clone());
            permit.send(super::cluster_chat_presentation::ChatPresentation {
                message,
                sender,
                chat_type: EMOTE_COMMAND,
            });
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
            let Some(permit) = permit else { return false };
            if !is_valid_chat_body(&broadcast.body) || !is_valid_chat_name(&broadcast.sender_name) {
                debug!("cluster say dropped: invalid body or name");
                return false;
            }
            let message = TextComponent::text(broadcast.body.clone());
            let sender = TextComponent::text(broadcast.sender_name.clone());
            permit.send(super::cluster_chat_presentation::ChatPresentation {
                message,
                sender,
                chat_type: SAY_COMMAND,
            });
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TeamSenderAuthentication {
    Accepted,
    Deferred,
    Rejected,
}

fn authenticate_team_transport(
    local: ServerId,
    peer: ServerId,
    generation: u64,
    current: Option<u64>,
    broadcast: &pumpkin_cluster::chat_sync::TeamChatBroadcast,
) -> TeamSenderAuthentication {
    if peer == local || broadcast.sender.server != peer || generation == 0 {
        return TeamSenderAuthentication::Rejected;
    }
    match current {
        None => TeamSenderAuthentication::Deferred,
        Some(current) if current < generation => TeamSenderAuthentication::Deferred,
        Some(current) if current == generation => TeamSenderAuthentication::Accepted,
        Some(_) => TeamSenderAuthentication::Rejected,
    }
}

fn authenticate_team_sender(
    broadcast: &pumpkin_cluster::chat_sync::TeamChatBroadcast,
    boot: Option<u64>,
) -> TeamSenderAuthentication {
    if !broadcast.valid() {
        return TeamSenderAuthentication::Rejected;
    }
    match boot {
        None | Some(0) => TeamSenderAuthentication::Deferred,
        Some(boot) if boot != broadcast.source.generation => TeamSenderAuthentication::Rejected,
        Some(_) => TeamSenderAuthentication::Accepted,
    }
}

#[derive(Default)]
struct TeamMessageDedupe {
    seen: std::collections::HashSet<(pumpkin_cluster::realm_completion::PeerGeneration, [u8; 16])>,
    order:
        std::collections::VecDeque<(pumpkin_cluster::realm_completion::PeerGeneration, [u8; 16])>,
}

impl TeamMessageDedupe {
    fn remember(&mut self, broadcast: &pumpkin_cluster::chat_sync::TeamChatBroadcast) -> bool {
        let key = (broadcast.source, broadcast.message_id);
        if !self.seen.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > 4096 {
            self.seen.remove(&self.order.pop_front().unwrap());
        }
        true
    }
}

pub async fn chat_delivery_task(
    server: Arc<Server>,
    local: ServerId,
    mut chat_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut team_dedupe = TeamMessageDedupe::default();
    let mut team_diagnostics = super::cluster_control_registers::Diagnostics::default();
    let mut team_deferred_since = std::collections::HashMap::<
        (pumpkin_cluster::realm_completion::PeerGeneration, [u8; 16]),
        std::time::Instant,
    >::new();
    let mut first = true;
    let mut pending = std::collections::VecDeque::new();
    let mut source_open = true;
    let mut retry = tokio::time::interval(std::time::Duration::from_millis(10));
    loop {
        tokio::select! {
            parcel = chat_rx.recv(), if source_open && pending.len() < 1024 => {
                if let Some(parcel) = parcel { pending.push_back(parcel); }
                else { source_open = false; }
            }
            _ = retry.tick() => {}
        }
        let work = pending.len();
        for _ in 0..work {
            let Some(parcel) = pending.front() else {
                break;
            };
            if parcel.header.kind != StreamKind::Control {
                pending.pop_front();
                continue;
            }
            if first {
                first = false;
                debug!(from = parcel.peer.0, "cluster chat stream started");
            }
            let Ok(message) = decode_control_message(&parcel.bytes) else {
                pending.pop_front();
                continue;
            };
            let generations = server.cluster_peer_generations.load();
            if let pumpkin_cluster::chat_sync::ChatControlMessage::Team(broadcast) = &message {
                match authenticate_team_transport(
                    local,
                    parcel.peer,
                    parcel.generation,
                    generations.get(&parcel.peer).copied(),
                    broadcast,
                ) {
                    TeamSenderAuthentication::Deferred => {
                        let since = team_deferred_since
                            .entry((broadcast.source, broadcast.message_id))
                            .or_insert_with(std::time::Instant::now);
                        if since.elapsed().as_secs() >= 10 {
                            team_diagnostics.deferred(
                                "team_chat",
                                parcel.peer.0,
                                "awaiting transport publication",
                            );
                        }
                        let parcel = pending.pop_front().unwrap();
                        pending.push_back(parcel);
                        continue;
                    }
                    TeamSenderAuthentication::Rejected => {
                        team_diagnostics.rejected(
                            "team_chat",
                            parcel.peer.0,
                            "stale or forged transport origin",
                        );
                        team_deferred_since.remove(&(broadcast.source, broadcast.message_id));
                        pending.pop_front();
                        continue;
                    }
                    TeamSenderAuthentication::Accepted => {}
                }
                if !broadcast.valid()
                    || serde_json::from_str::<TextComponent>(&broadcast.team_display_json).is_err()
                    || serde_json::from_str::<TextComponent>(&broadcast.sender_display_json)
                        .is_err()
                {
                    team_diagnostics.rejected(
                        "team_chat",
                        parcel.peer.0,
                        "invalid frozen team message",
                    );
                    pending.pop_front();
                    continue;
                }
                match authenticate_team_sender(
                    broadcast,
                    server
                        .cluster_peer_incarnations
                        .load()
                        .get(&parcel.peer)
                        .copied(),
                ) {
                    TeamSenderAuthentication::Deferred => {
                        let since = team_deferred_since
                            .entry((broadcast.source, broadcast.message_id))
                            .or_insert_with(std::time::Instant::now);
                        if since.elapsed().as_secs() >= 10 {
                            team_diagnostics.deferred(
                                "team_chat",
                                parcel.peer.0,
                                "awaiting source boot publication",
                            );
                        }
                        let parcel = pending.pop_front().unwrap();
                        pending.push_back(parcel);
                        continue;
                    }
                    TeamSenderAuthentication::Rejected => {
                        team_diagnostics.rejected(
                            "team_chat",
                            parcel.peer.0,
                            "stale sender identity",
                        );
                        team_deferred_since.remove(&(broadcast.source, broadcast.message_id));
                        pending.pop_front();
                        continue;
                    }
                    TeamSenderAuthentication::Accepted => {
                        team_deferred_since.remove(&(broadcast.source, broadcast.message_id));
                    }
                }
            }
            if !authorized_chat_message(
                local,
                parcel.peer,
                parcel.generation,
                generations.get(&parcel.peer).copied(),
                &message,
            ) {
                pending.pop_front();
                continue;
            }
            if let pumpkin_cluster::chat_sync::ChatControlMessage::Team(broadcast) = &message {
                if !team_dedupe.remember(broadcast) {
                    pending.pop_front();
                    continue;
                }
            }
            let effect = match handle_chat_bytes(local, &parcel.bytes) {
                Ok(effect) => effect,
                Err(error) => {
                    debug!(%error, "cluster chat parcel decode failed");
                    pending.pop_front();
                    continue;
                }
            };
            let permit = if matches!(
                effect,
                ChatInboundEffect::EmoteDelivery(_) | ChatInboundEffect::SayDelivery(_)
            ) {
                match server
                    .cluster_chat_presentation_tx
                    .clone()
                    .try_reserve_owned()
                {
                    Ok(permit) => Some(permit),
                    Err(mpsc::error::TrySendError::Full(_)) => break,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::error!("cluster chat presentation owner closed");
                        return;
                    }
                }
            } else {
                None
            };
            apply_chat_effect(&server, effect, permit);
            pending.pop_front();
        }
        if !source_open && pending.is_empty() {
            break;
        }
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

pub(crate) async fn control_fanout_task(
    mut source: mpsc::Receiver<InboundParcel>,
    admin_tx: mpsc::Sender<InboundParcel>,
    chat_tx: mpsc::Sender<InboundParcel>,
    invsee_tx: mpsc::Sender<super::cluster_invsee::InvseeCommand>,
    presence_tx: mpsc::Sender<InboundParcel>,
    hide_tx: mpsc::Sender<InboundParcel>,
    world_time_tx: mpsc::Sender<InboundParcel>,
    health_policy_tx: mpsc::Sender<InboundParcel>,
    handoff_tx: mpsc::Sender<InboundParcel>,
    #[cfg(feature = "cluster-e2e")] test_tx: mpsc::Sender<InboundParcel>,
) {
    const LIMIT: usize = 64;
    let mut admin = std::collections::VecDeque::new();
    let mut chat = std::collections::VecDeque::new();
    let mut invsee = std::collections::VecDeque::new();
    let mut presence = std::collections::VecDeque::new();
    let mut hide = std::collections::VecDeque::new();
    let mut world_time = std::collections::VecDeque::new();
    let mut health_policy = std::collections::VecDeque::new();
    let mut handoff = std::collections::VecDeque::new();
    #[cfg(feature = "cluster-e2e")]
    let mut test = std::collections::VecDeque::new();
    let mut source_open = true;
    let mut flush = tokio::time::interval(std::time::Duration::from_millis(10));
    macro_rules! flush_queue {
        ($queue:expr, $tx:expr) => {
            while let Some(value) = $queue.pop_front() {
                match $tx.try_send(value) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(value)) => {
                        $queue.push_front(value);
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        $queue.clear();
                        break;
                    }
                }
            }
        };
    }
    loop {
        let full = [
            admin.len(),
            chat.len(),
            invsee.len(),
            presence.len(),
            hide.len(),
            world_time.len(),
            health_policy.len(),
            handoff.len(),
        ]
        .into_iter()
        .any(|len| len >= LIMIT);
        #[cfg(feature = "cluster-e2e")]
        let full = full || test.len() >= LIMIT;
        tokio::select! {
            parcel = source.recv(), if source_open && !full => {
                if let Some(parcel) = parcel {
                    if !admin_tx.is_closed() { admin.push_back(parcel.clone()); }
                    if !chat_tx.is_closed() { chat.push_back(parcel.clone()); }
                    if !invsee_tx.is_closed() { invsee.push_back(super::cluster_invsee::InvseeCommand::Inbound(parcel.clone())); }
                    if !presence_tx.is_closed() { presence.push_back(parcel.clone()); }
                    if !hide_tx.is_closed() { hide.push_back(parcel.clone()); }
                    if !world_time_tx.is_closed() { world_time.push_back(parcel.clone()); }
                    if !health_policy_tx.is_closed() { health_policy.push_back(parcel.clone()); }
                    if !handoff_tx.is_closed() { handoff.push_back(parcel.clone()); }
                    #[cfg(feature = "cluster-e2e")]
                    if !test_tx.is_closed() { test.push_back(parcel); }
                } else { source_open = false; }
            }
            _ = flush.tick() => {}
        }
        flush_queue!(admin, admin_tx);
        flush_queue!(chat, chat_tx);
        flush_queue!(invsee, invsee_tx);
        flush_queue!(presence, presence_tx);
        flush_queue!(hide, hide_tx);
        flush_queue!(world_time, world_time_tx);
        flush_queue!(health_policy, health_policy_tx);
        flush_queue!(handoff, handoff_tx);
        #[cfg(feature = "cluster-e2e")]
        flush_queue!(test, test_tx);
        let empty = admin.is_empty()
            && chat.is_empty()
            && invsee.is_empty()
            && presence.is_empty()
            && hide.is_empty()
            && world_time.is_empty()
            && health_policy.is_empty()
            && handoff.is_empty();
        #[cfg(feature = "cluster-e2e")]
        let empty = empty && test.is_empty();
        if !source_open && empty {
            break;
        }
        if admin_tx.is_closed()
            && chat_tx.is_closed()
            && invsee_tx.is_closed()
            && presence_tx.is_closed()
            && hide_tx.is_closed()
            && world_time_tx.is_closed()
            && health_policy_tx.is_closed()
            && handoff_tx.is_closed()
            && {
                #[cfg(feature = "cluster-e2e")]
                {
                    test_tx.is_closed()
                }
                #[cfg(not(feature = "cluster-e2e"))]
                {
                    true
                }
            }
        {
            warn!("cluster control fanout closed, stopping");
            break;
        }
    }
    debug!("cluster control fanout closed");
}

pub(crate) fn spawn_control_fanout(
    server: &Arc<Server>,
    source: mpsc::Receiver<InboundParcel>,
    admin_tx: mpsc::Sender<InboundParcel>,
    chat_tx: mpsc::Sender<InboundParcel>,
    invsee_tx: mpsc::Sender<super::cluster_invsee::InvseeCommand>,
    presence_tx: mpsc::Sender<InboundParcel>,
    hide_tx: mpsc::Sender<InboundParcel>,
    world_time_tx: mpsc::Sender<InboundParcel>,
    health_policy_tx: mpsc::Sender<InboundParcel>,
    handoff_tx: mpsc::Sender<InboundParcel>,
    #[cfg(feature = "cluster-e2e")] test_tx: mpsc::Sender<InboundParcel>,
) {
    server.spawn_task(control_fanout_task(
        source,
        admin_tx,
        chat_tx,
        invsee_tx,
        presence_tx,
        hide_tx,
        world_time_tx,
        health_policy_tx,
        handoff_tx,
        #[cfg(feature = "cluster-e2e")]
        test_tx,
    ));
}

#[cfg(test)]
mod control_fanout_tests {
    use super::*;
    use pumpkin_cluster::streams::StreamHeader;

    #[tokio::test]
    async fn slow_control_consumer_does_not_hold_other_consumer() {
        let (source_tx, source_rx) = mpsc::channel(2);
        let (admin_tx, mut admin_rx) = mpsc::channel(1);
        let (chat_tx, mut chat_rx) = mpsc::channel(1);
        let (invsee_tx, invsee_rx) = mpsc::channel(1);
        let (presence_tx, presence_rx) = mpsc::channel(1);
        let (hide_tx, hide_rx) = mpsc::channel(1);
        let (world_time_tx, world_time_rx) = mpsc::channel(1);
        let (health_policy_tx, health_policy_rx) = mpsc::channel(1);
        let (handoff_tx, handoff_rx) = mpsc::channel(1);
        #[cfg(feature = "cluster-e2e")]
        let (test_tx, test_rx) = mpsc::channel(1);
        drop((
            invsee_rx,
            presence_rx,
            hide_rx,
            world_time_rx,
            health_policy_rx,
            handoff_rx,
        ));
        #[cfg(feature = "cluster-e2e")]
        drop(test_rx);
        let task = tokio::spawn(control_fanout_task(
            source_rx,
            admin_tx,
            chat_tx,
            invsee_tx,
            presence_tx,
            hide_tx,
            world_time_tx,
            health_policy_tx,
            handoff_tx,
            #[cfg(feature = "cluster-e2e")]
            test_tx,
        ));
        for sequence in [1_u8, 2] {
            source_tx
                .send(InboundParcel {
                    peer: ServerId(2),
                    generation: 0,
                    header: StreamHeader::new(StreamKind::Control, None),
                    bytes: vec![sequence],
                })
                .await
                .unwrap();
        }
        drop(source_tx);
        let received = tokio::time::timeout(std::time::Duration::from_millis(250), async {
            let first = chat_rx.recv().await.unwrap();
            let second = chat_rx.recv().await.unwrap();
            (first.bytes, second.bytes)
        })
        .await
        .unwrap();
        assert_eq!(received, (vec![1], vec![2]));
        assert_eq!(admin_rx.recv().await.unwrap().bytes, vec![1]);
        assert_eq!(admin_rx.recv().await.unwrap().bytes, vec![2]);
        tokio::time::timeout(std::time::Duration::from_millis(250), task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[cfg(test)]
mod team_tests {
    use super::*;
    use pumpkin_cluster::chat_sync::TeamChatBroadcast;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot};
    use pumpkin_cluster::realm_completion::PeerGeneration;

    fn message() -> TeamChatBroadcast {
        TeamChatBroadcast::new(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            "Alice".into(),
            pumpkin_cluster::protocol::WorldId::new(0, 1),
            "red".into(),
            "hello".into(),
            PeerGeneration {
                server: ServerId(2),
                generation: 12,
            },
            [1; 16],
            17,
            vec![[1; 16], [2; 16]],
            "{\"text\":\"Red\",\"color\":\"red\"}".into(),
            "{\"text\":\"Alice\",\"bold\":true}".into(),
        )
    }

    #[test]
    fn newer_transport_parcel_defers_until_publication_but_old_epoch_rejects() {
        let message = message();
        assert_eq!(
            authenticate_team_transport(ServerId(1), ServerId(2), 3, Some(2), &message),
            TeamSenderAuthentication::Deferred
        );
        assert_eq!(
            authenticate_team_transport(ServerId(1), ServerId(2), 3, Some(3), &message),
            TeamSenderAuthentication::Accepted
        );
        assert_eq!(
            authenticate_team_transport(ServerId(1), ServerId(2), 2, Some(3), &message),
            TeamSenderAuthentication::Rejected
        );
        assert_eq!(
            authenticate_team_transport(ServerId(1), ServerId(9), 3, Some(3), &message),
            TeamSenderAuthentication::Rejected
        );
    }

    #[test]
    fn missing_source_boot_defers_then_exact_original_boot_authenticates() {
        let message = message();
        assert_eq!(
            authenticate_team_sender(&message, None),
            TeamSenderAuthentication::Deferred
        );
        assert_eq!(
            authenticate_team_sender(&message, Some(12)),
            TeamSenderAuthentication::Accepted
        );
        assert_eq!(
            authenticate_team_sender(&message, Some(13)),
            TeamSenderAuthentication::Rejected
        );
        let mut forged = message.clone();
        forged.source.server = ServerId(9);
        assert_eq!(
            authenticate_team_sender(&forged, Some(12)),
            TeamSenderAuthentication::Rejected
        );
    }

    #[test]
    fn original_source_attested_session_survives_newer_session_arriving_first() {
        let original = message();
        let mut relogged = original.clone();
        relogged.sender_session += 1;
        relogged.sender_uuid = [3; 16];
        relogged.recipients.push([3; 16]);
        relogged.message_id = [4; 16];
        assert_eq!(
            authenticate_team_sender(&relogged, Some(12)),
            TeamSenderAuthentication::Accepted
        );
        assert_eq!(
            authenticate_team_sender(&original, Some(12)),
            TeamSenderAuthentication::Accepted
        );
        let mut dedupe = TeamMessageDedupe::default();
        assert!(dedupe.remember(&relogged));
        assert!(dedupe.remember(&original));
        assert!(!dedupe.remember(&original));
    }

    #[test]
    fn original_team_message_id_deduplicates_across_payload_replay() {
        let message = message();
        let mut dedupe = TeamMessageDedupe::default();
        assert!(dedupe.remember(&message));
        let mut replay = message.clone();
        replay.body = "different current text".into();
        replay.recipients = vec![[1; 16]];
        assert!(!dedupe.remember(&replay));
        replay.source.generation += 1;
        assert!(dedupe.remember(&replay));
    }
}
