use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use pumpkin_config::AntiSpamConfig;
use pumpkin_data::world::RAW;
use pumpkin_protocol::bedrock::server::text::SText;
use pumpkin_protocol::java::client::play::CSystemChatMessage;
use pumpkin_util::PermissionLvl;
use pumpkin_util::text::TextComponent;
use tokio::sync::{mpsc, oneshot};

use crate::entity::player::Player;
use crate::error::PumpkinError;
use crate::net::{ClientPlatform, DisconnectReason};
use crate::plugin::player::player_chat::PlayerChatEvent;
use crate::plugin::player::player_command_send::PlayerCommandSendEvent;
use crate::server::cluster_lobby::LobbyCommandSender;

use super::Server;

static CALLBACK_WARN_LAST: AtomicU64 = AtomicU64::new(0);

tokio::task_local! {
    static COMMAND_TASKS: mpsc::Sender<oneshot::Receiver<()>>;
}

pub enum StagedSessionInput {
    Java {
        body: String,
        received_at: Instant,
    },
    Bedrock {
        body: String,
        xuid: String,
        filtered: Option<String>,
        received_at: Instant,
    },
    Command {
        command: String,
        received_at: Instant,
    },
    SecureJava {
        input: super::cluster_secure_chat::SignedInput,
        received_at: Instant,
    },
    SecureAck {
        offset: i32,
        received_at: Instant,
    },
    SecureSession {
        session: pumpkin_protocol::java::server::play::SPlayerSession,
        received_at: Instant,
    },
}

impl StagedSessionInput {
    pub fn received_at(&self) -> Instant {
        match self {
            Self::Java { received_at, .. }
            | Self::Bedrock { received_at, .. }
            | Self::Command { received_at, .. }
            | Self::SecureJava { received_at, .. }
            | Self::SecureAck { received_at, .. }
            | Self::SecureSession { received_at, .. } => *received_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Completed,
    Rejected,
    Failed,
}

pub struct SessionInput {
    pub player: Arc<Player>,
    pub staged: StagedSessionInput,
    pub completion: oneshot::Sender<SessionOutcome>,
}

pub struct LobbyCommandInput {
    source: Arc<LobbyCommandSender>,
    command: String,
    received_at: Instant,
}

pub fn submit_lobby_command(
    server: &Server,
    source: Arc<LobbyCommandSender>,
    command: String,
) -> bool {
    server
        .cluster_lobby_command_tx
        .try_send(LobbyCommandInput {
            source,
            command,
            received_at: Instant::now(),
        })
        .is_ok()
}

pub fn spawn_lobby_commands(server: &Arc<Server>, mut rx: mpsc::Receiver<LobbyCommandInput>) {
    struct Route {
        client: std::sync::Weak<crate::net::ClientPlatform>,
        tx: mpsc::Sender<LobbyCommandInput>,
        budget: SpamBudget,
    }
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut routes: std::collections::HashMap<
            pumpkin_cluster::identity::GlobalPlayerId,
            Route,
        > = std::collections::HashMap::new();
        let mut sweep = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                input = rx.recv() => {
                    let Some(input) = input else { break };
                    let gid = input.source.gid;
                    if !routes.get(&gid).is_some_and(|route| route.client.upgrade().is_some_and(|client| Arc::ptr_eq(&client, &input.source.client))) {
                        if routes.len() >= 4096 && !routes.contains_key(&gid) {
                            routes.retain(|_, route| route.client.upgrade().is_some());
                            if routes.len() >= 4096 {
                                input.source.send_message(&TextComponent::text("Cluster command queue is full"));
                                continue;
                            }
                        }
                        let (tx, mut source_rx) = mpsc::channel::<LobbyCommandInput>(64);
                        let worker_server = Arc::clone(&task_server);
                        task_server.spawn_task(async move {
                            while let Some(next) = source_rx.recv().await {
                                worker_server.command_dispatcher.load().handle_command(
                                    &crate::command::CommandSender::PreJoin(next.source.clone())
                                        .into_source(&worker_server),
                                    &next.command,
                                );
                            }
                        });
                        routes.insert(gid, Route {
                            client: Arc::downgrade(&input.source.client),
                            tx,
                            budget: SpamBudget { count: 0, last: input.received_at },
                        });
                    }
                    let route = routes.get_mut(&gid).unwrap();
                    let anti_spam = &task_server.advanced_config.chat.anti_spam;
                    if anti_spam.enabled
                        && !(anti_spam.ops_bypass && input.source.permission > PermissionLvl::Zero)
                        && route.budget.charge(input.received_at, anti_spam.message_cost, anti_spam.decay_per_tick, anti_spam.command_threshold_ticks())
                    {
                        input.source.send_message(&TextComponent::text("Disconnected for spam"));
                        continue;
                    }
                    if let Err(error) = route.tx.try_send(input) {
                        error.into_inner().source.send_message(&TextComponent::text("Cluster command queue is full"));
                    }
                }
                _ = sweep.tick() => {
                    routes.retain(|_, route| route.client.upgrade().is_some());
                }
            }
        }
    });
}

impl SessionInput {
    pub fn new(
        player: Arc<Player>,
        staged: StagedSessionInput,
        completion: oneshot::Sender<SessionOutcome>,
    ) -> Self {
        Self {
            player,
            staged,
            completion,
        }
    }
}

struct SpamBudget {
    count: u32,
    last: Instant,
}

impl SpamBudget {
    fn charge(&mut self, now: Instant, cost: u32, decay_per_tick: u32, threshold: u32) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_millis() / 50;
        let ticks = u32::try_from(elapsed).unwrap_or(u32::MAX);
        self.count = self
            .count
            .saturating_sub(ticks.saturating_mul(decay_per_tick));
        self.last += std::time::Duration::from_millis(u64::from(ticks) * 50);
        self.count = self.count.saturating_add(cost);
        self.count > threshold
    }
}

pub fn try_submit(server: &Server, input: SessionInput) -> Result<(), SessionInput> {
    server
        .cluster_session_ingress_tx
        .try_send(input)
        .map_err(|error| error.into_inner())
}

pub fn stage(player: &Arc<Player>, staged: StagedSessionInput) -> Result<(), StagedSessionInput> {
    if player.cluster_departing.load(Ordering::Acquire) {
        return Err(staged);
    }
    player.cluster_session_staged.push(staged)
}

fn warn_timeout() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_secs())
        .unwrap_or(0);
    let previous = CALLBACK_WARN_LAST.load(Ordering::Relaxed);
    if now.saturating_sub(previous) >= 60
        && CALLBACK_WARN_LAST
            .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!("cluster public chat plugin callback timed out");
    }
}

struct SessionRoute {
    player: std::sync::Weak<Player>,
    budget: SpamBudget,
    tx: mpsc::Sender<SessionInput>,
}

fn spam_threshold(input: &StagedSessionInput, config: &AntiSpamConfig) -> u32 {
    match input {
        StagedSessionInput::Command { .. } => config.command_threshold_ticks(),
        StagedSessionInput::Java { .. }
        | StagedSessionInput::Bedrock { .. }
        | StagedSessionInput::SecureJava { .. } => config.chat_threshold_ticks(),
        StagedSessionInput::SecureAck { .. } | StagedSessionInput::SecureSession { .. } => u32::MAX,
    }
}

pub fn register_command_task<F>(server: &Server, task: F) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    let (completion, receiver) = oneshot::channel();
    let admitted = COMMAND_TASKS
        .try_with(|tasks| tasks.try_send(receiver).is_ok())
        .unwrap_or(true);
    if admitted {
        server.spawn_task(async move {
            task.await;
            let _ = completion.send(());
        });
    }
    admitted
}

async fn process_input(server: &Arc<Server>, input: &SessionInput) -> SessionOutcome {
    let player = input.player.clone();
    if let StagedSessionInput::SecureAck { offset, .. } = &input.staged {
        let Some(result) = server.cluster_secure_chat.ack(player.clone(), *offset) else {
            return SessionOutcome::Failed;
        };
        return match result.await {
            Ok(Ok(())) => SessionOutcome::Completed,
            Ok(Err(error)) => {
                player.kick(
                    DisconnectReason::Kicked,
                    &TextComponent::text(
                        error
                            .client_kick_reason()
                            .unwrap_or_else(|| error.to_string()),
                    ),
                );
                SessionOutcome::Rejected
            }
            Err(_) => SessionOutcome::Failed,
        };
    }
    if let StagedSessionInput::SecureSession { session, .. } = &input.staged {
        let ClientPlatform::Java(client) = player.client.as_ref() else {
            return SessionOutcome::Rejected;
        };
        if let Err(error) = client
            .validate_chat_session_uuid(player.gameprofile.id, server, session)
            .await
        {
            player.kick(
                DisconnectReason::Kicked,
                &TextComponent::text(
                    error
                        .client_kick_reason()
                        .unwrap_or_else(|| error.to_string()),
                ),
            );
            return SessionOutcome::Rejected;
        }
        let session = pumpkin_protocol::java::server::play::SPlayerSession {
            session_id: session.session_id,
            expires_at: session.expires_at,
            public_key: session.public_key.clone(),
            key_signature: session.key_signature.clone(),
        };
        return if server.cluster_secure_chat.install(player, session) {
            SessionOutcome::Completed
        } else {
            SessionOutcome::Failed
        };
    }
    if let StagedSessionInput::SecureJava { input: signed, .. } = &input.staged {
        let Some(validation) = server
            .cluster_secure_chat
            .validate(player.clone(), signed.clone())
        else {
            return SessionOutcome::Failed;
        };
        match validation.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                player.kick(
                    DisconnectReason::Kicked,
                    &TextComponent::text(
                        error
                            .client_kick_reason()
                            .unwrap_or_else(|| error.to_string()),
                    ),
                );
                return SessionOutcome::Rejected;
            }
            Err(_) => return SessionOutcome::Failed,
        }
        let original = PlayerChatEvent::new(
            player.clone(),
            signed.body.clone(),
            vec![],
            signed
                .signature
                .as_ref()
                .map(|signature| signature.to_vec()),
        );
        let mut event = original.clone();
        if tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.plugin_manager.fire(server, &mut event),
        )
        .await
        .is_err()
        {
            warn_timeout();
            event = original;
        }
        if event.cancelled {
            return SessionOutcome::Rejected;
        }
        let message =
            super::seasonal_events::modify_chat_message(&event.message, &server.advanced_config)
                .unwrap_or(event.message);
        let Some(permit) = server.cluster_secure_chat.reserve_publish() else {
            player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
            return SessionOutcome::Rejected;
        };
        if !super::cluster_chat_out::broadcast_public_chat_from_player(server, &player, &message) {
            player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
            return SessionOutcome::Rejected;
        }
        let decorated = TextComponent::chat_decorated(
            &server.advanced_config.chat.format,
            &player.gameprofile.name,
            &message,
        );
        let published = permit.publish(player, signed.clone(), decorated);
        return if published.await.is_ok() {
            SessionOutcome::Completed
        } else {
            SessionOutcome::Failed
        };
    }
    if let StagedSessionInput::Command { command, .. } = &input.staged {
        let original = PlayerCommandSendEvent::new(player.clone(), command.clone());
        let mut event = original.clone();
        if tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.plugin_manager.fire(server, &mut event),
        )
        .await
        .is_err()
        {
            warn_timeout();
            event = original;
        }
        if event.cancelled {
            return SessionOutcome::Rejected;
        }
        let (tasks, mut registered) = mpsc::channel(16);
        COMMAND_TASKS
            .scope(tasks, async {
                server
                    .command_dispatcher
                    .load()
                    .handle_command(&player.get_command_source(server), &event.command);
            })
            .await;
        while let Ok(completion) = registered.try_recv() {
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(30), completion).await,
                Ok(Ok(()))
            ) {
                return SessionOutcome::Failed;
            }
        }
        if server.advanced_config.commands.log_console {
            tracing::info!(
                "Player ({}): executed command /{}",
                player.gameprofile.name,
                event.command
            );
        }
        return SessionOutcome::Completed;
    }
    let body = match &input.staged {
        StagedSessionInput::Java { body, .. } | StagedSessionInput::Bedrock { body, .. } => {
            body.clone()
        }
        StagedSessionInput::Command { .. }
        | StagedSessionInput::SecureJava { .. }
        | StagedSessionInput::SecureAck { .. }
        | StagedSessionInput::SecureSession { .. } => unreachable!(),
    };
    let original = PlayerChatEvent::new(player.clone(), body, vec![], None);
    let mut event = original.clone();
    if tokio::time::timeout(
        std::time::Duration::from_secs(5),
        server.plugin_manager.fire(server, &mut event),
    )
    .await
    .is_err()
    {
        warn_timeout();
        event = original;
    }
    if event.cancelled {
        return SessionOutcome::Rejected;
    }
    let message =
        super::seasonal_events::modify_chat_message(&event.message, &server.advanced_config)
            .unwrap_or(event.message);
    if !super::cluster_chat_out::broadcast_public_chat_from_player(server, &player, &message) {
        player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
        return SessionOutcome::Rejected;
    }
    let decorated = TextComponent::chat_decorated(
        &server.advanced_config.chat.format,
        &player.gameprofile.name,
        &message,
    );
    let world = player.world();
    match &input.staged {
        StagedSessionInput::Java { .. } => {
            let outgoing = crate::net::chat::PlayerChatMessage::system(message.clone())
                .with_unsigned_content(decorated);
            world.broadcast_chat_message(
                &outgoing,
                Player::is_text_filtering_enabled,
                Some(&player),
                (RAW + 1).into(),
                &TextComponent::empty(),
                None,
            );
        }
        StagedSessionInput::Bedrock { xuid, filtered, .. } => {
            let je_packet = CSystemChatMessage::new(&decorated, false);
            let be_packet = SText::chat_with_xuid(
                message.clone(),
                player.gameprofile.name.clone(),
                xuid.clone(),
                filtered.clone(),
            );
            world.broadcast_editioned(&je_packet, &be_packet);
        }
        StagedSessionInput::Command { .. }
        | StagedSessionInput::SecureJava { .. }
        | StagedSessionInput::SecureAck { .. }
        | StagedSessionInput::SecureSession { .. } => unreachable!(),
    }
    tracing::info!("<chat> {}: {}", player.gameprofile.name, message);
    SessionOutcome::Completed
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<SessionInput>) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut sessions: std::collections::HashMap<uuid::Uuid, SessionRoute> =
            std::collections::HashMap::new();
        let mut sweep = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                input = rx.recv() => {
                    let Some(input) = input else { break };
                    let player = input.player.clone();
                    let uuid = player.gameprofile.id;
                    if !sessions.get(&uuid).is_some_and(|route| route.player.upgrade().is_some_and(|old| Arc::ptr_eq(&old, &player))) {
                        if sessions.len() >= 4096 && !sessions.contains_key(&uuid) {
                            sessions.retain(|_, route| route.player.upgrade().is_some());
                            if sessions.len() >= 4096 {
                                player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
                                let _ = input.completion.send(SessionOutcome::Rejected);
                                continue;
                            }
                        }
                        let (tx, mut player_rx) = mpsc::channel(64);
                        let worker_server = Arc::clone(&task_server);
                        task_server.spawn_task(async move {
                            while let Some(next) = player_rx.recv().await {
                                let outcome = process_input(&worker_server, &next).await;
                                let _ = next.completion.send(outcome);
                            }
                        });
                        sessions.insert(uuid, SessionRoute {
                            player: Arc::downgrade(&player),
                            budget: SpamBudget { count: 0, last: input.staged.received_at() },
                            tx,
                        });
                    }
                    let route = sessions.get_mut(&uuid).unwrap();
                    let anti_spam = &task_server.advanced_config.chat.anti_spam;
                    if anti_spam.enabled
                        && !matches!(input.staged, StagedSessionInput::SecureAck { .. } | StagedSessionInput::SecureSession { .. })
                        && !(anti_spam.ops_bypass && player.permission_lvl.load() > PermissionLvl::Zero)
                        && route.budget.charge(input.staged.received_at(), anti_spam.message_cost, anti_spam.decay_per_tick, spam_threshold(&input.staged, anti_spam))
                    {
                        player.kick(
                            DisconnectReason::Kicked,
                            &TextComponent::translate_cross(
                                pumpkin_data::translation::java::DISCONNECT_SPAM,
                                pumpkin_data::translation::bedrock::DISCONNECT_SPAM,
                                [],
                            ),
                        );
                        let _ = input.completion.send(SessionOutcome::Rejected);
                        continue;
                    }
                    if let Err(error) = route.tx.try_send(input) {
                        player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
                        let _ = error.into_inner().completion.send(SessionOutcome::Rejected);
                    }
                }
                _ = sweep.tick() => {
                    sessions.retain(|_, route| route.player.upgrade().is_some());
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_and_command_share_decaying_session_budget() {
        let start = Instant::now();
        let mut budget = SpamBudget {
            count: 0,
            last: start,
        };
        assert!(!budget.charge(start, 20, 1, 40));
        assert!(!budget.charge(start, 20, 1, 50));
        assert!(budget.charge(start, 20, 1, 40));
        assert!(budget.charge(start + std::time::Duration::from_secs(1), 20, 1, 50));
        assert!(!budget.charge(start + std::time::Duration::from_secs(3), 20, 1, 40));
    }
}
