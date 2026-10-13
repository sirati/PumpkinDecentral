use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use pumpkin_data::world::RAW;
use pumpkin_protocol::java::client::play::{
    CPlayerInfoUpdate, InitChat, PlayerAction, PlayerInfoFlags,
};
use pumpkin_protocol::java::server::play::SPlayerSession;
use pumpkin_util::math::polynomial_rolling_hash;
use pumpkin_util::text::TextComponent;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::entity::player::{ChatSession, MessageCache, Player};
use crate::net::chat::{
    FilterMask, OutgoingChatMessage, PlayerChatMessage, SignedMessageBody, SignedMessageLink,
};
use crate::net::java::play::ChatError;

use super::Server;

#[derive(Clone)]
pub struct ChatSessionView {
    pub player_ptr: usize,
    pub session_id: Uuid,
    pub expires_at: i64,
    pub public_key: Box<[u8]>,
    pub signature: Box<[u8]>,
}

impl ChatSessionView {
    pub fn init_chat(&self) -> Option<InitChat> {
        (self.session_id != Uuid::nil()
            && !self.public_key.is_empty()
            && !self.signature.is_empty())
        .then(|| InitChat {
            session_id: self.session_id,
            expires_at: self.expires_at,
            public_key: self.public_key.clone(),
            signature: self.signature.clone(),
        })
    }
}

#[derive(Clone)]
pub struct SignedInput {
    pub body: String,
    pub timestamp: i64,
    pub salt: i64,
    pub signature: Option<Box<[u8]>>,
    pub message_count: i32,
    pub acknowledged: Box<[u8]>,
    pub checksum: u8,
}

pub(super) enum SecureChatInput {
    Promote { player: Arc<Player> },
    Remove {
        uuid: Uuid,
        player_ptr: usize,
    },
    Install {
        player: Arc<Player>,
        session: SPlayerSession,
    },
    LobbyInstall {
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        session: SPlayerSession,
    },
    LobbyValidate {
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        input: SignedInput,
        result: oneshot::Sender<Result<(), ChatError>>,
    },
    LobbyAck {
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        offset: i32,
        result: oneshot::Sender<Result<(), ChatError>>,
    },
    Ack {
        player: Arc<Player>,
        offset: i32,
        result: oneshot::Sender<Result<(), ChatError>>,
    },
    Validate {
        player: Arc<Player>,
        input: SignedInput,
        result: oneshot::Sender<Result<(), ChatError>>,
    },
    Publish {
        player: Arc<Player>,
        input: SignedInput,
        decorated: TextComponent,
        result: oneshot::Sender<()>,
    },
}

struct OwnedState {
    player: Weak<Player>,
    client: Weak<crate::net::ClientPlatform>,
    session: ChatSession,
    cache: MessageCache,
}

impl OwnedState {
    fn new(player: &Arc<Player>) -> Self {
        Self {
            player: Arc::downgrade(player),
            client: Arc::downgrade(&player.client),
            session: ChatSession::default(),
            cache: MessageCache::default(),
        }
    }
}

fn state_for<'a>(
    states: &'a mut HashMap<Uuid, OwnedState>,
    player: &Arc<Player>,
) -> &'a mut OwnedState {
    let uuid = player.gameprofile.id;
    if !states.get(&uuid).is_some_and(|old| {
        old.client
            .upgrade()
            .is_some_and(|current| Arc::ptr_eq(&current, &player.client))
    }) {
        states.insert(uuid, OwnedState::new(player));
    }
    let state = states.get_mut(&uuid).unwrap();
    state.player = Arc::downgrade(player);
    state
}

fn lobby_state_for<'a>(
    states: &'a mut HashMap<Uuid, OwnedState>,
    source: &super::cluster_lobby::LobbyCommandSender,
) -> &'a mut OwnedState {
    let uuid = source.profile.id;
    if !states.get(&uuid).is_some_and(|state| {
        state
            .client
            .upgrade()
            .is_some_and(|client| Arc::ptr_eq(&client, &source.client))
    }) {
        states.insert(
            uuid,
            OwnedState {
                player: Weak::new(),
                client: Arc::downgrade(&source.client),
                session: ChatSession::default(),
                cache: MessageCache::default(),
            },
        );
    }
    states.get_mut(&uuid).unwrap()
}

fn validate(state: &mut OwnedState, input: &SignedInput) -> Result<(), ChatError> {
    if input.body.encode_utf16().nth(256).is_some() {
        return Err(ChatError::OversizedMessage);
    }
    if input
        .body
        .chars()
        .any(|c| c == '§' || c < ' ' || c == '\x7f')
    {
        return Err(ChatError::IllegalCharacters);
    }
    if !input
        .signature
        .as_ref()
        .is_some_and(|signature| signature.len() == 256)
    {
        return Err(ChatError::UnsignedChat);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    if input.timestamp > now || input.timestamp < now - 120_000 {
        return Err(ChatError::OutOfOrderChat);
    }
    if state.session.expires_at < now {
        return Err(ChatError::ExpiredPublicKey);
    }
    let offset =
        usize::try_from(input.message_count).map_err(|_| ChatError::ChatValidationFailed)?;
    if input.acknowledged.is_empty() {
        state
            .cache
            .last_seen_validator
            .apply_offset(offset)
            .map_err(|_| ChatError::ChatValidationFailed)?;
    } else {
        state
            .cache
            .last_seen_validator
            .apply_update(offset, &input.acknowledged)
            .map_err(|_| ChatError::ChatValidationFailed)?;
    }
    if state.cache.last_seen_validator.tracked_messages_count() > 4096 {
        return Err(ChatError::TooManyPendingChats);
    }
    if input.checksum != 0
        && polynomial_rolling_hash(state.cache.last_seen.as_ref()) != input.checksum
    {
        return Err(ChatError::ChatValidationFailed);
    }
    Ok(())
}

pub struct SecureChatRuntime {
    tx: mpsc::Sender<SecureChatInput>,
    pub views: Arc<ArcSwap<HashMap<Uuid, ChatSessionView>>>,
}

pub struct SecurePublishPermit(mpsc::OwnedPermit<SecureChatInput>);

impl SecurePublishPermit {
    pub fn publish(
        self,
        player: Arc<Player>,
        input: SignedInput,
        decorated: TextComponent,
    ) -> oneshot::Receiver<()> {
        let (result, receiver) = oneshot::channel();
        self.0.send(SecureChatInput::Publish {
            player,
            input,
            decorated,
            result,
        });
        receiver
    }
}

impl SecureChatRuntime {
    pub fn new() -> (Self, mpsc::Receiver<SecureChatInput>) {
        let (tx, rx) = mpsc::channel(1024);
        (
            Self {
                tx,
                views: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            },
            rx,
        )
    }

    pub(super) fn reserve_promote(&self) -> Option<mpsc::OwnedPermit<SecureChatInput>> {
        self.tx.clone().try_reserve_owned().ok()
    }

    pub fn install(&self, player: Arc<Player>, session: SPlayerSession) -> bool {
        self.tx
            .try_send(SecureChatInput::Install { player, session })
            .is_ok()
    }

    pub fn install_lobby(
        &self,
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        session: SPlayerSession,
    ) -> bool {
        self.tx
            .try_send(SecureChatInput::LobbyInstall { source, session })
            .is_ok()
    }

    pub fn validate_lobby(
        &self,
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        input: SignedInput,
    ) -> Option<oneshot::Receiver<Result<(), ChatError>>> {
        let (result, receiver) = oneshot::channel();
        self.tx
            .try_send(SecureChatInput::LobbyValidate {
                source,
                input,
                result,
            })
            .ok()?;
        Some(receiver)
    }

    pub fn ack_lobby(
        &self,
        source: Arc<super::cluster_lobby::LobbyCommandSender>,
        offset: i32,
    ) -> Option<oneshot::Receiver<Result<(), ChatError>>> {
        let (result, receiver) = oneshot::channel();
        self.tx
            .try_send(SecureChatInput::LobbyAck {
                source,
                offset,
                result,
            })
            .ok()?;
        Some(receiver)
    }

    pub fn remove(&self, player: &Player) -> bool {
        self.tx
            .try_send(SecureChatInput::Remove {
                uuid: player.gameprofile.id,
                player_ptr: player as *const Player as usize,
            })
            .is_ok()
    }

    pub fn ack(
        &self,
        player: Arc<Player>,
        offset: i32,
    ) -> Option<oneshot::Receiver<Result<(), ChatError>>> {
        let (result, receiver) = oneshot::channel();
        self.tx
            .try_send(SecureChatInput::Ack {
                player,
                offset,
                result,
            })
            .ok()?;
        Some(receiver)
    }

    pub fn validate(
        &self,
        player: Arc<Player>,
        input: SignedInput,
    ) -> Option<oneshot::Receiver<Result<(), ChatError>>> {
        let (result, receiver) = oneshot::channel();
        self.tx
            .try_send(SecureChatInput::Validate {
                player,
                input,
                result,
            })
            .ok()?;
        Some(receiver)
    }

    pub fn reserve_publish(&self) -> Option<SecurePublishPermit> {
        self.tx
            .clone()
            .try_reserve_owned()
            .ok()
            .map(SecurePublishPermit)
    }
}

pub fn spawn(server: &Arc<Server>, mut rx: mpsc::Receiver<SecureChatInput>) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut states = HashMap::<Uuid, OwnedState>::new();
        let mut views = HashMap::<Uuid, ChatSessionView>::new();
        let mut sweep = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            let input = tokio::select! {
                input = rx.recv() => {
                    let Some(input) = input else { break };
                    input
                }
                _ = sweep.tick() => {
                    states.retain(|_, state| state.client.upgrade().is_some());
                    views.retain(|uuid, view| states.get(uuid).is_some_and(|state| {
                        state.player.as_ptr() as usize == view.player_ptr
                    }));
                    task_server.cluster_secure_chat.views.store(Arc::new(views.clone()));
                    continue;
                }
            };
            match input {
                SecureChatInput::LobbyInstall { source, session } => {
                    let state = lobby_state_for(&mut states, &source);
                    if state.session.session_id != session.session_id {
                        state.session = ChatSession::new(
                            session.session_id,
                            session.expires_at,
                            session.public_key,
                            session.key_signature,
                        );
                    }
                }
                SecureChatInput::LobbyValidate {
                    source,
                    input,
                    result,
                } => {
                    let state = lobby_state_for(&mut states, &source);
                    let outcome = validate(state, &input);
                    if outcome.is_ok() {
                        state.session.messages_sent += 1;
                    }
                    let _ = result.send(outcome);
                }
                SecureChatInput::LobbyAck {
                    source,
                    offset,
                    result,
                } => {
                    let outcome = usize::try_from(offset)
                        .map_err(|_| ChatError::ChatValidationFailed)
                        .and_then(|offset| {
                            lobby_state_for(&mut states, &source)
                                .cache
                                .last_seen_validator
                                .apply_offset(offset)
                                .map_err(|_| ChatError::ChatValidationFailed)
                        });
                    let _ = result.send(outcome);
                }
                SecureChatInput::Promote { player } => {
                    let state = state_for(&mut states, &player);
                    views.insert(player.gameprofile.id, ChatSessionView {
                        player_ptr: Arc::as_ptr(&player) as usize,
                        session_id: state.session.session_id,
                        expires_at: state.session.expires_at,
                        public_key: state.session.public_key.clone(),
                        signature: state.session.signature.clone(),
                    });
                    task_server.cluster_secure_chat.views.store(Arc::new(views.clone()));
                    if !super::cluster_hide::is_hidden_player(&task_server, &player) {
                        let actions = [PlayerAction::InitializeChat(views.get(&player.gameprofile.id).and_then(ChatSessionView::init_chat))];
                        task_server.broadcast_packet_all(&CPlayerInfoUpdate::new(
                            PlayerInfoFlags::INITIALIZE_CHAT.bits(),
                            &[pumpkin_protocol::java::client::play::Player { uuid: player.gameprofile.id, actions: &actions }],
                        ));
                    }
                }
                SecureChatInput::Remove { uuid, player_ptr } => {
                    if states
                        .get(&uuid)
                        .is_some_and(|state| state.player.as_ptr() as usize == player_ptr)
                    {
                        states.remove(&uuid);
                        views.remove(&uuid);
                        task_server
                            .cluster_secure_chat
                            .views
                            .store(Arc::new(views.clone()));
                    }
                }
                SecureChatInput::Install { player, session } => {
                    let state = state_for(&mut states, &player);
                    if state.session.session_id != session.session_id {
                        state.session = ChatSession::new(
                            session.session_id,
                            session.expires_at,
                            session.public_key.clone(),
                            session.key_signature.clone(),
                        );
                    }
                    views.insert(
                        player.gameprofile.id,
                        ChatSessionView {
                            player_ptr: Arc::as_ptr(&player) as usize,
                            session_id: session.session_id,
                            expires_at: session.expires_at,
                            public_key: session.public_key.clone(),
                            signature: session.key_signature.clone(),
                        },
                    );
                    task_server
                        .cluster_secure_chat
                        .views
                        .store(Arc::new(views.clone()));
                    let actions = [PlayerAction::InitializeChat(
                        views
                            .get(&player.gameprofile.id)
                            .and_then(ChatSessionView::init_chat),
                    )];
                    if !super::cluster_hide::is_hidden_player(&task_server, &player) {
                        task_server.broadcast_packet_all(&CPlayerInfoUpdate::new(
                            PlayerInfoFlags::INITIALIZE_CHAT.bits(),
                            &[pumpkin_protocol::java::client::play::Player {
                                uuid: player.gameprofile.id,
                                actions: &actions,
                            }],
                        ));
                    }
                }
                SecureChatInput::Ack {
                    player,
                    offset,
                    result,
                } => {
                    let outcome = usize::try_from(offset)
                        .map_err(|_| ChatError::ChatValidationFailed)
                        .and_then(|offset| {
                            state_for(&mut states, &player)
                                .cache
                                .last_seen_validator
                                .apply_offset(offset)
                                .map_err(|_| ChatError::ChatValidationFailed)
                        });
                    let _ = result.send(outcome);
                }
                SecureChatInput::Validate {
                    player,
                    input,
                    result,
                } => {
                    let _ = result.send(validate(state_for(&mut states, &player), &input));
                }
                SecureChatInput::Publish {
                    player,
                    input,
                    decorated,
                    result,
                } => {
                    let sender = state_for(&mut states, &player);
                    let message = PlayerChatMessage::new(
                        SignedMessageLink::new(
                            sender.session.messages_sent,
                            player.gameprofile.id,
                            sender.session.session_id,
                        ),
                        input.signature.clone(),
                        SignedMessageBody::new(
                            input.body,
                            input.timestamp,
                            input.salt,
                            sender.cache.last_seen.as_ref().to_vec(),
                        ),
                        Some(decorated),
                        FilterMask::PassThrough,
                    );
                    sender.session.messages_sent += 1;
                    for recipient in player.world().players.load().iter() {
                        let state = state_for(&mut states, recipient);
                        OutgoingChatMessage::send_signed_to_player_owned(
                            &message,
                            recipient,
                            recipient.is_text_filtering_enabled(),
                            (RAW + 1).into(),
                            &TextComponent::empty(),
                            None,
                            &mut state.session,
                            &mut state.cache,
                        );
                    }
                    let _ = result.send(());
                }
            }
        }
    });
}
