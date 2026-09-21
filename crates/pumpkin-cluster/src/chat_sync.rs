use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::StreamKind;

pub const MAX_CHAT_BODY_LEN: usize = 256;

pub const MAX_CHAT_NAME_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSyncError {
    pub message: String,
}

impl core::fmt::Display for ChatSyncError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ChatSyncError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicChatBroadcast {
    pub sender: GlobalPlayerId,
    pub sender_name: String,
    pub body: String,
}

impl PublicChatBroadcast {
    #[must_use]
    pub fn new(sender: GlobalPlayerId, sender_name: String, body: String) -> Self {
        Self {
            sender,
            sender_name,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateChatRequest {
    pub from: GlobalPlayerId,
    pub from_name: String,
    pub to: GlobalPlayerId,
    pub to_name: String,
    pub body: String,
}

impl PrivateChatRequest {
    #[must_use]
    pub fn new(
        from: GlobalPlayerId,
        from_name: String,
        to: GlobalPlayerId,
        to_name: String,
        body: String,
    ) -> Self {
        Self {
            from,
            from_name,
            to,
            to_name,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamChatBroadcast {
    pub sender: GlobalPlayerId,
    pub sender_name: String,
    pub team: String,
    pub body: String,
}

impl TeamChatBroadcast {
    #[must_use]
    pub fn new(
        sender: GlobalPlayerId,
        sender_name: String,
        team: String,
        body: String,
    ) -> Self {
        Self {
            sender,
            sender_name,
            team,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmoteChatBroadcast {
    pub sender: GlobalPlayerId,
    pub sender_name: String,
    pub body: String,
}

impl EmoteChatBroadcast {
    #[must_use]
    pub fn new(sender: GlobalPlayerId, sender_name: String, body: String) -> Self {
        Self {
            sender,
            sender_name,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SayChatBroadcast {
    pub sender: GlobalPlayerId,
    pub sender_name: String,
    pub body: String,
}

impl SayChatBroadcast {
    #[must_use]
    pub fn new(sender: GlobalPlayerId, sender_name: String, body: String) -> Self {
        Self {
            sender,
            sender_name,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatControlMessage {
    Public(PublicChatBroadcast),
    Private(PrivateChatRequest),
    Team(TeamChatBroadcast),
    Emote(EmoteChatBroadcast),
    Say(SayChatBroadcast),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

impl ChatParcel {
    #[must_use]
    pub fn new(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> Self {
        Self { peer, kind, bytes }
    }
}

#[must_use]
pub const fn chat_control_kind() -> StreamKind {
    StreamKind::Control
}

#[must_use]
pub fn is_valid_chat_body(body: &str) -> bool {
    !body.is_empty() && body.len() <= MAX_CHAT_BODY_LEN
}

#[must_use]
pub fn is_valid_chat_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_CHAT_NAME_LEN
}

#[must_use]
pub fn canonical_chat_key(name: &str) -> String {
    name.to_lowercase()
}

pub fn encode_control_message(
    message: &ChatControlMessage,
) -> Result<Vec<u8>, ChatSyncError> {
    postcard::to_allocvec(message).map_err(|error| ChatSyncError {
        message: format!("encode chat control: {error}"),
    })
}

pub fn decode_control_message(bytes: &[u8]) -> Result<ChatControlMessage, ChatSyncError> {
    postcard::from_bytes(bytes).map_err(|error| ChatSyncError {
        message: format!("decode chat control: {error}"),
    })
}

pub fn control_parcels_for_peers(
    message: &ChatControlMessage,
    peers: &[u16],
) -> Result<Vec<ChatParcel>, ChatSyncError> {
    let bytes = encode_control_message(message)?;
    let mut parcels = Vec::with_capacity(peers.len());
    for peer in peers {
        parcels.push(ChatParcel::new(*peer, chat_control_kind(), bytes.clone()));
    }
    Ok(parcels)
}

pub fn public_parcels_for_peers(
    broadcast: &PublicChatBroadcast,
    peers: &[u16],
) -> Result<Vec<ChatParcel>, ChatSyncError> {
    control_parcels_for_peers(&ChatControlMessage::Public(broadcast.clone()), peers)
}

pub fn team_parcels_for_peers(
    broadcast: &TeamChatBroadcast,
    peers: &[u16],
) -> Result<Vec<ChatParcel>, ChatSyncError> {
    control_parcels_for_peers(&ChatControlMessage::Team(broadcast.clone()), peers)
}

pub fn emote_parcels_for_peers(
    broadcast: &EmoteChatBroadcast,
    peers: &[u16],
) -> Result<Vec<ChatParcel>, ChatSyncError> {
    control_parcels_for_peers(&ChatControlMessage::Emote(broadcast.clone()), peers)
}

pub fn say_parcels_for_peers(
    broadcast: &SayChatBroadcast,
    peers: &[u16],
) -> Result<Vec<ChatParcel>, ChatSyncError> {
    control_parcels_for_peers(&ChatControlMessage::Say(broadcast.clone()), peers)
}

pub fn private_parcel_for_host(
    request: &PrivateChatRequest,
) -> Result<ChatParcel, ChatSyncError> {
    let bytes = encode_control_message(&ChatControlMessage::Private(request.clone()))?;
    Ok(ChatParcel::new(
        request.to.server.0,
        chat_control_kind(),
        bytes,
    ))
}

pub async fn broadcast_control_message(
    tx: &mpsc::Sender<ChatParcel>,
    message: &ChatControlMessage,
    peers: &[u16],
) -> Result<usize, ChatSyncError> {
    let bytes = encode_control_message(message)?;
    let mut sent = 0_usize;
    for peer in peers {
        let parcel = ChatParcel::new(*peer, chat_control_kind(), bytes.clone());
        if tx.send(parcel).await.is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
}

pub fn try_broadcast_control_message(
    tx: &mpsc::Sender<ChatParcel>,
    message: &ChatControlMessage,
    peers: &[u16],
) -> Result<usize, ChatSyncError> {
    let bytes = encode_control_message(message)?;
    let mut sent = 0_usize;
    for peer in peers {
        let parcel = ChatParcel::new(*peer, chat_control_kind(), bytes.clone());
        if tx.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
}

#[derive(Debug, Clone)]
pub struct ChatDirectorySnapshot {
    pub local: ServerId,
    by_name: HashMap<String, GlobalPlayerId>,
    names: HashMap<GlobalPlayerId, String>,
}

impl ChatDirectorySnapshot {
    #[must_use]
    pub fn new(local: ServerId) -> Self {
        Self {
            local,
            by_name: HashMap::new(),
            names: HashMap::new(),
        }
    }

    pub fn insert(&mut self, gid: GlobalPlayerId, name: String) {
        if let Some(previous) = self.names.insert(gid, name.clone()) {
            self.by_name.remove(&canonical_chat_key(&previous));
        }
        self.by_name.insert(canonical_chat_key(&name), gid);
    }

    pub fn remove_gid(&mut self, gid: &GlobalPlayerId) -> bool {
        match self.names.remove(gid) {
            Some(name) => {
                self.by_name.remove(&canonical_chat_key(&name));
                true
            }
            None => false,
        }
    }

    #[must_use]
    pub fn locate_by_name(&self, name: &str) -> Option<GlobalPlayerId> {
        self.by_name.get(&canonical_chat_key(name)).copied()
    }

    #[must_use]
    pub fn player_name(&self, gid: &GlobalPlayerId) -> Option<&String> {
        self.names.get(gid)
    }

    #[must_use]
    pub fn host_of(&self, gid: &GlobalPlayerId) -> ServerId {
        gid.server
    }

    #[must_use]
    pub fn is_local(&self, gid: &GlobalPlayerId) -> bool {
        gid.server == self.local
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    #[must_use]
    pub fn all_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.names.values().cloned().collect();
        names.sort();
        names.dedup();
        names
    }

    #[must_use]
    pub fn completion_names(&self, prefix: &str) -> Vec<String> {
        let folded = canonical_chat_key(prefix);
        let mut matches: Vec<String> = self
            .names
            .values()
            .filter(|name| canonical_chat_key(name).starts_with(folded.as_str()))
            .cloned()
            .collect();
        matches.sort();
        matches.dedup();
        matches
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateDelivery {
    pub target: GlobalPlayerId,
    pub local: bool,
    pub request: PrivateChatRequest,
}

#[must_use]
pub fn resolve_private_by_name(
    directory: &ChatDirectorySnapshot,
    from: GlobalPlayerId,
    from_name: String,
    target_name: &str,
    body: String,
) -> Option<PrivateDelivery> {
    let target = directory.locate_by_name(target_name)?;
    if target == from {
        return None;
    }
    let canonical = directory
        .player_name(&target)
        .cloned()
        .unwrap_or_else(|| target_name.to_string());
    Some(PrivateDelivery {
        target,
        local: directory.is_local(&target),
        request: PrivateChatRequest::new(from, from_name, target, canonical, body),
    })
}

#[must_use]
pub fn resolve_private_by_id(
    directory: &ChatDirectorySnapshot,
    from: GlobalPlayerId,
    from_name: String,
    target: GlobalPlayerId,
    body: String,
) -> Option<PrivateDelivery> {
    if target == from {
        return None;
    }
    let canonical = directory.player_name(&target)?.clone();
    Some(PrivateDelivery {
        target,
        local: directory.is_local(&target),
        request: PrivateChatRequest::new(from, from_name, target, canonical, body),
    })
}

#[must_use]
pub fn should_deliver_private_locally(request: &PrivateChatRequest, local: ServerId) -> bool {
    request.to.server == local
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatInboundEffect {
    PublicDelivery(PublicChatBroadcast),
    PrivateDelivery(PrivateChatRequest),
    TeamDelivery(TeamChatBroadcast),
    EmoteDelivery(EmoteChatBroadcast),
    SayDelivery(SayChatBroadcast),
    Ignored,
}

pub fn handle_chat_bytes(
    local: ServerId,
    bytes: &[u8],
) -> Result<ChatInboundEffect, ChatSyncError> {
    let message = decode_control_message(bytes)?;
    Ok(match message {
        ChatControlMessage::Public(broadcast) => {
            ChatInboundEffect::PublicDelivery(broadcast)
        }
        ChatControlMessage::Private(request) => {
            if should_deliver_private_locally(&request, local) {
                ChatInboundEffect::PrivateDelivery(request)
            } else {
                ChatInboundEffect::Ignored
            }
        }
        ChatControlMessage::Team(broadcast) => ChatInboundEffect::TeamDelivery(broadcast),
        ChatControlMessage::Emote(broadcast) => ChatInboundEffect::EmoteDelivery(broadcast),
        ChatControlMessage::Say(broadcast) => ChatInboundEffect::SayDelivery(broadcast),
    })
}

#[derive(Debug)]
pub struct ChatCompletionQuery {
    pub prefix: String,
    pub reply: oneshot::Sender<Vec<String>>,
}

impl ChatCompletionQuery {
    #[must_use]
    pub fn new(prefix: String, reply: oneshot::Sender<Vec<String>>) -> Self {
        Self { prefix, reply }
    }
}

pub fn answer_completion_query(directory: &ChatDirectorySnapshot, query: ChatCompletionQuery) {
    let ChatCompletionQuery { prefix, reply } = query;
    let _ = reply.send(directory.completion_names(prefix.as_str()));
}

pub async fn query_completion_names(
    tx: &mpsc::Sender<ChatCompletionQuery>,
    prefix: String,
) -> Option<Vec<String>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let query = ChatCompletionQuery::new(prefix, reply_tx);
    if tx.send(query).await.is_err() {
        return None;
    }
    match reply_rx.await {
        Ok(names) => Some(names),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn directory() -> ChatDirectorySnapshot {
        let mut snapshot = ChatDirectorySnapshot::new(ServerId(1));
        snapshot.insert(gid(1, 1), String::from("Alice"));
        snapshot.insert(gid(2, 7), String::from("Bob"));
        snapshot.insert(gid(3, 2), String::from("Bobby"));
        snapshot
    }

    fn public() -> PublicChatBroadcast {
        PublicChatBroadcast::new(gid(1, 1), String::from("Alice"), String::from("hello all"))
    }

    fn private() -> PrivateChatRequest {
        PrivateChatRequest::new(
            gid(1, 1),
            String::from("Alice"),
            gid(2, 7),
            String::from("Bob"),
            String::from("hello bob"),
        )
    }

    fn team() -> TeamChatBroadcast {
        TeamChatBroadcast::new(
            gid(1, 1),
            String::from("Alice"),
            String::from("red"),
            String::from("go red"),
        )
    }

    fn say() -> SayChatBroadcast {
        SayChatBroadcast::new(
            gid(1, 1),
            String::from("Alice"),
            String::from("hello from say"),
        )
    }

    fn emote() -> EmoteChatBroadcast {
        EmoteChatBroadcast::new(
            gid(1, 1),
            String::from("Alice"),
            String::from("waves"),
        )
    }

    #[test]
    fn control_travels_on_control_kind() {
        assert_eq!(chat_control_kind(), StreamKind::Control);
    }

    #[test]
    fn body_and_name_validation() {
        assert!(is_valid_chat_body("hi"));
        assert!(!is_valid_chat_body(""));
        assert!(!is_valid_chat_body(str::repeat("x", MAX_CHAT_BODY_LEN + 1).as_str()));
        assert!(is_valid_chat_name("Alice"));
        assert!(!is_valid_chat_name(""));
        assert!(!is_valid_chat_name(str::repeat("y", MAX_CHAT_NAME_LEN + 1).as_str()));
    }

    #[test]
    fn control_message_roundtrips() {
        for message in [
            ChatControlMessage::Public(public()),
            ChatControlMessage::Private(private()),
            ChatControlMessage::Team(team()),
            ChatControlMessage::Emote(emote()),
            ChatControlMessage::Say(say()),
        ] {
            let bytes = encode_control_message(&message).unwrap();
            assert_eq!(decode_control_message(&bytes).unwrap(), message);
        }
    }

    #[test]
    fn control_rejects_garbage() {
        assert!(decode_control_message(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn public_and_team_fan_out_to_every_peer() {
        let public_parcels = public_parcels_for_peers(&public(), &[2, 3]).unwrap();
        assert_eq!(public_parcels.len(), 2);
        assert!(
            public_parcels
                .iter()
                .all(|parcel| parcel.kind == StreamKind::Control)
        );
        assert_eq!(public_parcels[0].peer, 2);
        assert_eq!(public_parcels[1].peer, 3);
        let team_parcels = team_parcels_for_peers(&team(), &[2, 3]).unwrap();
        assert_eq!(team_parcels.len(), 2);
        assert!(
            team_parcels
                .iter()
                .all(|parcel| parcel.kind == StreamKind::Control)
        );
    }

    #[test]
    fn private_routes_by_global_id() {
        let snapshot = directory();
        let local =
            resolve_private_by_name(&snapshot, gid(2, 7), String::from("Bob"), "alice", String::from("hi"))
                .unwrap();
        assert!(local.local);
        assert_eq!(local.target, gid(1, 1));
        let remote =
            resolve_private_by_name(&snapshot, gid(1, 1), String::from("Alice"), "BOB", String::from("hi"))
                .unwrap();
        assert!(!remote.local);
        assert_eq!(remote.target, gid(2, 7));
        assert_eq!(remote.request.to_name, String::from("Bob"));
        assert!(
            resolve_private_by_name(&snapshot, gid(1, 1), String::from("Alice"), "Nobody", String::from("hi"))
                .is_none()
        );
        assert!(
            resolve_private_by_name(&snapshot, gid(1, 1), String::from("Alice"), "alice", String::from("hi"))
                .is_none()
        );
        let by_id =
            resolve_private_by_id(&snapshot, gid(1, 1), String::from("Alice"), gid(2, 7), String::from("hi"))
                .unwrap();
        assert_eq!(by_id.request, remote.request);
        assert!(
            resolve_private_by_id(&snapshot, gid(1, 1), String::from("Alice"), gid(9, 9), String::from("hi"))
                .is_none()
        );
        let parcel = private_parcel_for_host(&remote.request).unwrap();
        assert_eq!(parcel.peer, 2);
        assert!(should_deliver_private_locally(&remote.request, ServerId(2)));
        assert!(!should_deliver_private_locally(&remote.request, ServerId(1)));
    }

    #[test]
    fn directory_tracks_global_names() {
        let mut snapshot = directory();
        assert_eq!(snapshot.len(), 3);
        assert!(!snapshot.is_empty());
        assert_eq!(snapshot.locate_by_name("ALICE"), Some(gid(1, 1)));
        assert_eq!(snapshot.host_of(&gid(2, 7)), ServerId(2));
        assert!(snapshot.is_local(&gid(1, 1)));
        assert!(!snapshot.is_local(&gid(2, 7)));
        assert!(snapshot.remove_gid(&gid(3, 2)));
        assert!(!snapshot.remove_gid(&gid(9, 9)));
        assert_eq!(snapshot.locate_by_name("bobby"), None);
    }

    #[test]
    fn completion_covers_every_secondary() {
        let snapshot = directory();
        assert_eq!(
            snapshot.completion_names("b"),
            vec![String::from("Bob"), String::from("Bobby")]
        );
        assert_eq!(snapshot.completion_names("BO"), vec![String::from("Bob"), String::from("Bobby")]);
        assert_eq!(snapshot.completion_names("ali"), vec![String::from("Alice")]);
        assert!(snapshot.completion_names("zzz").is_empty());
        assert_eq!(snapshot.all_names().len(), 3);
    }

    #[test]
    fn inbound_dispatch_delivers_or_ignores() {
        let bytes = encode_control_message(&ChatControlMessage::Public(public())).unwrap();
        let effect = handle_chat_bytes(ServerId(2), &bytes).unwrap();
        assert!(matches!(effect, ChatInboundEffect::PublicDelivery(_)));
        let bytes = encode_control_message(&ChatControlMessage::Team(team())).unwrap();
        let effect = handle_chat_bytes(ServerId(2), &bytes).unwrap();
        assert!(matches!(effect, ChatInboundEffect::TeamDelivery(_)));
        let bytes = encode_control_message(&ChatControlMessage::Private(private())).unwrap();
        let effect = handle_chat_bytes(ServerId(2), &bytes).unwrap();
        assert!(matches!(effect, ChatInboundEffect::PrivateDelivery(_)));
        let effect = handle_chat_bytes(ServerId(1), &bytes).unwrap();
        assert_eq!(effect, ChatInboundEffect::Ignored);
        let bytes = encode_control_message(&ChatControlMessage::Emote(emote())).unwrap();
        let effect = handle_chat_bytes(ServerId(2), &bytes).unwrap();
        assert!(matches!(effect, ChatInboundEffect::EmoteDelivery(_)));
        let bytes = encode_control_message(&ChatControlMessage::Say(say())).unwrap();
        let effect = handle_chat_bytes(ServerId(2), &bytes).unwrap();
        assert!(matches!(effect, ChatInboundEffect::SayDelivery(_)));
    }

    #[tokio::test]
    async fn broadcast_sends_to_all_peers() {
        let (tx, mut rx) = mpsc::channel(8);
        let count = broadcast_control_message(&tx, &ChatControlMessage::Public(public()), &[2, 3])
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
    }

    #[test]
    fn try_broadcast_skips_full_queue() {
        let (tx, _rx) = mpsc::channel(1);
        let count =
            try_broadcast_control_message(&tx, &ChatControlMessage::Team(team()), &[2, 3]).unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn completion_query_roundtrips_without_locks() {
        let snapshot = directory();
        let (tx, mut rx) = mpsc::channel(4);
        let task = tokio::spawn(async move {
            let Some(query) = rx.recv().await else {
                return;
            };
            answer_completion_query(&snapshot, query);
        });
        let names = query_completion_names(&tx, String::from("b")).await.unwrap();
        assert_eq!(names, vec![String::from("Bob"), String::from("Bobby")]);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn completion_query_missing_receiver_returns_none() {
        let (tx, rx) = mpsc::channel::<ChatCompletionQuery>(1);
        drop(rx);
        let names = query_completion_names(&tx, String::from("b")).await;
        assert!(names.is_none());
    }
}
