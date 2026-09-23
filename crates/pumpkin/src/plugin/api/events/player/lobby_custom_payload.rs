use std::sync::Arc;

use bytes::Bytes;
use pumpkin_macros::Event;

use crate::server::cluster_lobby::LobbyCommandSender;

#[derive(Event, Clone)]
pub struct LobbyCustomPayloadEvent {
    pub source: Arc<LobbyCommandSender>,
    pub channel: String,
    pub data: Bytes,
}

#[derive(Event, Clone)]
pub struct LobbyResourcePackStatusEvent {
    pub source: Arc<LobbyCommandSender>,
    pub pack_id: uuid::Uuid,
    pub status: String,
}

#[derive(Event, Clone)]
pub struct LobbyDialogActionEvent {
    pub source: Arc<LobbyCommandSender>,
    pub action_id: String,
    pub payload: Option<Bytes>,
}
