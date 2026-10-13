use pumpkin_protocol::java::server::play::{
    SChatAck, SChatCommand, SChatCommandSigned, SChatMessage, SClientInformationPlay,
    SCommandSuggestion, SCustomClickAction, SCustomPayload, SPlayPong, SPlayResourcePack,
    SPlayerSession,
};
use pumpkin_protocol::packet::MultiVersionJavaPacket;
use pumpkin_protocol::{RawPacket, ServerPacket};
use pumpkin_util::version::JavaMinecraftVersion;

use crate::error::PumpkinError;
use crate::net::PlayerConfig;
use crate::server::Server;

pub(crate) fn bounded_view_distance(
    requested: std::num::NonZeroU8,
    server: &Server,
) -> std::num::NonZeroU8 {
    std::num::NonZeroU8::new(
        requested
            .get()
            .max(2)
            .min(
                server
                    .advanced_config
                    .networking
                    .java
                    .view_distance
                    .get()
                    .max(2),
            )
            .min(pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE),
    )
    .expect("bounded session view distance is positive")
}

pub(crate) fn player_config(
    packet: &SClientInformationPlay<'_>,
    server: &Server,
) -> Result<PlayerConfig, &'static str> {
    let main_hand =
        pumpkin_util::Hand::try_from(packet.main_hand.0).map_err(|_| "Invalid main hand")?;
    let chat_mode = crate::entity::player::ChatMode::try_from(packet.chat_mode.0)
        .map_err(|_| "Invalid chat mode")?;
    let view_distance = u8::try_from(packet.view_distance)
        .ok()
        .and_then(std::num::NonZeroU8::new)
        .ok_or("Invalid view distance")?;
    Ok(PlayerConfig {
        locale: packet.locale.to_string(),
        view_distance: bounded_view_distance(view_distance, server),
        chat_mode,
        chat_colors: packet.chat_colors,
        skin_parts: packet.skin_parts,
        main_hand,
        text_filtering: packet.text_filtering,
        server_listing: packet.server_listing,
    })
}

pub(crate) enum PlaySessionPacket<'a> {
    ChatAck(SChatAck),
    Command(SChatCommand<'a>),
    SignedCommand(&'a str),
    Chat(SChatMessage<'a>),
    Settings(SClientInformationPlay<'a>),
    Suggestions(SCommandSuggestion<'a>),
    Session(SPlayerSession),
    Payload(SCustomPayload<'a>),
    ResourcePack(SPlayResourcePack),
    Pong(SPlayPong),
    Dialog(SCustomClickAction<'a>),
}

pub(crate) fn decode(
    packet: &RawPacket,
    version: JavaMinecraftVersion,
) -> Result<Option<PlaySessionPacket<'_>>, Box<dyn PumpkinError>> {
    let mut payload = &packet.payload[..];
    let parsed = match packet.id {
        id if id == SChatAck::to_id(version) => {
            PlaySessionPacket::ChatAck(SChatAck::read(&mut payload, &version)?)
        }
        id if id == SChatCommand::to_id(version) => {
            PlaySessionPacket::Command(SChatCommand::read(&mut payload, &version)?)
        }
        id if id == SChatCommandSigned::to_id(version) => {
            let mut signed_payload = payload;
            let command = match SChatCommandSigned::read(&mut signed_payload, &version) {
                Ok(packet) => packet.command,
                Err(_) => SChatCommand::read(&mut payload, &version)?.command,
            };
            PlaySessionPacket::SignedCommand(command)
        }
        id if id == SChatMessage::to_id(version) => {
            PlaySessionPacket::Chat(SChatMessage::read(&mut payload, &version)?)
        }
        id if id == SClientInformationPlay::to_id(version) => {
            PlaySessionPacket::Settings(SClientInformationPlay::read(&mut payload, &version)?)
        }
        id if id == SCommandSuggestion::to_id(version) => {
            PlaySessionPacket::Suggestions(SCommandSuggestion::read(&mut payload, &version)?)
        }
        id if id == SPlayerSession::to_id(version) => {
            PlaySessionPacket::Session(SPlayerSession::read(&mut payload, &version)?)
        }
        id if id == SCustomPayload::to_id(version) => {
            PlaySessionPacket::Payload(SCustomPayload::read(&mut payload, &version)?)
        }
        id if id == SPlayResourcePack::to_id(version) => {
            PlaySessionPacket::ResourcePack(SPlayResourcePack::read(&mut payload, &version)?)
        }
        id if id == SPlayPong::to_id(version) => {
            PlaySessionPacket::Pong(SPlayPong::read(&mut payload, &version)?)
        }
        id if id == SCustomClickAction::to_id(version) => {
            PlaySessionPacket::Dialog(SCustomClickAction::read(&mut payload, &version)?)
        }
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}
