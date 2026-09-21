use serde::{Deserialize, Serialize};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::StreamKind;

const LOBBY_CONTROL_TAG: &[u8] = b"PMLC\x01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LobbyControlError {
    pub message: String,
}

impl core::fmt::Display for LobbyControlError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LobbyControlError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LobbyControl {
    Hold { target: GlobalPlayerId },
    Release { target: GlobalPlayerId },
}

impl LobbyControl {
    #[must_use]
    pub const fn target(self) -> GlobalPlayerId {
        match self {
            Self::Hold { target } | Self::Release { target } => target,
        }
    }

    #[must_use]
    pub const fn host(self) -> ServerId {
        self.target().server
    }
}

#[must_use]
pub const fn lobby_control_kind() -> StreamKind {
    StreamKind::Control
}

pub fn encode_control(control: &LobbyControl) -> Result<Vec<u8>, LobbyControlError> {
    let payload = postcard::to_allocvec(control).map_err(|error| LobbyControlError {
        message: format!("encode lobby control: {error}"),
    })?;
    let mut bytes = Vec::with_capacity(LOBBY_CONTROL_TAG.len() + payload.len());
    bytes.extend_from_slice(LOBBY_CONTROL_TAG);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_control(bytes: &[u8]) -> Result<LobbyControl, LobbyControlError> {
    let Some(payload) = bytes.strip_prefix(LOBBY_CONTROL_TAG) else {
        return Err(LobbyControlError {
            message: String::from("decode lobby control: invalid tag"),
        });
    };
    postcard::from_bytes(payload).map_err(|error| LobbyControlError {
        message: format!("decode lobby control: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    #[test]
    fn control_roundtrips_and_routes_to_the_target_host() {
        let target = GlobalPlayerId::new(ServerId(4), PlayerSlot(9));
        let control = LobbyControl::Hold { target };
        let bytes = encode_control(&control).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), control);
        assert_eq!(control.host(), ServerId(4));
        assert!(decode_control(&[]).is_err());
    }
}
