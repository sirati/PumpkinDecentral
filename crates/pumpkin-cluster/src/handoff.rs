use serde::{Deserialize, Serialize};

use crate::protocol::ChunkKey;

pub const HANDOFF_MAGIC: [u8; 8] = *b"PHOF\0\0\0\x01";
pub const MAX_PLAYERDATA_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HandoffMessage {
    ResumeHintRequest {
        request_id: u64,
        requester: u16,
        uuid: [u8; 16],
    },
    ResumeHintResponse {
        request_id: u64,
        hint: Option<PlayerResumeHint>,
    },
    ResumeHintPublished(PlayerResumeHint),
    PlayerdataRequest {
        request_id: u64,
        requester: u16,
        uuid: [u8; 16],
        revision: u64,
    },
    PlayerdataResponse {
        request_id: u64,
        uuid: [u8; 16],
        revision: u64,
        data: Vec<u8>,
    },
    PlayerdataUnavailable {
        request_id: u64,
        uuid: [u8; 16],
        revision: u64,
    },
    CommitPlayerdata {
        request_id: u64,
        expected_revision: u64,
        hint: PlayerResumeHint,
        data: Vec<u8>,
    },
    CommitResult {
        request_id: u64,
        uuid: [u8; 16],
        previous_revision: u64,
        revision: u64,
        accepted: bool,
    },
}

#[derive(Debug)]
pub enum HandoffCodecError {
    Oversized,
    WrongProtocol,
    Codec(postcard::Error),
}

impl core::fmt::Display for HandoffCodecError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Oversized => formatter.write_str("handoff payload exceeds limit"),
            Self::WrongProtocol => formatter.write_str("not a handoff payload"),
            Self::Codec(error) => write!(formatter, "handoff codec: {error}"),
        }
    }
}

impl std::error::Error for HandoffCodecError {}

pub fn encode_message(message: &HandoffMessage) -> Result<Vec<u8>, HandoffCodecError> {
    if matches!(message, HandoffMessage::PlayerdataResponse { data, .. } | HandoffMessage::CommitPlayerdata { data, .. } if data.len() > MAX_PLAYERDATA_BYTES)
    {
        return Err(HandoffCodecError::Oversized);
    }
    postcard::to_extend(message, Vec::from(HANDOFF_MAGIC)).map_err(HandoffCodecError::Codec)
}

pub fn decode_message(bytes: &[u8]) -> Result<HandoffMessage, HandoffCodecError> {
    let Some(payload) = bytes.strip_prefix(&HANDOFF_MAGIC) else {
        return Err(HandoffCodecError::WrongProtocol);
    };
    if payload.len() > MAX_PLAYERDATA_BYTES.saturating_add(1024) {
        return Err(HandoffCodecError::Oversized);
    }
    let message = postcard::from_bytes(payload).map_err(HandoffCodecError::Codec)?;
    if matches!(&message, HandoffMessage::PlayerdataResponse { data, .. } | HandoffMessage::CommitPlayerdata { data, .. } if data.len() > MAX_PLAYERDATA_BYTES)
    {
        return Err(HandoffCodecError::Oversized);
    }
    Ok(message)
}

#[must_use]
pub fn is_handoff_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(&HANDOFF_MAGIC)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerResumeHint {
    pub uuid: [u8; 16],
    pub dimension: String,
    pub position: [f64; 3],
    pub rotation: [f32; 2],
    pub view_radius: u8,
    pub playerdata_revision: u64,
    pub last_owner: Option<u16>,
    pub retained_chunks: Vec<ChunkKey>,
    pub retained_until_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffError {
    AlreadyPinned,
    RevisionMismatch { expected: u64, received: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PinnedHandoff {
    hint: Option<PlayerResumeHint>,
}

impl PinnedHandoff {
    #[must_use]
    pub const fn new() -> Self {
        Self { hint: None }
    }

    pub fn pin(&mut self, hint: PlayerResumeHint) -> Result<(), HandoffError> {
        if self.hint.is_some() {
            return Err(HandoffError::AlreadyPinned);
        }
        self.hint = Some(hint);
        Ok(())
    }

    #[must_use]
    pub const fn hint(&self) -> Option<&PlayerResumeHint> {
        self.hint.as_ref()
    }

    pub fn validate_playerdata_revision(&self, received: u64) -> Result<(), HandoffError> {
        let Some(hint) = &self.hint else {
            return Err(HandoffError::AlreadyPinned);
        };
        if hint.playerdata_revision != received {
            return Err(HandoffError::RevisionMismatch {
                expected: hint.playerdata_revision,
                received,
            });
        }
        Ok(())
    }
}

impl Default for PinnedHandoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(revision: u64, position: [f64; 3]) -> PlayerResumeHint {
        PlayerResumeHint {
            uuid: [3; 16],
            dimension: "minecraft:overworld".to_string(),
            position,
            rotation: [0.0, 0.0],
            view_radius: 8,
            playerdata_revision: revision,
            last_owner: Some(2),
            retained_chunks: vec![ChunkKey::new(crate::protocol::WorldId::new(0, 7), 4, -2)],
            retained_until_millis: 30_000,
        }
    }

    #[test]
    fn acquisition_target_cannot_retarget_after_it_is_pinned() {
        let mut handoff = PinnedHandoff::new();
        let resume = hint(11, [80.0, 70.0, -32.0]);
        handoff.pin(resume.clone()).unwrap();
        assert_eq!(
            handoff.pin(hint(12, [0.0, 64.0, 0.0])),
            Err(HandoffError::AlreadyPinned)
        );
        assert_eq!(handoff.hint(), Some(&resume));
    }

    #[test]
    fn mismatched_playerdata_cannot_complete_handoff() {
        let mut handoff = PinnedHandoff::new();
        handoff.pin(hint(11, [80.0, 70.0, -32.0])).unwrap();
        assert_eq!(
            handoff.validate_playerdata_revision(12),
            Err(HandoffError::RevisionMismatch {
                expected: 11,
                received: 12,
            })
        );
        assert_eq!(handoff.validate_playerdata_revision(11), Ok(()));
    }
}
