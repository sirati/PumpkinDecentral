use serde::{Deserialize, Serialize};

const MAGIC: [u8; 4] = *b"HMSK";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum HealthMaskMode {
    Off,
    Players,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthPolicySnapshot {
    pub version: u64,
    pub mode: HealthMaskMode,
}

impl HealthPolicySnapshot {
    #[must_use]
    pub const fn new(version: u64, mode: HealthMaskMode) -> Self {
        Self { version, mode }
    }

    #[must_use]
    pub const fn accepts(self, candidate: Self) -> bool {
        candidate.version > self.version
    }

    #[must_use]
    pub const fn next(self, mode: HealthMaskMode) -> Option<Self> {
        match self.version.checked_add(1) {
            Some(version) => Some(Self { version, mode }),
            None => None,
        }
    }
}

#[must_use]
pub fn required_health(
    mode: HealthMaskMode,
    actor_is_player: bool,
    visual_requires_health: bool,
    viewer_requires_actual: bool,
    actual: f32,
) -> f32 {
    if viewer_requires_actual || visual_requires_health {
        return actual;
    }
    match mode {
        HealthMaskMode::Off => actual,
        HealthMaskMode::Players if !actor_is_player => actual,
        HealthMaskMode::Players | HealthMaskMode::All => 20.0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthPolicyMessage {
    RequestChange {
        base_version: u64,
        mode: HealthMaskMode,
    },
    Snapshot(HealthPolicySnapshot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Envelope {
    magic: [u8; 4],
    message: HealthPolicyMessage,
}

pub fn encode(message: HealthPolicyMessage) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&Envelope {
        magic: MAGIC,
        message,
    })
}

#[must_use]
pub fn is_health_policy_message(bytes: &[u8]) -> bool {
    bytes.starts_with(&MAGIC)
}

pub fn decode(bytes: &[u8]) -> Result<HealthPolicyMessage, postcard::Error> {
    let envelope: Envelope = postcard::from_bytes(bytes)?;
    if envelope.magic != MAGIC {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(envelope.message)
}

#[cfg(test)]
mod tests {
    use super::{
        HealthMaskMode, HealthPolicyMessage, HealthPolicySnapshot, decode, encode,
        is_health_policy_message, required_health,
    };

    #[test]
    fn wire_discriminator_rejects_other_control_payloads() {
        assert!(!is_health_policy_message(&[1, 2, 3, 4, 5]));
        let message =
            HealthPolicyMessage::Snapshot(HealthPolicySnapshot::new(42, HealthMaskMode::All));
        let encoded = encode(message).unwrap();
        assert!(is_health_policy_message(&encoded));
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn snapshots_advance_only_by_version() {
        let current = HealthPolicySnapshot::new(9, HealthMaskMode::Players);
        assert!(!current.accepts(HealthPolicySnapshot::new(9, HealthMaskMode::All)));
        assert!(!current.accepts(HealthPolicySnapshot::new(8, HealthMaskMode::Off)));
        assert!(current.accepts(HealthPolicySnapshot::new(10, HealthMaskMode::Off)));
        assert_eq!(
            current.next(HealthMaskMode::All),
            Some(HealthPolicySnapshot::new(10, HealthMaskMode::All))
        );
        assert_eq!(
            HealthPolicySnapshot::new(u64::MAX, HealthMaskMode::Players).next(HealthMaskMode::All),
            None
        );
    }

    #[test]
    fn required_fields_follow_modes_and_accuracy_exceptions() {
        let actual = 3.5;
        assert_eq!(
            required_health(HealthMaskMode::Off, true, false, false, actual),
            actual
        );
        assert_eq!(
            required_health(HealthMaskMode::Players, true, false, false, actual),
            20.0
        );
        assert_eq!(
            required_health(HealthMaskMode::Players, false, false, false, actual),
            actual
        );
        assert_eq!(
            required_health(HealthMaskMode::All, false, false, false, actual),
            20.0
        );
        assert_eq!(
            required_health(HealthMaskMode::All, false, true, false, actual),
            actual
        );
        assert_eq!(
            required_health(HealthMaskMode::All, false, false, true, actual),
            actual
        );
    }
}
