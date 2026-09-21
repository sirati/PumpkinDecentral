use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::identity::GlobalPlayerId;
use crate::protocol::StreamKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HideError {
    pub message: String,
}

impl core::fmt::Display for HideError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HideError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HideUpdate {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub hidden: bool,
}

impl HideUpdate {
    #[must_use]
    pub fn new(gid: GlobalPlayerId, uuid: [u8; 16], name: String, hidden: bool) -> Self {
        Self {
            gid,
            uuid,
            name,
            hidden,
        }
    }
}

#[must_use]
pub const fn hide_control_kind() -> StreamKind {
    StreamKind::Control
}

#[must_use]
pub fn is_valid_hide_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= super::presence::PRESENCE_MAX_NAME_LEN
}

pub fn encode_hide(update: &HideUpdate) -> Result<Vec<u8>, HideError> {
    postcard::to_allocvec(update).map_err(|error| HideError {
        message: format!("encode hide update: {error}"),
    })
}

pub fn decode_hide(bytes: &[u8]) -> Result<HideUpdate, HideError> {
    postcard::from_bytes(bytes).map_err(|error| HideError {
        message: format!("decode hide update: {error}"),
    })
}

#[must_use]
pub fn apply_hide_to_sets(
    gids: &mut HashSet<GlobalPlayerId>,
    uuids: &mut HashSet<[u8; 16]>,
    update: &HideUpdate,
) -> bool {
    if !is_valid_hide_name(&update.name) {
        return false;
    }
    if update.hidden {
        let fresh_gid = gids.insert(update.gid);
        let fresh_uuid = uuids.insert(update.uuid);
        fresh_gid || fresh_uuid
    } else {
        let had_gid = gids.remove(&update.gid);
        let had_uuid = uuids.remove(&update.uuid);
        had_gid || had_uuid
    }
}

#[must_use]
pub fn hide_set_contains_gid(set: &HashSet<GlobalPlayerId>, gid: &GlobalPlayerId) -> bool {
    set.contains(gid)
}

#[must_use]
pub fn hide_set_contains_uuid(set: &HashSet<[u8; 16]>, uuid: &[u8; 16]) -> bool {
    set.contains(uuid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn update(server: u16, player: u16, hidden: bool) -> HideUpdate {
        HideUpdate::new(gid(server, player), [player as u8; 16], String::from("Steve"), hidden)
    }

    #[test]
    fn roundtrips() {
        let message = update(2, 7, true);
        let bytes = encode_hide(&message).unwrap();
        assert_eq!(decode_hide(&bytes).unwrap(), message);
        let message = update(2, 7, false);
        let bytes = encode_hide(&message).unwrap();
        assert_eq!(decode_hide(&bytes).unwrap(), message);
        assert_eq!(hide_control_kind(), StreamKind::Control);
    }

    #[test]
    fn names_are_bounded() {
        assert!(is_valid_hide_name("Steve"));
        assert!(!is_valid_hide_name(""));
        assert!(!is_valid_hide_name("this-name-is-way-too-long"));
    }

    #[test]
    fn sets_apply_and_clear() {
        let mut gids = HashSet::new();
        let mut uuids = HashSet::new();
        assert!(apply_hide_to_sets(&mut gids, &mut uuids, &update(2, 7, true)));
        assert!(hide_set_contains_gid(&gids, &gid(2, 7)));
        assert!(hide_set_contains_uuid(&uuids, &[7_u8; 16]));
        assert!(!apply_hide_to_sets(&mut gids, &mut uuids, &update(2, 7, true)));
        assert!(apply_hide_to_sets(&mut gids, &mut uuids, &update(2, 7, false)));
        assert!(!hide_set_contains_gid(&gids, &gid(2, 7)));
        assert!(!hide_set_contains_uuid(&uuids, &[7_u8; 16]));
        assert!(!apply_hide_to_sets(&mut gids, &mut uuids, &update(2, 7, false)));
    }

    #[test]
    fn rejects_invalid_names() {
        let mut gids = HashSet::new();
        let mut uuids = HashSet::new();
        let bad = HideUpdate::new(gid(2, 7), [7_u8; 16], String::new(), true);
        assert!(!apply_hide_to_sets(&mut gids, &mut uuids, &bad));
        assert!(gids.is_empty());
        assert!(decode_hide(&[]).is_err());
    }
}
