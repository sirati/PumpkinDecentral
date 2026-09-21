use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::{PlayerGameMode, StreamKind};

pub const PRESENCE_MAX_NAME_LEN: usize = 16;

pub const PRESENCE_MAX_ENTRIES: usize = 4096;

pub const PRESENCE_MAX_PROPERTIES: usize = 4;

pub const PRESENCE_MAX_PROPERTY_NAME_LEN: usize = 64;

pub const PRESENCE_MAX_PROPERTY_VALUE_LEN: usize = 8192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceError {
    pub message: String,
}

impl core::fmt::Display for PresenceError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PresenceError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceProperty {
    pub name: String,
    pub value: String,
    pub signature: Option<String>,
}

impl PresenceProperty {
    #[must_use]
    pub fn new(name: String, value: String, signature: Option<String>) -> Self {
        Self {
            name,
            value,
            signature,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceLogin {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub properties: Vec<PresenceProperty>,
    pub gamemode: PlayerGameMode,
    pub in_lobby: bool,
}

impl PresenceLogin {
    #[must_use]
    pub fn new(
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        name: String,
        properties: Vec<PresenceProperty>,
        gamemode: PlayerGameMode,
        in_lobby: bool,
    ) -> Self {
        Self {
            gid,
            uuid,
            name,
            properties: sanitize_presence_properties(properties),
            gamemode,
            in_lobby,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceLogout {
    pub gid: GlobalPlayerId,
}

impl PresenceLogout {
    #[must_use]
    pub const fn new(gid: GlobalPlayerId) -> Self {
        Self { gid }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PresenceControl {
    Login(PresenceLogin),
    Logout(PresenceLogout),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

impl PresenceParcel {
    #[must_use]
    pub fn new(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> Self {
        Self { peer, kind, bytes }
    }
}

#[must_use]
pub const fn presence_control_kind() -> StreamKind {
    StreamKind::Control
}

#[must_use]
pub fn is_valid_presence_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= PRESENCE_MAX_NAME_LEN
}

#[must_use]
pub fn is_valid_presence_property(property: &PresenceProperty) -> bool {
    !property.name.is_empty()
        && property.name.len() <= PRESENCE_MAX_PROPERTY_NAME_LEN
        && !property.value.is_empty()
        && property.value.len() <= PRESENCE_MAX_PROPERTY_VALUE_LEN
        && property
            .signature
            .as_ref()
            .is_none_or(|signature| signature.len() <= PRESENCE_MAX_PROPERTY_VALUE_LEN)
}

fn truncate_to(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut end = max_len;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    text[..end].to_string()
}

#[must_use]
pub fn sanitize_presence_properties(
    properties: Vec<PresenceProperty>,
) -> Vec<PresenceProperty> {
    properties
        .into_iter()
        .filter(|property| !property.name.is_empty() && !property.value.is_empty())
        .take(PRESENCE_MAX_PROPERTIES)
        .map(|property| {
            PresenceProperty::new(
                truncate_to(property.name.as_str(), PRESENCE_MAX_PROPERTY_NAME_LEN),
                truncate_to(property.value.as_str(), PRESENCE_MAX_PROPERTY_VALUE_LEN),
                property
                    .signature
                    .as_ref()
                    .map(|signature| truncate_to(signature, PRESENCE_MAX_PROPERTY_VALUE_LEN)),
            )
        })
        .collect()
}

pub fn encode_control(message: &PresenceControl) -> Result<Vec<u8>, PresenceError> {
    postcard::to_allocvec(message).map_err(|error| PresenceError {
        message: format!("encode presence control: {error}"),
    })
}

pub fn decode_control(bytes: &[u8]) -> Result<PresenceControl, PresenceError> {
    postcard::from_bytes(bytes).map_err(|error| PresenceError {
        message: format!("decode presence control: {error}"),
    })
}

pub fn login_parcels_for_peers(
    login: &PresenceLogin,
    peers: &[u16],
) -> Result<Vec<PresenceParcel>, PresenceError> {
    let bytes = encode_control(&PresenceControl::Login(login.clone()))?;
    let mut parcels = Vec::with_capacity(peers.len());
    for peer in peers {
        parcels.push(PresenceParcel::new(
            *peer,
            presence_control_kind(),
            bytes.clone(),
        ));
    }
    Ok(parcels)
}

pub fn logout_parcels_for_peers(
    logout: &PresenceLogout,
    peers: &[u16],
) -> Result<Vec<PresenceParcel>, PresenceError> {
    let bytes = encode_control(&PresenceControl::Logout(*logout))?;
    let mut parcels = Vec::with_capacity(peers.len());
    for peer in peers {
        parcels.push(PresenceParcel::new(
            *peer,
            presence_control_kind(),
            bytes.clone(),
        ));
    }
    Ok(parcels)
}

pub fn try_broadcast_control_message(
    tx: &mpsc::Sender<PresenceParcel>,
    message: &PresenceControl,
    peers: &[u16],
) -> Result<usize, PresenceError> {
    let bytes = encode_control(message)?;
    let mut sent = 0_usize;
    for peer in peers {
        let parcel = PresenceParcel::new(*peer, presence_control_kind(), bytes.clone());
        if tx.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePlayerEntry {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub properties: Vec<PresenceProperty>,
    pub gamemode: PlayerGameMode,
    pub joined_at_millis: u64,
    pub in_lobby: bool,
}

impl RemotePlayerEntry {
    #[must_use]
    pub fn new(
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        name: String,
        properties: Vec<PresenceProperty>,
        gamemode: PlayerGameMode,
        joined_at_millis: u64,
        in_lobby: bool,
    ) -> Self {
        Self {
            gid,
            uuid,
            name,
            properties: sanitize_presence_properties(properties),
            gamemode,
            joined_at_millis,
            in_lobby,
        }
    }

    #[must_use]
    pub const fn host(&self) -> ServerId {
        self.gid.server
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePlayerProfile {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub properties: Vec<PresenceProperty>,
    pub gamemode: PlayerGameMode,
}

impl RemotePlayerEntry {
    #[must_use]
    pub fn replica_profile(&self) -> Option<RemotePlayerProfile> {
        (!self.in_lobby).then(|| RemotePlayerProfile {
            gid: self.gid,
            uuid: self.uuid,
            name: self.name.clone(),
            properties: self.properties.clone(),
            gamemode: self.gamemode,
        })
    }
}

#[derive(Debug, Default)]
pub struct PresenceTable {
    entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
}

impl PresenceTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn apply_login(&mut self, login: &PresenceLogin, now_millis: u64) -> bool {
        if !is_valid_presence_name(&login.name) {
            return false;
        }
        if !login.properties.iter().all(is_valid_presence_property) {
            return false;
        }
        let properties = sanitize_presence_properties(login.properties.clone());
        if let Some(entry) = self.entries.get_mut(&login.gid) {
            entry.uuid = login.uuid;
            entry.name = login.name.clone();
            entry.properties = properties;
            entry.gamemode = login.gamemode;
            entry.in_lobby = login.in_lobby;
            return true;
        }
        if self.entries.len() >= PRESENCE_MAX_ENTRIES {
            if let Some(evicted) = self.entries.keys().next().copied() {
                self.entries.remove(&evicted);
            }
        }
        self.entries.insert(
            login.gid,
            RemotePlayerEntry::new(
                login.gid,
                login.uuid,
                login.name.clone(),
                properties,
                login.gamemode,
                now_millis,
                login.in_lobby,
            ),
        );
        true
    }

    pub fn apply_logout(&mut self, logout: &PresenceLogout) -> Option<RemotePlayerEntry> {
        self.entries.remove(&logout.gid)
    }

    #[must_use]
    pub fn get(&self, gid: &GlobalPlayerId) -> Option<&RemotePlayerEntry> {
        self.entries.get(gid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries_iter(&self) -> impl Iterator<Item = (&GlobalPlayerId, &RemotePlayerEntry)> {
        self.entries.iter()
    }

    #[must_use]
    pub fn locate_by_name(&self, name: &str) -> Option<GlobalPlayerId> {
        let folded = name.to_lowercase();
        self.entries
            .iter()
            .find(|(_, entry)| entry.name.to_lowercase() == folded)
            .map(|(gid, _)| *gid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceInboundEffect {
    LoginApplied(PresenceLogin),
    LogoutApplied {
        logout: PresenceLogout,
        removed: Option<RemotePlayerEntry>,
    },
    Ignored,
}

pub fn handle_presence_bytes(
    table: &mut PresenceTable,
    bytes: &[u8],
    now_millis: u64,
) -> Result<PresenceInboundEffect, PresenceError> {
    let message = decode_control(bytes)?;
    Ok(match message {
        PresenceControl::Login(login) => {
            if table.apply_login(&login, now_millis) {
                PresenceInboundEffect::LoginApplied(login)
            } else {
                PresenceInboundEffect::Ignored
            }
        }
        PresenceControl::Logout(logout) => {
            let removed = table.apply_logout(&logout);
            PresenceInboundEffect::LogoutApplied { logout, removed }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn login(server: u16, player: u16, name: &str) -> PresenceLogin {
        PresenceLogin::new(
            gid(server, player),
            [player as u8; 16],
            name.to_string(),
            Vec::new(),
            PlayerGameMode::Survival,
            false,
        )
    }

    #[test]
    fn login_update_tracks_lobby_state() {
        let mut table = PresenceTable::new();
        let mut waiting = login(7, 3, "LobbyAlice");
        waiting.in_lobby = true;
        assert!(table.apply_login(&waiting, 10));
        assert!(table.get(&waiting.gid).is_some_and(|entry| entry.in_lobby));

        let active = login(7, 3, "LobbyAlice");
        assert!(table.apply_login(&active, 20));
        assert!(table.get(&active.gid).is_some_and(|entry| !entry.in_lobby));
    }

    fn textured() -> Vec<PresenceProperty> {
        vec![PresenceProperty::new(
            String::from("textures"),
            String::from("dGV4dHVyZXM="),
            Some(String::from("c2lnbmF0dXJl")),
        )]
    }

    #[test]
    fn control_roundtrips() {
        let message = PresenceControl::Login(login(2, 7, "Steve"));
        let bytes = encode_control(&message).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), message);
        let message = PresenceControl::Logout(PresenceLogout::new(gid(2, 7)));
        let bytes = encode_control(&message).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), message);
        assert_eq!(presence_control_kind(), StreamKind::Control);
    }

    #[test]
    fn names_are_bounded() {
        assert!(is_valid_presence_name("Steve"));
        assert!(!is_valid_presence_name(""));
        assert!(!is_valid_presence_name("this-name-is-way-too-long"));
    }

    #[test]
    fn properties_roundtrip_and_sanitize() {
        let mut announced = login(2, 7, "Steve");
        announced.properties = textured();
        let bytes = encode_control(&PresenceControl::Login(announced.clone())).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), PresenceControl::Login(announced));
        assert!(is_valid_presence_property(&textured()[0]));
        assert!(!is_valid_presence_property(&PresenceProperty::new(
            String::new(),
            String::from("v"),
            None,
        )));
        let oversized = vec![
            PresenceProperty::new(String::from("textures"), String::from("v"), None),
            PresenceProperty::new(String::from("a"), String::from("v"), None),
            PresenceProperty::new(String::from("b"), String::from("v"), None),
            PresenceProperty::new(String::from("c"), String::from("v"), None),
            PresenceProperty::new(String::from("d"), String::from("v"), None),
        ];
        let sanitized = sanitize_presence_properties(oversized);
        assert_eq!(sanitized.len(), PRESENCE_MAX_PROPERTIES);
        let mut table = PresenceTable::new();
        let mut announced = login(2, 7, "Steve");
        announced.properties = textured();
        assert!(table.apply_login(&announced, 100));
        assert_eq!(table.get(&gid(2, 7)).unwrap().properties, textured());
        announced.properties = Vec::new();
        assert!(table.apply_login(&announced, 200));
        assert!(table.get(&gid(2, 7)).unwrap().properties.is_empty());
    }

    #[test]
    fn parcels_target_every_peer() {
        let parcels = login_parcels_for_peers(&login(2, 7, "Steve"), &[1, 3]).unwrap();
        assert_eq!(parcels.len(), 2);
        assert_eq!(parcels[0].peer, 1);
        assert_eq!(parcels[1].peer, 3);
        let parcels =
            logout_parcels_for_peers(&PresenceLogout::new(gid(2, 7)), &[1, 3]).unwrap();
        assert_eq!(parcels.len(), 2);
        assert_eq!(parcels[0].kind, StreamKind::Control);
    }

    #[test]
    fn table_expires_on_logout() {
        let mut table = PresenceTable::new();
        assert!(table.is_empty());
        assert!(table.apply_login(&login(2, 7, "Steve"), 100));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&gid(2, 7)).unwrap().name, String::from("Steve"));
        assert_eq!(table.get(&gid(2, 7)).unwrap().joined_at_millis, 100);
        assert_eq!(table.get(&gid(2, 7)).unwrap().host(), ServerId(2));
        assert!(table.apply_login(&login(2, 7, "Alex"), 200));
        assert_eq!(table.get(&gid(2, 7)).unwrap().name, String::from("Alex"));
        assert_eq!(table.get(&gid(2, 7)).unwrap().joined_at_millis, 100);
        assert_eq!(table.locate_by_name("alex"), Some(gid(2, 7)));
        let removed = table.apply_logout(&PresenceLogout::new(gid(2, 7)));
        assert_eq!(removed.unwrap().name, String::from("Alex"));
        assert!(table.is_empty());
        assert!(table.apply_logout(&PresenceLogout::new(gid(9, 9))).is_none());
    }

    #[test]
    fn table_rejects_invalid_names() {
        let mut table = PresenceTable::new();
        assert!(!table.apply_login(&login(2, 7, ""), 100));
        assert!(table.is_empty());
    }

    #[test]
    fn inbound_dispatch_applies_or_ignores() {
        let mut table = PresenceTable::new();
        let bytes = encode_control(&PresenceControl::Login(login(2, 7, "Steve"))).unwrap();
        let effect = handle_presence_bytes(&mut table, &bytes, 50).unwrap();
        assert!(matches!(
            effect,
            PresenceInboundEffect::LoginApplied(_)
        ));
        assert!(decode_control(&[]).is_err());
        assert!(handle_presence_bytes(&mut table, &[], 50).is_err());
        let bytes = encode_control(&PresenceControl::Logout(PresenceLogout::new(gid(2, 7)))).unwrap();
        let effect = handle_presence_bytes(&mut table, &bytes, 60).unwrap();
        match effect {
            PresenceInboundEffect::LogoutApplied { logout, removed } => {
                assert_eq!(logout.gid, gid(2, 7));
                assert_eq!(removed.unwrap().name, String::from("Steve"));
            }
            PresenceInboundEffect::LoginApplied(_) | PresenceInboundEffect::Ignored => {
                panic!("expected logout effect")
            }
        }
        let bytes = encode_control(&PresenceControl::Login(login(2, 8, ""))).unwrap();
        let effect = handle_presence_bytes(&mut table, &bytes, 70).unwrap();
        assert_eq!(effect, PresenceInboundEffect::Ignored);
    }

    #[test]
    fn try_broadcast_skips_full_queue() {
        let (tx, _rx) = mpsc::channel(1);
        let count = try_broadcast_control_message(
            &tx,
            &PresenceControl::Login(login(2, 7, "Steve")),
            &[1, 3],
        )
        .unwrap();
        assert_eq!(count, 1);
    }
}
