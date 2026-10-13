use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::StreamKind;

pub const PRESENCE_MAX_NAME_LEN: usize = 16;

pub const PRESENCE_MAX_ENTRIES: usize = 131_072;

pub const PRESENCE_MAX_PROPERTIES: usize = 4;

pub const PRESENCE_MAX_PROPERTY_NAME_LEN: usize = 64;

pub const PRESENCE_MAX_PROPERTY_VALUE_LEN: usize = 8192;

const PRESENCE_MAGIC: [u8; 4] = *b"PRES";

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
    pub in_lobby: bool,
    pub listed: bool,
    pub native_skin: bool,
}

impl PresenceLogin {
    #[must_use]
    pub fn new(
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        name: String,
        properties: Vec<PresenceProperty>,
        in_lobby: bool,
    ) -> Self {
        Self {
            gid,
            uuid,
            name,
            properties: sanitize_presence_properties(properties),
            in_lobby,
            listed: true,
            native_skin: false,
        }
    }

    #[must_use]
    pub fn with_native_skin(mut self, native_skin: bool) -> Self {
        self.native_skin = native_skin;
        self
    }

    #[must_use]
    pub fn with_listing(mut self, listed: bool) -> Self {
        self.listed = listed;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DepartureAnnouncement {
    Standard,
    Suppressed,
    CustomJson(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceLogout {
    pub gid: GlobalPlayerId,
    pub announcement: DepartureAnnouncement,
}

impl PresenceLogout {
    #[must_use]
    pub const fn new(gid: GlobalPlayerId) -> Self {
        Self {
            gid,
            announcement: DepartureAnnouncement::Standard,
        }
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
pub fn sanitize_presence_properties(properties: Vec<PresenceProperty>) -> Vec<PresenceProperty> {
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

fn valid_announcement(message: &PresenceControl) -> bool {
    !matches!(message, PresenceControl::Logout(PresenceLogout { announcement: DepartureAnnouncement::CustomJson(json), .. }) if json.len() > 16384)
}

pub fn encode_control(message: &PresenceControl) -> Result<Vec<u8>, PresenceError> {
    if !valid_announcement(message) {
        return Err(PresenceError {
            message: "presence announcement exceeds payload budget".to_owned(),
        });
    }
    let encoded = postcard::to_allocvec(message).map_err(|error| PresenceError {
        message: format!("encode presence control: {error}"),
    })?;
    let mut bytes = Vec::with_capacity(PRESENCE_MAGIC.len() + encoded.len());
    bytes.extend_from_slice(&PRESENCE_MAGIC);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

pub fn decode_control(bytes: &[u8]) -> Result<PresenceControl, PresenceError> {
    let Some(encoded) = bytes.strip_prefix(&PRESENCE_MAGIC) else {
        return Err(PresenceError {
            message: String::from("decode presence control: wrong discriminator"),
        });
    };
    let message = postcard::from_bytes(encoded).map_err(|error| PresenceError {
        message: format!("decode presence control: {error}"),
    })?;
    if !valid_announcement(&message) {
        return Err(PresenceError {
            message: "presence announcement exceeds payload budget".to_owned(),
        });
    }
    Ok(message)
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
    let bytes = encode_control(&PresenceControl::Logout(logout.clone()))?;
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
    pub source: Option<crate::realm_completion::PeerGeneration>,
    pub session: u64,
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub properties: Vec<PresenceProperty>,
    pub joined_at_millis: u64,
    pub in_lobby: bool,
    pub listed: bool,
    pub native_skin: bool,
}

impl RemotePlayerEntry {
    #[must_use]
    pub fn new(
        gid: GlobalPlayerId,
        uuid: [u8; 16],
        name: String,
        properties: Vec<PresenceProperty>,
        joined_at_millis: u64,
        in_lobby: bool,
        listed: bool,
    ) -> Self {
        Self {
            source: None,
            session: 0,
            gid,
            uuid,
            name,
            properties: sanitize_presence_properties(properties),
            joined_at_millis,
            in_lobby,
            listed,
            native_skin: false,
        }
    }

    #[must_use]
    pub fn with_native_skin(mut self, native_skin: bool) -> Self {
        self.native_skin = native_skin;
        self
    }

    #[must_use]
    pub const fn host(&self) -> ServerId {
        self.gid.server
    }
}

#[derive(Debug, Default)]
pub struct PresenceTable {
    names: HashMap<String, std::collections::BTreeSet<GlobalPlayerId>>,
    entries: HashMap<GlobalPlayerId, RemotePlayerEntry>,
}

impl PresenceTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            names: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    fn remove_name(&mut self, name: &str, gid: GlobalPlayerId) {
        if let Some(matches) = self.names.get_mut(name) {
            matches.remove(&gid);
            if matches.is_empty() { self.names.remove(name); }
        }
    }

    pub fn apply_login(&mut self, login: &PresenceLogin, now_millis: u64) -> bool {
        if !is_valid_presence_name(&login.name) {
            return false;
        }
        let properties = sanitize_presence_properties(login.properties.clone());
        if let Some(entry) = self.entries.get_mut(&login.gid) {
            let old_name = entry.name.to_ascii_lowercase();
            let new_name = login.name.to_ascii_lowercase();
            entry.uuid = login.uuid;
            entry.name = login.name.clone();
            entry.properties = properties;
            entry.in_lobby = login.in_lobby;
            entry.listed = login.listed;
            entry.native_skin = login.native_skin;
            if old_name != new_name {
                self.remove_name(&old_name, login.gid);
                self.names.entry(new_name).or_default().insert(login.gid);
            }
            return true;
        }
        if self.entries.len() >= PRESENCE_MAX_ENTRIES {
            return false;
        }
        self.names.entry(login.name.to_ascii_lowercase()).or_default().insert(login.gid);
        self.entries.insert(
            login.gid,
            RemotePlayerEntry::new(
                login.gid,
                login.uuid,
                login.name.clone(),
                properties,
                now_millis,
                login.in_lobby,
                login.listed,
            )
            .with_native_skin(login.native_skin),
        );
        true
    }

    pub fn apply_logout(&mut self, logout: &PresenceLogout) -> Option<RemotePlayerEntry> {
        let entry = self.entries.remove(&logout.gid)?;
        self.remove_name(&entry.name.to_ascii_lowercase(), logout.gid);
        Some(entry)
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
        let matches = self.names.get(&name.to_ascii_lowercase())?;
        (matches.len() == 1).then(|| matches.iter().next().copied()).flatten()
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

    #[test]
    fn bounded_global_directory_preserves_existing_players_when_full() {
        let mut table = PresenceTable::default();
        for index in 0..PRESENCE_MAX_ENTRIES {
            let entry = login((index / 65_536) as u16 + 1, index as u16, "online");
            assert!(table.apply_login(&entry, 0));
        }
        let first = gid(1, 0);
        let before = table.entries.get(&first).cloned().unwrap();
        assert!(!table.apply_login(&login(4, 0, "overflow"), 0));
        assert_eq!(table.len(), PRESENCE_MAX_ENTRIES);
        assert_eq!(table.entries.get(&first), Some(&before));
        assert_eq!(table.locate_by_name("overflow"), None);
    }

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn login(server: u16, player: u16, name: &str) -> PresenceLogin {
        PresenceLogin::new(
            gid(server, player),
            [player as u8; 16],
            name.to_string(),
            Vec::new(),
            false,
        )
    }

    #[test]
    fn normalized_name_index_rejects_ambiguity_and_tracks_rename_and_logout() {
        let mut table = PresenceTable::new();
        assert!(table.apply_login(&login(2, 7, "Steve"), 1));
        assert_eq!(table.locate_by_name("sTEVE"), Some(gid(2, 7)));
        assert!(table.apply_login(&login(3, 8, "STEVE"), 2));
        assert_eq!(table.locate_by_name("steve"), None);
        assert!(table.apply_login(&login(3, 8, "Alex"), 3));
        assert_eq!(table.locate_by_name("steve"), Some(gid(2, 7)));
        assert_eq!(table.locate_by_name("ALEX"), Some(gid(3, 8)));
        table.apply_logout(&PresenceLogout::new(gid(2, 7)));
        assert_eq!(table.locate_by_name("steve"), None);
        assert_eq!(table.locate_by_name("alex"), Some(gid(3, 8)));
    }

    #[test]
    fn native_skin_metadata_survives_wire_and_presence_updates() {
        let native = login(7, 3, "NativeAlice").with_native_skin(true);
        let bytes = encode_control(&PresenceControl::Login(native.clone())).unwrap();
        let mut table = PresenceTable::new();
        handle_presence_bytes(&mut table, &bytes, 10).unwrap();
        assert!(table.get(&native.gid).unwrap().native_skin);
        let java = login(7, 3, "NativeAlice");
        let bytes = encode_control(&PresenceControl::Login(java)).unwrap();
        handle_presence_bytes(&mut table, &bytes, 20).unwrap();
        assert!(!table.get(&native.gid).unwrap().native_skin);
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
        assert_eq!(
            decode_control(&bytes).unwrap(),
            PresenceControl::Login(announced)
        );
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
        let parcels = logout_parcels_for_peers(&PresenceLogout::new(gid(2, 7)), &[1, 3]).unwrap();
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
        assert!(
            table
                .apply_logout(&PresenceLogout::new(gid(9, 9)))
                .is_none()
        );
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
        assert!(matches!(effect, PresenceInboundEffect::LoginApplied(_)));
        assert!(decode_control(&[]).is_err());
        assert!(handle_presence_bytes(&mut table, &[], 50).is_err());
        let bytes =
            encode_control(&PresenceControl::Logout(PresenceLogout::new(gid(2, 7)))).unwrap();
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
    fn control_discriminator_prevents_hide_cross_talk() {
        let presence = encode_control(&PresenceControl::Login(login(2, 7, "Steve"))).unwrap();
        assert!(crate::hide::decode_hide(&presence).is_err());
        let hide = crate::hide::encode_hide(&crate::hide::HideUpdate::new(
            gid(2, 7),
            [7; 16],
            String::from("Steve"),
            true,
        ))
        .unwrap();
        assert!(decode_control(&hide).is_err());
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
