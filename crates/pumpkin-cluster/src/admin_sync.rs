use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::identity::{GlobalPlayerId, ServerId};
use crate::primary::{AcceptedTick, PrimarySaveHandle};
use crate::protocol::StreamKind;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminSyncError {
    pub message: String,
}

impl core::fmt::Display for AdminSyncError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AdminSyncError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpGrant {
    pub uuid: [u8; 16],
    pub name: String,
    pub level: u8,
    pub bypasses_player_limit: bool,
}

impl OpGrant {
    #[must_use]
    pub fn new(uuid: [u8; 16], name: String, level: u8, bypasses_player_limit: bool) -> Self {
        Self {
            uuid,
            name,
            level,
            bypasses_player_limit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpRevoke {
    pub uuid: [u8; 16],
    /// Kept with the revoke so remote audit output remains human-readable even
    /// when the target is offline and the local op entry has already gone.
    pub name: String,
}

impl OpRevoke {
    #[must_use]
    pub fn new(uuid: [u8; 16], name: String) -> Self {
        Self { uuid, name }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BanEntry {
    pub uuid: [u8; 16],
    pub name: String,
    pub source: String,
    pub reason: String,
    pub created_epoch_secs: i64,
    pub expires_epoch_secs: Option<i64>,
}

impl BanEntry {
    #[must_use]
    pub fn new(
        uuid: [u8; 16],
        name: String,
        source: String,
        reason: String,
        created_epoch_secs: i64,
        expires_epoch_secs: Option<i64>,
    ) -> Self {
        Self {
            uuid,
            name,
            source,
            reason,
            created_epoch_secs,
            expires_epoch_secs,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BanRevoke {
    pub uuid: [u8; 16],
    pub name: String,
}

impl BanRevoke {
    #[must_use]
    pub fn new(uuid: [u8; 16], name: String) -> Self {
        Self { uuid, name }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminMutation {
    GrantOp(OpGrant),
    RevokeOp(OpRevoke),
    AddBan(BanEntry),
    RemoveBan(BanRevoke),
}

/// An operator-visible record of a mutation performed on another cluster
/// member.  Mutations themselves deliberately contain only state: this
/// companion message carries the command issuer so peers can present the same
/// administrative feedback to their local operators without replaying the
/// command or rebroadcasting the mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAudit {
    pub issuer: String,
    pub mutation: AdminMutation,
}

impl AdminAudit {
    #[must_use]
    pub fn new(issuer: String, mutation: AdminMutation) -> Self {
        Self { issuer, mutation }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KickRequest {
    pub target: GlobalPlayerId,
    pub target_name: String,
    pub reason: String,
    pub issuer: String,
}

impl KickRequest {
    #[must_use]
    pub fn new(
        target: GlobalPlayerId,
        target_name: String,
        reason: String,
        issuer: String,
    ) -> Self {
        Self {
            target,
            target_name,
            reason,
            issuer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminControlMessage {
    Mutation(AdminMutation),
    /// A state mutation together with the one operator-visible notification
    /// that belongs to it. Keeping them in one reliable control parcel means
    /// a peer cannot display a success before it has applied the change.
    MutationWithAudit(AdminAudit),
    Kick(KickRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

impl AdminParcel {
    #[must_use]
    pub fn new(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> Self {
        Self { peer, kind, bytes }
    }
}

#[must_use]
pub const fn admin_control_kind() -> StreamKind {
    StreamKind::Control
}

#[must_use]
pub fn is_valid_op_level(level: u8) -> bool {
    level <= 4
}

#[must_use]
pub fn normalize_player_name(name: &str) -> String {
    name.to_lowercase()
}

pub fn encode_mutation(mutation: &AdminMutation) -> Result<Vec<u8>, AdminSyncError> {
    postcard::to_allocvec(mutation).map_err(|error| AdminSyncError {
        message: format!("encode admin mutation: {error}"),
    })
}

pub fn decode_mutation(bytes: &[u8]) -> Result<AdminMutation, AdminSyncError> {
    postcard::from_bytes(bytes).map_err(|error| AdminSyncError {
        message: format!("decode admin mutation: {error}"),
    })
}

pub fn encode_control_message(
    message: &AdminControlMessage,
) -> Result<Vec<u8>, AdminSyncError> {
    postcard::to_allocvec(message).map_err(|error| AdminSyncError {
        message: format!("encode admin control: {error}"),
    })
}

pub fn decode_control_message(bytes: &[u8]) -> Result<AdminControlMessage, AdminSyncError> {
    postcard::from_bytes(bytes).map_err(|error| AdminSyncError {
        message: format!("decode admin control: {error}"),
    })
}

pub fn control_parcels_for_peers(
    message: &AdminControlMessage,
    peers: &[u16],
) -> Result<Vec<AdminParcel>, AdminSyncError> {
    let bytes = encode_control_message(message)?;
    let mut parcels = Vec::with_capacity(peers.len());
    for peer in peers {
        parcels.push(AdminParcel::new(*peer, admin_control_kind(), bytes.clone()));
    }
    Ok(parcels)
}

pub fn mutation_parcels_for_peers(
    mutation: &AdminMutation,
    peers: &[u16],
) -> Result<Vec<AdminParcel>, AdminSyncError> {
    control_parcels_for_peers(&AdminControlMessage::Mutation(mutation.clone()), peers)
}

pub fn kick_parcel_for_host(request: &KickRequest) -> Result<AdminParcel, AdminSyncError> {
    let bytes = encode_control_message(&AdminControlMessage::Kick(request.clone()))?;
    Ok(AdminParcel::new(
        request.target.server.0,
        admin_control_kind(),
        bytes,
    ))
}

pub fn try_broadcast_control_message(
    tx: &mpsc::Sender<AdminParcel>,
    message: &AdminControlMessage,
    peers: &[u16],
) -> Result<usize, AdminSyncError> {
    let bytes = encode_control_message(message)?;
    let mut sent = 0_usize;
    for peer in peers {
        let parcel = AdminParcel::new(*peer, admin_control_kind(), bytes.clone());
        if tx.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
}

pub fn submit_admin_mutation(
    handle: &PrimarySaveHandle,
    tick: TickStamp,
    mutation: &AdminMutation,
) -> Result<(), AdminSyncError> {
    let payload = encode_mutation(mutation)?;
    handle.try_submit(tick, payload).map_err(|_| AdminSyncError {
        message: String::from("primary admin queue full"),
    })
}

pub fn try_submit_admin_mutation(
    tx: &mpsc::Sender<AcceptedTick>,
    tick: TickStamp,
    mutation: &AdminMutation,
) -> Result<(), AdminSyncError> {
    let payload = encode_mutation(mutation)?;
    crate::primary::try_submit(tx, tick, payload).map_err(|_| AdminSyncError {
        message: String::from("primary admin queue full"),
    })
}

pub fn decode_primary_tick(tick: &AcceptedTick) -> Result<AdminMutation, AdminSyncError> {
    decode_mutation(&tick.payload)
}

pub fn persist_and_parcels_for_peers(
    handle: &PrimarySaveHandle,
    tick: TickStamp,
    mutation: &AdminMutation,
    peers: &[u16],
) -> Result<Vec<AdminParcel>, AdminSyncError> {
    submit_admin_mutation(handle, tick, mutation)?;
    mutation_parcels_for_peers(mutation, peers)
}

pub trait OpStore {
    fn grant(&mut self, grant: &OpGrant) -> bool;
    fn revoke(&mut self, revoke: &OpRevoke) -> bool;
}

pub trait BanStore {
    fn add(&mut self, entry: &BanEntry) -> bool;
    fn remove(&mut self, revoke: &BanRevoke) -> bool;
}

pub fn apply_mutation_to_stores<Op, Ban>(
    op_store: &mut Op,
    ban_store: &mut Ban,
    mutation: &AdminMutation,
) -> bool
where
    Op: OpStore,
    Ban: BanStore,
{
    match mutation {
        AdminMutation::GrantOp(grant) => op_store.grant(grant),
        AdminMutation::RevokeOp(revoke) => op_store.revoke(revoke),
        AdminMutation::AddBan(entry) => ban_store.add(entry),
        AdminMutation::RemoveBan(revoke) => ban_store.remove(revoke),
    }
}

pub fn apply_primary_tick_to_stores<Op, Ban>(
    op_store: &mut Op,
    ban_store: &mut Ban,
    tick: &AcceptedTick,
) -> Result<bool, AdminSyncError>
where
    Op: OpStore,
    Ban: BanStore,
{
    let mutation = decode_primary_tick(tick)?;
    Ok(apply_mutation_to_stores(op_store, ban_store, &mutation))
}

#[derive(Debug, Default)]
pub struct InMemoryOpStore {
    pub entries: HashMap<[u8; 16], OpGrant>,
}

impl InMemoryOpStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn contains(&self, uuid: &[u8; 16]) -> bool {
        self.entries.contains_key(uuid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl OpStore for InMemoryOpStore {
    fn grant(&mut self, grant: &OpGrant) -> bool {
        if !is_valid_op_level(grant.level) {
            return false;
        }
        match self.entries.get(&grant.uuid) {
            Some(existing)
                if existing.level == grant.level
                    && existing.name == grant.name
                    && existing.bypasses_player_limit == grant.bypasses_player_limit =>
            {
                false
            }
            Some(_) | None => {
                self.entries.insert(grant.uuid, grant.clone());
                true
            }
        }
    }

    fn revoke(&mut self, revoke: &OpRevoke) -> bool {
        self.entries.remove(&revoke.uuid).is_some()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryBanStore {
    pub entries: HashMap<[u8; 16], BanEntry>,
}

impl InMemoryBanStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn contains(&self, uuid: &[u8; 16]) -> bool {
        self.entries.contains_key(uuid)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl BanStore for InMemoryBanStore {
    fn add(&mut self, entry: &BanEntry) -> bool {
        match self.entries.get(&entry.uuid) {
            Some(existing) if existing == entry => false,
            Some(_) | None => {
                self.entries.insert(entry.uuid, entry.clone());
                true
            }
        }
    }

    fn remove(&mut self, revoke: &BanRevoke) -> bool {
        self.entries.remove(&revoke.uuid).is_some()
    }
}

#[derive(Debug, Clone)]
pub struct PlayerDirectorySnapshot {
    pub local: ServerId,
    by_name: HashMap<String, GlobalPlayerId>,
    names: HashMap<GlobalPlayerId, String>,
}

impl PlayerDirectorySnapshot {
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
            self.by_name.remove(&normalize_player_name(&previous));
        }
        self.by_name.insert(normalize_player_name(&name), gid);
    }

    pub fn remove_gid(&mut self, gid: &GlobalPlayerId) -> bool {
        match self.names.remove(gid) {
            Some(name) => {
                self.by_name.remove(&normalize_player_name(&name));
                true
            }
            None => false,
        }
    }

    pub fn apply_presence_login(&mut self, gid: GlobalPlayerId, name: String) {
        self.insert(gid, name);
    }

    pub fn apply_presence_logout(&mut self, gid: &GlobalPlayerId) -> bool {
        self.remove_gid(gid)
    }

    #[must_use]
    pub fn locate_by_name(&self, name: &str) -> Option<GlobalPlayerId> {
        self.by_name.get(&normalize_player_name(name)).copied()
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KickDelivery {
    pub gid: GlobalPlayerId,
    pub local: bool,
    pub request: KickRequest,
}

#[must_use]
pub fn resolve_kick_by_name(
    directory: &PlayerDirectorySnapshot,
    target_name: &str,
    reason: String,
    issuer: String,
) -> Option<KickDelivery> {
    let gid = directory.locate_by_name(target_name)?;
    let canonical = directory
        .player_name(&gid)
        .cloned()
        .unwrap_or_else(|| target_name.to_string());
    Some(KickDelivery {
        gid,
        local: directory.is_local(&gid),
        request: KickRequest::new(gid, canonical, reason, issuer),
    })
}

#[must_use]
pub fn resolve_kick_by_id(
    directory: &PlayerDirectorySnapshot,
    target: GlobalPlayerId,
    reason: String,
    issuer: String,
) -> Option<KickDelivery> {
    let canonical = directory
        .player_name(&target)
        .cloned()
        .unwrap_or_default();
    if canonical.is_empty() && directory.player_name(&target).is_none() {
        return None;
    }
    Some(KickDelivery {
        gid: target,
        local: directory.is_local(&target),
        request: KickRequest::new(target, canonical, reason, issuer),
    })
}

#[must_use]
pub fn should_deliver_kick_locally(request: &KickRequest, local: ServerId) -> bool {
    request.target.server == local
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminInboundEffect {
    MutationApplied {
        mutation: AdminMutation,
        changed: bool,
    },
    KickDelivery(KickRequest),
    Ignored,
}

pub fn handle_control_bytes<Op, Ban>(
    op_store: &mut Op,
    ban_store: &mut Ban,
    local: ServerId,
    bytes: &[u8],
) -> Result<AdminInboundEffect, AdminSyncError>
where
    Op: OpStore,
    Ban: BanStore,
{
    let message = decode_control_message(bytes)?;
    Ok(match message {
        AdminControlMessage::Mutation(mutation) => {
            let changed = apply_mutation_to_stores(op_store, ban_store, &mutation);
            AdminInboundEffect::MutationApplied { mutation, changed }
        }
        AdminControlMessage::MutationWithAudit(audit) => {
            let mutation = audit.mutation;
            let changed = apply_mutation_to_stores(op_store, ban_store, &mutation);
            AdminInboundEffect::MutationApplied { mutation, changed }
        }
        AdminControlMessage::Kick(request) => {
            if should_deliver_kick_locally(&request, local) {
                AdminInboundEffect::KickDelivery(request)
            } else {
                AdminInboundEffect::Ignored
            }
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

    fn directory() -> PlayerDirectorySnapshot {
        let mut snapshot = PlayerDirectorySnapshot::new(ServerId(1));
        snapshot.insert(gid(1, 1), String::from("Alice"));
        snapshot.insert(gid(2, 7), String::from("Bob"));
        snapshot
    }

    fn grant() -> AdminMutation {
        AdminMutation::GrantOp(OpGrant::new([1_u8; 16], String::from("Alice"), 4, false))
    }

    fn ban() -> AdminMutation {
        AdminMutation::AddBan(BanEntry::new(
            [2_u8; 16],
            String::from("Bob"),
            String::from("op"),
            String::from("griefing"),
            100,
            None,
        ))
    }

    #[test]
    fn control_travels_on_control_kind() {
        assert_eq!(admin_control_kind(), StreamKind::Control);
    }

    #[test]
    fn mutation_roundtrips() {
        for mutation in [grant(), ban()] {
            let bytes = encode_mutation(&mutation).unwrap();
            assert_eq!(decode_mutation(&bytes).unwrap(), mutation);
        }
    }

    #[test]
    fn control_message_roundtrips() {
        let kick = KickRequest::new(gid(2, 7), String::from("Bob"), String::from("stop"), String::from("Alice"));
        let message = AdminControlMessage::Kick(kick);
        let bytes = encode_control_message(&message).unwrap();
        assert_eq!(decode_control_message(&bytes).unwrap(), message);
    }

    #[test]
    fn mutation_with_audit_roundtrips_and_applies_once() {
        let message = AdminControlMessage::MutationWithAudit(AdminAudit::new(
            String::from("Alice"),
            grant(),
        ));
        let bytes = encode_control_message(&message).unwrap();
        assert_eq!(decode_control_message(&bytes).unwrap(), message);

        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(2), &bytes).unwrap();
        assert!(matches!(
            effect,
            AdminInboundEffect::MutationApplied { changed: true, .. }
        ));
        let repeat = handle_control_bytes(&mut ops, &mut bans, ServerId(2), &bytes).unwrap();
        assert!(matches!(
            repeat,
            AdminInboundEffect::MutationApplied { changed: false, .. }
        ));
    }

    #[test]
    fn control_rejects_garbage() {
        assert!(decode_control_message(&[0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_mutation(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn op_grant_revoke_apply() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &grant()));
        assert!(!apply_mutation_to_stores(&mut ops, &mut bans, &grant()));
        let revoke = AdminMutation::RevokeOp(OpRevoke::new([1_u8; 16], "Alice".to_string()));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &revoke));
        assert!(!apply_mutation_to_stores(&mut ops, &mut bans, &revoke));
    }

    #[test]
    fn op_grants_key_by_uuid_not_name() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        let first =
            AdminMutation::GrantOp(OpGrant::new([7_u8; 16], String::from("Alice"), 4, false));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &first));
        let renamed =
            AdminMutation::GrantOp(OpGrant::new([7_u8; 16], String::from("Alicia"), 3, false));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &renamed));
        assert_eq!(ops.len(), 1);
        let namesake =
            AdminMutation::GrantOp(OpGrant::new([8_u8; 16], String::from("Alicia"), 4, false));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &namesake));
        assert_eq!(ops.len(), 2);
        let revoke = AdminMutation::RevokeOp(OpRevoke::new([7_u8; 16], "Alice".to_string()));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &revoke));
        assert!(!ops.contains(&[7_u8; 16]));
        assert!(ops.contains(&[8_u8; 16]));
    }

    #[test]
    fn ban_add_remove_apply() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &ban()));
        let revoke = AdminMutation::RemoveBan(BanRevoke::new([2_u8; 16], String::from("Bob")));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &revoke));
        assert!(!apply_mutation_to_stores(&mut ops, &mut bans, &revoke));
    }

    #[test]
    fn invalid_op_level_rejected() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        let bad = AdminMutation::GrantOp(OpGrant::new([9_u8; 16], String::from("Zed"), 9, false));
        assert!(!apply_mutation_to_stores(&mut ops, &mut bans, &bad));
        assert!(ops.is_empty());
    }

    #[test]
    fn primary_payload_roundtrips() {
        let tick = AcceptedTick {
            tick: TickStamp(12),
            payload: encode_mutation(&grant()).unwrap(),
        };
        assert_eq!(decode_primary_tick(&tick).unwrap(), grant());
    }

    #[test]
    fn primary_handle_submits_and_reports_full() {
        let (handle, _inbox) = crate::primary::primary_save_channel(1);
        submit_admin_mutation(&handle, TickStamp(1), &grant()).unwrap();
        let full = submit_admin_mutation(&handle, TickStamp(2), &grant());
        assert!(full.is_err());
    }

    #[test]
    fn parcels_fan_out_to_every_peer() {
        let parcels = mutation_parcels_for_peers(&grant(), &[2, 3]).unwrap();
        assert_eq!(parcels.len(), 2);
        assert!(parcels.iter().all(|parcel| parcel.kind == StreamKind::Control));
        assert_eq!(parcels[0].peer, 2);
        assert_eq!(parcels[1].peer, 3);
    }

    #[test]
    fn kick_routes_by_global_id() {
        let snapshot = directory();
        let local = resolve_kick_by_name(&snapshot, String::from("alice").as_str(), String::from("stop"), String::from("op")).unwrap();
        assert!(local.local);
        let remote = resolve_kick_by_name(&snapshot, "BOB", String::from("stop"), String::from("op")).unwrap();
        assert!(!remote.local);
        assert_eq!(remote.gid, gid(2, 7));
        assert!(resolve_kick_by_name(&snapshot, "Nobody", String::from("x"), String::from("op")).is_none());
        let parcel = kick_parcel_for_host(&remote.request).unwrap();
        assert_eq!(parcel.peer, 2);
        assert!(should_deliver_kick_locally(&remote.request, ServerId(2)));
        assert!(!should_deliver_kick_locally(&remote.request, ServerId(1)));
    }

    #[test]
    fn inbound_dispatch_applies_or_delivers() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        let bytes = encode_control_message(&AdminControlMessage::Mutation(grant())).unwrap();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(1), &bytes).unwrap();
        assert!(matches!(
            effect,
            AdminInboundEffect::MutationApplied { changed: true, .. }
        ));
        let kick = KickRequest::new(gid(1, 1), String::from("Alice"), String::from("stop"), String::from("op"));
        let bytes = encode_control_message(&AdminControlMessage::Kick(kick)).unwrap();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(1), &bytes).unwrap();
        assert!(matches!(effect, AdminInboundEffect::KickDelivery(_)));
        let remote_kick = KickRequest::new(gid(2, 7), String::from("Bob"), String::from("stop"), String::from("op"));
        let bytes = encode_control_message(&AdminControlMessage::Kick(remote_kick)).unwrap();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(1), &bytes).unwrap();
        assert_eq!(effect, AdminInboundEffect::Ignored);
    }

    #[test]
    fn primary_tick_replays_into_stores() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        let tick = AcceptedTick {
            tick: TickStamp(3),
            payload: encode_mutation(&ban()).unwrap(),
        };
        assert!(apply_primary_tick_to_stores(&mut ops, &mut bans, &tick).unwrap());
        assert!(bans.contains(&[2_u8; 16]));
        assert!(!apply_primary_tick_to_stores(&mut ops, &mut bans, &tick).unwrap());
    }

    #[test]
    fn ban_reason_change_counts_as_changed() {
        let mut ops = InMemoryOpStore::new();
        let mut bans = InMemoryBanStore::new();
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &ban()));
        let changed = AdminMutation::AddBan(BanEntry::new(
            [2_u8; 16],
            String::from("Bob"),
            String::from("op"),
            String::from("xray"),
            100,
            None,
        ));
        assert!(apply_mutation_to_stores(&mut ops, &mut bans, &changed));
    }

    #[test]
    fn persist_and_parcels_wires_primary_to_peers() {
        let (handle, _inbox) = crate::primary::primary_save_channel(8);
        let parcels =
            persist_and_parcels_for_peers(&handle, TickStamp(4), &grant(), &[2, 3]).unwrap();
        assert_eq!(parcels.len(), 2);
        assert_eq!(parcels[0].peer, 2);
        assert!(parcels.iter().all(|parcel| parcel.kind == StreamKind::Control));
        for parcel in &parcels {
            assert_eq!(
                decode_control_message(&parcel.bytes).unwrap(),
                AdminControlMessage::Mutation(grant())
            );
        }
    }
}
