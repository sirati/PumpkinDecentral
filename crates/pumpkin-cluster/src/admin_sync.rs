use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::sync::{mpsc, oneshot};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::primary::{AcceptedTick, PrimarySaveHandle};
use crate::protocol::StreamKind;
use crate::time::TickStamp;

pub const ADMIN_CONTROL_FRAME_BYTES: usize = 1_048_576;
pub const ADMIN_SNAPSHOT_PART_BYTES: usize = 900_000;
pub const ADMIN_SNAPSHOT_MAX_PARTS: usize = 64;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminSnapshot {
    pub revision: u64,
    pub operators: Vec<OpGrant>,
    pub banned_players: Vec<BanEntry>,
}

pub struct AdminSnapshotAssembler {
    revision: u64,
    digest: [u8; 32],
    parts: Vec<Option<Vec<u8>>>,
    received: usize,
}

impl AdminSnapshotAssembler {
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn accept(
        pending: &mut Option<Self>,
        revision: u64,
        index: u16,
        total: u16,
        digest: [u8; 32],
        bytes: Vec<u8>,
    ) -> Result<Option<AdminSnapshot>, AdminSyncError> {
        if total == 0
            || usize::from(total) > ADMIN_SNAPSHOT_MAX_PARTS
            || index >= total
            || bytes.is_empty()
            || bytes.len() > ADMIN_SNAPSHOT_PART_BYTES
        {
            return Err(AdminSyncError {
                message: String::from("invalid admin snapshot part"),
            });
        }
        if pending
            .as_ref()
            .is_some_and(|current| revision < current.revision)
        {
            return Ok(None);
        }
        if pending.as_ref().is_none_or(|current| {
            current.revision != revision
                || current.digest != digest
                || current.parts.len() != usize::from(total)
        }) {
            *pending = Some(Self {
                revision,
                digest,
                parts: vec![None; usize::from(total)],
                received: 0,
            });
        }
        let current = pending.as_mut().expect("snapshot assembler initialized");
        match &current.parts[usize::from(index)] {
            Some(existing) if existing != &bytes => {
                *pending = None;
                return Err(AdminSyncError {
                    message: String::from("conflicting admin snapshot part"),
                });
            }
            Some(_) => return Ok(None),
            None => {}
        }
        current.received += 1;
        current.parts[usize::from(index)] = Some(bytes);
        if current.received != current.parts.len() {
            return Ok(None);
        }
        let current = pending.take().expect("snapshot assembler complete");
        let mut joined = Vec::new();
        for part in current.parts {
            joined.extend(part.expect("all snapshot parts received"));
        }
        let actual: [u8; 32] = sha2::Sha256::digest(&joined).into();
        if actual != current.digest {
            return Err(AdminSyncError {
                message: String::from("admin snapshot digest mismatch"),
            });
        }
        let snapshot: AdminSnapshot =
            postcard::from_bytes(&joined).map_err(|error| AdminSyncError {
                message: format!("decode admin snapshot: {error}"),
            })?;
        if snapshot.revision != revision {
            return Err(AdminSyncError {
                message: String::from("admin snapshot revision mismatch"),
            });
        }
        Ok(Some(snapshot))
    }
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
    pub issuer_identity: Option<KickIssuerIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KickIssuerIdentity {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub permission_revision: u64,
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
            issuer_identity: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpectateRequest {
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub target_name: String,
}

impl SpectateRequest {
    #[must_use]
    pub fn new(viewer: GlobalPlayerId, target: GlobalPlayerId, target_name: String) -> Self {
        Self {
            viewer,
            target,
            target_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpectateLocation {
    pub target: GlobalPlayerId,
    pub target_name: String,
    pub host: ServerId,
}

impl SpectateLocation {
    #[must_use]
    pub fn new(target: GlobalPlayerId, target_name: String, host: ServerId) -> Self {
        Self {
            target,
            target_name,
            host,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminControlMessage {
    Mutation(AdminMutation),
    Snapshot(AdminSnapshot),
    SnapshotPart {
        revision: u64,
        index: u16,
        total: u16,
        digest: [u8; 32],
        bytes: Vec<u8>,
    },
    SnapshotRequest,
    /// A state mutation together with the one operator-visible notification
    /// that belongs to it. Keeping them in one reliable control parcel means
    /// a peer cannot display a success before it has applied the change.
    MutationWithAudit(AdminAudit),
    VersionedMutationWithAudit {
        revision: u64,
        audit: AdminAudit,
    },
    SequencedMutation {
        id: [u8; 16],
        audit: AdminAudit,
    },
    MutationApplied {
        id: [u8; 16],
    },
    Kick(KickRequest),
    Spectate(SpectateRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAuthority {
    Primary,
    Secondary,
}

#[must_use]
pub fn authorized_control_message(
    authority: AdminAuthority,
    local: ServerId,
    source: ServerId,
    parcel_generation: u64,
    current_generation: Option<u64>,
    source_is_primary: bool,
    message: &AdminControlMessage,
) -> bool {
    if source == local || current_generation != Some(parcel_generation) {
        return false;
    }
    match message {
        AdminControlMessage::Mutation(_)
        | AdminControlMessage::MutationWithAudit(_)
        | AdminControlMessage::VersionedMutationWithAudit { .. }
        | AdminControlMessage::Snapshot(_)
        | AdminControlMessage::SnapshotPart { .. } => {
            matches!(authority, AdminAuthority::Secondary) && source_is_primary
        }
        AdminControlMessage::SnapshotRequest => matches!(authority, AdminAuthority::Primary),
        AdminControlMessage::SequencedMutation { .. } => {
            matches!(authority, AdminAuthority::Primary)
        }
        AdminControlMessage::MutationApplied { .. } => {
            matches!(authority, AdminAuthority::Secondary) && source_is_primary
        }
        AdminControlMessage::Kick(request) => {
            request.target.server == local
                && match request.issuer_identity.as_ref() {
                    Some(issuer) => issuer.gid.server == source,
                    None => source_is_primary,
                }
        }
        AdminControlMessage::Spectate(request) => {
            request.target.server == local && request.viewer.server == source
        }
    }
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

pub fn encode_control_message(message: &AdminControlMessage) -> Result<Vec<u8>, AdminSyncError> {
    postcard::to_allocvec(message).map_err(|error| AdminSyncError {
        message: format!("encode admin control: {error}"),
    })
}

pub fn encode_admin_snapshot_frames(
    snapshot: &AdminSnapshot,
) -> Result<VecDeque<Vec<u8>>, AdminSyncError> {
    let raw = postcard::to_allocvec(snapshot).map_err(|error| AdminSyncError {
        message: format!("encode admin snapshot: {error}"),
    })?;
    if raw.len() <= ADMIN_CONTROL_FRAME_BYTES {
        let whole = encode_control_message(&AdminControlMessage::Snapshot(snapshot.clone()))?;
        if whole.len() <= ADMIN_CONTROL_FRAME_BYTES {
            return Ok(VecDeque::from([whole]));
        }
    }
    let total = raw.len().div_ceil(ADMIN_SNAPSHOT_PART_BYTES);
    if total == 0 || total > ADMIN_SNAPSHOT_MAX_PARTS {
        return Err(AdminSyncError {
            message: format!("admin snapshot requires {total} parts"),
        });
    }
    let digest: [u8; 32] = sha2::Sha256::digest(&raw).into();
    let mut frames = VecDeque::with_capacity(total);
    for (index, part) in raw.chunks(ADMIN_SNAPSHOT_PART_BYTES).enumerate() {
        let frame = encode_control_message(&AdminControlMessage::SnapshotPart {
            revision: snapshot.revision,
            index: index as u16,
            total: total as u16,
            digest,
            bytes: part.to_vec(),
        })?;
        if frame.len() > ADMIN_CONTROL_FRAME_BYTES {
            return Err(AdminSyncError {
                message: String::from("admin snapshot part exceeds control frame bound"),
            });
        }
        frames.push_back(frame);
    }
    Ok(frames)
}

pub fn decode_control_message(bytes: &[u8]) -> Result<AdminControlMessage, AdminSyncError> {
    if bytes.len() > ADMIN_CONTROL_FRAME_BYTES {
        return Err(AdminSyncError {
            message: String::from("admin control frame exceeds maximum length"),
        });
    }
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

pub fn spectate_parcel_for_host(request: &SpectateRequest) -> Result<AdminParcel, AdminSyncError> {
    let bytes = encode_control_message(&AdminControlMessage::Spectate(request.clone()))?;
    Ok(AdminParcel::new(
        request.target.server.0,
        admin_control_kind(),
        bytes,
    ))
}

pub async fn broadcast_control_message(
    tx: &mpsc::Sender<AdminParcel>,
    message: &AdminControlMessage,
    peers: &[u16],
) -> Result<usize, AdminSyncError> {
    let bytes = encode_control_message(message)?;
    let mut sent = 0_usize;
    for peer in peers {
        let parcel = AdminParcel::new(*peer, admin_control_kind(), bytes.clone());
        if tx.send(parcel).await.is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
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
    handle
        .try_submit(tick, payload)
        .map_err(|_| AdminSyncError {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpectateResolution {
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub remote: bool,
    pub location: SpectateLocation,
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
    let canonical = directory.player_name(&target).cloned().unwrap_or_default();
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
pub fn resolve_spectate_by_name(
    directory: &PlayerDirectorySnapshot,
    viewer: GlobalPlayerId,
    target_name: &str,
) -> Option<SpectateResolution> {
    let target = directory.locate_by_name(target_name)?;
    if target == viewer {
        return None;
    }
    let canonical = directory
        .player_name(&target)
        .cloned()
        .unwrap_or_else(|| target_name.to_string());
    Some(SpectateResolution {
        viewer,
        target,
        remote: !directory.is_local(&target),
        location: SpectateLocation::new(target, canonical, directory.host_of(&target)),
    })
}

#[must_use]
pub fn resolve_spectate_by_id(
    directory: &PlayerDirectorySnapshot,
    viewer: GlobalPlayerId,
    target: GlobalPlayerId,
) -> Option<SpectateResolution> {
    if target == viewer {
        return None;
    }
    let canonical = directory.player_name(&target)?.clone();
    Some(SpectateResolution {
        viewer,
        target,
        remote: !directory.is_local(&target),
        location: SpectateLocation::new(target, canonical, directory.host_of(&target)),
    })
}

#[must_use]
pub fn should_deliver_kick_locally(request: &KickRequest, local: ServerId) -> bool {
    request.target.server == local
}

#[must_use]
pub fn should_deliver_spectate_locally(request: &SpectateRequest, local: ServerId) -> bool {
    request.target.server == local
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminInboundEffect {
    MutationApplied {
        mutation: AdminMutation,
        changed: bool,
    },
    KickDelivery(KickRequest),
    SpectateDelivery(SpectateRequest),
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
        AdminControlMessage::VersionedMutationWithAudit { audit, .. } => {
            let mutation = audit.mutation;
            let changed = apply_mutation_to_stores(op_store, ban_store, &mutation);
            AdminInboundEffect::MutationApplied { mutation, changed }
        }
        AdminControlMessage::SequencedMutation { audit, .. } => {
            let mutation = audit.mutation;
            let changed = apply_mutation_to_stores(op_store, ban_store, &mutation);
            AdminInboundEffect::MutationApplied { mutation, changed }
        }
        AdminControlMessage::Snapshot(_)
        | AdminControlMessage::SnapshotPart { .. }
        | AdminControlMessage::SnapshotRequest
        | AdminControlMessage::MutationApplied { .. } => AdminInboundEffect::Ignored,
        AdminControlMessage::Kick(request) => {
            if should_deliver_kick_locally(&request, local) {
                AdminInboundEffect::KickDelivery(request)
            } else {
                AdminInboundEffect::Ignored
            }
        }
        AdminControlMessage::Spectate(request) => {
            if should_deliver_spectate_locally(&request, local) {
                AdminInboundEffect::SpectateDelivery(request)
            } else {
                AdminInboundEffect::Ignored
            }
        }
    })
}

#[derive(Debug)]
pub struct SpectateQuery {
    pub viewer: GlobalPlayerId,
    pub target_name: String,
    pub reply: oneshot::Sender<Option<SpectateLocation>>,
}

impl SpectateQuery {
    #[must_use]
    pub fn new(
        viewer: GlobalPlayerId,
        target_name: String,
        reply: oneshot::Sender<Option<SpectateLocation>>,
    ) -> Self {
        Self {
            viewer,
            target_name,
            reply,
        }
    }
}

pub fn answer_spectate_query(directory: &PlayerDirectorySnapshot, query: SpectateQuery) {
    let SpectateQuery {
        viewer,
        target_name,
        reply,
    } = query;
    let location =
        resolve_spectate_by_name(directory, viewer, &target_name).map(|found| found.location);
    let _ = reply.send(location);
}

pub async fn query_spectate_location(
    tx: &mpsc::Sender<SpectateQuery>,
    viewer: GlobalPlayerId,
    target_name: String,
) -> Option<SpectateLocation> {
    let (reply_tx, reply_rx) = oneshot::channel();
    let query = SpectateQuery::new(viewer, target_name, reply_tx);
    if tx.send(query).await.is_err() {
        return None;
    }
    match reply_rx.await {
        Ok(location) => location,
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
        let kick = KickRequest::new(
            gid(2, 7),
            String::from("Bob"),
            String::from("stop"),
            String::from("Alice"),
        );
        let message = AdminControlMessage::Kick(kick);
        let bytes = encode_control_message(&message).unwrap();
        assert_eq!(decode_control_message(&bytes).unwrap(), message);
    }

    #[test]
    fn mutation_with_audit_roundtrips_and_applies_once() {
        let message =
            AdminControlMessage::MutationWithAudit(AdminAudit::new(String::from("Alice"), grant()));
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
        assert!(
            parcels
                .iter()
                .all(|parcel| parcel.kind == StreamKind::Control)
        );
        assert_eq!(parcels[0].peer, 2);
        assert_eq!(parcels[1].peer, 3);
    }

    #[test]
    fn kick_routes_by_global_id() {
        let snapshot = directory();
        let local = resolve_kick_by_name(
            &snapshot,
            String::from("alice").as_str(),
            String::from("stop"),
            String::from("op"),
        )
        .unwrap();
        assert!(local.local);
        let remote =
            resolve_kick_by_name(&snapshot, "BOB", String::from("stop"), String::from("op"))
                .unwrap();
        assert!(!remote.local);
        assert_eq!(remote.gid, gid(2, 7));
        assert!(
            resolve_kick_by_name(&snapshot, "Nobody", String::from("x"), String::from("op"))
                .is_none()
        );
        let parcel = kick_parcel_for_host(&remote.request).unwrap();
        assert_eq!(parcel.peer, 2);
        assert!(should_deliver_kick_locally(&remote.request, ServerId(2)));
        assert!(!should_deliver_kick_locally(&remote.request, ServerId(1)));
    }

    #[test]
    fn spectate_resolves_live_location() {
        let snapshot = directory();
        let viewer = gid(1, 1);
        let remote = resolve_spectate_by_name(&snapshot, viewer, "bob").unwrap();
        assert!(remote.remote);
        assert_eq!(remote.location.host, ServerId(2));
        assert_eq!(remote.location.target, gid(2, 7));
        assert!(resolve_spectate_by_name(&snapshot, viewer, "alice").is_none());
        let by_id = resolve_spectate_by_id(&snapshot, viewer, gid(2, 7)).unwrap();
        assert_eq!(by_id.location.target_name, String::from("Bob"));
        assert!(resolve_spectate_by_id(&snapshot, viewer, gid(9, 9)).is_none());
        let request = SpectateRequest::new(viewer, gid(2, 7), String::from("Bob"));
        assert!(should_deliver_spectate_locally(&request, ServerId(2)));
        assert!(!should_deliver_spectate_locally(&request, ServerId(1)));
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
        let kick = KickRequest::new(
            gid(1, 1),
            String::from("Alice"),
            String::from("stop"),
            String::from("op"),
        );
        let bytes = encode_control_message(&AdminControlMessage::Kick(kick)).unwrap();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(1), &bytes).unwrap();
        assert!(matches!(effect, AdminInboundEffect::KickDelivery(_)));
        let remote_kick = KickRequest::new(
            gid(2, 7),
            String::from("Bob"),
            String::from("stop"),
            String::from("op"),
        );
        let bytes = encode_control_message(&AdminControlMessage::Kick(remote_kick)).unwrap();
        let effect = handle_control_bytes(&mut ops, &mut bans, ServerId(1), &bytes).unwrap();
        assert_eq!(effect, AdminInboundEffect::Ignored);
    }

    #[test]
    fn full_admin_snapshot_is_primary_only_and_bounded_before_decode() {
        let admin_snapshot = AdminSnapshot {
            revision: 17,
            operators: vec![OpGrant::new([1; 16], String::from("Alice"), 4, false)],
            banned_players: Vec::new(),
        };
        let frames = encode_admin_snapshot_frames(&admin_snapshot).unwrap();
        assert_eq!(frames.len(), 1);
        let encoded = frames[0].clone();
        let snapshot = AdminControlMessage::Snapshot(admin_snapshot);
        assert_eq!(decode_control_message(&encoded).unwrap(), snapshot);
        assert!(authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(2),
            ServerId(1),
            3,
            Some(3),
            true,
            &snapshot,
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(2),
            ServerId(3),
            3,
            Some(3),
            false,
            &snapshot,
        ));
        assert!(decode_control_message(&vec![0; 1_048_577]).is_err());
    }

    #[test]
    fn multipart_admin_snapshot_converges_out_of_order_and_rejects_corruption() {
        let snapshot = AdminSnapshot {
            revision: 23,
            operators: Vec::new(),
            banned_players: (0_u16..400)
                .map(|index| {
                    let mut uuid = [0; 16];
                    uuid[..2].copy_from_slice(&index.to_le_bytes());
                    BanEntry::new(
                        uuid,
                        format!("Player{index}"),
                        String::from("console"),
                        "x".repeat(4_000),
                        0,
                        None,
                    )
                })
                .collect(),
        };
        let raw = postcard::to_allocvec(&snapshot).unwrap();
        assert!(raw.len() > ADMIN_CONTROL_FRAME_BYTES);
        let frames = encode_admin_snapshot_frames(&snapshot).unwrap();
        assert_eq!(frames.len(), 2);
        let parts: Vec<_> = frames
            .into_iter()
            .map(|frame| {
                assert!(frame.len() <= ADMIN_CONTROL_FRAME_BYTES);
                match decode_control_message(&frame).unwrap() {
                    AdminControlMessage::SnapshotPart {
                        revision,
                        index,
                        total,
                        digest,
                        bytes,
                    } => (revision, index, total, digest, bytes),
                    _ => panic!("large snapshot did not use multipart control frames"),
                }
            })
            .collect();
        let mut pending = None;
        assert!(
            AdminSnapshotAssembler::accept(
                &mut pending,
                parts[1].0,
                parts[1].1,
                parts[1].2,
                parts[1].3,
                parts[1].4.clone(),
            )
            .unwrap()
            .is_none()
        );
        let mut corrupted = parts[0].4.clone();
        corrupted[0] ^= 1;
        assert!(
            AdminSnapshotAssembler::accept(
                &mut pending,
                parts[0].0,
                parts[0].1,
                parts[0].2,
                parts[0].3,
                corrupted,
            )
            .is_err()
        );
        assert!(pending.is_none());
        assert!(
            AdminSnapshotAssembler::accept(
                &mut pending,
                parts[1].0,
                parts[1].1,
                parts[1].2,
                parts[1].3,
                parts[1].4.clone(),
            )
            .unwrap()
            .is_none()
        );
        let recovered = AdminSnapshotAssembler::accept(
            &mut pending,
            parts[0].0,
            parts[0].1,
            parts[0].2,
            parts[0].3,
            parts[0].4.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(recovered, snapshot);
    }

    #[test]
    fn control_authority_tracks_connection_generation_and_role() {
        let mutation = AdminControlMessage::Mutation(grant());
        assert!(!authorized_control_message(
            AdminAuthority::Primary,
            ServerId(1),
            ServerId(2),
            7,
            Some(7),
            false,
            &mutation,
        ));
        let sequenced = AdminControlMessage::SequencedMutation {
            id: [3; 16],
            audit: AdminAudit::new(String::from("issuer"), grant()),
        };
        assert!(authorized_control_message(
            AdminAuthority::Primary,
            ServerId(1),
            ServerId(2),
            7,
            Some(7),
            false,
            &sequenced,
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(2),
            7,
            Some(7),
            false,
            &mutation,
        ));
        assert!(authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(1),
            9,
            Some(9),
            true,
            &mutation,
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(1),
            8,
            Some(9),
            true,
            &mutation,
        ));

        let unauthenticated = AdminControlMessage::Kick(KickRequest::new(
            gid(3, 4),
            String::from("Carol"),
            String::from("stop"),
            String::from("Alice"),
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(2),
            4,
            Some(4),
            false,
            &unauthenticated,
        ));
        assert!(authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(1),
            4,
            Some(4),
            true,
            &unauthenticated,
        ));
        let mut kick = unauthenticated;
        if let AdminControlMessage::Kick(request) = &mut kick {
            request.issuer_identity = Some(KickIssuerIdentity {
                gid: gid(2, 1),
                uuid: [8; 16],
                permission_revision: 3,
            });
        }
        assert!(authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(2),
            4,
            Some(4),
            false,
            &kick,
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(1),
            4,
            Some(4),
            false,
            &kick,
        ));

        let forged = AdminControlMessage::Spectate(SpectateRequest::new(
            gid(4, 1),
            gid(3, 4),
            String::from("Carol"),
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(2),
            4,
            Some(4),
            false,
            &forged,
        ));

        let ack = AdminControlMessage::MutationApplied { id: [7; 16] };
        assert!(authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(1),
            9,
            Some(9),
            true,
            &ack,
        ));
        assert!(!authorized_control_message(
            AdminAuthority::Secondary,
            ServerId(3),
            ServerId(2),
            9,
            Some(9),
            false,
            &ack,
        ));
    }

    #[tokio::test]
    async fn broadcast_sends_to_all_peers() {
        let (tx, mut rx) = mpsc::channel(8);
        let count =
            broadcast_control_message(&tx, &AdminControlMessage::Mutation(grant()), &[2, 3])
                .await
                .unwrap();
        assert_eq!(count, 2);
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn spectate_query_roundtrips_without_locks() {
        let snapshot = directory();
        let (tx, mut rx) = mpsc::channel(4);
        let viewer = gid(1, 1);
        let task = tokio::spawn(async move {
            let Some(query) = rx.recv().await else {
                return;
            };
            answer_spectate_query(&snapshot, query);
        });
        let found = query_spectate_location(&tx, viewer, String::from("Bob"))
            .await
            .unwrap();
        assert_eq!(found.host, ServerId(2));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn spectate_query_missing_sender_returns_none() {
        let (tx, rx) = mpsc::channel::<SpectateQuery>(1);
        drop(rx);
        let found = query_spectate_location(&tx, gid(1, 1), String::from("Bob")).await;
        assert!(found.is_none());
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
        assert!(
            parcels
                .iter()
                .all(|parcel| parcel.kind == StreamKind::Control)
        );
        for parcel in &parcels {
            assert_eq!(
                decode_control_message(&parcel.bytes).unwrap(),
                AdminControlMessage::Mutation(grant())
            );
        }
    }
}
