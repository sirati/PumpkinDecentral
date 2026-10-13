use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::inventory::{
    INV_ARMOR, INV_MAIN, INV_OFFHAND, InvLoc, InventoryCell, InventoryOp, InventoryStateSnapshot,
    SemanticInventoryOp, capture_inventory_transaction,
};
use crate::protocol::StreamKind;
use crate::time::TickStamp;

pub const INVSEE_MAX_SLOTS: usize = 48;
pub const INVSEE_MAX_NAME_CHARS: usize = 16;
pub const INVSEE_MAX_SLOT_NBT_BYTES: usize = 24 * 1024;
pub const INVSEE_MAX_CONTROL_BYTES: usize = 3 * 1024 * 1024;
pub const INVSEE_REQUEST_TIMEOUT_MS: u64 = 1500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvseeError {
    pub message: String,
}

impl core::fmt::Display for InvseeError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for InvseeError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeSlot {
    pub index: u16,
    pub item_id: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

impl InvseeSlot {
    #[must_use]
    pub fn new(index: u16, item_id: u16, count: u8, nbt: Vec<u8>) -> Self {
        Self {
            index,
            item_id,
            count,
            nbt,
        }
    }

    #[must_use]
    pub const fn is_empty_slot(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryPreviewAuthority {
    pub provider: crate::realm_completion::PeerGeneration,
    pub cut: crate::time::TickStamp,
    pub frontiers: Vec<(
        crate::action_graph::InventoryRealmId,
        crate::action_graph::PlayerInventoryGroundFrontier,
    )>,
    pub living: crate::canonical_action::Cell,
    pub living_ground_anchor: crate::action_graph::GroundAnchor,
    pub inventory_cells:
        std::collections::BTreeMap<crate::inventory::InvLoc, crate::canonical_action::Cell>,
}

impl InventoryPreviewAuthority {
    pub fn is_valid(
        &self,
        lease: crate::canonical_action::LivePlayerLease,
        affected: &[crate::protocol::ChunkKey],
    ) -> bool {
        use crate::action_graph::InventoryRealmId;
        matches!(&self.living.value, crate::canonical_action::Value::Living(living)
            if living.combat.equipment.is_valid() && living.avatar.as_ref().is_some_and(|avatar| avatar.is_valid() && affected.first().is_some_and(|chunk| avatar.world == chunk.world)))
            && !self.living_ground_anchor.settled_through.is_after(self.cut)
            && self.living_ground_anchor.value.as_ref().is_some_and(|cell| matches!(&cell.value,
                crate::canonical_action::Value::Living(living) if living.avatar.as_ref().is_some_and(|avatar| avatar.is_valid()
                    && affected.first().is_some_and(|chunk| avatar.world == chunk.world))))
            && self.provider.server == lease.gid.server && self.provider.generation != 0
            && self.inventory_cells.len() == 43 && self.inventory_cells.iter().all(|(loc, cell)| {
                matches!(&cell.value, crate::canonical_action::Value::Inventory(value) if value.loc == *loc
                    && value.count <= crate::inventory::INV_MAX_STACK
                    && value.nbt.len() <= INVSEE_MAX_SLOT_NBT_BYTES
                    && ((value.item == 0 && value.count == 0 && value.nbt.is_empty())
                        || (value.item != 0 && value.count != 0))
                    && match loc.inv {
                        crate::inventory::INV_MAIN => loc.slot < 36,
                        crate::inventory::INV_ARMOR => loc.slot < 4,
                        crate::inventory::INV_OFFHAND | crate::inventory::INV_CURSOR | crate::inventory::INV_SELECTED => loc.slot == 0,
                        _ => false,
                    })
            })
            && affected.len() == 1 && self.frontiers.len() == 4
            && [crate::inventory::INV_MAIN, crate::inventory::INV_ARMOR, crate::inventory::INV_OFFHAND, crate::inventory::INV_SELECTED]
                .into_iter().all(|inv| self.frontiers.iter().filter(|(realm, frontier)| {
                    *realm == InventoryRealmId::Player(inv) && frontier.lease == lease
                        && frontier.accepted_chunk == affected[0] && !frontier.settled_through.is_after(self.cut)
                }).count() == 1)
    }

    pub fn matches_projection(&self, snapshot: &InventorySnapshot) -> bool {
        use crate::inventory::{INV_ARMOR, INV_MAIN, INV_OFFHAND, INV_SELECTED, InvLoc};
        let mut seen = std::collections::BTreeSet::new();
        if !snapshot.slots.iter().all(|slot| {
            let loc = match slot.index {
                0..=35 => InvLoc::new(INV_MAIN, slot.index),
                36..=39 => InvLoc::new(INV_ARMOR, slot.index - 36),
                40 => InvLoc::new(INV_OFFHAND, 0),
                _ => return false,
            };
            seen.insert(loc) && self.inventory_cells.get(&loc).is_some_and(|cell| {
                matches!(&cell.value, crate::canonical_action::Value::Inventory(value)
                    if value.item == slot.item_id && value.count == slot.count && value.nbt == slot.nbt)
            })
        }) { return false; }
        self.inventory_cells.iter().all(|(loc, cell)| {
            if !matches!(loc.inv, INV_MAIN | INV_ARMOR | INV_OFFHAND) || seen.contains(loc) { return true; }
            matches!(&cell.value, crate::canonical_action::Value::Inventory(value) if value.is_empty())
        }) && snapshot.selected < 9 && self.inventory_cells.get(&InvLoc::new(INV_SELECTED, 0)).is_some_and(|cell| {
            matches!(&cell.value, crate::canonical_action::Value::Inventory(value)
                if value.item == u16::from(snapshot.selected) + 1 && value.count == 1 && value.nbt.is_empty())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventorySnapshot {
    pub target: GlobalPlayerId,
    pub incarnation: u64,
    pub lease: Option<crate::canonical_action::LivePlayerLease>,
    pub authority: Option<InventoryPreviewAuthority>,
    pub owner_name: String,
    pub revision: u64,
    pub selected: u8,
    pub slots: Vec<InvseeSlot>,
    pub affected: Vec<crate::protocol::ChunkKey>,
}

impl InventorySnapshot {
    #[must_use]
    pub fn new(
        target: GlobalPlayerId,
        owner_name: String,
        revision: u64,
        selected: u8,
        slots: Vec<InvseeSlot>,
    ) -> Self {
        let capped_name: String = owner_name.chars().take(INVSEE_MAX_NAME_CHARS).collect();
        let mut capped_slots: Vec<_> = slots.into_iter().filter(valid_slot).collect();
        capped_slots.truncate(INVSEE_MAX_SLOTS);
        Self {
            target,
            incarnation: 0,
            lease: None,
            authority: None,
            owner_name: capped_name,
            revision,
            selected,
            slots: capped_slots,
            affected: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_affected(mut self, mut affected: Vec<crate::protocol::ChunkKey>) -> Self {
        affected.sort_unstable();
        affected.dedup();
        self.affected = affected;
        self
    }

    #[must_use]
    pub fn with_incarnation(mut self, incarnation: u64) -> Self {
        self.incarnation = incarnation;
        self
    }

    #[must_use]
    pub fn with_lease(mut self, lease: crate::canonical_action::LivePlayerLease) -> Self {
        self.incarnation = lease.incarnation;
        self.lease = Some(lease);
        self
    }

    #[must_use]
    pub fn with_authority(mut self, authority: InventoryPreviewAuthority) -> Self {
        self.authority = Some(authority);
        self
    }

    #[must_use]
    pub fn slot(&self, index: u16) -> Option<&InvseeSlot> {
        self.slots.iter().find(|slot| slot.index == index)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeMutation {
    pub before: InvseeSlot,
    pub after: InvseeSlot,
}

impl InvseeMutation {
    #[must_use]
    pub const fn new(before: InvseeSlot, after: InvseeSlot) -> Self {
        Self { before, after }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeRequest {
    pub request_id: u64,
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub target_name: String,
    pub editable: bool,
}

impl InvseeRequest {
    #[must_use]
    pub fn new(
        request_id: u64,
        viewer: GlobalPlayerId,
        target: GlobalPlayerId,
        target_name: String,
        editable: bool,
    ) -> Self {
        let capped_name: String = target_name.chars().take(INVSEE_MAX_NAME_CHARS).collect();
        Self {
            request_id,
            viewer,
            target,
            target_name: capped_name,
            editable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvseeOutcome {
    Found(InventorySnapshot),
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeResponse {
    pub request_id: u64,
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub outcome: InvseeOutcome,
}

impl InvseeResponse {
    #[must_use]
    pub const fn new(
        request_id: u64,
        viewer: GlobalPlayerId,
        target: GlobalPlayerId,
        outcome: InvseeOutcome,
    ) -> Self {
        Self {
            request_id,
            viewer,
            target,
            outcome,
        }
    }

    #[must_use]
    pub fn found(request: &InvseeRequest, snapshot: InventorySnapshot) -> Self {
        Self::new(
            request.request_id,
            request.viewer,
            request.target,
            InvseeOutcome::Found(snapshot),
        )
    }

    #[must_use]
    pub fn offline(request: &InvseeRequest) -> Self {
        Self::new(
            request.request_id,
            request.viewer,
            request.target,
            InvseeOutcome::Offline,
        )
    }

    #[must_use]
    pub const fn snapshot(&self) -> Option<&InventorySnapshot> {
        match &self.outcome {
            InvseeOutcome::Found(snapshot) => Some(snapshot),
            InvseeOutcome::Offline => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeWrite {
    pub request_id: u64,
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub expected_revision: u64,
    pub mutations: Vec<InvseeMutation>,
    pub affected: Vec<crate::protocol::ChunkKey>,
}

impl InvseeWrite {
    #[must_use]
    pub fn new(
        viewer: GlobalPlayerId,
        target: GlobalPlayerId,
        expected_revision: u64,
        mutations: Vec<InvseeMutation>,
    ) -> Self {
        let mut capped: Vec<_> = mutations
            .into_iter()
            .filter(|mutation| {
                mutation.before.index == mutation.after.index
                    && valid_slot(&mutation.before)
                    && valid_slot(&mutation.after)
            })
            .collect();
        capped.truncate(INVSEE_MAX_SLOTS);
        Self {
            request_id: 0,
            viewer,
            target,
            expected_revision,
            mutations: capped,
            affected: Vec::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn replacement(
        viewer: GlobalPlayerId,
        snapshot: &InventorySnapshot,
        desired: Vec<InvseeSlot>,
    ) -> Result<Self, InvseeError> {
        if desired.len() > INVSEE_MAX_SLOTS || desired.iter().any(|slot| !valid_slot(slot)) {
            return Err(InvseeError {
                message: String::from("invalid replacement inventory"),
            });
        }
        let mut before: HashMap<_, _> = snapshot
            .slots
            .iter()
            .cloned()
            .map(|slot| (slot.index, slot))
            .collect();
        let mut after = HashMap::with_capacity(desired.len());
        for slot in desired {
            if after.insert(slot.index, slot).is_some() {
                return Err(InvseeError {
                    message: String::from("duplicate replacement inventory slot"),
                });
            }
        }
        let mut indices: Vec<_> = before.keys().chain(after.keys()).copied().collect();
        indices.sort_unstable();
        indices.dedup();
        let mutations = indices
            .into_iter()
            .filter_map(|index| {
                let before = before
                    .remove(&index)
                    .unwrap_or_else(|| InvseeSlot::new(index, 0, 0, Vec::new()));
                let after = after
                    .remove(&index)
                    .unwrap_or_else(|| InvseeSlot::new(index, 0, 0, Vec::new()));
                (before != after).then(|| InvseeMutation::new(before, after))
            })
            .collect();
        let mut write = Self::new(viewer, snapshot.target, snapshot.revision, mutations);
        write.affected = snapshot.affected.clone();
        Ok(write)
    }

    pub fn inventory_action(
        &self,
        seq: crate::identity::PlayerSeq,
        tick: TickStamp,
        affected: Vec<crate::protocol::ChunkKey>,
    ) -> Result<InventoryOp, InvseeError> {
        let mutation = self.inventory_mutation()?;
        let actor_chunk = affected.first().copied().ok_or_else(|| InvseeError {
            message: "inventory action has no actor chunk".to_string(),
        })?;
        Ok(capture_inventory_transaction(
            self.viewer,
            self.target,
            seq,
            tick,
            self.expected_revision,
            mutation.operations,
            actor_chunk,
            affected,
        ))
    }

    pub fn inventory_mutation(&self) -> Result<crate::inventory::InventoryMutation, InvseeError> {
        if self.mutations.is_empty() {
            return Err(InvseeError {
                message: String::from("empty inventory action"),
            });
        }
        let changes = self
            .mutations
            .iter()
            .map(|mutation| {
                let Some(loc) = player_inventory_loc(mutation.before.index) else {
                    return Err(InvseeError {
                        message: String::from("invalid player inventory slot"),
                    });
                };
                Ok(SemanticInventoryOp {
                    target: crate::inventory::InventoryTarget::Player(self.target),
                    before: InventoryCell {
                        loc,
                        item: mutation.before.item_id,
                        count: mutation.before.count,
                        nbt: mutation.before.nbt.clone(),
                    },
                    after: InventoryCell {
                        loc,
                        item: mutation.after.item_id,
                        count: mutation.after.count,
                        nbt: mutation.after.nbt.clone(),
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crate::inventory::InventoryMutation {
            expected_revisions: vec![(
                crate::inventory::InventoryTarget::Player(self.target),
                self.expected_revision,
            )],
            operations: changes,
        })
    }
}

pub fn player_inventory_loc(index: u16) -> Option<InvLoc> {
    match index {
        0..=35 => Some(InvLoc::new(INV_MAIN, index)),
        36..=39 => Some(InvLoc::new(INV_ARMOR, index - 36)),
        40 => Some(InvLoc::new(INV_OFFHAND, 0)),
        _ => None,
    }
}

#[must_use]
pub fn inventory_snapshot_from_state(
    state: InventoryStateSnapshot,
    owner_name: String,
    selected: u8,
) -> InventorySnapshot {
    let slots = state
        .cells
        .into_iter()
        .filter_map(|cell| {
            let index = match (cell.loc.inv, cell.loc.slot) {
                (INV_MAIN, slot @ 0..=35) => slot,
                (INV_ARMOR, slot @ 0..=3) => slot + 36,
                (INV_OFFHAND, 0) => 40,
                _ => return None,
            };
            Some(InvseeSlot::new(index, cell.item, cell.count, cell.nbt))
        })
        .collect();
    let crate::inventory::InventoryTarget::Player(target) = state.target else {
        unreachable!()
    };
    InventorySnapshot::new(target, owner_name, state.revision, selected, slots)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvseeControl {
    Request(InvseeRequest),
    Response(InvseeResponse),
    Write(InvseeWrite),
    Subscribe(InvseeSubscription),
    Unsubscribe(InvseeSubscription),
    Invalidated(InvseeInvalidation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InvseeSubscription {
    pub viewer: GlobalPlayerId,
    pub session: u64,
    pub target: GlobalPlayerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvseeInvalidation {
    pub subscription: InvseeSubscription,
    pub revision: Option<u64>,
    pub force_refresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvseeParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

impl InvseeParcel {
    #[must_use]
    pub fn new(peer: u16, kind: StreamKind, bytes: Vec<u8>) -> Self {
        Self { peer, kind, bytes }
    }
}

#[must_use]
pub const fn invsee_control_kind() -> StreamKind {
    StreamKind::Control
}

pub fn encode_control(message: &InvseeControl) -> Result<Vec<u8>, InvseeError> {
    postcard::to_allocvec(message).map_err(|error| InvseeError {
        message: format!("encode invsee control: {error}"),
    })
}

pub fn decode_control(bytes: &[u8]) -> Result<InvseeControl, InvseeError> {
    if bytes.len() > INVSEE_MAX_CONTROL_BYTES {
        return Err(InvseeError {
            message: String::from("invsee control exceeds size limit"),
        });
    }
    let message: InvseeControl = postcard::from_bytes(bytes).map_err(|error| InvseeError {
        message: format!("decode invsee control: {error}"),
    })?;
    if !valid_control(&message) {
        return Err(InvseeError {
            message: String::from("invalid invsee control payload"),
        });
    }
    Ok(message)
}

fn valid_slot(slot: &InvseeSlot) -> bool {
    usize::from(slot.index) < INVSEE_MAX_SLOTS
        && slot.count <= crate::inventory::INV_MAX_STACK
        && slot.nbt.len() <= INVSEE_MAX_SLOT_NBT_BYTES
        && ((slot.item_id == 0 && slot.count == 0 && slot.nbt.is_empty())
            || (slot.item_id != 0 && slot.count != 0))
}

fn valid_control(message: &InvseeControl) -> bool {
    match message {
        InvseeControl::Request(request) => {
            request.target_name.chars().count() <= INVSEE_MAX_NAME_CHARS
        }
        InvseeControl::Response(response) => match &response.outcome {
            InvseeOutcome::Found(snapshot) => {
                snapshot.owner_name.chars().count() <= INVSEE_MAX_NAME_CHARS
                    && snapshot.lease.is_none_or(|lease| {
                        lease.gid == snapshot.target
                            && lease.incarnation != 0
                            && lease.incarnation == snapshot.incarnation
                    })
                    && snapshot.authority.as_ref().is_none_or(|authority| {
                        snapshot
                            .lease
                            .is_some_and(|lease| authority.is_valid(lease, &snapshot.affected))
                            && authority.matches_projection(snapshot)
                    })
                    && snapshot.slots.len() <= INVSEE_MAX_SLOTS
                    && snapshot.slots.iter().all(valid_slot)
            }
            InvseeOutcome::Offline => true,
        },
        InvseeControl::Write(write) => {
            if write.mutations.len() > INVSEE_MAX_SLOTS {
                return false;
            }
            let mut seen = [false; INVSEE_MAX_SLOTS];
            write.mutations.iter().all(|mutation| {
                let index = usize::from(mutation.before.index);
                mutation.before.index == mutation.after.index
                    && valid_slot(&mutation.before)
                    && valid_slot(&mutation.after)
                    && !std::mem::replace(&mut seen[index], true)
            })
        }
        InvseeControl::Subscribe(subscription) | InvseeControl::Unsubscribe(subscription) => {
            subscription.viewer.server != subscription.target.server && subscription.session != 0
        }
        InvseeControl::Invalidated(invalidation) => {
            invalidation.subscription.viewer.server != invalidation.subscription.target.server
                && invalidation.subscription.session != 0
        }
    }
}

pub fn request_parcel(request: &InvseeRequest) -> Result<InvseeParcel, InvseeError> {
    let bytes = encode_control(&InvseeControl::Request(request.clone()))?;
    Ok(InvseeParcel::new(
        request.target.server.0,
        invsee_control_kind(),
        bytes,
    ))
}

pub fn response_parcel(response: &InvseeResponse) -> Result<InvseeParcel, InvseeError> {
    let bytes = encode_control(&InvseeControl::Response(response.clone()))?;
    Ok(InvseeParcel::new(
        response.viewer.server.0,
        invsee_control_kind(),
        bytes,
    ))
}

pub fn write_parcel(write: &InvseeWrite) -> Result<InvseeParcel, InvseeError> {
    let bytes = encode_control(&InvseeControl::Write(write.clone()))?;
    Ok(InvseeParcel::new(
        write.target.server.0,
        invsee_control_kind(),
        bytes,
    ))
}

#[derive(Debug, Default)]
pub struct InvseeExchange {
    next: u64,
    waiters: HashMap<u64, oneshot::Sender<InvseeResponse>>,
}

impl InvseeExchange {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(
        &mut self,
        viewer: GlobalPlayerId,
        target: GlobalPlayerId,
        target_name: String,
        editable: bool,
    ) -> (InvseeRequest, oneshot::Receiver<InvseeResponse>) {
        let request_id = self.next;
        self.next = self.next.wrapping_add(1);
        let (sender, receiver) = oneshot::channel();
        self.waiters.insert(request_id, sender);
        (
            InvseeRequest::new(request_id, viewer, target, target_name, editable),
            receiver,
        )
    }

    pub fn resolve(&mut self, response: InvseeResponse) -> bool {
        match self.waiters.remove(&response.request_id) {
            Some(sender) => sender.send(response).is_ok(),
            None => false,
        }
    }

    pub fn cancel(&mut self, request_id: u64) -> bool {
        self.waiters.remove(&request_id).is_some()
    }

    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.waiters.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

pub async fn request_snapshot(
    exchange: &mut InvseeExchange,
    tx: &mpsc::Sender<InvseeParcel>,
    viewer: GlobalPlayerId,
    target: GlobalPlayerId,
    target_name: String,
    editable: bool,
) -> Result<InvseeResponse, InvseeError> {
    let (request, pending) = exchange.begin(viewer, target, target_name, editable);
    let parcel = request_parcel(&request)?;
    if tx.try_send(parcel).is_err() {
        exchange.cancel(request.request_id);
        return Err(InvseeError {
            message: String::from("invsee control channel closed"),
        });
    }
    match tokio::time::timeout(Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS), pending).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => Err(InvseeError {
            message: String::from("invsee responder dropped"),
        }),
        Err(_) => {
            exchange.cancel(request.request_id);
            Err(InvseeError {
                message: String::from("invsee snapshot timed out"),
            })
        }
    }
}

pub async fn submit_write(
    tx: &mpsc::Sender<InvseeParcel>,
    write: &InvseeWrite,
) -> Result<(), InvseeError> {
    let parcel = write_parcel(write)?;
    tx.try_send(parcel).map_err(|_| InvseeError {
        message: String::from("invsee control queue unavailable"),
    })
}

pub fn try_submit_write(
    tx: &mpsc::Sender<InvseeParcel>,
    write: &InvseeWrite,
) -> Result<(), InvseeError> {
    let parcel = write_parcel(write)?;
    tx.try_send(parcel).map_err(|_| InvseeError {
        message: String::from("invsee control queue full"),
    })
}

#[must_use]
pub const fn should_deliver_to_host(target: &GlobalPlayerId, local: ServerId) -> bool {
    target.server.0 == local.0
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvseeInboundEffect {
    RequestDelivery(InvseeRequest),
    ResponseDelivery(InvseeResponse),
    WriteDelivery(InvseeWrite),
    Ignored,
}

#[must_use]
pub fn classify_inbound(message: InvseeControl, local: ServerId) -> InvseeInboundEffect {
    match message {
        InvseeControl::Request(request) => {
            if should_deliver_to_host(&request.target, local) {
                InvseeInboundEffect::RequestDelivery(request)
            } else {
                InvseeInboundEffect::Ignored
            }
        }
        InvseeControl::Response(response) => {
            if should_deliver_to_host(&response.viewer, local) {
                InvseeInboundEffect::ResponseDelivery(response)
            } else {
                InvseeInboundEffect::Ignored
            }
        }
        InvseeControl::Write(write) => {
            if should_deliver_to_host(&write.target, local) {
                InvseeInboundEffect::WriteDelivery(write)
            } else {
                InvseeInboundEffect::Ignored
            }
        }
        InvseeControl::Subscribe(_)
        | InvseeControl::Unsubscribe(_)
        | InvseeControl::Invalidated(_) => InvseeInboundEffect::Ignored,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PlayerSlot;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    fn slot(index: u16) -> InvseeSlot {
        InvseeSlot::new(index, index.saturating_add(1), 1, vec![index as u8])
    }

    fn mutation(index: u16) -> InvseeMutation {
        let before = slot(index);
        let mut after = before.clone();
        after.count = 2;
        InvseeMutation::new(before, after)
    }

    fn snapshot() -> InventorySnapshot {
        InventorySnapshot::new(
            gid(2, 7),
            String::from("Bob"),
            41,
            3,
            vec![slot(0), slot(9)],
        )
    }

    fn request() -> InvseeRequest {
        InvseeRequest::new(11, gid(1, 1), gid(2, 7), String::from("Bob"), false)
    }

    #[test]
    fn control_travels_on_control_kind() {
        assert_eq!(invsee_control_kind(), StreamKind::Control);
    }

    #[test]
    fn control_envelope_round_trips() {
        let message = InvseeControl::Request(request());
        let bytes = encode_control(&message).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), message);
        let response = InvseeResponse::found(&request(), snapshot());
        let bytes = encode_control(&InvseeControl::Response(response.clone())).unwrap();
        assert_eq!(
            decode_control(&bytes).unwrap(),
            InvseeControl::Response(response)
        );
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), 41, vec![mutation(4)]);
        let bytes = encode_control(&InvseeControl::Write(write.clone())).unwrap();
        assert_eq!(decode_control(&bytes).unwrap(), InvseeControl::Write(write));
    }

    #[test]
    fn parcels_route_to_host_peer() {
        let parcel = request_parcel(&request()).unwrap();
        assert_eq!(parcel.peer, 2);
        assert_eq!(parcel.kind, StreamKind::Control);
        let response = InvseeResponse::offline(&request());
        assert_eq!(response_parcel(&response).unwrap().peer, 1);
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), 41, vec![mutation(4)]);
        assert_eq!(write_parcel(&write).unwrap().peer, 2);
    }

    #[test]
    fn snapshot_caps_name_and_slots() {
        let many: Vec<InvseeSlot> = (0..200).map(slot).collect();
        let capped = InventorySnapshot::new(
            gid(2, 7),
            String::from("averylongplayernamehere"),
            41,
            0,
            many,
        );
        assert_eq!(capped.owner_name.chars().count(), INVSEE_MAX_NAME_CHARS);
        assert_eq!(capped.len(), INVSEE_MAX_SLOTS);
        assert!(!capped.is_empty());
        assert!(capped.slot(9).is_some());
        assert!(capped.slot(190).is_none());
    }

    #[test]
    fn offline_response_has_no_snapshot() {
        let response = InvseeResponse::offline(&request());
        assert!(response.snapshot().is_none());
        let found = InvseeResponse::found(&request(), snapshot());
        assert!(found.snapshot().is_some());
    }

    #[test]
    fn replacement_clears_omitted_slots_and_preserves_unchanged_slots() {
        let baseline = snapshot();
        let retained = slot(0);
        let added = slot(4);
        let write =
            InvseeWrite::replacement(gid(1, 1), &baseline, vec![retained.clone(), added.clone()])
                .unwrap();
        assert_eq!(write.expected_revision, 41);
        assert_eq!(write.mutations.len(), 2);
        assert!(write.mutations.iter().any(|mutation| {
            mutation.before == slot(9) && mutation.after == InvseeSlot::new(9, 0, 0, Vec::new())
        }));
        assert!(write.mutations.iter().any(|mutation| {
            mutation.before == InvseeSlot::new(4, 0, 0, Vec::new()) && mutation.after == added
        }));
        assert!(
            !write
                .mutations
                .iter()
                .any(|mutation| mutation.before == retained)
        );
    }

    #[test]
    fn remote_replacement_uses_owner_snapshot_revision_and_chunk() {
        let chunk = crate::protocol::ChunkKey::new(crate::protocol::WorldId::new(0, 9), 4, -2);
        let baseline = snapshot().with_affected(vec![chunk]);
        let write = InvseeWrite::replacement(gid(1, 1), &baseline, Vec::new()).unwrap();
        let mutation = write.inventory_mutation().unwrap();
        assert_eq!(write.target, gid(2, 7));
        assert_eq!(write.affected, vec![chunk]);
        assert_eq!(
            mutation.expected_revisions,
            vec![(crate::inventory::InventoryTarget::Player(gid(2, 7)), 41)]
        );
        assert!(mutation.operations.iter().all(|operation| {
            operation.target == crate::inventory::InventoryTarget::Player(gid(2, 7))
                && operation.after.is_empty()
        }));
    }

    #[test]
    fn exchange_resolves_and_cancels() {
        let mut exchange = InvseeExchange::new();
        assert!(exchange.is_empty());
        let (first, _first_waiter) =
            exchange.begin(gid(1, 1), gid(2, 7), String::from("Bob"), false);
        let (second, _second_waiter) =
            exchange.begin(gid(1, 1), gid(2, 7), String::from("Bob"), true);
        assert_eq!(exchange.pending_len(), 2);
        assert_ne!(first.request_id, second.request_id);
        assert!(exchange.cancel(first.request_id));
        assert!(!exchange.cancel(first.request_id));
        let response = InvseeResponse::offline(&second);
        assert!(exchange.resolve(response));
        assert!(exchange.is_empty());
        assert!(!exchange.resolve(InvseeResponse::offline(&second)));
    }

    #[test]
    fn inbound_classifies_by_host() {
        let local = ServerId(2);
        let delivered = classify_inbound(InvseeControl::Request(request()), local);
        assert_eq!(delivered, InvseeInboundEffect::RequestDelivery(request()));
        let remote = classify_inbound(InvseeControl::Request(request()), ServerId(9));
        assert_eq!(remote, InvseeInboundEffect::Ignored);
        let response = InvseeResponse::offline(&request());
        assert_eq!(
            classify_inbound(InvseeControl::Response(response.clone()), ServerId(1)),
            InvseeInboundEffect::ResponseDelivery(response)
        );
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), 41, vec![mutation(4)]);
        assert_eq!(
            classify_inbound(InvseeControl::Write(write.clone()), local),
            InvseeInboundEffect::WriteDelivery(write)
        );
    }

    #[test]
    fn garbage_bytes_do_not_decode() {
        assert!(decode_control(&[0xFF, 0xFE, 0xFD]).is_err());
    }

    #[test]
    fn decoded_write_rejects_duplicate_slot_mutations() {
        let write = InvseeWrite {
            request_id: 8,
            viewer: gid(1, 1),
            target: gid(2, 7),
            expected_revision: 41,
            mutations: vec![mutation(4), mutation(4)],
            affected: Vec::new(),
        };
        let bytes = postcard::to_allocvec(&InvseeControl::Write(write)).unwrap();
        assert!(decode_control(&bytes).is_err());
    }

    #[tokio::test]
    async fn snapshot_times_out_without_responder() {
        let mut exchange = InvseeExchange::new();
        let (tx, _rx) = mpsc::channel(1);
        let result = request_snapshot(
            &mut exchange,
            &tx,
            gid(1, 1),
            gid(2, 7),
            String::from("Bob"),
            false,
        )
        .await;
        assert!(result.is_err());
        assert!(exchange.is_empty());
    }

    #[tokio::test]
    async fn exchange_delivers_response_to_waiter() {
        let mut exchange = InvseeExchange::new();
        let (tx, mut rx) = mpsc::channel(4);
        let viewer = gid(1, 1);
        let target = gid(2, 7);
        let (request, waiter) = exchange.begin(viewer, target, String::from("Bob"), false);
        tx.send(request_parcel(&request).unwrap()).await.unwrap();
        let parcel = rx.recv().await.unwrap();
        let decoded = decode_control(&parcel.bytes).unwrap();
        assert_eq!(decoded, InvseeControl::Request(request.clone()));
        exchange.resolve(InvseeResponse::found(&request, snapshot()));
        let response = waiter.await.unwrap();
        assert!(response.snapshot().is_some());
    }
}
