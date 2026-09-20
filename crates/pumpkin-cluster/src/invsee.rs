use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::StreamKind;

pub const INVSEE_MAX_SLOTS: usize = 48;
pub const INVSEE_MAX_NAME_CHARS: usize = 16;
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
pub struct InventorySnapshot {
    pub target: GlobalPlayerId,
    pub owner_name: String,
    pub selected: u8,
    pub slots: Vec<InvseeSlot>,
}

impl InventorySnapshot {
    #[must_use]
    pub fn new(
        target: GlobalPlayerId,
        owner_name: String,
        selected: u8,
        slots: Vec<InvseeSlot>,
    ) -> Self {
        let capped_name: String = owner_name.chars().take(INVSEE_MAX_NAME_CHARS).collect();
        let mut capped_slots = slots;
        capped_slots.truncate(INVSEE_MAX_SLOTS);
        Self {
            target,
            owner_name: capped_name,
            selected,
            slots: capped_slots,
        }
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
    pub viewer: GlobalPlayerId,
    pub target: GlobalPlayerId,
    pub slots: Vec<InvseeSlot>,
}

impl InvseeWrite {
    #[must_use]
    pub fn new(viewer: GlobalPlayerId, target: GlobalPlayerId, slots: Vec<InvseeSlot>) -> Self {
        let mut capped = slots;
        capped.truncate(INVSEE_MAX_SLOTS);
        Self {
            viewer,
            target,
            slots: capped,
        }
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
pub enum InvseeControl {
    Request(InvseeRequest),
    Response(InvseeResponse),
    Write(InvseeWrite),
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
    postcard::from_bytes(bytes).map_err(|error| InvseeError {
        message: format!("decode invsee control: {error}"),
    })
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
    if tx.send(parcel).await.is_err() {
        exchange.cancel(request.request_id);
        return Err(InvseeError {
            message: String::from("invsee control channel closed"),
        });
    }
    match tokio::time::timeout(
        Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS),
        pending,
    )
    .await
    {
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
    tx.send(parcel).await.map_err(|_| InvseeError {
        message: String::from("invsee control channel closed"),
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

    fn snapshot() -> InventorySnapshot {
        InventorySnapshot::new(gid(2, 7), String::from("Bob"), 3, vec![slot(0), slot(9)])
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
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), vec![slot(4)]);
        let bytes = encode_control(&InvseeControl::Write(write.clone())).unwrap();
        assert_eq!(
            decode_control(&bytes).unwrap(),
            InvseeControl::Write(write)
        );
    }

    #[test]
    fn parcels_route_to_host_peer() {
        let parcel = request_parcel(&request()).unwrap();
        assert_eq!(parcel.peer, 2);
        assert_eq!(parcel.kind, StreamKind::Control);
        let response = InvseeResponse::offline(&request());
        assert_eq!(response_parcel(&response).unwrap().peer, 1);
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), vec![slot(4)]);
        assert_eq!(write_parcel(&write).unwrap().peer, 2);
    }

    #[test]
    fn snapshot_caps_name_and_slots() {
        let many: Vec<InvseeSlot> = (0..200).map(slot).collect();
        let capped =
            InventorySnapshot::new(gid(2, 7), String::from("averylongplayernamehere"), 0, many);
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
        assert_eq!(
            delivered,
            InvseeInboundEffect::RequestDelivery(request())
        );
        let remote = classify_inbound(InvseeControl::Request(request()), ServerId(9));
        assert_eq!(remote, InvseeInboundEffect::Ignored);
        let response = InvseeResponse::offline(&request());
        assert_eq!(
            classify_inbound(InvseeControl::Response(response.clone()), ServerId(1)),
            InvseeInboundEffect::ResponseDelivery(response)
        );
        let write = InvseeWrite::new(gid(1, 1), gid(2, 7), vec![slot(4)]);
        assert_eq!(
            classify_inbound(InvseeControl::Write(write.clone()), local),
            InvseeInboundEffect::WriteDelivery(write)
        );
    }

    #[test]
    fn garbage_bytes_do_not_decode() {
        assert!(decode_control(&[0xFF, 0xFE, 0xFD]).is_err());
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
