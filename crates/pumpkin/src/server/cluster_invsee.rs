//! Cluster-wide invsee enforcement.
//!
//! [`pumpkin_cluster::invsee`] already defines the wire parcels for inventory
//! snapshot requests, responses, and writes. This module hooks those parcels
//! up to the [`Server`] so `/invsee` can view and edit players hosted on any
//! cluster peer. Requests carry a target player name and are fanned out to
//! every known peer; the peer hosting that player answers with a snapshot.
//! Edits are sent back to the hosting peer as [`InvseeWrite`] parcels when
//! the viewer closes the screen.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
use pumpkin_cluster::invsee::{
    INVSEE_REQUEST_TIMEOUT_MS, InvseeControl, InvseeExchange, InvseeResponse, InvseeWrite,
    InventorySnapshot, classify_inbound, decode_control, request_parcel, response_parcel,
    write_parcel,
};
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::command::commands::invsee::snapshot_player;

struct InvseeOutbox {
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static INVSEE_OUTBOX: OnceLock<InvseeOutbox> = OnceLock::new();
static INVSEE_EXCHANGE: OnceLock<Mutex<InvseeExchange>> = OnceLock::new();
static INVSEE_REQUESTS_SERVED: AtomicU64 = AtomicU64::new(0);
static INVSEE_WRITES_APPLIED: AtomicU64 = AtomicU64::new(0);
static INVSEE_SNAPSHOTS_RECEIVED: AtomicU64 = AtomicU64::new(0);

fn exchange() -> &'static Mutex<InvseeExchange> {
    INVSEE_EXCHANGE.get_or_init(|| Mutex::new(InvseeExchange::new()))
}

/// Number of inbound invsee requests answered locally.
#[must_use]
pub fn invsee_requests_served() -> u64 {
    INVSEE_REQUESTS_SERVED.load(Ordering::Relaxed)
}

/// Number of inbound invsee writes applied locally.
#[must_use]
pub fn invsee_writes_applied() -> u64 {
    INVSEE_WRITES_APPLIED.load(Ordering::Relaxed)
}

/// Number of remote snapshots received for local viewers.
#[must_use]
pub fn invsee_snapshots_received() -> u64 {
    INVSEE_SNAPSHOTS_RECEIVED.load(Ordering::Relaxed)
}

/// Installs the outbound invsee channel used for snapshot requests and writes.
pub fn install_invsee_outbox(
    _local: ServerId,
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = INVSEE_OUTBOX.set(InvseeOutbox { peers, outbound });
    let _ = INVSEE_EXCHANGE.set(Mutex::new(InvseeExchange::new()));
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

fn target_for_peer(peer: u16, known: Option<GlobalPlayerId>) -> GlobalPlayerId {
    known
        .filter(|gid| gid.server.0 == peer)
        .unwrap_or(GlobalPlayerId::new(ServerId(peer), PlayerSlot(0)))
}

fn remote_target_for_peer(server: &Server, peer: u16, target_name: &str) -> GlobalPlayerId {
    target_for_peer(
        peer,
        super::cluster_chat_pm::directory_snapshot(server).locate_by_name(target_name),
    )
}

fn find_player_by_gid(server: &Arc<Server>, gid: GlobalPlayerId) -> Option<Arc<crate::entity::player::Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.cluster_gid() == Some(gid) {
                return Some(player.clone());
            }
        }
    }
    None
}

fn find_local_target(
    server: &Arc<Server>,
    target: GlobalPlayerId,
    target_name: &str,
) -> Option<Arc<crate::entity::player::Player>> {
    if let Some(player) = find_player_by_gid(server, target) {
        return Some(player);
    }
    if target_name.is_empty() {
        return None;
    }
    server.get_player_by_name(target_name)
}

fn send_parcel_to_peer(peer: ServerId, bytes: Vec<u8>) -> bool {
    let Some(outbox) = INVSEE_OUTBOX.get() else {
        return false;
    };
    let parcel = OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes,
    };
    outbox.outbound.try_send(parcel).is_ok()
}

fn answer_request(server: &Arc<Server>, local: ServerId, request: &pumpkin_cluster::invsee::InvseeRequest) {
    let Some(target) = find_local_target(server, request.target, &request.target_name) else {
        let response = InvseeResponse::offline(request);
        let Ok(parcel) = response_parcel(&response) else {
            warn!("cluster invsee offline response encode failed");
            return;
        };
        send_parcel_to_peer(ServerId(parcel.peer), parcel.bytes);
        return;
    };
    if target.is_in_cluster_lobby() {
        let response = InvseeResponse::offline(request);
        let Ok(parcel) = response_parcel(&response) else {
            warn!("cluster invsee offline response encode failed");
            return;
        };
        send_parcel_to_peer(ServerId(parcel.peer), parcel.bytes);
        return;
    }
    let target_gid = target
        .cluster_gid()
        .unwrap_or(GlobalPlayerId::new(local, PlayerSlot(0)));
    let snapshot = snapshot_player(&target, target_gid);
    let response = InvseeResponse::found(request, snapshot);
    let Ok(parcel) = response_parcel(&response) else {
        warn!("cluster invsee snapshot response encode failed");
        return;
    };
    if send_parcel_to_peer(ServerId(parcel.peer), parcel.bytes) {
        INVSEE_REQUESTS_SERVED.fetch_add(1, Ordering::Relaxed);
        debug!(
            target = request.target_name.as_str(),
            "cluster invsee request served"
        );
    }
}

struct LivePlayerCells<'inventory> {
    inventory: &'inventory pumpkin_inventory::player::player_inventory::PlayerInventory,
}

impl pumpkin_cluster::inventory::InvCells for LivePlayerCells<'_> {
    fn cell(
        &self,
        loc: pumpkin_cluster::inventory::InvLoc,
    ) -> Option<(u16, u8)> {
        if loc.inv != pumpkin_cluster::inventory::INV_MAIN {
            return None;
        }
        let index = usize::from(loc.slot);
        if index
            < pumpkin_inventory::player::player_inventory::PlayerInventory::MAIN_SIZE
        {
            let stacks = self.inventory.main_inventory.try_read().ok()?;
            let stack = stacks.get(index)?;
            if stack.is_empty() {
                Some((0, 0))
            } else {
                Some((stack.item.id, stack.item_count))
            }
        } else {
            let slot = self.inventory.equipment_slots.get(&index)?;
            let equipment = self.inventory.entity_equipment.try_lock().ok()?;
            let stack = equipment.get(slot);
            if stack.is_empty() {
                Some((0, 0))
            } else {
                Some((stack.item.id, stack.item_count))
            }
        }
    }

    fn set_cell(
        &mut self,
        loc: pumpkin_cluster::inventory::InvLoc,
        item: u16,
        count: u8,
    ) -> bool {
        if loc.inv != pumpkin_cluster::inventory::INV_MAIN {
            return false;
        }
        let index = usize::from(loc.slot);
        if item == 0 || count == 0 {
            if index
                < pumpkin_inventory::player::player_inventory::PlayerInventory::MAIN_SIZE
            {
                let Ok(mut stacks) = self.inventory.main_inventory.try_write() else {
                    return false;
                };
                let Some(slot) = stacks.get_mut(index) else {
                    return false;
                };
                *slot = pumpkin_data::item_stack::ItemStack::EMPTY.clone();
            } else {
                let Some(key) = self.inventory.equipment_slots.get(&index) else {
                    return false;
                };
                let Ok(mut equipment) = self.inventory.entity_equipment.try_lock() else {
                    return false;
                };
                equipment.put(
                    key,
                    pumpkin_data::item_stack::ItemStack::EMPTY.clone(),
                );
            }
            return true;
        }
        let Some(def) = pumpkin_data::item::Item::from_id(item) else {
            return false;
        };
        let stack = pumpkin_data::item_stack::ItemStack::new(count, def);
        if index
            < pumpkin_inventory::player::player_inventory::PlayerInventory::MAIN_SIZE
        {
            let Ok(mut stacks) = self.inventory.main_inventory.try_write() else {
                return false;
            };
            let Some(slot) = stacks.get_mut(index) else {
                return false;
            };
            *slot = stack;
        } else {
            let Some(key) = self.inventory.equipment_slots.get(&index) else {
                return false;
            };
            let Ok(mut equipment) = self.inventory.entity_equipment.try_lock() else {
                return false;
            };
            equipment.put(key, stack);
        }
        true
    }
}

fn restore_full_stack(
    target: &crate::entity::player::Player,
    slot: &pumpkin_cluster::invsee::InvseeSlot,
) {
    if slot.nbt.is_empty() {
        return;
    }
    let mut cursor = std::io::Cursor::new(slot.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let Ok(nbt) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) else {
        return;
    };
    let Some(stack) = pumpkin_data::item_stack::ItemStack::read_item_stack(&nbt.root_tag)
    else {
        return;
    };
    let index = usize::from(slot.index);
    let inventory = target.inventory();
    if index < pumpkin_inventory::player::player_inventory::PlayerInventory::MAIN_SIZE {
        if let Ok(mut stacks) = inventory.main_inventory.try_write() {
            if let Some(cell) = stacks.get_mut(index) {
                *cell = stack;
            }
        }
    } else if let Some(key) = inventory.equipment_slots.get(&index) {
        if let Ok(mut equipment) = inventory.entity_equipment.try_lock() {
            equipment.put(key, stack);
        }
    }
}

fn deliver_write(server: &Arc<Server>, write: &InvseeWrite) {
    let Some(target) = find_player_by_gid(server, write.target) else {
        return;
    };
    let tick = TickStamp::now();
    let mut cells = LivePlayerCells {
        inventory: target.inventory(),
    };
    let mut applied = 0_u32;
    for slot in &write.slots {
        let dst = pumpkin_cluster::inventory::InvLoc::new(
            pumpkin_cluster::inventory::INV_MAIN,
            slot.index,
        );
        let op = pumpkin_cluster::inventory::capture_inv_set(
            write.viewer,
            pumpkin_cluster::inventory::next_inv_seq(write.viewer),
            tick,
            dst,
            slot.item_id,
            slot.count,
            slot.nbt.clone(),
        );
        if pumpkin_cluster::inventory::replay(&mut cells, &op)
            != pumpkin_cluster::inventory::InvVerdict::Applied
        {
            continue;
        }
        restore_full_stack(&target, slot);
        applied = applied.saturating_add(1);
    }
    if applied > 0 {
        INVSEE_WRITES_APPLIED.fetch_add(u64::from(applied), Ordering::Relaxed);
        info!("cluster invsee write applied");
    }
}

/// Handles one decoded invsee control message for the local [`Server`].
pub fn handle_invsee_control(server: &Arc<Server>, local: ServerId, message: InvseeControl) {
    match classify_inbound(message, local) {
        pumpkin_cluster::invsee::InvseeInboundEffect::RequestDelivery(request) => {
            answer_request(server, local, &request);
        }
        pumpkin_cluster::invsee::InvseeInboundEffect::ResponseDelivery(response) => {
            if response.snapshot().is_some() {
                INVSEE_SNAPSHOTS_RECEIVED.fetch_add(1, Ordering::Relaxed);
            }
            let Ok(mut guard) = exchange().lock() else {
                return;
            };
            guard.resolve(response);
        }
        pumpkin_cluster::invsee::InvseeInboundEffect::WriteDelivery(write) => {
            deliver_write(server, &write);
        }
        pumpkin_cluster::invsee::InvseeInboundEffect::Ignored => {}
    }
}

/// Applies raw control bytes when they decode as an invsee message.
///
/// Returns `true` when the bytes were an invsee message handled here.
pub fn apply_invsee_bytes(server: &Arc<Server>, local: ServerId, bytes: &[u8]) -> bool {
    let Ok(message) = decode_control(bytes) else {
        return false;
    };
    handle_invsee_control(server, local, message);
    true
}

/// Submits an inventory edit to the peer hosting the target player.
pub fn submit_remote_write(write: &InvseeWrite) {
    let Ok(parcel) = write_parcel(write) else {
        warn!("cluster invsee write encode failed");
        return;
    };
    let Some(outbox) = INVSEE_OUTBOX.get() else {
        return;
    };
    let outbound = OutboundParcel {
        peer: ServerId(parcel.peer),
        header: StreamHeader::new(parcel.kind, None),
        bytes: parcel.bytes,
    };
    if outbox.outbound.try_send(outbound).is_err() {
        warn!("cluster invsee write queue full");
    }
}

/// Requests a remote player's inventory snapshot from every known peer.
///
/// Fans the request out to all peers and returns the first `Found` snapshot.
/// Returns `None` when no peer hosts the name or the request times out.
pub async fn request_remote_snapshot(
    server: &Arc<Server>,
    viewer: GlobalPlayerId,
    target_name: &str,
    editable: bool,
) -> Option<InventorySnapshot> {
    if !cluster_enabled(server) {
        return None;
    }
    let outbox = INVSEE_OUTBOX.get()?;
    if outbox.peers.is_empty() {
        return None;
    }
    let trimmed = target_name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut pending: Vec<(u64, tokio::sync::oneshot::Receiver<InvseeResponse>)> = Vec::new();
    for peer in &outbox.peers {
        let target = remote_target_for_peer(server, *peer, trimmed);
        let (request, waiter) = match exchange().lock() {
            Ok(mut guard) => guard.begin(viewer, target, trimmed.to_string(), editable),
            Err(_) => continue,
        };
        let Ok(parcel) = request_parcel(&request) else {
            if let Ok(mut guard) = exchange().lock() {
                guard.cancel(request.request_id);
            }
            continue;
        };
        let outbound = OutboundParcel {
            peer: ServerId(parcel.peer),
            header: StreamHeader::new(parcel.kind, None),
            bytes: parcel.bytes,
        };
        if outbox.outbound.try_send(outbound).is_err() {
            if let Ok(mut guard) = exchange().lock() {
                guard.cancel(request.request_id);
            }
            continue;
        }
        pending.push((request.request_id, waiter));
    }
    if pending.is_empty() {
        return None;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS);
    let mut found: Option<InventorySnapshot> = None;
    let mut offline_count = 0_usize;
    let total = pending.len();
    let mut ids: Vec<u64> = Vec::with_capacity(total);
    let mut receivers: Vec<Option<tokio::sync::oneshot::Receiver<InvseeResponse>>> =
        Vec::with_capacity(total);
    for (id, rx) in pending {
        ids.push(id);
        receivers.push(Some(rx));
    }
    loop {
        if found.is_some() || offline_count >= total {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let mut progressed = false;
        for slot in receivers.iter_mut() {
            let Some(rx) = slot.as_mut() else {
                continue;
            };
            match rx.try_recv() {
                Ok(response) => {
                    progressed = true;
                    *slot = None;
                    match response.outcome {
                        pumpkin_cluster::invsee::InvseeOutcome::Found(snapshot) => {
                            found = Some(snapshot);
                            break;
                        }
                        pumpkin_cluster::invsee::InvseeOutcome::Offline => {
                            offline_count = offline_count.saturating_add(1);
                        }
                    }
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    progressed = true;
                    *slot = None;
                    offline_count = offline_count.saturating_add(1);
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }
        if found.is_some() || offline_count >= total {
            break;
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    drop(receivers);
    cancel_outstanding(&ids);
    found
}

/// Cancels any still-pending invsee waiters left behind by a timed-out lookup.
fn cancel_outstanding(ids: &[u64]) {
    if let Ok(mut guard) = exchange().lock() {
        for id in ids {
            guard.cancel(*id);
        }
    }
}

async fn invsee_task(
    server: Arc<Server>,
    local: ServerId,
    mut invsee_rx: mpsc::Receiver<InboundParcel>,
) {
    let mut first = true;
    while let Some(parcel) = invsee_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster invsee stream started");
        }
        apply_invsee_bytes(&server, local, &parcel.bytes);
    }
    debug!("cluster invsee stream closed");
}

/// Spawns the background task that serves invsee requests and applies writes.
pub fn spawn_invsee_apply(
    server: &Arc<Server>,
    local: ServerId,
    invsee_rx: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(invsee_task(task_server, local, invsee_rx));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_readable() {
        let _ = (
            invsee_requests_served(),
            invsee_writes_applied(),
            invsee_snapshots_received(),
        );
    }

    #[test]
    fn abandoned_lookups_cancel_their_waiters() {
        use pumpkin_cluster::identity::PlayerSlot;

        let mut local = InvseeExchange::new();
        let viewer = GlobalPlayerId::new(ServerId(1), PlayerSlot(1));
        let target = GlobalPlayerId::new(ServerId(2), PlayerSlot(0));
        let (first, _first_rx) = local.begin(viewer, target, String::from("Bob"), false);
        let (second, _second_rx) = local.begin(viewer, target, String::from("Bob"), true);
        assert_eq!(local.pending_len(), 2);
        assert!(local.cancel(first.request_id));
        assert!(local.cancel(second.request_id));
        assert!(local.is_empty());
    }

    #[test]
    fn remote_lookup_uses_the_directory_global_id_for_its_host() {
        let known = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        assert_eq!(target_for_peer(2, Some(known)), known);
        assert_eq!(
            target_for_peer(1, Some(known)),
            GlobalPlayerId::new(ServerId(1), PlayerSlot(0))
        );
        assert_eq!(
            target_for_peer(2, None),
            GlobalPlayerId::new(ServerId(2), PlayerSlot(0))
        );
    }
}
