use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::InboundParcel;
use tokio::sync::mpsc;

use super::Server;

static VISUAL_BATCHES: AtomicU64 = AtomicU64::new(0);
static VISUAL_APPLIED: AtomicU64 = AtomicU64::new(0);
static VISUAL_DROPPED: AtomicU64 = AtomicU64::new(0);

pub fn visual_batches() -> u64 {
    VISUAL_BATCHES.load(Ordering::Relaxed)
}
pub fn visual_applied() -> u64 {
    VISUAL_APPLIED.load(Ordering::Relaxed)
}
pub fn visual_dropped() -> u64 {
    VISUAL_DROPPED.load(Ordering::Relaxed)
}

pub(super) fn record_applied() {
    VISUAL_APPLIED.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn decode_visual(
    server: &Server,
    parcel: &InboundParcel,
) -> Result<pumpkin_cluster::replica_visual::PlayerVisualEnvelope, &'static str> {
    VISUAL_BATCHES.fetch_add(1, Ordering::Relaxed);
    let Some(envelope) =
        pumpkin_cluster::replica_visual::PlayerVisualEnvelope::decode(&parcel.bytes)
    else {
        VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
        return Err("envelope");
    };
    if parcel.header.kind != StreamKind::PlayerVisual
        || parcel.header.player != Some(envelope.lease.gid)
        || envelope.source.server != parcel.peer
        || server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation)
        || server.cluster_peer_incarnations.load().get(&parcel.peer)
            != Some(&envelope.source.generation)
    {
        VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
        return Err("source");
    }
    Ok(envelope)
}

pub fn spawn_visual_apply(
    server: &Arc<Server>,
    _local: ServerId,
    visual_rx: mpsc::Receiver<InboundParcel>,
) {
    assert!(
        server
            .cluster_remote_player_visual_inbox
            .push(visual_rx)
            .is_ok()
    );
}

pub(super) fn decode_ender(
    server: &Server,
    parcel: &InboundParcel,
) -> Result<pumpkin_cluster::replica_inventory::PlayerEnderSnapshot, &'static str> {
    VISUAL_BATCHES.fetch_add(1, Ordering::Relaxed);
    let Some(snapshot) =
        pumpkin_cluster::replica_inventory::PlayerEnderSnapshot::decode(&parcel.bytes)
    else {
        VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
        return Err("ender envelope");
    };
    if parcel.header.kind != StreamKind::PlayerVisual
        || parcel.header.player != Some(snapshot.lease.gid)
        || snapshot.source.server != parcel.peer
        || server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation)
        || server.cluster_peer_incarnations.load().get(&parcel.peer)
            != Some(&snapshot.source.generation)
    {
        VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
        return Err("ender source");
    }
    if snapshot.cells.as_ref().is_some_and(|cells| {
        cells
            .iter()
            .any(|cell| super::cluster_invsee::item_stack_from_inventory_cell(cell).is_err())
    }) {
        VISUAL_DROPPED.fetch_add(1, Ordering::Relaxed);
        return Err("ender stack");
    }
    Ok(snapshot)
}
