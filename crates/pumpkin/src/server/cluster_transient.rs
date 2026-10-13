use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::streams::InboundParcel;
use tokio::sync::mpsc;

use super::Server;

static TRANSIENT_BATCHES: AtomicU64 = AtomicU64::new(0);
static TRANSIENT_DROPPED: AtomicU64 = AtomicU64::new(0);

pub fn transient_batches() -> u64 {
    TRANSIENT_BATCHES.load(Ordering::Relaxed)
}

pub fn transient_applied() -> u64 {
    0
}

pub fn transient_dropped() -> u64 {
    TRANSIENT_DROPPED.load(Ordering::Relaxed)
}

pub fn spawn_retired_transient_drain(
    server: &Arc<Server>,
    mut receiver: mpsc::Receiver<InboundParcel>,
) {
    server.spawn_task(async move {
        let mut diagnostics = std::collections::HashMap::new();
        while let Some(parcel) = receiver.recv().await {
            TRANSIENT_BATCHES.fetch_add(1, Ordering::Relaxed);
            TRANSIENT_DROPPED.fetch_add(1, Ordering::Relaxed);
            let now = std::time::Instant::now();
            diagnostics.retain(|_, last: &mut std::time::Instant| {
                now.duration_since(*last).as_secs() < 300
            });
            if let std::collections::hash_map::Entry::Vacant(entry) = diagnostics.entry(parcel.peer)
            {
                entry.insert(now);
                tracing::warn!(
                    peer = parcel.peer.0,
                    stream = "player-transient",
                    reason = "retired-unqualified-schema",
                    "cluster visual envelope rejected"
                );
            }
        }
    });
}
