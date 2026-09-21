use std::sync::Arc;

use pumpkin_cluster::codec::{decode_batch, encode_batch};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::primary::AcceptedTick;
use pumpkin_cluster::protocol::{StreamKind, TickBatch};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::Server;

const PRIMARY_TICK_MAGIC: [u8; 4] = [0x50, 0x54, 0x49, 0x4B];

fn primary_payload(bytes: Vec<u8>) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PRIMARY_TICK_MAGIC.len().saturating_add(bytes.len()));
    payload.extend_from_slice(&PRIMARY_TICK_MAGIC);
    payload.extend_from_slice(&bytes);
    payload
}

fn decode_primary_payload(tick: &AcceptedTick) -> Option<TickBatch> {
    let bytes = tick.payload.strip_prefix(&PRIMARY_TICK_MAGIC)?;
    let batch = decode_batch(bytes).ok()?;
    (batch.tick == tick.tick).then_some(batch)
}

pub fn apply_primary_accepted_tick(
    server: &Arc<Server>,
    ledger: &mut pumpkin_cluster::inventory::InvLedger,
    tick: &AcceptedTick,
) -> bool {
    if let Ok(control) = pumpkin_cluster::world_time::decode_primary_tick(tick) {
        return super::cluster_world_time::persist_primary_control(server, &control);
    }
    let Some(batch) = decode_primary_payload(tick) else {
        return false;
    };
    super::cluster_world_apply::apply_batch_to_server(server, ledger, &batch);
    true
}

async fn forward_accepted_ticks(
    local: ServerId,
    primary: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
    mut accepted: mpsc::Receiver<TickBatch>,
) {
    while let Some(batch) = accepted.recv().await {
        if local == primary {
            continue;
        }
        let Ok(bytes) = encode_batch(&batch) else {
            warn!(tick = batch.tick.0, "cluster primary tick encode failed");
            continue;
        };
        if outbound
            .try_send(OutboundParcel {
                peer: primary,
                header: StreamHeader::new(StreamKind::PrimaryTick, None),
                bytes,
            })
            .is_err()
        {
            warn!(tick = batch.tick.0, "cluster primary tick delivery queue full");
        }
    }
    debug!("cluster primary tick forwarder stopped");
}

async fn persist_accepted_ticks(server: Arc<Server>, mut inbound: mpsc::Receiver<InboundParcel>) {
    let primary = server.primary_save.clone();
    while let Some(parcel) = inbound.recv().await {
        if parcel.header.kind != StreamKind::PrimaryTick {
            continue;
        }
        let Some(handle) = primary.as_ref() else {
            warn!(from = parcel.peer.0, "cluster primary tick received by secondary");
            continue;
        };
        let batch = match decode_batch(&parcel.bytes) {
            Ok(batch) => batch,
            Err(error) => {
                warn!(from = parcel.peer.0, %error, "cluster primary tick decode failed");
                continue;
            }
        };
        if handle
            .try_submit(batch.tick, primary_payload(parcel.bytes))
            .is_err()
        {
            warn!(from = parcel.peer.0, tick = batch.tick.0, "cluster primary tick save queue full");
        }
    }
    debug!("cluster primary tick persistence receiver stopped");
}

pub fn install_primary_tick_stream(
    server: &Arc<Server>,
    local: ServerId,
    primary: ServerId,
    outbound: mpsc::Sender<OutboundParcel>,
    inbound: mpsc::Receiver<InboundParcel>,
) {
    let (accepted_tx, accepted_rx) = mpsc::channel(1024);
    super::cluster_world_apply::install_accepted_tick_copy(accepted_tx);
    server.spawn_task(forward_accepted_ticks(local, primary, outbound, accepted_rx));
    server.spawn_task(persist_accepted_ticks(Arc::clone(server), inbound));
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::time::TickStamp;

    #[test]
    fn accepted_batch_payload_roundtrips() {
        let batch = TickBatch::new(TickStamp(17));
        let payload = primary_payload(encode_batch(&batch).unwrap());
        let accepted = AcceptedTick {
            tick: batch.tick,
            payload,
        };
        assert_eq!(decode_primary_payload(&accepted), Some(batch));
    }

    #[test]
    fn non_primary_payload_is_not_game_tick() {
        let accepted = AcceptedTick {
            tick: TickStamp(17),
            payload: vec![1, 2, 3],
        };
        assert!(decode_primary_payload(&accepted).is_none());
    }
}
