use std::sync::Arc;

use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_cluster::world_time::{EnvironmentControl, decode_environment};
use tokio::sync::mpsc;

use super::Server;
use crate::world::World;

pub fn publish_time_for_world(server: &Server, world: &Arc<World>) {
    if server.advanced_config.cluster.enabled {
        server.cluster_environment.publish(world, false);
    }
}

pub fn publish_weather_for_world(server: &Server, world: &Arc<World>) {
    if server.advanced_config.cluster.enabled {
        server.cluster_environment.publish(world, true);
    }
}

pub fn handle_world_time_parcel(server: &Arc<Server>, parcel: &InboundParcel) -> bool {
    if parcel.header.kind != StreamKind::Control {
        return false;
    }
    decode_environment(&parcel.bytes).is_ok_and(|control| {
        server
            .cluster_environment
            .remote(parcel.peer, parcel.generation, control)
    })
}

pub fn spawn_world_time_apply(server: &Arc<Server>, mut rx: mpsc::Receiver<InboundParcel>) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut pending = None::<(pumpkin_cluster::identity::ServerId, u64, EnvironmentControl)>;
        let mut diagnostics = super::cluster_control_registers::Diagnostics::default();
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            if task_server
                .cluster_global_shutdown
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            let parcel = tokio::select! {
                parcel = rx.recv(), if pending.is_none() => {
                    let Some(parcel) = parcel else { break };
                    Some(parcel)
                }
                _ = flush.tick() => None,
            };
            if let Some(parcel) = parcel {
                if !parcel
                    .bytes
                    .starts_with(pumpkin_cluster::world_time::ENVIRONMENT_PREFIX)
                    || parcel.header.kind != StreamKind::Control
                {
                    continue;
                }
                match decode_environment(&parcel.bytes) {
                    Ok(control) => pending = Some((parcel.peer, parcel.generation, control)),
                    _ => diagnostics.rejected(
                        "environment_control",
                        parcel.peer.0,
                        "malformed control",
                    ),
                }
            }
            if let Some((peer, generation, control)) = pending.take() {
                let known_generation = task_server
                    .cluster_peer_generations
                    .load()
                    .get(&peer)
                    .copied();
                if known_generation.is_some_and(|known| known > generation) {
                    diagnostics.rejected("environment_control", peer.0, "stale transport");
                    continue;
                }
                if !task_server
                    .cluster_environment
                    .remote(peer, generation, control.clone())
                {
                    pending = Some((peer, generation, control));
                }
            }
        }
    });
}
