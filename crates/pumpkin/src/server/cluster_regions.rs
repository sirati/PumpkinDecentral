use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pumpkin_cluster::regions::{RegionRegistry, SyncedRegion};
use pumpkin_util::math::vector2::Vector2;
use pumpkin_world::level::is_cluster_secondary;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, trace, warn};

use super::Server;
use crate::STOP_INTERRUPT;

static REGION_COMMANDS: OnceLock<mpsc::Sender<SyncedRegion>> = OnceLock::new();
static REGION_COUNT: AtomicUsize = AtomicUsize::new(0);
static REGION_DROPPED: AtomicUsize = AtomicUsize::new(0);

const REGION_SWEEP_SECS: u64 = 30;
const REGION_CHANNEL: usize = 64;

pub const SPAWN_REGION_RADIUS: i32 = 2;

pub fn spawn_region_keeper(server: &Arc<Server>) {
    let (commands_tx, commands_rx) = mpsc::channel(REGION_CHANNEL);
    if REGION_COMMANDS.set(commands_tx).is_err() {
        return;
    }
    let keeper_server = server.clone();
    let stop = STOP_INTERRUPT.clone();
    server.spawn_task(async move {
        run_region_keeper(keeper_server, commands_rx, stop).await;
    });
}

pub fn register_synced_region(region: SyncedRegion) {
    let Some(commands) = REGION_COMMANDS.get() else {
        REGION_DROPPED.fetch_add(1, Ordering::Relaxed);
        warn!(
            world = %region.world,
            "cluster region keeper not running, region dropped"
        );
        return;
    };
    if let Err(rejected) = commands.try_send(region) {
        REGION_DROPPED.fetch_add(1, Ordering::Relaxed);
        warn!(
            world = %rejected.into_inner().world,
            "cluster region command channel full, region dropped"
        );
    }
}

#[must_use]
pub fn synced_region_count() -> usize {
    REGION_COUNT.load(Ordering::Relaxed)
}

#[must_use]
pub fn region_dropped() -> usize {
    REGION_DROPPED.load(Ordering::Relaxed)
}

async fn run_region_keeper(
    server: Arc<Server>,
    mut commands: mpsc::Receiver<SyncedRegion>,
    stop: CancellationToken,
) {
    let mut registry = RegionRegistry::new();
    let mut spawned: Vec<SyncedRegion> = Vec::new();
    let region_loads = TaskTracker::new();
    let region_stop = stop.child_token();
    let mut sweep = tokio::time::interval(Duration::from_secs(REGION_SWEEP_SECS));
    ensure_spawn_regions(&server, &mut registry, &mut spawned);
    REGION_COUNT.store(registry.len(), Ordering::Relaxed);
    sync_regions(&server, &registry, &region_stop, &region_loads);
    sweep.reset();
    loop {
        tokio::select! {
            () = stop.cancelled() => break,
            command = commands.recv() => {
                let Some(region) = command else { break };
                info!(
                    world = %region.world,
                    min_x = region.min_x,
                    max_x = region.max_x,
                    min_z = region.min_z,
                    max_z = region.max_z,
                    chunks = region.chunk_count(),
                    "cluster region registered"
                );
                registry.register(region);
                REGION_COUNT.store(registry.len(), Ordering::Relaxed);
                sync_regions(&server, &registry, &region_stop, &region_loads);
            }
            _ = sweep.tick() => {
                ensure_spawn_regions(&server, &mut registry, &mut spawned);
                REGION_COUNT.store(registry.len(), Ordering::Relaxed);
                if !registry.is_empty() {
                    sync_regions(&server, &registry, &region_stop, &region_loads);
                }
            }
        }
    }
    region_stop.cancel();
    region_loads.close();
    region_loads.wait().await;
}

fn ensure_spawn_regions(
    server: &Arc<Server>,
    registry: &mut RegionRegistry,
    spawned: &mut Vec<SyncedRegion>,
) {
    let worlds = server.worlds.load();
    let mut desired: Vec<SyncedRegion> = Vec::new();
    for world in worlds.iter() {
        let spawn = world.get_spawn_location().0;
        desired.push(SyncedRegion::spawn_area(
            world.get_world_name().to_owned(),
            spawn.0.x >> 4,
            spawn.0.z >> 4,
            SPAWN_REGION_RADIUS,
        ));
    }
    let mut removed: Vec<SyncedRegion> = Vec::new();
    spawned.retain(|region| {
        if desired.contains(region) {
            true
        } else {
            removed.push(region.clone());
            false
        }
    });
    for region in removed {
        registry.remove(&region);
        for (x, z) in region.chunks() {
            if registry.is_synced(&region.world, x, z) {
                continue;
            }
            let pos = Vector2::new(x, z);
            if let Some(world) = worlds
                .iter()
                .find(|world| world.get_world_name() == region.world)
            {
                world.level.unpin_cluster_chunk(&pos);
                world.level.unwant_cluster_chunk(pos);
            }
        }
    }
    for region in &desired {
        if spawned.contains(region) {
            continue;
        }
        info!(
            world = %region.world,
            min_x = region.min_x,
            max_x = region.max_x,
            min_z = region.min_z,
            max_z = region.max_z,
            chunks = region.chunk_count(),
            "cluster region registered"
        );
        registry.register(region.clone());
        spawned.push(region.clone());
    }
    for region in registry.regions() {
        let Some(world) = worlds
            .iter()
            .find(|world| world.get_world_name() == region.world)
        else {
            continue;
        };
        for (x, z) in region.chunks() {
            world.level.pin_cluster_chunk(Vector2::new(x, z));
        }
    }
}

fn sync_regions(
    server: &Arc<Server>,
    registry: &RegionRegistry,
    stop: &CancellationToken,
    region_loads: &TaskTracker,
) {
    if stop.is_cancelled() {
        return;
    }
    let worlds = server.worlds.load();
    for region in registry.regions() {
        let Some(world) = worlds
            .iter()
            .find(|world| world.get_world_name() == region.world)
        else {
            warn!(world = %region.world, "cluster region world not loaded");
            continue;
        };
        if is_cluster_secondary() {
            for (x, z) in region.chunks() {
                world.level.prefetch_cluster_chunk(Vector2::new(x, z));
            }
            trace!(
                world = %region.world,
                chunks = region.chunk_count(),
                "cluster region synced"
            );
            continue;
        }
        let load_world = world.clone();
        let missing: Vec<Vector2<i32>> = region
            .chunks()
            .map(|(x, z)| Vector2::new(x, z))
            .filter(|pos| !load_world.level.is_cluster_held(pos))
            .collect();
        let ensured = u64::try_from(missing.len()).unwrap_or(u64::MAX);
        if !missing.is_empty() {
            let stop = stop.clone();
            region_loads.spawn(async move {
                for pos in missing {
                    if stop.is_cancelled() {
                        break;
                    }
                    tokio::select! {
                        () = stop.cancelled() => break,
                        _ = load_world.level.get_or_fetch_chunk(pos, |_| ()) => {}
                    }
                }
            });
        }
        trace!(
            world = %region.world,
            ensured,
            chunks = region.chunk_count(),
            "cluster region synced"
        );
    }
}
