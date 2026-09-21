use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::world_time::{
    TimeUpdate, WeatherUpdate, WorldTimeControl, encode_control, is_valid_control,
    submit_world_time_control,
};
use pumpkin_protocol::java::client::play::{CGameEvent, GameEvent};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::Server;
use crate::world::World;

struct WorldTimeOutbox {
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
}

static WORLD_TIME_OUTBOX: OnceLock<WorldTimeOutbox> = OnceLock::new();
static WORLD_TIME_SENT: AtomicU64 = AtomicU64::new(0);
static WORLD_TIME_UNSENT: AtomicU64 = AtomicU64::new(0);
static WORLD_TIME_APPLIED: AtomicU64 = AtomicU64::new(0);
static WORLD_TIME_BROADCAST_WARN_LAST_MILLIS: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn world_time_sent() -> u64 {
    WORLD_TIME_SENT.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_time_unsent() -> u64 {
    WORLD_TIME_UNSENT.load(Ordering::Relaxed)
}

#[must_use]
pub fn world_time_applied() -> u64 {
    WORLD_TIME_APPLIED.load(Ordering::Relaxed)
}

fn broadcast_warn_cooldown_elapsed() -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0);
    let last = WORLD_TIME_BROADCAST_WARN_LAST_MILLIS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000 {
        return false;
    }
    WORLD_TIME_BROADCAST_WARN_LAST_MILLIS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

pub fn install_world_time_outbox(
    peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
) {
    let _ = WORLD_TIME_OUTBOX.set(WorldTimeOutbox { peers, outbound });
}

fn cluster_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled
}

fn persist_on_primary(server: &Server, control: &WorldTimeControl) {
    let Some(handle) = server.primary_save.as_ref() else {
        return;
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as i64)
        .unwrap_or(0);
    if submit_world_time_control(
        handle,
        super::cluster::disciplined_tick_stamp(millis),
        control,
    )
    .is_err()
    {
        warn!("cluster world time persist queue full");
    }
}

fn try_broadcast(control: &WorldTimeControl) -> usize {
    let Some(outbox) = WORLD_TIME_OUTBOX.get() else {
        WORLD_TIME_UNSENT.fetch_add(1, Ordering::Relaxed);
        if broadcast_warn_cooldown_elapsed() {
            warn!("cluster world time broadcast dropped: outbox not installed");
        }
        return 0;
    };
    if outbox.peers.is_empty() {
        WORLD_TIME_UNSENT.fetch_add(1, Ordering::Relaxed);
        if broadcast_warn_cooldown_elapsed() {
            warn!("cluster world time broadcast dropped: no peers");
        }
        return 0;
    }
    let Ok(bytes) = encode_control(control) else {
        warn!("cluster world time control encode failed");
        return 0;
    };
    let mut sent = 0_usize;
    for peer in &outbox.peers {
        let parcel = OutboundParcel {
            peer: ServerId(*peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes: bytes.clone(),
        };
        if outbox.outbound.try_send(parcel).is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    WORLD_TIME_SENT.fetch_add(sent as u64, Ordering::Relaxed);
    let unsent = outbox.peers.len().saturating_sub(sent);
    if unsent > 0 {
        WORLD_TIME_UNSENT.fetch_add(unsent as u64, Ordering::Relaxed);
        if broadcast_warn_cooldown_elapsed() {
            warn!(
                sent = sent,
                peers = outbox.peers.len(),
                "cluster world time broadcast partially dropped: mesh queue full"
            );
        }
    }
    sent
}

fn snapshot_time_of(world: &World) -> TimeUpdate {
    let guard = world
        .level_time
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    TimeUpdate::new(
        world.dimension.minecraft_name.to_string(),
        guard.time_of_day,
        guard.partial_tick,
        guard.rate,
        guard.paused,
    )
}

fn snapshot_weather_of(world: &World) -> WeatherUpdate {
    let guard = world
        .weather
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    WeatherUpdate::new(
        world.dimension.minecraft_name.to_string(),
        guard.clear_weather_time,
        guard.rain_time,
        guard.thunder_time,
        guard.raining,
        guard.thundering,
    )
}

pub fn publish_time_for_world(server: &Server, world: &Arc<World>) {
    if !cluster_enabled(server) {
        return;
    }
    let control = WorldTimeControl::Time(snapshot_time_of(world));
    if !is_valid_control(&control) {
        warn!("cluster world time publish dropped: local snapshot failed validation");
        return;
    }
    persist_on_primary(server, &control);
    let sent = try_broadcast(&control);
    if sent > 0 {
        debug!(peers = sent, "cluster world time broadcast");
    }
}

pub fn publish_weather_for_world(server: &Server, world: &Arc<World>) {
    if !cluster_enabled(server) {
        return;
    }
    let control = WorldTimeControl::Weather(snapshot_weather_of(world));
    if !is_valid_control(&control) {
        warn!("cluster world weather publish dropped: local snapshot failed validation");
        return;
    }
    persist_on_primary(server, &control);
    let sent = try_broadcast(&control);
    if sent > 0 {
        debug!(peers = sent, "cluster world weather broadcast");
    }
}

fn matching_worlds(server: &Arc<Server>, dimension: &str) -> Vec<Arc<World>> {
    let mut out = Vec::new();
    for world in server.worlds.load().iter() {
        if world.dimension.minecraft_name == dimension {
            out.push(Arc::clone(world));
        }
    }
    out
}

fn apply_time_update(server: &Arc<Server>, update: &TimeUpdate) -> bool {
    let targets = matching_worlds(server, &update.dimension);
    if targets.is_empty() {
        warn!(
            dimension = update.dimension.as_str(),
            "cluster world time dropped: unknown dimension"
        );
        return false;
    }
    for target in &targets {
        let snapshot = {
            let mut guard = target
                .level_time
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.time_of_day = update.time_of_day;
            guard.partial_tick = update.partial_tick;
            guard.rate = update.rate;
            guard.paused = update.paused;
            guard.clone()
        };
        snapshot.send_time(target);
    }
    WORLD_TIME_APPLIED.fetch_add(1, Ordering::Relaxed);
    info!(
        dimension = update.dimension.as_str(),
        time_of_day = update.time_of_day,
        worlds = targets.len(),
        applied = WORLD_TIME_APPLIED.load(Ordering::Relaxed),
        "cluster world time applied"
    );
    persist_on_primary(server, &WorldTimeControl::Time(update.clone()));
    true
}

fn apply_weather_update(server: &Arc<Server>, update: &WeatherUpdate) -> bool {
    let targets = matching_worlds(server, &update.dimension);
    if targets.is_empty() {
        warn!(
            dimension = update.dimension.as_str(),
            "cluster world weather dropped: unknown dimension"
        );
        return false;
    }
    for target in &targets {
        let was_raining = {
            let mut guard = target
                .weather
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let was_raining = guard.raining;
            guard.clear_weather_time = update.clear_weather_time;
            guard.rain_time = update.rain_time;
            guard.thunder_time = update.thunder_time;
            guard.raining = update.raining;
            guard.thundering = update.thundering;
            was_raining
        };
        if was_raining != update.raining {
            let lobby_waiters: Vec<_> = target
                .players
                .load()
                .iter()
                .filter(|player| player.is_in_cluster_lobby())
                .map(|player| player.gameprofile.id)
                .collect();
            if was_raining {
                target.broadcast_packet_except(
                    &lobby_waiters,
                    &CGameEvent::new(GameEvent::EndRaining, 0.0),
                );
            } else {
                target.broadcast_packet_except(
                    &lobby_waiters,
                    &CGameEvent::new(GameEvent::BeginRaining, 0.0),
                );
            }
        }
    }
    WORLD_TIME_APPLIED.fetch_add(1, Ordering::Relaxed);
    info!(
        dimension = update.dimension.as_str(),
        raining = update.raining,
        thundering = update.thundering,
        worlds = targets.len(),
        applied = WORLD_TIME_APPLIED.load(Ordering::Relaxed),
        "cluster world weather applied"
    );
    persist_on_primary(server, &WorldTimeControl::Weather(update.clone()));
    true
}

pub fn apply_world_time_bytes(server: &Arc<Server>, bytes: &[u8]) -> bool {
    let control: WorldTimeControl = match pumpkin_cluster::world_time::decode_control(bytes) {
        Ok(control) => control,
        Err(_) => return false,
    };
    match &control {
        WorldTimeControl::Time(update) => apply_time_update(server, update),
        WorldTimeControl::Weather(update) => apply_weather_update(server, update),
    }
}

pub fn handle_world_time_parcel(server: &Arc<Server>, parcel: &InboundParcel) -> bool {
    if parcel.header.kind != StreamKind::Control {
        return false;
    }
    apply_world_time_bytes(server, &parcel.bytes)
}

pub async fn world_time_task(server: Arc<Server>, mut world_time_rx: mpsc::Receiver<InboundParcel>) {
    let mut first = true;
    while let Some(parcel) = world_time_rx.recv().await {
        if parcel.header.kind != StreamKind::Control {
            continue;
        }
        if first {
            first = false;
            debug!(from = parcel.peer.0, "cluster world time stream started");
        }
        apply_world_time_bytes(&server, &parcel.bytes);
    }
    debug!("cluster world time stream closed");
}

pub fn spawn_world_time_apply(server: &Arc<Server>, world_time_rx: mpsc::Receiver<InboundParcel>) {
    let task_server = Arc::clone(server);
    server.spawn_task(world_time_task(task_server, world_time_rx));
}
