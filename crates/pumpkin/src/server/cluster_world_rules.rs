use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::WorldId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::streams::InboundParcel;
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_rules::{
    WorldRulesHistory, WorldRulesId, WorldRulesSnapshot, WorldRulesWitness,
};
use tokio::sync::mpsc;

use super::Server;

pub const PREFIX: &[u8; 8] = b"WRules01";

pub fn primary(server: &Server) -> Option<PeerGeneration> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let incarnations = server.cluster_peer_incarnations.load();
    let peers = server.cluster_primary_peers.load();
    let local_primary = matches!(
        server.advanced_config.cluster.role,
        pumpkin_config::ClusterRole::Primary
    );
    let server = peers
        .iter()
        .copied()
        .chain(local_primary.then_some(local))
        .filter(|peer| incarnations.contains_key(peer))
        .min_by_key(|peer| peer.0)?;
    Some(PeerGeneration {
        server,
        generation: *incarnations.get(&server)?,
    })
}

pub fn disciplined_slot() -> Option<u64> {
    let offset = pumpkin_cluster::ntp::shared_offset_millis()?;
    let millis = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis(),
    )
    .ok()?;
    u64::try_from(millis.checked_add(offset)?)
        .ok()
        .map(|value| value / 50)
}

pub fn slot_for_tick(tick: TickStamp) -> Option<u64> {
    let now = disciplined_slot()?;
    let age = TickStamp(now as u16).distance_since(tick);
    (age <= 4)
        .then(|| now.checked_sub(u64::from(age)))
        .flatten()
}

pub fn current(
    server: &Server,
    world: WorldId,
    tick: TickStamp,
) -> Option<(WorldRulesWitness, WorldRulesSnapshot)> {
    let issuer = primary(server)?;
    let slot = slot_for_tick(tick)?;
    let history = server.cluster_world_rules.load();
    history
        .snapshots()
        .filter(|snapshot| snapshot.rules.world == world && snapshot.rules.issuer == issuer)
        .filter(|snapshot| {
            history
                .resolve(
                    WorldRulesWitness {
                        rules: snapshot.rules,
                        slot,
                    },
                    tick,
                )
                .is_ok()
        })
        .max_by_key(|snapshot| snapshot.rules.epoch)
        .map(|snapshot| {
            (
                WorldRulesWitness {
                    rules: snapshot.rules,
                    slot,
                },
                snapshot.clone(),
            )
        })
}

pub fn receive(server: &Server, parcel: InboundParcel) -> Result<(), InboundParcel> {
    let Some(route) = server.cluster_world_rules_inbound.load_full() else {
        return Err(parcel);
    };
    route.try_send(parcel).map_err(|error| error.into_inner())
}

enum SnapshotIntake {
    Applied,
    Deferred,
    Rejected,
}

fn warn_snapshot(warnings: &mut HashMap<ServerId, std::time::Instant>, peer: ServerId) {
    let now = std::time::Instant::now();
    if warnings
        .get(&peer)
        .is_none_or(|last| now.duration_since(*last).as_secs() >= 300)
    {
        warnings.insert(peer, now);
        tracing::warn!(peer = peer.0, "invalid cluster world rules snapshot");
    }
}

fn accept_snapshot(
    server: &Server,
    history: &mut WorldRulesHistory,
    parcel: &InboundParcel,
) -> SnapshotIntake {
    match server
        .cluster_peer_generations
        .load()
        .get(&parcel.peer)
        .copied()
    {
        Some(epoch) if epoch > parcel.generation => return SnapshotIntake::Rejected,
        Some(epoch) if epoch == parcel.generation => {}
        _ => return SnapshotIntake::Deferred,
    }
    let Some(bytes) = parcel
        .bytes
        .strip_prefix(PREFIX)
        .filter(|bytes| bytes.len() <= 4096)
    else {
        return SnapshotIntake::Rejected;
    };
    let Ok(snapshot) = postcard::from_bytes::<WorldRulesSnapshot>(bytes) else {
        return SnapshotIntake::Rejected;
    };
    if !snapshot.valid() {
        return SnapshotIntake::Rejected;
    }
    let Some(incarnation) = server
        .cluster_peer_incarnations
        .load()
        .get(&parcel.peer)
        .copied()
    else {
        return SnapshotIntake::Deferred;
    };
    let Some(elected) = primary(server) else {
        return SnapshotIntake::Deferred;
    };
    let source = PeerGeneration {
        server: parcel.peer,
        generation: incarnation,
    };
    match history.insert(source, elected, snapshot) {
        Ok(()) => {}
        Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity) => {
            return SnapshotIntake::Deferred;
        }
        Err(_) => return SnapshotIntake::Rejected,
    }
    server.cluster_world_rules.store(Arc::new(history.clone()));
    SnapshotIntake::Applied
}

pub fn spawn(server: &Arc<Server>) {
    let (tx, mut rx) = mpsc::channel::<InboundParcel>(128);
    server.cluster_world_rules_inbound.store(Some(Arc::new(tx)));
    let weak = Arc::downgrade(server);
    server.spawn_task(async move {
        let mut history = WorldRulesHistory::default();
        let mut latest = BTreeMap::<WorldId, WorldRulesSnapshot>::new();
        let mut peers = HashMap::new();
        let mut outgoing = VecDeque::new();
        let mut epoch = 0_u64;
        let mut pending_input = VecDeque::new();
        let mut warnings = HashMap::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            let Some(server) = weak.upgrade() else { break; };
            let stopping = server.cluster_global_shutdown.load(std::sync::atomic::Ordering::Acquire);
            if stopping { rx.close(); }
            if stopping && rx.is_empty() && pending_input.is_empty() && outgoing.is_empty() { break; }
            drop(server);
            tokio::select! {
                parcel = rx.recv(), if pending_input.len() < 128 && outgoing.len() < 16384 && (!rx.is_closed() || !rx.is_empty()) => {
                    let Some(parcel) = parcel else { continue; };
                    let Some(server) = weak.upgrade() else { break; };
                    match accept_snapshot(&server, &mut history, &parcel) {
                        SnapshotIntake::Applied => {},
                        SnapshotIntake::Deferred => pending_input.push_back(parcel),
                        SnapshotIntake::Rejected => warn_snapshot(&mut warnings, parcel.peer),
                    }
                }
                _ = interval.tick() => {
                    let Some(server) = weak.upgrade() else { break; };
                    for _ in 0..pending_input.len() {
                        let Some(parcel) = pending_input.pop_front() else { break; };
                        match accept_snapshot(&server, &mut history, &parcel) {
                            SnapshotIntake::Applied => {},
                            SnapshotIntake::Deferred => pending_input.push_back(parcel),
                            SnapshotIntake::Rejected => warn_snapshot(&mut warnings, parcel.peer),
                        }
                    }
                    let local = ServerId(server.advanced_config.cluster.server_id);
                    if !stopping
                        && let Some(elected) = primary(&server)
                        && elected.server == local
                        && outgoing.len().saturating_add(server.cluster_connected_peers.load().len()) < 16384
                        && let Some(slot) = disciplined_slot()
                        && let Some(config) = super::cluster_gamerules::view(&server) {
                        for world in server.worlds.load().iter() {
                            let world_id = super::cluster_world_delta::world_id(world);
                            let rules = &config.rules;
                            let mut snapshot = WorldRulesSnapshot {
                                rules: WorldRulesId { world: world_id, issuer: elected, epoch: 1 },
                                configuration: pumpkin_cluster::world_rules::WorldConfigurationVersion { issuer: config.issuer, revision: config.revision, from_slot: config.slot },
                                from_slot: slot,
                                keep_inventory: rules.keep_inventory,
                                do_entity_drops: rules.entity_drops,
                                do_mob_loot: rules.mob_drops,
                                mob_griefing: rules.mob_griefing,
                                tnt_explodes: rules.tnt_explodes,
                                random_tick_speed: rules.random_tick_speed,
                                do_mob_spawning: rules.spawn_mobs,
                                water_source_conversion: rules.water_source_conversion,
                                lava_source_conversion: rules.lava_source_conversion,
                                fire_spread_radius_around_player: rules.fire_spread_radius_around_player,
                                natural_regeneration: rules.natural_health_regeneration,
                                advance_time: rules.advance_time,
                                advance_weather: rules.advance_weather,
                                players_sleeping_percentage: rules.players_sleeping_percentage,
                                min_y: world.dimension.min_y,
                                height: world.dimension.height as u32,
                                fire_damage: rules.fire_damage,
                                drowning_damage: rules.drowning_damage,
                                freeze_damage: rules.freeze_damage,
                                fall_damage: rules.fall_damage,
                                difficulty: config.difficulty as u8,
                            };
                            if let Some(previous) = latest.get(&world_id) {
                                snapshot.rules.epoch = previous.rules.epoch;
                                snapshot.from_slot = previous.from_slot;
                                if snapshot == *previous { continue; }
                                snapshot.from_slot = slot;
                                if slot <= previous.from_slot { continue; }
                            }
                            let Some(next) = epoch.checked_add(1) else { break; };
                            snapshot.rules.epoch = next;
                            if history.insert(elected, elected, snapshot.clone()).is_err() { continue; }
                            epoch = next;
                            latest.insert(world_id, snapshot.clone());
                            for peer in server.cluster_connected_peers.load().iter() {
                                outgoing.push_back((*peer, snapshot.clone()));
                            }
                            server.cluster_world_rules.store(Arc::new(history.clone()));
                        }
                        let connected = server.cluster_connected_peers.load();
                        let generations = server.cluster_peer_generations.load();
                        for peer in connected.iter() {
                            let generation = generations.get(peer).copied();
                            if peers.get(peer) != Some(&generation) {
                                let snapshots = history.snapshots().collect::<Vec<_>>();
                                if outgoing.len().saturating_add(snapshots.len()) <= 16384 {
                                    for snapshot in snapshots { outgoing.push_back((*peer, snapshot.clone())); }
                                    peers.insert(*peer, generation);
                                }
                            }
                        }
                        peers.retain(|peer, _| connected.contains(peer));
                    }
                    for _ in 0..16 {
                        let Some((peer, snapshot)) = outgoing.front() else { break; };
                        if !server.cluster_connected_peers.load().contains(peer) { outgoing.pop_front(); continue; }
                        let Ok(payload) = postcard::to_allocvec(snapshot) else { outgoing.pop_front(); continue; };
                        let mut bytes = PREFIX.to_vec(); bytes.extend_from_slice(&payload);
                        if !server.cluster_global_outbound.try_event(&server, vec![*peer], bytes) { break; }
                        outgoing.pop_front();
                    }
                }
            }
        }
    });
}
