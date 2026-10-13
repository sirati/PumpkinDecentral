use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::admin_replica::{
    AdminReplica, AdminReplicaAssembly, AdminReplicaOutcome, ReplicatedAdminControl,
    ReplicatedAdminSnapshot,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::streams::InboundParcel;
use tokio::sync::mpsc;

use super::Server;
use super::cluster_admin_apply::LocalAdminMutation;

const MAX_PENDING: usize = 4096;

pub fn receive(server: &Server, parcel: InboundParcel) -> Result<(), InboundParcel> {
    let Some(route) = server.cluster_admin_replica_inbound.load_full() else {
        return Err(parcel);
    };
    route.try_send(parcel).map_err(|error| error.into_inner())
}

fn path() -> std::io::Result<std::path::PathBuf> {
    Ok(std::env::current_dir()?.join("data/cluster-admin-replica.bin"))
}

fn write_bytes(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing admin directory"))?;
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(".admin-replica-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn save(snapshot: &ReplicatedAdminSnapshot) -> std::io::Result<()> {
    let mut replica = AdminReplica::new(snapshot.baseline.clone());
    replica
        .reconcile(snapshot.clone())
        .map_err(|()| std::io::Error::other("invalid admin replica"))?;
    let view = replica.view();
    let ops = crate::data::op::OperatorConfig {
        ops: view
            .operators
            .iter()
            .map(|grant| {
                let level = match grant.level {
                    0 => pumpkin_util::PermissionLvl::Zero,
                    1 => pumpkin_util::PermissionLvl::One,
                    2 => pumpkin_util::PermissionLvl::Two,
                    3 => pumpkin_util::PermissionLvl::Three,
                    _ => pumpkin_util::PermissionLvl::Four,
                };
                pumpkin_config::op::Op::new(
                    uuid::Uuid::from_bytes(grant.uuid),
                    grant.name.clone(),
                    level,
                    grant.bypasses_player_limit,
                )
            })
            .collect(),
    };
    let bans = crate::data::banned_player::BannedPlayerList {
        banned_players: view
            .banned_players
            .iter()
            .map(|ban| {
                Ok(crate::data::banlist_serializer::BannedPlayerEntry {
                    uuid: uuid::Uuid::from_bytes(ban.uuid),
                    name: ban.name.clone(),
                    source: ban.source.clone(),
                    reason: ban.reason.clone(),
                    created: time::OffsetDateTime::from_unix_timestamp(ban.created_epoch_secs)
                        .map_err(std::io::Error::other)?,
                    expires: ban
                        .expires_epoch_secs
                        .map(time::OffsetDateTime::from_unix_timestamp)
                        .transpose()
                        .map_err(std::io::Error::other)?,
                })
            })
            .collect::<std::io::Result<Vec<_>>>()?,
    };
    let path = path()?;
    write_bytes(
        &path,
        &postcard::to_allocvec(snapshot).map_err(std::io::Error::other)?,
    )?;
    write_bytes(
        &path.with_file_name("ops.json"),
        &serde_json::to_vec_pretty(&ops).map_err(std::io::Error::other)?,
    )?;
    write_bytes(
        &path.with_file_name("banned-players.json"),
        &serde_json::to_vec_pretty(&bans).map_err(std::io::Error::other)?,
    )
}

pub fn spawn(
    server: &Arc<Server>,
    local: ServerId,
    mut local_rx: mpsc::Receiver<LocalAdminMutation>,
    mut seed_rx: mpsc::Receiver<ServerId>,
) {
    let (tx, mut rx) = mpsc::channel::<InboundParcel>(1024);
    server
        .cluster_admin_replica_inbound
        .store(Some(Arc::new(tx)));
    let primary = matches!(
        server.advanced_config.cluster.role,
        pumpkin_config::ClusterRole::Primary
    );
    let baseline = server.cluster_admin_view.load().as_ref().clone();
    let mut replica = AdminReplica::new(baseline);
    if primary {
        match path().and_then(std::fs::read) {
            Ok(bytes) => match postcard::from_bytes::<ReplicatedAdminSnapshot>(&bytes) {
                Ok(snapshot) if bytes.len() <= 57_600_000 => {
                    let mut candidate = replica.clone();
                    if candidate.reconcile(snapshot).is_err()
                        || !super::cluster_admin_apply::valid_admin_snapshot(&candidate.view())
                    {
                        tracing::error!("invalid persisted admin replica");
                        return;
                    }
                    replica = candidate;
                }
                _ => {
                    tracing::error!("invalid persisted admin replica payload");
                    return;
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::error!(%error, "failed to load admin replica");
                return;
            }
        }
    }
    let (save_tx, mut save_rx) = mpsc::channel::<ReplicatedAdminSnapshot>(1);
    if primary {
        server.spawn_task(async move {
            let mut pending = None;
            let mut closed = false;
            let mut last_error = None::<std::time::Instant>;
            let mut retry = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    snapshot = save_rx.recv(), if !closed => {
                        match snapshot { Some(snapshot) => pending = Some(snapshot), None => closed = true }
                    }
                    _ = retry.tick(), if pending.is_some() => {},
                }
                if let Some(snapshot) = pending.take() {
                    match tokio::task::spawn_blocking(move || { let result = save(&snapshot); (snapshot, result) }).await {
                        Ok((_, Ok(()))) => {},
                        Ok((snapshot, Err(error))) => {
                            pending = Some(snapshot);
                            let now = std::time::Instant::now();
                            if last_error.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                                last_error = Some(now);
                                tracing::error!(%error, "failed to persist admin replica");
                            }
                        }
                        Err(error) => { tracing::error!(%error, "admin replica persistence task failed"); break; }
                    }
                }
                if closed && pending.is_none() { break; }
            }
        });
    }
    let weak = Arc::downgrade(server);
    tokio::spawn(async move {
        let mut outgoing = VecDeque::new();
        let mut seeds = VecDeque::new();
        let mut peers = HashMap::new();
        let mut assemblies = HashMap::<(ServerId, u64, [u8; 16]), AdminReplicaAssembly>::new();
        let mut pending_save = None;
        let mut dirty = true;
        let mut dirty_since = None;
        let mut revision = 0_u64;
        let mut stopping = false;
        let mut diagnostics = super::cluster_control_registers::Diagnostics::default();
        let mut pending_inbound = VecDeque::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            let Some(server) = weak.upgrade() else {
                break;
            };
            for _ in 0..pending_inbound.len() {
                if stopping || dirty {
                    break;
                }
                let Some(parcel): Option<InboundParcel> = pending_inbound.pop_front() else {
                    break;
                };
                if parcel.generation == 0 {
                    diagnostics.rejected("administration", parcel.peer.0, "zero transport epoch");
                    continue;
                }
                match server
                    .cluster_peer_generations
                    .load()
                    .get(&parcel.peer)
                    .copied()
                {
                    Some(generation) if generation > parcel.generation => {
                        diagnostics.rejected(
                            "administration",
                            parcel.peer.0,
                            "stale transport epoch",
                        );
                        continue;
                    }
                    Some(generation) if generation == parcel.generation => {}
                    _ => {
                        pending_inbound.push_back(parcel);
                        continue;
                    }
                }
                if !server
                    .cluster_peer_incarnations
                    .load()
                    .contains_key(&parcel.peer)
                {
                    pending_inbound.push_back(parcel);
                    continue;
                }
                match pumpkin_cluster::admin_replica::decode_control(&parcel.bytes) {
                    Some(ReplicatedAdminControl::Mutation(mutation)) => {
                        let mut candidate = replica.clone();
                        let outcome = candidate.receive(parcel.peer, mutation);
                        if outcome == AdminReplicaOutcome::Applied
                            && super::cluster_admin_apply::valid_admin_snapshot(&candidate.view())
                        {
                            replica = candidate;
                            if primary {
                                pending_save = Some(replica.export());
                            }
                            dirty = true;
                        } else if matches!(
                            outcome,
                            AdminReplicaOutcome::Applied | AdminReplicaOutcome::InvalidIdentity
                        ) {
                            diagnostics.rejected(
                                "administration",
                                parcel.peer.0,
                                "invalid mutation identity or state",
                            );
                        }
                    }
                    Some(ReplicatedAdminControl::SnapshotPart {
                        transfer,
                        index,
                        parts,
                        bytes,
                    }) => {
                        let key = (parcel.peer, parcel.generation, transfer);
                        if !assemblies.contains_key(&key) {
                            if assemblies.len() >= 64 {
                                pending_inbound.push_back(parcel);
                                continue;
                            }
                            let Some(assembly) = AdminReplicaAssembly::new(transfer, parts) else {
                                diagnostics.rejected(
                                    "administration",
                                    parcel.peer.0,
                                    "invalid snapshot part count",
                                );
                                continue;
                            };
                            assemblies.insert(key, assembly);
                        }
                        let result = assemblies
                            .get_mut(&key)
                            .unwrap()
                            .insert(transfer, index, parts, bytes);
                        match result {
                            Ok(Some(snapshot)) => {
                                assemblies.remove(&key);
                                let mut candidate = replica.clone();
                                if candidate.reconcile(snapshot).is_err()
                                    || !super::cluster_admin_apply::valid_admin_snapshot(
                                        &candidate.view(),
                                    )
                                {
                                    diagnostics.rejected(
                                        "administration",
                                        parcel.peer.0,
                                        "invalid replicated snapshot state",
                                    );
                                    continue;
                                }
                                if candidate.export() != replica.export() {
                                    replica = candidate;
                                    if primary {
                                        pending_save = Some(replica.export());
                                    }
                                    dirty = true;
                                    for peer in server.cluster_connected_peers.load().iter() {
                                        if *peer != parcel.peer
                                            && seeds.len() < 256
                                            && !seeds.contains(peer)
                                        {
                                            seeds.push_back(*peer);
                                        }
                                    }
                                }
                            }
                            Err(()) => {
                                assemblies.remove(&key);
                                diagnostics.rejected(
                                    "administration",
                                    parcel.peer.0,
                                    "conflicting or malformed snapshot parts",
                                );
                            }
                            Ok(None) => {}
                        }
                    }
                    None => diagnostics.rejected(
                        "administration",
                        parcel.peer.0,
                        "invalid control encoding",
                    ),
                }
            }
            tokio::select! {
                command = local_rx.recv(), if (!stopping || !local_rx.is_empty()) && outgoing.len() < MAX_PENDING && !dirty => {
                    let Some(command) = command else { break; };
                    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)).unwrap_or(0);
                    let mut candidate = replica.clone();
                    let mutation = candidate.issue(local, millis, command.id, command.audit);
                    if candidate.receive(local, mutation.clone()) == AdminReplicaOutcome::Applied
                        && super::cluster_admin_apply::valid_admin_snapshot(&candidate.view()) {
                        replica = candidate;
                        if primary { pending_save = Some(replica.export()); }
                        dirty = true;
                        if let Ok(bytes) = pumpkin_cluster::admin_replica::encode_control(&ReplicatedAdminControl::Mutation(mutation)) {
                            outgoing.push_back((None, bytes));
                        }
                    }
                }
                parcel = rx.recv(), if !stopping && pending_inbound.len() < 256 && outgoing.len() < MAX_PENDING && !dirty => {
                    let Some(parcel) = parcel else { break; };
                    pending_inbound.push_back(parcel);
                }
                peer = seed_rx.recv(), if !stopping && seeds.len() < 256 => {
                    if let Some(peer) = peer { if !seeds.contains(&peer) { seeds.push_back(peer); } }
                }
                _ = interval.tick() => {
                    if server.cluster_global_shutdown.load(std::sync::atomic::Ordering::Acquire) && !stopping {
                        stopping = true;
                        server.cluster_admin_replica_inbound.store(None);
                        server.cluster_admin_local_route.store(None);
                        local_rx.close();
                        rx.close();
                        seed_rx.close();
                        seeds.clear();
                        pending_inbound.clear();
                        outgoing.retain(|(peer, _)| peer.is_none());
                        if primary { pending_save = Some(replica.export()); }
                        dirty = true;
                    }
                    if dirty {
                        let since = dirty_since.get_or_insert_with(std::time::Instant::now);
                        let mut snapshot = replica.view();
                        snapshot.revision = revision.wrapping_add(1);
                        if super::cluster_admin_apply::apply_admin_snapshot(&server, &snapshot) {
                            revision = snapshot.revision;
                            dirty = false;
                            dirty_since = None;
                            if primary { pending_save = Some(replica.export()); }
                        } else if since.elapsed().as_secs() >= 10 {
                            diagnostics.deferred("administration", local.0, "permission presentation capacity");
                        }
                    }
                    if let Some(snapshot) = pending_save.take() {
                        if let Err(error) = save_tx.try_send(snapshot) { pending_save = Some(error.into_inner()); }
                    }
                    if stopping && local_rx.is_empty() && !dirty && pending_save.is_none() && outgoing.is_empty() { break; }
                    let connected = server.cluster_connected_peers.load();
                    let generations = server.cluster_peer_generations.load();
                    for peer in connected.iter() {
                        let generation = generations.get(peer).copied();
                        if peers.get(peer) != Some(&generation) && seeds.len() < 256 {
                            if !seeds.contains(peer) { seeds.push_back(*peer); }
                            peers.insert(*peer, generation);
                        }
                    }
                    peers.retain(|peer, _| connected.contains(peer));
                    assemblies.retain(|(peer, generation, _), _| generations.get(peer) == Some(generation));
                    if !stopping && outgoing.len() < MAX_PENDING - 64 && !dirty {
                        if let Some(peer) = seeds.pop_front() {
                            if connected.contains(&peer) {
                                if let Ok(frames) = pumpkin_cluster::admin_replica::snapshot_frames(&replica.export(), *uuid::Uuid::new_v4().as_bytes()) {
                                    outgoing.extend(frames.into_iter().map(|bytes| (Some(peer), bytes)));
                                }
                            }
                        }
                    }
                    for _ in 0..16 {
                        let Some((peer, bytes)) = outgoing.front() else { break; };
                        let targets = match peer {
                            Some(peer) if connected.contains(peer) => vec![*peer],
                            Some(_) => { outgoing.pop_front(); continue; },
                            None => connected.iter().copied().collect(),
                        };
                        if targets.is_empty() || server.cluster_global_outbound.try_event(&server, targets, bytes.clone()) { outgoing.pop_front(); }
                        else { break; }
                    }
                }
            }
        }
    });
}
