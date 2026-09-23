use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumpkin_cluster::health_policy::{
    HealthMaskMode, HealthPolicyMessage, HealthPolicySnapshot, decode, encode,
    is_health_policy_message,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};

use super::Server;

const POLICY_FILE: &str = "cluster-policy.bin";
const DIAGNOSTIC_COOLDOWN: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PolicyDiagnostic {
    Decode(u16),
    Encode,
    QueueFull(u16),
    NoPrimary,
    Persistence,
    VersionExhausted,
}

#[derive(Default)]
struct DiagnosticLimiter {
    last: HashMap<PolicyDiagnostic, Instant>,
}

impl DiagnosticLimiter {
    fn permits(&mut self, key: PolicyDiagnostic) -> bool {
        self.permits_at(key, Instant::now())
    }

    fn permits_at(&mut self, key: PolicyDiagnostic, now: Instant) -> bool {
        if self
            .last
            .get(&key)
            .is_some_and(|last| now.duration_since(*last) < DIAGNOSTIC_COOLDOWN)
        {
            return false;
        }
        self.last.insert(key, now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_are_suppressed_per_key_for_five_minutes() {
        let start = Instant::now();
        let mut limiter = DiagnosticLimiter::default();

        assert!(limiter.permits_at(PolicyDiagnostic::QueueFull(7), start));
        assert!(!limiter.permits_at(
            PolicyDiagnostic::QueueFull(7),
            start + Duration::from_secs(299)
        ));
        assert!(limiter.permits_at(
            PolicyDiagnostic::QueueFull(8),
            start + Duration::from_secs(299)
        ));
        assert!(limiter.permits_at(
            PolicyDiagnostic::QueueFull(7),
            start + Duration::from_secs(300)
        ));
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HealthPolicyCommand {
    Set(HealthMaskMode),
    PublishTo(u16),
    PeerConnected(u16),
    PrimaryObserved(u16),
    PeerAdmitted(u16),
    PeerDisconnected(u16),
    LocalAdmitted,
}

fn policy_path(server: &Server) -> PathBuf {
    server
        .basic_config
        .get_world_path()
        .join("data")
        .join(POLICY_FILE)
}

fn load_policy(path: &Path, fallback: HealthPolicySnapshot) -> HealthPolicySnapshot {
    let Ok(bytes) = std::fs::read(path) else {
        return fallback;
    };
    match decode(&bytes) {
        Ok(HealthPolicyMessage::Snapshot(snapshot)) => snapshot,
        _ => {
            warn!(path = %path.display(), "cluster health policy file is invalid; using configured default");
            fallback
        }
    }
}

fn spawn_persistence(server: &Arc<Server>) -> watch::Sender<Option<HealthPolicySnapshot>> {
    let (tx, mut rx) = watch::channel::<Option<HealthPolicySnapshot>>(None);
    let path = policy_path(server);
    server.spawn_task(async move {
        let mut diagnostics = DiagnosticLimiter::default();
        while rx.changed().await.is_ok() {
            let Some(snapshot) = *rx.borrow_and_update() else {
                continue;
            };
            let path = path.clone();
            let result = tokio::task::spawn_blocking(move || {
                let Some(parent) = path.parent() else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "policy path has no parent",
                    ));
                };
                std::fs::create_dir_all(parent)?;
                let bytes = encode(HealthPolicyMessage::Snapshot(snapshot)).map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                })?;
                let temporary = path.with_extension("bin.tmp");
                std::fs::write(&temporary, bytes)?;
                std::fs::rename(temporary, path)
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) if diagnostics.permits(PolicyDiagnostic::Persistence) => {
                    error!(%error, "cluster health policy persistence failed");
                }
                Err(error) if diagnostics.permits(PolicyDiagnostic::Persistence) => {
                    error!(%error, "cluster health policy persistence task failed");
                }
                _ => {}
            }
        }
    });
    tx
}

fn send_snapshot(
    outbound: &mpsc::Sender<OutboundParcel>,
    peer: u16,
    snapshot: HealthPolicySnapshot,
) -> bool {
    let Ok(bytes) = encode(HealthPolicyMessage::Snapshot(snapshot)) else {
        return false;
    };
    outbound
        .try_send(OutboundParcel {
            peer: ServerId(peer),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes,
        })
        .is_ok()
}

fn broadcast_snapshot(
    outbound: &mpsc::Sender<OutboundParcel>,
    peers: &HashSet<u16>,
    snapshot: HealthPolicySnapshot,
    diagnostics: &mut DiagnosticLimiter,
) {
    for peer in peers {
        if !send_snapshot(outbound, *peer, snapshot)
            && diagnostics.permits(PolicyDiagnostic::QueueFull(*peer))
        {
            warn!(
                peer,
                version = snapshot.version,
                "cluster health policy update dropped: mesh queue full"
            );
        }
    }
}

fn request_change(
    outbound: &mpsc::Sender<OutboundParcel>,
    primary: Option<u16>,
    base_version: u64,
    mode: HealthMaskMode,
) -> Result<(), PolicyDiagnostic> {
    let Some(primary) = primary else {
        return Err(PolicyDiagnostic::NoPrimary);
    };
    let bytes = encode(HealthPolicyMessage::RequestChange { base_version, mode })
        .map_err(|_| PolicyDiagnostic::Encode)?;
    if outbound
        .try_send(OutboundParcel {
            peer: ServerId(primary),
            header: StreamHeader::new(StreamKind::Control, None),
            bytes,
        })
        .is_err()
    {
        return Err(PolicyDiagnostic::QueueFull(primary));
    }
    Ok(())
}

fn apply_snapshot(server: &Server, snapshot: HealthPolicySnapshot, initial: bool) {
    let current = **server.cluster_health_policy.load();
    if initial || current.accepts(snapshot) {
        server.cluster_health_policy.store(Arc::new(snapshot));
        server.refresh_client_health_metadata();
    }
}

async fn health_policy_task(
    server: Arc<Server>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut inbound: mpsc::Receiver<InboundParcel>,
    mut commands: mpsc::Receiver<HealthPolicyCommand>,
) {
    let primary = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let configured = **server.cluster_health_policy.load();
    let mut snapshot = if primary {
        load_policy(&policy_path(&server), configured)
    } else {
        configured
    };
    server.cluster_health_policy.store(Arc::new(snapshot));
    let persistence = primary.then(|| spawn_persistence(&server));
    let mut primary_peer = None;
    let mut pending_snapshots = HashMap::<u16, HealthPolicySnapshot>::new();
    let mut connected = HashSet::new();
    let mut admitted = HashSet::new();
    let mut authoritative_seen = false;
    let mut diagnostics = DiagnosticLimiter::default();

    loop {
        tokio::select! {
            incoming = inbound.recv() => {
                let Some(parcel) = incoming else { break };
                if !is_health_policy_message(&parcel.bytes) {
                    continue;
                }
                let Ok(message) = decode(&parcel.bytes) else {
                    if diagnostics.permits(PolicyDiagnostic::Decode(parcel.peer.0)) {
                        warn!(peer = parcel.peer.0, "cluster health policy control decode failed");
                    }
                    continue;
                };
                match message {
                    HealthPolicyMessage::RequestChange { base_version, mode }
                        if primary && admitted.contains(&parcel.peer.0) => {
                        if base_version != snapshot.version {
                            if !send_snapshot(&outbound, parcel.peer.0, snapshot)
                                && diagnostics
                                    .permits(PolicyDiagnostic::QueueFull(parcel.peer.0))
                            {
                                warn!(
                                    peer = parcel.peer.0,
                                    version = snapshot.version,
                                    "cluster health policy correction dropped: mesh queue full"
                                );
                            }
                            continue;
                        }
                        let Some(next) = snapshot.next(mode) else {
                            if diagnostics.permits(PolicyDiagnostic::VersionExhausted) {
                                error!("cluster health policy version exhausted");
                            }
                            continue;
                        };
                        snapshot = next;
                        server.cluster_health_policy.store(Arc::new(snapshot));
                        server.refresh_client_health_metadata();
                        if let Some(persistence) = &persistence {
                            persistence.send_replace(Some(snapshot));
                        }
                        broadcast_snapshot(&outbound, &admitted, snapshot, &mut diagnostics);
                    }
                    HealthPolicyMessage::Snapshot(candidate) if !primary => {
                        if primary_peer == Some(parcel.peer.0) && admitted.contains(&parcel.peer.0) {
                            apply_snapshot(&server, candidate, !authoritative_seen);
                            authoritative_seen = true;
                            snapshot = **server.cluster_health_policy.load();
                        } else {
                            pending_snapshots
                                .entry(parcel.peer.0)
                                .and_modify(|current| {
                                    if current.accepts(candidate) {
                                        *current = candidate;
                                    }
                                })
                                .or_insert(candidate);
                        }
                    }
                    _ => {}
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    HealthPolicyCommand::Set(mode) if primary => {
                        let Some(next) = snapshot.next(mode) else {
                            if diagnostics.permits(PolicyDiagnostic::VersionExhausted) {
                                error!("cluster health policy version exhausted");
                            }
                            continue;
                        };
                        snapshot = next;
                        server.cluster_health_policy.store(Arc::new(snapshot));
                        server.refresh_client_health_metadata();
                        if let Some(persistence) = &persistence {
                            persistence.send_replace(Some(snapshot));
                        }
                        broadcast_snapshot(&outbound, &admitted, snapshot, &mut diagnostics);
                    }
                    HealthPolicyCommand::Set(mode) => {
                        if let Err(key) = request_change(
                            &outbound,
                            primary_peer.filter(|peer| admitted.contains(peer)),
                            snapshot.version,
                            mode,
                        ) && diagnostics.permits(key) {
                            match key {
                                PolicyDiagnostic::NoPrimary => warn!("cluster health policy request dropped: primary is not connected and admitted"),
                                PolicyDiagnostic::QueueFull(primary) => warn!(primary, "cluster health policy request dropped: mesh queue full"),
                                PolicyDiagnostic::Encode => warn!("cluster health policy request encoding failed"),
                                _ => {}
                            }
                        }
                    }
                    HealthPolicyCommand::PublishTo(peer) if primary => {
                        connected.insert(peer);
                        admitted.insert(peer);
                        if !send_snapshot(&outbound, peer, snapshot)
                            && diagnostics.permits(PolicyDiagnostic::QueueFull(peer))
                        {
                            warn!(peer, version = snapshot.version, "cluster health policy snapshot dropped: mesh queue full");
                        }
                    }
                    HealthPolicyCommand::PeerConnected(peer) => {
                        connected.insert(peer);
                    }
                    HealthPolicyCommand::PrimaryObserved(peer) if !primary => {
                        connected.insert(peer);
                        primary_peer = Some(peer);
                        if admitted.contains(&peer)
                            && let Some(candidate) = pending_snapshots.remove(&peer)
                        {
                            apply_snapshot(&server, candidate, !authoritative_seen);
                            authoritative_seen = true;
                            snapshot = **server.cluster_health_policy.load();
                        }
                    }
                    HealthPolicyCommand::PeerAdmitted(peer) => {
                        connected.insert(peer);
                        admitted.insert(peer);
                        if !primary && primary_peer == Some(peer)
                            && let Some(candidate) = pending_snapshots.remove(&peer)
                        {
                            apply_snapshot(&server, candidate, !authoritative_seen);
                            authoritative_seen = true;
                            snapshot = **server.cluster_health_policy.load();
                        }
                    }
                    HealthPolicyCommand::PeerDisconnected(peer) => {
                        connected.remove(&peer);
                        admitted.remove(&peer);
                        if primary_peer == Some(peer) {
                            primary_peer = None;
                        }
                        pending_snapshots.remove(&peer);
                    }
                    HealthPolicyCommand::LocalAdmitted => {
                        admitted.extend(connected.iter().copied());
                        if primary {
                            broadcast_snapshot(
                                &outbound,
                                &admitted,
                                snapshot,
                                &mut diagnostics,
                            );
                        } else if let Some(peer) = primary_peer
                            && admitted.contains(&peer)
                            && let Some(candidate) = pending_snapshots.remove(&peer)
                        {
                            apply_snapshot(&server, candidate, !authoritative_seen);
                            authoritative_seen = true;
                            snapshot = **server.cluster_health_policy.load();
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

pub fn spawn_health_policy(
    server: &Arc<Server>,
    outbound: mpsc::Sender<OutboundParcel>,
    inbound: mpsc::Receiver<InboundParcel>,
) {
    let (command_tx, command_rx) = mpsc::channel(32);
    if server.cluster_health_policy_tx.set(command_tx).is_err() {
        error!("cluster health policy owner already installed");
        return;
    }
    server.spawn_task(health_policy_task(
        Arc::clone(server),
        outbound,
        inbound,
        command_rx,
    ));
}
