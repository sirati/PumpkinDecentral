use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use pumpkin_cluster::health_policy::{
    HealthMaskMode, HealthPolicyControl, HealthPolicyControlMessage as Message,
    HealthPolicyMessage, HealthPolicySnapshot, HealthPolicyState, decode, decode_control,
    encode_control, is_health_policy_message,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_config::ClusterRole;
use tokio::sync::{mpsc, watch};
use tracing::{error, warn};

use super::{Server, cluster_control_registers as control};

const POLICY_FILE: &str = "cluster-policy.bin";
const INPUT_CAPACITY: usize = 256;
const CHANGE_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy)]
pub enum HealthPolicyCommand {
    Set(HealthMaskMode),
}

pub(crate) fn policy_path(server: &Server) -> PathBuf {
    server
        .basic_config
        .get_world_path()
        .join("data")
        .join(POLICY_FILE)
}

pub(crate) fn load_policy(
    path: &Path,
    fallback: HealthPolicySnapshot,
) -> (HealthPolicySnapshot, Option<HealthPolicyState>) {
    let Ok(bytes) = std::fs::read(path) else {
        return (fallback, None);
    };
    if let Ok(HealthPolicyControl {
        message: Message::State(state),
        ..
    }) = decode_control(&bytes)
    {
        return (state.snapshot, Some(state));
    }
    if let Ok(HealthPolicyMessage::Snapshot(snapshot)) = decode(&bytes) {
        return (snapshot, None);
    }
    warn!(path = %path.display(), "cluster health policy file is invalid; using configured default");
    (fallback, None)
}

fn save_policy(path: &Path, state: HealthPolicyState) -> std::io::Result<()> {
    use std::io::Write;
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing policy directory"))?;
    std::fs::create_dir_all(directory)?;
    let temporary = directory.join(format!(".health-policy-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let bytes = encode_control(HealthPolicyControl {
            sender: state.source,
            message: Message::State(state),
        })
        .map_err(std::io::Error::other)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn spawn_persistence(server: &Arc<Server>) -> watch::Sender<Option<HealthPolicyState>> {
    let (tx, mut rx) = watch::channel::<Option<HealthPolicyState>>(None);
    let path = policy_path(server);
    server.spawn_task(async move {
        let mut diagnostics = control::Diagnostics::default();
        while rx.changed().await.is_ok() {
            let Some(state) = *rx.borrow_and_update() else {
                continue;
            };
            let path = path.clone();
            match tokio::task::spawn_blocking(move || save_policy(&path, state)).await {
                Ok(Ok(())) => {}
                _ => {
                    diagnostics.rejected("health_policy", 0, "primary persistence failed");
                }
            }
        }
    });
    tx
}

fn local_source(server: &Server) -> Option<PeerGeneration> {
    let server_id = ServerId(server.advanced_config.cluster.server_id);
    server
        .cluster_peer_incarnations
        .load()
        .get(&server_id)
        .copied()
        .filter(|boot| *boot != 0)
        .map(|generation| PeerGeneration {
            server: server_id,
            generation,
        })
}

fn source_matches(
    peer: ServerId,
    transport: u64,
    published_transport: Option<u64>,
    published_boot: Option<u64>,
    sender: PeerGeneration,
) -> bool {
    sender.server == peer
        && published_transport == Some(transport)
        && sender.generation != 0
        && published_boot == Some(sender.generation)
}

fn authenticated(server: &Server, parcel: &InboundParcel, sender: PeerGeneration) -> bool {
    source_matches(
        parcel.peer,
        parcel.generation,
        server
            .cluster_peer_generations
            .load()
            .get(&parcel.peer)
            .copied(),
        server
            .cluster_peer_incarnations
            .load()
            .get(&parcel.peer)
            .copied(),
        sender,
    )
}

fn qualified_historical_state(
    server: &Server,
    primary: PeerGeneration,
    state: HealthPolicyState,
) -> bool {
    super::cluster_world_rules::disciplined_slot()
        .and_then(|slot| super::cluster_environment::authority(server, primary, slot))
        .is_some_and(|cut| cut.health == state)
}

fn historical_source_ready(server: &Server, parcel: &InboundParcel) -> bool {
    let Ok(control) = decode_control(&parcel.bytes) else {
        return true;
    };
    let Message::State(state) = control.message else {
        return true;
    };
    if state.source == control.sender {
        return true;
    }
    super::cluster_world_rules::disciplined_slot()
        .and_then(|slot| super::cluster_environment::authority(server, control.sender, slot))
        .is_some()
}

fn send(
    outbound: &mpsc::Sender<OutboundParcel>,
    peer: ServerId,
    sender: PeerGeneration,
    message: Message,
) -> bool {
    let Ok(bytes) = encode_control(HealthPolicyControl { sender, message }) else {
        return false;
    };
    outbound
        .try_send(OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::Control, None),
            bytes,
        })
        .is_ok()
}

pub fn render_cut(server: &Server) -> (Option<PeerGeneration>, HealthPolicySnapshot) {
    server.cluster_health_policy_state.load_full().map_or_else(
        || (None, **server.cluster_health_policy.load()),
        |state| (Some(state.source), state.snapshot),
    )
}

fn publish(server: &Server, state: HealthPolicyState) {
    server
        .cluster_health_policy_state
        .store(Some(Arc::new(state)));
    server.cluster_health_policy.store(Arc::new(state.snapshot));
}

fn merge_state(
    current: Option<HealthPolicyState>,
    candidate: HealthPolicyState,
) -> Result<bool, &'static str> {
    let Some(current) = current else {
        return Ok(true);
    };
    if candidate.snapshot.version > current.snapshot.version {
        return Ok(true);
    }
    if candidate.snapshot.version == current.snapshot.version
        && candidate.snapshot.mode != current.snapshot.mode
    {
        return Err("contradictory policy version");
    }
    Ok(false)
}

fn next_change(
    snapshot: HealthPolicySnapshot,
    source: PeerGeneration,
    baseline_ready: bool,
    changes: &mut VecDeque<HealthMaskMode>,
) -> Result<Option<HealthPolicyState>, &'static str> {
    if !baseline_ready {
        return Ok(None);
    }
    let Some(mode) = changes.pop_front() else {
        return Ok(None);
    };
    snapshot
        .next(mode)
        .map(|snapshot| Some(HealthPolicyState { source, snapshot }))
        .ok_or("policy version exhausted")
}

async fn health_policy_task(
    server: Arc<Server>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut inbound: mpsc::Receiver<InboundParcel>,
    mut commands: mpsc::Receiver<HealthPolicyCommand>,
) {
    let primary = matches!(server.advanced_config.cluster.role, ClusterRole::Primary);
    let configured = **server.cluster_health_policy.load();
    let mut snapshot = configured;
    let mut state: Option<HealthPolicyState> = None;
    let mut baseline_ready = false;
    let persistence = primary.then(|| spawn_persistence(&server));

    let mut states = HashMap::<u16, Message>::new();
    let mut changes = VecDeque::<HealthMaskMode>::new();
    let mut pending = VecDeque::<InboundParcel>::new();
    let mut diagnostics = control::Diagnostics::default();
    let mut epochs = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_millis(50));
    let mut shutting_down = false;
    #[cfg(feature = "cluster-e2e")]
    let mut observer = super::cluster_test_health_policy::HealthPolicyObserver::default();

    loop {
        if server.cluster_global_shutdown.load(Ordering::Relaxed) && !shutting_down {
            shutting_down = true;
            commands.close();
            inbound.close();
        }
        let source = local_source(&server);
        let primary_peer =
            super::cluster_world_rules::primary(&server).map(|source| source.server.0);
        let admitted: HashSet<u16> = server
            .cluster_connected_peers
            .load()
            .iter()
            .map(|peer| peer.0)
            .collect();

        if primary
            && !baseline_ready
            && let (Some(source), Some(slot)) =
                (source, super::cluster_world_rules::disciplined_slot())
            && let Some(cut) = super::cluster_environment::authority(&server, source, slot)
        {
            let initial = cut.health;
            snapshot = initial.snapshot;
            state = Some(initial);
            baseline_ready = true;
            publish(&server, initial);
            if let Some(persistence) = &persistence {
                persistence.send_replace(Some(initial));
            }
            for peer in &admitted {
                states.insert(*peer, Message::State(initial));
            }
            #[cfg(feature = "cluster-e2e")]
            observer.applied(&server, initial.snapshot, initial.source, true);
        }
        #[cfg(feature = "cluster-e2e")]
        observer.drain(&server, snapshot);
        if let Some(source) = source {
            let generations = server.cluster_peer_generations.load();
            for (peer, epoch) in generations.iter() {
                if epochs.insert(*peer, *epoch) != Some(*epoch)
                    && let Some(current) = state
                {
                    if primary && admitted.contains(&peer.0) {
                        states.insert(peer.0, Message::State(current));
                    }
                }
            }
            drop(generations);
            let ready: Vec<_> = states.keys().copied().collect();
            for peer in ready {
                if server
                    .cluster_peer_generations
                    .load()
                    .contains_key(&ServerId(peer))
                    && send(&outbound, ServerId(peer), source, states[&peer])
                {
                    states.remove(&peer);
                }
            }
            if !primary && let Some(peer) = primary_peer.filter(|peer| admitted.contains(peer)) {
                while let Some(mode) = changes.front().copied() {
                    if !send(&outbound, ServerId(peer), source, Message::Change(mode)) {
                        break;
                    }
                    changes.pop_front();
                }
            }
        }
        if shutting_down && commands.is_empty() && changes.is_empty() && states.is_empty() {
            break;
        }

        let ready_index = pending.iter().position(|parcel| {
            let generations = server.cluster_peer_generations.load();
            let boots = server.cluster_peer_incarnations.load();
            control::control_publication_ready(
                parcel.generation,
                generations.get(&parcel.peer).copied(),
                boots.get(&parcel.peer).copied(),
            ) && (primary || primary_peer.is_some())
                && historical_source_ready(&server, parcel)
        });
        if let Some(index) = ready_index.filter(|_| changes.len() < CHANGE_CAPACITY) {
            let parcel = pending.remove(index).unwrap();
            let Ok(control) = decode_control(&parcel.bytes) else {
                diagnostics.rejected("health_policy", parcel.peer.0, "malformed control");
                continue;
            };
            if !admitted.contains(&parcel.peer.0)
                || !authenticated(&server, &parcel, control.sender)
            {
                diagnostics.rejected("health_policy", parcel.peer.0, "stale or forged source");
                continue;
            }
            match control.message {
                Message::Change(mode) if primary => changes.push_back(mode),
                Message::State(candidate) if !primary && primary_peer == Some(parcel.peer.0) => {
                    if candidate.source != control.sender
                        && !qualified_historical_state(&server, control.sender, candidate)
                    {
                        diagnostics.rejected(
                            "health_policy",
                            parcel.peer.0,
                            "uncertified historical policy",
                        );
                        continue;
                    }
                    match merge_state(state, candidate) {
                        Ok(true) => {
                            snapshot = candidate.snapshot;
                            state = Some(candidate);
                            publish(&server, candidate);
                            #[cfg(feature = "cluster-e2e")]
                            observer.applied(&server, snapshot, candidate.source, false);
                        }
                        Ok(false) => {
                            if state.is_some_and(|current| {
                                candidate.snapshot.version < current.snapshot.version
                            }) {
                                diagnostics.rejected(
                                    "health_policy",
                                    parcel.peer.0,
                                    "stale policy version",
                                );
                            }
                        }
                        Err(reason) => diagnostics.rejected("health_policy", parcel.peer.0, reason),
                    }
                }
                _ => diagnostics.rejected(
                    "health_policy",
                    parcel.peer.0,
                    "wrong role or policy namespace",
                ),
            }
        }
        if primary && let Some(source) = source {
            match next_change(snapshot, source, baseline_ready, &mut changes) {
                Ok(Some(next)) => {
                    snapshot = next.snapshot;
                    state = Some(next);
                    publish(&server, next);
                    if let Some(persistence) = &persistence {
                        persistence.send_replace(Some(next));
                    }
                    for peer in &admitted {
                        states.insert(*peer, Message::State(next));
                    }
                    #[cfg(feature = "cluster-e2e")]
                    observer.applied(&server, snapshot, source, true);
                }
                Ok(None) => {}
                Err(reason) => diagnostics.rejected("health_policy", source.server.0, reason),
            }
        }
        tokio::select! {
            _ = interval.tick() => {},
            incoming = inbound.recv(), if !shutting_down && !inbound.is_closed() && pending.len() < INPUT_CAPACITY && changes.len() < CHANGE_CAPACITY => {
                if let Some(parcel) = incoming { if is_health_policy_message(&parcel.bytes) { if pending.len() + 1 == INPUT_CAPACITY { diagnostics.deferred("health_policy", parcel.peer.0, "control retention capacity"); } pending.push_back(parcel); } }
            }
            command = commands.recv(), if source.is_some() && changes.len() < CHANGE_CAPACITY && (!commands.is_closed() || !commands.is_empty()) => {
                let Some(command) = command else {
                    if !shutting_down { shutting_down = true; inbound.close(); }
                    continue;
                };
                match command {
                    HealthPolicyCommand::Set(mode) => changes.push_back(mode),

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

#[cfg(test)]
mod tests {
    use super::*;

    fn state(version: u64, mode: HealthMaskMode, boot: u64) -> HealthPolicyState {
        HealthPolicyState {
            source: PeerGeneration {
                server: ServerId(0),
                generation: boot,
            },
            snapshot: HealthPolicySnapshot::new(version, mode),
        }
    }

    #[test]
    fn queued_changes_preserve_intent_until_certified_survivor_baseline() {
        let source = PeerGeneration {
            server: ServerId(0),
            generation: 44,
        };
        let mut changes = VecDeque::from([HealthMaskMode::Off, HealthMaskMode::All]);
        let cold = HealthPolicySnapshot::new(0, HealthMaskMode::Players);
        assert_eq!(next_change(cold, source, false, &mut changes), Ok(None));
        assert_eq!(changes.len(), 2);
        let certified = state(17, HealthMaskMode::Players, 43);
        let first = next_change(certified.snapshot, source, true, &mut changes)
            .unwrap()
            .unwrap();
        assert_eq!(first, state(18, HealthMaskMode::Off, 44));
        let second = next_change(first.snapshot, source, true, &mut changes)
            .unwrap()
            .unwrap();
        assert_eq!(second, state(19, HealthMaskMode::All, 44));
        assert!(changes.is_empty());
    }

    #[test]
    fn authenticated_control_binds_original_sender_to_exact_transport_and_boot() {
        let sender = PeerGeneration {
            server: ServerId(7),
            generation: 33,
        };
        assert!(source_matches(ServerId(7), 4, Some(4), Some(33), sender));
        assert!(!source_matches(ServerId(8), 4, Some(4), Some(33), sender));
        assert!(!source_matches(ServerId(7), 3, Some(4), Some(33), sender));
        assert!(!source_matches(ServerId(7), 4, Some(3), Some(33), sender));
        assert!(!source_matches(ServerId(7), 4, Some(4), Some(34), sender));
        assert!(!source_matches(ServerId(7), 4, Some(4), None, sender));
        assert!(!source_matches(ServerId(7), 4, None, Some(33), sender));
        assert!(!source_matches(
            ServerId(7),
            4,
            Some(4),
            Some(0),
            PeerGeneration {
                generation: 0,
                ..sender
            }
        ));
    }

    #[test]
    fn survivor_version_preserves_original_issuer_and_rejects_rollback_or_conflict() {
        let original = state(17, HealthMaskMode::All, 3);
        assert_eq!(
            merge_state(Some(original), state(2, HealthMaskMode::Players, 4)),
            Ok(false)
        );
        assert_eq!(
            merge_state(Some(original), state(17, HealthMaskMode::Off, 4)),
            Err("contradictory policy version")
        );
        assert_eq!(merge_state(Some(original), original), Ok(false));
        assert_eq!(
            merge_state(Some(original), state(18, HealthMaskMode::Off, 4)),
            Ok(true)
        );
        let bytes = encode_control(HealthPolicyControl {
            sender: PeerGeneration {
                server: ServerId(8),
                generation: 99,
            },
            message: Message::Seed(original),
        })
        .unwrap();
        let decoded = decode_control(&bytes).unwrap();
        assert_eq!(decoded.message, Message::Seed(original));
        assert_ne!(decoded.sender, original.source);
    }

    #[test]
    fn durable_policy_restart_keeps_source_and_replaces_complete_state() {
        let directory =
            std::env::temp_dir().join(format!("health-policy-{}", uuid::Uuid::new_v4()));
        let path = directory.join("data").join(POLICY_FILE);
        let original = state(7, HealthMaskMode::All, 31);
        save_policy(&path, original).unwrap();
        assert_eq!(
            load_policy(&path, HealthPolicySnapshot::new(0, HealthMaskMode::Players)),
            (original.snapshot, Some(original))
        );
        let next = state(8, HealthMaskMode::Off, 32);
        save_policy(&path, next).unwrap();
        assert_eq!(
            load_policy(&path, original.snapshot),
            (next.snapshot, Some(next))
        );
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
