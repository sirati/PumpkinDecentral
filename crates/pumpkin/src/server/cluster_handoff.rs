use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use pumpkin_cluster::handoff::{
    HandoffMessage, PlayerResumeHint, decode_message, encode_message, is_handoff_payload,
};
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_nbt::Nbt;
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_nbt::tag::NbtTag;
use tokio::sync::mpsc;
use tracing::{error, warn};
use uuid::Uuid;

use super::Server;
use crate::entity::NBTStorage;
use crate::entity::player::Player;

const REVISION_KEY: &str = "PumpkinClusterPlayerdataRevision";
const HINT_KEY: &str = "PumpkinClusterResumeHint";
const OUTBOUND_CAPACITY: usize = 4096;
const MAX_PENDING_COMMIT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_COMMITS: usize = 65_536;

#[derive(Debug, Clone)]
pub struct RevisionedPlayerdata {
    pub token: u64,
    pub revision: u64,
    pub data: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitOutcome {
    pub token: u64,
    pub revision: u64,
    pub accepted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct HandoffSnapshot {
    #[cfg(feature = "cluster-e2e")]
    pub revision_mismatch_armed: Option<Uuid>,
    pub hints: HashMap<Uuid, PlayerResumeHint>,
    pub missing_hints: HashSet<Uuid>,
    pub failed_hints: HashSet<Uuid>,
    pub playerdata: HashMap<Uuid, RevisionedPlayerdata>,
    pub missing_playerdata: HashMap<Uuid, (u64, u64)>,
    pub failed_playerdata: HashMap<Uuid, u64>,
    pub commit_outcomes: HashMap<(Uuid, u64), CommitOutcome>,
    commit_observers: HashMap<(Uuid, u64), std::sync::Weak<crate::net::ClientPlatform>>,
}

#[derive(Debug)]
pub enum HandoffCommand {
    #[cfg(feature = "cluster-e2e")]
    ArmRevisionMismatch {
        uuid: Uuid,
    },
    PrimaryConnected(ServerId),
    PeerConnected(ServerId),
    ReleaseCommitOutcome {
        uuid: Uuid,
        token: u64,
    },
    PeerDropped(ServerId),
    RequestHint {
        uuid: Uuid,
    },
    RequestPlayerdata {
        uuid: Uuid,
        revision: u64,
        token: u64,
    },
    Commit {
        token: u64,
        hint: PlayerResumeHint,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffCommit {
    observer: Option<std::sync::Weak<crate::net::ClientPlatform>>,
    token: u64,
    hint: PlayerResumeHint,
    data: Arc<[u8]>,
}

fn observe_commit(state: &mut HandoffSnapshot, commit: &HandoffCommit) {
    if let Some(observer) = commit.observer.as_ref() {
        state.commit_observers.insert(
            (Uuid::from_bytes(commit.hint.uuid), commit.token),
            observer.clone(),
        );
    }
}

fn prune_commit_outcomes(
    state: &mut HandoffSnapshot,
    commits: &HashMap<Uuid, Arc<HandoffCommit>>,
) -> bool {
    let before = state.commit_outcomes.len();
    state.commit_outcomes.retain(|key, outcome| {
        (!outcome.accepted
            && commits
                .get(&key.0)
                .is_some_and(|commit| commit.token == key.1))
            || state
                .commit_observers
                .get(key)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|client| !client.closed())
    });
    state
        .commit_observers
        .retain(|key, _| state.commit_outcomes.contains_key(key));
    state.commit_outcomes.len() != before
}

pub(crate) fn pending_commit(server: &Server, uuid: Uuid) -> Option<(u64, String)> {
    server
        .cluster_handoff_commits
        .load()
        .get(&uuid)
        .map(|commit| (commit.token, commit.hint.dimension.clone()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementCommitError {
    Capacity,
    Unavailable,
    InvalidSnapshot,
}

pub fn commit_logout(
    server: &Server,
    player: &Player,
    chunks: &[pumpkin_cluster::protocol::ChunkKey],
    retained_until_millis: u64,
    logout: bool,
    frozen: &super::cluster_controller::FrozenPlayerDeparture,
) -> Option<u64> {
    match try_commit_logout(
        server,
        player,
        chunks,
        retained_until_millis,
        logout,
        frozen,
    ) {
        Ok(token) => Some(token),
        Err(error) => {
            tracing::error!(gid = ?frozen.lease.gid, ?error, "cluster departure retirement commit unavailable");
            None
        }
    }
}

pub(crate) fn try_commit_logout(
    server: &Server,
    player: &Player,
    chunks: &[pumpkin_cluster::protocol::ChunkKey],
    retained_until_millis: u64,
    logout: bool,
    frozen: &super::cluster_controller::FrozenPlayerDeparture,
) -> Result<u64, RetirementCommitError> {
    let retire_route = server
        .cluster_actor_statistics_retire_route
        .load_full()
        .ok_or(RetirementCommitError::Unavailable)?;
    let retire_permit = retire_route.try_reserve().map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => RetirementCommitError::Capacity,
        mpsc::error::TrySendError::Closed(_) => RetirementCommitError::Unavailable,
    })?;
    let token = server
        .cluster_lobby_session_generation
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if server.cluster_handoff.load().commit_outcomes.len() >= MAX_PENDING_COMMITS {
        server
            .cluster_handoff_commit_wake
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        return Err(RetirementCommitError::Capacity);
    }
    let mut snapshot =
        super::cluster_lobby::local_snapshot_from_player(player, server, frozen, None)
            .ok_or(RetirementCommitError::InvalidSnapshot)?;
    snapshot.hint.retained_until_millis = retained_until_millis;
    snapshot.hint.retained_chunks = chunks
        .iter()
        .filter(|chunk| chunk.world == frozen.spatial.world)
        .copied()
        .collect();
    let data = snapshot.data;
    if data.len() > pumpkin_cluster::handoff::MAX_PLAYERDATA_BYTES {
        return Err(RetirementCommitError::InvalidSnapshot);
    }
    let commit = Arc::new(HandoffCommit {
        observer: (!logout).then(|| Arc::downgrade(&player.client)),
        token,
        hint: snapshot.hint,
        data: data.into(),
    });
    let mut published = false;
    server.cluster_handoff_commits.rcu(|current| {
        published = false;
        let previous = current
            .get(&player.gameprofile.id)
            .map_or(0, |entry| entry.data.len());
        let retained_bytes = current.values().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.data.len())
        });
        if (!current.contains_key(&player.gameprofile.id) && current.len() >= MAX_PENDING_COMMITS)
            || retained_bytes
                .saturating_sub(previous)
                .saturating_add(commit.data.len())
                > MAX_PENDING_COMMIT_BYTES
        {
            return (**current).clone();
        }
        let mut next = (**current).clone();
        next.insert(player.gameprofile.id, commit.clone());
        published = true;
        next
    });
    if !published {
        return Err(RetirementCommitError::Capacity);
    }
    retire_permit.send(super::cluster_actor_statistics::ActorStatisticsRetire {
        actor: frozen.lease.gid,
        player_key: frozen.lease.key.0,
        incarnation: frozen.lease.incarnation,
        install_generation: player
            .cluster_owner_install_generation
            .load(std::sync::atomic::Ordering::Acquire),
    });
    server
        .cluster_handoff_commit_wake
        .send_modify(|revision| *revision = revision.wrapping_add(1));
    Ok(token)
}

pub(crate) fn serialize_nbt(data: NbtCompound) -> Vec<u8> {
    Nbt::from(data).write_unnamed().to_vec()
}

pub(crate) fn deserialize_nbt(data: &[u8]) -> Option<NbtCompound> {
    let mut cursor = Cursor::new(data);
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    Nbt::read_unnamed(&mut reader).ok().map(|nbt| nbt.root_tag)
}

pub(crate) fn deserialize_retired_playerdata(
    data: &[u8],
    hint: &PlayerResumeHint,
) -> Option<NbtCompound> {
    let mut data = deserialize_nbt(data)?;
    put_handoff_metadata(&mut data, hint, hint.playerdata_revision).then_some(data)
}

fn revision(data: &NbtCompound) -> u64 {
    data.get_long(REVISION_KEY)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0)
}

fn stored_hint(data: &NbtCompound) -> Option<PlayerResumeHint> {
    let bytes = data.get_byte_array(HINT_KEY)?;
    let bytes: Vec<u8> = bytes.iter().map(|byte| *byte as u8).collect();
    postcard::from_bytes(&bytes).ok()
}

fn put_handoff_metadata(data: &mut NbtCompound, hint: &PlayerResumeHint, revision: u64) -> bool {
    let Ok(encoded) = postcard::to_allocvec(hint) else {
        return false;
    };
    let encoded = encoded
        .into_iter()
        .map(|byte| byte as i8)
        .collect::<Vec<_>>()
        .into_boxed_slice();
    data.put_long(REVISION_KEY, i64::try_from(revision).unwrap_or(i64::MAX));
    data.put(HINT_KEY, NbtTag::ByteArray(encoded));
    true
}

fn fallback_hint(
    server: &Server,
    uuid: Uuid,
    data: &NbtCompound,
    revision: u64,
) -> PlayerResumeHint {
    let dimension = data
        .get_string("Dimension")
        .map_or_else(|| "minecraft:overworld".to_string(), ToOwned::to_owned);
    let position = data.get_list("Pos").map_or([0.0, 64.0, 0.0], |values| {
        [
            values
                .first()
                .and_then(NbtTag::extract_double)
                .unwrap_or(0.0),
            values
                .get(1)
                .and_then(NbtTag::extract_double)
                .unwrap_or(64.0),
            values
                .get(2)
                .and_then(NbtTag::extract_double)
                .unwrap_or(0.0),
        ]
    });
    let rotation = data.get_list("Rotation").map_or([0.0, 0.0], |values| {
        [
            values
                .first()
                .and_then(NbtTag::extract_float)
                .unwrap_or(0.0),
            values.get(1).and_then(NbtTag::extract_float).unwrap_or(0.0),
        ]
    });
    PlayerResumeHint {
        uuid: *uuid.as_bytes(),
        dimension,
        position,
        rotation,
        view_radius: server.advanced_config.networking.java.view_distance.get(),
        playerdata_revision: revision,
        last_owner: None,
        retained_chunks: Vec::new(),
        retained_until_millis: 0,
    }
}

fn send_to(outbound: &mut VecDeque<OutboundParcel>, peer: ServerId, message: &HandoffMessage) {
    let Ok(bytes) = encode_message(message) else {
        return;
    };
    outbound.push_back(OutboundParcel {
        peer,
        header: StreamHeader::new(StreamKind::Control, None),
        bytes,
    });
}

fn broadcast(outbound: &mut VecDeque<OutboundParcel>, peers: &[u16], message: &HandoffMessage) {
    for peer in peers {
        send_to(outbound, ServerId(*peer), message);
    }
}

fn take_hint_request(
    pending: &mut HashMap<u64, (Uuid, ServerId, u64)>,
    request_id: u64,
    source: ServerId,
    generation: u64,
) -> Option<Uuid> {
    let (uuid, expected_source, expected_generation) = pending.get(&request_id)?;
    if *expected_source != source || *expected_generation != generation {
        return None;
    }
    let uuid = *uuid;
    pending.remove(&request_id);
    Some(uuid)
}

fn take_playerdata_request(
    pending: &mut HashMap<u64, (Uuid, u64, u64, ServerId, u64)>,
    request_id: u64,
    source: ServerId,
    generation: u64,
) -> Option<(Uuid, u64, u64)> {
    let (uuid, revision, token, expected_source, expected_generation) = pending.get(&request_id)?;
    if *expected_source != source || *expected_generation != generation {
        return None;
    }
    let result = (*uuid, *revision, *token);
    pending.remove(&request_id);
    Some(result)
}

fn take_commit_request(
    pending: &mut HashMap<u64, (Arc<HandoffCommit>, ServerId, u64)>,
    request_id: u64,
    source: ServerId,
    generation: u64,
    received: Uuid,
) -> Option<Arc<HandoffCommit>> {
    let (commit, expected_source, expected_generation) = pending.get(&request_id)?;
    if *expected_source != source
        || *expected_generation != generation
        || Uuid::from_bytes(commit.hint.uuid) != received
    {
        return None;
    }
    pending.remove(&request_id).map(|(commit, _, _)| commit)
}

enum PersistenceCommand {
    Handle(ServerId, u64, HandoffMessage),
}

enum PersistenceResult {
    CatalogHint(PlayerResumeHint),
    Reply(ServerId, u64, HandoffMessage),
    Commit(ServerId, u64, HandoffMessage, Option<PlayerResumeHint>),
}

fn primary_message(
    server: &Server,
    peer: ServerId,
    generation: u64,
    message: HandoffMessage,
    advancement_failures: &mut HashMap<Uuid, std::time::Instant>,
) -> Option<PersistenceResult> {
    if server.cluster_peer_generations.load().get(&peer).copied() != Some(generation) {
        return None;
    }
    match message {
        HandoffMessage::ResumeHintRequest {
            request_id,
            requester,
            uuid,
        } => {
            if requester != peer.0 {
                return None;
            }
            let uuid = Uuid::from_bytes(uuid);
            let hint = server
                .player_data_storage
                .load_data(&uuid)
                .ok()
                .flatten()
                .map(|data| {
                    let revision = revision(&data);
                    stored_hint(&data)
                        .unwrap_or_else(|| fallback_hint(server, uuid, &data, revision))
                });
            Some(PersistenceResult::Reply(
                peer,
                generation,
                HandoffMessage::ResumeHintResponse { request_id, hint },
            ))
        }
        HandoffMessage::PlayerdataRequest {
            request_id,
            requester,
            uuid,
            ..
        } => {
            if requester != peer.0 {
                return None;
            }
            let player_uuid = Uuid::from_bytes(uuid);
            Some(PersistenceResult::Reply(
                peer,
                generation,
                match server.player_data_storage.load_data(&player_uuid) {
                    Ok(Some(mut data)) => {
                        let state = if data.get_byte_array("PumpkinClusterAdvancements").is_some() {
                            super::cluster_advancements::read_nbt(&data).ok_or_else(|| {
                                "invalid authoritative advancement supplement".to_owned()
                            })
                        } else {
                            super::cluster_advancements::load_primary_player(
                                &server
                                    .advancement_manager
                                    .advancement_path
                                    .join(format!("{player_uuid}.json")),
                                &data,
                            )
                        };
                        match state {
                            Ok(state)
                                if super::cluster_advancements::write_nbt(&mut data, &state) =>
                            {
                                HandoffMessage::PlayerdataResponse {
                                    request_id,
                                    uuid,
                                    revision: revision(&data),
                                    data: serialize_nbt(data),
                                }
                            }
                            result => {
                                let now = std::time::Instant::now();
                                advancement_failures.retain(|_, previous| {
                                    now.duration_since(*previous).as_secs() < 300
                                });
                                if !advancement_failures.contains_key(&player_uuid)
                                    && advancement_failures.len() < 4096
                                {
                                    advancement_failures.insert(player_uuid, now);
                                    error!(%player_uuid, ?result, "cluster authoritative advancement import failed");
                                }
                                HandoffMessage::PlayerdataUnavailable {
                                    request_id,
                                    uuid,
                                    revision: revision(&data),
                                }
                            }
                        }
                    }
                    _ => HandoffMessage::PlayerdataUnavailable {
                        request_id,
                        uuid,
                        revision: 0,
                    },
                },
            ))
        }
        HandoffMessage::CommitPlayerdata {
            request_id,
            expected_revision,
            mut hint,
            data,
        } => {
            let uuid = Uuid::from_bytes(hint.uuid);
            let current = server
                .player_data_storage
                .load_data(&uuid)
                .ok()
                .flatten()
                .map_or(0, |data| revision(&data));
            let mut accepted = current == expected_revision;
            let next = current.wrapping_add(u64::from(accepted));
            if accepted {
                hint.playerdata_revision = next;
                if let Some(mut saved) = deserialize_nbt(&data)
                    && put_handoff_metadata(&mut saved, &hint, next)
                {
                    if let Err(error) = server.player_data_storage.save_cluster_data(&uuid, saved) {
                        error!(%uuid, %error, "cluster playerdata commit failed");
                        accepted = false;
                    }
                } else {
                    accepted = false;
                }
            }
            let result = HandoffMessage::CommitResult {
                request_id,
                uuid: *uuid.as_bytes(),
                previous_revision: current,
                revision: if accepted { next } else { current },
                accepted,
            };
            Some(PersistenceResult::Commit(
                peer,
                generation,
                result,
                accepted.then_some(hint),
            ))
        }
        _ => None,
    }
}

fn seed_hints(
    outbound: &mut VecDeque<OutboundParcel>,
    hints: &BTreeMap<Uuid, PlayerResumeHint>,
    seeds: &mut HashMap<ServerId, (u64, Option<Uuid>)>,
    generations: &HashMap<ServerId, u64>,
) {
    seeds.retain(|peer, (generation, _)| generations.get(peer).copied() == Some(*generation));
    let mut completed = Vec::new();
    for (peer, (_, cursor)) in seeds.iter_mut() {
        if outbound.len() >= OUTBOUND_CAPACITY {
            break;
        }
        let next = match cursor {
            Some(uuid) => hints
                .range((std::ops::Bound::Excluded(*uuid), std::ops::Bound::Unbounded))
                .next(),
            None => hints.first_key_value(),
        };
        if let Some((uuid, hint)) = next {
            send_to(
                outbound,
                *peer,
                &HandoffMessage::ResumeHintPublished(hint.clone()),
            );
            *cursor = Some(*uuid);
        } else {
            completed.push(*peer);
        }
    }
    for peer in completed {
        seeds.remove(&peer);
    }
}

pub async fn run(
    server: Arc<Server>,
    local: ServerId,
    _peers: Vec<u16>,
    outbound: mpsc::Sender<OutboundParcel>,
    mut commands: mpsc::Receiver<HandoffCommand>,
    mut inbound: mpsc::Receiver<InboundParcel>,
) {
    let primary = server.is_cluster_primary();
    let mut state = HandoffSnapshot::default();
    #[cfg(feature = "cluster-e2e")]
    let mut revision_mismatch = None;
    let mut next_request = 1_u64;
    let mut pending_hints = HashMap::<u64, (Uuid, ServerId, u64)>::new();
    let mut pending_playerdata = HashMap::<u64, (Uuid, u64, u64, ServerId, u64)>::new();
    let mut pending_commits = HashMap::<u64, (Arc<HandoffCommit>, ServerId, u64)>::new();
    let mut queued_commits = HashMap::<Uuid, Arc<HandoffCommit>>::new();
    let mut pending_outbound = VecDeque::new();
    let mut primary_hints = BTreeMap::<Uuid, PlayerResumeHint>::new();
    let mut hint_seeds = HashMap::<ServerId, (u64, Option<Uuid>)>::new();
    let mut pending_persistence = VecDeque::new();
    let mut primary_peers = (**server.cluster_primary_peers.load()).clone();
    let mut waiting = VecDeque::new();
    let mut commit_wake = server.cluster_handoff_commit_wake.subscribe();
    let (persistence_tx, mut persistence_rx) = mpsc::channel(256);
    let persistence_command_tx = if primary {
        let (command_tx, mut command_rx) = mpsc::channel(256);
        let persistence_server = server.clone();
        tokio::task::spawn_blocking(move || {
            let mut advancement_failures = HashMap::new();
            let mut catalog = match persistence_server.player_data_storage.read_resume_catalog() {
                Ok(entries) => Some(entries),
                Err(error) => {
                    tracing::error!(?error, "cluster resume catalog could not be opened");
                    None
                }
            };
            loop {
                let command = if catalog.is_some() {
                    match command_rx.try_recv() {
                        Ok(command) => Some(command),
                        Err(mpsc::error::TryRecvError::Empty) => None,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                } else {
                    let Some(command) = command_rx.blocking_recv() else {
                        break;
                    };
                    Some(command)
                };
                if let Some(PersistenceCommand::Handle(peer, generation, message)) = command {
                    if let Some(result) = primary_message(
                        &persistence_server,
                        peer,
                        generation,
                        message,
                        &mut advancement_failures,
                    ) && persistence_tx.blocking_send(result).is_err()
                    {
                        break;
                    }
                    continue;
                }
                let Some(entry) = catalog.as_mut().and_then(Iterator::next) else {
                    catalog = None;
                    continue;
                };
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        tracing::error!(?error, "cluster resume catalog entry rejected");
                        continue;
                    }
                };
                let path = entry.path();
                if path.extension().and_then(|part| part.to_str()) != Some("dat") {
                    continue;
                }
                let Some(uuid) = path
                    .file_stem()
                    .and_then(|part| part.to_str())
                    .and_then(|part| Uuid::parse_str(part).ok())
                else {
                    continue;
                };
                let data = match persistence_server.player_data_storage.load_data(&uuid) {
                    Ok(Some(data)) => data,
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::error!(%uuid, ?error, "cluster resume catalog player rejected");
                        continue;
                    }
                };
                let revision = revision(&data);
                let hint = stored_hint(&data)
                    .unwrap_or_else(|| fallback_hint(&persistence_server, uuid, &data, revision));
                if hint.uuid != *uuid.as_bytes() || hint.playerdata_revision != revision {
                    tracing::error!(%uuid, revision, "cluster resume catalog identity rejected");
                    continue;
                }
                if persistence_tx
                    .blocking_send(PersistenceResult::CatalogHint(hint))
                    .is_err()
                {
                    break;
                }
            }
        });
        Some(command_tx)
    } else {
        None
    };
    let initial_commits = server.cluster_handoff_commits.load_full();
    for commit in initial_commits.values() {
        if let Some(target) = primary_peers.iter().copied().min_by_key(|peer| peer.0) {
            let request_id = next_request;
            next_request = next_request.wrapping_add(1);
            let generation = server
                .cluster_peer_generations
                .load()
                .get(&target)
                .copied()
                .unwrap_or(0);
            pending_commits.insert(request_id, (commit.clone(), target, generation));
            send_to(
                &mut pending_outbound,
                target,
                &HandoffMessage::CommitPlayerdata {
                    request_id,
                    expected_revision: commit.hint.playerdata_revision,
                    hint: commit.hint.clone(),
                    data: commit.data.to_vec(),
                },
            );
        } else {
            waiting.push_back(HandoffCommand::Commit {
                token: commit.token,
                hint: commit.hint.clone(),
                data: commit.data.to_vec(),
            });
        }
    }
    loop {
        if prune_commit_outcomes(&mut state, &server.cluster_handoff_commits.load()) {
            server.cluster_handoff.store(Arc::new(state.clone()));
        }
        if !pending_persistence.is_empty()
            && let Some(route) = persistence_command_tx.as_ref()
        {
            match route.try_reserve() {
                Ok(permit) => {
                    if let Some(command) = pending_persistence.pop_front() {
                        permit.send(command);
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    error!("cluster primary persistence worker unavailable");
                    break;
                }
            }
        }
        if primary {
            seed_hints(
                &mut pending_outbound,
                &primary_hints,
                &mut hint_seeds,
                &server.cluster_peer_generations.load(),
            );
        }
        tokio::select! {
            changed = commit_wake.changed() => {
                if changed.is_err() {
                    break;
                }
                let commits = server
                    .cluster_handoff_commits
                    .load_full();
                for commit in commits.values() {
                    let uuid = Uuid::from_bytes(commit.hint.uuid);
                    if state.commit_outcomes.contains_key(&(uuid, commit.token))
                        || pending_commits.values().any(|(pending, _, _)| pending.token == commit.token)
                        || queued_commits.get(&uuid).is_some_and(|pending| pending.token == commit.token)
                        || waiting.iter().any(|command| matches!(command, HandoffCommand::Commit { token, hint, .. } if *token == commit.token && hint.uuid == commit.hint.uuid))
                    { continue; }
                    if pending_commits.values().any(|(pending, _, _)| pending.hint.uuid == commit.hint.uuid) {
                        if let Some(superseded) = queued_commits.insert(uuid, commit.clone()) {
                            observe_commit(&mut state, &superseded);
                            state.commit_outcomes.insert((uuid, superseded.token), CommitOutcome {
                                token: superseded.token,
                                revision: superseded.hint.playerdata_revision,
                                accepted: false,
                            });
                            server.cluster_handoff.store(Arc::new(state.clone()));
                        }
                        continue;
                    }
                    let command = HandoffCommand::Commit {
                        token: commit.token,
                        hint: commit.hint.clone(),
                        data: commit.data.to_vec(),
                    };
                    let Some(target) = primary_peers.iter().copied().min_by_key(|peer| peer.0) else {
                        waiting.push_back(command);
                        continue;
                    };
                    let request_id = next_request;
                    next_request = next_request.wrapping_add(1);
                    let generation = server
                        .cluster_peer_generations
                        .load()
                        .get(&target)
                        .copied()
                        .unwrap_or(0);
                    pending_commits.insert(request_id, (commit.clone(), target, generation));
                    send_to(&mut pending_outbound, target, &HandoffMessage::CommitPlayerdata {
                        request_id,
                        expected_revision: commit.hint.playerdata_revision,
                        hint: commit.hint.clone(),
                        data: commit.data.to_vec(),
                    });
                }
            }
            permit = outbound.reserve(), if !pending_outbound.is_empty() => {
                let Ok(permit) = permit else { break; };
                if let Some(parcel) = pending_outbound.pop_front() {
                    permit.send(parcel);
                }
            }
            result = persistence_rx.recv(), if primary && pending_outbound.len().saturating_add(server.cluster_connected_peers.load().len()).saturating_add(1) <= OUTBOUND_CAPACITY => {
                let Some(result) = result else { break; };
                match result {
                    PersistenceResult::CatalogHint(hint) => {
                        let uuid = Uuid::from_bytes(hint.uuid);
                        if primary_hints.get(&uuid).is_none_or(|current| current.playerdata_revision <= hint.playerdata_revision) {
                            primary_hints.insert(uuid, hint.clone());
                            let peers = server.cluster_connected_peers.load().iter().map(|peer| peer.0).collect::<Vec<_>>();
                            broadcast(&mut pending_outbound, &peers, &HandoffMessage::ResumeHintPublished(hint));
                        }
                    }
                    PersistenceResult::Reply(peer, generation, message) => {
                        if server.cluster_peer_generations.load().get(&peer).copied() == Some(generation) {
                            send_to(&mut pending_outbound, peer, &message);
                        }
                    }
                    PersistenceResult::Commit(peer, generation, result, published) => {
                        if server.cluster_peer_generations.load().get(&peer).copied() == Some(generation) {
                            send_to(&mut pending_outbound, peer, &result);
                        }
                        if let Some(hint) = published {
                            primary_hints.insert(Uuid::from_bytes(hint.uuid), hint.clone());
                            let connected = server.cluster_connected_peers.load();
                            let peers: Vec<u16> = connected.iter().map(|peer| peer.0).collect();
                            broadcast(&mut pending_outbound, &peers, &HandoffMessage::ResumeHintPublished(hint));
                        }
                    }
                }
            }
            command = commands.recv(), if pending_outbound.len().saturating_add(waiting.len()).saturating_add(primary_peers.len()).saturating_add(1) <= OUTBOUND_CAPACITY => {
                let Some(command) = command else { break; };
                let commands_to_route = match command {
                    #[cfg(feature = "cluster-e2e")]
                    HandoffCommand::ArmRevisionMismatch { uuid } => {
                        revision_mismatch = Some(uuid);
                        state.revision_mismatch_armed = Some(uuid);
                        server.cluster_handoff.store(Arc::new(state.clone()));
                        continue;
                    }
                    HandoffCommand::ReleaseCommitOutcome { uuid, token } => {
                        if state.commit_outcomes.get(&(uuid, token)).is_some_and(|outcome| outcome.accepted) {
                            state.commit_outcomes.remove(&(uuid, token));
                            state.commit_observers.remove(&(uuid, token));
                            server.cluster_handoff.store(Arc::new(state.clone()));
                        }
                        continue;
                    }
                    HandoffCommand::PeerConnected(peer) => {
                        if primary && let Some(generation) = server.cluster_peer_generations.load().get(&peer).copied() {
                            hint_seeds.insert(peer, (generation, None));
                        }
                        continue;
                    }
                    HandoffCommand::PrimaryConnected(peer) => {
                        primary_peers.insert(peer);
                        waiting.drain(..).collect::<Vec<_>>()
                    }
                    HandoffCommand::PeerDropped(peer) => {
                        hint_seeds.remove(&peer);
                        primary_peers.remove(&peer);
                        let mut lost = pending_hints
                            .extract_if(|_, (_, source, _)| *source == peer)
                            .map(|(_, (uuid, _, _))| HandoffCommand::RequestHint { uuid })
                            .collect::<Vec<_>>();
                        lost.extend(pending_playerdata
                            .extract_if(|_, (_, _, _, source, _)| *source == peer)
                            .map(|(_, (uuid, revision, token, _, _))| {
                                HandoffCommand::RequestPlayerdata { uuid, revision, token }
                            }));
                        for (_, (commit, _, _)) in pending_commits
                            .extract_if(|_, (_, source, _)| *source == peer)
                        {
                            lost.push(HandoffCommand::Commit {
                                token: commit.token,
                                hint: commit.hint.clone(),
                                data: commit.data.to_vec(),
                            });
                        }
                        if primary_peers.is_empty() {
                            waiting.extend(lost);
                            continue;
                        }
                        lost
                    }
                    command if primary_peers.is_empty() && !primary => {
                        waiting.push_back(command);
                        continue;
                    }
                    command => vec![command],
                };
                for command in commands_to_route {
                    let request_id = next_request;
                    next_request = next_request.wrapping_add(1);
                    let target = primary_peers.iter().copied().min_by_key(|peer| peer.0);
                    match command {
                        HandoffCommand::RequestHint { uuid } => {
                            state.hints.remove(&uuid);
                            state.missing_hints.remove(&uuid);
                            state.failed_hints.remove(&uuid);
                            server.cluster_handoff.store(Arc::new(state.clone()));
                            let Some(target) = target else { continue; };
                            let generation = server
                                .cluster_peer_generations
                                .load()
                                .get(&target)
                                .copied()
                                .unwrap_or(0);
                            pending_hints.insert(request_id, (uuid, target, generation));
                            send_to(&mut pending_outbound, target, &HandoffMessage::ResumeHintRequest {
                                request_id,
                                requester: local.0,
                                uuid: *uuid.as_bytes(),
                            });
                        }
                        HandoffCommand::RequestPlayerdata { uuid, revision, token } => {
                            state.playerdata.remove(&uuid);
                            state.missing_playerdata.remove(&uuid);
                            state.failed_playerdata.remove(&uuid);
                            server.cluster_handoff.store(Arc::new(state.clone()));
                            let Some(target) = target else { continue; };
                            let generation = server
                                .cluster_peer_generations
                                .load()
                                .get(&target)
                                .copied()
                                .unwrap_or(0);
                            pending_playerdata.insert(request_id, (uuid, revision, token, target, generation));
                            send_to(&mut pending_outbound, target, &HandoffMessage::PlayerdataRequest {
                                request_id,
                                requester: local.0,
                                uuid: *uuid.as_bytes(),
                                revision,
                            });
                        }
                        HandoffCommand::Commit { token, hint, data } => {
                            if pending_commits
                                .values()
                                .any(|(pending, _, _)| pending.hint.uuid == hint.uuid)
                            {
                                let uuid = Uuid::from_bytes(hint.uuid);
                                if let Some(superseded) = queued_commits.insert(
                                    uuid,
                                    Arc::new(HandoffCommit {
                                        observer: server.cluster_handoff_commits.load().get(&uuid).filter(|commit| commit.token == token).and_then(|commit| commit.observer.clone()),
                                        token,
                                        hint,
                                        data: data.into(),
                                    }),
                                ) {
                                    observe_commit(&mut state, &superseded);
                                    state.commit_outcomes.insert((uuid, superseded.token), CommitOutcome {
                                        token: superseded.token,
                                        revision: superseded.hint.playerdata_revision,
                                        accepted: false,
                                    });
                                    server.cluster_handoff.store(Arc::new(state.clone()));
                                }
                                continue;
                            }
                            let Some(target) = target else { continue; };
                            let generation = server
                                .cluster_peer_generations
                                .load()
                                .get(&target)
                                .copied()
                                .unwrap_or(0);
                            let commit = Arc::new(HandoffCommit {
                                observer: server.cluster_handoff_commits.load().get(&Uuid::from_bytes(hint.uuid)).filter(|commit| commit.token == token).and_then(|commit| commit.observer.clone()),
                                token,
                                hint: hint.clone(),
                                data: data.clone().into(),
                            });
                            pending_commits.insert(request_id, (commit, target, generation));
                            send_to(&mut pending_outbound, target, &HandoffMessage::CommitPlayerdata {
                                request_id,
                                expected_revision: hint.playerdata_revision,
                                hint,
                                data,
                            });
                        }
                        #[cfg(feature = "cluster-e2e")]
                        HandoffCommand::ArmRevisionMismatch { .. } => {}
                        HandoffCommand::ReleaseCommitOutcome { .. } | HandoffCommand::PrimaryConnected(_) | HandoffCommand::PeerConnected(_) | HandoffCommand::PeerDropped(_) => {}
                    }
                }
            }
            parcel = inbound.recv(), if pending_outbound.len().saturating_add(pending_persistence.len()).saturating_add(server.cluster_connected_peers.load().len()).saturating_add(1) <= OUTBOUND_CAPACITY => {
                let Some(parcel) = parcel else { break; };
                if !is_handoff_payload(&parcel.bytes) {
                    continue;
                }
                let Ok(message) = decode_message(&parcel.bytes) else {
                    warn!(peer = parcel.peer.0, "cluster handoff payload rejected");
                    continue;
                };
                let generation = server
                    .cluster_peer_generations
                    .load()
                    .get(&parcel.peer)
                    .copied()
                    .unwrap_or(0);
                if parcel.generation != generation {
                    continue;
                }
                if primary {
                    pending_persistence.push_back(PersistenceCommand::Handle(
                        parcel.peer,
                        parcel.generation,
                        message,
                    ));
                    continue;
                }
                match message {
                    HandoffMessage::ResumeHintResponse { request_id, hint } => {
                        let Some(uuid) = take_hint_request(
                            &mut pending_hints,
                            request_id,
                            parcel.peer,
                            parcel.generation,
                        ) else { continue; };
                        if let Some(hint) = hint {
                            if hint.uuid == *uuid.as_bytes() {
                                if state
                                    .hints
                                    .get(&uuid)
                                    .is_some_and(|current| current.playerdata_revision > hint.playerdata_revision)
                                {
                                    continue;
                                }
                                state.failed_hints.remove(&uuid);
                                state.missing_hints.remove(&uuid);
                                state.hints.insert(uuid, hint);
                            } else {
                                state.failed_hints.insert(uuid);
                                warn!(peer = parcel.peer.0, request_id, %uuid, "cluster handoff hint UUID mismatch");
                            }
                        } else {
                            if !state.hints.contains_key(&uuid) {
                                state.failed_hints.remove(&uuid);
                                state.missing_hints.insert(uuid);
                            }
                        }
                        server.cluster_handoff.store(Arc::new(state.clone()));
                    }
                    HandoffMessage::ResumeHintPublished(hint) => {
                        if !server.cluster_primary_peers.load().contains(&parcel.peer) {
                            continue;
                        }
                        let uuid = Uuid::from_bytes(hint.uuid);
                        if state
                            .hints
                            .get(&uuid)
                            .is_some_and(|current| current.playerdata_revision > hint.playerdata_revision)
                        {
                            continue;
                        }
                        state.missing_hints.remove(&uuid);
                        state.hints.insert(uuid, hint);
                        server.cluster_handoff.store(Arc::new(state.clone()));
                    }
                    HandoffMessage::PlayerdataResponse { request_id, uuid, revision, data } => {
                        let received = Uuid::from_bytes(uuid);
                        let Some((expected, _, token)) = take_playerdata_request(
                            &mut pending_playerdata,
                            request_id,
                            parcel.peer,
                            parcel.generation,
                        ) else {
                            continue;
                        };
                        if expected != received {
                            state.failed_playerdata.insert(expected, token);
                            server.cluster_handoff.store(Arc::new(state.clone()));
                            warn!(peer = parcel.peer.0, request_id, uuid = %expected, received = %received, "cluster handoff playerdata UUID mismatch");
                            continue;
                        }
                        #[cfg(feature = "cluster-e2e")]
                        let revision = if revision_mismatch == Some(expected) {
                            revision_mismatch = None;
                            state.revision_mismatch_armed = None;
                            revision.wrapping_add(1)
                        } else {
                            revision
                        };
                        state.failed_playerdata.remove(&expected);
                        state.playerdata.insert(expected, RevisionedPlayerdata {
                            token,
                            revision,
                            data: data.into(),
                        });
                        state.missing_playerdata.remove(&expected);
                        server.cluster_handoff.store(Arc::new(state.clone()));
                    }
                    HandoffMessage::PlayerdataUnavailable { request_id, uuid, revision } => {
                        let received = Uuid::from_bytes(uuid);
                        let Some((expected, _, token)) = take_playerdata_request(
                            &mut pending_playerdata,
                            request_id,
                            parcel.peer,
                            parcel.generation,
                        ) else {
                            continue;
                        };
                        if expected != received {
                            state.failed_playerdata.insert(expected, token);
                            server.cluster_handoff.store(Arc::new(state.clone()));
                            warn!(peer = parcel.peer.0, request_id, uuid = %expected, received = %received, "cluster handoff unavailable UUID mismatch");
                            continue;
                        }
                        #[cfg(feature = "cluster-e2e")]
                        let revision = if revision_mismatch == Some(expected) {
                            revision_mismatch = None;
                            state.revision_mismatch_armed = None;
                            revision.wrapping_add(1)
                        } else {
                            revision
                        };
                        state.failed_playerdata.remove(&expected);
                        state.playerdata.remove(&expected);
                        state.missing_playerdata.insert(expected, (token, revision));
                        server.cluster_handoff.store(Arc::new(state.clone()));
                    }
                    HandoffMessage::CommitResult { request_id, uuid, revision, accepted, .. } => {
                        let received = Uuid::from_bytes(uuid);
                        let Some(commit) = take_commit_request(
                            &mut pending_commits,
                            request_id,
                            parcel.peer,
                            parcel.generation,
                            received,
                        ) else {
                            continue;
                        };
                        let uuid = Uuid::from_bytes(commit.hint.uuid);
                        if accepted {
                            let mut hint = commit.hint.clone();
                            hint.playerdata_revision = revision;
                            if state.hints.get(&uuid).is_none_or(|current| current.playerdata_revision <= hint.playerdata_revision) { state.hints.insert(uuid, hint); }
                            state.playerdata.remove(&uuid);
                            state.missing_playerdata.remove(&uuid);
                            server.cluster_handoff.store(Arc::new(state.clone()));
                        } else {
                            warn!(peer = parcel.peer.0, request_id, %uuid, revision, "cluster playerdata commit rejected");
                        }
                        observe_commit(&mut state, &commit);
                        state.commit_outcomes.insert(
                            (uuid, commit.token),
                            CommitOutcome {
                                token: commit.token,
                                revision,
                                accepted,
                            },
                        );
                        server.cluster_handoff.store(Arc::new(state.clone()));
                        if accepted {
                            server.cluster_handoff_commits.rcu(|current| {
                                let mut next = (**current).clone();
                                if next.get(&uuid).is_some_and(|entry| entry.token == commit.token) { next.remove(&uuid); }
                                next
                            });
                        }
                        if let Some(queued) = queued_commits.remove(&uuid) {
                            if !accepted {
                                observe_commit(&mut state, &queued);
                                state.commit_outcomes.insert(
                                    (uuid, queued.token),
                                    CommitOutcome {
                                        token: queued.token,
                                        revision,
                                        accepted: false,
                                    },
                                );
                                server.cluster_handoff.store(Arc::new(state.clone()));
                                continue;
                            }
                            let mut hint = queued.hint.clone();
                            hint.playerdata_revision = revision;
                            let Some(target) = primary_peers.iter().copied().min_by_key(|peer| peer.0) else {
                                waiting.push_back(HandoffCommand::Commit {
                                    token: queued.token,
                                    hint,
                                    data: queued.data.to_vec(),
                                });
                                continue;
                            };
                            let generation = server
                                .cluster_peer_generations
                                .load()
                                .get(&target)
                                .copied()
                                .unwrap_or(0);
                            let request_id = next_request;
                            next_request = next_request.wrapping_add(1);
                            let queued = Arc::new(HandoffCommit {
                                observer: queued.observer.clone(),
                                token: queued.token,
                                hint: hint.clone(),
                                data: queued.data.clone(),
                            });
                            pending_commits.insert(request_id, (queued.clone(), target, generation));
                            send_to(
                                &mut pending_outbound,
                                target,
                                &HandoffMessage::CommitPlayerdata {
                                    request_id,
                                    expected_revision: revision,
                                    hint,
                                    data: queued.data.to_vec(),
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_raw_playerdata_updates_only_accepted_revision_metadata_at_decode() {
        let mut data = NbtCompound::new();
        data.put_int("foodLevel", 7);
        let mut hint = PlayerResumeHint {
            uuid: *Uuid::from_u128(12).as_bytes(),
            dimension: "minecraft:the_nether".into(),
            position: [8.5, 100.0, -3.5],
            rotation: [90.0, 0.0],
            view_radius: 8,
            playerdata_revision: 7,
            last_owner: Some(1),
            retained_chunks: Vec::new(),
            retained_until_millis: 0,
        };
        assert!(put_handoff_metadata(&mut data, &hint, 7));
        let raw = serialize_nbt(data);
        hint.playerdata_revision = 8;
        let decoded = deserialize_retired_playerdata(&raw, &hint).unwrap();
        assert_eq!(revision(&decoded), 8);
        assert_eq!(stored_hint(&decoded), Some(hint));
        assert_eq!(decoded.get_int("foodLevel"), Some(7));
        assert_eq!(revision(&deserialize_nbt(&raw).unwrap()), 7);
    }

    #[test]
    fn hint_seed_preserves_target_under_capacity_and_peer_replacement() {
        let peer = ServerId(2);
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);
        let hint = |uuid: Uuid, dimension: &str, revision| PlayerResumeHint {
            uuid: *uuid.as_bytes(),
            dimension: dimension.into(),
            position: [1.5, 100.0, -2.5],
            rotation: [90.0, 0.0],
            view_radius: 8,
            playerdata_revision: revision,
            last_owner: Some(3),
            retained_chunks: Vec::new(),
            retained_until_millis: 0,
        };
        let hints = BTreeMap::from([
            (first, hint(first, "minecraft:the_nether", 7)),
            (second, hint(second, "minecraft:the_end", 8)),
        ]);
        let mut seeds = HashMap::from([(peer, (5, None))]);
        let mut generations = HashMap::from([(peer, 5)]);
        let mut outbound = VecDeque::new();
        for _ in 0..OUTBOUND_CAPACITY {
            send_to(
                &mut outbound,
                peer,
                &HandoffMessage::ResumeHintPublished(hints[&first].clone()),
            );
        }
        seed_hints(&mut outbound, &hints, &mut seeds, &generations);
        assert_eq!(outbound.len(), OUTBOUND_CAPACITY);
        assert_eq!(seeds[&peer].1, None);
        outbound.clear();
        seed_hints(&mut outbound, &hints, &mut seeds, &generations);
        let parcel = outbound.pop_front().unwrap();
        assert_eq!(parcel.peer, peer);
        assert!(
            matches!(decode_message(&parcel.bytes).unwrap(), HandoffMessage::ResumeHintPublished(actual) if actual == hints[&first])
        );
        generations.insert(peer, 6);
        seed_hints(&mut outbound, &hints, &mut seeds, &generations);
        assert!(outbound.is_empty());
        assert!(seeds.is_empty());
        seeds.insert(peer, (6, None));
        for uuid in [first, second] {
            seed_hints(&mut outbound, &hints, &mut seeds, &generations);
            let parcel = outbound.pop_front().unwrap();
            assert!(
                matches!(decode_message(&parcel.bytes).unwrap(), HandoffMessage::ResumeHintPublished(actual) if actual == hints[&uuid])
            );
        }
        seed_hints(&mut outbound, &hints, &mut seeds, &generations);
        assert!(seeds.is_empty());
        assert!(outbound.is_empty());
    }

    #[test]
    fn stale_hint_response_cannot_consume_live_request() {
        let uuid = Uuid::new_v4();
        let mut pending = HashMap::from([(9, (uuid, ServerId(2), 4))]);

        assert_eq!(take_hint_request(&mut pending, 9, ServerId(3), 4), None);
        assert_eq!(take_hint_request(&mut pending, 9, ServerId(2), 3), None);
        assert_eq!(
            take_hint_request(&mut pending, 9, ServerId(2), 4),
            Some(uuid)
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn stale_playerdata_response_cannot_consume_live_request() {
        let uuid = Uuid::new_v4();
        let mut pending = HashMap::from([(11, (uuid, 7, 13, ServerId(2), 5))]);

        assert_eq!(
            take_playerdata_request(&mut pending, 11, ServerId(2), 4),
            None
        );
        assert_eq!(
            take_playerdata_request(&mut pending, 11, ServerId(7), 5),
            None
        );
        assert_eq!(
            take_playerdata_request(&mut pending, 11, ServerId(2), 5),
            Some((uuid, 7, 13))
        );
        assert!(pending.is_empty());
    }

    #[cfg(feature = "cluster-e2e")]
    #[test]
    fn manual_commit_outcome_survives_until_real_transport_closes() {
        let uuid = Uuid::from_u128(10);
        let profile = crate::net::GameProfile {
            id: uuid,
            name: "retirement-test".into(),
            properties: arc_swap::ArcSwap::from_pointee(Vec::new()),
            profile_actions: None,
        };
        let (java, _sink) = crate::net::java::JavaClient::offline(
            10,
            profile,
            crate::net::PlayerConfig::default(),
            pumpkin_data::packet::CURRENT_MC_VERSION,
            None,
        );
        let client = Arc::new(crate::net::ClientPlatform::Java(java));
        let mut state = HandoffSnapshot::default();
        state.commit_outcomes.insert(
            (uuid, 5),
            CommitOutcome {
                token: 5,
                revision: 8,
                accepted: true,
            },
        );
        state
            .commit_observers
            .insert((uuid, 5), Arc::downgrade(&client));
        assert!(!prune_commit_outcomes(&mut state, &HashMap::new()));
        let crate::net::ClientPlatform::Java(java) = client.as_ref() else {
            unreachable!()
        };
        java.close();
        assert!(prune_commit_outcomes(&mut state, &HashMap::new()));
        assert!(state.commit_outcomes.is_empty());
        assert!(state.commit_observers.is_empty());
    }

    #[test]
    fn commit_outcomes_keep_only_live_observers_or_current_rejected_record() {
        let uuid = Uuid::from_u128(9);
        let commit = Arc::new(HandoffCommit {
            observer: None,
            token: 3,
            hint: PlayerResumeHint {
                uuid: *uuid.as_bytes(),
                dimension: "minecraft:overworld".into(),
                position: [0.0; 3],
                rotation: [0.0; 2],
                view_radius: 2,
                playerdata_revision: 7,
                last_owner: None,
                retained_chunks: Vec::new(),
                retained_until_millis: 0,
            },
            data: Arc::from([]),
        });
        let mut state = HandoffSnapshot::default();
        for (token, accepted) in [(1, true), (2, false), (3, false), (4, true)] {
            state.commit_outcomes.insert(
                (uuid, token),
                CommitOutcome {
                    token,
                    revision: 7,
                    accepted,
                },
            );
        }
        state
            .commit_observers
            .insert((uuid, 4), std::sync::Weak::new());
        let commits = HashMap::from([(uuid, commit)]);
        assert!(prune_commit_outcomes(&mut state, &commits));
        assert_eq!(state.commit_outcomes.len(), 1);
        assert!(state.commit_outcomes.contains_key(&(uuid, 3)));
        assert!(state.commit_observers.is_empty());
        assert!(prune_commit_outcomes(&mut state, &HashMap::new()));
        assert!(state.commit_outcomes.is_empty());
    }

    #[test]
    fn stale_commit_result_cannot_consume_live_request() {
        let uuid = Uuid::new_v4();
        let other = Uuid::new_v4();
        let commit = Arc::new(HandoffCommit {
            observer: None,
            token: 0,
            hint: PlayerResumeHint {
                uuid: *uuid.as_bytes(),
                dimension: String::new(),
                position: [0.0; 3],
                rotation: [0.0; 2],
                view_radius: 0,
                playerdata_revision: 0,
                last_owner: None,
                retained_chunks: Vec::new(),
                retained_until_millis: 0,
            },
            data: Arc::from([]),
        });
        let mut pending = HashMap::from([(13, (commit, ServerId(2), 6))]);

        assert!(take_commit_request(&mut pending, 13, ServerId(3), 6, uuid).is_none());
        assert!(take_commit_request(&mut pending, 13, ServerId(2), 5, uuid).is_none());
        assert!(take_commit_request(&mut pending, 13, ServerId(2), 6, other).is_none());
        assert!(take_commit_request(&mut pending, 13, ServerId(2), 6, uuid).is_some());
        assert!(pending.is_empty());
    }
}
