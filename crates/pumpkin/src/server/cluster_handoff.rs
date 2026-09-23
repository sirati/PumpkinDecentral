use std::collections::{HashMap, HashSet, VecDeque};
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
    pub hints: HashMap<Uuid, PlayerResumeHint>,
    pub missing_hints: HashSet<Uuid>,
    pub failed_hints: HashSet<Uuid>,
    pub playerdata: HashMap<Uuid, RevisionedPlayerdata>,
    pub missing_playerdata: HashMap<Uuid, (u64, u64)>,
    pub failed_playerdata: HashMap<Uuid, u64>,
    pub commit_outcomes: HashMap<(Uuid, u64), CommitOutcome>,
}

#[derive(Debug)]
pub enum HandoffCommand {
    PrimaryConnected(ServerId),
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
    token: u64,
    hint: PlayerResumeHint,
    data: Arc<[u8]>,
}

pub fn commit_logout(
    server: &Server,
    player: &Player,
    chunks: &[pumpkin_util::math::vector2::Vector2<i32>],
    retained_until_millis: u64,
    logout: bool,
) -> Option<u64> {
    let gid = player.cluster_gid()?;
    let statistics = server.cluster_actor_statistics.load();
    let Some(statistics) = statistics.get(&gid) else {
        tracing::error!(?gid, "cluster player statistics missing at logout commit");
        return None;
    };
    if statistics.leave_game_recorded != logout {
        tracing::error!(
            ?gid,
            logout,
            revision = statistics.revision,
            "cluster player statistics departure state mismatch"
        );
        return None;
    }
    let Some(retire_route) = server.cluster_actor_statistics_retire_route.load_full() else {
        tracing::error!(?gid, "cluster statistics retirement route unavailable");
        return None;
    };
    let Ok(retire_permit) = retire_route.try_reserve() else {
        tracing::error!(?gid, "cluster statistics retirement queue saturated");
        return None;
    };
    let token = server
        .cluster_lobby_session_generation
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let world = player.world();
    let world_id = super::cluster_world_delta::world_id(&world);
    let revision = server
        .cluster_handoff
        .load()
        .hints
        .get(&player.gameprofile.id)
        .map_or(0, |hint| hint.playerdata_revision);
    let position = player.position();
    let rotation = player.rotation();
    let hint = PlayerResumeHint {
        uuid: *player.gameprofile.id.as_bytes(),
        dimension: world.dimension.minecraft_name.to_string(),
        position: [position.x, position.y, position.z],
        rotation: [rotation.0, rotation.1],
        view_radius: player.config.load().view_distance.get(),
        playerdata_revision: revision,
        last_owner: Some(server.advanced_config.cluster.server_id),
        retained_chunks: chunks
            .iter()
            .map(|chunk| pumpkin_cluster::protocol::ChunkKey::new(world_id, chunk.x, chunk.y))
            .collect(),
        retained_until_millis,
    };
    let mut nbt = NbtCompound::new();
    player.write_nbt(&mut nbt);
    statistics.write_nbt(&mut nbt);
    let commit = Arc::new(HandoffCommit {
        token,
        hint,
        data: serialize_nbt(nbt).into(),
    });
    server.cluster_handoff_commits.rcu(|current| {
        let mut next = (**current).clone();
        next.insert(player.gameprofile.id, commit.clone());
        next
    });
    retire_permit.send(super::cluster_actor_statistics::ActorStatisticsRetire {
        actor: gid,
        incarnation: statistics.incarnation,
    });
    server
        .cluster_handoff_commit_wake
        .send_modify(|revision| *revision = revision.wrapping_add(1));
    Some(token)
}

fn serialize_nbt(data: NbtCompound) -> Vec<u8> {
    Nbt::from(data).write_unnamed().to_vec()
}

pub(crate) fn deserialize_nbt(data: &[u8]) -> Option<NbtCompound> {
    let mut cursor = Cursor::new(data);
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    Nbt::read_unnamed(&mut reader).ok().map(|nbt| nbt.root_tag)
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
    Reply(ServerId, u64, HandoffMessage),
    Commit(ServerId, u64, HandoffMessage, Option<PlayerResumeHint>),
}

fn primary_message(
    server: &Server,
    peer: ServerId,
    generation: u64,
    message: HandoffMessage,
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
                    Ok(Some(data)) => HandoffMessage::PlayerdataResponse {
                        request_id,
                        uuid,
                        revision: revision(&data),
                        data: serialize_nbt(data),
                    },
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
    let mut next_request = 1_u64;
    let mut pending_hints = HashMap::<u64, (Uuid, ServerId, u64)>::new();
    let mut pending_playerdata = HashMap::<u64, (Uuid, u64, u64, ServerId, u64)>::new();
    let mut pending_commits = HashMap::<u64, (Arc<HandoffCommit>, ServerId, u64)>::new();
    let mut queued_commits = HashMap::<Uuid, Arc<HandoffCommit>>::new();
    let mut pending_outbound = VecDeque::new();
    let mut pending_persistence = VecDeque::new();
    let mut primary_peers = (**server.cluster_primary_peers.load()).clone();
    let mut waiting = VecDeque::new();
    let mut commit_wake = server.cluster_handoff_commit_wake.subscribe();
    let (persistence_tx, mut persistence_rx) = mpsc::channel(256);
    let persistence_command_tx = if primary {
        let (command_tx, mut command_rx) = mpsc::channel(256);
        let persistence_server = server.clone();
        tokio::task::spawn_blocking(move || {
            while let Some(PersistenceCommand::Handle(peer, generation, message)) =
                command_rx.blocking_recv()
            {
                if let Some(result) =
                    primary_message(&persistence_server, peer, generation, message)
                    && persistence_tx.blocking_send(result).is_err()
                {
                    break;
                }
            }
        });
        Some(command_tx)
    } else {
        None
    };
    let initial_commits = server
        .cluster_handoff_commits
        .swap(Arc::new(HashMap::new()));
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
        tokio::select! {
            changed = commit_wake.changed() => {
                if changed.is_err() {
                    break;
                }
                let commits = server
                    .cluster_handoff_commits
                    .swap(Arc::new(HashMap::new()));
                for commit in commits.values() {
                    let uuid = Uuid::from_bytes(commit.hint.uuid);
                    if pending_commits.values().any(|(pending, _, _)| pending.hint.uuid == commit.hint.uuid) {
                        if let Some(superseded) = queued_commits.insert(uuid, commit.clone()) {
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
                    PersistenceResult::Reply(peer, generation, message) => {
                        if server.cluster_peer_generations.load().get(&peer).copied() == Some(generation) {
                            send_to(&mut pending_outbound, peer, &message);
                        }
                    }
                    PersistenceResult::Commit(peer, generation, result, published) => {
                        if server.cluster_peer_generations.load().get(&peer).copied() != Some(generation) {
                            continue;
                        }
                        send_to(&mut pending_outbound, peer, &result);
                        if let Some(hint) = published {
                            let connected = server.cluster_connected_peers.load();
                            let peers: Vec<u16> = connected.iter().map(|peer| peer.0).collect();
                            broadcast(&mut pending_outbound, &peers, &HandoffMessage::ResumeHintPublished(hint));
                        }
                    }
                }
            }
            permit = async {
                persistence_command_tx.as_ref()?.reserve().await.ok()
            }, if primary && !pending_persistence.is_empty() => {
                let Some(permit) = permit else { break; };
                if let Some(command) = pending_persistence.pop_front() {
                    permit.send(command);
                }
            }
            command = commands.recv(), if pending_outbound.len().saturating_add(waiting.len()).saturating_add(primary_peers.len()).saturating_add(1) <= OUTBOUND_CAPACITY => {
                let Some(command) = command else { break; };
                let commands_to_route = match command {
                    HandoffCommand::PrimaryConnected(peer) => {
                        primary_peers.insert(peer);
                        waiting.drain(..).collect::<Vec<_>>()
                    }
                    HandoffCommand::PeerDropped(peer) => {
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
                                        token,
                                        hint,
                                        data: data.into(),
                                    }),
                                ) {
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
                        HandoffCommand::PrimaryConnected(_) | HandoffCommand::PeerDropped(_) => {}
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
                        if !primary_peers.contains(&parcel.peer) {
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
                            state.hints.insert(uuid, hint);
                            state.playerdata.remove(&uuid);
                            state.missing_playerdata.remove(&uuid);
                            server.cluster_handoff_commits.rcu(|current| {
                                let mut next = (**current).clone();
                                if next
                                    .get(&uuid)
                                    .is_some_and(|current| Arc::ptr_eq(current, &commit))
                                {
                                    next.remove(&uuid);
                                }
                                next
                            });
                            server.cluster_handoff.store(Arc::new(state.clone()));
                        } else {
                            warn!(peer = parcel.peer.0, request_id, %uuid, revision, "cluster playerdata commit rejected");
                        }
                        state.commit_outcomes.insert(
                            (uuid, commit.token),
                            CommitOutcome {
                                token: commit.token,
                                revision,
                                accepted,
                            },
                        );
                        server.cluster_handoff.store(Arc::new(state.clone()));
                        if let Some(queued) = queued_commits.remove(&uuid) {
                            if !accepted {
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

    #[test]
    fn stale_commit_result_cannot_consume_live_request() {
        let uuid = Uuid::new_v4();
        let other = Uuid::new_v4();
        let commit = Arc::new(HandoffCommit {
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
