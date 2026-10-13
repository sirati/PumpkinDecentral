use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
use pumpkin_cluster::checkpoint;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::xfer::ChunkPayload;
const MAX_FILE_SIZE: usize = 64 * 1024 * 1024 + 50;
static TEMP_ID: AtomicU64 = AtomicU64::new(0);
pub use pumpkin_cluster::checkpoint::ChunkCheckpoint;

pub(super) fn validate_image(payload: &ChunkPayload) -> io::Result<()> {
    payload
        .validate_transfer_evidence()
        .map_err(|_| io::ErrorKind::InvalidData)?;
    let (_, min_y, blocks, biomes) = pumpkin_world::level::cluster_decode_snapshot_with_blocks(
        payload.chunk.x(),
        payload.chunk.z(),
        &payload.snapshot,
    )
    .ok_or(io::ErrorKind::InvalidData)?;
    let image = super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(min_y, blocks, biomes);
    let entities = pumpkin_world::level::cluster_snapshot_block_entities(&payload.snapshot)
        .ok_or(io::ErrorKind::InvalidData)?
        .into_iter()
        .map(|(position, bytes)| {
            let position = pumpkin_cluster::protocol::BlockPos {
                x: position.0.x,
                y: position.0.y,
                z: position.0.z,
            };
            let state = image
                .block(payload.chunk, position)
                .map_err(|_| io::ErrorKind::InvalidData)?;
            if !pumpkin_world::level::cluster_block_entity_document_type_matches(state, &bytes) {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok((position, bytes))
        })
        .collect::<io::Result<std::collections::BTreeMap<_, _>>>()?;
    let projected = payload
        .project_ground()
        .map_err(|_| io::ErrorKind::InvalidData)?;
    for (target, cell) in projected {
        if let Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(position),
            inventory: None,
        } = target
        {
            if key != payload.chunk {
                return Err(io::ErrorKind::InvalidData.into());
            }
            match cell {
                Some(pumpkin_cluster::canonical_action::Cell {
                    value: Value::BlockEntity(expected),
                    ..
                }) if entities.get(&position) == Some(&expected) => {}
                None if !entities.contains_key(&position) => {}
                _ => return Err(io::ErrorKind::InvalidData.into()),
            }
            continue;
        }
        let Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            inventory: None,
        } = target
        else {
            continue;
        };
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::Block(expected),
            ..
        }) = cell
        else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        let actual = image
            .block(key, position)
            .map_err(|_| io::ErrorKind::InvalidData)?;
        if actual.as_u16() != expected {
            return Err(io::ErrorKind::InvalidData.into());
        }
    }
    Ok(())
}

fn checkpoint_path(folder: &Path, chunk: ChunkKey) -> PathBuf {
    checkpoint::path(folder, chunk)
}

fn encode(checkpoint: &ChunkCheckpoint) -> io::Result<Vec<u8>> {
    validate_image(&checkpoint.payload)?;
    checkpoint::encode(checkpoint)
}

fn decode(bytes: &[u8], expected: ChunkKey) -> io::Result<ChunkCheckpoint> {
    let checkpoint = checkpoint::decode(bytes, expected)?;
    validate_image(&checkpoint.payload)?;
    Ok(checkpoint)
}

pub fn save(folder: &Path, checkpoint: &ChunkCheckpoint) -> io::Result<()> {
    let path = checkpoint_path(folder, checkpoint.payload.chunk);
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    std::fs::create_dir_all(parent)?;
    let bytes = encode(checkpoint)?;
    let temp = path.with_extension(format!(
        "cpk.{}.{}.tmp",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temp, &path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn load(
    folder: &Path,
    chunk: ChunkKey,
    expected_generation: u64,
) -> io::Result<Option<ChunkCheckpoint>> {
    let checkpoint = load_unverified_generation(folder, chunk)?;
    if checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.generation != expected_generation)
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(checkpoint)
}

pub fn load_unverified_generation(
    folder: &Path,
    chunk: ChunkKey,
) -> io::Result<Option<ChunkCheckpoint>> {
    let path = checkpoint_path(folder, chunk);
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let length = file.metadata()?.len();
    if length > MAX_FILE_SIZE as u64 {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.read_to_end(&mut bytes)?;
    let checkpoint = decode(&bytes, chunk)?;
    Ok(Some(checkpoint))
}

pub fn spawn_persisted_admission(server: &std::sync::Arc<super::Server>) {
    let (route, mut receive) = tokio::sync::mpsc::channel(256);
    let (failed, mut failures) = tokio::sync::mpsc::channel::<(ChunkKey, u64)>(4_096);
    for world in server.worlds.load().iter() {
        if world.level.cluster_genesis_required {
            world
                .level
                .persisted_chunk_admission
                .store(Some(std::sync::Arc::new(route.clone())));
            if let Some(wake) = world.level.fresh_chunk_wake.load_full() {
                let _ = wake.try_send(());
            }
        }
    }
    let server = std::sync::Arc::clone(server);
    let task_server = std::sync::Arc::clone(&server);
    server.spawn_task(async move {
        let server = task_server;
        let mut submitted =
            std::collections::HashMap::<ChunkKey, (u64, tokio::sync::oneshot::Sender<()>)>::new();
        loop {
            let admission = tokio::select! {
                failed = failures.recv() => {
                    if let Some((chunk, generation)) = failed
                        && submitted.get(&chunk).is_some_and(|(current, _)| *current == generation)
                    {
                        submitted.remove(&chunk);
                    }
                    continue;
                }
                admission = receive.recv() => admission,
            };
            let Some(admission) = admission else {
                break;
            };
            match admission {
                pumpkin_world::level::PersistedChunkAdmission::Prepare {
                    world_key,
                    dimension,
                    position,
                    generation,
                    original,
                    respond,
                    wake,
                } => {
                    let chunk = ChunkKey::new(
                        pumpkin_cluster::protocol::WorldId::new(dimension, world_key),
                        position.x,
                        position.y,
                    );
                    let level = server
                        .worlds
                        .load()
                        .iter()
                        .find(|world| {
                            world.level.cluster_genesis_required
                                && world.level.cluster_world_key == world_key
                                && world.level.cluster_dimension == dimension
                        })
                        .map(|world| std::sync::Arc::clone(&world.level));
                    let folder = level
                        .as_ref()
                        .map(|level| level.level_folder.dim_folder.clone());
                    if submitted
                        .get(&chunk)
                        .is_some_and(|(current, _)| *current == generation)
                        || folder.is_none()
                    {
                        let _ = respond.send(None);
                        let _ = wake.try_send(());
                        continue;
                    }
                    let (published, published_rx) = tokio::sync::oneshot::channel();
                    submitted.insert(chunk, (generation, published));
                    let server = std::sync::Arc::clone(&server);
                    let task_server = std::sync::Arc::clone(&server);
                    let failed = failed.clone();
                    server.spawn_task(async move {
                        let server = task_server;
                        let _ = original;
                        let Some(folder) = folder else {
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let checkpoint = tokio::task::spawn_blocking(move || {
                            load_unverified_generation(&folder, chunk)
                        })
                        .await;
                        let Ok(Ok(Some(checkpoint))) = checkpoint else {
                            if let Some(level) = level.as_ref() {
                                level.prefetch_cluster_chunk(position);
                            }
                            tracing::error!(
                                x = chunk.x(),
                                z = chunk.z(),
                                "cluster persisted chunk has no valid accepted checkpoint"
                            );
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let local = pumpkin_cluster::identity::ServerId(
                            server.advanced_config.cluster.server_id,
                        );
                        if server.cluster_peer_incarnations.load().get(&local)
                            != Some(&checkpoint.generation)
                        {
                            if let Some(level) = level.as_ref() {
                                level.prefetch_cluster_chunk(position);
                            }
                            tracing::error!(
                                x = chunk.x(),
                                z = chunk.z(),
                                checkpoint_generation = checkpoint.generation,
                                "cluster persisted chunk lacks a fenced current-epoch checkpoint"
                            );
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        }
                        let payload = checkpoint.payload;
                        let Some((approved, min_y, blocks, biomes)) =
                            pumpkin_world::level::cluster_decode_snapshot_with_blocks(
                                chunk.x(),
                                chunk.z(),
                                &payload.snapshot,
                            )
                        else {
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let Some(owner) = server.cluster_chunk_inventory_route.load_full() else {
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let Ok(install_permit) = owner.as_ref().clone().try_reserve_owned() else {
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let Ok(commit_permit) = owner.as_ref().clone().try_reserve_owned() else {
                            let _ = failed.try_send((chunk, generation));
                            let _ = respond.send(None);
                            let _ = wake.try_send(());
                            return;
                        };
                        let (installed, installed_rx) = tokio::sync::oneshot::channel();
                        let request = super::cluster_world_owner::ChunkInventoryRequest::Install {
                            chunk,
                            persisted_generation: Some(checkpoint.generation),
                            peer_reservation_generation: None,
                            peer_source: None,
                            genesis: payload.genesis,
                            occupancy: payload.occupancy,
                            image: std::sync::Arc::from(payload.snapshot.clone()),
                            blocks: super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
                                min_y, blocks, biomes,
                            ),
                            states: payload.inventory,
                            pendings: payload.pendings,
                            snapshot_cut: payload.snapshot_cut,
                            graphs: payload.graphs,
                            ground_anchors: payload.ground_anchors,
                            completion: payload.completion,
                            ground_frontier: payload.ground_frontier,
                            player_inventory_frontiers: payload.player_inventory_frontiers,
                            player_living: payload.player_living,
                            player_avatar_absent: payload.player_avatar_absent,
                            entity_bodies: payload.entity_bodies,
                            entity_history: payload.entity_history,
                            dependency_history: payload.dependency_history,
                            item_spatial: payload.item_spatial,
                            respond: installed,
                        };
                        install_permit.send(request);
                        let accepted = installed_rx.await.is_ok_and(|result| result.is_ok());
                        if !accepted {
                            let _ = failed.try_send((chunk, generation));
                            tracing::error!(
                                x = chunk.x(),
                                z = chunk.z(),
                                "cluster persisted chunk owner admission failed"
                            );
                        }
                        let sent = respond.send(accepted.then_some(approved)).is_ok();
                        let _ = wake.try_send(());
                        if !accepted || !sent || published_rx.await.is_err() {
                            return;
                        }
                        let (ready_respond, ready_receive) = tokio::sync::oneshot::channel();
                        commit_permit.send(
                            super::cluster_world_owner::ChunkInventoryRequest::CommitReady {
                                chunk,
                                identity: super::cluster_world_owner::ChunkAdmissionIdentity {
                                    persisted_generation: Some(generation),
                                    peer_reservation_generation: None,
                                    peer_source: None,
                                },
                                respond: ready_respond,
                            },
                        );
                        if !matches!(ready_receive.await, Ok(Ok(()))) {
                            tracing::error!(
                                x = chunk.x(),
                                z = chunk.z(),
                                "cluster persisted owner publication failed"
                            );
                        }
                    });
                }
                pumpkin_world::level::PersistedChunkAdmission::Published {
                    world_key,
                    dimension,
                    position,
                    generation,
                } => {
                    let chunk = ChunkKey::new(
                        pumpkin_cluster::protocol::WorldId::new(dimension, world_key),
                        position.x,
                        position.y,
                    );
                    let Some((current, published)) = submitted.remove(&chunk) else {
                        tracing::error!(
                            x = chunk.x(),
                            z = chunk.z(),
                            generation,
                            "cluster persisted publication has no owner admission"
                        );
                        continue;
                    };
                    if current != generation {
                        tracing::error!(
                            x = chunk.x(),
                            z = chunk.z(),
                            generation,
                            "cluster persisted publication has no matching owner admission"
                        );
                        continue;
                    }
                    let _ = published.send(());
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::action_graph::ChunkGroundFrontier;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::realm_completion::RealmCompletionSnapshot;
    use pumpkin_cluster::time::TickStamp;
    use pumpkin_world::chunk::ChunkData;

    fn checkpoint() -> ChunkCheckpoint {
        let chunk = ChunkKey::new(WorldId::new(0, 17), -3, 5);
        ChunkCheckpoint {
            generation: 9,
            payload: ChunkPayload::new(
                chunk,
                1,
                TickStamp(12),
                pumpkin_world::level::cluster_encode_snapshot(&ChunkData::empty_sync(-3, 5)),
                vec![],
                vec![1],
            )
            .with_ground_frontier(Some(ChunkGroundFrontier {
                epoch: 9,
                settled_through: TickStamp(11),
            }))
            .with_completion(RealmCompletionSnapshot {
                requirements: vec![],
                players: vec![],
                actions: vec![],
                finishes: vec![],
                conflicting_finishes: vec![],
            }),
        }
    }

    #[test]
    fn checkpoint_detects_corrupt_ground_image_and_wrong_chunk() {
        let initial = checkpoint();
        let mut bytes = encode(&initial).unwrap();
        assert_eq!(
            decode(&bytes, initial.payload.chunk).unwrap().payload,
            initial.payload
        );
        assert!(decode(&bytes, ChunkKey::new(WorldId::new(0, 17), -2, 5)).is_err());
        let mut old_protocol = bytes.clone();
        old_protocol[4..6].copy_from_slice(
            &(pumpkin_cluster::membership::CLUSTER_PROTOCOL_VERSION - 1).to_le_bytes(),
        );
        assert!(decode(&old_protocol, initial.payload.chunk).is_err());
        *bytes.last_mut().unwrap() ^= 1;
        assert!(decode(&bytes, initial.payload.chunk).is_err());
    }

    #[test]
    fn checkpoint_rejects_unproven_or_mistyped_block_entity_image() {
        let mut candidate = checkpoint();
        let position = pumpkin_cluster::protocol::BlockPos {
            x: candidate.payload.chunk.x() * 16 + 1,
            y: 64,
            z: candidate.payload.chunk.z() * 16 + 2,
        };
        let document = pumpkin_world::level::cluster_default_block_entity_document(
            pumpkin_data::Block::CHEST.default_state.id,
            pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z),
        )
        .unwrap();
        candidate.payload.ground_anchors.push((
            Target::Chunk {
                key: candidate.payload.chunk,
                part: ChunkPart::BlockEntity(position),
                inventory: None,
            },
            pumpkin_cluster::action_graph::GroundAnchor {
                settled_through: TickStamp(11),
                value: Some(pumpkin_cluster::canonical_action::Cell {
                    revision: 1,
                    value: Value::BlockEntity(document.clone()),
                }),
            },
        ));
        assert!(encode(&candidate).is_err());
        candidate.payload.ground_anchors.clear();
        candidate.payload.snapshot = pumpkin_world::level::cluster_replace_snapshot_block_entities(
            &candidate.payload.snapshot,
            &[(
                pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z),
                document,
            )],
        )
        .unwrap();
        assert!(encode(&candidate).is_err());
    }

    #[test]
    fn checkpoint_replaces_atomically_and_rejects_truncated_file() {
        let initial = checkpoint();
        let folder = std::env::temp_dir().join(format!(
            "pumpkin-cluster-checkpoint-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        save(&folder, &initial).unwrap();
        assert_eq!(
            load_unverified_generation(&folder, initial.payload.chunk)
                .unwrap()
                .unwrap()
                .generation,
            initial.generation
        );
        assert!(load(&folder, initial.payload.chunk, initial.generation + 1).is_err());
        assert_eq!(
            load(&folder, initial.payload.chunk, initial.generation)
                .unwrap()
                .unwrap()
                .payload,
            initial.payload
        );
        let mut replacement = checkpoint();
        replacement.payload.holders = vec![1, 2];
        save(&folder, &replacement).unwrap();
        assert_eq!(
            load(&folder, initial.payload.chunk, initial.generation)
                .unwrap()
                .unwrap()
                .payload
                .holders,
            vec![1, 2]
        );
        std::fs::write(checkpoint_path(&folder, initial.payload.chunk), b"CPK1").unwrap();
        assert!(load(&folder, initial.payload.chunk, initial.generation).is_err());
        std::fs::remove_dir_all(folder).unwrap();
    }
}
