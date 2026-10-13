use crate::chunk::format::linear::LinearV2File;
use crate::chunk::format::pump::PumpFile;
use crate::chunk_system::{ChunkListener, ChunkLoading, GenerationSchedule, LevelChannel};
use crate::generation::generator::WorldGenerator;
use crate::lighting::DynamicLightEngine;
use crate::{
    chunk::{
        ChunkData, ChunkEntityData, ChunkReadingError,
        format::anvil::{AnvilChunkFile, SingleChunkDataSerializer},
        io::{
            Dirtiable, FileIO, LoadedData,
            file_manager::{ChunkFileManager, LevelFileIO},
        },
        palette::has_random_ticking_fluid,
    },
    generation::get_world_gen_with_all_settings,
    tick::{OrderedTick, ScheduledTick, TickPriority},
    world::WorldPortalExt,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use crossbeam::queue::{ArrayQueue, SegQueue};
use dashmap::{DashMap, Entry};
use pumpkin_config::{chunk::ChunkConfig, lighting::LightingEngineConfig, world::LevelConfig};
use pumpkin_data::biome::Biome;
use pumpkin_data::dimension::Dimension;
use pumpkin_data::{Block, BlockStateId, block_properties::has_random_ticks, fluid::Fluid};
use pumpkin_util::math::{position::BlockPos, vector2::Vector2};
use pumpkin_util::world_seed::Seed;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    thread,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
// use tokio::runtime::Handle;
use tokio::{
    select,
    sync::{
        mpsc::{self, Receiver},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::task::TaskTracker;

pub type SyncChunk = Arc<ChunkData>;
pub type SyncEntityChunk = Arc<ChunkEntityData>;

pub fn cluster_checkpoint_path(
    folder: &std::path::Path,
    dimension: u8,
    world_key: u64,
    position: Vector2<i32>,
) -> std::path::PathBuf {
    folder.join("cluster-checkpoints").join(format!(
        "{}_{}_{}_{}.cpk",
        dimension, world_key, position.x, position.y
    ))
}

pub enum FreshChunkAdmission {
    Prepare {
        world_key: u64,
        dimension: u8,
        position: Vector2<i32>,
        generation: u64,
        image: std::sync::Arc<[u8]>,
        palettes: crate::chunk_system::chunk_state::InitialChunkPalettes,
        respond: oneshot::Sender<bool>,
        wake: crossbeam::channel::Sender<()>,
    },
    Published {
        world_key: u64,
        dimension: u8,
        position: Vector2<i32>,
        generation: u64,
    },
    Aborted {
        world_key: u64,
        dimension: u8,
        position: Vector2<i32>,
        generation: u64,
    },
}

pub enum PersistedChunkAdmission {
    Prepare {
        world_key: u64,
        dimension: u8,
        position: Vector2<i32>,
        generation: u64,
        original: SyncChunk,
        respond: oneshot::Sender<Option<SyncChunk>>,
        wake: crossbeam::channel::Sender<()>,
    },
    Published {
        world_key: u64,
        dimension: u8,
        position: Vector2<i32>,
        generation: u64,
    },
}

pub struct ClusterSnapshotPublication {
    pub position: Vector2<i32>,
    pub snapshot: SyncChunk,
    pub reservation_generation: Option<u64>,
    pub respond: oneshot::Sender<bool>,
}

pub struct PersistedChunkDecision {
    pub world_key: u64,
    pub dimension: u8,
    pub position: Vector2<i32>,
    pub generation: u64,
    pub recovery: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PersistedChunkReservationState {
    Pending,
    Recovery,
    Approved,
    Lost,
}

#[derive(Clone, Copy)]
pub struct PersistedChunkReservation {
    pub generation: u64,
    pub state: PersistedChunkReservationState,
}

#[derive(Clone)]
pub enum OwnerChunkDemand {
    RetainPeerRequest {
        position: Vector2<i32>,
    },
    ReleasePeerRequest {
        position: Vector2<i32>,
    },
    Acquire {
        position: Vector2<i32>,
        lease: u64,
        ready: Option<tokio::sync::mpsc::Sender<OwnerChunkReady>>,
    },
    Release {
        position: Vector2<i32>,
        lease: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerChunkReady {
    pub world_key: u64,
    pub dimension: u8,
    pub position: Vector2<i32>,
    pub lease: u64,
    pub publication_generation: u64,
}

pub enum PeerChunkInstallClaim {
    Claim {
        position: Vector2<i32>,
        generation: u64,
        respond: tokio::sync::mpsc::Sender<PeerChunkInstallClaimResult>,
    },
    Release {
        position: Vector2<i32>,
        generation: u64,
    },
}

pub struct PeerChunkInstallClaimResult {
    pub world_key: u64,
    pub dimension: u8,
    pub position: Vector2<i32>,
    pub generation: u64,
    pub allowed: bool,
}

struct ClusterLobbyNeedCounts {
    counts: ArcSwap<rustc_hash::FxHashMap<Vector2<i32>, u32>>,
}

impl ClusterLobbyNeedCounts {
    fn new() -> Self {
        Self {
            counts: ArcSwap::from_pointee(rustc_hash::FxHashMap::default()),
        }
    }

    fn contains(&self, position: &Vector2<i32>) -> bool {
        self.counts.load().contains_key(position)
    }

    fn acquire(&self, positions: &[Vector2<i32>]) {
        self.counts.rcu(|current| {
            let mut next = (**current).clone();
            for position in positions {
                let count = next.entry(*position).or_default();
                *count = count.saturating_add(1);
            }
            Arc::new(next)
        });
    }

    fn release(&self, positions: &[Vector2<i32>]) {
        self.counts.rcu(|current| {
            let mut next = (**current).clone();
            for position in positions {
                if let Some(count) = next.get_mut(position) {
                    *count -= 1;
                    if *count == 0 {
                        next.remove(position);
                    }
                }
            }
            Arc::new(next)
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadedChunkChange {
    Loaded(Vector2<i32>),
    Unloaded(Vector2<i32>),
}

pub type ChunkSaver =
    LevelFileIO<LinearV2File<ChunkData>, AnvilChunkFile<ChunkData>, PumpFile<ChunkData>>;

pub type EntitySaver = LevelFileIO<
    LinearV2File<ChunkEntityData>,
    AnvilChunkFile<ChunkEntityData>,
    PumpFile<ChunkEntityData>,
>;

/// The `Level` module provides functionality for working with chunks within or outside a Minecraft world.
///
/// Key features include:
///
/// - **Chunk Loading:** Efficiently loads chunks from disk.
/// - **Chunk Caching:** Stores accessed chunks in memory for faster access.
/// - **Chunk Generation:** Generates new chunks on-demand using a specified `WorldGenerator`.
///
/// For more details on world generation, refer to the `WorldGenerator` module.
pub struct Level {
    pub seed: Seed,
    pub world_portal: ArcSwap<Option<Arc<dyn WorldPortalExt>>>,
    pub level_folder: Arc<LevelFolder>,
    pub cluster_dimension: u8,
    pub cluster_world_key: u64,
    pub cluster_genesis_required: bool,
    pub fresh_chunk_admission: ArcSwapOption<tokio::sync::mpsc::Sender<FreshChunkAdmission>>,
    pub persisted_chunk_admission:
        ArcSwapOption<tokio::sync::mpsc::Sender<PersistedChunkAdmission>>,
    pub fresh_chunk_wake: ArcSwapOption<crossbeam::channel::Sender<()>>,
    pub lighting_config: LightingEngineConfig,

    /// Counts the number of ticks that have been scheduled for this world
    schedule_tick_counts: AtomicU64,

    // Chunks that are paired with chunk watchers. When a chunk is no longer watched, it is removed
    // from the loaded chunks map and sent to the underlying ChunkIO
    pub loaded_chunks: Arc<DashMap<Vector2<i32>, SyncChunk>>,
    pub cluster_snapshots: Arc<DashMap<Vector2<i32>, SyncChunk>>,
    pub cluster_snapshot_publication:
        ArcSwapOption<crossbeam::channel::Sender<ClusterSnapshotPublication>>,
    pub cluster_publication_reservations:
        Arc<ArcSwap<rustc_hash::FxHashMap<Vector2<i32>, PersistedChunkReservation>>>,
    pub cluster_persisted_decision:
        ArcSwapOption<tokio::sync::mpsc::Sender<PersistedChunkDecision>>,
    pub cluster_peer_install_claim:
        ArcSwapOption<crossbeam::channel::Sender<PeerChunkInstallClaim>>,
    pub cluster_owner_chunk_demand: ArcSwapOption<crossbeam::channel::Sender<OwnerChunkDemand>>,
    /// Owner-published primary chunks whose unresolved action history forbids unload.
    pub cluster_unsettled_primary_chunks: ArcSwap<FxHashSet<Vector2<i32>>>,
    /// Scheduler-issued generation tokens. Owner claims a token before an action
    /// can use its chunk; unload atomically claims the opposite state.
    pub cluster_primary_chunk_fences: ArcSwap<FxHashMap<Vector2<i32>, Arc<AtomicU8>>>,
    pub cluster_peer_install_releases: Arc<ArrayQueue<(Vector2<i32>, u64)>>,
    pub cluster_peer_claim_capacity: ArcSwapOption<tokio::sync::mpsc::Sender<()>>,
    pub cluster_pinned: Arc<dashmap::DashSet<Vector2<i32>>>,
    cluster_lobby_pins: ClusterLobbyNeedCounts,
    pub cluster_fetch_wanted: Arc<dashmap::DashSet<Vector2<i32>>>,
    pub cluster_dual: ClusterDualState,
    pub(crate) loaded_chunk_changes: Arc<SegQueue<LoadedChunkChange>>,
    loaded_entity_chunks: Arc<DashMap<Vector2<i32>, SyncEntityChunk>>,
    pub chunks_with_scheduled_ticks: Arc<dashmap::DashSet<Vector2<i32>>>,
    pub chunk_loading: Mutex<ChunkLoading>,

    chunk_watchers: Arc<DashMap<Vector2<i32>, usize>>,

    pub chunk_saver: Arc<ChunkSaver>,
    entity_saver: Arc<EntitySaver>,

    pub world_gen: ArcSwap<WorldGenerator>,

    /// Handles runtime lighting updates
    pub light_engine: DynamicLightEngine,

    /// Tracks tasks associated with this world instance
    tasks: TaskTracker,
    pub chunk_system_tasks: TaskTracker,
    /// Notification that interrupts tasks for shutdown
    pub cancel_token: CancellationToken,

    pub shut_down_chunk_system: AtomicBool,
    pub should_save: AtomicBool,
    pub should_unload: AtomicBool,
    /// Whether periodic autosaving is enabled. Toggled by `/save-off` and `/save-on`;
    /// a manual `/save-all` still saves while this is `false`.
    pub save_enabled: AtomicBool,
    /// Number of ticks between autosave checks. If 0, autosave is disabled.
    pub autosave_ticks: u64,

    pending_entity_generations: Arc<DashMap<Vector2<i32>, Vec<oneshot::Sender<SyncEntityChunk>>>>,

    pub level_channel: Arc<LevelChannel>,
    pub thread_tracker: Mutex<Vec<thread::JoinHandle<()>>>,
    pub chunk_listener: Arc<ChunkListener>,
}

pub struct TickData {
    pub block_ticks: Vec<OrderedTick<&'static Block>>,
    pub fluid_ticks: Vec<OrderedTick<&'static Fluid>>,
    pub random_ticks: Vec<RandomTickSample>,
}

#[derive(Clone, Copy)]
pub struct RandomTickSample {
    pub position: BlockPos,
    pub tick_block: bool,
    pub tick_fluid: bool,
}

pub struct LevelFolder {
    pub root_folder: PathBuf,
    pub dim_folder: PathBuf,
    pub region_folder: PathBuf,
    pub entities_folder: PathBuf,
    pub poi_folder: PathBuf,
}

impl Level {
    #[must_use]
    pub fn from_root_folder(
        level_config: &LevelConfig,
        root_folder: PathBuf,
        seed: i64,
        dimension: Dimension,
    ) -> Arc<Self> {
        Self::from_root_folder_with_cluster_genesis(
            level_config,
            root_folder,
            seed,
            dimension,
            false,
        )
    }

    #[expect(clippy::too_many_lines)]
    pub fn from_root_folder_with_cluster_genesis(
        level_config: &LevelConfig,
        root_folder: PathBuf,
        seed: i64,
        dimension: Dimension,
        cluster_genesis_required: bool,
    ) -> Arc<Self> {
        let cluster_world_key = root_folder
            .file_name()
            .and_then(|name| name.to_str())
            .map_or(0, |name| xxhash_rust::xxh64::xxh64(name.as_bytes(), 0));
        let cluster_dimension = dimension.id;
        let (namespace, name) = match dimension.minecraft_name.split_once(':') {
            Some((ns, n)) => (ns, n),
            None => ("minecraft", dimension.minecraft_name),
        };

        // 26.2 canonical layout: root_folder/dimensions/<namespace>/<name>
        let canonical_dim_folder = root_folder.join("dimensions").join(namespace).join(name);

        // Check if canonical 26.2 folder exists, or fall back to pre-26.2 legacy folders
        let dim_folder = if canonical_dim_folder.exists() {
            canonical_dim_folder
        } else if dimension.minecraft_name == Dimension::OVERWORLD.minecraft_name
            && root_folder.join("region").exists()
        {
            root_folder.clone()
        } else if dimension.minecraft_name == Dimension::THE_NETHER.minecraft_name
            && root_folder.join("DIM-1").join("region").exists()
        {
            root_folder.join("DIM-1")
        } else if dimension.minecraft_name == Dimension::THE_END.minecraft_name
            && root_folder.join("DIM1").join("region").exists()
        {
            root_folder.join("DIM1")
        } else {
            canonical_dim_folder
        };

        let region_folder = dim_folder.join("region");
        let entities_folder = dim_folder.join("entities");
        let poi_folder = dim_folder.join("poi");

        if !is_cluster_secondary() {
            let _ = std::fs::create_dir_all(&region_folder);
            let _ = std::fs::create_dir_all(&entities_folder);
            let _ = std::fs::create_dir_all(&poi_folder);
        }

        let level_folder = Arc::new(LevelFolder {
            root_folder,
            dim_folder,
            region_folder,
            entities_folder,
            poi_folder,
        });

        let main_folder = &level_folder.root_folder;

        let mut is_flat = false;
        let mut flat_layers = Vec::new();
        let mut flat_biome = "minecraft:plains".to_string();
        let mut generator_settings_name: Option<String> = None;
        let mut biome_source: Option<crate::world_info::BiomeSource> = None;
        let mut structure_overrides: Option<Vec<String>> = None;

        if let Some(wgs) = crate::world_info::data_files::read_world_gen_settings(main_folder)
            && let Some(dim_settings) = wgs.dimensions.get(dimension.minecraft_name)
        {
            biome_source.clone_from(&dim_settings.generator.biome_source);

            if dim_settings.generator.generator_type == "minecraft:flat" {
                is_flat = true;
                let flat_settings = dim_settings
                    .generator
                    .settings
                    .as_ref()
                    .and_then(crate::world_info::GeneratorSettings::as_flat_settings)
                    .or_else(|| {
                        crate::world_info::FlatLevelGeneratorPreset::from_name("classic_flat")
                            .map(|p| p.settings)
                    });
                if let Some(flat_settings) = flat_settings {
                    flat_layers = flat_settings.to_flat_layers();
                    structure_overrides = flat_settings.structure_overrides_vec();
                    flat_biome = flat_settings.biome;
                }
            } else if let Some(crate::world_info::GeneratorSettings::Reference(s)) =
                &dim_settings.generator.settings
            {
                generator_settings_name = Some(s.clone());
            }
        }

        let dim_min_y = dimension.min_y;
        let dim_height = dimension.height;
        let seed = Seed(seed as u64);
        let world_gen: Arc<WorldGenerator> = Arc::from(get_world_gen_with_all_settings(
            seed,
            dimension,
            is_flat,
            flat_layers,
            flat_biome,
            generator_settings_name.as_deref(),
            biome_source.as_ref(),
            structure_overrides.as_deref(),
        ));

        let chunk_saver = match &level_config.chunk {
            ChunkConfig::Linear => Arc::new(ChunkSaver::Linear(ChunkFileManager::new(()))),
            ChunkConfig::Anvil(config) => {
                Arc::new(ChunkSaver::Anvil(ChunkFileManager::new(config.clone())))
            }
            ChunkConfig::Pump => Arc::new(ChunkSaver::Pump(ChunkFileManager::new(()))),
        };
        let entity_saver = match &level_config.chunk {
            ChunkConfig::Linear => Arc::new(EntitySaver::Linear(ChunkFileManager::new(()))),
            ChunkConfig::Anvil(config) => {
                Arc::new(EntitySaver::Anvil(ChunkFileManager::new(config.clone())))
            }
            ChunkConfig::Pump => Arc::new(EntitySaver::Pump(ChunkFileManager::new(()))),
        };

        let pending_entity_generations = Arc::new(DashMap::new());
        let level_channel = Arc::new(LevelChannel::new());
        let thread_tracker = Mutex::new(Vec::new());
        let listener = Arc::new(ChunkListener::new());

        let level_ref = Arc::new(Self {
            seed,
            world_portal: ArcSwap::new(Arc::new(None)),
            world_gen: ArcSwap::new(world_gen),
            level_folder,
            cluster_dimension,
            cluster_world_key,
            cluster_genesis_required,
            fresh_chunk_admission: ArcSwapOption::empty(),
            persisted_chunk_admission: ArcSwapOption::empty(),
            fresh_chunk_wake: ArcSwapOption::empty(),
            lighting_config: level_config.lighting,
            light_engine: DynamicLightEngine::new(dim_min_y, dim_min_y + dim_height),
            chunk_saver,
            entity_saver,
            schedule_tick_counts: AtomicU64::new(0),
            loaded_chunks: Arc::new(DashMap::new()),
            cluster_snapshots: Arc::new(DashMap::new()),
            cluster_snapshot_publication: ArcSwapOption::empty(),
            cluster_publication_reservations: Arc::new(ArcSwap::from_pointee(
                rustc_hash::FxHashMap::default(),
            )),
            cluster_persisted_decision: ArcSwapOption::empty(),
            cluster_peer_install_claim: ArcSwapOption::empty(),
            cluster_owner_chunk_demand: ArcSwapOption::empty(),
            cluster_unsettled_primary_chunks: ArcSwap::from_pointee(FxHashSet::default()),
            cluster_primary_chunk_fences: ArcSwap::from_pointee(FxHashMap::default()),
            cluster_peer_install_releases: Arc::new(ArrayQueue::new(1_024)),
            cluster_peer_claim_capacity: ArcSwapOption::empty(),
            cluster_pinned: Arc::new(dashmap::DashSet::new()),
            cluster_lobby_pins: ClusterLobbyNeedCounts::new(),
            cluster_fetch_wanted: Arc::new(dashmap::DashSet::new()),
            cluster_dual: ClusterDualState::new(),
            loaded_chunk_changes: Arc::new(SegQueue::new()),
            loaded_entity_chunks: Arc::new(DashMap::new()),
            chunks_with_scheduled_ticks: Arc::new(dashmap::DashSet::new()),
            chunk_loading: Mutex::new(ChunkLoading::new(level_channel.clone())),
            chunk_watchers: Arc::new(DashMap::new()),
            tasks: TaskTracker::new(),
            chunk_system_tasks: TaskTracker::new(),
            cancel_token: CancellationToken::new(),
            shut_down_chunk_system: AtomicBool::new(false),
            should_save: AtomicBool::new(false),
            should_unload: AtomicBool::new(false),
            save_enabled: AtomicBool::new(true),
            autosave_ticks: level_config.autosave_ticks,
            pending_entity_generations,
            level_channel: level_channel.clone(),
            thread_tracker,
            chunk_listener: listener.clone(),
        });

        GenerationSchedule::create(
            4,
            level_ref.clone(),
            level_channel,
            listener,
            level_ref
                .thread_tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut(),
        );

        level_ref
    }

    pub fn set_world_gen(&self, generator: Arc<WorldGenerator>) {
        self.world_gen.store(generator);
    }

    #[must_use]
    pub fn world_gen(&self) -> Arc<WorldGenerator> {
        self.world_gen.load_full()
    }

    pub fn spawn_entity_generation(self: &Arc<Self>, pos: Vector2<i32>) {
        if is_cluster_secondary() {
            self.prefetch_cluster_chunk(pos);
            drop(self.pending_entity_generations.remove(&pos));
            return;
        }
        let level = self.clone();
        rayon::spawn(move || {
            let arc_chunk = Arc::new(ChunkEntityData {
                x: pos.x,
                z: pos.y,
                data: std::sync::Mutex::new(Vec::new()),
                dirty: AtomicBool::new(false),
            });

            level.loaded_entity_chunks.insert(pos, arc_chunk.clone());

            if let Some((_, waiters)) = level.pending_entity_generations.remove(&pos) {
                for tx in waiters {
                    let _ = tx.send(arc_chunk.clone());
                }
            }
        });
    }

    /// Spawns a task associated with this world. All tasks spawned with this method are awaited
    /// when the client. This means tasks should complete in a reasonable (no looping) amount of time.
    pub fn spawn_task<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(task)
    }

    pub async fn shutdown(&self) {
        let world_id = self.level_folder.root_folder.display();
        info!("Saving level ({})...", world_id);
        self.cancel_token.cancel();
        self.shut_down_chunk_system.store(true, Ordering::Relaxed);
        self.level_channel.notify();

        self.tasks.close();
        self.chunk_system_tasks.close();

        let handles = {
            let mut lock = self
                .thread_tracker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lock.drain(..).collect::<Vec<_>>()
        };

        let handle_count = handles.len();
        info!("Joining {} threads for {}...", handle_count, world_id);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = std::thread::Builder::new()
            .name("Thread-Joiner".into())
            .spawn(move || {
                let mut failed_count = 0;
                for handle in handles {
                    if handle.join().is_err() {
                        failed_count += 1;
                    }
                }
                let _ = tx.send(failed_count);
            });

        match timeout(Duration::from_secs(3), rx).await {
            Ok(Ok(failed_count)) => {
                if failed_count > 0 {
                    warn!(
                        "{} threads failed to join properly for {}.",
                        failed_count, world_id
                    );
                }
            }
            Ok(Err(_)) => {
                warn!("Thread join task panicked for {}.", world_id);
            }
            Err(_) => {
                warn!("Timed out waiting for threads to join for {}.", world_id);
            }
        }

        self.tasks.wait().await;
        self.chunk_system_tasks.wait().await;

        info!("Flushing chunk data to disk for {}...", world_id);
        self.chunk_saver.block_and_await_ongoing_tasks().await;
        info!("Flushing entity data to disk for {}...", world_id);
        self.entity_saver.block_and_await_ongoing_tasks().await;

        // save all chunks currently in memory
        let chunks_to_write = self
            .loaded_entity_chunks
            .iter()
            .map(|chunk| (*chunk.key(), chunk.value().clone()))
            .collect::<Vec<_>>();
        self.loaded_entity_chunks.clear();

        // TODO: I think the chunk_saver should be at the server level
        self.entity_saver.clear_watched_chunks().await;
        self.write_entity_chunks(chunks_to_write).await;
    }

    pub fn loaded_chunk_count(&self) -> usize {
        self.loaded_chunks.len()
    }

    pub fn list_cached(&self) {
        for entry in self.loaded_chunks.iter() {
            debug!("In map: {:?}", entry.key());
        }
    }

    /// Marks chunks as "watched" by a unique player. When no players are watching a chunk,
    /// it is removed from memory. Should only be called on chunks the player was not watching
    /// before
    pub async fn mark_chunks_as_newly_watched(&self, chunks: &[Vector2<i32>]) {
        for chunk in chunks {
            self.chunk_watchers
                .entry(*chunk)
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
        }

        self.entity_saver
            .watch_chunks(&self.level_folder, chunks)
            .await;
    }

    /// Marks chunks no longer "watched" by a unique player. When no players are watching a chunk,
    /// it is removed from memory. Should only be called on chunks the player was watching before
    pub async fn mark_chunks_as_not_watched(
        &self,
        chunks: impl IntoIterator<Item = impl std::borrow::Borrow<Vector2<i32>>>,
    ) -> Vec<Vector2<i32>> {
        let mut chunks_to_clean = Vec::new();
        let chunks_vec: Vec<Vector2<i32>> = chunks.into_iter().map(|c| *c.borrow()).collect();

        for chunk in &chunks_vec {
            if let Entry::Occupied(mut entry) = self.chunk_watchers.entry(*chunk) {
                *entry.get_mut() = entry.get().saturating_sub(1);
                if *entry.get() == 0 {
                    entry.remove();
                    chunks_to_clean.push(*chunk);
                }
            }
        }

        self.entity_saver
            .unwatch_chunks(&self.level_folder, &chunks_vec)
            .await;
        chunks_to_clean
    }

    /// Returns whether the chunk should be removed from memory
    #[inline]
    pub async fn mark_chunk_as_not_watched(&self, chunk: Vector2<i32>) -> bool {
        !self.mark_chunks_as_not_watched([chunk]).await.is_empty()
    }

    // In Level::clean_entity_chunks()
    pub fn clean_entity_chunks(
        self: &Arc<Self>,
        chunks: impl IntoIterator<Item = impl std::borrow::Borrow<Vector2<i32>>>,
    ) {
        let chunks_to_process: Vec<_> = chunks
            .into_iter()
            .filter_map(|pos_borrow| {
                let pos = pos_borrow.borrow();
                // Only include chunks with no watchers
                let has_watchers = self
                    .chunk_watchers
                    .get(pos)
                    .is_some_and(|count| *count != 0);

                if has_watchers {
                    return None;
                }

                // Remove immediately to prevent race conditions
                self.loaded_entity_chunks.remove(pos)
            })
            .collect();

        if chunks_to_process.is_empty() {
            return;
        }

        let level = self.clone();
        self.spawn_task(async move {
            debug!("Writing {} entity chunks to disk", chunks_to_process.len());
            level.write_entity_chunks(chunks_to_process).await;
        });
    }

    pub fn get_tick_data(
        &self,
        active_chunks: &FxHashSet<Vector2<i32>>,
        random_tick_speed: i64,
    ) -> TickData {
        self.collect_tick_data(active_chunks, random_tick_speed, true)
    }

    /// Samples random ticks without stepping the legacy scheduled queues.
    /// This is a transition seam only: it still reads `Level` chunk locks, so
    /// owner-lane simulation must move random sampling into owned state too.
    pub fn get_random_tick_data(
        &self,
        active_chunks: &FxHashSet<Vector2<i32>>,
        random_tick_speed: i64,
    ) -> TickData {
        self.collect_tick_data(active_chunks, random_tick_speed, false)
    }

    fn collect_tick_data(
        &self,
        active_chunks: &FxHashSet<Vector2<i32>>,
        random_tick_speed: i64,
        include_scheduled: bool,
    ) -> TickData {
        let samples_per_section = random_tick_speed.max(0);

        let mut ticks = TickData {
            block_ticks: Vec::new(),
            fluid_ticks: Vec::new(),
            random_ticks: Vec::with_capacity(active_chunks.len() * 3),
        };

        // 1. Process active chunks (random ticks, block entities)
        for pos in active_chunks {
            if let Some(chunk) = self.loaded_chunks.get(pos) {
                let chunk = chunk.value();
                let chunk_x_base = chunk.x * 16;
                let chunk_z_base = chunk.z * 16;
                let section_count = chunk.section.count;

                // Use the bitmask to skip sections
                let mask = chunk.section.randomly_ticking_mask.load(Ordering::Relaxed);
                if mask != 0 {
                    let sections = chunk
                        .section
                        .block_sections
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let min_y = chunk.section.min_y;

                    for i in 0..section_count {
                        if (mask & (1 << i)) == 0 {
                            continue;
                        }
                        let y_base = min_y + (i as i32 * 16);
                        for _ in 0..samples_per_section {
                            let r = rand::random::<u32>();
                            let x_offset = (r & 0xF) as usize;
                            let z_offset = (r >> 8 & 0xF) as usize;
                            let y_in_section = ((r >> 4) & 0xF) as usize;

                            let block_state_id = sections[i].get(x_offset, y_in_section, z_offset);
                            let tick_block = has_random_ticks(block_state_id);
                            let tick_fluid = has_random_ticking_fluid(block_state_id);
                            if tick_block || tick_fluid {
                                ticks.random_ticks.push(RandomTickSample {
                                    position: BlockPos::new(
                                        chunk_x_base + x_offset as i32,
                                        y_base + y_in_section as i32,
                                        chunk_z_base + z_offset as i32,
                                    ),
                                    tick_block,
                                    tick_fluid,
                                });
                            }
                        }
                    }
                }
            }
        }

        // 2. Process chunks with scheduled ticks
        // We collect keys first to avoid holding DashSet shard lock while accessing loaded_chunks (deadlock risk)
        let scheduled_chunk_pos: Vec<_> = if include_scheduled {
            self.chunks_with_scheduled_ticks
                .iter()
                .map(|p| *p)
                .collect()
        } else {
            Vec::new()
        };
        for pos in scheduled_chunk_pos {
            if let Some(chunk) = self.loaded_chunks.get(&pos) {
                let chunk = chunk.value();
                ticks.block_ticks.append(&mut chunk.block_ticks.step_tick());
                ticks.fluid_ticks.append(&mut chunk.fluid_ticks.step_tick());

                // Remove from set if it no longer has ticks
                if !chunk.block_ticks.has_ticks() && !chunk.fluid_ticks.has_ticks() {
                    self.chunks_with_scheduled_ticks.remove(&pos);
                }
            } else {
                self.chunks_with_scheduled_ticks.remove(&pos); // Chunk unloaded
            }
        }

        ticks.block_ticks.sort_unstable();
        ticks.fluid_ticks.sort_unstable();

        ticks
    }

    pub fn clean_entity_chunk(self: &Arc<Self>, chunk: &Vector2<i32>) {
        self.clean_entity_chunks([*chunk]);
    }

    pub fn is_chunk_watched(&self, chunk: &Vector2<i32>) -> bool {
        self.chunk_watchers.get(chunk).is_some()
    }

    pub fn clean_memory(self: &Arc<Self>) -> Vec<Vector2<i32>> {
        self.chunk_watchers.retain(|_, watcher| *watcher != 0);

        let entity_chunks_to_remove: Vec<_> = self
            .loaded_entity_chunks
            .iter()
            .filter(|entry| !self.chunk_watchers.contains_key(entry.key()))
            .map(|entry| *entry.key())
            .collect();

        // We do not clean them here because we want the caller to save any active entities in them first.

        // if the difference is too big, we can shrink the loaded chunks
        // (1024 chunks is the equivalent to a 32x32 chunks area)
        if self.chunk_watchers.capacity() - self.chunk_watchers.len() >= 4096 {
            self.chunk_watchers.shrink_to_fit();
        }

        if self.loaded_chunks.capacity() - self.loaded_chunks.len() >= 4096 {
            self.loaded_chunks.shrink_to_fit();
        }

        if self.loaded_entity_chunks.capacity() - self.loaded_entity_chunks.len() >= 4096 {
            self.loaded_entity_chunks.shrink_to_fit();
        }
        entity_chunks_to_remove
    }

    pub async fn get_or_fetch_chunk<R, F: Fn(&SyncChunk) -> R>(
        self: &Arc<Self>,
        pos: Vector2<i32>,
        f: F,
    ) -> R {
        if let Some(res) = self.read_chunk_sync(&pos, &f) {
            return res;
        }
        if is_cluster_secondary() {
            self.prefetch_cluster_chunk(pos);
            let empty = ChunkData::empty_sync(pos.x, pos.y);
            return f(&empty);
        }
        let chunk = self.fetch_chunk(pos).await;
        if self.loaded_chunks.insert(pos, chunk.clone()).is_none() {
            self.loaded_chunk_changes
                .push(LoadedChunkChange::Loaded(pos));
        }
        f(&chunk)
    }

    pub fn loaded_chunk_changes(&self) -> impl Iterator<Item = LoadedChunkChange> + '_ {
        std::iter::from_fn(|| self.loaded_chunk_changes.pop())
    }

    async fn fetch_chunk(self: &Arc<Self>, pos: Vector2<i32>) -> SyncChunk {
        let recv = self.chunk_listener.add_single_chunk_listener(pos);

        {
            let mut lock = self
                .chunk_loading
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lock.add_ticket(pos, ChunkLoading::FULL_CHUNK_LEVEL);
            lock.send_change();
        };

        let chunk = recv
            .await
            .unwrap_or_else(|_| ChunkData::empty_sync(pos.x, pos.y));

        {
            let mut lock = self
                .chunk_loading
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lock.remove_ticket(pos, ChunkLoading::FULL_CHUNK_LEVEL);
            lock.send_change();
        };

        CLUSTER_CHUNK_GENERATED.fetch_add(1, Ordering::Relaxed);

        chunk
    }

    async fn load_single_entity_chunk(
        &self,
        pos: Vector2<i32>,
    ) -> Result<(SyncEntityChunk, bool), ChunkReadingError> {
        if is_cluster_secondary() {
            return Err(ChunkReadingError::ChunkNotExist);
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.entity_saver
            .fetch_chunks(&self.level_folder, &[pos], tx)
            .await;

        match rx.recv().await {
            Some(LoadedData::Loaded(chunk)) => Ok((chunk, false)),
            Some(LoadedData::Error((_, err))) => Err(err),
            _ => Err(ChunkReadingError::ChunkNotExist),
        }
    }

    pub fn receive_entity_chunks(
        self: &Arc<Self>,
        chunks: Vec<Vector2<i32>>,
    ) -> Receiver<(Weak<ChunkEntityData>, bool)> {
        let (sender, receiver) = mpsc::channel(64);
        let level = self.clone();

        self.spawn_task(async move {
            let cancel_notifier = level.cancel_token.cancelled();

            let fetch_task = async {
                let to_fetch: Vec<_> = chunks
                    .iter()
                    .filter(|pos| {
                        level.loaded_entity_chunks.get(pos).is_none_or(|chunk| {
                            let _ = sender.try_send((Arc::downgrade(chunk.value()), false));
                            false // Don't fetch
                        })
                    })
                    .copied()
                    .collect();

                if !to_fetch.is_empty() {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<
                        LoadedData<SyncEntityChunk, ChunkReadingError>,
                    >(to_fetch.len());

                    if is_cluster_secondary() {
                        for pos in &to_fetch {
                            let _ = tx.send(LoadedData::Missing(*pos)).await;
                        }
                    } else {
                        level
                            .entity_saver
                            .fetch_chunks(&level.level_folder, &to_fetch, tx)
                            .await;
                    }

                    while let Some(data) = rx.recv().await {
                        match data {
                            LoadedData::Loaded(chunk) => {
                                let pos = Vector2::new(chunk.x, chunk.z);
                                level.loaded_entity_chunks.insert(pos, chunk.clone());
                                let _ = sender.send((Arc::downgrade(&chunk), true)).await;
                            }
                            LoadedData::Missing(pos) | LoadedData::Error((pos, _)) => {
                                if is_cluster_secondary() {
                                    level.prefetch_cluster_chunk(pos);
                                } else {
                                    let (tx, rx) = oneshot::channel();
                                    match level.pending_entity_generations.entry(pos) {
                                        dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                                            entry.get_mut().push(tx);
                                        }
                                        dashmap::mapref::entry::Entry::Vacant(entry) => {
                                            entry.insert(vec![tx]);
                                            level.spawn_entity_generation(pos);
                                        }
                                    }
                                    let sender_clone = sender.clone();
                                    tokio::spawn(async move {
                                        if let Ok(chunk) = rx.await {
                                            let _ = sender_clone
                                                .send((Arc::downgrade(&chunk), true))
                                                .await;
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
            };

            select! {
                () = cancel_notifier => {},
                () = fetch_task => {}
            }
        });

        receiver
    }

    pub async fn get_entity_chunk(self: &Arc<Self>, pos: Vector2<i32>) -> SyncEntityChunk {
        if let Some(chunk) = self.loaded_entity_chunks.get(&pos) {
            return chunk.clone();
        }

        if let Ok((chunk, _)) = self.load_single_entity_chunk(pos).await {
            self.loaded_entity_chunks.insert(pos, chunk.clone());
            chunk
        } else if is_cluster_secondary() {
            self.prefetch_cluster_chunk(pos);
            Arc::new(ChunkEntityData {
                x: pos.x,
                z: pos.y,
                data: std::sync::Mutex::new(Vec::new()),
                dirty: AtomicBool::new(false),
            })
        } else {
            let (tx, rx) = oneshot::channel();
            match self.pending_entity_generations.entry(pos) {
                dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(tx);
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    entry.insert(vec![tx]);
                    self.spawn_entity_generation(pos);
                }
            }
            rx.await.unwrap_or_else(|_| {
                Arc::new(ChunkEntityData {
                    x: pos.x,
                    z: pos.y,
                    data: std::sync::Mutex::new(Vec::new()),
                    dirty: AtomicBool::new(false),
                })
            })
        }
    }

    pub fn get_block_state(&self, position: &BlockPos) -> BlockStateId {
        let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
        let id = self
            .read_chunk_sync(&chunk_coordinate, |chunk| {
                chunk.section.get_block_absolute_y(
                    relative.x as usize,
                    relative.y,
                    relative.z as usize,
                )
            })
            .flatten();

        id.unwrap_or(Block::VOID_AIR.default_state.id)
    }

    pub fn set_block_state(
        &self,
        position: &BlockPos,
        block_state_id: BlockStateId,
    ) -> BlockStateId {
        let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
        self.read_chunk_sync(&chunk_coordinate, |chunk| {
            let replaced_block_state_id = chunk.set_block_absolute_y(
                relative.x as usize,
                relative.y,
                relative.z as usize,
                block_state_id,
            );
            if replaced_block_state_id != block_state_id {
                chunk.mark_dirty(true);
            }
            replaced_block_state_id
        })
        .unwrap_or(Block::VOID_AIR.default_state.id)
    }
}

fn cluster_fetch_sender_for(pos: Vector2<i32>) -> Option<mpsc::Sender<ClusterFetchRequest>> {
    if !cluster_has_peers() {
        CLUSTER_CHUNK_REFUSED.fetch_add(1, Ordering::Relaxed);
        if fetch_warn_cooldown_elapsed() {
            error!(
                chunk_x = pos.x,
                chunk_z = pos.y,
                refused = CLUSTER_CHUNK_REFUSED.load(Ordering::Relaxed),
                "cluster secondary has no peers configured, chunk stays empty until a holder is known"
            );
        }
        return None;
    }
    CLUSTER_FETCH_SENDER.get().cloned()
}

impl Level {
    pub fn install_primary_chunk_fence(&self, pos: Vector2<i32>) {
        self.cluster_primary_chunk_fences.rcu(|current| {
            let mut next = (**current).clone();
            next.entry(pos)
                .or_insert_with(|| Arc::new(AtomicU8::new(0)));
            Arc::new(next)
        });
    }

    pub fn retire_primary_chunk_fence(&self, pos: Vector2<i32>) {
        let retiring = self.cluster_primary_chunk_fences.load().get(&pos).cloned();
        if let Some(fence) = &retiring {
            // A reader may hold an older ArcSwap map after this method returns.
            fence.store(2, Ordering::Release);
        }
        self.cluster_primary_chunk_fences.rcu(|current| {
            let mut next = (**current).clone();
            if current
                .get(&pos)
                .zip(retiring.as_ref())
                .is_some_and(|(existing, retiring)| Arc::ptr_eq(existing, retiring))
            {
                next.remove(&pos);
            }
            Arc::new(next)
        });
    }

    pub fn claim_primary_chunk_for_actions(&self, pos: Vector2<i32>) -> bool {
        let fences = self.cluster_primary_chunk_fences.load();
        let Some(fence) = fences.get(&pos) else {
            return false;
        };
        fence.load(Ordering::Acquire) == 1
            || fence
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    pub fn release_primary_chunk_action_claim(&self, pos: Vector2<i32>) {
        if let Some(fence) = self.cluster_primary_chunk_fences.load().get(&pos) {
            let _ = fence.compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    pub fn try_begin_primary_chunk_unload(&self, pos: Vector2<i32>) -> bool {
        self.cluster_primary_chunk_fences
            .load()
            .get(&pos)
            .is_some_and(|fence| {
                fence
                    .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
    }

    pub fn pin_cluster_chunk(&self, pos: Vector2<i32>) {
        self.cluster_pinned.insert(pos);
    }

    pub fn is_cluster_pinned(&self, pos: &Vector2<i32>) -> bool {
        self.cluster_pinned.contains(pos) || self.cluster_lobby_pins.contains(pos)
    }

    pub fn unpin_cluster_chunk(&self, pos: &Vector2<i32>) {
        self.cluster_pinned.remove(pos);
    }

    pub fn acquire_cluster_lobby_need(&self, positions: &[Vector2<i32>]) {
        self.cluster_lobby_pins.acquire(positions);
        for position in positions {
            self.prefetch_cluster_chunk(*position);
        }
    }

    pub fn release_cluster_lobby_need(&self, positions: &[Vector2<i32>]) {
        self.cluster_lobby_pins.release(positions);
    }

    pub fn is_cluster_held(&self, pos: &Vector2<i32>) -> bool {
        self.loaded_chunks.contains_key(pos)
    }

    pub fn prefetch_cluster_chunk(&self, pos: Vector2<i32>) {
        if self.is_cluster_held(&pos) || self.cluster_snapshots.contains_key(&pos) {
            return;
        }
        if self.cluster_fetch_wanted.insert(pos) {
            self.level_channel.notify();
        }
    }

    pub fn prefetch_cluster_chunk_from(&self, pos: Vector2<i32>, preferred: u16) -> bool {
        if self.is_cluster_held(&pos) || self.cluster_snapshots.contains_key(&pos) {
            return true;
        }
        self.cluster_fetch_wanted.insert(pos);
        let Some(sender) = cluster_fetch_sender_for(pos) else {
            return false;
        };
        sender
            .try_send(ClusterFetchRequest {
                dimension: self.cluster_dimension,
                world: self.cluster_world_key,
                pos,
                preferred: Some(preferred),
            })
            .is_ok()
    }

    pub fn try_dispatch_cluster_fetch(&self, pos: Vector2<i32>) -> bool {
        if !self.cluster_fetch_wanted.contains(&pos) {
            return false;
        }
        let Some(sender) = cluster_fetch_sender_for(pos) else {
            return false;
        };
        if sender
            .try_send(ClusterFetchRequest {
                dimension: self.cluster_dimension,
                world: self.cluster_world_key,
                pos,
                preferred: None,
            })
            .is_ok()
        {
            return true;
        }
        if fetch_warn_cooldown_elapsed() {
            warn!(
                chunk_x = pos.x,
                chunk_z = pos.y,
                "cluster chunk want channel full, request remains queued locally"
            );
        }
        false
    }

    pub fn clear_cluster_fetch_wanted(&self, pos: &Vector2<i32>) {
        self.cluster_fetch_wanted.remove(pos);
    }

    pub fn unwant_cluster_chunk(&self, pos: Vector2<i32>) {
        self.cluster_fetch_wanted.remove(&pos);
        if let Some(sender) = CLUSTER_UNWANT_SENDER.get().cloned() {
            let _ = sender.try_send(ClusterFetchRequest {
                dimension: self.cluster_dimension,
                world: self.cluster_world_key,
                pos,
                preferred: None,
            });
        }
    }

    pub fn keep_cluster_chunks_for_relog(&self, chunks: Vec<Vector2<i32>>, expires_millis: u64) {
        if chunks.is_empty() {
            return;
        }
        for pos in &chunks {
            self.pin_cluster_chunk(*pos);
        }
        if let Some(sender) = CLUSTER_LOGOUT_SENDER.get().cloned() {
            let _ = sender.try_send(ClusterLogoutGrace {
                dimension: self.cluster_dimension,
                world: self.cluster_world_key,
                chunks,
                expires_millis,
            });
        }
    }
}

static FETCH_WARN_LAST_MILLIS: AtomicU64 = AtomicU64::new(0);

fn fetch_warn_cooldown_elapsed() -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0);
    let last = FETCH_WARN_LAST_MILLIS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000 {
        return false;
    }
    FETCH_WARN_LAST_MILLIS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

pub struct ClusterFetchRequest {
    pub dimension: u8,
    pub world: u64,
    pub pos: Vector2<i32>,
    pub preferred: Option<u16>,
}

static CLUSTER_SECONDARY_MODE: AtomicBool = AtomicBool::new(false);
static CLUSTER_HAS_PEERS: AtomicBool = AtomicBool::new(false);
static CLUSTER_FETCH_SENDER: OnceLock<mpsc::Sender<ClusterFetchRequest>> = OnceLock::new();
static CLUSTER_UNWANT_SENDER: OnceLock<mpsc::Sender<ClusterFetchRequest>> = OnceLock::new();
static CLUSTER_LOGOUT_SENDER: OnceLock<mpsc::Sender<ClusterLogoutGrace>> = OnceLock::new();

pub struct ClusterLogoutGrace {
    pub dimension: u8,
    pub world: u64,
    pub chunks: Vec<Vector2<i32>>,
    pub expires_millis: u64,
}
static CLUSTER_CHUNK_FETCHED: AtomicU64 = AtomicU64::new(0);
static CLUSTER_CHUNK_GENERATED: AtomicU64 = AtomicU64::new(0);
static CLUSTER_CHUNK_REFUSED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn cluster_chunk_fetched() -> u64 {
    CLUSTER_CHUNK_FETCHED.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_chunk_generated() -> u64 {
    CLUSTER_CHUNK_GENERATED.load(Ordering::Relaxed)
}

#[must_use]
pub fn cluster_chunk_refused() -> u64 {
    CLUSTER_CHUNK_REFUSED.load(Ordering::Relaxed)
}

#[must_use]
pub fn is_cluster_secondary() -> bool {
    CLUSTER_SECONDARY_MODE.load(Ordering::Relaxed)
}

pub fn set_cluster_secondary(enabled: bool) {
    CLUSTER_SECONDARY_MODE.store(enabled, Ordering::Relaxed);
}

pub fn set_cluster_has_peers(enabled: bool) {
    CLUSTER_HAS_PEERS.store(enabled, Ordering::Relaxed);
}

#[must_use]
pub fn cluster_has_peers() -> bool {
    CLUSTER_HAS_PEERS.load(Ordering::Relaxed)
}

pub fn set_cluster_fetch_sender(sender: mpsc::Sender<ClusterFetchRequest>) {
    let _ = CLUSTER_FETCH_SENDER.set(sender);
}

pub fn set_cluster_unwant_sender(sender: mpsc::Sender<ClusterFetchRequest>) {
    let _ = CLUSTER_UNWANT_SENDER.set(sender);
}

pub fn set_cluster_logout_sender(sender: mpsc::Sender<ClusterLogoutGrace>) {
    let _ = CLUSTER_LOGOUT_SENDER.set(sender);
}

#[must_use]
pub fn cluster_fetch_sender() -> Option<mpsc::Sender<ClusterFetchRequest>> {
    CLUSTER_FETCH_SENDER.get().cloned()
}

const CLUSTER_SNAPSHOT_VERSION: u8 = 1;

#[must_use]
pub fn cluster_encode_snapshot(chunk: &SyncChunk) -> Vec<u8> {
    let body = chunk.to_bytes().unwrap_or_default();
    let mut out = Vec::with_capacity(1_usize.saturating_add(body.len()));
    out.push(CLUSTER_SNAPSHOT_VERSION);
    out.extend_from_slice(&body);
    out
}

/// Checks an immutable admission image without constructing a lock-backed chunk.
/// The owner uses this before binding the image to its separately supplied palettes.
#[must_use]
pub fn cluster_snapshot_layout_matches(
    image: &[u8],
    x: i32,
    z: i32,
    min_y: i32,
    section_count: usize,
) -> bool {
    let Some((version, body)) = image.split_first() else {
        return false;
    };
    if *version != CLUSTER_SNAPSHOT_VERSION || section_count == 0 {
        return false;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let Ok(nbt) = pumpkin_nbt::Nbt::read(&mut reader) else {
        return false;
    };
    if nbt.root_tag.get_int("xPos") != Some(x)
        || nbt.root_tag.get_int("zPos") != Some(z)
        || nbt.root_tag.get_int("yPos") != Some(min_y.div_euclid(16))
    {
        return false;
    }
    let Some(pumpkin_nbt::tag::NbtTag::List(sections)) = nbt.root_tag.child_tags.get("sections")
    else {
        return false;
    };
    if sections.len() != section_count {
        return false;
    }
    let mut seen = vec![false; section_count];
    for section in sections {
        let pumpkin_nbt::tag::NbtTag::Compound(section) = section else {
            return false;
        };
        let Some(y) = section.get_byte("Y") else {
            return false;
        };
        let Ok(index) = usize::try_from(i32::from(y) - min_y.div_euclid(16)) else {
            return false;
        };
        let Some(slot) = seen.get_mut(index) else {
            return false;
        };
        if *slot {
            return false;
        }
        *slot = true;
    }
    seen.into_iter().all(|present| present)
}

/// Advances the owner-held inhabited counter in a private immutable image.
/// The caller must serialize this with every other accepted image mutation.
#[must_use]
pub fn cluster_advance_snapshot_inhabited(image: &[u8], ticks: u64) -> Option<Vec<u8>> {
    let (version, body) = image.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let mut nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    let current = nbt.root_tag.get_long("InhabitedTime").unwrap_or(0);
    let advance = i64::try_from(ticks).ok()?;
    nbt.root_tag
        .put_long("InhabitedTime", current.checked_add(advance)?);
    let encoded = nbt.write();
    let mut out = Vec::with_capacity(encoded.len() + 1);
    out.push(CLUSTER_SNAPSHOT_VERSION);
    out.extend_from_slice(&encoded);
    Some(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterScheduledKind {
    Block,
    Fluid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterScheduledTick {
    pub kind: ClusterScheduledKind,
    pub id: u16,
    pub position: BlockPos,
    pub delay: u8,
    pub priority: TickPriority,
}

/// Decodes the exact legacy queue in a private admission image. Invalid entries
/// fail admission instead of silently changing which callbacks will run.
#[must_use]
pub fn cluster_snapshot_scheduled_ticks(image: &[u8]) -> Option<Vec<ClusterScheduledTick>> {
    use pumpkin_util::resource_location::{FromResourceLocation, ResourceLocation};
    use std::str::FromStr;
    let (version, body) = image.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    let mut out = Vec::new();
    for (name, kind) in [
        ("block_ticks", ClusterScheduledKind::Block),
        ("fluid_ticks", ClusterScheduledKind::Fluid),
    ] {
        let Some(tag) = nbt.root_tag.child_tags.get(name) else {
            continue;
        };
        let pumpkin_nbt::tag::NbtTag::List(list) = tag else {
            return None;
        };
        for entry in list {
            let pumpkin_nbt::tag::NbtTag::Compound(entry) = entry else {
                return None;
            };
            let position = BlockPos::new(
                entry.get_int("x")?,
                entry.get_int("y")?,
                entry.get_int("z")?,
            );
            let delay = u8::try_from(entry.get_int("t")?).ok()?;
            let priority = TickPriority::try_from(entry.get_int("p")?).ok()?;
            let location = ResourceLocation::from_str(entry.get_string("i")?).ok()?;
            let id = match kind {
                ClusterScheduledKind::Block => <&'static Block>::from_resource_location(&location)?
                    .id
                    .as_u16(),
                ClusterScheduledKind::Fluid => {
                    <&'static Fluid>::from_resource_location(&location)?.id
                }
            };
            out.push(ClusterScheduledTick {
                kind,
                id,
                position,
                delay,
                priority,
            });
        }
    }
    Some(out)
}

/// Writes the owner queue into an immutable image; every unrelated chunk field
/// remains byte-equivalent after NBT decoding.
#[must_use]
pub fn cluster_replace_snapshot_scheduled_ticks(
    image: &[u8],
    ticks: &[ClusterScheduledTick],
) -> Option<Vec<u8>> {
    let (version, body) = image.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let mut nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    replace_scheduled_ticks_nbt(&mut nbt.root_tag, ticks)?;
    let encoded = nbt.write();
    let mut out = Vec::with_capacity(encoded.len() + 1);
    out.push(CLUSTER_SNAPSHOT_VERSION);
    out.extend_from_slice(&encoded);
    Some(out)
}

fn replace_scheduled_ticks_nbt(
    root: &mut pumpkin_nbt::NbtCompound,
    ticks: &[ClusterScheduledTick],
) -> Option<()> {
    use pumpkin_util::resource_location::ToResourceLocation;
    for (name, kind) in [
        ("block_ticks", ClusterScheduledKind::Block),
        ("fluid_ticks", ClusterScheduledKind::Fluid),
    ] {
        let mut list = Vec::new();
        for tick in ticks.iter().filter(|tick| tick.kind == kind) {
            let name = match kind {
                ClusterScheduledKind::Block => {
                    let block = Block::from_id(pumpkin_data::BlockId::new(tick.id)?);
                    block.to_resource_location()
                }
                ClusterScheduledKind::Fluid => Fluid::from_id(tick.id)?.to_resource_location(),
            };
            let mut entry = pumpkin_nbt::compound::NbtCompound::new();
            entry.put_int("x", tick.position.0.x);
            entry.put_int("y", tick.position.0.y);
            entry.put_int("z", tick.position.0.z);
            entry.put_int("t", i32::from(tick.delay));
            entry.put_int("p", tick.priority as i32);
            entry.put_string("i", name);
            list.push(pumpkin_nbt::tag::NbtTag::Compound(entry));
        }
        root.put_list(name, list);
    }
    Some(())
}

/// Extracts exact block-entity compounds from a private immutable image.
/// Each value is a standalone NBT document suitable for a canonical cell.
#[must_use]
pub fn cluster_snapshot_block_entities(image: &[u8]) -> Option<Vec<(BlockPos, Vec<u8>)>> {
    use pumpkin_nbt::tag::NbtTag;
    let (version, body) = image.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    let chunk_x = nbt.root_tag.get_int("xPos")?;
    let chunk_z = nbt.root_tag.get_int("zPos")?;
    let Some(tag) = nbt.root_tag.child_tags.get("block_entities") else {
        return Some(Vec::new());
    };
    let NbtTag::List(list) = tag else {
        return None;
    };
    if list.len() > 4096 {
        return None;
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(list.len());
    let mut total_bytes = 0usize;
    for tag in list {
        let NbtTag::Compound(compound) = tag else {
            return None;
        };
        let position = BlockPos::new(
            compound.get_int("x")?,
            compound.get_int("y")?,
            compound.get_int("z")?,
        );
        if position.0.x.div_euclid(16) != chunk_x
            || position.0.z.div_euclid(16) != chunk_z
            || !seen.insert((position.0.x, position.0.y, position.0.z))
        {
            return None;
        }
        let bytes = pumpkin_nbt::Nbt::new(String::new(), compound.clone())
            .write_unnamed_canonical()
            .ok()?;
        if bytes.is_empty() || bytes.len() > 65_536 {
            return None;
        }
        total_bytes = total_bytes.checked_add(bytes.len())?;
        if total_bytes > 8 * 1024 * 1024 {
            return None;
        }
        out.push((position, bytes.to_vec()));
    }
    Some(out)
}

#[must_use]
pub fn cluster_block_entity_document_matches(position: BlockPos, bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > 65_536 {
        return false;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(bytes));
    let Ok(entity) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) else {
        return false;
    };
    entity.root_tag.get_int("x") == Some(position.0.x)
        && entity.root_tag.get_int("y") == Some(position.0.y)
        && entity.root_tag.get_int("z") == Some(position.0.z)
        && entity
            .write_unnamed_canonical()
            .is_ok_and(|canonical| canonical.as_ref() == bytes)
}

/// Checks that a canonical block-entity document belongs to the block-state
/// entity type. Only the exact Minecraft resource ID and known legacy aliases
/// are accepted; another namespace cannot impersonate a vanilla entity.
#[must_use]
pub fn cluster_block_entity_document_type_matches(
    state: pumpkin_data::BlockStateId,
    bytes: &[u8],
) -> bool {
    let type_id = pumpkin_data::BlockState::from_id(state).block_entity_type;
    let Some(expected) =
        pumpkin_data::block_properties::BLOCK_ENTITY_TYPES.get(usize::from(type_id))
    else {
        return false;
    };
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(bytes));
    let Ok(entity) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) else {
        return false;
    };
    let Some(actual) = entity.root_tag.get_string("id") else {
        return false;
    };
    actual == *expected
        || actual.strip_prefix("minecraft:") == Some(*expected)
        || matches!((*expected, actual), ("bed", "Bed"))
}

#[must_use]
pub fn cluster_default_block_entity_document(
    state: pumpkin_data::BlockStateId,
    position: BlockPos,
) -> Option<Vec<u8>> {
    let type_id = pumpkin_data::BlockState::from_id(state).block_entity_type;
    let name = pumpkin_data::block_properties::BLOCK_ENTITY_TYPES.get(usize::from(type_id))?;
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_string("id", format!("minecraft:{name}"));
    root.put_int("x", position.0.x);
    root.put_int("y", position.0.y);
    root.put_int("z", position.0.z);
    pumpkin_nbt::Nbt::new(String::new(), root)
        .write_unnamed_canonical()
        .ok()
        .map(|bytes| bytes.to_vec())
}

/// Replaces block-entity compounds in an immutable image without touching
/// palettes, ticks, lighting, or any unrelated chunk field.
#[must_use]
pub fn cluster_replace_snapshot_block_entities(
    image: &[u8],
    entities: &[(BlockPos, Vec<u8>)],
) -> Option<Vec<u8>> {
    let (version, body) = image.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION || entities.len() > 4096 {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let mut nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    replace_block_entities_nbt(&mut nbt.root_tag, entities)?;
    let encoded = nbt.write();
    let mut out = Vec::with_capacity(encoded.len() + 1);
    out.push(CLUSTER_SNAPSHOT_VERSION);
    out.extend_from_slice(&encoded);
    Some(out)
}

fn replace_block_entities_nbt(
    root: &mut pumpkin_nbt::NbtCompound,
    entities: &[(BlockPos, Vec<u8>)],
) -> Option<()> {
    use pumpkin_nbt::tag::NbtTag;
    if entities.len() > 4096 {
        return None;
    }
    let chunk_x = root.get_int("xPos")?;
    let chunk_z = root.get_int("zPos")?;
    let mut seen = std::collections::BTreeSet::new();
    let mut list = Vec::with_capacity(entities.len());
    let mut total_bytes = 0usize;
    for (position, bytes) in entities {
        if position.0.x.div_euclid(16) != chunk_x
            || position.0.z.div_euclid(16) != chunk_z
            || bytes.is_empty()
            || bytes.len() > 65_536
            || !seen.insert((position.0.x, position.0.y, position.0.z))
        {
            return None;
        }
        total_bytes = total_bytes.checked_add(bytes.len())?;
        if total_bytes > 8 * 1024 * 1024 {
            return None;
        }
        let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(
            bytes.as_slice(),
        ));
        if !cluster_block_entity_document_matches(*position, bytes) {
            return None;
        }
        let entity = pumpkin_nbt::Nbt::read_unnamed(&mut reader).ok()?;
        if entity.root_tag.get_int("x")? != position.0.x
            || entity.root_tag.get_int("y")? != position.0.y
            || entity.root_tag.get_int("z")? != position.0.z
        {
            return None;
        }
        list.push(NbtTag::Compound(entity.root_tag));
    }
    root.put_list("block_entities", list);
    Some(())
}

#[must_use]
pub fn cluster_reencode_snapshot_with_blocks(
    snapshot: &[u8],
    min_y: i32,
    blocks: &[crate::chunk::palette::BlockPalette],
    biomes: &[crate::chunk::palette::BiomePalette],
) -> Option<Vec<u8>> {
    let (version, body) = snapshot.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION || blocks.is_empty() || blocks.len() != biomes.len() {
        return None;
    }
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
    let mut nbt = pumpkin_nbt::Nbt::read(&mut reader).ok()?;
    replace_palettes_nbt(&mut nbt.root_tag, min_y, blocks, biomes)?;
    let bytes = nbt.write();
    let mut output = Vec::with_capacity(bytes.len() + 1);
    output.push(CLUSTER_SNAPSHOT_VERSION);
    output.extend_from_slice(&bytes);
    Some(output)
}

fn replace_palettes_nbt(
    root: &mut pumpkin_nbt::NbtCompound,
    min_y: i32,
    blocks: &[crate::chunk::palette::BlockPalette],
    biomes: &[crate::chunk::palette::BiomePalette],
) -> Option<()> {
    use crate::chunk::{ChunkHeightmapType, ChunkHeightmaps};
    use pumpkin_nbt::tag::NbtTag;
    if blocks.is_empty() || blocks.len() != biomes.len() {
        return None;
    }
    min_y.checked_sub(1)?;
    min_y.checked_add(i32::try_from(blocks.len().checked_mul(16)?).ok()?)?;
    if root.get_int("yPos")? != min_y.div_euclid(16) {
        return None;
    }
    let NbtTag::List(sections) = root.child_tags.get_mut("sections")? else {
        return None;
    };
    if sections.len() != blocks.len() {
        return None;
    }
    let mut seen = vec![false; blocks.len()];
    for section in sections {
        let NbtTag::Compound(section) = section else {
            return None;
        };
        let y = i32::from(section.get_byte("Y")?);
        let index = usize::try_from(y - min_y.div_euclid(16)).ok()?;
        if *seen.get(index)? {
            return None;
        }
        seen[index] = true;
        let palette = blocks.get(index)?.to_disk_nbt();
        let mut state = pumpkin_nbt::NbtCompound::new();
        if let Some(data) = palette.data {
            state.put("data", NbtTag::LongArray(data.to_vec()));
        }
        let names = palette
            .palette
            .iter()
            .map(|&id| {
                let block = Block::from_state_id(id);
                let mut value = pumpkin_nbt::NbtCompound::new();
                let name = if block.name.starts_with("minecraft:") {
                    block.name.to_string()
                } else {
                    format!("minecraft:{}", block.name)
                };
                value.put_string("Name", name);
                if let Some(properties) = block.properties(id) {
                    let properties = properties.to_props();
                    if !properties.is_empty() {
                        let mut tags = pumpkin_nbt::NbtCompound::new();
                        for (key, value) in properties {
                            tags.put_string(key, value.to_string());
                        }
                        value.put_compound("Properties", tags);
                    }
                }
                NbtTag::Compound(value)
            })
            .collect();
        state.put_list("palette", names);
        section.put_compound("block_states", state);
        let palette = biomes.get(index)?.to_disk_nbt();
        let mut state = pumpkin_nbt::NbtCompound::new();
        if let Some(data) = palette.data {
            state.put("data", NbtTag::LongArray(data.to_vec()));
        }
        let names = palette
            .palette
            .iter()
            .map(|&id| {
                let name = pumpkin_data::biome::Biome::from_id(id)
                    .map_or("plains", |biome| biome.registry_id);
                NbtTag::String(
                    if name.starts_with("minecraft:") {
                        name.to_string()
                    } else {
                        format!("minecraft:{name}")
                    }
                    .into(),
                )
            })
            .collect();
        state.put_list("palette", names);
        section.put_compound("biomes", state);
    }
    if seen.iter().any(|value| !value) {
        return None;
    }
    let chunk_x = root.get_int("xPos")?;
    let chunk_z = root.get_int("zPos")?;
    if let Some(NbtTag::List(entities)) = root.child_tags.get_mut("block_entities") {
        let mut kept = Vec::with_capacity(entities.len());
        for entity in std::mem::take(entities) {
            let NbtTag::Compound(ref data) = entity else {
                return None;
            };
            let x = data.get_int("x")?;
            let y = data.get_int("y")?;
            let z = data.get_int("z")?;
            if x.div_euclid(16) != chunk_x || z.div_euclid(16) != chunk_z {
                return None;
            }
            let relative_y = y.checked_sub(min_y)?;
            let section = usize::try_from(relative_y.div_euclid(16)).ok()?;
            let local_y = usize::try_from(relative_y.rem_euclid(16)).ok()?;
            let state = blocks.get(section)?.get(
                x.rem_euclid(16) as usize,
                local_y,
                z.rem_euclid(16) as usize,
            );
            if pumpkin_data::BlockState::from_id(state).block_entity_type != u16::MAX {
                kept.push(entity);
            }
        }
        *entities = kept;
    }
    let mut heightmaps = ChunkHeightmaps::default();
    for x in 0..16 {
        for z in 0..16 {
            let mut found = [false; 3];
            for section in (0..blocks.len()).rev() {
                for y in (0..16).rev() {
                    let id = blocks[section].get(x, y, z);
                    let state = pumpkin_data::BlockState::from_id(id);
                    let absolute_y = min_y + (section * 16 + y) as i32;
                    for (index, kind) in [
                        ChunkHeightmapType::WorldSurface,
                        ChunkHeightmapType::MotionBlocking,
                        ChunkHeightmapType::MotionBlockingNoLeaves,
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        if !found[index] && kind.is_opaque(state) {
                            heightmaps.set(kind, x as i32, z as i32, absolute_y, min_y);
                            found[index] = true;
                        }
                    }
                    if found.iter().all(|found| *found) {
                        break;
                    }
                }
                if found.iter().all(|found| *found) {
                    break;
                }
            }
            for (index, kind) in [
                ChunkHeightmapType::WorldSurface,
                ChunkHeightmapType::MotionBlocking,
                ChunkHeightmapType::MotionBlockingNoLeaves,
            ]
            .into_iter()
            .enumerate()
            {
                if !found[index] {
                    heightmaps.set(kind, x as i32, z as i32, min_y - 1, min_y);
                }
            }
        }
    }
    let mut tags = pumpkin_nbt::NbtCompound::new();
    if let Some(values) = heightmaps.world_surface {
        tags.put("WORLD_SURFACE", NbtTag::LongArray(values.to_vec()));
    }
    if let Some(values) = heightmaps.motion_blocking {
        tags.put("MOTION_BLOCKING", NbtTag::LongArray(values.to_vec()));
    }
    if let Some(values) = heightmaps.motion_blocking_no_leaves {
        tags.put(
            "MOTION_BLOCKING_NO_LEAVES",
            NbtTag::LongArray(values.to_vec()),
        );
    }
    root.put_compound("Heightmaps", tags);
    Some(())
}

/// Complete immutable admission image owned by the world lane. Rendering a cut
/// clones only this private document and never reads a mutable SyncChunk.
#[derive(Clone)]
pub struct OwnedChunkImage {
    nbt: pumpkin_nbt::Nbt,
}

impl OwnedChunkImage {
    pub fn document(&self) -> &pumpkin_nbt::Nbt {
        &self.nbt
    }

    pub fn light_sections(&self) -> Option<Vec<(i32, Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();
        for section in self.nbt.root_tag.get_list("sections")? {
            let pumpkin_nbt::tag::NbtTag::Compound(section) = section else {
                return None;
            };
            let y = i32::from(section.get_byte("Y")?);
            let array = |name| -> Option<Vec<u8>> {
                match section.get_byte_array(name) {
                    Some(data) if data.len() == 2048 => {
                        Some(data.iter().map(|value| *value as u8).collect())
                    }
                    Some(_) => None,
                    None => Some(vec![0; 2048]),
                }
            };
            result.push((y, array("SkyLight")?, array("BlockLight")?));
        }
        result.sort_by_key(|(y, _, _)| *y);
        if result.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return None;
        }
        Some(result)
    }

    pub fn with_palettes(
        &self,
        min_y: i32,
        blocks: &[crate::chunk::palette::BlockPalette],
        biomes: &[crate::chunk::palette::BiomePalette],
    ) -> Option<Self> {
        let mut next = self.clone();
        replace_palettes_nbt(&mut next.nbt.root_tag, min_y, blocks, biomes)?;
        Some(next)
    }

    #[must_use]
    pub fn decode(image: &[u8]) -> Option<Self> {
        let (version, body) = image.split_first()?;
        if *version != CLUSTER_SNAPSHOT_VERSION {
            return None;
        }
        let mut reader =
            pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(body));
        Some(Self {
            nbt: pumpkin_nbt::Nbt::read(&mut reader).ok()?,
        })
    }

    /// Composes every currently owned field at the same caller-selected phase.
    /// Unknown admission fields remain intact until their own mutation route
    /// replaces them; this method does not sample any legacy world state.
    #[must_use]
    pub fn render_ground(
        &self,
        min_y: i32,
        blocks: &[crate::chunk::palette::BlockPalette],
        biomes: &[crate::chunk::palette::BiomePalette],
        inhabited_delta: u64,
        scheduled: &[ClusterScheduledTick],
        entities: &[(BlockPos, Vec<u8>)],
        lighting: Option<&crate::lighting::owned::OwnedChunkLighting>,
    ) -> Option<Vec<u8>> {
        let chunk_x = self.nbt.root_tag.get_int("xPos")?;
        let chunk_z = self.nbt.root_tag.get_int("zPos")?;
        for (position, bytes) in entities {
            if position.0.x.div_euclid(16) != chunk_x || position.0.z.div_euclid(16) != chunk_z {
                return None;
            }
            let relative_y = position.0.y.checked_sub(min_y)?;
            let section = usize::try_from(relative_y.div_euclid(16)).ok()?;
            let state = blocks.get(section)?.get(
                position.0.x.rem_euclid(16) as usize,
                relative_y.rem_euclid(16) as usize,
                position.0.z.rem_euclid(16) as usize,
            );
            if !cluster_block_entity_document_type_matches(state, bytes) {
                return None;
            }
        }
        let mut nbt = self.nbt.clone();
        let current = nbt.root_tag.get_long("InhabitedTime").unwrap_or(0);
        let advance = i64::try_from(inhabited_delta).ok()?;
        nbt.root_tag
            .put_long("InhabitedTime", current.checked_add(advance)?);
        replace_palettes_nbt(&mut nbt.root_tag, min_y, blocks, biomes)?;
        replace_scheduled_ticks_nbt(&mut nbt.root_tag, scheduled)?;
        replace_block_entities_nbt(&mut nbt.root_tag, entities)?;
        if let Some(lighting) = lighting {
            if lighting.block.len() != blocks.len() || lighting.sky.len() != blocks.len() {
                return None;
            }
            let pumpkin_nbt::tag::NbtTag::List(sections) =
                nbt.root_tag.child_tags.get_mut("sections")?
            else {
                return None;
            };
            for section in sections {
                let pumpkin_nbt::tag::NbtTag::Compound(section) = section else {
                    return None;
                };
                let index =
                    usize::try_from(i32::from(section.get_byte("Y")?) - min_y.div_euclid(16))
                        .ok()?;
                section.put(
                    "BlockLight",
                    pumpkin_nbt::tag::NbtTag::ByteArray(
                        lighting
                            .block
                            .get(index)?
                            .iter()
                            .map(|byte| *byte as i8)
                            .collect(),
                    ),
                );
                section.put(
                    "SkyLight",
                    pumpkin_nbt::tag::NbtTag::ByteArray(
                        lighting
                            .sky
                            .get(index)?
                            .iter()
                            .map(|byte| *byte as i8)
                            .collect(),
                    ),
                );
            }
            nbt.root_tag.put_bool("isLightOn", true);
        }
        let bytes = nbt.write();
        let mut out = Vec::with_capacity(bytes.len() + 1);
        out.push(CLUSTER_SNAPSHOT_VERSION);
        out.extend_from_slice(&bytes);
        Some(out)
    }
}

#[must_use]
pub fn cluster_decode_snapshot(x: i32, z: i32, bytes: &[u8]) -> Option<SyncChunk> {
    let (version, body) = bytes.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        error!(
            chunk_x = x,
            chunk_z = z,
            version,
            expected = CLUSTER_SNAPSHOT_VERSION,
            "cluster chunk snapshot version mismatch, refusing decode"
        );
        return None;
    }
    let body = bytes::Bytes::copy_from_slice(body);
    match ChunkData::from_bytes(&body, Vector2::new(x, z)) {
        Ok(chunk) => Some(Arc::new(chunk)),
        Err(error) => {
            warn!(
                chunk_x = x,
                chunk_z = z,
                error = error.to_string(),
                "cluster chunk snapshot decode failed"
            );
            None
        }
    }
}

#[must_use]
pub fn cluster_decode_snapshot_with_blocks(
    x: i32,
    z: i32,
    bytes: &[u8],
) -> Option<(
    SyncChunk,
    i32,
    Box<[crate::chunk::palette::BlockPalette]>,
    Box<[crate::chunk::palette::BiomePalette]>,
)> {
    let (version, body) = bytes.split_first()?;
    if *version != CLUSTER_SNAPSHOT_VERSION {
        error!(
            chunk_x = x,
            chunk_z = z,
            version,
            expected = CLUSTER_SNAPSHOT_VERSION,
            "cluster chunk snapshot version mismatch, refusing decode"
        );
        return None;
    }
    match ChunkData::internal_from_bytes_with_palettes(body, Vector2::new(x, z)) {
        Ok((mut chunk, min_y, blocks, biomes)) => {
            chunk.origin = crate::chunk::ChunkOrigin::PeerSnapshot;
            Some((Arc::new(chunk), min_y, blocks, biomes))
        }
        Err(error) => {
            warn!(
                chunk_x = x,
                chunk_z = z,
                error = error.to_string(),
                "cluster chunk snapshot decode failed"
            );
            None
        }
    }
}

const CLUSTER_DUAL_WIDTH: i32 = crate::chunk::CHUNK_WIDTH as i32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterBlockEdit {
    pub x: usize,
    pub y: i32,
    pub z: usize,
    pub state: BlockStateId,
}

#[derive(Clone)]
pub struct ClusterDualPair {
    pub ground: SyncChunk,
    pub local: SyncChunk,
}

pub struct ClusterDualState {
    pub enabled: AtomicBool,
    pub chunks: DashMap<Vector2<i32>, ClusterDualPair>,
    pub pending: DashMap<Vector2<i32>, Vec<ClusterBlockEdit>>,
    pub applied_tick: AtomicU32,
    pub ground_tick: AtomicU32,
}

impl Default for ClusterDualState {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            chunks: DashMap::new(),
            pending: DashMap::new(),
            applied_tick: AtomicU32::new(CLUSTER_NO_TICK),
            ground_tick: AtomicU32::new(CLUSTER_NO_TICK),
        }
    }
}

impl ClusterDualState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn cluster_copy_blocks(source: &ChunkData, dest: &ChunkData) -> bool {
    let mut changed = false;
    let start = dest.section.min_y;
    let count = dest.section.count;
    for section in 0..count {
        for dy in 0..16 {
            let y = start.saturating_add(ClusterDual::section_offset(section, dy));
            for x in 0..crate::chunk::CHUNK_WIDTH {
                for z in 0..crate::chunk::CHUNK_WIDTH {
                    if let Some(state) = source.section.get_block_absolute_y(x, y, z)
                        && dest.set_block_absolute_y(x, y, z, state) != state
                    {
                        changed = true;
                    }
                }
            }
        }
    }
    changed
}

const CLUSTER_NO_TICK: u32 = u32::MAX;

struct ClusterDual;

impl ClusterDual {
    fn section_offset(section: usize, dy: i32) -> i32 {
        let per_section = i32::try_from(section).unwrap_or(i32::MAX);
        per_section.saturating_mul(16).saturating_add(dy)
    }

    fn fork_chunk(source: &SyncChunk) -> SyncChunk {
        let fork = Arc::new(ChunkData {
            origin: crate::chunk::ChunkOrigin::Unknown,
            section: crate::chunk::ChunkSections::new(source.section.count, source.section.min_y),
            heightmap: std::sync::Mutex::new(crate::chunk::ChunkHeightmaps::default()),
            x: source.x,
            z: source.z,
            block_ticks: crate::tick::scheduler::ChunkTickScheduler::default(),
            fluid_ticks: crate::tick::scheduler::ChunkTickScheduler::default(),
            pending_block_entities: std::sync::Mutex::new(rustc_hash::FxHashMap::default()),
            light_engine: std::sync::Mutex::new(crate::chunk::ChunkLight::default()),
            light_populated: AtomicBool::new(false),
            status: source.status,
            blending_data: None,
            dirty: AtomicBool::new(false),
            inhabited_time: AtomicU64::new(0),
            custom_data: std::sync::Mutex::new(pumpkin_nbt::compound::NbtCompound::new()),
        });
        cluster_copy_blocks(source, &fork);
        fork.mark_dirty(source.is_dirty());
        fork
    }

    fn edit_in_bounds(edit: &ClusterBlockEdit) -> bool {
        edit.x < crate::chunk::CHUNK_WIDTH && edit.z < crate::chunk::CHUNK_WIDTH
    }
}

impl Level {
    #[must_use]
    pub fn cluster_dual_enabled(&self) -> bool {
        self.cluster_dual.enabled.load(Ordering::Relaxed)
    }

    pub fn set_cluster_dual_enabled(&self, enabled: bool) {
        self.cluster_dual.enabled.store(enabled, Ordering::Relaxed);
    }

    fn cluster_pair_or_fork(&self, pos: &Vector2<i32>) -> Option<ClusterDualPair> {
        if !self.cluster_dual_enabled() {
            return None;
        }
        if !self.cluster_dual.chunks.contains_key(pos) {
            let loaded = self.loaded_chunks.get(pos).map(|entry| entry.clone());
            if let Some(chunk) = loaded {
                let pair = ClusterDualPair {
                    ground: ClusterDual::fork_chunk(&chunk),
                    local: ClusterDual::fork_chunk(&chunk),
                };
                self.cluster_dual.chunks.insert(*pos, pair);
            }
        }
        self.cluster_dual.chunks.get(pos).map(|entry| entry.clone())
    }

    pub fn cluster_track_chunk(&self, pos: Vector2<i32>) -> bool {
        self.cluster_pair_or_fork(&pos).is_some()
    }

    pub fn cluster_untrack_chunk(&self, pos: &Vector2<i32>) -> bool {
        let had_pair = self.cluster_dual.chunks.remove(pos).is_some();
        let had_pending = self.cluster_dual.pending.remove(pos).is_some();
        had_pair || had_pending
    }

    #[must_use]
    pub fn cluster_tracked_len(&self) -> usize {
        self.cluster_dual.chunks.len()
    }

    #[must_use]
    pub fn cluster_applied_tick(&self) -> Option<u16> {
        let raw = self.cluster_dual.applied_tick.load(Ordering::Relaxed);
        if raw == CLUSTER_NO_TICK {
            None
        } else {
            u16::try_from(raw).ok()
        }
    }

    pub fn cluster_note_applied_tick(&self, tick: u16) {
        self.cluster_dual
            .applied_tick
            .store(u32::from(tick), Ordering::Relaxed);
    }

    #[must_use]
    pub fn cluster_ground_tick(&self) -> Option<u16> {
        let raw = self.cluster_dual.ground_tick.load(Ordering::Relaxed);
        if raw == CLUSTER_NO_TICK {
            None
        } else {
            u16::try_from(raw).ok()
        }
    }

    pub fn cluster_note_ground_tick(&self, tick: u16) {
        self.cluster_dual
            .ground_tick
            .store(u32::from(tick), Ordering::Relaxed);
    }

    pub fn cluster_apply_local(&self, pos: &Vector2<i32>, edits: &[ClusterBlockEdit]) -> bool {
        let Some(pair) = self.cluster_pair_or_fork(pos) else {
            return false;
        };
        let mut changed = false;
        for edit in edits {
            if !ClusterDual::edit_in_bounds(edit) {
                continue;
            }
            if pair
                .local
                .set_block_absolute_y(edit.x, edit.y, edit.z, edit.state)
                != edit.state
            {
                changed = true;
            }
        }
        if changed {
            pair.local.mark_dirty(true);
        }
        if let Some(mut pendings) = self.cluster_dual.pending.get_mut(pos) {
            pendings.extend(edits.iter().copied().filter(ClusterDual::edit_in_bounds));
        } else {
            self.cluster_dual.pending.insert(
                *pos,
                edits
                    .iter()
                    .copied()
                    .filter(ClusterDual::edit_in_bounds)
                    .collect(),
            );
        }
        true
    }

    pub fn cluster_promote_tick(
        &self,
        pos: &Vector2<i32>,
        accepted: &[ClusterBlockEdit],
        still_pending: &[ClusterBlockEdit],
    ) -> bool {
        let Some(pair) = self.cluster_pair_or_fork(pos) else {
            return false;
        };
        if !accepted.is_empty() {
            let batch: Vec<(usize, i32, usize, BlockStateId)> = accepted
                .iter()
                .copied()
                .filter(ClusterDual::edit_in_bounds)
                .map(|edit| (edit.x, edit.y, edit.z, edit.state))
                .collect();
            pair.ground.set_blocks_batch(batch);
        }
        self.cluster_dual.pending.insert(
            *pos,
            still_pending
                .iter()
                .copied()
                .filter(ClusterDual::edit_in_bounds)
                .collect(),
        );
        let mut local_changed = cluster_copy_blocks(&pair.ground, &pair.local);
        for edit in still_pending {
            if !ClusterDual::edit_in_bounds(edit) {
                continue;
            }
            if pair
                .local
                .set_block_absolute_y(edit.x, edit.y, edit.z, edit.state)
                != edit.state
            {
                local_changed = true;
            }
        }
        if local_changed {
            pair.local.mark_dirty(true);
        }
        true
    }

    pub fn store_cluster_snapshot(&self, pos: Vector2<i32>, snapshot: &SyncChunk) {
        self.cluster_fetch_wanted.remove(&pos);
        self.cluster_snapshots.insert(pos, snapshot.clone());
        self.level_channel.notify();
    }

    pub fn try_publish_cluster_snapshot(
        &self,
        position: Vector2<i32>,
        snapshot: SyncChunk,
        reservation_generation: Option<u64>,
        respond: oneshot::Sender<bool>,
    ) -> bool {
        let queued = self
            .cluster_snapshot_publication
            .load_full()
            .is_some_and(|route| {
                route
                    .try_send(ClusterSnapshotPublication {
                        position,
                        snapshot,
                        reservation_generation,
                        respond,
                    })
                    .is_ok()
            });
        if queued {
            self.level_channel.notify();
        }
        queued
    }

    pub fn try_claim_peer_chunk_install(
        &self,
        position: Vector2<i32>,
        generation: u64,
        respond: tokio::sync::mpsc::Sender<PeerChunkInstallClaimResult>,
    ) -> bool {
        let queued = self
            .cluster_peer_install_claim
            .load_full()
            .is_some_and(|route| {
                route
                    .try_send(PeerChunkInstallClaim::Claim {
                        position,
                        generation,
                        respond,
                    })
                    .is_ok()
            });
        if queued {
            self.level_channel.notify();
        }
        queued
    }

    pub fn try_release_peer_chunk_install(&self, position: Vector2<i32>, generation: u64) -> bool {
        let queued = self
            .cluster_peer_install_claim
            .load_full()
            .is_some_and(|route| {
                match route.try_send(PeerChunkInstallClaim::Release {
                    position,
                    generation,
                }) {
                    Ok(()) => true,
                    Err(crossbeam::channel::TrySendError::Full(_)) => self
                        .cluster_peer_install_releases
                        .push((position, generation))
                        .is_ok(),
                    Err(crossbeam::channel::TrySendError::Disconnected(_)) => false,
                }
            });
        if queued {
            self.level_channel.notify();
        }
        queued
    }

    pub fn cluster_ingest_snapshot(&self, pos: Vector2<i32>, snapshot: &SyncChunk) -> bool {
        if !self.cluster_dual_enabled() {
            return false;
        }
        let pair = ClusterDualPair {
            ground: ClusterDual::fork_chunk(snapshot),
            local: ClusterDual::fork_chunk(snapshot),
        };
        self.cluster_dual.chunks.insert(pos, pair);
        self.cluster_dual.pending.remove(&pos);
        true
    }

    #[must_use]
    pub fn cluster_get_block_state(&self, position: &BlockPos) -> BlockStateId {
        if self.cluster_dual_enabled() {
            let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
            if relative.x >= 0
                && relative.x < CLUSTER_DUAL_WIDTH
                && relative.z >= 0
                && relative.z < CLUSTER_DUAL_WIDTH
                && let Some(pair) = self.cluster_pair_or_fork(&chunk_coordinate)
                && let Some(id) = pair.local.section.get_block_absolute_y(
                    relative.x as usize,
                    relative.y,
                    relative.z as usize,
                )
            {
                return id;
            }
        }
        self.get_block_state(position)
    }

    pub fn cluster_get_ground_block_state(&self, position: &BlockPos) -> BlockStateId {
        if self.cluster_dual_enabled() {
            let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
            if relative.x >= 0
                && relative.x < CLUSTER_DUAL_WIDTH
                && relative.z >= 0
                && relative.z < CLUSTER_DUAL_WIDTH
                && let Some(pair) = self.cluster_pair_or_fork(&chunk_coordinate)
                && let Some(id) = pair.ground.section.get_block_absolute_y(
                    relative.x as usize,
                    relative.y,
                    relative.z as usize,
                )
            {
                return id;
            }
        }
        self.get_block_state(position)
    }

    pub fn cluster_set_block_state(
        &self,
        position: &BlockPos,
        block_state_id: BlockStateId,
    ) -> BlockStateId {
        if self.cluster_dual_enabled() {
            let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
            if relative.x >= 0
                && relative.x < CLUSTER_DUAL_WIDTH
                && relative.z >= 0
                && relative.z < CLUSTER_DUAL_WIDTH
                && let Some(pair) = self.cluster_pair_or_fork(&chunk_coordinate)
            {
                let x = relative.x as usize;
                let z = relative.z as usize;
                let replaced = pair
                    .local
                    .section
                    .get_block_absolute_y(x, relative.y, z)
                    .unwrap_or(Block::VOID_AIR.default_state.id);
                if pair
                    .local
                    .set_block_absolute_y(x, relative.y, z, block_state_id)
                    != block_state_id
                {
                    pair.local.mark_dirty(true);
                }
                let edit = ClusterBlockEdit {
                    x,
                    y: relative.y,
                    z,
                    state: block_state_id,
                };
                if let Some(mut pendings) = self.cluster_dual.pending.get_mut(&chunk_coordinate) {
                    pendings.push(edit);
                } else {
                    self.cluster_dual
                        .pending
                        .insert(chunk_coordinate, vec![edit]);
                }
                return replaced;
            }
        }
        self.set_block_state(position, block_state_id)
    }
}

impl Level {
    pub async fn write_chunks(&self, chunks_to_write: Vec<(Vector2<i32>, SyncChunk)>) {
        if chunks_to_write.is_empty() {
            return;
        }
        if is_cluster_secondary() {
            for (_, chunk) in &chunks_to_write {
                chunk.mark_dirty(false);
            }
            debug!(
                chunks = chunks_to_write.len(),
                "refusing region write on cluster secondary (diskless); discarding"
            );
            return;
        }

        let chunk_saver = self.chunk_saver.clone();
        let level_folder = self.level_folder.clone();

        trace!("Sending chunks to ChunkIO {:}", chunks_to_write.len());
        if let Err(error) = chunk_saver
            .save_chunks(&level_folder, chunks_to_write)
            .await
        {
            error!("Failed writing Chunk to disk {error}");
        }
    }

    pub async fn write_entity_chunks(&self, chunks_to_write: Vec<(Vector2<i32>, SyncEntityChunk)>) {
        if chunks_to_write.is_empty() {
            return;
        }
        if is_cluster_secondary() {
            for (_, chunk) in &chunks_to_write {
                chunk.mark_dirty(false);
            }
            debug!(
                chunks = chunks_to_write.len(),
                "refusing entity write on cluster secondary (diskless); discarding"
            );
            return;
        }

        let chunk_saver = self.entity_saver.clone();
        let level_folder = self.level_folder.clone();

        trace!("Sending chunks to ChunkIO {:}", chunks_to_write.len());
        if let Err(error) = chunk_saver
            .save_chunks(&level_folder, chunks_to_write)
            .await
        {
            error!("Failed writing Chunk to disk {error}");
        }
    }

    pub fn is_chunk_loaded(&self, coordinates: &Vector2<i32>) -> bool {
        self.loaded_chunks.contains_key(coordinates)
    }

    pub fn read_chunk_sync<R, F: Fn(&SyncChunk) -> R>(
        &self,
        coordinates: &Vector2<i32>,
        f: F,
    ) -> Option<R> {
        self.loaded_chunks.get(coordinates).map(|x| f(x.value()))
    }

    pub fn read_entity_chunk_sync<R, F: Fn(&SyncEntityChunk) -> R>(
        &self,
        coordinates: &Vector2<i32>,
        f: F,
    ) -> Option<R> {
        self.loaded_entity_chunks
            .get(coordinates)
            .map(|x| f(x.value()))
    }

    pub fn get_rough_biome(&self, position: &BlockPos) -> &'static Biome {
        let (chunk_coordinate, relative) = position.chunk_and_chunk_relative_position();
        let id = self.read_chunk_sync(&chunk_coordinate, |chunk| {
            chunk.section.get_rough_biome_absolute_y(
                relative.x as usize,
                relative.y,
                relative.z as usize,
            )
        });
        Biome::from_id(id.flatten().unwrap_or(0)).unwrap_or(&Biome::THE_VOID)
    }

    pub fn get_entity_chunk_sync(&self, pos: &Vector2<i32>) -> Option<SyncEntityChunk> {
        self.loaded_entity_chunks
            .get(pos)
            .map(|x| x.value().clone())
    }

    pub async fn get_or_fetch_entity_chunk<R, F: Fn(&SyncEntityChunk) -> R>(
        self: &Arc<Self>,
        pos: Vector2<i32>,
        f: F,
    ) -> R {
        if let Some(res) = self.read_entity_chunk_sync(&pos, &f) {
            return res;
        }
        let chunk = self.get_entity_chunk(pos).await;
        f(&chunk)
    }

    pub fn try_get_entity_chunk(
        &self,
        coordinates: Vector2<i32>,
    ) -> Option<dashmap::mapref::one::Ref<'_, Vector2<i32>, Arc<ChunkEntityData>>> {
        self.loaded_entity_chunks.try_get(&coordinates).try_unwrap()
    }

    pub fn schedule_block_tick(
        &self,
        block: &Block,
        block_pos: BlockPos,
        delay: u8,
        priority: TickPriority,
    ) {
        let tick_order = self.schedule_tick_counts.fetch_add(1, Ordering::Relaxed);
        let scheduled_tick = ScheduledTick {
            delay,
            position: block_pos,
            priority,
            // SAFETY: `block` is a valid reference that outlives this function call for scheduling.
            value: unsafe { &*std::ptr::from_ref::<Block>(block) },
        };

        let chunk_pos = block_pos.chunk_position();
        if self
            .read_chunk_sync(&chunk_pos, |chunk| {
                chunk.block_ticks.schedule_tick(&scheduled_tick, tick_order);
            })
            .is_some()
        {
            self.chunks_with_scheduled_ticks.insert(chunk_pos);
        }
    }

    pub fn schedule_fluid_tick(
        &self,
        fluid: &Fluid,
        block_pos: BlockPos,
        delay: u8,
        priority: TickPriority,
    ) {
        let tick_order = self.schedule_tick_counts.fetch_add(1, Ordering::Relaxed);
        let scheduled_tick = ScheduledTick {
            delay,
            position: block_pos,
            priority,
            // SAFETY: `fluid` is a valid reference that outlives this function call for scheduling.
            value: unsafe { &*std::ptr::from_ref::<Fluid>(fluid) },
        };

        let chunk_pos = block_pos.chunk_position();
        if self
            .read_chunk_sync(&chunk_pos, |chunk| {
                chunk.fluid_ticks.schedule_tick(&scheduled_tick, tick_order);
            })
            .is_some()
        {
            self.chunks_with_scheduled_ticks.insert(chunk_pos);
        }
    }

    pub fn is_block_tick_scheduled(&self, block_pos: &BlockPos, block: &Block) -> bool {
        self.read_chunk_sync(&block_pos.chunk_position(), |chunk| {
            chunk.block_ticks.is_scheduled(*block_pos, block)
        })
        .unwrap_or(false)
    }

    pub fn is_fluid_tick_scheduled(&self, block_pos: &BlockPos, fluid: &Fluid) -> bool {
        self.read_chunk_sync(&block_pos.chunk_position(), |chunk| {
            chunk.fluid_ticks.is_scheduled(*block_pos, fluid)
        })
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_config::world::LevelConfig;
    use tempfile::TempDir;

    #[test]
    fn cluster_snapshot_round_trip_preserves_full_state() {
        let mut chunk = ChunkData::empty(3, -7);
        let stone = Block::STONE.default_state.id;
        chunk.set_block_absolute_y(1, 0, 2, stone);
        chunk.set_block_absolute_y(1, 1, 2, Block::CHEST.default_state.id);
        {
            let mut biomes = chunk.section.biome_sections.write().unwrap();
            let desert = pumpkin_data::biome::Biome::from_name("desert").unwrap().id;
            biomes[4].set(0, 0, 0, desert);
        }
        {
            let mut light = chunk.light_engine.lock().unwrap();
            let count = chunk.section.count;
            light.block_light = vec![crate::chunk::format::LightContainer::Empty(0); count].into();
            light.sky_light = vec![crate::chunk::format::LightContainer::Empty(0); count].into();
            light.block_light[4] =
                crate::chunk::format::LightContainer::Full(vec![0xAB; 2048].into());
            light.sky_light[4] =
                crate::chunk::format::LightContainer::Full(vec![0xCD; 2048].into());
        }
        chunk.light_populated.store(true, Ordering::Relaxed);
        {
            let mut heightmap = chunk.heightmap.lock().unwrap();
            heightmap.world_surface = Some(vec![5_i64; 37].into());
        }
        {
            let mut entities = chunk.pending_block_entities.lock().unwrap();
            let mut entity = pumpkin_nbt::compound::NbtCompound::new();
            entity.put_int("x", 49);
            entity.put_int("y", 1);
            entity.put_int("z", -110);
            entity.put_string("id", "minecraft:chest".to_string());
            entities.insert(BlockPos::new(49, 1, -110), entity);
        }
        {
            let tick = ScheduledTick {
                delay: 4,
                priority: TickPriority::Normal,
                position: BlockPos::new(49, 0, -110),
                value: &Block::STONE,
            };
            chunk.block_ticks = crate::tick::scheduler::ChunkTickScheduler::from_iter([tick]);
            let fluid_tick = ScheduledTick {
                delay: 6,
                priority: TickPriority::High,
                position: BlockPos::new(50, 0, -110),
                value: &Fluid::WATER,
            };
            chunk.fluid_ticks = crate::tick::scheduler::ChunkTickScheduler::from_iter([fluid_tick]);
        }
        chunk.inhabited_time.store(12345, Ordering::Relaxed);
        {
            let mut custom = chunk.custom_data.lock().unwrap();
            custom.put_int("cluster_probe", 7);
        }

        let chunk = Arc::new(chunk);
        let bytes = cluster_encode_snapshot(&chunk);
        assert_eq!(bytes[0], 1);
        let advanced = cluster_advance_snapshot_inhabited(&bytes, 3).unwrap();
        let mut scheduled = cluster_snapshot_scheduled_ticks(&advanced).unwrap();
        assert_eq!(scheduled.len(), 2);
        assert!(
            scheduled
                .iter()
                .any(|tick| tick.kind == ClusterScheduledKind::Block && tick.delay == 4)
        );
        assert!(
            scheduled
                .iter()
                .any(|tick| tick.kind == ClusterScheduledKind::Fluid && tick.delay == 6)
        );
        scheduled[0].delay = 2;
        let scheduled_image =
            cluster_replace_snapshot_scheduled_ticks(&advanced, &scheduled).unwrap();
        assert_eq!(
            cluster_snapshot_scheduled_ticks(&scheduled_image).unwrap(),
            scheduled
        );
        let block_entities = cluster_snapshot_block_entities(&scheduled_image).unwrap();
        assert_eq!(block_entities.len(), 1);
        assert_eq!(block_entities[0].0, BlockPos::new(49, 1, -110));
        assert!(cluster_block_entity_document_matches(
            block_entities[0].0,
            &block_entities[0].1
        ));
        assert!(cluster_block_entity_document_type_matches(
            Block::CHEST.default_state.id,
            &block_entities[0].1
        ));
        assert!(!cluster_block_entity_document_type_matches(
            Block::FURNACE.default_state.id,
            &block_entities[0].1
        ));
        let default_chest = cluster_default_block_entity_document(
            Block::CHEST.default_state.id,
            block_entities[0].0,
        )
        .unwrap();
        assert!(cluster_block_entity_document_matches(
            block_entities[0].0,
            &default_chest
        ));
        assert!(cluster_block_entity_document_type_matches(
            Block::CHEST.default_state.id,
            &default_chest
        ));
        assert!(
            cluster_default_block_entity_document(
                Block::STONE.default_state.id,
                block_entities[0].0,
            )
            .is_none()
        );
        for spoofed in ["othermod:chest", "minecraft:ch-est"] {
            let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(
                std::io::Cursor::new(block_entities[0].1.as_slice()),
            );
            let mut entity = pumpkin_nbt::Nbt::read_unnamed(&mut reader).unwrap();
            entity.root_tag.put_string("id", spoofed.to_string());
            let bytes = entity.write_unnamed_canonical().unwrap();
            assert!(!cluster_block_entity_document_type_matches(
                Block::CHEST.default_state.id,
                &bytes
            ));
        }
        assert!(!cluster_block_entity_document_matches(
            BlockPos::new(50, 1, -110),
            &block_entities[0].1
        ));
        assert!(
            cluster_replace_snapshot_block_entities(
                &scheduled_image,
                &[block_entities[0].clone(), block_entities[0].clone()]
            )
            .is_none()
        );
        let rewritten =
            cluster_replace_snapshot_block_entities(&scheduled_image, &block_entities).unwrap();
        assert_eq!(
            cluster_snapshot_block_entities(&rewritten).unwrap(),
            block_entities
        );
        assert_eq!(
            cluster_snapshot_scheduled_ticks(&rewritten).unwrap(),
            scheduled
        );
        let without_entity = cluster_replace_snapshot_block_entities(&rewritten, &[]).unwrap();
        assert!(
            cluster_snapshot_block_entities(&without_entity)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            cluster_snapshot_scheduled_ticks(&without_entity).unwrap(),
            scheduled
        );
        let (scheduled_chunk, _, _, _) =
            cluster_decode_snapshot_with_blocks(3, -7, &scheduled_image).unwrap();
        assert_eq!(
            scheduled_chunk.pending_block_entities.lock().unwrap().len(),
            1
        );
        assert_eq!(
            scheduled_chunk.inhabited_time.load(Ordering::Relaxed),
            12348
        );
        let (advanced_chunk, _, _, _) =
            cluster_decode_snapshot_with_blocks(3, -7, &advanced).unwrap();
        assert_eq!(advanced_chunk.inhabited_time.load(Ordering::Relaxed), 12348);
        assert_eq!(advanced_chunk.block_ticks.to_vec().len(), 1);
        assert_eq!(advanced_chunk.fluid_ticks.to_vec().len(), 1);
        assert_eq!(
            advanced_chunk.pending_block_entities.lock().unwrap().len(),
            1
        );
        assert!(cluster_snapshot_layout_matches(
            &bytes,
            3,
            -7,
            chunk.section.min_y,
            chunk.section.count,
        ));
        assert!(!cluster_snapshot_layout_matches(
            &bytes,
            4,
            -7,
            chunk.section.min_y,
            chunk.section.count,
        ));
        assert!(!cluster_snapshot_layout_matches(
            &bytes,
            3,
            -7,
            chunk.section.min_y,
            chunk.section.count - 1,
        ));
        let (back, block_min_y, owned_blocks, owned_biomes) =
            cluster_decode_snapshot_with_blocks(3, -7, &bytes).expect("snapshot decodes");

        assert_eq!(back.origin, crate::chunk::ChunkOrigin::PeerSnapshot);
        assert_eq!(back.x, 3);
        assert_eq!(back.z, -7);
        assert_eq!(back.section.min_y, chunk.section.min_y);
        assert_eq!(back.section.count, chunk.section.count);
        assert_eq!(back.section.get_block_absolute_y(1, 0, 2), Some(stone));
        assert_eq!(block_min_y, back.section.min_y);
        assert_eq!(owned_blocks[4].get(1, 0, 2), stone);
        let desert = pumpkin_data::biome::Biome::from_name("desert").unwrap().id;
        assert_eq!(owned_biomes[4].get(0, 0, 0), desert);
        let owner_image = OwnedChunkImage::decode(&bytes).unwrap();
        // Mutating the old Level image cannot affect the owner's private cut.
        chunk
            .custom_data
            .lock()
            .unwrap()
            .put_int("cluster_probe", 99);
        let composed = owner_image
            .render_ground(
                block_min_y,
                &owned_blocks,
                &owned_biomes,
                3,
                &scheduled,
                &block_entities,
                None,
            )
            .unwrap();
        let expected = cluster_reencode_snapshot_with_blocks(
            &rewritten,
            block_min_y,
            &owned_blocks,
            &owned_biomes,
        )
        .unwrap();
        let read = |image: &[u8]| {
            let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(
                std::io::Cursor::new(&image[1..]),
            );
            pumpkin_nbt::Nbt::read(&mut reader).unwrap()
        };
        assert_eq!(read(&composed), read(&expected));
        assert_eq!(
            read(&composed).root_tag.get_long("InhabitedTime"),
            Some(12348)
        );
        assert_eq!(
            read(&composed)
                .root_tag
                .get_compound("PumpkinCustomData")
                .unwrap()
                .get_int("cluster_probe"),
            Some(7)
        );
        assert!(
            owner_image
                .render_ground(
                    block_min_y,
                    &owned_blocks,
                    &owned_biomes,
                    u64::MAX,
                    &scheduled,
                    &block_entities,
                    None,
                )
                .is_none()
        );
        // A failed or repeated render never advances the admission baseline.
        let repeated = owner_image
            .render_ground(
                block_min_y,
                &owned_blocks,
                &owned_biomes,
                3,
                &scheduled,
                &block_entities,
                None,
            )
            .unwrap();
        assert_eq!(read(&repeated), read(&composed));
        let lighting = crate::lighting::owned::OwnedChunkLighting {
            block: (0..owned_blocks.len())
                .map(|_| Box::new([0x11; 2048]))
                .collect(),
            sky: (0..owned_blocks.len())
                .map(|_| Box::new([0x22; 2048]))
                .collect(),
        };
        let relit = owner_image
            .render_ground(
                block_min_y,
                &owned_blocks,
                &owned_biomes,
                3,
                &scheduled,
                &block_entities,
                Some(&lighting),
            )
            .unwrap();
        let (relit_chunk, _, _, _) = cluster_decode_snapshot_with_blocks(3, -7, &relit).unwrap();
        let light = relit_chunk.light_engine.lock().unwrap();
        assert!(
            matches!(&light.block_light[4], crate::chunk::format::LightContainer::Full(bytes)
            if bytes.iter().all(|byte| *byte == 0x11))
        );
        assert!(
            matches!(&light.sky_light[4], crate::chunk::format::LightContainer::Full(bytes)
            if bytes.iter().all(|byte| *byte == 0x22))
        );
        let mut incompatible = owned_blocks.clone();
        incompatible[4].set(1, 1, 2, BlockStateId::AIR);
        assert!(
            owner_image
                .render_ground(
                    block_min_y,
                    &incompatible,
                    &owned_biomes,
                    3,
                    &scheduled,
                    &block_entities,
                    None,
                )
                .is_none()
        );
        {
            let biomes = back.section.biome_sections.read().unwrap();
            let desert = pumpkin_data::biome::Biome::from_name("desert").unwrap().id;
            assert_eq!(biomes[4].get(0, 0, 0), desert);
        }
        {
            let light = back.light_engine.lock().unwrap();
            match &light.block_light[4] {
                crate::chunk::format::LightContainer::Full(data) => {
                    assert!(data.iter().all(|byte| *byte == 0xAB));
                }
                other => panic!("block light lost: {other:?}"),
            }
            match &light.sky_light[4] {
                crate::chunk::format::LightContainer::Full(data) => {
                    assert!(data.iter().all(|byte| *byte == 0xCD));
                }
                other => panic!("sky light lost: {other:?}"),
            }
        }
        assert!(back.light_populated.load(Ordering::Relaxed));
        {
            let heightmap = back.heightmap.lock().unwrap();
            assert_eq!(
                heightmap.world_surface.as_deref(),
                Some(vec![5_i64; 37].as_slice())
            );
        }
        {
            let entities = back.pending_block_entities.lock().unwrap();
            let entity = entities
                .get(&BlockPos::new(49, 1, -110))
                .expect("entity kept");
            assert_eq!(entity.get_string("id"), Some("minecraft:chest"));
        }
        {
            let ticks = back.block_ticks.to_vec();
            assert_eq!(ticks.len(), 1);
            assert_eq!(ticks[0].delay, 4);
            assert_eq!(ticks[0].position, BlockPos::new(49, 0, -110));
            let fluid_ticks = back.fluid_ticks.to_vec();
            assert_eq!(fluid_ticks.len(), 1);
            assert_eq!(fluid_ticks[0].delay, 6);
            assert_eq!(fluid_ticks[0].position, BlockPos::new(50, 0, -110));
        }
        assert_eq!(back.inhabited_time.load(Ordering::Relaxed), 12345);
        assert_eq!(back.status, pumpkin_data::chunk::ChunkStatus::Full);
        {
            let custom = back.custom_data.lock().unwrap();
            assert_eq!(custom.get_int("cluster_probe"), Some(7));
        }
    }

    #[test]
    fn canonical_reencode_replaces_stale_ground_state() {
        let chunk = ChunkData::empty_sync(0, 0);
        let stone = Block::STONE.default_state.id;
        chunk.set_block_absolute_y(1, 0, 1, stone);
        let chest = Block::CHEST.default_state.id;
        chunk.set_block_absolute_y(2, 0, 2, chest);
        chunk.set_block_absolute_y(3, 0, 3, chest);
        {
            let mut entities = chunk.pending_block_entities.lock().unwrap();
            for coordinate in [2, 3] {
                let mut entity = pumpkin_nbt::compound::NbtCompound::new();
                entity.put_int("x", coordinate);
                entity.put_int("y", 0);
                entity.put_int("z", coordinate);
                entity.put_string("id", "minecraft:chest".to_string());
                entities.insert(BlockPos::new(coordinate, 0, coordinate), entity);
            }
        }
        chunk.custom_data.lock().unwrap().put_int("probe", 17);
        let stale = cluster_encode_snapshot(&chunk);
        let (_, min_y, mut blocks, mut biomes) =
            cluster_decode_snapshot_with_blocks(0, 0, &stale).unwrap();
        let section = usize::try_from((0 - min_y).div_euclid(16)).unwrap();
        blocks[section].set(1, 0, 1, Block::AIR.default_state.id);
        blocks[section].set(1, 1, 1, stone);
        blocks[section].set(2, 0, 2, Block::AIR.default_state.id);
        let desert = pumpkin_data::biome::Biome::from_name("desert").unwrap().id;
        biomes[section].set(0, 0, 0, desert);
        let corrected =
            cluster_reencode_snapshot_with_blocks(&stale, min_y, &blocks, &biomes).unwrap();
        let (received, _, _, received_biomes) =
            cluster_decode_snapshot_with_blocks(0, 0, &corrected).unwrap();
        assert_eq!(
            received.section.get_block_absolute_y(1, 0, 1),
            Some(Block::AIR.default_state.id)
        );
        assert_eq!(received.section.get_block_absolute_y(1, 1, 1), Some(stone));
        let entities = received.pending_block_entities.lock().unwrap();
        assert!(!entities.contains_key(&BlockPos::new(2, 0, 2)));
        assert!(entities.contains_key(&BlockPos::new(3, 0, 3)));
        assert_eq!(
            received.heightmap.lock().unwrap().get(
                crate::chunk::ChunkHeightmapType::WorldSurface,
                1,
                1,
                min_y,
            ),
            1
        );
        assert_eq!(
            received.custom_data.lock().unwrap().get_int("probe"),
            Some(17)
        );
        assert_eq!(received_biomes[section].get(0, 0, 0), desert);
    }

    #[test]
    fn cluster_snapshot_rejects_legacy_blocks_only_payload() {
        let mut legacy = vec![0_u8; 8];
        legacy[0..4].copy_from_slice(&(-64_i32).to_le_bytes());
        legacy[4..8].copy_from_slice(&24_u32.to_le_bytes());
        legacy.extend_from_slice(&vec![0_u8; 24 * 16 * 256 * 2]);
        assert!(cluster_decode_snapshot(0, 0, &legacy).is_none());
        assert!(cluster_decode_snapshot(0, 0, &[]).is_none());
        assert!(cluster_decode_snapshot(0, 0, &[2, 0, 1]).is_none());
    }

    #[tokio::test]
    async fn cluster_prefetch_coalesces_each_missing_chunk() {
        let temp_dir = TempDir::new().unwrap();
        let level = Level::from_root_folder(
            &LevelConfig::default(),
            temp_dir.path().to_path_buf(),
            0,
            Dimension::OVERWORLD,
        );
        let pos = Vector2::new(3, 5);
        level.prefetch_cluster_chunk(pos);
        level.prefetch_cluster_chunk(pos);
        assert_eq!(level.cluster_fetch_wanted.len(), 1);
        assert!(level.cluster_fetch_wanted.contains(&pos));
        level.shutdown().await;
    }

    #[tokio::test]
    async fn dimension_paths_26_2() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().to_path_buf();
        let config = LevelConfig::default();

        let overworld_level =
            Level::from_root_folder(&config, root.clone(), 0, Dimension::OVERWORLD);
        assert_eq!(
            overworld_level.level_folder.dim_folder,
            root.join("dimensions").join("minecraft").join("overworld")
        );
        assert_eq!(
            overworld_level.level_folder.region_folder,
            root.join("dimensions")
                .join("minecraft")
                .join("overworld")
                .join("region")
        );

        let nether_level = Level::from_root_folder(&config, root.clone(), 0, Dimension::THE_NETHER);
        assert_eq!(
            nether_level.level_folder.dim_folder,
            root.join("dimensions").join("minecraft").join("the_nether")
        );

        let end_level = Level::from_root_folder(&config, root.clone(), 0, Dimension::THE_END);
        assert_eq!(
            end_level.level_folder.dim_folder,
            root.join("dimensions").join("minecraft").join("the_end")
        );
    }

    #[tokio::test]
    async fn legacy_dimension_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().to_path_buf();
        let config = LevelConfig::default();

        // Create legacy directories
        std::fs::create_dir_all(root.join("region")).unwrap();
        std::fs::create_dir_all(root.join("DIM-1").join("region")).unwrap();
        std::fs::create_dir_all(root.join("DIM1").join("region")).unwrap();

        let overworld_level =
            Level::from_root_folder(&config, root.clone(), 0, Dimension::OVERWORLD);
        assert_eq!(overworld_level.level_folder.dim_folder, root);

        let nether_level = Level::from_root_folder(&config, root.clone(), 0, Dimension::THE_NETHER);
        assert_eq!(nether_level.level_folder.dim_folder, root.join("DIM-1"));

        let end_level = Level::from_root_folder(&config, root.clone(), 0, Dimension::THE_END);
        assert_eq!(end_level.level_folder.dim_folder, root.join("DIM1"));
    }

    #[test]
    fn overlapping_lobby_chunk_needs_survive_one_departure() {
        let needs = ClusterLobbyNeedCounts::new();
        let shared = Vector2::new(3, -4);
        let only_first = Vector2::new(4, -4);
        needs.acquire(&[shared, only_first]);
        needs.acquire(&[shared]);
        needs.release(&[shared, only_first]);
        assert!(needs.contains(&shared));
        assert!(!needs.contains(&only_first));
        needs.release(&[shared]);
        assert!(!needs.contains(&shared));
    }
}
