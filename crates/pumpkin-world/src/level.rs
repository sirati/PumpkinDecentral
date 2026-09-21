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
use arc_swap::ArcSwap;
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, Entry};
use pumpkin_config::{chunk::ChunkConfig, lighting::LightingEngineConfig, world::LevelConfig};
use pumpkin_data::biome::Biome;
use pumpkin_data::chunk::ChunkStatus;
use pumpkin_data::dimension::Dimension;
use pumpkin_data::{Block, BlockStateId, block_properties::has_random_ticks, fluid::Fluid};
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_util::math::{position::BlockPos, vector2::Vector2};
use pumpkin_util::world_seed::Seed;
use rustc_hash::FxHashSet;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
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
    pub lighting_config: LightingEngineConfig,

    /// Counts the number of ticks that have been scheduled for this world
    schedule_tick_counts: AtomicU64,

    // Chunks that are paired with chunk watchers. When a chunk is no longer watched, it is removed
    // from the loaded chunks map and sent to the underlying ChunkIO
    pub loaded_chunks: Arc<DashMap<Vector2<i32>, SyncChunk>>,
    pub cluster_snapshots: Arc<DashMap<Vector2<i32>, SyncChunk>>,
    pub cluster_pinned: Arc<dashmap::DashSet<Vector2<i32>>>,
    pub cluster_load_requested: Arc<dashmap::DashSet<Vector2<i32>>>,
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
    #[expect(clippy::too_many_lines)]
    pub fn from_root_folder(
        level_config: &LevelConfig,
        root_folder: PathBuf,
        seed: i64,
        dimension: Dimension,
    ) -> Arc<Self> {
        let (namespace, name) = match dimension.minecraft_name.split_once(':') {
            Some((ns, n)) => (ns, n),
            None => ("minecraft", dimension.minecraft_name),
        };

        // 26.2 canonical layout: root_folder/dimensions/<namespace>/<name>
        let canonical_dim_folder = root_folder.join("dimensions").join(namespace).join(name);

        // Check if canonical 26.2 folder exists, or fall back to pre-26.2 legacy folders
        let dim_folder = if is_cluster_secondary() {
            canonical_dim_folder
        } else if canonical_dim_folder.exists() {
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

        if !is_cluster_secondary()
            && let Some(wgs) = crate::world_info::data_files::read_world_gen_settings(main_folder)
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
            lighting_config: level_config.lighting,
            light_engine: DynamicLightEngine::new(dim_min_y, dim_min_y + dim_height),
            chunk_saver,
            entity_saver,
            schedule_tick_counts: AtomicU64::new(0),
            loaded_chunks: Arc::new(DashMap::new()),
            cluster_snapshots: Arc::new(DashMap::new()),
            cluster_pinned: Arc::new(dashmap::DashSet::new()),
            cluster_load_requested: Arc::new(dashmap::DashSet::new()),
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
                data: ArcSwap::from_pointee(Vec::new()),
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
        let scheduled_chunk_pos: Vec<_> = self
            .chunks_with_scheduled_ticks
            .iter()
            .map(|p| *p)
            .collect();
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
            if chunk.status == ChunkStatus::Full {
                note_cluster_chunk_full(pos);
            }
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
                                            let _ =
                                                sender_clone.send((Arc::downgrade(&chunk), true)).await;
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
                data: ArcSwap::from_pointee(Vec::new()),
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
                    data: ArcSwap::from_pointee(Vec::new()),
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
    pub fn pin_cluster_chunk(&self, pos: Vector2<i32>) {
        self.cluster_pinned.insert(pos);
    }

    pub fn is_cluster_pinned(&self, pos: &Vector2<i32>) -> bool {
        self.cluster_pinned.contains(pos)
    }

    pub fn unpin_cluster_chunk(&self, pos: &Vector2<i32>) {
        self.cluster_pinned.remove(pos);
    }

    pub fn is_cluster_held(&self, pos: &Vector2<i32>) -> bool {
        self.loaded_chunks.contains_key(pos)
    }

    pub fn is_cluster_full(&self, pos: &Vector2<i32>) -> bool {
        self.loaded_chunks
            .get(pos)
            .is_some_and(|chunk| chunk.status == ChunkStatus::Full)
    }

    pub fn request_cluster_full_chunk(&self, pos: Vector2<i32>) {
        if is_cluster_secondary() {
            error!(
                chunk_x = pos.x,
                chunk_z = pos.y,
                "cluster secondary refused primary chunk materialization request"
            );
            return;
        }
        if self.is_cluster_full(&pos) {
            note_cluster_chunk_full(pos);
            return;
        }
        if !self.cluster_load_requested.insert(pos) {
            return;
        }
        let mut loading = self
            .chunk_loading
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loading.add_force_ticket(pos);
        loading.send_change();
    }

    pub fn finish_cluster_full_chunk_request(&self, pos: Vector2<i32>) {
        if self.cluster_load_requested.remove(&pos).is_none() {
            return;
        }
        let mut loading = self
            .chunk_loading
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loading.remove_force_ticket(pos);
        loading.send_change();
    }

    pub fn prefetch_cluster_chunk(&self, pos: Vector2<i32>) {
        if self.is_cluster_held(&pos) || self.cluster_snapshots.contains_key(&pos) {
            return;
        }
        if self.cluster_fetch_wanted.insert(pos) {
            self.level_channel.notify();
        }
    }

    pub fn try_dispatch_cluster_fetch(&self, pos: Vector2<i32>) -> bool {
        if !self.cluster_fetch_wanted.contains(&pos) {
            return false;
        }
        let Some(sender) = cluster_fetch_sender_for(pos) else {
            return false;
        };
        if sender.try_send(ClusterFetchRequest { pos }).is_ok() {
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
            let _ = sender.try_send(ClusterFetchRequest { pos });
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
    pub pos: Vector2<i32>,
}

static CLUSTER_SECONDARY_MODE: AtomicBool = AtomicBool::new(false);
static CLUSTER_HAS_PEERS: AtomicBool = AtomicBool::new(false);
static CLUSTER_FETCH_SENDER: OnceLock<mpsc::Sender<ClusterFetchRequest>> = OnceLock::new();
static CLUSTER_UNWANT_SENDER: OnceLock<mpsc::Sender<ClusterFetchRequest>> = OnceLock::new();
static CLUSTER_LOGOUT_SENDER: OnceLock<mpsc::Sender<ClusterLogoutGrace>> = OnceLock::new();
static CLUSTER_CHUNK_AVAILABILITY_SENDER: OnceLock<mpsc::Sender<ClusterChunkAvailability>> =
    OnceLock::new();

pub struct ClusterLogoutGrace {
    pub chunks: Vec<Vector2<i32>>,
    pub expires_millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterChunkAvailability {
    Full(Vector2<i32>),
    Drop(Vector2<i32>),
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

pub fn set_cluster_chunk_availability_sender(sender: mpsc::Sender<ClusterChunkAvailability>) {
    let _ = CLUSTER_CHUNK_AVAILABILITY_SENDER.set(sender);
}

pub fn note_cluster_chunk_full(pos: Vector2<i32>) {
    if let Some(sender) = CLUSTER_CHUNK_AVAILABILITY_SENDER.get()
        && sender.try_send(ClusterChunkAvailability::Full(pos)).is_err()
    {
        warn!(chunk_x = pos.x, chunk_z = pos.y, "cluster full chunk availability event dropped");
    }
}

pub fn note_cluster_chunk_drop(pos: Vector2<i32>) {
    if let Some(sender) = CLUSTER_CHUNK_AVAILABILITY_SENDER.get()
        && sender.try_send(ClusterChunkAvailability::Drop(pos)).is_err()
    {
        warn!(chunk_x = pos.x, chunk_z = pos.y, "cluster dropped chunk availability event dropped");
    }
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
            if pair.local.set_block_absolute_y(edit.x, edit.y, edit.z, edit.state) != edit.state {
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
                edits.iter().copied().filter(ClusterDual::edit_in_bounds).collect(),
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
            still_pending.iter().copied().filter(ClusterDual::edit_in_bounds).collect(),
        );
        let mut local_changed = cluster_copy_blocks(&pair.ground, &pair.local);
        for edit in still_pending {
            if !ClusterDual::edit_in_bounds(edit) {
                continue;
            }
            if pair.local.set_block_absolute_y(edit.x, edit.y, edit.z, edit.state) != edit.state {
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
                if pair.local.set_block_absolute_y(x, relative.y, z, block_state_id)
                    != block_state_id
                {
                    pair.local.mark_dirty(true);
                }
                let edit = ClusterBlockEdit { x, y: relative.y, z, state: block_state_id };
                if let Some(mut pendings) = self.cluster_dual.pending.get_mut(&chunk_coordinate) {
                    pendings.push(edit);
                } else {
                    self.cluster_dual.pending.insert(chunk_coordinate, vec![edit]);
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

    pub async fn persist_cluster_entity_nbt(
        self: &Arc<Self>,
        pos: Vector2<i32>,
        nbt: NbtCompound,
    ) -> bool {
        if is_cluster_secondary() {
            return false;
        }
        let chunk = self.get_entity_chunk(pos).await;
        chunk.data.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.push(nbt.clone());
            Arc::new(next)
        });
        chunk.mark_dirty(true);
        self.write_entity_chunks(vec![(pos, chunk.clone())]).await;
        self.loaded_entity_chunks.remove(&pos);
        true
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
        {
            let mut biomes = chunk.section.biome_sections.write().unwrap();
            let desert = pumpkin_data::biome::Biome::from_name("desert").unwrap().id;
            biomes[4].set(0, 0, 0, desert);
        }
        {
            let mut light = chunk.light_engine.lock().unwrap();
            let count = chunk.section.count;
            light.block_light =
                vec![crate::chunk::format::LightContainer::Empty(0); count].into();
            light.sky_light =
                vec![crate::chunk::format::LightContainer::Empty(0); count].into();
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
            entity.put_int("x", 17);
            entity.put_int("y", 1);
            entity.put_int("z", -110);
            entity.put_string("id", "minecraft:chest".to_string());
            entities.insert(BlockPos::new(17, 1, -110), entity);
        }
        {
            let tick = ScheduledTick {
                delay: 4,
                priority: TickPriority::Normal,
                position: BlockPos::new(17, 0, -110),
                value: &Block::STONE,
            };
            chunk.block_ticks =
                crate::tick::scheduler::ChunkTickScheduler::from_iter([tick]);
        }
        chunk
            .inhabited_time
            .store(12345, Ordering::Relaxed);
        {
            let mut custom = chunk.custom_data.lock().unwrap();
            custom.put_int("cluster_probe", 7);
        }

        let chunk = Arc::new(chunk);
        let bytes = cluster_encode_snapshot(&chunk);
        assert_eq!(bytes[0], 1);
        let back = cluster_decode_snapshot(3, -7, &bytes).expect("snapshot decodes");

        assert_eq!(back.x, 3);
        assert_eq!(back.z, -7);
        assert_eq!(back.section.min_y, chunk.section.min_y);
        assert_eq!(back.section.count, chunk.section.count);
        assert_eq!(
            back.section.get_block_absolute_y(1, 0, 2),
            Some(stone)
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
            let entity = entities.get(&BlockPos::new(17, 1, -110)).expect("entity kept");
            assert_eq!(entity.get_string("id"), Some("minecraft:chest"));
        }
        {
            let ticks = back.block_ticks.to_vec();
            assert_eq!(ticks.len(), 1);
            assert_eq!(ticks[0].delay, 4);
            assert_eq!(ticks[0].position, BlockPos::new(17, 0, -110));
        }
        assert_eq!(back.inhabited_time.load(Ordering::Relaxed), 12345);
        assert_eq!(back.status, pumpkin_data::chunk::ChunkStatus::Full);
        {
            let custom = back.custom_data.lock().unwrap();
            assert_eq!(custom.get_int("cluster_probe"), Some(7));
        }
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
}
