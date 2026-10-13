use std::{path::PathBuf, sync::Arc};

use pumpkin_config::world::LevelConfig;
use pumpkin_data::dimension::Dimension;

use crate::level::Level;

#[must_use]
pub fn into_level(
    dimension: Dimension,
    level_config: &LevelConfig,
    base_directory: PathBuf,
    seed: i64,
) -> Arc<Level> {
    Level::from_root_folder(level_config, base_directory, seed, dimension)
}

#[must_use]
pub fn into_level_with_cluster_genesis(
    dimension: Dimension,
    level_config: &LevelConfig,
    base_directory: PathBuf,
    seed: i64,
    cluster_genesis_required: bool,
) -> Arc<Level> {
    Level::from_root_folder_with_cluster_genesis(
        level_config,
        base_directory,
        seed,
        dimension,
        cluster_genesis_required,
    )
}
