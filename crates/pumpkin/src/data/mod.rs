use std::{env, fs, path::Path, sync::RwLock};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use pumpkin_world::level::is_cluster_secondary;

const DATA_FOLDER: &str = "data/";

fn load_secondary_operator_config(path: &Path) -> op::OperatorConfig {
    let Ok(contents) = fs::read_to_string(path) else {
        return op::OperatorConfig::default();
    };
    match serde_json::from_str(&contents) {
        Ok(operators) => operators,
        Err(error) => {
            error!(%error, path = %path.display(), "secondary operator bootstrap invalid");
            op::OperatorConfig::default()
        }
    }
}

pub mod op;

pub mod advancement_data;
pub mod banlist_serializer;
pub mod banned_ip;
pub mod banned_player;
pub mod datapack;
pub mod player_server;
pub mod usercache;
pub mod whitelist;

pub struct VanillaData {
    pub banned_ip_list: RwLock<banned_ip::BannedIpList>,
    pub banned_player_list: RwLock<banned_player::BannedPlayerList>,
    pub operator_config: RwLock<op::OperatorConfig>,
    pub user_cache: RwLock<usercache::UserCache>,
    pub whitelist_config: RwLock<whitelist::WhitelistConfig>,
}

impl VanillaData {
    #[must_use]
    pub fn load() -> Self {
        if is_cluster_secondary() {
            let mut data = Self::empty();
            let ops_path = env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join(DATA_FOLDER)
                .join(op::OperatorConfig::get_path());
            *data
                .operator_config
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                load_secondary_operator_config(&ops_path);
            return data;
        }
        Self {
            banned_ip_list: RwLock::new(banned_ip::BannedIpList::load()),
            banned_player_list: RwLock::new(banned_player::BannedPlayerList::load()),
            operator_config: RwLock::new(op::OperatorConfig::load()),
            user_cache: RwLock::new(usercache::UserCache::load()),
            whitelist_config: RwLock::new(whitelist::WhitelistConfig::load()),
        }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self {
            banned_ip_list: RwLock::new(banned_ip::BannedIpList::default()),
            banned_player_list: RwLock::new(banned_player::BannedPlayerList::default()),
            operator_config: RwLock::new(op::OperatorConfig::default()),
            user_cache: RwLock::new(usercache::UserCache::default()),
            whitelist_config: RwLock::new(whitelist::WhitelistConfig::default()),
        }
    }
}

pub trait LoadJSONConfiguration {
    #[must_use]
    fn load() -> Self
    where
        Self: Sized + Default + Serialize + for<'de> Deserialize<'de>,
    {
        let exe_dir = env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let data_dir = exe_dir.join(DATA_FOLDER);
        if !data_dir.exists() {
            debug!("creating new data root folder");
            let _ = fs::create_dir(&data_dir);
        }
        let path = data_dir.join(Self::get_path());

        let config = if path.exists() {
            let file_content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(err) => {
                    error!(
                        "Couldn't read configuration file at {}: {err}",
                        path.display()
                    );
                    return Self::default();
                }
            };

            match serde_json::from_str(&file_content) {
                Ok(c) => c,
                Err(err) => {
                    error!(
                        "Couldn't parse data config at {}. Reason: {err}. Falling back to default.",
                        path.display()
                    );
                    Self::default()
                }
            }
        } else {
            let content = Self::default();

            if !pumpkin_world::level::is_cluster_secondary()
                && let Ok(json_str) = serde_json::to_string_pretty(&content)
            {
                let _ = fs::write(&path, json_str);
            }

            content
        };

        config.validate();
        config
    }

    fn get_path() -> &'static Path;

    fn validate(&self);
}

pub trait SaveJSONConfiguration: LoadJSONConfiguration {
    fn save(&self)
    where
        Self: Sized + Default + Serialize + for<'de> Deserialize<'de>,
    {
        let exe_dir = env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        let data_dir = exe_dir.join(DATA_FOLDER);
        if !data_dir.exists() {
            debug!("creating new data root folder");
            let _ = fs::create_dir(&data_dir);
        }
        let path = data_dir.join(Self::get_path());

        if pumpkin_world::level::is_cluster_secondary() {
            return;
        }
        let content = match serde_json::to_string_pretty(self) {
            Ok(content) => content,
            Err(err) => {
                warn!(
                    "Couldn't serialize operator data config to {}. Reason: {err}",
                    path.display()
                );
                return;
            }
        };

        if let Err(err) = std::fs::write(&path, content) {
            warn!(
                "Couldn't write operator config to {}. Reason: {err}",
                path.display()
            );
        }
    }
}

#[cfg(test)]
mod cluster_operator_bootstrap_tests {
    use super::*;

    #[test]
    fn secondary_operator_bootstrap_reads_existing_file_without_creating_missing_data() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing").join("ops.json");
        assert!(load_secondary_operator_config(&missing).ops.is_empty());
        assert!(!missing.parent().unwrap().exists());

        let existing = directory.path().join("ops.json");
        let operator = pumpkin_config::op::Op::new(
            uuid::Uuid::new_v4(),
            String::from("Operator"),
            pumpkin_util::PermissionLvl::Four,
            false,
        );
        let encoded = serde_json::to_vec(&op::OperatorConfig {
            ops: vec![operator],
        })
        .unwrap();
        fs::write(&existing, encoded).unwrap();
        let loaded = load_secondary_operator_config(&existing);
        assert_eq!(loaded.ops.len(), 1);
        assert_eq!(loaded.ops[0].name, "Operator");
    }
}
