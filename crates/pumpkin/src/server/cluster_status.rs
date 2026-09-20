use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pumpkin_data::packet::{CURRENT_MC_VERSION, LOWEST_SUPPORTED_MC_VERSION};
use pumpkin_protocol::{Players, Sample, StatusResponse, Version};
use pumpkin_util::text::TextComponent;

use super::Server;

static ONLINE: AtomicU32 = AtomicU32::new(0);
static FAVICON: OnceLock<String> = OnceLock::new();
static VERSION_NAME: LazyLock<String> =
    LazyLock::new(|| format!("{LOWEST_SUPPORTED_MC_VERSION}-{CURRENT_MC_VERSION}"));
const SAMPLE_LIMIT: usize = 12;

#[must_use]
pub fn cluster_status_online() -> u32 {
    ONLINE.load(Ordering::Relaxed)
}

pub fn install_favicon(favicon: Option<String>) {
    if let Some(favicon) = favicon {
        let _ = FAVICON.set(favicon);
    }
}

#[must_use]
fn live_status_players(server: &Server) -> (u32, Vec<Sample>) {
    let mut sample = Vec::new();
    for player in server.get_all_players().iter() {
        if !player.config.load().server_listing {
            continue;
        }
        if super::cluster_hide::is_hidden_player(player) {
            continue;
        }
        sample.push(Sample {
            name: player.gameprofile.name.clone(),
            id: player.gameprofile.id.to_string(),
        });
    }
    for (gid, entry) in super::cluster_presence::remote_presence_entries()
        .iter()
    {
        if super::cluster_hide::is_hidden_remote_gid(gid) {
            continue;
        }
        if super::cluster_hide::is_hidden_uuid_bytes(&entry.uuid) {
            continue;
        }
        sample.push(Sample {
            name: entry.name.clone(),
            id: uuid::Uuid::from_bytes(entry.uuid).to_string(),
        });
    }
    sample.sort_by(|left, right| {
        left.name.cmp(&right.name).then_with(|| left.id.cmp(&right.id))
    });
    let online = u32::try_from(sample.len()).unwrap_or(u32::MAX);
    sample.truncate(SAMPLE_LIMIT);
    (online, sample)
}

#[must_use]
pub fn local_status_sample(server: &Server) -> Vec<Sample> {
    live_status_players(server).1
}

fn apply_protocol(mut response: StatusResponse, client_protocol: i32) -> StatusResponse {
    let supported_min = LOWEST_SUPPORTED_MC_VERSION.protocol_version();
    let supported_max = CURRENT_MC_VERSION.protocol_version();
    if client_protocol >= supported_min
        && client_protocol <= supported_max
        && let Some(version) = response.version.as_mut()
    {
        version.protocol = client_protocol as u32;
    }
    response
}

pub fn base_status_response(server: &Server, client_protocol: i32) -> StatusResponse {
    let base = StatusResponse {
        version: Some(Version {
            name: VERSION_NAME.clone(),
            protocol: LOWEST_SUPPORTED_MC_VERSION.protocol_version() as u32,
        }),
        players: Some(Players {
            max: server.advanced_config.networking.java.max_players,
            online: 0,
            sample: Vec::new(),
        }),
        description: TextComponent::text(
            server.advanced_config.networking.java.motd.clone(),
        ),
        favicon: FAVICON.get().cloned(),
        enforce_secure_chat: true,
    };
    let mut response = apply_protocol(base, client_protocol);
    if let Some(players) = response.players.as_mut() {
        let (online, sample) = live_status_players(server);
        players.online = online;
        players.sample = sample;
    }
    response
}

pub fn refresh_cluster_status(server: &Server) {
    ONLINE.store(live_status_players(server).0, Ordering::Relaxed);
}

pub fn spawn_cluster_status_poller(server: &Arc<Server>) {
    refresh_cluster_status(server);
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            refresh_cluster_status(&task_server);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_response() -> StatusResponse {
        StatusResponse {
            version: Some(Version {
                name: String::from("test"),
                protocol: 0,
            }),
            players: None,
            description: TextComponent::text(String::from("motd")),
            favicon: None,
            enforce_secure_chat: true,
        }
    }

    #[test]
    fn snapshot_load_store_roundtrips() {
        ONLINE.store(7, Ordering::Relaxed);
        assert_eq!(cluster_status_online(), 7);
        ONLINE.store(0, Ordering::Relaxed);
        assert_eq!(cluster_status_online(), 0);
    }

    #[test]
    fn protocol_override_applies_in_range() {
        let supported = CURRENT_MC_VERSION.protocol_version();
        let updated = apply_protocol(empty_response(), supported);
        assert_eq!(
            updated.version.as_ref().unwrap().protocol,
            supported as u32
        );
    }

    #[test]
    fn protocol_override_keeps_base_out_of_range() {
        let updated = apply_protocol(empty_response(), -1);
        assert_eq!(updated.version.as_ref().unwrap().protocol, 0);
        let updated = apply_protocol(empty_response(), i32::MAX);
        assert_eq!(updated.version.as_ref().unwrap().protocol, 0);
    }
}
