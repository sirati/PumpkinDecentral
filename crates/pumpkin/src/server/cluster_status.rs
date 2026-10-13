use std::sync::Arc;
use std::sync::OnceLock;

use pumpkin_data::packet::{CURRENT_MC_VERSION, LOWEST_SUPPORTED_MC_VERSION};
use pumpkin_protocol::{Players, Sample, StatusResponse, Version};
use pumpkin_util::text::TextComponent;

use super::Server;

static FAVICON: OnceLock<String> = OnceLock::new();
const SAMPLE_LIMIT: usize = 12;

#[must_use]
pub fn cluster_status_online(server: &Server) -> u32 {
    live_status_players(server).0
}

pub fn install_favicon(favicon: Option<String>) {
    if let Some(favicon) = favicon {
        let _ = FAVICON.set(favicon);
    }
}

#[must_use]
fn live_status_players(server: &Server) -> (u32, Vec<Sample>) {
    let snapshot = server.cluster_presence.snapshot();
    let mut sample = Vec::new();
    let mut visible = std::collections::HashSet::new();
    for player in server.get_all_players().iter() {
        if snapshot.hidden_uuids.contains(player.gameprofile.id.as_bytes())
            || player.cluster_gid().is_some_and(|gid| snapshot.hidden_gids.contains(&gid)) { continue; }
        let id = player.gameprofile.id.to_string();
        visible.insert(id.clone());
        if player.config.load().server_listing {
            sample.push(Sample { name: player.gameprofile.name.clone(), id });
        }
    }
    for entry in snapshot.entries.values() {
        if snapshot.is_hidden(&entry.gid, &entry.uuid) { continue; }
        let id = uuid::Uuid::from_bytes(entry.uuid).to_string();
        if visible.insert(id.clone()) && entry.listed {
            sample.push(Sample { name: entry.name.clone(), id });
        }
    }
    sample.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.id.cmp(&right.id))
    });
    let online = u32::try_from(visible.len()).unwrap_or(u32::MAX);
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
            name: format!("{LOWEST_SUPPORTED_MC_VERSION}-{CURRENT_MC_VERSION}"),
            protocol: LOWEST_SUPPORTED_MC_VERSION.protocol_version() as u32,
        }),
        players: Some(Players {
            max: server.advanced_config.networking.java.max_players,
            online: 0,
            sample: Vec::new(),
        }),
        description: TextComponent::text(server.advanced_config.networking.java.motd.clone()),
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
    let _ = live_status_players(server);
}

pub fn spawn_cluster_status_poller(server: &Arc<Server>) {
    refresh_cluster_status(server);
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
    fn protocol_override_applies_in_range() {
        let supported = CURRENT_MC_VERSION.protocol_version();
        let updated = apply_protocol(empty_response(), supported);
        assert_eq!(updated.version.as_ref().unwrap().protocol, supported as u32);
    }

    #[test]
    fn protocol_override_keeps_base_out_of_range() {
        let updated = apply_protocol(empty_response(), -1);
        assert_eq!(updated.version.as_ref().unwrap().protocol, 0);
        let updated = apply_protocol(empty_response(), i32::MAX);
        assert_eq!(updated.version.as_ref().unwrap().protocol, 0);
    }
}
