use std::sync::{Arc, atomic::Ordering};

use pumpkin_cluster::health_policy::{HealthMaskMode, HealthPolicySnapshot};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_util::text::TextComponent;

use crate::command::CommandSource;
use crate::entity::player::Player;

use super::Server;

struct Arm {
    player: Arc<Player>,
    gid: GlobalPlayerId,
    session: u64,
    mode: HealthMaskMode,
    prior_version: u64,
}

impl Arm {
    fn current(&self, server: &Server) -> bool {
        self.player.cluster_gid() == Some(self.gid)
            && self
                .player
                .cluster_owner_install_generation
                .load(Ordering::Relaxed)
                == self.session
            && self.session != 0
            && server
                .cluster_presence
                .snapshot()
                .entries
                .get(&self.gid)
                .is_some_and(|entry| {
                    entry.uuid == *self.player.gameprofile.id.as_bytes()
                        && entry.session == self.session
                        && !entry.in_lobby
                        && entry.source.is_some_and(|source| {
                            source.server == self.gid.server
                                && server.cluster_peer_incarnations.load().get(&source.server)
                                    == Some(&source.generation)
                        })
                })
    }
}

pub(crate) struct HealthPolicyRuntime {
    arms: crossbeam::queue::ArrayQueue<Arm>,
}

impl HealthPolicyRuntime {
    pub(crate) fn new() -> Self {
        Self {
            arms: crossbeam::queue::ArrayQueue::new(64),
        }
    }
}

pub(crate) fn parse(id: &str) -> Option<HealthMaskMode> {
    match id {
        "health-policy-off-arm" => Some(HealthMaskMode::Off),
        "health-policy-players-arm" => Some(HealthMaskMode::Players),
        "health-policy-all-arm" => Some(HealthMaskMode::All),
        _ => None,
    }
}

fn name(mode: HealthMaskMode) -> &'static str {
    match mode {
        HealthMaskMode::Off => "off",
        HealthMaskMode::Players => "players",
        HealthMaskMode::All => "all",
    }
}

pub(crate) fn submit(server: &Server, source: &CommandSource, mode: HealthMaskMode) -> bool {
    let Some(player) = source.output.as_player() else {
        return false;
    };
    let Some(gid) = player.cluster_gid() else {
        return false;
    };
    if super::cluster_test::registered_players(server).is_none_or(|players| !players.contains(&gid))
    {
        return false;
    }
    let session = player
        .cluster_owner_install_generation
        .load(Ordering::Relaxed);
    if session == 0 {
        return false;
    }
    server
        .cluster_test
        .health_policy
        .arms
        .push(Arm {
            player,
            gid,
            session,
            mode,
            prior_version: 0,
        })
        .is_ok()
}

#[derive(Default)]
pub(crate) struct HealthPolicyObserver {
    watches: Vec<Arm>,
}

impl HealthPolicyObserver {
    pub(crate) fn drain(&mut self, server: &Server, snapshot: HealthPolicySnapshot) {
        self.watches.retain(|arm| arm.current(server));
        loop {
            let Some(mut arm) = server.cluster_test.health_policy.arms.pop() else {
                break;
            };
            if !arm.current(server) {
                continue;
            }
            self.watches
                .retain(|old| old.gid != arm.gid || old.session != arm.session);
            if self.watches.len() == 64 {
                arm.player.send_system_message(&TextComponent::text(
                    "cluster test health policy observer capacity unavailable",
                ));
                continue;
            }
            arm.prior_version = snapshot.version;
            arm.player.send_system_message(&TextComponent::text(format!(
                "cluster test health-policy-{} armed prior_version={}",
                name(arm.mode),
                arm.prior_version
            )));
            self.watches.push(arm);
        }
    }

    pub(crate) fn applied(
        &mut self,
        server: &Server,
        snapshot: HealthPolicySnapshot,
        source: PeerGeneration,
        primary: bool,
    ) {
        let stamp = format!("{}:{}", source.server.0, source.generation);
        if primary {
            tracing::info!(mode = name(snapshot.mode), version = snapshot.version, source = %stamp, "cluster e2e health policy applied");
        }
        self.watches.retain(|arm| {
            if !arm.current(server) { return false; }
            if arm.mode != snapshot.mode || snapshot.version <= arm.prior_version { return true; }
            arm.player.send_system_message(&TextComponent::text(format!("cluster test health-policy-{} ready version={} source={}", name(arm.mode), snapshot.version, stamp)));
            tracing::info!(participant = %arm.player.gameprofile.id, session = arm.session, mode = name(arm.mode), version = snapshot.version, source = %stamp, prior_version = arm.prior_version, "cluster e2e health policy ready");
            false
        });
    }
}
