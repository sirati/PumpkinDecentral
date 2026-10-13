use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use pumpkin_cluster::canonical_action::LivePlayerLease;
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::player_command_control::{PlayerCommandMutation, TeleportDestination};
use pumpkin_cluster::protocol::WorldId;
use pumpkin_protocol::ServerPacket;
use pumpkin_protocol::java::server::play::SSwingArm;
use pumpkin_protocol::packet::MultiVersionJavaPacket;
use pumpkin_util::text::TextComponent;

use crate::command::{CommandSender, CommandSource};
use crate::entity::player::Player;
use crate::net::ClientPlatform;
use crate::plugin::server::packet::PacketReceivedEvent;
use crate::plugin::{BoxFuture, EventHandler, EventPriority};

use super::Server;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreflightScenario {
    Lobby,
    Disconnect,
}

impl PreflightScenario {
    pub(crate) fn parse(id: &str) -> Option<Self> {
        match id {
            "packet-preflight-lobby" => Some(Self::Lobby),
            "packet-preflight-disconnect" => Some(Self::Disconnect),
            _ => None,
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Lobby => "packet-preflight-lobby",
            Self::Disconnect => "packet-preflight-disconnect",
        }
    }
}

#[derive(Clone)]
struct ArmRequest {
    scenario: PreflightScenario,
    lease: LivePlayerLease,
    player: Arc<Player>,
    submitted: Instant,
}

#[derive(Clone, Copy)]
pub(crate) struct DepartureContext {
    pub scenario: PreflightScenario,
    pub target: GlobalPlayerId,
    pub uuid: uuid::Uuid,
    pub world: WorldId,
    pub position: [f64; 3],
    pub prior_revision: u64,
}

#[derive(Clone)]
struct ArmedCallback {
    request: ArmRequest,
    departure: DepartureContext,
}

struct CallbackFinished {
    lease: LivePlayerLease,
    scenario: PreflightScenario,
    callback_ms: u64,
    departing: bool,
    correlation: Option<[u8; 16]>,
    revision_at_finish: u64,
}

enum CallbackEvent {
    Started(LivePlayerLease, PreflightScenario),
    Finished(CallbackFinished),
}

pub(crate) struct PreflightRuntime {
    arms: crossbeam::queue::ArrayQueue<ArmRequest>,
    callbacks: crossbeam::queue::ArrayQueue<ArmedCallback>,
    events: crossbeam::queue::ArrayQueue<CallbackEvent>,
}

impl PreflightRuntime {
    pub(crate) fn new() -> Self {
        Self {
            arms: crossbeam::queue::ArrayQueue::new(8),
            callbacks: crossbeam::queue::ArrayQueue::new(1),
            events: crossbeam::queue::ArrayQueue::new(16),
        }
    }
}

pub(crate) fn submit_arm(
    server: &Server,
    source: &CommandSource,
    scenario: PreflightScenario,
) -> bool {
    let Some(player) = source.output.as_player() else {
        return false;
    };
    let Some(lease) = super::cluster_world_owner::player_action_lease(server, &player) else {
        return false;
    };
    if super::cluster_test::registered_players(server).is_none_or(|players| players[0] != lease.gid)
    {
        return false;
    }
    server
        .cluster_test
        .preflight
        .arms
        .push(ArmRequest {
            scenario,
            lease,
            player,
            submitted: Instant::now(),
        })
        .is_ok()
}

pub(crate) fn authorize_operator(server: &Server, source: &CommandSource) -> bool {
    let Some(player) = source.output.as_player() else {
        return false;
    };
    let Some(lease) = super::cluster_world_owner::player_action_lease(server, &player) else {
        return false;
    };
    super::cluster_test::registered_players(server).is_some_and(|players| players[1] == lease.gid)
}

struct PreflightHandler;

impl EventHandler<PacketReceivedEvent> for PreflightHandler {
    fn handle_blocking<'a>(
        &'a self,
        server: &'a Arc<Server>,
        event: &'a mut PacketReceivedEvent,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let ClientPlatform::Java(client) = event.player.client.as_ref() else {
                return;
            };
            let version = client.version.load();
            if event.packet_id != SSwingArm::to_id(version) {
                return;
            }
            let mut payload = event.payload.as_ref();
            let Ok(swing) = SSwingArm::read(&mut payload, &version) else {
                return;
            };
            if swing.hand.0 != 1 || !payload.is_empty() {
                return;
            }
            if super::cluster_test::registered_players(server).is_none_or(|players| event.player.cluster_gid() != Some(players[0])) { return; }
            let runtime = &server.cluster_test.preflight;
            let Some(armed) = runtime.callbacks.pop() else {
                return;
            };
            if armed.request.submitted.elapsed() >= Duration::from_secs(5) {
                tracing::error!("cluster test packet preflight arm expired");
                return;
            }
            if event.player.gameprofile.id != armed.request.lease.key.0
                || super::cluster_world_owner::player_action_lease(server, &event.player)
                    != Some(armed.request.lease)
            {
                if runtime.callbacks.push(armed).is_err() {
                    tracing::error!("cluster test packet preflight arm lost");
                }
                return;
            }
            let started = Instant::now();
            if runtime
                .events
                .push(CallbackEvent::Started(
                    armed.request.lease,
                    armed.request.scenario,
                ))
                .is_err()
            {
                tracing::error!("cluster test packet preflight start queue full");
                return;
            }
            event
                .player
                .send_system_message(&TextComponent::text(format!(
                    "cluster test {} callback started",
                    armed.request.scenario.id(),
                )));
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let departing = event.player.cluster_departing.load(Ordering::Acquire);
            let source = CommandSource::new(
                CommandSender::Console,
                event.player.world(),
                None,
                pumpkin_util::math::vector3::Vector3::default(),
                pumpkin_util::math::vector2::Vector2::default(),
                "cluster-e2e-packet-preflight".to_owned(),
                TextComponent::text("cluster-e2e-packet-preflight"),
                Arc::clone(server),
            );
            let correlation = super::cluster_player_commands::submit_test_departure(
                server,
                &source,
                armed.request.lease.gid,
                *armed.request.lease.key.0.as_bytes(),
                PlayerCommandMutation::Teleport(TeleportDestination::Absolute {
                    world: armed.departure.world,
                    position: armed.departure.position,
                    rotation: Some([0.0, 0.0]),
                }),
            )
            .ok();
            let revision_at_finish = server
                .cluster_handoff
                .load()
                .hints
                .get(&armed.departure.uuid)
                .map_or(0, |hint| hint.playerdata_revision);
            if runtime
                .events
                .push(CallbackEvent::Finished(CallbackFinished {
                    lease: armed.request.lease,
                    scenario: armed.request.scenario,
                    callback_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                    departing,
                    correlation,
                    revision_at_finish,
                }))
                .is_err()
            {
                tracing::error!("cluster test packet preflight finish queue full");
            }
        })
    }
}

fn register(server: &Server) {
    server.plugin_manager.register::<PacketReceivedEvent, _>(
        Arc::new(PreflightHandler),
        EventPriority::Highest,
        true,
    );
}

struct ActivePreflight {
    armed: ArmedCallback,
    started: bool,
    finished: Option<CallbackFinished>,
    external_trigger: bool,
}

#[derive(Default)]
pub(crate) struct PreflightOwner {
    registered: bool,
    retained_arm: Option<ArmRequest>,
    active: Option<ActivePreflight>,
}

impl PreflightOwner {
    pub(crate) fn advance(&mut self, server: &Server, departure: Option<DepartureContext>) {
        let runtime = &server.cluster_test.preflight;
        while let Some(event) = runtime.events.pop() {
            let Some(active) = self.active.as_mut() else {
                continue;
            };
            match event {
                CallbackEvent::Started(lease, scenario)
                    if lease == active.armed.request.lease
                        && scenario == active.armed.request.scenario =>
                {
                    active.started = true
                }
                CallbackEvent::Finished(finished)
                    if finished.lease == active.armed.request.lease
                        && finished.scenario == active.armed.request.scenario =>
                {
                    active.finished = Some(finished)
                }
                _ => {}
            }
        }
        if let Some(active) = &self.active {
            if active.armed.request.submitted.elapsed() >= Duration::from_secs(15) {
                tracing::error!(
                    scenario = active.armed.request.scenario.id(),
                    "cluster test packet preflight settlement expired"
                );
                let _ = runtime.callbacks.pop();
                self.active = None;
            } else if let Some(finished) = &active.finished {
                let terminal = finished
                    .correlation
                    .and_then(|correlation| server.cluster_player_commands.terminal(correlation));
                let hint = server.cluster_handoff.load();
                if let Some(terminal) = terminal
                    && terminal.applied
                    && let Some(action) = terminal.action
                    && action.id.actor
                        == pumpkin_cluster::actor::ActorId::Player(active.armed.request.lease.gid)
                    && action.id.incarnation == active.armed.request.lease.incarnation
                    && let Some(hint) = hint.hints.get(&active.armed.departure.uuid)
                    && hint.playerdata_revision > active.armed.departure.prior_revision
                    && hint.position == active.armed.departure.position
                {
                    tracing::info!(
                        scenario = active.armed.request.scenario.id(),
                        callback_started = active.started,
                        departure_during_callback = finished.departing,
                        callback_before_commit = finished.revision_at_finish <= active.armed.departure.prior_revision,
                        action_accepted = terminal.applied,
                        action = ?action,
                        action_tick = action.tick.0,
                        action_sequence = action.id.sequence,
                        action_family = action.id.family,
                        action_incarnation = action.id.incarnation,
                        action_server = active.armed.request.lease.gid.server.0,
                        action_player = active.armed.request.lease.gid.player.0,
                        uuid = %active.armed.departure.uuid,
                        revision = hint.playerdata_revision,
                        callback_ms = finished.callback_ms,
                        departure_trigger = if active.external_trigger { "other_operator" } else { "disconnect" },
                        "cluster e2e packet preflight departure committed",
                    );
                    self.active = None;
                }
            }
        }
        if self.active.is_some() {
            return;
        }
        let Some(request) = self.retained_arm.take().or_else(|| runtime.arms.pop()) else {
            return;
        };
        if request.submitted.elapsed() >= Duration::from_secs(5) {
            request
                .player
                .send_system_message(&TextComponent::text(format!(
                    "cluster test {} failed",
                    request.scenario.id()
                )));
            return;
        }
        let Some(departure) = departure.filter(|departure| {
            departure.target == request.lease.gid
                && departure.uuid == request.lease.key.0
                && departure.scenario == request.scenario
        }) else {
            self.retained_arm = Some(request);
            return;
        };
        if !self.registered {
            register(server);
            self.registered = true;
        }
        let armed = ArmedCallback { request, departure };
        if runtime.callbacks.push(armed.clone()).is_err() {
            self.retained_arm = Some(armed.request);
            return;
        }
        armed
            .request
            .player
            .send_system_message(&TextComponent::text(format!(
                "cluster test {} armed",
                armed.request.scenario.id()
            )));
        self.active = Some(ActivePreflight {
            armed,
            started: false,
            finished: None,
            external_trigger: false,
        });
    }

    pub(crate) fn request_external_departure(&mut self, server: &Server, origin: ServerId) -> bool {
        self.advance(server, None);
        let Some(active) = self.active.as_mut() else {
            return false;
        };
        if active.armed.request.scenario != PreflightScenario::Lobby
            || !active.started
            || active.finished.is_some()
            || super::cluster_test::registered_players(server)
                .is_none_or(|players| players[1].server != origin || players[0].server == origin)
        {
            return false;
        }
        if !super::cluster_lobby::lobby_enter_manual(&active.armed.request.player, false) {
            return false;
        }
        active.external_trigger = true;
        true
    }
}
