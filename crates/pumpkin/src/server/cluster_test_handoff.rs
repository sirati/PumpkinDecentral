use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use pumpkin_util::text::TextComponent;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::entity::player::Player;
use crate::net::{ClientPlatform, DisconnectReason};
use crate::plugin::api::events::player::LobbyCustomPayloadEvent;
use crate::plugin::{BoxFuture, EventHandler, EventPriority};

use super::Server;

const CHANNEL: &str = "pumpkin:cluster_e2e_handoff";

struct SessionIdentity {
    uuid: Uuid,
    token: u64,
    client: Arc<ClientPlatform>,
    created: Instant,
}

struct Callback {
    identity: Arc<SessionIdentity>,
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct Confirmation {
    identity: Arc<SessionIdentity>,
    release: oneshot::Sender<()>,
}

pub(crate) struct HandoffRuntime {
    callbacks: crossbeam::queue::ArrayQueue<Callback>,
    confirmations: crossbeam::queue::ArrayQueue<Confirmation>,
    releases: crossbeam::queue::ArrayQueue<Confirmation>,
}

impl HandoffRuntime {
    pub(crate) fn new() -> Self {
        Self {
            callbacks: crossbeam::queue::ArrayQueue::new(1),
            confirmations: crossbeam::queue::ArrayQueue::new(1),
            releases: crossbeam::queue::ArrayQueue::new(1),
        }
    }
}

pub(crate) struct EarlyConfirmGate {
    started: oneshot::Receiver<()>,
    observed: bool,
    created: Instant,
}

impl EarlyConfirmGate {
    pub(crate) fn started(&mut self) -> Result<bool, ()> {
        if self.observed {
            return Ok(true);
        }
        if self.created.elapsed() >= Duration::from_secs(4) {
            return Err(());
        }
        match self.started.try_recv() {
            Ok(()) => {
                self.observed = true;
                Ok(true)
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(false),
            Err(oneshot::error::TryRecvError::Closed) => Err(()),
        }
    }
}

struct HandoffHandler;

impl EventHandler<LobbyCustomPayloadEvent> for HandoffHandler {
    fn handle<'a>(
        &'a self,
        server: &'a Arc<Server>,
        event: &'a LobbyCustomPayloadEvent,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if event.channel != CHANNEL {
                return;
            }
            let Some(callback) = server.cluster_test.handoff.callbacks.pop() else {
                return;
            };
            let exact = event.source.profile.id == callback.identity.uuid
                && event.source.session_generation == callback.identity.token
                && Arc::ptr_eq(&event.source.client, &callback.identity.client)
                && event.data.as_ref() == callback.identity.token.to_be_bytes()
                && callback.identity.created.elapsed() < Duration::from_secs(4);
            if !exact {
                if server
                    .cluster_test
                    .handoff
                    .callbacks
                    .push(callback)
                    .is_err()
                {
                    tracing::error!("cluster test handoff callback queue overflow");
                }
                return;
            }
            let token = callback.identity.token;
            event.source.send_message(&TextComponent::text(format!(
                "cluster test handoff callback started {token}"
            )));
            if callback.started.send(()).is_err() {
                return;
            }
            match tokio::time::timeout(Duration::from_secs(4), callback.release).await {
                Ok(Ok(())) => event.source.send_message(&TextComponent::text(format!(
                    "cluster test handoff callback released {token}"
                ))),
                _ => tracing::error!(token, "cluster test handoff callback release failed"),
            }
        })
    }
}

pub(crate) fn begin(server: &Arc<Server>, player: &Arc<Player>) -> Option<EarlyConfirmGate> {
    let selected = std::env::var("PUMPKIN_CLUSTER_TEST_EARLY_CONFIRM_NAME").ok()?;
    if player.gameprofile.name != selected {
        return None;
    }
    let identity = Arc::new(SessionIdentity {
        uuid: player.gameprofile.id,
        token: player
            .cluster_owner_install_generation
            .load(Ordering::Acquire),
        client: Arc::clone(&player.client),
        created: Instant::now(),
    });
    let (started_tx, started) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    if server
        .cluster_test
        .handoff
        .callbacks
        .push(Callback {
            identity: Arc::clone(&identity),
            started: started_tx,
            release: release_rx,
        })
        .is_err()
        || server
            .cluster_test
            .handoff
            .confirmations
            .push(Confirmation {
                identity: Arc::clone(&identity),
                release,
            })
            .is_err()
    {
        tracing::error!("cluster test handoff gate resource failure");
        player.client.try_kick(
            DisconnectReason::Kicked,
            &TextComponent::text("cluster test handoff gate resource failure"),
        );
        return None;
    }
    server
        .plugin_manager
        .register::<LobbyCustomPayloadEvent, _>(
            Arc::new(HandoffHandler),
            EventPriority::Highest,
            true,
        );
    player.send_system_message(&TextComponent::text(format!(
        "cluster test handoff callback cue {}",
        identity.token
    )));
    Some(EarlyConfirmGate {
        started,
        observed: false,
        created: identity.created,
    })
}

pub(crate) fn confirmed(server: &Server, player: &Player, teleport_id: i32) {
    let Some(confirmation) = server.cluster_test.handoff.confirmations.pop() else {
        return;
    };
    if confirmation.identity.uuid != player.gameprofile.id
        || confirmation.identity.token
            != player
                .cluster_owner_install_generation
                .load(Ordering::Acquire)
        || !Arc::ptr_eq(&confirmation.identity.client, &player.client)
    {
        if server
            .cluster_test
            .handoff
            .confirmations
            .push(confirmation)
            .is_err()
        {
            tracing::error!("cluster test handoff confirmation queue overflow");
        }
        return;
    }
    if confirmation.identity.created.elapsed() >= Duration::from_secs(4) {
        tracing::error!("cluster test handoff confirmation expired");
        return;
    }
    let token = confirmation.identity.token;
    if server
        .cluster_test
        .handoff
        .releases
        .push(confirmation)
        .is_err()
    {
        tracing::error!("cluster test handoff release queue overflow");
        return;
    }
    for observer in server.get_all_players() {
        observer.send_system_message(&TextComponent::text(format!(
            "cluster test handoff actual confirmation {token} {teleport_id}"
        )));
    }
}

pub(crate) fn release(server: &Server, origin: pumpkin_cluster::identity::ServerId) -> bool {
    let Some(players) = super::cluster_test::registered_players(server) else {
        return false;
    };
    if players[1].server != origin || players[0].server == origin {
        return false;
    }
    let Some(confirmation) = server.cluster_test.handoff.releases.pop() else {
        return false;
    };
    confirmation.identity.created.elapsed() < Duration::from_secs(4)
        && confirmation.release.send(()).is_ok()
}

pub(crate) struct RetirementBarrierEvidence {
    uuid: Uuid,
    new_session: u64,
    old_install: Option<u64>,
    pending_commit: Option<u64>,
    expected_revision: Option<u64>,
    started: Instant,
}

impl RetirementBarrierEvidence {
    pub(crate) fn entered(
        uuid: Uuid,
        new_session: u64,
        old_install: Option<u64>,
        pending_commit: Option<u64>,
        expected_revision: Option<u64>,
    ) -> Option<Self> {
        let selected = std::env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
        if !selected
            .split(',')
            .any(|id| matches!(id, "chat-departure-delay" | "command-departure-delay"))
        {
            return None;
        }
        if new_session == 0 || old_install.is_none() && pending_commit.is_none() {
            tracing::error!(%uuid,new_session,"cluster test retirement barrier identity missing");
            return None;
        }
        tracing::info!(%uuid,new_session,?old_install,?pending_commit,?expected_revision,"cluster e2e retirement barrier entered");
        Some(Self {
            uuid,
            new_session,
            old_install,
            pending_commit,
            expected_revision,
            started: Instant::now(),
        })
    }

    pub(crate) fn released(self, revision: u64, world: &str, position: [f64; 3]) {
        if !position.iter().all(|component| component.is_finite())
            || self
                .expected_revision
                .is_some_and(|expected| revision < expected)
        {
            tracing::error!(uuid=%self.uuid,new_session=self.new_session,revision,"cluster test retirement barrier release revision mismatch");
            return;
        }
        tracing::info!(uuid=%self.uuid,new_session=self.new_session,old_install=?self.old_install,pending_commit=?self.pending_commit,expected_revision=?self.expected_revision,actual_revision=revision,world,x=position[0],y=position[1],z=position[2],elapsed_millis=self.started.elapsed().as_millis() as u64,"cluster e2e retirement barrier released");
    }
}
