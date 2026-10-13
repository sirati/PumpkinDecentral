use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::player_command_control::{
    CommandPrincipal, CommandTarget, PlayerCommandMutation, PlayerCommandRequest,
};
use pumpkin_cluster::realm_completion::PeerGeneration;
use tokio::sync::mpsc;

use crate::command::{CommandSender, CommandSource};

use super::Server;

pub struct AuthenticatedPlayerCommand {
    pub origin: PeerGeneration,
    pub transport_epoch: u64,
    pub request: PlayerCommandRequest,
}

struct CommandCompletionRegistration {
    result: Option<pumpkin_command::source::ReturnValue>,
    correlation: [u8; 16],
    completion: pumpkin_command::deferred::DeferredCommandCompletion<CommandSource>,
    _reservation: tokio::sync::OwnedSemaphorePermit,
}

pub struct PlayerCommandRuntime {
    pub(crate) branch_events:
        mpsc::Sender<super::cluster_execute_continuation::BranchCompletionEvent>,
    branch_receivers: crossbeam::queue::ArrayQueue<
        mpsc::Receiver<super::cluster_execute_continuation::BranchCompletionEvent>,
    >,
    deferred_completions: crossbeam::queue::ArrayQueue<CommandCompletionRegistration>,
    completion_slots: std::sync::Arc<tokio::sync::Semaphore>,
    tx: mpsc::Sender<AuthenticatedPlayerCommand>,
    continuations:
        crossbeam::queue::ArrayQueue<super::cluster_execute_continuation::SummonContinuation>,
    continuation_slots: std::sync::Arc<tokio::sync::Semaphore>,
    observations: crossbeam::queue::ArrayQueue<CommandObservation>,
    registrations: std::sync::Arc<tokio::sync::Semaphore>,
    sources: std::sync::Arc<tokio::sync::Semaphore>,
    terminals: arc_swap::ArcSwap<std::collections::HashMap<[u8; 16], CommandTerminal>>,
}

#[derive(Debug, Clone, Copy)]
pub struct CommandTerminal {
    pub tick: pumpkin_cluster::time::TickStamp,
    pub action: Option<pumpkin_cluster::canonical_action::ActionRef>,
    pub applied: bool,
}

impl PlayerCommandRuntime {
    pub fn new() -> (Self, mpsc::Receiver<AuthenticatedPlayerCommand>) {
        let (tx, rx) = mpsc::channel(512);
        let (branch_events, branch_rx) = mpsc::channel(65536);
        let branch_receivers = crossbeam::queue::ArrayQueue::new(1);
        assert!(branch_receivers.push(branch_rx).is_ok());
        (
            Self {
                branch_events,
                branch_receivers,
                deferred_completions: crossbeam::queue::ArrayQueue::new(2048),
                completion_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(2048)),
                tx,
                continuations: crossbeam::queue::ArrayQueue::new(64),
                continuation_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
                observations: crossbeam::queue::ArrayQueue::new(2048),
                registrations: std::sync::Arc::new(tokio::sync::Semaphore::new(2048)),
                sources: std::sync::Arc::new(tokio::sync::Semaphore::new(2048)),
                terminals: arc_swap::ArcSwap::from_pointee(std::collections::HashMap::new()),
            },
            rx,
        )
    }

    pub(crate) fn reserve_continuation(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, PlayerCommandSubmitError> {
        self.continuation_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| PlayerCommandSubmitError::QueueFull)
    }

    pub(crate) fn enqueue_continuation(
        &self,
        continuation: super::cluster_execute_continuation::SummonContinuation,
    ) {
        assert!(
            self.continuations.push(continuation).is_ok(),
            "reserved execute continuation capacity"
        );
    }

    pub fn receive(
        &self,
        command: AuthenticatedPlayerCommand,
    ) -> Result<(), AuthenticatedPlayerCommand> {
        self.tx
            .try_send(command)
            .map_err(|error| error.into_inner())
    }

    pub fn terminal(&self, correlation: [u8; 16]) -> Option<CommandTerminal> {
        self.terminals.load().get(&correlation).copied()
    }
    pub fn record_input_failure(
        &self,
        origin: PeerGeneration,
        correlation: [u8; 16],
        reason: PlayerCommandSubmitError,
    ) -> Result<(), CommandObservation> {
        self.observations.push(CommandObservation::Failure {
            origin,
            correlation,
            reason,
        })
    }

    pub fn register_action(
        &self,
        origin: PeerGeneration,
        correlation: [u8; 16],
        action: pumpkin_cluster::canonical_action::ActionRef,
        receipt: Option<pumpkin_cluster::issuer_entity_admission::CommandAdmissionReceipt>,
    ) -> Result<(), CommandObservation> {
        let event = CommandObservation::Registered {
            origin,
            correlation,
            action,
            receipt,
            reservation: None,
        };
        self.retry_observation(event)
    }

    pub fn accepted_tick(
        &self,
        tick: pumpkin_cluster::time::TickStamp,
        outcomes: Vec<(pumpkin_cluster::canonical_action::ActionRef, bool)>,
    ) -> Result<(), CommandObservation> {
        self.observations
            .push(CommandObservation::Accepted { tick, outcomes })
    }

    pub fn next_observation(&self) -> Option<CommandObservation> {
        self.observations.pop()
    }

    pub fn retry_observation(
        &self,
        mut observation: CommandObservation,
    ) -> Result<(), CommandObservation> {
        if let CommandObservation::Registered { reservation, .. } = &mut observation {
            if reservation.is_none() {
                let Ok(slot) = std::sync::Arc::clone(&self.registrations).try_acquire_owned()
                else {
                    return Err(observation);
                };
                *reservation = Some(slot);
            }
        }
        self.observations.push(observation)
    }

    pub fn receive_observer(
        &self,
        origin: PeerGeneration,
        transport_epoch: u64,
        batch: pumpkin_cluster::player_command_control::PlayerCommandObserverBatch,
    ) -> Result<(), CommandObservation> {
        self.observations.push(CommandObservation::Remote {
            origin,
            transport_epoch,
            batch,
        })
    }
}

pub enum CommandObservation {
    Source {
        correlation: [u8; 16],
        source: CommandSender,
        owner: PeerGeneration,
        success: Option<pumpkin_util::text::TextComponent>,
        reservation: tokio::sync::OwnedSemaphorePermit,
    },
    Remote {
        origin: PeerGeneration,
        transport_epoch: u64,
        batch: pumpkin_cluster::player_command_control::PlayerCommandObserverBatch,
    },
    Failure {
        origin: PeerGeneration,
        correlation: [u8; 16],
        reason: PlayerCommandSubmitError,
    },
    Registered {
        origin: PeerGeneration,
        correlation: [u8; 16],
        action: pumpkin_cluster::canonical_action::ActionRef,
        receipt: Option<pumpkin_cluster::issuer_entity_admission::CommandAdmissionReceipt>,
        reservation: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    Accepted {
        tick: pumpkin_cluster::time::TickStamp,
        outcomes: Vec<(pumpkin_cluster::canonical_action::ActionRef, bool)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerCommandSubmitError {
    Unauthorized,
    NoTarget,
    NoRoute,
    QueueFull,
    InvalidInput,
    PendingAuthority,
}

pub fn permission_revision(server: &Server) -> u64 {
    permission_revision_snapshot(&server.cluster_admin_view.load())
}

pub(crate) fn permission_revision_snapshot(
    snapshot: &pumpkin_cluster::admin_sync::AdminSnapshot,
) -> u64 {
    let mut grants = snapshot
        .operators
        .iter()
        .map(|entry| (entry.uuid, entry.level))
        .collect::<Vec<_>>();
    grants.sort_unstable();
    let mut fingerprint = 0;
    for (uuid, level) in grants {
        fingerprint = xxhash_rust::xxh64::xxh64(&uuid, fingerprint);
        fingerprint = xxhash_rust::xxh64::xxh64(&[level], fingerprint);
    }
    fingerprint
}

fn player_principal(
    server: &Server,
    gid: GlobalPlayerId,
    uuid: uuid::Uuid,
) -> Result<
    (
        CommandPrincipal,
        pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot,
    ),
    PlayerCommandSubmitError,
> {
    let source = ServerId(server.advanced_config.cluster.server_id);
    let admin = server.cluster_admin_view.load();
    let level = admin
        .operators
        .iter()
        .find(|op| op.uuid == *uuid.as_bytes())
        .map(|op| op.level)
        .unwrap_or(0);
    if gid.server != source || level < 2 {
        return Err(PlayerCommandSubmitError::Unauthorized);
    }
    let revision = permission_revision_snapshot(&admin);
    Ok((
        CommandPrincipal::Player {
            gid,
            uuid: *uuid.as_bytes(),
            permission_revision: revision,
        },
        pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot { revision, level },
    ))
}

pub fn submit(
    server: &Server,
    source: &CommandSource,
    target: GlobalPlayerId,
    target_uuid: [u8; 16],
    mutation: PlayerCommandMutation,
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    submit_with_feedback(server, source, target, target_uuid, mutation, None)
}

pub fn submit_with_feedback(
    server: &Server,
    source: &CommandSource,
    target: GlobalPlayerId,
    target_uuid: [u8; 16],
    mutation: PlayerCommandMutation,
    success: Option<pumpkin_util::text::TextComponent>,
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    submit_correlated(
        server,
        source,
        CommandTarget::Player {
            gid: target,
            uuid: target_uuid,
        },
        mutation,
        success,
        *uuid::Uuid::new_v4().as_bytes(),
    )
}

#[cfg(feature = "cluster-e2e")]
pub fn submit_test_departure(
    server: &Server,
    source: &CommandSource,
    target: GlobalPlayerId,
    target_uuid: [u8; 16],
    mutation: PlayerCommandMutation,
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    let mut correlation = *uuid::Uuid::new_v4().as_bytes();
    correlation[..8].copy_from_slice(b"PumpDAdm");
    submit_correlated(
        server,
        source,
        CommandTarget::Player {
            gid: target,
            uuid: target_uuid,
        },
        mutation,
        None,
        correlation,
    )
}

pub fn submit_target(
    server: &Server,
    source: &CommandSource,
    target: CommandTarget,
    mutation: PlayerCommandMutation,
    success: Option<pumpkin_util::text::TextComponent>,
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    submit_correlated(
        server,
        source,
        target,
        mutation,
        success,
        *uuid::Uuid::new_v4().as_bytes(),
    )
}

pub fn entity_target(
    server: &Server,
    entity: &dyn crate::entity::EntityBase,
) -> Option<CommandTarget> {
    if let Some(shell) = entity
        .cast_any()
        .downcast_ref::<super::cluster_entity_registry::CanonicalEntityShell>()
    {
        if server
            .cluster_peer_incarnations
            .load()
            .get(&shell.body.owner.server)
            != Some(&shell.body.owner.generation)
        {
            return None;
        }
        return Some(CommandTarget::Entity {
            identity: pumpkin_cluster::item_contact::ContactIdentity::Entity {
                id: shell.body.id,
                birth: shell.body.birth.clone(),
            },
            owner: shell.body.owner,
        });
    }
    let targets = server.cluster_combat_client_targets.load();
    let target = targets.get(&entity.get_entity().entity_id)?;
    if !target.contact.valid()
        || server
            .cluster_peer_incarnations
            .load()
            .get(&target.contact.owner.server)
            != Some(&target.contact.owner.generation)
    {
        return None;
    }
    Some(match &target.contact.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => CommandTarget::Player {
            gid: lease.gid,
            uuid: *lease.key.0.as_bytes(),
        },
        identity @ pumpkin_cluster::item_contact::ContactIdentity::Entity { .. } => {
            CommandTarget::Entity {
                identity: identity.clone(),
                owner: target.contact.owner,
            }
        }
    })
}

fn submit_correlated(
    server: &Server,
    source: &CommandSource,
    target: CommandTarget,
    mutation: PlayerCommandMutation,
    success: Option<pumpkin_util::text::TextComponent>,
    correlation: [u8; 16],
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    submit_correlated_captured(server, source, target, mutation, success, correlation, None)
        .map(|(correlation, _)| correlation)
}

fn submit_correlated_captured(
    server: &Server,
    source: &CommandSource,
    target: CommandTarget,
    mutation: PlayerCommandMutation,
    success: Option<pumpkin_util::text::TextComponent>,
    correlation: [u8; 16],
    remaining_ast_digest: Option<[u8; 32]>,
) -> Result<
    (
        [u8; 16],
        Option<super::cluster_command_issuance::CapturedCommandAuthority>,
    ),
    PlayerCommandSubmitError,
> {
    let frozen_permissions = if matches!(&mutation, PlayerCommandMutation::Summon(_)) {
        Some(
            super::cluster_permissions::capture_command_permissions(server, source)
                .ok_or(PlayerCommandSubmitError::PendingAuthority)?,
        )
    } else {
        super::cluster_permissions::active_command_permissions(source)
    };
    let (principal, permission_snapshot) = if let Some(permissions) = &frozen_permissions {
        (permissions.principal.clone(), permissions.permission)
    } else {
        match &source.output {
            CommandSender::Player(player) | CommandSender::Lobby(player) => player_principal(
                server,
                player
                    .cluster_gid()
                    .ok_or(PlayerCommandSubmitError::Unauthorized)?,
                player.gameprofile.id,
            )?,
            CommandSender::PreJoin(session) => {
                player_principal(server, session.gid, session.profile.id)?
            }
            CommandSender::Console | CommandSender::Rcon(_) => (
                CommandPrincipal::Console,
                pumpkin_cluster::issuer_entity_admission::CommandPermissionSnapshot {
                    revision: 0,
                    level: 4,
                },
            ),
            CommandSender::CommandBlock(..) | CommandSender::Dummy => {
                return Err(PlayerCommandSubmitError::Unauthorized);
            }
        }
    };
    if matches!(&mutation, PlayerCommandMutation::Reset(_)) && permission_snapshot.level < 4 {
        return Err(PlayerCommandSubmitError::Unauthorized);
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    let incarnations = server.cluster_peer_incarnations.load();
    let source_incarnation = incarnations
        .get(&local)
        .copied()
        .ok_or(PlayerCommandSubmitError::NoRoute)?;
    let target_server = match &target {
        CommandTarget::Player { gid, .. } => gid.server,
        CommandTarget::Entity { owner, .. } | CommandTarget::World { owner, .. } => owner.server,
    };
    let owner = PeerGeneration {
        server: target_server,
        generation: incarnations
            .get(&target_server)
            .copied()
            .ok_or(PlayerCommandSubmitError::NoRoute)?,
    };
    let captured_permission =
        matches!(&mutation, PlayerCommandMutation::Summon(_)).then_some(permission_snapshot);
    let mut request = PlayerCommandRequest {
        source: local,
        source_incarnation,
        principal,
        permission_snapshot: captured_permission,
        continuation: None,
        target,
        correlation,
        mutation,
    };
    super::cluster_command_issuance::attach_continuation(server, source, &mut request)?;
    if !request.is_valid() {
        return Err(PlayerCommandSubmitError::InvalidInput);
    }
    let captured = if matches!(&request.mutation, PlayerCommandMutation::Summon(_)) {
        Some(super::cluster_command_issuance::capture_submission(
            server,
            source,
            &request,
            owner,
            frozen_permissions.ok_or(PlayerCommandSubmitError::PendingAuthority)?,
            remaining_ast_digest,
        )?)
    } else {
        None
    };
    let correlation = request.correlation;
    enum Submission {
        Local(mpsc::OwnedPermit<AuthenticatedPlayerCommand>),
        Remote(super::cluster_global_outbound::EventReservation, Vec<u8>),
    }
    let submission = if target_server == local {
        Submission::Local(
            server
                .cluster_player_commands
                .tx
                .clone()
                .try_reserve_owned()
                .map_err(|_| PlayerCommandSubmitError::QueueFull)?,
        )
    } else {
        if !server
            .cluster_connected_peers
            .load()
            .contains(&target_server)
        {
            return Err(PlayerCommandSubmitError::NoRoute);
        }
        let bytes = pumpkin_cluster::player_command_control::encode(&request)
            .map_err(|_| PlayerCommandSubmitError::InvalidInput)?;
        let reserved = server
            .cluster_global_outbound
            .try_reserve_event(server, vec![target_server])
            .ok_or(PlayerCommandSubmitError::QueueFull)?;
        Submission::Remote(reserved, bytes)
    };
    let reservation = std::sync::Arc::clone(&server.cluster_player_commands.sources)
        .try_acquire_owned()
        .map_err(|_| PlayerCommandSubmitError::QueueFull)?;
    server
        .cluster_player_commands
        .observations
        .push(CommandObservation::Source {
            correlation,
            source: source.output.clone(),
            owner,
            success,
            reservation,
        })
        .map_err(|_| PlayerCommandSubmitError::QueueFull)?;
    match submission {
        Submission::Local(permit) => {
            permit.send(AuthenticatedPlayerCommand {
                origin: PeerGeneration {
                    server: local,
                    generation: source_incarnation,
                },
                transport_epoch: 0,
                request,
            });
        }
        Submission::Remote(permit, bytes) => permit.commit(bytes),
    }
    Ok((correlation, captured))
}

pub fn authorize(
    server: &Server,
    origin: PeerGeneration,
    request: &PlayerCommandRequest,
) -> Result<(), PlayerCommandSubmitError> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    if !request.is_valid()
        || request.source != origin.server
        || request.source_incarnation != origin.generation
        || request.target_owner() != local
    {
        return Err(PlayerCommandSubmitError::Unauthorized);
    }
    if server.cluster_peer_incarnations.load().get(&origin.server) != Some(&origin.generation)
        || (origin.server != local
            && (!server
                .cluster_connected_peers
                .load()
                .contains(&origin.server)))
    {
        return Err(PlayerCommandSubmitError::Unauthorized);
    }
    if !matches!(&request.mutation, PlayerCommandMutation::Summon(_))
        && request.continuation.is_none()
        && let CommandPrincipal::Player {
            gid,
            uuid,
            permission_revision: revision,
        } = &request.principal
    {
        let presence = server.cluster_presence.snapshot();
        let issuer = presence
            .entries
            .get(gid)
            .ok_or(PlayerCommandSubmitError::PendingAuthority)?;
        if issuer.uuid != *uuid || gid.server != origin.server {
            return Err(PlayerCommandSubmitError::Unauthorized);
        }
        if *revision != permission_revision(server) {
            return Err(PlayerCommandSubmitError::PendingAuthority);
        }
        if !server.cluster_admin_view.load().operators.iter().any(|op| {
            op.uuid == *uuid
                && op.level
                    >= if matches!(request.mutation, PlayerCommandMutation::Reset(_)) {
                        4
                    } else {
                        2
                    }
        }) {
            return Err(PlayerCommandSubmitError::Unauthorized);
        }
    }
    if let Some((gid, uuid)) = request.player_target() {
        let target = server
            .cluster_presence
            .snapshot()
            .entries
            .get(&gid)
            .cloned()
            .ok_or(PlayerCommandSubmitError::NoTarget)?;
        if target.uuid != uuid || target.in_lobby {
            return Err(PlayerCommandSubmitError::NoTarget);
        }
    } else if let CommandTarget::Entity { owner, .. } | CommandTarget::World { owner, .. } =
        &request.target
    {
        if server.cluster_peer_incarnations.load().get(&owner.server) != Some(&owner.generation) {
            return Err(PlayerCommandSubmitError::NoTarget);
        }
    }
    Ok(())
}

pub fn authorize_authenticated(
    server: &Server,
    input: &AuthenticatedPlayerCommand,
) -> Result<(), PlayerCommandSubmitError> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    if input.origin.server != local
        && server
            .cluster_peer_generations
            .load()
            .get(&input.origin.server)
            != Some(&input.transport_epoch)
    {
        return Err(PlayerCommandSubmitError::Unauthorized);
    }
    authorize(server, input.origin, &input.request)
}

pub fn spawn_observer(server: &std::sync::Arc<Server>) {
    let mut branch_events = server
        .cluster_player_commands
        .branch_receivers
        .pop()
        .expect("one command observer branch owner");
    let server = std::sync::Arc::downgrade(server);
    tokio::spawn(async move {
        use pumpkin_cluster::player_command_control::{
            PlayerCommandObserverBatch, PlayerCommandOutcome,
        };
        use std::collections::{BTreeMap, HashMap};
        let mut sources = HashMap::<
            [u8; 16],
            (
                CommandSender,
                PeerGeneration,
                std::time::Instant,
                Option<pumpkin_util::text::TextComponent>,
                tokio::sync::OwnedSemaphorePermit,
            ),
        >::new();
        let mut continuations =
            HashMap::<[u8; 16], super::cluster_execute_continuation::SummonContinuation>::new();
        let mut deferred_completions = HashMap::<[u8; 16], CommandCompletionRegistration>::new();
        let mut continuation_receipts = HashMap::new();
        let mut actions = BTreeMap::new();
        let mut pending_notified = std::collections::HashSet::new();
        let mut terminals = HashMap::<[u8; 16], (std::time::Instant, CommandTerminal)>::new();
        let mut outgoing = BTreeMap::<PeerGeneration, Vec<PlayerCommandOutcome>>::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        let mut last_sent = BTreeMap::<PeerGeneration, pumpkin_cluster::time::TickStamp>::new();
        loop {
            interval.tick().await;
            let Some(server) = server.upgrade() else {
                break;
            };
            let local = ServerId(server.advanced_config.cluster.server_id);
            let tick = pumpkin_cluster::time::TickStamp::now();
            last_sent.retain(|peer, _| {
                server.cluster_peer_incarnations.load().get(&peer.server) == Some(&peer.generation)
            });
            while outgoing.values().map(Vec::len).sum::<usize>() < 2048 {
                let Some(event) = server.cluster_player_commands.next_observation() else {
                    break;
                };
                match event {
                    CommandObservation::Source {
                        correlation,
                        source,
                        owner,
                        success,
                        reservation,
                    } => {
                        sources.insert(
                            correlation,
                            (
                                source,
                                owner,
                                std::time::Instant::now(),
                                success,
                                reservation,
                            ),
                        );
                    }
                    CommandObservation::Registered {
                        origin,
                        correlation,
                        action,
                        receipt,
                        reservation,
                    } => {
                        if let Some(receipt) = receipt {
                            continuation_receipts.insert(correlation, receipt);
                        }
                        if let Some(reservation) = reservation {
                            actions.insert(action, (origin, correlation, reservation));
                        }
                    }
                    CommandObservation::Failure {
                        origin,
                        correlation,
                        ..
                    } => {
                        outgoing
                            .entry(origin)
                            .or_default()
                            .push(PlayerCommandOutcome {
                                correlation,
                                action: None,
                                applied: false,
                            });
                    }
                    CommandObservation::Accepted { tick: _, outcomes } => {
                        for (action, applied) in outcomes {
                            if let Some((origin, correlation, _reservation)) =
                                actions.remove(&action)
                            {
                                outgoing
                                    .entry(origin)
                                    .or_default()
                                    .push(PlayerCommandOutcome {
                                        correlation,
                                        action: Some(action),
                                        applied,
                                    });
                            }
                        }
                    }
                    CommandObservation::Remote {
                        origin,
                        transport_epoch,
                        batch,
                    } => {
                        if batch.recipient.server != local
                            || server.cluster_peer_incarnations.load().get(&origin.server)
                                != Some(&origin.generation)
                            || server.cluster_peer_generations.load().get(&origin.server)
                                != Some(&transport_epoch)
                            || server.cluster_peer_incarnations.load().get(&local)
                                != Some(&batch.recipient.generation)
                        {
                            continue;
                        }
                        for outcome in batch.outcomes {
                            if sources
                                .get(&outcome.correlation)
                                .is_some_and(|(_, owner, _, _, _)| *owner == origin)
                                && let Some((source, _, _, success, _reservation)) =
                                    sources.remove(&outcome.correlation)
                            {
                                terminals.insert(
                                    outcome.correlation,
                                    (
                                        std::time::Instant::now(),
                                        CommandTerminal {
                                            tick: outcome
                                                .action
                                                .map_or(batch.tick, |action| action.tick),
                                            action: outcome.action,
                                            applied: outcome.applied,
                                        },
                                    ),
                                );
                                source.send_message(if outcome.applied {
                                    success.unwrap_or_else(|| {
                                        pumpkin_util::text::TextComponent::text("Command applied")
                                    })
                                } else {
                                    pumpkin_util::text::TextComponent::text(
                                        "Command could not be applied",
                                    )
                                });
                            }
                        }
                    }
                }
            }
            let mut emptied = Vec::new();
            for (origin, outcomes) in &mut outgoing {
                if origin.server == local {
                    for outcome in outcomes.drain(..) {
                        if let Some((source, _, _, success, _reservation)) =
                            sources.remove(&outcome.correlation)
                        {
                            terminals.insert(
                                outcome.correlation,
                                (
                                    std::time::Instant::now(),
                                    CommandTerminal {
                                        tick: outcome.action.map_or(tick, |action| action.tick),
                                        action: outcome.action,
                                        applied: outcome.applied,
                                    },
                                ),
                            );
                            source.send_message(if outcome.applied {
                                success.unwrap_or_else(|| {
                                    pumpkin_util::text::TextComponent::text("Command applied")
                                })
                            } else {
                                pumpkin_util::text::TextComponent::text(
                                    "Command could not be applied",
                                )
                            });
                        }
                    }
                } else if server.cluster_peer_incarnations.load().get(&origin.server)
                    != Some(&origin.generation)
                {
                    outcomes.clear();
                } else {
                    if last_sent.get(origin) == Some(&tick) {
                        continue;
                    }
                    let len = outcomes.len().min(256);
                    let batch = PlayerCommandObserverBatch {
                        recipient: *origin,
                        tick,
                        outcomes: outcomes[..len].to_vec(),
                    };
                    if let Ok(bytes) =
                        pumpkin_cluster::player_command_control::encode_observer(&batch)
                        && server.cluster_global_outbound.try_event(
                            &server,
                            vec![origin.server],
                            bytes,
                        )
                    {
                        outcomes.drain(..len);
                        last_sent.insert(*origin, tick);
                    }
                }
                if outcomes.is_empty() {
                    emptied.push(*origin);
                }
            }
            for origin in emptied {
                outgoing.remove(&origin);
            }
            while let Some(completion) = server.cluster_player_commands.deferred_completions.pop() {
                assert!(
                    !deferred_completions.contains_key(&completion.correlation),
                    "unique deferred command correlation"
                );
                deferred_completions.insert(completion.correlation, completion);
            }
            let completed = deferred_completions
                .iter()
                .filter_map(|(correlation, completion)| {
                    let result = completion.result.or_else(|| {
                        terminals.get(correlation).map(|(_, terminal)| {
                            if terminal.applied && terminal.action.is_some() {
                                pumpkin_command::source::ReturnValue::Success(1)
                            } else {
                                pumpkin_command::source::ReturnValue::Failure
                            }
                        })
                    })?;
                    Some((*correlation, result))
                })
                .collect::<Vec<_>>();
            for (correlation, result) in completed {
                let mut completion = deferred_completions.remove(&correlation).unwrap();
                if let Err((sink, result)) = completion.completion.try_finish(result) {
                    completion.completion = sink;
                    completion.result = Some(result);
                    deferred_completions.insert(correlation, completion);
                }
            }
            while let Some(continuation) = server.cluster_player_commands.continuations.pop() {
                continuations.insert(continuation.parent_token.0, continuation);
            }
            while let Ok(event) = branch_events.try_recv() {
                if let Some(continuation) = continuations.get_mut(&event.parent.0) {
                    continuation.child_completed(event.child);
                }
            }
            continuations.retain(|_, continuation| {
                if let Some(receipt) = continuation_receipts.remove(&continuation.correlation) {
                    continuation.receipt = Some(receipt);
                }
                let terminal = terminals
                    .get(&continuation.correlation)
                    .map(|(_, terminal)| *terminal);
                !continuation.advance(&server, terminal)
            });
            continuation_receipts.retain(|correlation, _| {
                sources.contains_key(correlation)
                    || continuations
                        .values()
                        .any(|continuation| continuation.correlation == *correlation)
                    || actions
                        .values()
                        .any(|(_, registered, _)| registered == correlation)
            });
            terminals.retain(|_, (received, _)| {
                received.elapsed() < std::time::Duration::from_secs(300)
            });
            while terminals.len() > 4096 {
                let Some(oldest) = terminals
                    .iter()
                    .min_by_key(|(_, (received, _))| *received)
                    .map(|(key, _)| *key)
                else {
                    break;
                };
                terminals.remove(&oldest);
            }
            server
                .cluster_player_commands
                .terminals
                .store(std::sync::Arc::new(
                    terminals
                        .iter()
                        .map(|(key, (_, value))| (*key, *value))
                        .collect(),
                ));
            sources.retain(|correlation, (source, _, started, _, _)| {
                if started.elapsed() > std::time::Duration::from_secs(60)
                    && pending_notified.insert(*correlation)
                {
                    source.send_message(pumpkin_util::text::TextComponent::text(
                        "Command settlement remains pending",
                    ));
                }
                if started.elapsed() > std::time::Duration::from_secs(300)
                    && !deferred_completions.contains_key(correlation)
                    && !continuations
                        .values()
                        .any(|continuation| continuation.correlation == *correlation)
                {
                    source.send_message(pumpkin_util::text::TextComponent::text(
                        "Command settlement observation expired; outcome remains unknown",
                    ));
                    return false;
                }
                true
            });
            pending_notified.retain(|correlation| sources.contains_key(correlation));
        }
    });
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CapturedPermission {
    pub issuer: PeerGeneration,
    pub viewer: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub tick: pumpkin_cluster::time::TickStamp,
    pub policy_revision: u64,
    pub decision_version: u64,
    pub node: String,
    pub allowed: bool,
}

pub fn capture_permission(
    server: &Server,
    viewer: pumpkin_cluster::canonical_action::LivePlayerLease,
    tick: pumpkin_cluster::time::TickStamp,
    node: &str,
) -> Option<CapturedPermission> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    let incarnation = server
        .cluster_peer_incarnations
        .load()
        .get(&local)
        .copied()?;
    if viewer.gid.server != local
        || server
            .cluster_actor_statistics
            .load()
            .get(&viewer.gid)
            .is_none_or(|state| state.incarnation != viewer.incarnation)
    {
        return None;
    }
    let policy = super::cluster_permissions::freeze_permission_policy(server)?;
    super::cluster_permissions::capture_permission_for_action(
        viewer,
        PeerGeneration {
            server: local,
            generation: incarnation,
        },
        tick,
        node,
        &policy,
    )
}

pub(crate) fn submit_summon_deferred(
    server: &Server,
    source: std::sync::Arc<CommandSource>,
    request: pumpkin_cluster::mob_initialization::EntityAdmissionRequest,
) -> Result<pumpkin_command::deferred::DeferredCommandToken, PlayerCommandSubmitError> {
    let reservation = server
        .cluster_player_commands
        .completion_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| PlayerCommandSubmitError::QueueFull)?;
    let correlation = *uuid::Uuid::new_v4().as_bytes();
    let token = pumpkin_command::deferred::DeferredCommandToken(correlation);
    let completion = pumpkin_command::deferred::DeferredCommandCompletion::reserve(
        super::cluster_command_issuance::capture_completion_source(source.clone()),
        token,
    )
    .map_err(|_| PlayerCommandSubmitError::QueueFull)?;
    submit_summon_correlated_captured(
        server,
        &source,
        request,
        Some(pumpkin_util::text::TextComponent::text("Entity summoned")),
        None,
        correlation,
    )?;
    assert!(
        server
            .cluster_player_commands
            .deferred_completions
            .push(CommandCompletionRegistration {
                result: None,
                correlation,
                completion,
                _reservation: reservation
            })
            .is_ok(),
        "reserved command completion capacity"
    );
    Ok(token)
}

pub(crate) fn submit_summon(
    server: &Server,
    source: &CommandSource,
    request: pumpkin_cluster::mob_initialization::EntityAdmissionRequest,
) -> Result<[u8; 16], PlayerCommandSubmitError> {
    submit_summon_captured(
        server,
        source,
        request,
        Some(pumpkin_util::text::TextComponent::text("Entity summoned")),
        None,
    )
    .map(|(correlation, _)| correlation)
}

pub(crate) fn submit_summon_captured(
    server: &Server,
    source: &CommandSource,
    request: pumpkin_cluster::mob_initialization::EntityAdmissionRequest,
    success: Option<pumpkin_util::text::TextComponent>,
    remaining_ast_digest: Option<[u8; 32]>,
) -> Result<
    (
        [u8; 16],
        super::cluster_command_issuance::CapturedCommandAuthority,
    ),
    PlayerCommandSubmitError,
> {
    submit_summon_correlated_captured(
        server,
        source,
        request,
        success,
        remaining_ast_digest,
        *uuid::Uuid::new_v4().as_bytes(),
    )
}

fn submit_summon_correlated_captured(
    server: &Server,
    source: &CommandSource,
    request: pumpkin_cluster::mob_initialization::EntityAdmissionRequest,
    success: Option<pumpkin_util::text::TextComponent>,
    remaining_ast_digest: Option<[u8; 32]>,
    correlation: [u8; 16],
) -> Result<
    (
        [u8; 16],
        super::cluster_command_issuance::CapturedCommandAuthority,
    ),
    PlayerCommandSubmitError,
> {
    let target = if let Some(player) = source
        .output
        .as_player()
        .filter(|player| !player.is_in_cluster_lobby())
    {
        CommandTarget::Player {
            gid: player
                .cluster_gid()
                .ok_or(PlayerCommandSubmitError::NoTarget)?,
            uuid: *player.gameprofile.id.as_bytes(),
        }
    } else {
        let local = ServerId(server.advanced_config.cluster.server_id);
        let generation = server
            .cluster_peer_incarnations
            .load()
            .get(&local)
            .copied()
            .ok_or(PlayerCommandSubmitError::NoRoute)?;
        CommandTarget::World {
            world: request.spatial.world,
            owner: PeerGeneration {
                server: local,
                generation,
            },
        }
    };
    let (correlation, captured) = submit_correlated_captured(
        server,
        source,
        target,
        PlayerCommandMutation::Summon(request),
        success,
        correlation,
        remaining_ast_digest,
    )?;
    Ok((
        correlation,
        captured.ok_or(PlayerCommandSubmitError::InvalidInput)?,
    ))
}
