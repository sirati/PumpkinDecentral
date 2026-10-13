use pumpkin_cluster::canonical_action::{ActionRef, ApplyFailure, Stakeholder};
use pumpkin_cluster::issuer_entity_admission::{
    AcceptedCommandIssuance, IssuerAdmissionAuthority, IssuerAdmissionWitness, request_digest,
};
use pumpkin_cluster::player_command_control::{PlayerCommandMutation, PlayerCommandRequest};
use pumpkin_cluster::realm_completion::PeerGeneration;

pub(super) fn capture(
    server: &super::Server,
    origin: PeerGeneration,
    transport_generation: u64,
    command: &PlayerCommandRequest,
    issuer: PeerGeneration,
) -> Result<AcceptedCommandIssuance, ApplyFailure> {
    let PlayerCommandMutation::Summon(request) = &command.mutation else {
        return Err(ApplyFailure::InvalidAction);
    };
    if !command.is_valid()
        || command.source != origin.server
        || command.source_incarnation != origin.generation
        || command.target_owner() != issuer.server
        || issuer.server.0 != server.advanced_config.cluster.server_id
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let issuance = AcceptedCommandIssuance {
        issuer,
        origin,
        transport_generation,
        principal: command.principal.clone(),
        correlation: command.correlation,
        permission_snapshot: command
            .permission_snapshot
            .ok_or(ApplyFailure::InvalidAction)?,
        request_digest: request_digest(request)?,
    };
    issuance
        .valid()
        .then_some(issuance)
        .ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn validate(
    reference: ActionRef,
    witness: &IssuerAdmissionWitness,
    original_slot: Option<u64>,
) -> Result<(), ApplyFailure> {
    let IssuerAdmissionAuthority::Command { receipt } = &witness.authority else {
        return Err(ApplyFailure::InvalidAction);
    };
    if !witness.valid() || receipt.reference != reference || !receipt.matches(witness) {
        return Err(ApplyFailure::InvalidAction);
    }
    match original_slot {
        Some(slot) if slot == receipt.source_slot => Ok(()),
        Some(_) => Err(ApplyFailure::InvalidAction),
        None => Err(ApplyFailure::MissingStakeholder(Stakeholder::Chunk(
            witness.actor_chunk,
        ))),
    }
}

#[derive(Clone)]
pub(crate) struct CapturedCommandAuthority {
    permissions: super::cluster_permissions::FrozenCommandPermissions,
    owner: PeerGeneration,
    correlation: [u8; 16],
    request_digest: [u8; 32],
    command_digest: [u8; 32],
    source_lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    remaining_ast_digest: Option<[u8; 32]>,
}

#[derive(Clone)]
pub(crate) struct BoundCommandContinuation {
    captured: CapturedCommandAuthority,
    predecessor: ActionRef,
    original_slot: u64,
    expires_slot: u64,
    remaining_ast_digest: [u8; 32],
}

tokio::task_local! {
    static CONTINUATION: BoundCommandContinuation;
}

pub(crate) fn capture_submission(
    server: &super::Server,
    source: &crate::command::CommandSource,
    command: &PlayerCommandRequest,
    owner: PeerGeneration,
    permissions: super::cluster_permissions::FrozenCommandPermissions,
    remaining_ast_digest: Option<[u8; 32]>,
) -> Result<CapturedCommandAuthority, super::cluster_player_commands::PlayerCommandSubmitError> {
    use super::cluster_player_commands::PlayerCommandSubmitError as Error;
    let PlayerCommandMutation::Summon(request) = &command.mutation else {
        return Err(Error::InvalidInput);
    };
    if !command.is_valid()
        || !permissions.matches_source(source)
        || !permissions.allows("minecraft:command.summon")
        || command.principal != permissions.principal
        || command.permission_snapshot != Some(permissions.permission)
        || command.source != permissions.origin.server
        || command.source_incarnation != permissions.origin.generation
        || command.target_owner() != owner.server
    {
        return Err(Error::Unauthorized);
    }
    let source_lease = source
        .output
        .as_player()
        .and_then(|player| super::cluster_world_owner::player_action_lease(server, &player));
    Ok(CapturedCommandAuthority {
        permissions,
        owner,
        correlation: command.correlation,
        request_digest: request_digest(request).map_err(|_| Error::InvalidInput)?,
        command_digest: pumpkin_cluster::command_continuation::command_digest(command)
            .map_err(|_| Error::InvalidInput)?,
        source_lease,
        remaining_ast_digest,
    })
}

pub(crate) fn bind_continuation(
    captured: &CapturedCommandAuthority,
    receipt: &pumpkin_cluster::issuer_entity_admission::CommandAdmissionReceipt,
    remaining_ast_digest: [u8; 32],
) -> Result<BoundCommandContinuation, super::cluster_player_commands::PlayerCommandSubmitError> {
    use super::cluster_player_commands::PlayerCommandSubmitError as Error;
    if !receipt.valid_binding()
        || receipt.issuance.origin != captured.permissions.origin
        || receipt.issuance.issuer != captured.owner
        || receipt.issuance.principal != captured.permissions.principal
        || receipt.issuance.permission_snapshot != captured.permissions.permission
        || receipt.issuance.correlation != captured.correlation
        || receipt.issuance.request_digest != captured.request_digest
        || remaining_ast_digest == [0; 32]
        || captured.remaining_ast_digest != Some(remaining_ast_digest)
    {
        return Err(Error::Unauthorized);
    }
    Ok(BoundCommandContinuation {
        captured: captured.clone(),
        predecessor: receipt.reference,
        original_slot: receipt.source_slot,
        expires_slot: receipt
            .source_slot
            .checked_add(pumpkin_cluster::command_continuation::MAX_CONTINUATION_SLOTS)
            .ok_or(Error::InvalidInput)?,
        remaining_ast_digest,
    })
}

fn qualified_source_matches(
    origin: PeerGeneration,
    principal: &pumpkin_cluster::player_command_control::CommandPrincipal,
    session: u64,
    current_boot: Option<u64>,
    entry: Option<&pumpkin_cluster::presence::RemotePlayerEntry>,
) -> bool {
    if current_boot != Some(origin.generation) {
        return false;
    }
    match principal {
        pumpkin_cluster::player_command_control::CommandPrincipal::Console => session == 0,
        pumpkin_cluster::player_command_control::CommandPrincipal::Player { gid, uuid, .. } => {
            entry.is_some_and(|entry| {
                entry.gid == *gid
                    && entry.uuid == *uuid
                    && entry.session == session
                    && entry.source == Some(origin)
            })
        }
    }
}

impl BoundCommandContinuation {
    fn source_current(
        &self,
        server: &super::Server,
        source: &crate::command::CommandSource,
    ) -> bool {
        use pumpkin_cluster::player_command_control::CommandPrincipal;
        let permissions = &self.captured.permissions;
        if !permissions.matches_source(source)
            || server
                .cluster_peer_incarnations
                .load()
                .get(&permissions.origin.server)
                != Some(&permissions.origin.generation)
        {
            return false;
        }
        match &permissions.principal {
            CommandPrincipal::Console => true,
            CommandPrincipal::Player { gid, .. } => {
                let client_open = match &source.output {
                    crate::command::CommandSender::Player(player)
                    | crate::command::CommandSender::Lobby(player) => !player.client.closed(),
                    crate::command::CommandSender::PreJoin(player) => !player.client.closed(),
                    _ => false,
                };
                client_open
                    && qualified_source_matches(
                        permissions.origin,
                        &permissions.principal,
                        permissions.source_session,
                        server
                            .cluster_peer_incarnations
                            .load()
                            .get(&permissions.origin.server)
                            .copied(),
                        server.cluster_presence.snapshot().entries.get(gid),
                    )
                    && self.captured.source_lease.is_none_or(|lease| {
                        source.output.as_player().and_then(|player| {
                            super::cluster_world_owner::player_action_lease(server, &player)
                        }) == Some(lease)
                    })
            }
        }
    }
}

pub(crate) fn with_continuation<R>(
    server: &super::Server,
    continuation: &BoundCommandContinuation,
    source: &crate::command::CommandSource,
    remaining_ast_digest: [u8; 32],
    execute: impl FnOnce() -> R,
) -> Result<R, super::cluster_player_commands::PlayerCommandSubmitError> {
    use super::cluster_player_commands::PlayerCommandSubmitError as Error;
    if continuation.remaining_ast_digest != remaining_ast_digest
        || !continuation.source_current(server, source)
    {
        return Err(Error::Unauthorized);
    }
    let slot = super::cluster_world_rules::disciplined_slot().ok_or(Error::PendingAuthority)?;
    if slot < continuation.original_slot || slot > continuation.expires_slot {
        return Err(Error::Unauthorized);
    }
    Ok(CONTINUATION.sync_scope(continuation.clone(), || {
        super::cluster_permissions::with_command_permissions_sync(
            continuation.captured.permissions.clone(),
            execute,
        )
    }))
}

pub(crate) fn attach_continuation(
    server: &super::Server,
    source: &crate::command::CommandSource,
    request: &mut PlayerCommandRequest,
) -> Result<(), super::cluster_player_commands::PlayerCommandSubmitError> {
    use super::cluster_player_commands::PlayerCommandSubmitError as Error;
    let Some(continuation) = CONTINUATION.try_with(Clone::clone).ok() else {
        return Ok(());
    };
    if !continuation.source_current(server, source) {
        return Err(Error::Unauthorized);
    }
    let slot = super::cluster_world_rules::disciplined_slot().ok_or(Error::PendingAuthority)?;
    let permissions = &continuation.captured.permissions;
    request.principal = permissions.principal.clone();
    request.permission_snapshot = Some(permissions.permission);
    let mut proof = pumpkin_cluster::command_continuation::CommandContinuationProof {
        origin: permissions.origin,
        principal: permissions.principal.clone(),
        permission: permissions.permission,
        source_session: permissions.source_session,
        original_correlation: continuation.captured.correlation,
        original_request_digest: continuation.captured.command_digest,
        predecessor: continuation.predecessor,
        original_slot: continuation.original_slot,
        expires_slot: continuation.expires_slot,
        remaining_ast_digest: continuation.remaining_ast_digest,
        issued_slot: slot,
        issued_command_digest: [0; 32],
        binding_digest: [0; 32],
    };
    proof.seal(request).map_err(|_| Error::InvalidInput)?;
    if !proof.matches(request) {
        return Err(Error::Unauthorized);
    }
    request.continuation = Some(Box::new(proof));
    Ok(())
}

fn completion_authority_readiness(
    source_current: bool,
    slot: Option<u64>,
    original_slot: u64,
    expires_slot: u64,
) -> pumpkin_command::deferred::CompletionReadiness {
    use pumpkin_command::deferred::CompletionReadiness;
    if !source_current {
        return CompletionReadiness::Denied;
    }
    match slot {
        None => CompletionReadiness::Pending,
        Some(slot) if slot < original_slot || slot > expires_slot => CompletionReadiness::Denied,
        Some(_) => CompletionReadiness::Ready,
    }
}

struct ContinuationCompletion {
    continuation: BoundCommandContinuation,
    source: std::sync::Arc<crate::command::CommandSource>,
}

impl ContinuationCompletion {
    fn dispatch(&self, deliver: impl FnOnce()) {
        CONTINUATION.sync_scope(self.continuation.clone(), || {
            super::cluster_permissions::with_command_permissions_sync(
                self.continuation.captured.permissions.clone(),
                deliver,
            )
        });
    }
}

struct ContinuationTerminal {
    continuation: BoundCommandContinuation,
    terminal: Box<dyn pumpkin_command::deferred::DeferredCompletionTerminal>,
}

impl pumpkin_command::deferred::DeferredCompletionTerminal for ContinuationTerminal {
    fn complete(
        self: Box<Self>,
        token: pumpkin_command::deferred::DeferredCommandToken,
        result: Option<pumpkin_command::source::ReturnValue>,
    ) {
        let Self {
            continuation,
            terminal,
        } = *self;
        if result.is_some() {
            CONTINUATION.sync_scope(continuation.clone(), || {
                super::cluster_permissions::with_command_permissions_sync(
                    continuation.captured.permissions,
                    || terminal.complete(token, result),
                )
            });
        } else {
            terminal.complete(token, None);
        }
    }
}

impl pumpkin_command::source::ReturnValueCallable for ContinuationCompletion {
    fn reserve_deferred_terminal(
        &self,
        token: pumpkin_command::deferred::DeferredCommandToken,
    ) -> Result<
        Option<Box<dyn pumpkin_command::deferred::DeferredCompletionTerminal>>,
        pumpkin_command::deferred::DeferredCompletionUnavailable,
    > {
        Ok(self
            .source
            .command_result_taker
            .reserve_deferred_terminal(token)?
            .map(|terminal| {
                Box::new(ContinuationTerminal {
                    continuation: self.continuation.clone(),
                    terminal,
                }) as Box<dyn pumpkin_command::deferred::DeferredCompletionTerminal>
            }))
    }

    fn call(&self, result: pumpkin_command::source::ReturnValue) {
        self.dispatch(|| self.source.command_result_taker.call(result));
    }

    fn call_deferred(
        &self,
        token: pumpkin_command::deferred::DeferredCommandToken,
        result: pumpkin_command::source::ReturnValue,
    ) {
        self.dispatch(|| {
            self.source
                .command_result_taker
                .call_deferred(token, result)
        });
    }

    fn complete_deferred_branch(&self, token: pumpkin_command::deferred::DeferredCommandToken) {
        self.source
            .command_result_taker
            .complete_deferred_branch(token);
    }

    fn deferred_completion_readiness(
        &self,
        token: pumpkin_command::deferred::DeferredCommandToken,
    ) -> pumpkin_command::deferred::CompletionReadiness {
        use pumpkin_command::deferred::CompletionReadiness;
        let Some(server) = self.source.server.as_ref() else {
            return CompletionReadiness::Denied;
        };
        let readiness = completion_authority_readiness(
            self.continuation.source_current(server, &self.source),
            super::cluster_world_rules::disciplined_slot(),
            self.continuation.original_slot,
            self.continuation.expires_slot,
        );
        if readiness != CompletionReadiness::Ready {
            return readiness;
        }
        self.source
            .command_result_taker
            .deferred_completion_readiness(token)
    }
}

pub(crate) fn capture_completion_source(
    source: std::sync::Arc<crate::command::CommandSource>,
) -> std::sync::Arc<crate::command::CommandSource> {
    let Some(continuation) = CONTINUATION.try_with(Clone::clone).ok() else {
        return source;
    };
    let callback = std::sync::Arc::new(ContinuationCompletion {
        continuation,
        source: source.clone(),
    });
    std::sync::Arc::new(
        (*source)
            .clone()
            .with_command_result_taker(pumpkin_command::source::ResultValueTaker(vec![callback])),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use pumpkin_cluster::player_command_control::CommandPrincipal;

    #[test]
    fn deferred_completion_retains_clock_gap_and_denies_retired_or_expired_source() {
        use pumpkin_command::deferred::CompletionReadiness;
        assert_eq!(
            completion_authority_readiness(true, None, 100, 200),
            CompletionReadiness::Pending
        );
        assert_eq!(
            completion_authority_readiness(false, None, 100, 200),
            CompletionReadiness::Denied
        );
        assert_eq!(
            completion_authority_readiness(true, Some(99), 100, 200),
            CompletionReadiness::Denied
        );
        assert_eq!(
            completion_authority_readiness(true, Some(201), 100, 200),
            CompletionReadiness::Denied
        );
        assert_eq!(
            completion_authority_readiness(true, Some(100), 100, 200),
            CompletionReadiness::Ready
        );
        assert_eq!(
            completion_authority_readiness(true, Some(200), 100, 200),
            CompletionReadiness::Ready
        );
    }

    #[test]
    fn retired_or_reused_source_cannot_continue_original_program() {
        let origin = PeerGeneration {
            server: ServerId(2),
            generation: 9,
        };
        let gid = GlobalPlayerId::new(origin.server, PlayerSlot(1));
        let principal = CommandPrincipal::Player {
            gid,
            uuid: [3; 16],
            permission_revision: 5,
        };
        let mut entry = pumpkin_cluster::presence::RemotePlayerEntry::new(
            gid,
            [3; 16],
            "issuer".into(),
            Vec::new(),
            0,
            true,
            true,
        );
        entry.source = Some(origin);
        entry.session = 7;
        assert!(qualified_source_matches(
            origin,
            &principal,
            7,
            Some(9),
            Some(&entry)
        ));
        assert!(!qualified_source_matches(
            origin,
            &principal,
            7,
            Some(9),
            None
        ));
        assert!(!qualified_source_matches(
            origin,
            &principal,
            7,
            Some(10),
            Some(&entry)
        ));
        entry.session = 8;
        assert!(!qualified_source_matches(
            origin,
            &principal,
            7,
            Some(9),
            Some(&entry)
        ));
        entry.session = 7;
        entry.uuid = [4; 16];
        assert!(!qualified_source_matches(
            origin,
            &principal,
            7,
            Some(9),
            Some(&entry)
        ));
    }
}
