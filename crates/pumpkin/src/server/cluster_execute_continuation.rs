use super::{
    Server,
    cluster_player_commands::{CommandTerminal, PlayerCommandSubmitError},
};
use crate::command::{CommandSource, context::CommandContext};
use pumpkin_cluster::mob_initialization::EntityAdmissionRequest;
use pumpkin_command::context::command_context::{ContextChain, ParsedArgument, ParsedNode};
use pumpkin_command::context::string_range::StringRange;
use pumpkin_command::deferred::{DeferredBranchCompletion, DeferredCommandToken};
use pumpkin_command::node::{Command, RedirectModifier, attached::NodeId, tree::Tree};
use std::collections::BTreeSet;
use std::sync::Arc;

pub(crate) struct BranchCompletionEvent {
    pub(crate) parent: DeferredCommandToken,
    pub(crate) child: DeferredCommandToken,
}

struct BranchRelay {
    parent: DeferredCommandToken,
    events: tokio::sync::mpsc::Sender<BranchCompletionEvent>,
}

struct ReservedBranchTerminal {
    parent: DeferredCommandToken,
    permit: tokio::sync::mpsc::OwnedPermit<BranchCompletionEvent>,
}

impl pumpkin_command::deferred::DeferredCompletionTerminal for ReservedBranchTerminal {
    fn complete(
        self: Box<Self>,
        child: DeferredCommandToken,
        _: Option<pumpkin_command::source::ReturnValue>,
    ) {
        self.permit.send(BranchCompletionEvent {
            parent: self.parent,
            child,
        });
    }
}

impl pumpkin_command::source::ReturnValueCallable for BranchRelay {
    fn call(&self, _: pumpkin_command::source::ReturnValue) {}
    fn call_deferred(&self, _: DeferredCommandToken, _: pumpkin_command::source::ReturnValue) {}
    fn reserve_deferred_terminal(
        &self,
        _: DeferredCommandToken,
    ) -> Result<
        Option<Box<dyn pumpkin_command::deferred::DeferredCompletionTerminal>>,
        pumpkin_command::deferred::DeferredCompletionUnavailable,
    > {
        let permit = self
            .events
            .clone()
            .try_reserve_owned()
            .map_err(|_| pumpkin_command::deferred::DeferredCompletionUnavailable)?;
        Ok(Some(Box::new(ReservedBranchTerminal {
            parent: self.parent,
            permit,
        })))
    }
}

struct OwnedContext {
    input: String,
    arguments: rustc_hash::FxHashMap<String, Arc<ParsedArgument>>,
    root: NodeId,
    nodes: Vec<ParsedNode>,
    range: StringRange,
    child: Option<Box<OwnedContext>>,
    modifier: RedirectModifier<CommandSource>,
    forks: bool,
    command: Option<Command<CommandSource>>,
}

impl OwnedContext {
    fn capture(context: &CommandContext<'_>, remaining: usize) -> Option<Self> {
        if remaining == 0
            || context.input.len() > 32768
            || context.nodes.len() > 256
            || context.arguments.len() > 256
        {
            return None;
        }
        Some(Self {
            input: context.input.clone(),
            arguments: context.arguments.clone(),
            root: context.root,
            nodes: context.nodes.clone(),
            range: context.range,
            modifier: context.modifier.clone(),
            forks: context.forks,
            command: context.command.clone(),
            child: match context.child.as_ref() {
                Some(child) => Some(Box::new(Self::capture(child, remaining - 1)?)),
                None => None,
            },
        })
    }

    fn digest_into(&self, digest: &mut sha2::Sha256) {
        use sha2::Digest;
        digest.update((self.input.len() as u64).to_le_bytes());
        digest.update(self.input.as_bytes());
        digest.update((self.root.0.get() as u64).to_le_bytes());
        digest.update((self.range.start as u64).to_le_bytes());
        digest.update((self.range.end as u64).to_le_bytes());
        digest.update([u8::from(self.forks), u8::from(self.command.is_some())]);
        digest.update((self.nodes.len() as u64).to_le_bytes());
        for node in &self.nodes {
            digest.update((node.node.0.get() as u64).to_le_bytes());
            digest.update((node.range.start as u64).to_le_bytes());
            digest.update((node.range.end as u64).to_le_bytes());
        }
        let mut arguments = self.arguments.iter().collect::<Vec<_>>();
        arguments.sort_unstable_by_key(|(name, _)| name.as_str());
        digest.update((arguments.len() as u64).to_le_bytes());
        for (name, argument) in arguments {
            digest.update((name.len() as u64).to_le_bytes());
            digest.update(name.as_bytes());
            digest.update((argument.range.start as u64).to_le_bytes());
            digest.update((argument.range.end as u64).to_le_bytes());
        }
        digest.update([u8::from(self.child.is_some())]);
        if let Some(child) = &self.child {
            child.digest_into(digest);
        }
    }

    fn restore<'a>(
        &self,
        tree: &'a Tree<CommandSource>,
        source: &Arc<CommandSource>,
    ) -> CommandContext<'a> {
        CommandContext {
            source: source.clone(),
            input: self.input.clone(),
            arguments: self.arguments.clone(),
            tree,
            root: self.root,
            nodes: self.nodes.clone(),
            range: self.range,
            child: self
                .child
                .as_ref()
                .map(|child| Arc::new(child.restore(tree, source))),
            modifier: self.modifier.clone(),
            forks: self.forks,
            forked_execution: false,
            command: self.command.clone(),
        }
    }
}

pub(crate) struct RetainedCommandProgram {
    tree: Arc<Tree<CommandSource>>,
    context: OwnedContext,
}

impl RetainedCommandProgram {
    pub(crate) fn capture(context: &CommandContext<'_>) -> Option<Self> {
        ContextChain::try_flatten(context)?;
        Some(Self {
            tree: Arc::new(context.tree.clone()),
            context: OwnedContext::capture(context, 128)?,
        })
    }

    pub(crate) fn digest(&self, forked: bool) -> [u8; 32] {
        use sha2::Digest;
        let mut digest = sha2::Sha256::new();
        digest.update(b"PumpkinRetainedCommandProgram01");
        digest.update([u8::from(forked)]);
        self.context.digest_into(&mut digest);
        digest.finalize().into()
    }

    pub(crate) fn execute(
        &self,
        source: &Arc<CommandSource>,
        forked: bool,
        consumer: &dyn pumpkin_command::dispatcher::ResultConsumer<CommandSource>,
    ) -> Option<Result<i32, pumpkin_command::errors::command_syntax_error::CommandSyntaxError>>
    {
        let context = self.context.restore(&self.tree, source);
        Some(
            ContextChain::try_flatten(&context)?
                .execute_all_with_forked_mode(source, consumer, forked),
        )
    }
}

pub(crate) struct SummonContinuation {
    pub(crate) parent_token: DeferredCommandToken,
    branch: Option<DeferredBranchCompletion<CommandSource>>,
    waiting: Option<BTreeSet<DeferredCommandToken>>,
    pub(crate) correlation: [u8; 16],
    request: EntityAdmissionRequest,
    authority: Option<super::cluster_command_issuance::CapturedCommandAuthority>,
    pub(crate) ast_digest: [u8; 32],
    pub(crate) receipt: Option<pumpkin_cluster::issuer_entity_admission::CommandAdmissionReceipt>,
    tree: Arc<Tree<CommandSource>>,
    context: OwnedContext,
    source: Arc<CommandSource>,
    forked: bool,
    _reservation: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) fn submit(
    server: &Server,
    context: &CommandContext<'_>,
    request: EntityAdmissionRequest,
) -> Result<DeferredCommandToken, PlayerCommandSubmitError> {
    let reservation = server.cluster_player_commands.reserve_continuation()?;
    let parent_token = DeferredCommandToken(*uuid::Uuid::new_v4().as_bytes());
    let branch = DeferredBranchCompletion::reserve(context.source.clone(), parent_token)
        .map_err(|_| PlayerCommandSubmitError::QueueFull)?;
    let child = context
        .child
        .as_ref()
        .ok_or(PlayerCommandSubmitError::InvalidInput)?;
    let continuation =
        OwnedContext::capture(child, 128).ok_or(PlayerCommandSubmitError::InvalidInput)?;
    use sha2::Digest;
    let mut digest = sha2::Sha256::new();
    digest.update(b"PumpkinExecuteContinuation01");
    digest.update([u8::from(context.forked_execution)]);
    continuation.digest_into(&mut digest);
    let ast_digest = digest.finalize().into();
    let tree = Arc::new(context.tree.clone());
    let (correlation, authority) = match super::cluster_player_commands::submit_summon_captured(
        server,
        &context.source,
        request.clone(),
        None,
        Some(ast_digest),
    ) {
        Ok((correlation, authority)) => (correlation, Some(authority)),
        Err(PlayerCommandSubmitError::PendingAuthority) => (*uuid::Uuid::new_v4().as_bytes(), None),
        Err(error) => return Err(error),
    };
    server
        .cluster_player_commands
        .enqueue_continuation(SummonContinuation {
            parent_token,
            branch: Some(branch),
            waiting: None,
            correlation,
            request,
            authority,
            ast_digest,
            receipt: None,
            tree,
            context: continuation,
            source: context.source.clone(),
            forked: context.forked_execution,
            _reservation: reservation,
        });
    Ok(parent_token)
}

impl SummonContinuation {
    pub(crate) fn child_completed(&mut self, token: DeferredCommandToken) {
        if let Some(waiting) = &mut self.waiting {
            waiting.remove(&token);
        }
    }

    fn finish_branch(&mut self) {
        if let Some(branch) = self.branch.take() {
            branch.finish();
        }
    }

    pub(crate) fn advance(&mut self, server: &Server, terminal: Option<CommandTerminal>) -> bool {
        use pumpkin_command::source::{CommandSource as _, ReturnValue};
        if let Some(waiting) = &self.waiting {
            if !waiting.is_empty() {
                return false;
            }
            self.finish_branch();
            return true;
        }
        if self.authority.is_none() {
            let source_closed = match &self.source.output {
                crate::command::CommandSender::Player(player)
                | crate::command::CommandSender::Lobby(player) => player.client.closed(),
                crate::command::CommandSender::PreJoin(session) => session.client.closed(),
                _ => false,
            };
            if source_closed {
                self.source.call_result(ReturnValue::Failure);
                self.finish_branch();
                return true;
            }
            match super::cluster_player_commands::submit_summon_captured(
                server,
                &self.source,
                self.request.clone(),
                None,
                Some(self.ast_digest),
            ) {
                Ok((correlation, authority)) => {
                    self.correlation = correlation;
                    self.authority = Some(authority);
                }
                Err(
                    PlayerCommandSubmitError::PendingAuthority
                    | PlayerCommandSubmitError::QueueFull,
                ) => return false,
                Err(_) => {
                    self.source.call_result(ReturnValue::Failure);
                    self.finish_branch();
                    return true;
                }
            }
            return false;
        }
        let Some(terminal) = terminal else {
            return false;
        };
        if !terminal.applied {
            self.source.call_result(ReturnValue::Failure);
            self.finish_branch();
            return true;
        }
        let Some(reference) = terminal.action else {
            self.source.call_result(ReturnValue::Failure);
            self.finish_branch();
            return true;
        };
        let Some(receipt) = &self.receipt else {
            return false;
        };
        if receipt.reference != reference || receipt.issuance.correlation != self.correlation {
            self.source.call_result(ReturnValue::Failure);
            self.finish_branch();
            return true;
        }
        let bound = match super::cluster_command_issuance::bind_continuation(
            self.authority.as_ref().unwrap(),
            receipt,
            self.ast_digest,
        ) {
            Ok(bound) => bound,
            Err(PlayerCommandSubmitError::PendingAuthority) => return false,
            Err(_) => {
                self.source.call_result(ReturnValue::Failure);
                self.finish_branch();
                return true;
            }
        };
        match super::cluster_command_issuance::with_continuation(
            server,
            &bound,
            &self.source,
            self.ast_digest,
            || (),
        ) {
            Ok(()) => {}
            Err(PlayerCommandSubmitError::PendingAuthority) => return false,
            Err(_) => {
                self.source.call_result(ReturnValue::Failure);
                self.finish_branch();
                return true;
            }
        }
        let entity = match super::cluster_owned_mob_admission::accepted_birth(
            server,
            reference,
            &self.request,
        ) {
            Ok(Some((_, entity))) => entity,
            Ok(None) => return false,
            Err(error) => {
                tracing::error!(
                    ?reference,
                    ?error,
                    "cluster execute summon birth proof failed"
                );
                self.source.call_result(ReturnValue::Failure);
                self.finish_branch();
                return true;
            }
        };
        let mut source = self.source.as_ref().clone();
        source.entity = Some(entity);
        source.command_result_taker.0.push(Arc::new(BranchRelay {
            parent: self.parent_token,
            events: server.cluster_player_commands.branch_events.clone(),
        }));
        let source = Arc::new(source);
        let context = self.context.restore(&self.tree, &source);
        let Some(chain) = ContextChain::try_flatten(&context) else {
            source.call_result(ReturnValue::Failure);
            self.finish_branch();
            return true;
        };
        match super::cluster_command_issuance::with_continuation(
            server,
            &bound,
            &source,
            self.ast_digest,
            || {
                chain.execute_all_outcome(
                    &source,
                    &pumpkin_command::dispatcher::ResultDeferrer,
                    self.forked,
                )
            },
        ) {
            Ok(Ok(summary)) => {
                let waiting = summary.pending.into_iter().collect::<BTreeSet<_>>();
                if !waiting.is_empty() {
                    self.waiting = Some(waiting);
                    return false;
                }
            }
            Ok(Err(error)) => source.send_error(error.message),
            Err(PlayerCommandSubmitError::PendingAuthority) => return false,
            Err(_) => source.call_result(ReturnValue::Failure),
        }
        self.finish_branch();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_command::{ArgumentBuilder, CommandArgumentBuilder};
    #[test]
    fn branch_terminal_reserves_capacity_before_submission_and_consumes_once() {
        use pumpkin_command::source::{ReturnValue, ReturnValueCallable};
        let (events, mut received) = tokio::sync::mpsc::channel(1);
        let parent = DeferredCommandToken([1; 16]);
        let relay = BranchRelay { parent, events };
        let child = DeferredCommandToken([2; 16]);
        let terminal = relay.reserve_deferred_terminal(child).unwrap().unwrap();
        assert!(relay.reserve_deferred_terminal(child).is_err());
        relay.call_deferred(child, ReturnValue::Success(7));
        assert!(received.try_recv().is_err());
        terminal.complete(child, Some(ReturnValue::Failure));
        let event = received.try_recv().unwrap();
        assert_eq!(event.parent, parent);
        assert_eq!(event.child, child);
        assert!(received.try_recv().is_err());
        assert!(relay.reserve_deferred_terminal(child).is_ok());
    }

    struct Seven;
    impl pumpkin_command::node::CommandExecutor<CommandSource> for Seven {
        fn execute(
            &self,
            context: &CommandContext<'_>,
        ) -> pumpkin_command::node::CommandExecutorResult {
            assert_eq!(context.input, "seven");
            assert_eq!(context.nodes.len(), 1);
            Ok(7)
        }
    }

    #[test]
    fn retained_ast_executes_after_original_dispatcher_is_dropped() {
        let source = Arc::new(CommandSource::dummy());
        let (owned, tree) = {
            let mut dispatcher =
                pumpkin_command::dispatcher::CommandDispatcher::<CommandSource>::new();
            dispatcher.register(CommandArgumentBuilder::new("seven", "test").executes(Seven));
            let parsed = dispatcher.parse_input("seven", &source);
            let context = parsed.context.build("seven");
            (
                OwnedContext::capture(&context, 128).unwrap(),
                Arc::new(context.tree.clone()),
            )
        };
        let restored = owned.restore(&tree, &source);
        let chain = ContextChain::try_flatten(&restored).unwrap();
        assert_eq!(
            chain.execute_all_with_forked_mode(
                &source,
                &pumpkin_command::dispatcher::EmptyResultConsumer,
                false
            ),
            Ok(7)
        );
        assert_eq!(
            chain.execute_all_with_forked_mode(
                &source,
                &pumpkin_command::dispatcher::EmptyResultConsumer,
                true
            ),
            Ok(1)
        );
    }
}
