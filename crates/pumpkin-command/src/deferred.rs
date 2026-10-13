use std::collections::BTreeMap;
use std::sync::Arc;

use crate::source::{CommandSource, ReturnValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeferredCommandToken(pub [u8; 16]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandExecution {
    Complete(i32),
    Deferred(DeferredCommandToken),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandExecutionSummary {
    pub completed_result: i32,
    pub pending: Vec<DeferredCommandToken>,
}

impl CommandExecutionSummary {
    pub fn include(&mut self, execution: CommandExecution, forked: bool) {
        match execution {
            CommandExecution::Complete(value) => {
                self.completed_result += if forked { 1 } else { value };
            }
            CommandExecution::Deferred(token) => self.pending.push(token),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionReadiness {
    Ready,
    Pending,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredCompletionUnavailable;

pub trait DeferredCompletionTerminal: Send {
    fn complete(self: Box<Self>, token: DeferredCommandToken, result: Option<ReturnValue>);
}

pub struct DeferredTerminalGroup(pub Vec<Box<dyn DeferredCompletionTerminal>>);

impl DeferredCompletionTerminal for DeferredTerminalGroup {
    fn complete(self: Box<Self>, token: DeferredCommandToken, result: Option<ReturnValue>) {
        for terminal in self.0 {
            terminal.complete(token, result);
        }
    }
}

pub struct DeferredCommandCompletion<S: CommandSource> {
    source: Arc<S>,
    token: DeferredCommandToken,
    terminal: Option<Box<dyn DeferredCompletionTerminal>>,
}

impl<S: CommandSource> DeferredCommandCompletion<S> {
    #[cfg(test)]
    fn new(source: Arc<S>, token: DeferredCommandToken) -> Self {
        Self {
            source,
            token,
            terminal: None,
        }
    }

    pub fn reserve(
        source: Arc<S>,
        token: DeferredCommandToken,
    ) -> Result<Self, DeferredCompletionUnavailable> {
        let terminal = source.reserve_deferred_terminal(token)?;
        Ok(Self {
            source,
            token,
            terminal,
        })
    }

    pub fn try_finish(self, result: ReturnValue) -> Result<(), (Self, ReturnValue)> {
        match self.source.deferred_completion_readiness(self.token) {
            CompletionReadiness::Ready => {
                self.source.call_deferred_result(self.token, result);
                if let Some(terminal) = self.terminal {
                    terminal.complete(self.token, Some(result));
                }
            }
            CompletionReadiness::Pending => return Err((self, result)),
            CompletionReadiness::Denied => {
                self.source.complete_deferred_branch(self.token);
                if let Some(terminal) = self.terminal {
                    terminal.complete(self.token, None);
                }
            }
        }
        Ok(())
    }
}

pub struct DeferredBranchCompletion<S: CommandSource> {
    source: Arc<S>,
    token: DeferredCommandToken,
    terminal: Option<Box<dyn DeferredCompletionTerminal>>,
}

impl<S: CommandSource> DeferredBranchCompletion<S> {
    #[cfg(test)]
    fn new(source: Arc<S>, token: DeferredCommandToken) -> Self {
        Self {
            source,
            token,
            terminal: None,
        }
    }

    pub fn reserve(
        source: Arc<S>,
        token: DeferredCommandToken,
    ) -> Result<Self, DeferredCompletionUnavailable> {
        let terminal = source.reserve_deferred_terminal(token)?;
        Ok(Self {
            source,
            token,
            terminal,
        })
    }

    pub fn finish(self) {
        self.source.complete_deferred_branch(self.token);
        if let Some(terminal) = self.terminal {
            terminal.complete(self.token, None);
        }
    }
}

pub struct PendingCommandCompletions<S: CommandSource> {
    capacity: usize,
    pending: BTreeMap<DeferredCommandToken, DeferredCommandCompletion<S>>,
    pending_results: BTreeMap<DeferredCommandToken, ReturnValue>,
}

impl<S: CommandSource> PendingCommandCompletions<S> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pending: BTreeMap::new(),
            pending_results: BTreeMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        token: DeferredCommandToken,
        completion: DeferredCommandCompletion<S>,
    ) -> Result<(), DeferredCommandCompletion<S>> {
        if self.pending.len() >= self.capacity
            || self.pending.contains_key(&token)
            || completion.token != token
        {
            return Err(completion);
        }
        self.pending.insert(token, completion);
        Ok(())
    }

    pub fn finish(&mut self, token: DeferredCommandToken, result: ReturnValue) -> bool {
        if self.pending_results.contains_key(&token) {
            return false;
        }
        let Some(completion) = self.pending.remove(&token) else {
            return false;
        };
        if let Err((completion, result)) = completion.try_finish(result) {
            self.pending.insert(token, completion);
            self.pending_results.insert(token, result);
        }
        true
    }

    pub fn retry_ready(&mut self) -> usize {
        let tokens = self.pending_results.keys().copied().collect::<Vec<_>>();
        let mut completed = 0;
        for token in tokens {
            let result = self.pending_results.remove(&token).unwrap();
            let completion = self.pending.remove(&token).unwrap();
            if let Err((completion, result)) = completion.try_finish(result) {
                self.pending.insert(token, completion);
                self.pending_results.insert(token, result);
            } else {
                completed += 1;
            }
        }
        completed
    }

    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::argument_builder::{ArgumentBuilder, CommandArgumentBuilder};
    use crate::context::command_context::CommandContext;
    use crate::node::dispatcher::CommandDispatcher;
    use crate::node::{CommandExecutor, CommandExecutorResult};
    use crate::source::{DummySource, ResultValueTaker, ReturnValueCallable};

    struct Capture(std::sync::mpsc::Sender<(Option<DeferredCommandToken>, ReturnValue)>);

    impl ReturnValueCallable for Capture {
        fn call(&self, value: ReturnValue) {
            self.0.send((None, value)).unwrap();
        }

        fn call_deferred(&self, token: DeferredCommandToken, value: ReturnValue) {
            self.0.send((Some(token), value)).unwrap();
        }
    }

    struct DeferredExecutor;

    impl CommandExecutor for DeferredExecutor {
        fn execute(&self, _: &CommandContext) -> CommandExecutorResult {
            panic!("deferred executor must use typed outcome")
        }

        fn execute_outcome(
            &self,
            _: &CommandContext,
        ) -> Result<CommandExecution, crate::errors::command_syntax_error::CommandSyntaxError>
        {
            Ok(CommandExecution::Deferred(DeferredCommandToken([7; 16])))
        }
    }

    #[test]
    fn deferred_admission_does_not_store_a_result_and_owned_completion_fires_once() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let source = DummySource {
            command_result_taker: ResultValueTaker(vec![Arc::new(Capture(sender))]),
            ..DummySource::default()
        };
        let mut dispatcher = CommandDispatcher::new();
        dispatcher
            .register(CommandArgumentBuilder::new("later", "later").executes(DeferredExecutor));
        let outcome = dispatcher.execute_input_outcome("later", &source).unwrap();
        assert_eq!(outcome.completed_result, 0);
        assert_eq!(outcome.pending, vec![DeferredCommandToken([7; 16])]);
        assert!(receiver.try_recv().is_err());
        let mut pending = PendingCommandCompletions::new(1);
        assert!(
            pending
                .insert(
                    outcome.pending[0],
                    DeferredCommandCompletion::new(Arc::new(source), outcome.pending[0])
                )
                .is_ok()
        );
        assert!(pending.finish(outcome.pending[0], ReturnValue::Success(42)));
        assert_eq!(
            receiver.try_recv().unwrap(),
            (Some(outcome.pending[0]), ReturnValue::Success(42))
        );
        assert!(!pending.finish(outcome.pending[0], ReturnValue::Failure));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn capacity_failure_returns_the_unconsumed_completion() {
        let source = Arc::new(DummySource::default());
        let mut pending = PendingCommandCompletions::new(0);
        assert!(
            pending
                .insert(
                    DeferredCommandToken([1; 16]),
                    DeferredCommandCompletion::new(source, DeferredCommandToken([1; 16]))
                )
                .is_err()
        );
        assert_eq!(pending.remaining_capacity(), 0);
    }
    struct BranchCapture(std::sync::mpsc::Sender<DeferredCommandToken>);

    impl ReturnValueCallable for BranchCapture {
        fn call(&self, _: ReturnValue) {
            panic!("branch completion must not store a command result");
        }

        fn complete_deferred_branch(&self, token: DeferredCommandToken) {
            self.0.send(token).unwrap();
        }
    }

    #[test]
    fn deferred_redirect_retains_parent_token_without_executing_leaf_or_storing_result() {
        use crate::context::command_context::ContextChain;
        use crate::node::{RedirectModifier, RedirectOutcome};
        let (sender, receiver) = std::sync::mpsc::channel();
        let source = Arc::new(DummySource {
            command_result_taker: ResultValueTaker(vec![Arc::new(BranchCapture(sender))]),
            ..DummySource::default()
        });
        let mut dispatcher = CommandDispatcher::new();
        dispatcher
            .register(CommandArgumentBuilder::new("later", "later").executes(DeferredExecutor));
        let parsed = dispatcher.parse_input("later", source.as_ref());
        let leaf = parsed.context.build("later");
        let mut modifier = leaf.clone();
        modifier.modifier = RedirectModifier::CustomOutcome(Arc::new(|_| {
            Ok(RedirectOutcome::Deferred(DeferredCommandToken([9; 16])))
        }));
        let chain = ContextChain::new(vec![Arc::new(modifier)], Arc::new(leaf));
        let outcome = chain
            .execute_all_outcome(&source, dispatcher.consumer.as_ref(), false)
            .unwrap();
        assert_eq!(outcome.completed_result, 0);
        assert_eq!(outcome.pending, vec![DeferredCommandToken([9; 16])]);
        assert!(receiver.try_recv().is_err());
        DeferredBranchCompletion::new(source, outcome.pending[0]).finish();
        assert_eq!(receiver.try_recv().unwrap(), outcome.pending[0]);
        assert!(receiver.try_recv().is_err());
    }
    #[derive(Clone)]
    struct ReadinessSource {
        readiness: CompletionReadiness,
        sender: std::sync::mpsc::Sender<Option<ReturnValue>>,
    }

    impl CommandSource for ReadinessSource {
        fn send_message(&self, _: pumpkin_util::text::TextComponent) {}
        fn deferred_completion_readiness(&self, _: DeferredCommandToken) -> CompletionReadiness {
            self.readiness
        }
        fn call_deferred_result(&self, _: DeferredCommandToken, result: ReturnValue) {
            self.sender.send(Some(result)).unwrap();
        }
        fn complete_deferred_branch(&self, _: DeferredCommandToken) {
            self.sender.send(None).unwrap();
        }
    }

    #[test]
    fn pending_completion_retains_exact_result_and_denial_only_completes_bookkeeping() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let token = DeferredCommandToken([5; 16]);
        let source = Arc::new(ReadinessSource {
            readiness: CompletionReadiness::Pending,
            sender: sender.clone(),
        });
        let completion = DeferredCommandCompletion::reserve(source, token).unwrap();
        let mut pending = PendingCommandCompletions::new(1);
        assert!(pending.insert(token, completion).is_ok());
        assert!(pending.finish(token, ReturnValue::Success(8)));
        assert!(!pending.finish(token, ReturnValue::Success(9)));
        assert_eq!(pending.remaining_capacity(), 0);
        assert_eq!(pending.retry_ready(), 0);
        assert!(receiver.try_recv().is_err());
        Arc::make_mut(&mut pending.pending.get_mut(&token).unwrap().source).readiness =
            CompletionReadiness::Ready;
        assert_eq!(pending.retry_ready(), 1);
        assert_eq!(receiver.try_recv().unwrap(), Some(ReturnValue::Success(8)));
        assert_eq!(pending.remaining_capacity(), 1);
        let source = Arc::new(ReadinessSource {
            readiness: CompletionReadiness::Denied,
            sender,
        });
        assert!(
            DeferredCommandCompletion::reserve(source, token)
                .unwrap()
                .try_finish(ReturnValue::Success(99))
                .is_ok()
        );
        assert_eq!(receiver.try_recv().unwrap(), None);
        assert!(receiver.try_recv().is_err());
    }

    #[derive(Clone)]
    struct NoTerminalCredit;

    impl CommandSource for NoTerminalCredit {
        fn send_message(&self, _: pumpkin_util::text::TextComponent) {}
        fn reserve_deferred_terminal(
            &self,
            _: DeferredCommandToken,
        ) -> Result<Option<Box<dyn DeferredCompletionTerminal>>, DeferredCompletionUnavailable>
        {
            Err(DeferredCompletionUnavailable)
        }
    }

    #[test]
    fn completion_reservation_fails_before_any_command_admission() {
        let source = Arc::new(NoTerminalCredit);
        let token = DeferredCommandToken([6; 16]);
        assert!(DeferredCommandCompletion::reserve(source.clone(), token).is_err());
        assert!(DeferredBranchCompletion::reserve(source, token).is_err());
    }
}
