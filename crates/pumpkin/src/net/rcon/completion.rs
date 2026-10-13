use std::collections::BTreeSet;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use pumpkin_command::deferred::DeferredCommandToken;
use pumpkin_command::source::{ReturnValue, ReturnValueCallable};
use tokio::sync::mpsc;

pub(super) struct CompletionEvent {
    pub sequence: u64,
    pub token: DeferredCommandToken,
}

pub(super) struct CompletionSink {
    pub sequence: u64,
    pub events: mpsc::Sender<CompletionEvent>,
    pub lost_receipts: Arc<AtomicU64>,
}

impl CompletionSink {
    fn terminal(&self, token: DeferredCommandToken) {
        if self
            .events
            .try_send(CompletionEvent {
                sequence: self.sequence,
                token,
            })
            .is_err()
        {
            self.lost_receipts.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ReturnValueCallable for CompletionSink {
    fn call(&self, _: ReturnValue) {}

    fn call_deferred(&self, token: DeferredCommandToken, _: ReturnValue) {
        self.terminal(token);
    }

    fn complete_deferred_branch(&self, token: DeferredCommandToken) {
        self.terminal(token);
    }
}

pub(super) struct PendingResponse {
    pub sequence: u64,
    pub packet_id: i32,
    pub output: Arc<crate::command::RconOutput>,
    pub pending: BTreeSet<DeferredCommandToken>,
    pub lost_receipts: Arc<AtomicU64>,
}

impl PendingResponse {
    pub fn complete(&mut self, event: &CompletionEvent) {
        if self.sequence == event.sequence {
            self.pending.remove(&event.token);
        }
    }

    pub fn ready(&self) -> bool {
        self.pending.is_empty() || self.lost_receipts.load(Ordering::Acquire) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_rcon_tokens_ignore_immediate_results_child_tokens_and_other_requests() {
        let (tx, mut rx) = mpsc::channel(2);
        let lost_receipts = Arc::new(AtomicU64::new(0));
        let parent = DeferredCommandToken([1; 16]);
        let child = DeferredCommandToken([2; 16]);
        let sink = CompletionSink {
            sequence: 7,
            events: tx,
            lost_receipts: lost_receipts.clone(),
        };
        let source = crate::command::CommandSource::dummy().with_command_result_taker(
            pumpkin_command::source::ResultValueTaker(vec![Arc::new(CompletionSink {
                sequence: 7,
                events: sink.events.clone(),
                lost_receipts: sink.lost_receipts.clone(),
            })]),
        );
        let mut response = PendingResponse {
            sequence: 7,
            packet_id: 42,
            output: Arc::new(crate::command::RconOutput::default()),
            pending: BTreeSet::from([parent]),
            lost_receipts,
        };
        pumpkin_command::source::CommandSource::call_result(&source, ReturnValue::Success(1));
        assert!(rx.try_recv().is_err());
        pumpkin_command::source::CommandSource::call_deferred_result(
            &source,
            child,
            ReturnValue::Success(5),
        );
        response.complete(&rx.try_recv().unwrap());
        assert!(!response.ready());
        response.complete(&CompletionEvent {
            sequence: 8,
            token: parent,
        });
        assert!(!response.ready());
        pumpkin_command::source::CommandSource::complete_deferred_branch(&source, parent);
        response.complete(&rx.try_recv().unwrap());
        assert!(response.ready());
        pumpkin_command::source::CommandSource::complete_deferred_branch(&source, parent);
        response.complete(&rx.try_recv().unwrap());
        assert!(response.ready());
    }
}
