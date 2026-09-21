#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_config_keep_alive(&self, keep_alive: &SKeepAlive) {
        let pending = self.pending_keep_alives.load_full();
        if pending
            .iter()
            .any(|(id, _)| *id == keep_alive.keep_alive_id)
        {
            self.pending_keep_alives.rcu(|current| {
                current
                    .iter()
                    .filter(|(id, _)| *id != keep_alive.keep_alive_id)
                    .copied()
                    .collect::<Vec<_>>()
            });
            self.wait_for_keep_alive.store(false, Ordering::Relaxed);
        } else if keep_alive.keep_alive_id == self.keep_alive_id.load() {
            self.wait_for_keep_alive.store(false, Ordering::Relaxed);
        } else {
            debug!(
                "Ignored unexpected config keep alive id {}",
                keep_alive.keep_alive_id
            );
        }
    }
}
