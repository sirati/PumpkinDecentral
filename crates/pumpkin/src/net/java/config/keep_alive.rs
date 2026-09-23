#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_config_keep_alive(&self, keep_alive: &SKeepAlive) {
        let pending = self.pending_keep_alives.load();
        if pending
            .iter()
            .any(|(id, _)| *id == keep_alive.keep_alive_id)
        {
            self.pending_keep_alives.rcu(|current| {
                let mut pending = (**current).clone();
                if let Some(pos) = pending
                    .iter()
                    .position(|(id, _)| *id == keep_alive.keep_alive_id)
                {
                    pending.swap_remove(pos);
                }
                pending
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
