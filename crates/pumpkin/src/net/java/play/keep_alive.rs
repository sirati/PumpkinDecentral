#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_keep_alive(&self, player: &Player, keep_alive: &SKeepAlive) {
        let pending = self.pending_keep_alives.load();
        if let Some((_, send_time)) = pending
            .iter()
            .find(|(id, _)| *id == keep_alive.keep_alive_id)
        {
            let send_time = *send_time;
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
            let ping = send_time.elapsed().as_millis() as u32;
            // Vanilla logic
            player.ping.store(
                (player.ping.load(Ordering::Relaxed) * 3 + ping) / 4,
                Ordering::Relaxed,
            );
            self.wait_for_keep_alive.store(false, Ordering::Relaxed);
        } else if keep_alive.keep_alive_id == self.keep_alive_id.load() {
            let ping = self.last_keep_alive_time.load().elapsed().as_millis() as u32;
            player.ping.store(
                (player.ping.load(Ordering::Relaxed) * 3 + ping) / 4,
                Ordering::Relaxed,
            );
            self.wait_for_keep_alive.store(false, Ordering::Relaxed);
        } else {
            debug!(
                "Ignored unexpected or duplicate keep alive id {} from player {}",
                keep_alive.keep_alive_id, player.gameprofile.name
            );
        }
    }
}
