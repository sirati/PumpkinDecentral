#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_chunk_batch(&self, player: &Player, packet: &SChunkBatch) {
        self.note_chunk_batch_acknowledgement();
        if let Some((position, yaw, pitch)) = self.take_handoff_teleport() {
            player.living_entity.entity.no_physics.store(
                player.gamemode.load() == pumpkin_util::GameMode::Spectator,
                std::sync::atomic::Ordering::Release,
            );
            player.request_teleport(position, yaw, pitch);
        }
        player
            .chunk_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .on_batch_acknowledged(packet.chunks_per_tick);
        trace!(
            "Client requested {} chunks per tick",
            packet.chunks_per_tick
        );
    }
}
