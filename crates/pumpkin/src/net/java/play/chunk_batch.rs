#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_chunk_batch(&self, player: &Player, packet: &SChunkBatch) {
        let epoch = player
            .chunk_send_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let result = player
            .chunk_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .on_batch_acknowledged(packet.chunks_per_tick, epoch);
        if let crate::net::chunk_sender::BatchAckResult::RevisionMismatch { expected, received } =
            result
        {
            warn!(
                expected,
                received, "chunk batch acknowledgement revision mismatch"
            );
            self.try_kick(&TextComponent::text("Chunk delivery revision mismatch"));
            return;
        }
        trace!(
            "Client requested {} chunks per tick",
            packet.chunks_per_tick
        );
    }
}
