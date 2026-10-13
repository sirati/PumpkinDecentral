#[allow(clippy::wildcard_imports)]
use super::*;

impl BedrockClient {
    pub fn handle_request_chunk_radius(&self, player: &Arc<Player>, packet: &SRequestChunkRadius) {
        let chunk_radius = packet.chunk_radius;
        if chunk_radius.0 < 1 {
            self.try_kick(
                DisconnectReason::Kicked,
                "Cannot have zero or negative view distance!".to_string(),
            );
            return;
        }
        let Some(server) = player.world().server.upgrade() else {
            return;
        };

        let maximum = NonZero::<i32>::from(server.advanced_config.networking.bedrock.view_distance)
            .get()
            .max(2);
        let maximum = if server.advanced_config.cluster.enabled {
            maximum.min(i32::from(pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE))
        } else {
            maximum
        };
        let view_distance = chunk_radius.clamp(2, maximum);

        self.try_enqueue_client_packet(&CChunkRadiusUpdated {
            chunk_radius: VarInt(view_distance),
        });

        if server.advanced_config.cluster.enabled {
            let mut config = (**player.config.load()).clone();
            config.view_distance = NonZero::new(
                (view_distance as u8).min(pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE),
            )
            .expect("validated chunk radius is positive");
            if crate::server::cluster_session_ingress::stage(
                player,
                crate::server::cluster_session_ingress::StagedSessionInput::Settings {
                    config,
                    received_at: std::time::Instant::now(),
                },
            )
            .is_err()
            {
                self.try_kick(
                    DisconnectReason::Kicked,
                    "Cluster settings queue is full".to_string(),
                );
            }
            return;
        }

        let old_view_distance = {
            let current_config = player.config.load();
            let old_vd = current_config.view_distance;
            let mut new_config = (**current_config).clone();

            new_config.view_distance =
                NonZero::new(view_distance as u8).unwrap_or(NonZero::<u8>::MIN);
            player.config.store(std::sync::Arc::new(new_config));

            old_vd
        };

        debug!(
            "Player {} updated their render distance: {} -> {}.",
            player.gameprofile.name, old_view_distance, view_distance
        );
        chunker::update_position(player);
    }
}
