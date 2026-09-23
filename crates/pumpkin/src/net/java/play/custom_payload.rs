#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_play_custom_payload(
        &self,
        server: &Arc<Server>,
        player: &Arc<Player>,
        channel: &str,
        data: &[u8],
    ) {
        let server = server.clone();
        let player = player.clone();
        let channel = channel.to_string();
        let data = bytes::Bytes::copy_from_slice(data);
        server.clone().spawn_task(async move {
            let mut event = crate::plugin::player::player_custom_payload::PlayerCustomPayloadEvent::new(
                player.clone(),
                channel.clone(),
                data.clone(),
            );
            server.plugin_manager.fire(&server, &mut event).await;
            let Ok(channels) = std::str::from_utf8(&data) else {
                return;
            };
            if channel == "minecraft:register" {
                for channel in channels.split('\0').filter(|channel| !channel.is_empty()) {
                    let mut register = crate::plugin::api::events::player::player_register_channel::PlayerRegisterChannelEvent::new(
                        player.clone(),
                        channel.to_string(),
                    );
                    server.plugin_manager.fire(&server, &mut register).await;
                    let mut channel_event =
                        crate::plugin::api::events::player::player_channel::PlayerChannelEvent {
                            player: player.clone(),
                            channel: channel.to_string(),
                            cancelled: false,
                        };
                    server.plugin_manager.fire(&server, &mut channel_event).await;
                }
            } else if channel == "minecraft:unregister" {
                for channel in channels.split('\0').filter(|channel| !channel.is_empty()) {
                    let mut unregister = crate::plugin::api::events::player::player_unregister_channel::PlayerUnregisterChannelEvent::new(
                        player.clone(),
                        channel.to_string(),
                    );
                    server.plugin_manager.fire(&server, &mut unregister).await;
                }
            }
        });
    }
}
