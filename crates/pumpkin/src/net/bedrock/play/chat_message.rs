#[allow(clippy::wildcard_imports)]
use super::*;

impl BedrockClient {
    pub async fn handle_chat_message(
        &self,
        server: &Arc<Server>,
        player: &Arc<Player>,
        packet: SText<'_>,
    ) {
        player.update_last_action_time();
        if server.advanced_config.cluster.enabled && !server.basic_config.allow_chat_reports {
            if crate::server::cluster_session_ingress::stage(
                player,
                crate::server::cluster_session_ingress::StagedSessionInput::Bedrock {
                    body: packet.message.into_owned(),
                    xuid: packet.xuid.into_owned(),
                    filtered: packet.filtered_message.map(std::borrow::Cow::into_owned),
                    received_at: std::time::Instant::now(),
                },
            )
            .is_err()
            {
                player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
            }
            return;
        }
        if player.check_chat_spam(server, crate::entity::player::SpamType::Chat) {
            return;
        }
        let gameprofile = &player.gameprofile;

        send_cancellable! {{
            server;
            PlayerChatEvent::new(player.clone(), packet.message.into_owned(), vec![], None);

            'after: {
                info!("<chat> {}: {}", gameprofile.name, event.message);

                let config = &server.advanced_config;

                let message = match seasonal_events::modify_chat_message(&event.message, config) {
                    Some(m) => m,
                    None => event.message.clone(),
                };

                let accepted = crate::server::cluster_chat_out::broadcast_public_chat_from_player(
                    server, player, &message,
                );

                if !accepted {
                    player.send_system_message(&TextComponent::text("Cluster chat queue is full"));
                } else {

                let decorated_message = TextComponent::chat_decorated(
                    &config.chat.format,
                    &gameprofile.name,
                    &message,
                );

                let entity = &player.get_entity();
                if server.basic_config.allow_chat_reports {
                    //TODO Alex help, what is this?
                    //world.broadcast_secure_player_chat(player, &message, decorated_message).await;
                } else {
                    let je_packet = CSystemChatMessage::new(
                        &decorated_message,
                        false,
                    );

                    let be_packet = SText::chat_with_xuid(
                        message,
                        gameprofile.name.clone(),
                        packet.xuid.into_owned(),
                        packet.filtered_message.map(std::borrow::Cow::into_owned),
                    );

                    entity.world.load().broadcast_editioned(&je_packet, &be_packet);
                }
                }
            }
        }}
    }
}
