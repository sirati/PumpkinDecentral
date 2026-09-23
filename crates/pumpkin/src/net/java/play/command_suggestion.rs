#[allow(clippy::wildcard_imports)]
use super::*;

impl JavaClient {
    pub fn handle_command_suggestion(
        &self,
        player: &Arc<Player>,
        packet: &SCommandSuggestion<'_>,
        server: &Arc<Server>,
    ) {
        if server.advanced_config.cluster.enabled {
            let _ = crate::server::cluster_command_suggestions::submit(
                server,
                player.client.clone(),
                player.get_command_source(server),
                packet.command.to_string(),
                packet.id,
            );
            return;
        }
        let Some(cmd) = &packet.command.get(1..) else {
            return;
        };

        let last_word_start = cmd
            .char_indices()
            .rfind(|(_, c)| c.is_whitespace())
            .map_or(0, |(start, value)| start + value.len_utf8());

        let suggestions = server
            .command_dispatcher
            .load()
            .suggest(cmd, &player.get_command_source(server));

        let response = CCommandSuggestions::new(
            packet.id,
            ((last_word_start + 1) as i32).into(),
            ((cmd.len() - last_word_start) as i32).into(),
            suggestions.into(),
        );

        player.try_send_client_packet(&response);
    }
}
