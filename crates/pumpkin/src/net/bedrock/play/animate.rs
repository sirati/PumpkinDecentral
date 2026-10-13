#[allow(clippy::wildcard_imports)]
use super::*;

impl BedrockClient {
    pub fn handle_animate(&self, player: &Arc<Player>, packet: &SAnimate) {
        if !player.has_client_loaded() {
            return;
        }
        if player.cluster_gid().is_some() {
            if packet.action == AnimateAction::SwingArm
                && let Some(server) = player.world().server.upgrade()
            {
                crate::server::cluster_remote_player_presentation::submit_cosmetic(
                    &server,
                    player,
                    crate::server::cluster_remote_player_presentation::CosmeticChange::Swing(
                        pumpkin_cluster::replica_visual::SwingArm::Main,
                    ),
                );
            }
            return;
        }

        let entity = &player.get_entity();
        let world = entity.world.load();

        let java_animation = match packet.action {
            AnimateAction::NoAction => None,
            AnimateAction::SwingArm => Some(Animation::SwingMainArm),
            AnimateAction::WakeUp => Some(Animation::LeaveBed),
            AnimateAction::CriticalHit => Some(Animation::CriticalEffect),
            AnimateAction::MagicCriticalHit => Some(Animation::MagicCriticaleffect),
        };

        if let Some(animation) = java_animation {
            let je_packet = CEntityAnimation::new(VarInt(entity.entity_id), animation);
            let be_packet = SAnimate {
                action: packet.action,
                target_actor_runtime_id: VarULong(entity.entity_id as u64),
                data: 0.0,
                swing_source: None,
            };
            world.broadcast_editioned(&je_packet, &be_packet);
        }
    }
}
