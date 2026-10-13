#[allow(clippy::wildcard_imports)]
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::time::TickStamp;

impl JavaClient {
    pub fn handle_attack(&self, player: &Arc<Player>, attack: &SAttack, server: &Arc<Server>) {
        if !player.has_client_loaded() {
            return;
        }
        player.update_last_action_time();
        let entity_id = attack.entity_id;
        if server.advanced_config.cluster.enabled {
            stage_cluster_attack(player, server, entity_id.0, false);
            return;
        }
        let player_entity = &player.get_entity();
        let world = player_entity.world.load_full();

        let config = &server.advanced_config.pvp;
        if !config.enabled {
            return;
        }

        if entity_id.0 == player.entity_id() {
            self.try_kick(&TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                [],
            ));
            return;
        }

        let player_target = world.get_player_by_id(entity_id.0);
        let target: Option<Arc<dyn EntityBase>> = player_target
            .as_ref()
            .map(|p| Arc::clone(p) as Arc<dyn EntityBase>)
            .or_else(|| world.get_entity_by_id(entity_id.0));
        let Some(target) = target else {
            self.try_kick(&TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                [],
            ));
            return;
        };
        let replica_gid = crate::server::cluster_ghost::remote_player_gid(target.as_ref());
        if let Some(player_victim) = &player_target {
            if player_victim.living_entity.health.load() <= 0.0 {
                return;
            }
            if config.protect_creative && player_victim.gamemode.load() == GameMode::Creative {
                world.play_sound(
                    Sound::EntityPlayerAttackNodamage,
                    SoundCategory::Players,
                    &player_victim.position(),
                );
                return;
            }
        }
        attack_and_replicate(
            player,
            &target,
            player_target.is_some() || replica_gid.is_some(),
            player_target
                .as_ref()
                .and_then(|victim| victim.cluster_gid())
                .or(replica_gid),
            server,
        );
    }
}

pub(crate) fn combat_tick() -> TickStamp {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |age| i64::try_from(age.as_millis()).unwrap_or(0));
    crate::server::cluster::disciplined_tick_stamp(millis)
}

pub(crate) fn stage_cluster_attack(
    player: &Player,
    server: &Server,
    entity_id: i32,
    bedrock: bool,
) -> bool {
    let Some(gid) = player.cluster_gid() else {
        return false;
    };
    let targets = server.cluster_combat_client_targets.load();
    let Some(target) = targets.get(&entity_id).cloned() else {
        return true;
    };
    if !server.advanced_config.pvp.enabled
        || target.contact.actor() == pumpkin_cluster::actor::ActorId::Player(gid)
        || server.advanced_config.pvp.protect_creative && target.game_mode == Some(1)
    {
        return true;
    }
    crate::server::cluster_inventory_protocol::stage_inventory_input(
        player,
        crate::server::cluster_inventory_protocol::StagedInventoryInput::Combat { target, bedrock },
    )
}

pub(crate) fn attack_and_replicate(
    attacker: &Player,
    victim: &Arc<dyn EntityBase>,
    _victim_is_player: bool,
    _victim_gid: Option<GlobalPlayerId>,
    server: &Arc<Server>,
) {
    if server.advanced_config.cluster.enabled {
        stage_cluster_attack(
            attacker,
            server,
            victim.get_entity().entity_id,
            attacker.client.bedrock().is_some(),
        );
        return;
    }
    attacker.attack(victim);
}
