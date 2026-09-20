#[allow(clippy::wildcard_imports)]
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

use pumpkin_cluster::combat::{
    capture_hit_entity, capture_hit_player, chunk_of_pos, next_combat_seq, stage_hit_entity,
    stage_hit_player,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::protocol::EntityRef;
use pumpkin_cluster::time::TickStamp;

impl JavaClient {
    pub fn handle_attack(&self, player: &Arc<Player>, attack: &SAttack, server: &Arc<Server>) {
        if !player.has_client_loaded() {
            return;
        }
        player.update_last_action_time();
        let entity_id = attack.entity_id;
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
            player_target.is_some(),
            player_target
                .as_ref()
                .and_then(|victim| victim.cluster_gid()),
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

fn living_health(victim: &Arc<dyn EntityBase>) -> f32 {
    victim
        .get_living_entity()
        .map(|living| living.health.load())
        .unwrap_or(0.0)
}

fn damage_dealt_milli(before: f32, after: f32) -> u16 {
    (before - after).max(0.0).mul_add(1000.0, 0.0).clamp(0.0, f32::from(u16::MAX)) as u16
}

pub(crate) fn attack_and_replicate(
    attacker: &Player,
    victim: &Arc<dyn EntityBase>,
    victim_is_player: bool,
    victim_gid: Option<GlobalPlayerId>,
    server: &Arc<Server>,
) {
    let before = living_health(victim);
    attacker.attack(victim);
    if !server.advanced_config.cluster.enabled {
        return;
    }
    let Some(gid) = attacker.cluster_gid() else {
        return;
    };
    let damage_milli = damage_dealt_milli(before, living_health(victim));
    let seq = next_combat_seq(gid);
    let tick = combat_tick();
    if victim_is_player {
        let Some(target) = victim_gid else {
            return;
        };
        stage_hit_player(capture_hit_player(gid, seq, tick, target, damage_milli));
    } else {
        let entity = victim.get_entity();
        let pos = entity.pos.load();
        stage_hit_entity(capture_hit_entity(
            gid,
            seq,
            tick,
            EntityRef {
                owner: ServerId(server.advanced_config.cluster.server_id),
                local_id: entity.entity_id,
                chunk: chunk_of_pos(pos.x, pos.z),
            },
            damage_milli,
        ));
    }
}
