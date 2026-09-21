use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::combat::{
    CombatLimits, HitSnapshot, is_dir_sane, validate_fire, validate_hit_entity, validate_hit_player,
    FireDecision, HitDecision,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use pumpkin_cluster::protocol::{FireProjectileUpdate, HitEntityUpdate, HitPlayerUpdate, StreamKind};
use pumpkin_cluster::streams::InboundParcel;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::entity::EntityBase;

use super::Server;

static COMBAT_BATCHES: AtomicU64 = AtomicU64::new(0);
static COMBAT_HIT_PLAYER: AtomicU64 = AtomicU64::new(0);
static COMBAT_HIT_ENTITY: AtomicU64 = AtomicU64::new(0);
static COMBAT_FIRE: AtomicU64 = AtomicU64::new(0);
static COMBAT_REJECTED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn combat_batches() -> u64 {
    COMBAT_BATCHES.load(Ordering::Relaxed)
}

#[must_use]
pub fn combat_hit_player_applied() -> u64 {
    COMBAT_HIT_PLAYER.load(Ordering::Relaxed)
}

#[must_use]
pub fn combat_hit_entity_applied() -> u64 {
    COMBAT_HIT_ENTITY.load(Ordering::Relaxed)
}

#[must_use]
pub fn combat_fire_applied() -> u64 {
    COMBAT_FIRE.load(Ordering::Relaxed)
}

#[must_use]
pub fn combat_rejected() -> u64 {
    COMBAT_REJECTED.load(Ordering::Relaxed)
}

#[derive(Debug, Default)]
pub struct CombatApplyState {
    last_seq: HashMap<GlobalPlayerId, PlayerSeq>,
    limits: CombatLimits,
}

impl CombatApplyState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn note_accept(&mut self, gid: GlobalPlayerId, seq: PlayerSeq) {
        self.last_seq.insert(gid, seq);
    }

    fn last_for(&self, gid: GlobalPlayerId) -> Option<PlayerSeq> {
        self.last_seq.get(&gid).copied()
    }
}

fn find_player_by_gid(server: &Arc<Server>, gid: GlobalPlayerId) -> Option<Arc<crate::entity::player::Player>> {
    for world in server.worlds.load().iter() {
        for player in world.players.load().iter() {
            if player.cluster_gid() == Some(gid) {
                return Some(player.clone());
            }
        }
    }
    None
}

fn milli_to_hearts(damage_milli: u16) -> f32 {
    f32::from(damage_milli) / 1000.0
}

fn health_snapshot(player: &crate::entity::player::Player) -> HitSnapshot {
    let health = player.living_entity.health.load();
    let milli = (health.max(0.0) * 1000.0) as u32;
    HitSnapshot::new(milli, 0)
}

fn apply_hit_player(
    server: &Arc<Server>,
    state: &mut CombatApplyState,
    batch_tick: pumpkin_cluster::time::TickStamp,
    update: &HitPlayerUpdate,
) {
    let Some(target) = find_player_by_gid(server, update.target) else {
        return;
    };
    let before = health_snapshot(&target);
    match validate_hit_player(
        update,
        state.last_for(update.gid),
        batch_tick,
        before,
        &state.limits,
    ) {
        HitDecision::Reject(_) => {
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        }
        HitDecision::Accept { .. } => {
            state.note_accept(update.gid, update.seq);
            let amount = milli_to_hearts(update.damage_milli);
            if amount <= 0.0 {
                return;
            }
            if let Some(attacker) = find_player_by_gid(server, update.gid) {
                target.damage(
                    attacker.as_ref() as &dyn crate::entity::EntityBase,
                    amount,
                    pumpkin_data::damage::DamageType::PLAYER_ATTACK,
                );
            } else {
                target.damage(
                    target.as_ref() as &dyn crate::entity::EntityBase,
                    amount,
                    pumpkin_data::damage::DamageType::PLAYER_ATTACK,
                );
            }
            COMBAT_HIT_PLAYER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn apply_hit_entity(
    server: &Arc<Server>,
    state: &mut CombatApplyState,
    batch_tick: pumpkin_cluster::time::TickStamp,
    update: &HitEntityUpdate,
) {
    let mut victim: Option<Arc<dyn crate::entity::EntityBase>> = None;
    for world in server.worlds.load().iter() {
        if let Some(entity) = world.get_entity_by_id(update.target.local_id) {
            victim = Some(entity);
            break;
        }
    }
    let Some(victim) = victim else {
        return;
    };
    let before = HitSnapshot::new(0, 0);
    match validate_hit_entity(
        update,
        state.last_for(update.gid),
        batch_tick,
        before,
        &state.limits,
    ) {
        HitDecision::Reject(_) => {
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        }
        HitDecision::Accept { .. } => {
            state.note_accept(update.gid, update.seq);
            let amount = milli_to_hearts(update.damage_milli);
            if amount <= 0.0 {
                return;
            }
            if let Some(attacker) = find_player_by_gid(server, update.gid) {
                victim.damage_with_context(
                    attacker.as_ref() as &dyn crate::entity::EntityBase,
                    amount,
                    pumpkin_data::damage::DamageType::PLAYER_ATTACK,
                    None,
                    None,
                    None,
                );
            } else {
                let caller: &dyn crate::entity::EntityBase = victim.as_ref();
                victim.damage_with_context(
                    caller,
                    amount,
                    pumpkin_data::damage::DamageType::PLAYER_ATTACK,
                    None,
                    None,
                    None,
                );
            }
            COMBAT_HIT_ENTITY.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn arrow_speed_for_fire(update: &FireProjectileUpdate) -> f32 {
    if update.kind == 1 {
        return crate::item::items::crossbow::CrossbowItem::ARROW_POWER;
    }
    let charge = f32::from(update.charge_milli) / 1000.0;
    let power = charge.clamp(0.0, 1.0).max(0.1);
    crate::item::items::bow::BowItem::ARROW_SPEED_MULTIPLIER * power
}

fn apply_fire(
    server: &Arc<Server>,
    state: &mut CombatApplyState,
    batch_tick: pumpkin_cluster::time::TickStamp,
    update: &FireProjectileUpdate,
) {
    if !is_dir_sane(update.dir) {
        COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        return;
    }
    match validate_fire(update, state.last_for(update.gid), batch_tick, &state.limits) {
        FireDecision::Reject(_) => {
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        }
        FireDecision::Accept => {
            state.note_accept(update.gid, update.seq);
            let Some(shooter) = find_player_by_gid(server, update.gid) else {
                return;
            };
            let world = shooter.world();
            let shooter_entity = shooter.get_entity();
            let arrow_base = crate::entity::Entity::new(
                world.clone(),
                shooter_entity.pos.load(),
                &pumpkin_data::entity::EntityType::ARROW,
            );
            let stack = pumpkin_data::item_stack::ItemStack::new(1, &pumpkin_data::item::Item::ARROW);
            let arrow = crate::entity::projectile::arrow::ArrowEntity::new_shot(
                arrow_base,
                shooter_entity,
                &stack,
                crate::entity::projectile::arrow::ArrowPickup::Allowed,
            );
            let speed = f64::from(arrow_speed_for_fire(update));
            arrow.set_velocity(
                f64::from(update.dir[0]),
                f64::from(update.dir[1]),
                f64::from(update.dir[2]),
                speed,
                1.0,
            );
            world.spawn_entity(Arc::new(arrow));
            COMBAT_FIRE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn apply_batch_bytes(
    server: &Arc<Server>,
    state: &mut CombatApplyState,
    bytes: &[u8],
) {
    let batch = match decode_batch(bytes) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(%error, "cluster combat batch decode failed");
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    COMBAT_BATCHES.fetch_add(1, Ordering::Relaxed);
    for update in &batch.hit_player {
        apply_hit_player(server, state, batch.tick, update);
    }
    for update in &batch.hit_entity {
        apply_hit_entity(server, state, batch.tick, update);
    }
    for update in &batch.fire {
        apply_fire(server, state, batch.tick, update);
    }
}

pub fn spawn_combat_apply(
    server: &Arc<Server>,
    _local: ServerId,
    mut receiver: mpsc::Receiver<InboundParcel>,
) {
    let task_server = Arc::clone(server);
    server.spawn_task(async move {
        let mut state = CombatApplyState::new();
        let mut first = true;
        while let Some(parcel) = receiver.recv().await {
            if parcel.header.kind != StreamKind::PlayerCombat {
                continue;
            }
            if first {
                first = false;
                debug!(from = parcel.peer.0, "cluster combat stream started");
            }
            apply_batch_bytes(&task_server, &mut state, &parcel.bytes);
        }
        debug!("cluster combat stream closed");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bow_speed_scales_with_charge() {
        let base = FireProjectileUpdate {
            gid: GlobalPlayerId::new(ServerId(1), pumpkin_cluster::identity::PlayerSlot(1)),
            seq: PlayerSeq(0),
            tick: pumpkin_cluster::time::TickStamp(0),
            kind: 0,
            charge_milli: 1000,
            dir: [0.0, 0.0, 1.0],
        };
        let full = arrow_speed_for_fire(&base);
        let weak = arrow_speed_for_fire(&FireProjectileUpdate {
            charge_milli: 100,
            ..base
        });
        assert!(full > weak);
        let crossbow = arrow_speed_for_fire(&FireProjectileUpdate { kind: 1, ..base });
        assert!((crossbow - crate::item::items::crossbow::CrossbowItem::ARROW_POWER).abs() < f32::EPSILON);
    }

    #[test]
    fn bad_dir_rejected_without_server() {
        assert!(!is_dir_sane([f32::NAN, 0.0, 0.0]));
        assert!(!is_dir_sane([0.0, 0.0, 0.0]));
        assert!(is_dir_sane([0.0, 0.0, 1.0]));
    }
}
