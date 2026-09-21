use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pumpkin_cluster::combat::{CombatLimits, FireDecision, validate_fire};
use pumpkin_cluster::identity::{ActionActor, ActionSeq, GlobalPlayerId, PlayerSeq};
use pumpkin_cluster::protocol::{
    FireProjectileUpdate,
};

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
    fire_seq: HashMap<GlobalPlayerId, PlayerSeq>,
    mutation_seq: HashMap<ActionActor, ActionSeq>,
    limits: CombatLimits,
}

impl CombatApplyState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

}

fn apply_fire(
    state: &mut CombatApplyState,
    batch_tick: pumpkin_cluster::time::TickStamp,
    update: &FireProjectileUpdate,
) {
    match validate_fire(
        update,
        state.fire_seq.get(&update.gid).copied(),
        batch_tick,
        &state.limits,
    ) {
        FireDecision::Reject(_) => {
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        }
        FireDecision::Accept => {
            state.fire_seq.insert(update.gid, update.seq);
            COMBAT_FIRE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn apply_entity_mutations(
    state: &mut CombatApplyState,
    batch: &pumpkin_cluster::protocol::TickBatch,
) {
    let mut accepted = Vec::with_capacity(batch.entity_mutations.len());
    for update in &batch.entity_mutations {
        if state
            .mutation_seq
            .get(&update.actor)
            .is_none_or(|previous| update.seq.is_newer_than(*previous))
        {
            state.mutation_seq.insert(update.actor, update.seq);
            accepted.push(*update);
        } else {
            COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
        }
    }
    if !accepted.is_empty()
        && !super::cluster_entity_apply::promote_owned_entity_mutations(
            super::cluster_world_apply::CLUSTER_SEED,
            batch.tick,
            accepted,
        )
    {
        COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
    }
}

fn apply_combat_hits(batch: &pumpkin_cluster::protocol::TickBatch) {
    if batch.attacks.is_empty() {
        return;
    }
    if super::cluster_entity_apply::promote_captured_attacks(
        super::cluster_world_apply::CLUSTER_SEED,
        batch.tick,
        batch.attacks.clone(),
    ) {
        COMBAT_HIT_PLAYER.fetch_add(batch.attacks.len() as u64, Ordering::Relaxed);
    } else {
        COMBAT_REJECTED.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn apply_accepted_batch(
    _server: &Arc<Server>,
    state: &mut CombatApplyState,
    batch: &pumpkin_cluster::protocol::TickBatch,
) {
    COMBAT_BATCHES.fetch_add(1, Ordering::Relaxed);
    apply_combat_hits(batch);
    for update in &batch.fire {
        apply_fire(state, batch.tick, update);
    }
    if !batch.entity_mutations.is_empty() {
        apply_entity_mutations(state, batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::identity::ServerId;

    #[test]
    fn bad_dir_rejected_without_server() {
        let update = FireProjectileUpdate {
            gid: GlobalPlayerId::new(ServerId(1), pumpkin_cluster::identity::PlayerSlot(1)),
            seq: PlayerSeq(0),
            tick: pumpkin_cluster::time::TickStamp(0),
            chunk: pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
            kind: 0,
            charge_milli: 1000,
            dir: [f32::NAN, 0.0, 0.0],
        };
        assert!(matches!(
            validate_fire(
                &update,
                None,
                update.tick,
                &CombatLimits::default_limits()
            ),
            FireDecision::Reject(_)
        ));
    }
}
