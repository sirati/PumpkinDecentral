use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::accept::ConflictKey;
use crate::identity::{ActionActor, ActionSeq};
use crate::order::order_action_actors;
use crate::protocol::{
    EntityMutation, EntityMutationTarget, EntityMutationUpdate, StatusEffectState,
};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityMutationUndo {
    pub target: EntityMutationTarget,
    pub effect: u16,
    pub expected_current: Option<StatusEffectState>,
    pub restore: Option<StatusEffectState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityMutationVerdict {
    Applied(EntityMutationUndo),
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityLocalAcceptance {
    AlreadyOptimistic,
    Applied(EntityMutationUndo),
    Rejected,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EntityGroundPromotion {
    pub ground_applied: usize,
    pub ground_rejected: usize,
    pub local_applied: usize,
    pub local_rejected: usize,
    pub local_undone: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EntitySingleTruthResolution {
    pub local_applied: usize,
    pub local_rejected: usize,
    pub local_undone: usize,
}

#[derive(Debug, Clone, Copy)]
struct LocalEntityMutation {
    update: EntityMutationUpdate,
    undo: EntityMutationUndo,
}

#[derive(Debug, Default)]
pub struct EntityDualJournal {
    pending: BTreeMap<TickStamp, Vec<LocalEntityMutation>>,
}

impl EntityDualJournal {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn pending_for(&self, tick: TickStamp) -> usize {
        self.pending.get(&tick).map_or(0, Vec::len)
    }

    #[must_use]
    pub fn stage_local_optimistic(
        &mut self,
        local: &mut impl EntityEffects,
        update: EntityMutationUpdate,
    ) -> EntityMutationVerdict {
        let verdict = replay(local, update);
        if let EntityMutationVerdict::Applied(undo) = verdict {
            self.pending
                .entry(update.tick)
                .or_default()
                .push(LocalEntityMutation { update, undo });
        }
        verdict
    }

    #[must_use]
    pub fn apply_accepted_local(
        &self,
        local: &mut impl EntityEffects,
        update: EntityMutationUpdate,
    ) -> EntityLocalAcceptance {
        if self
            .pending
            .get(&update.tick)
            .is_some_and(|pending| pending.iter().any(|entry| entry.update.same_identity(update)))
        {
            return EntityLocalAcceptance::AlreadyOptimistic;
        }
        match replay(local, update) {
            EntityMutationVerdict::Applied(undo) => EntityLocalAcceptance::Applied(undo),
            EntityMutationVerdict::Rejected => EntityLocalAcceptance::Rejected,
        }
    }

    #[must_use]
    pub fn undo_local_loser(
        &mut self,
        local: &mut impl EntityEffects,
        update: EntityMutationUpdate,
    ) -> bool {
        let Some(pending) = self.pending.get_mut(&update.tick) else {
            return false;
        };
        let Some(index) = pending.iter().position(|entry| entry.update.same_identity(update)) else {
            return false;
        };
        let entry = pending.remove(index);
        if pending.is_empty() {
            self.pending.remove(&update.tick);
        }
        undo(local, entry.undo)
    }

    #[must_use]
    pub fn promote_globally_accepted(
        &mut self,
        local: &mut impl EntityEffects,
        ground: &mut impl EntityEffects,
        cluster_seed: u64,
        tick: TickStamp,
        accepted: &[EntityMutationUpdate],
    ) -> EntityGroundPromotion {
        let mut promotion = EntityGroundPromotion::default();
        let mut ordered: Vec<_> = accepted
            .iter()
            .copied()
            .filter(|update| update.tick == tick)
            .collect();
        sort_entity_mutations(cluster_seed, tick, &mut ordered);
        let pending = self.pending.remove(&tick).unwrap_or_default();
        for entry in pending.iter().filter(|entry| !ordered.iter().any(|update| entry.update.same_identity(*update))) {
            if undo(local, entry.undo) {
                promotion.local_undone += 1;
            }
        }
        for update in ordered {
            let locally_optimistic = pending.iter().any(|entry| entry.update.same_identity(update));
            match replay(ground, update) {
                EntityMutationVerdict::Applied(_) => {
                    promotion.ground_applied += 1;
                    if !locally_optimistic {
                        match replay(local, update) {
                            EntityMutationVerdict::Applied(_) => promotion.local_applied += 1,
                            EntityMutationVerdict::Rejected => promotion.local_rejected += 1,
                        }
                    }
                }
                EntityMutationVerdict::Rejected => {
                    promotion.ground_rejected += 1;
                    if let Some(entry) = pending.iter().find(|entry| entry.update.same_identity(update))
                        && undo(local, entry.undo)
                    {
                        promotion.local_undone += 1;
                    }
                }
            }
        }
        promotion
    }

    #[must_use]
    pub fn resolve_globally_accepted_single_truth(
        &mut self,
        local: &mut impl EntityEffects,
        cluster_seed: u64,
        tick: TickStamp,
        accepted: &[EntityMutationUpdate],
    ) -> EntitySingleTruthResolution {
        let mut resolution = EntitySingleTruthResolution::default();
        let mut ordered: Vec<_> = accepted.iter().copied().filter(|update| update.tick == tick).collect();
        sort_entity_mutations(cluster_seed, tick, &mut ordered);
        let pending = self.pending.remove(&tick).unwrap_or_default();
        for entry in pending.iter().filter(|entry| !ordered.iter().any(|update| entry.update.same_identity(*update))) {
            if undo(local, entry.undo) {
                resolution.local_undone += 1;
            }
        }
        for update in ordered {
            if pending.iter().any(|entry| entry.update.same_identity(update)) {
                continue;
            }
            match replay(local, update) {
                EntityMutationVerdict::Applied(_) => resolution.local_applied += 1,
                EntityMutationVerdict::Rejected => resolution.local_rejected += 1,
            }
        }
        resolution
    }
}

#[derive(Debug, Default)]
pub struct EntityMutationSeqClock {
    next: BTreeMap<ActionActor, u16>,
}

impl EntityMutationSeqClock {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn issue(&mut self, actor: ActionActor) -> ActionSeq {
        let next = self.next.entry(actor).or_insert(0);
        let sequence = ActionSeq(*next);
        *next = next.wrapping_add(1);
        sequence
    }
}

pub trait EntityEffects {
    fn effect(&self, target: EntityMutationTarget, effect: u16) -> Option<StatusEffectState>;
    fn set_effect(&mut self, target: EntityMutationTarget, effect: StatusEffectState) -> bool;
    fn remove_effect(&mut self, target: EntityMutationTarget, effect: u16) -> bool;
}

#[must_use]
pub fn capture_add_effect(
    actor: ActionActor,
    seq: ActionSeq,
    tick: TickStamp,
    target: EntityMutationTarget,
    before: Option<StatusEffectState>,
    after: StatusEffectState,
) -> EntityMutationUpdate {
    EntityMutationUpdate {
        actor,
        seq,
        tick,
        target,
        mutation: EntityMutation::AddEffect { before, after },
    }
}

#[must_use]
pub fn capture_remove_effect(
    actor: ActionActor,
    seq: ActionSeq,
    tick: TickStamp,
    target: EntityMutationTarget,
    before: StatusEffectState,
) -> EntityMutationUpdate {
    EntityMutationUpdate {
        actor,
        seq,
        tick,
        target,
        mutation: EntityMutation::RemoveEffect { before },
    }
}

pub fn stage(update: EntityMutationUpdate) {
    crate::combat::stage_entity_mutation(update);
}

#[must_use]
pub const fn is_well_formed(update: EntityMutationUpdate) -> bool {
    match update.mutation {
        EntityMutation::AddEffect { before, after } => {
            after.duration_ticks != 0
                && after.duration_ticks >= -1
                && match before {
                    Some(before) => {
                        before.effect == after.effect
                            && before.duration_ticks != 0
                            && before.duration_ticks >= -1
                    }
                    None => true,
                }
        }
        EntityMutation::RemoveEffect { before } => {
            before.duration_ticks != 0 && before.duration_ticks >= -1
        }
    }
}

#[must_use]
pub fn replay(
    effects: &mut impl EntityEffects,
    update: EntityMutationUpdate,
) -> EntityMutationVerdict {
    if !is_well_formed(update) {
        return EntityMutationVerdict::Rejected;
    }
    let effect = update.mutation.effect();
    let expected = update.mutation.expected();
    if effects.effect(update.target, effect) != expected {
        return EntityMutationVerdict::Rejected;
    }
    let applied = update.mutation.applied();
    let changed = match applied {
        Some(after) => effects.set_effect(update.target, after),
        None => effects.remove_effect(update.target, effect),
    };
    if changed {
        EntityMutationVerdict::Applied(EntityMutationUndo {
            target: update.target,
            effect,
            expected_current: applied,
            restore: expected,
        })
    } else {
        EntityMutationVerdict::Rejected
    }
}

#[must_use]
pub fn undo(effects: &mut impl EntityEffects, undo: EntityMutationUndo) -> bool {
    if effects.effect(undo.target, undo.effect) != undo.expected_current {
        return false;
    }
    match undo.restore {
        Some(effect) => effects.set_effect(undo.target, effect),
        None => effects.remove_effect(undo.target, undo.effect),
    }
}

#[must_use]
pub fn conflict_key(update: EntityMutationUpdate) -> ConflictKey {
    match update.target {
        EntityMutationTarget::Entity(entity) => ConflictKey::for_entity_effect(
            entity.origin.0,
            entity.local_id,
            update.mutation.effect(),
        ),
        EntityMutationTarget::Player { gid, .. } => {
            ConflictKey::for_player_effect(gid, update.mutation.effect())
        }
    }
}

#[must_use]
pub fn order_entity_mutations(
    cluster_seed: u64,
    tick: TickStamp,
    left: &EntityMutationUpdate,
    right: &EntityMutationUpdate,
) -> Ordering {
    conflict_key(*left)
        .cmp(&conflict_key(*right))
        .then_with(|| order_action_actors(cluster_seed, tick, left.actor, right.actor).reverse())
        .then_with(|| left.seq.cmp(&right.seq))
        .then_with(|| mutation_order(left.mutation).cmp(&mutation_order(right.mutation)))
}

pub fn sort_entity_mutations(
    cluster_seed: u64,
    tick: TickStamp,
    updates: &mut [EntityMutationUpdate],
) {
    updates.sort_by(|left, right| order_entity_mutations(cluster_seed, tick, left, right));
}

fn mutation_order(mutation: EntityMutation) -> (u8, i32, u8, u8) {
    match mutation {
        EntityMutation::AddEffect { after, .. } => {
            (0, after.duration_ticks, after.amplifier, after.flags)
        }
        EntityMutation::RemoveEffect { before } => {
            (1, before.duration_ticks, before.amplifier, before.flags)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::identity::{ActionSeq as PlayerSeq, GlobalPlayerId, PlayerSlot, ServerId};
    use crate::protocol::{ChunkAddr, EntityRef};

    #[derive(Default)]
    struct Effects(HashMap<(EntityMutationTarget, u16), StatusEffectState>);

    impl EntityEffects for Effects {
        fn effect(&self, target: EntityMutationTarget, effect: u16) -> Option<StatusEffectState> {
            self.0.get(&(target, effect)).copied()
        }

        fn set_effect(&mut self, target: EntityMutationTarget, effect: StatusEffectState) -> bool {
            self.0.insert((target, effect.effect), effect);
            true
        }

        fn remove_effect(&mut self, target: EntityMutationTarget, effect: u16) -> bool {
            self.0.remove(&(target, effect)).is_some()
        }
    }

    fn gid(server: u16, player: u16) -> ActionActor {
        ActionActor::Player(GlobalPlayerId::new(ServerId(server), PlayerSlot(player)))
    }

    fn target() -> EntityMutationTarget {
        EntityMutationTarget::Entity(EntityRef {
            origin: ServerId(2),
            owner: ServerId(2),
            local_id: 91,
            chunk: ChunkAddr { x: 3, z: -7 },
        })
    }

    fn effect(duration_ticks: i32) -> StatusEffectState {
        StatusEffectState {
            effect: 5,
            duration_ticks,
            amplifier: 2,
            flags: 3,
        }
    }

    #[test]
    fn add_replays_and_undo_restores_absence() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut effects = Effects::default();
        let EntityMutationVerdict::Applied(undo_action) = replay(&mut effects, update) else {
            panic!("add must apply");
        };
        assert_eq!(effects.effect(target(), 5), Some(effect(100)));
        assert!(undo(&mut effects, undo_action));
        assert_eq!(effects.effect(target(), 5), None);
    }

    #[test]
    fn replacement_undo_restores_exact_prior_effect() {
        let old = effect(100);
        let new = effect(200);
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            Some(old),
            new,
        );
        let mut effects = Effects::default();
        assert!(effects.set_effect(target(), old));
        let EntityMutationVerdict::Applied(undo_action) = replay(&mut effects, update) else {
            panic!("replacement must apply");
        };
        assert_eq!(effects.effect(target(), 5), Some(new));
        assert!(undo(&mut effects, undo_action));
        assert_eq!(effects.effect(target(), 5), Some(old));
    }

    #[test]
    fn remove_is_conditional_and_undoable() {
        let old = effect(100);
        let update = capture_remove_effect(gid(1, 1), PlayerSeq(1), TickStamp(9), target(), old);
        let mut effects = Effects::default();
        assert!(effects.set_effect(target(), old));
        let EntityMutationVerdict::Applied(undo_action) = replay(&mut effects, update) else {
            panic!("remove must apply");
        };
        assert_eq!(effects.effect(target(), 5), None);
        assert!(undo(&mut effects, undo_action));
        assert_eq!(effects.effect(target(), 5), Some(old));
    }

    #[test]
    fn stale_precondition_rejects_without_mutating() {
        let old = effect(100);
        let update = capture_remove_effect(gid(1, 1), PlayerSeq(1), TickStamp(9), target(), old);
        let mut effects = Effects::default();
        assert_eq!(
            replay(&mut effects, update),
            EntityMutationVerdict::Rejected
        );
        assert_eq!(effects.effect(target(), 5), None);
    }

    #[test]
    fn target_and_effect_define_conflict_key() {
        let first = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let second = capture_add_effect(
            gid(2, 1),
            PlayerSeq(2),
            TickStamp(9),
            target(),
            None,
            effect(200),
        );
        assert_eq!(conflict_key(first), conflict_key(second));
    }

    #[test]
    fn ordering_is_identical_for_all_receivers() {
        let mut first = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut second = capture_add_effect(
            gid(2, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(200),
        );
        let mut left = vec![first, second];
        let mut right = vec![second, first];
        sort_entity_mutations(7, TickStamp(9), &mut left);
        sort_entity_mutations(7, TickStamp(9), &mut right);
        assert_eq!(left, right);
        first = left[0];
        second = left[1];
        assert_ne!(first.actor, second.actor);
    }

    #[test]
    fn malformed_effects_reject() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(0),
        );
        assert!(!is_well_formed(update));
    }

    #[test]
    fn local_action_promotes_only_after_global_acceptance() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut local = Effects::default();
        let mut ground = Effects::default();
        let mut journal = EntityDualJournal::new();
        assert!(matches!(
            journal.stage_local_optimistic(&mut local, update),
            EntityMutationVerdict::Applied(_)
        ));
        assert_eq!(local.effect(target(), 5), Some(effect(100)));
        assert_eq!(ground.effect(target(), 5), None);
        let promotion =
            journal.promote_globally_accepted(&mut local, &mut ground, 1, TickStamp(9), &[update]);
        assert_eq!(promotion.ground_applied, 1);
        assert_eq!(promotion.local_applied, 0);
        assert_eq!(journal.pending_for(TickStamp(9)), 0);
        assert_eq!(local.effect(target(), 5), Some(effect(100)));
        assert_eq!(ground.effect(target(), 5), Some(effect(100)));
    }

    #[test]
    fn global_winner_undoes_local_loser_then_updates_both_truths() {
        let local_update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let winning_update = capture_add_effect(
            gid(2, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(200),
        );
        let mut local = Effects::default();
        let mut ground = Effects::default();
        let mut journal = EntityDualJournal::new();
        assert!(matches!(
            journal.stage_local_optimistic(&mut local, local_update),
            EntityMutationVerdict::Applied(_)
        ));
        let promotion = journal.promote_globally_accepted(
            &mut local,
            &mut ground,
            1,
            TickStamp(9),
            &[winning_update],
        );
        assert_eq!(promotion.local_undone, 1);
        assert_eq!(promotion.ground_applied, 1);
        assert_eq!(promotion.local_applied, 1);
        assert_eq!(local.effect(target(), 5), Some(effect(200)));
        assert_eq!(ground.effect(target(), 5), Some(effect(200)));
    }

    #[test]
    fn accepted_remote_action_updates_local_without_ground_promotion() {
        let update = capture_add_effect(
            gid(2, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut local = Effects::default();
        let journal = EntityDualJournal::new();
        assert!(matches!(
            journal.apply_accepted_local(&mut local, update),
            EntityLocalAcceptance::Applied(_)
        ));
        assert_eq!(local.effect(target(), 5), Some(effect(100)));
    }

    #[test]
    fn explicit_loser_undo_removes_only_its_optimism() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut local = Effects::default();
        let mut journal = EntityDualJournal::new();
        assert!(matches!(
            journal.stage_local_optimistic(&mut local, update),
            EntityMutationVerdict::Applied(_)
        ));
        assert!(journal.undo_local_loser(&mut local, update));
        assert_eq!(journal.pending_for(TickStamp(9)), 0);
        assert_eq!(local.effect(target(), 5), None);
    }

    #[test]
    fn tick_batch_codec_preserves_mutation() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let mut batch = crate::protocol::TickBatch::new(TickStamp(9));
        batch.entity_mutations.push(update);
        let bytes = crate::codec::encode_batch(&batch).expect("encode mutation batch");
        let decoded = crate::codec::decode_batch(&bytes).expect("decode mutation batch");
        assert_eq!(decoded.entity_mutations, vec![update]);
    }

    #[test]
    fn infinite_effect_is_well_formed() {
        let update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(-1),
        );
        assert!(is_well_formed(update));
    }

    #[test]
    fn server_actor_has_its_own_sequence_and_codec_identity() {
        let mut clock = EntityMutationSeqClock::new();
        let actor = ActionActor::Server(ServerId(4));
        assert_eq!(clock.issue(actor), ActionSeq(0));
        assert_eq!(clock.issue(actor), ActionSeq(1));
        let update = capture_add_effect(
            actor,
            ActionSeq(2),
            TickStamp(9),
            target(),
            None,
            effect(-1),
        );
        assert_eq!(update.actor, actor);
        assert_eq!(update.stream_kind(), crate::protocol::StreamKind::EntityCombat);
    }

    #[test]
    fn holder_single_truth_undoes_loser_then_applies_winner() {
        let local_update = capture_add_effect(
            gid(1, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(100),
        );
        let winner = capture_add_effect(
            gid(2, 1),
            PlayerSeq(1),
            TickStamp(9),
            target(),
            None,
            effect(200),
        );
        let mut local = Effects::default();
        let mut journal = EntityDualJournal::new();
        assert!(matches!(
            journal.stage_local_optimistic(&mut local, local_update),
            EntityMutationVerdict::Applied(_)
        ));
        let resolution = journal.resolve_globally_accepted_single_truth(
            &mut local,
            1,
            TickStamp(9),
            &[winner],
        );
        assert_eq!(resolution.local_undone, 1);
        assert_eq!(resolution.local_applied, 1);
        assert_eq!(local.effect(target(), 5), Some(effect(200)));
    }
}
