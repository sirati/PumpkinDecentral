use serde::{Deserialize, Serialize};
use crate::canonical_action::{Action, ActionId, ActionState, ApplyFailure, Cell, ChunkPart,
    Condition, Effect, Expected, LivePlayerLease, SpatialEpoch, Target, TruthView, Value};
use crate::item_action::{PlayerPresentationChange, PlayerSleepingState};
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BedRelease {
    pub target: Target,
    pub before: u16,
    pub after: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeClaim {
    pub owner_install_generation: u64,
    pub source_epoch: SpatialEpoch,
    pub sleeping: PlayerSleepingState,
    pub bed_entity: Option<Cell>,
    pub morning: Option<crate::world_time::MorningTransition>,
    pub beds: Vec<BedRelease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenWake {
    pub lease: LivePlayerLease,
    pub chunk: ChunkKey,
    pub before: Cell,
    pub rest_target: Target,
    pub rest_before: Cell,
    pub bed_target: Target,
    pub bed_entity: Option<Cell>,
    pub morning: Option<crate::world_time::MorningTransition>,
    pub beds: Vec<BedRelease>,
}

impl FrozenWake {
    pub fn capture(state: &impl ActionState, lease: LivePlayerLease, chunk: ChunkKey,
        claim: &WakeClaim, source: crate::canonical_action::ActionRef) -> Result<Self, ApplyFailure> {
        state.validate_wake_beds(chunk.world, claim.sleeping, &claim.beds)?;
        if let Some(morning) = claim.morning { state.validate_morning(chunk.world, morning, source)?; }
        let target = Target::Chunk { key: chunk, part: ChunkPart::Player(lease.gid), inventory: None };
        let (category, key) = state.wake_rest_stat()?;
        let rest_target = Target::Chunk { key: chunk, part: ChunkPart::LivingProperty {
            actor: lease.gid.into(), property: crate::living_property::LivingProperty::Statistic { category, key } }, inventory: None };
        let rest_before = state.read(TruthView::Local, rest_target).ok_or(ApplyFailure::Condition(rest_target))?;
        if !matches!(rest_before.value, Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(_))) {
            return Err(ApplyFailure::Type(rest_target));
        }
        let before = state.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
        let Value::Living(living) = &before.value else { return Err(ApplyFailure::Type(target)); };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
        if avatar.spatial_epoch != claim.source_epoch || avatar.presentation.sleeping != Some(claim.sleeping)
            || claim.beds.len() > 2 || claim.owner_install_generation == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        for bed in &claim.beds {
            if !state.available(TruthView::Local, bed.target)
                && let Target::Chunk { key, .. } = bed.target {
                return Err(ApplyFailure::MissingStakeholder(crate::canonical_action::Stakeholder::Chunk(key)));
            }
            if !matches!(bed.target, Target::Chunk { key, part: ChunkPart::Block(_), inventory: None } if key.world == chunk.world)
                || !state.read(TruthView::Local, bed.target).is_some_and(|cell| cell.value == Value::Block(bed.before)) {
                return Err(ApplyFailure::Condition(bed.target));
            }
        }
        let bed_target = Target::Chunk { key: ChunkKey::new(chunk.world, claim.sleeping.bed.x.div_euclid(16), claim.sleeping.bed.z.div_euclid(16)),
            part: ChunkPart::BlockEntity(claim.sleeping.bed), inventory: None };
        if !state.available(TruthView::Local, bed_target) {
            let Target::Chunk { key, .. } = bed_target else { unreachable!(); };
            return Err(ApplyFailure::MissingStakeholder(crate::canonical_action::Stakeholder::Chunk(key)));
        }
        if state.read(TruthView::Local, bed_target) != claim.bed_entity
            || claim.morning.is_some_and(|morning| morning.issuer.generation == 0 || morning.revision == 0
                || TickStamp(morning.slot as u16).distance_since(claim.sleeping.since) >= 32768) {
            return Err(ApplyFailure::Condition(bed_target));
        }
        Ok(Self { lease, chunk, before, rest_target, rest_before, beds: claim.beds.clone(), bed_target,
            bed_entity: claim.bed_entity.clone(), morning: claim.morning })
    }
    pub fn validate_beds(&self, state: &impl ActionState) -> Result<(), ApplyFailure> {
        let Value::Living(living) = &self.before.value else { return Err(ApplyFailure::InvalidAction); };
        let sleeping = living.avatar.as_ref().and_then(|avatar| avatar.presentation.sleeping)
            .ok_or(ApplyFailure::InvalidAction)?;
        let (category, key) = state.wake_rest_stat()?;
        let expected = Target::Chunk { key: self.chunk, part: ChunkPart::LivingProperty {
            actor: self.lease.gid.into(), property: crate::living_property::LivingProperty::Statistic { category, key } }, inventory: None };
        if self.rest_target != expected || !matches!(self.rest_before.value,
            Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(_))) {
            return Err(ApplyFailure::InvalidAction);
        }
        state.validate_wake_beds(self.chunk.world, sleeping, &self.beds)
    }
    pub fn targets(&self) -> Vec<Target> {
        std::iter::once(Target::Chunk { key: self.chunk, part: ChunkPart::Player(self.lease.gid), inventory: None })
            .chain(std::iter::once(self.bed_target)).chain(std::iter::once(self.rest_target)).chain(self.beds.iter().map(|bed| bed.target)).collect()
    }
    fn original_action(&self, id: ActionId, tick: TickStamp) -> Result<Action, ApplyFailure> {
        let mut conditions = self.beds.iter().map(|bed| Condition { target: bed.target, expected: Expected::BlockState(bed.before) }).collect::<Vec<_>>();
        let mut effects = self.beds.iter().map(|bed| Effect::Put { target: bed.target, value: Value::Block(bed.after) }).collect::<Vec<_>>();
        conditions.push(Condition { target: self.rest_target, expected: Expected::Exact(self.rest_before.clone()) });
        effects.push(Effect::Put { target: self.rest_target,
            value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(0)) });
        conditions.push(Condition { target: self.bed_target, expected: self.bed_entity.clone().map_or(Expected::Missing, Expected::Exact) });
        effects.push(self.bed_entity.as_ref().map_or(Effect::Delete { target: self.bed_target },
            |cell| Effect::Put { target: self.bed_target, value: cell.value.clone() }));
        Action::from_player_presentation(id, self.lease, tick, self.chunk, self.before.clone(),
            PlayerPresentationChange::Sleeping(None),
            conditions, effects)
    }
    pub fn effects(&self, id: ActionId, tick: TickStamp) -> Result<Vec<Effect>, ApplyFailure> {
        Ok(self.original_action(id, tick)?.effects)
    }
    pub fn matches_action(&self, action: &Action) -> bool {
        let Value::Living(living) = &self.before.value else { return false; };
        let Some(avatar) = living.avatar.as_ref() else { return false; };
        let Some(sleeping) = avatar.presentation.sleeping else { return false; };
        let expected_bed = Target::Chunk { key: ChunkKey::new(self.chunk.world, sleeping.bed.x.div_euclid(16), sleeping.bed.z.div_euclid(16)),
            part: ChunkPart::BlockEntity(sleeping.bed), inventory: None };
        if self.bed_target != expected_bed || self.beds.len() > 2
            || self.morning.is_some_and(|morning| morning.issuer.generation == 0 || morning.revision == 0
                || TickStamp(morning.slot as u16).distance_since(sleeping.since) >= 32768) { return false; }
        self.original_action(action.id, action.tick).is_ok_and(|original|
            original.conditions == action.conditions && original.effects == action.effects
                && original.player_leases == action.player_leases && original.actor_chunk == action.actor_chunk
                && action.counterfactual_conditions == self.targets() && action.counterfactual_effects == self.targets())
    }
    pub fn materialize(&self, id: ActionId, tick: TickStamp) -> Result<Action, ApplyFailure> {
        let mut action = self.original_action(id, tick)?;
        action.class = crate::canonical_action::ActionClass::Replayable;
        action.counterfactual_conditions = self.targets(); action.counterfactual_effects = self.targets();
        action.semantic_intent = Some(crate::canonical_action::SemanticIntent::Wake(Box::new(self.clone())));
        action.validate()?;
        Ok(action)
    }

}
