use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Condition, Effect, Expected,
    Target, TruthView, Value,
};
use crate::living_property::{LivingProperty, LivingPropertyValue};
use crate::player_advancement::AdvancementEvent;
use crate::protocol::ChunkKey;
use std::collections::BTreeMap;

pub fn targets(actor: ActorId, chunk: ChunkKey) -> [Target; 2] {
    [LivingProperty::Advancements, LivingProperty::Experience].map(|property| Target::Chunk {
        key: chunk,
        part: ChunkPart::LivingProperty { actor, property },
        inventory: None,
    })
}

fn winning_value(effects: &[Effect], target: Target, before: &Cell) -> Result<Value, ApplyFailure> {
    for effect in effects.iter().rev() {
        if let Effect::Put {
            target: written,
            value,
        } = effect
            && *written == target
        {
            return Ok(value.clone());
        }
        if effect.targets().any(|written| written == target) {
            return Err(ApplyFailure::Type(target));
        }
    }
    Ok(before.value.clone())
}

fn put(effects: &mut Vec<Effect>, target: Target, value: Value) {
    if let Some(Effect::Put {
        value: previous, ..
    }) = effects
        .iter_mut()
        .rev()
        .find(|effect| matches!(effect, Effect::Put { target: written, .. } if *written == target))
    {
        *previous = value;
    } else {
        effects.push(Effect::Put { target, value });
    }
}

pub fn append_effects<S: ActionState>(
    state: &S,
    reference: ActionRef,
    actor: ActorId,
    chunk: ChunkKey,
    values: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    event: &AdvancementEvent,
) -> Result<(), ApplyFailure> {
    if !matches!(actor, ActorId::Player(_)) {
        return Err(ApplyFailure::InvalidAction);
    }
    let slot = state
        .action_source_slot(reference)
        .ok_or(ApplyFailure::MissingStakeholder(
            crate::canonical_action::Stakeholder::Chunk(chunk),
        ))?;
    if slot as u16 != reference.tick.0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let millis = slot.checked_mul(50).ok_or(ApplyFailure::InvalidAction)?;
    let [progress_target, experience_target] = targets(actor, chunk);
    let before = values
        .get(&progress_target)
        .ok_or(ApplyFailure::Condition(progress_target))?;
    let Value::LivingProperty(LivingPropertyValue::Advancements(progress)) =
        winning_value(effects, progress_target, before)?
    else {
        return Err(ApplyFailure::Type(progress_target));
    };
    let outcome = state.player_advancement_outcome(event, &progress, millis)?;
    if !outcome.state.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let experience = if outcome.experience != 0 {
        let before = values
            .get(&experience_target)
            .ok_or(ApplyFailure::Condition(experience_target))?;
        let Value::LivingProperty(LivingPropertyValue::Experience(current)) =
            winning_value(effects, experience_target, before)?
        else {
            return Err(ApplyFailure::Type(experience_target));
        };
        Some(
            current
                .collect(outcome.experience)
                .ok_or(ApplyFailure::Overflow(experience_target))?,
        )
    } else {
        None
    };
    if outcome.state != progress {
        put(
            effects,
            progress_target,
            Value::LivingProperty(LivingPropertyValue::Advancements(outcome.state)),
        );
    }
    if let Some(next) = experience {
        put(
            effects,
            experience_target,
            Value::LivingProperty(LivingPropertyValue::Experience(next)),
        );
    }
    Ok(())
}

pub fn append<S: ActionState>(
    state: &S,
    action: &mut Action,
    actor: ActorId,
    chunk: ChunkKey,
    event: &AdvancementEvent,
) -> Result<(), ApplyFailure> {
    let ActorId::Player(gid) = actor else {
        return Err(ApplyFailure::InvalidAction);
    };
    if !action
        .player_leases
        .iter()
        .any(|lease| lease.gid == gid && action.player_chunks.contains(&(lease.key, chunk)))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut values = BTreeMap::new();
    for target in targets(actor, chunk) {
        let before = state
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        values.insert(target, before);
    }
    append_effects(
        state,
        ActionRef {
            id: action.id,
            tick: action.tick,
        },
        actor,
        chunk,
        &values,
        &mut action.effects,
        event,
    )?;
    for (target, before) in values {
        if !action
            .conditions
            .iter()
            .any(|condition| condition.target == target)
        {
            action.conditions.push(Condition {
                target,
                expected: Expected::Exact(before),
            });
        }
        if !action.counterfactual_conditions.contains(&target) {
            action.counterfactual_conditions.push(target);
        }
        if !action.counterfactual_effects.contains(&target) {
            action.counterfactual_effects.push(target);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_action::{ActionContext, ActionId, AppliedWrite};
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::player_advancement::{
        AdvancementOutcome, AdvancementProgress, PlayerAdvancementState,
    };
    use crate::player_experience::PlayerExperienceState;
    use crate::protocol::WorldId;
    use crate::time::TickStamp;
    use std::collections::BTreeMap;

    struct State {
        slot: Option<u64>,
    }

    impl ActionState for State {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> {
            None
        }

        fn commit(
            &mut self,
            _: ActionContext,
            _: TruthView,
            _: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }

        fn action_source_slot(&self, _: ActionRef) -> Option<u64> {
            self.slot
        }

        fn player_advancement_outcome(
            &self,
            _: &AdvancementEvent,
            before: &PlayerAdvancementState,
            achieved_millis: u64,
        ) -> Result<AdvancementOutcome, ApplyFailure> {
            let mut state = before.clone();
            state.progress.insert(
                "test:reward".into(),
                AdvancementProgress {
                    criteria: BTreeMap::from([("done".into(), Some(achieved_millis))]),
                    completed: true,
                },
            );
            Ok(AdvancementOutcome {
                state,
                experience: 7,
            })
        }
    }

    fn fixture() -> (ActionRef, ActorId, ChunkKey, BTreeMap<Target, Cell>) {
        let actor = ActorId::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(2)));
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let reference = ActionRef {
            id: ActionId {
                actor,
                incarnation: 1,
                sequence: 1,
                family: 1,
            },
            tick: TickStamp(0),
        };
        let [progress, xp] = targets(actor, chunk);
        let values = BTreeMap::from([
            (
                progress,
                Cell {
                    revision: 2,
                    value: Value::LivingProperty(LivingPropertyValue::Advancements(
                        PlayerAdvancementState::default(),
                    )),
                },
            ),
            (
                xp,
                Cell {
                    revision: 3,
                    value: Value::LivingProperty(LivingPropertyValue::Experience(
                        PlayerExperienceState::from_total(10).unwrap(),
                    )),
                },
            ),
        ]);
        (reference, actor, chunk, values)
    }

    #[test]
    fn merges_winning_progress_and_experience_without_discarding_family_awards() {
        let (reference, actor, chunk, values) = fixture();
        let [progress, xp] = targets(actor, chunk);
        let mut previous = PlayerAdvancementState::default();
        previous.unlocked_recipes.insert("test:existing".into());
        let winning_xp = PlayerExperienceState::from_total(100)
            .unwrap()
            .spend_levels(2)
            .unwrap();
        let mut effects = vec![
            Effect::Put {
                target: progress,
                value: Value::LivingProperty(LivingPropertyValue::Advancements(previous)),
            },
            Effect::Put {
                target: xp,
                value: Value::LivingProperty(LivingPropertyValue::Experience(winning_xp)),
            },
        ];
        append_effects(
            &State { slot: Some(65536) },
            reference,
            actor,
            chunk,
            &values,
            &mut effects,
            &AdvancementEvent::SleptInBed,
        )
        .unwrap();
        assert_eq!(effects.len(), 2);
        let Effect::Put {
            value: Value::LivingProperty(LivingPropertyValue::Advancements(after)),
            ..
        } = &effects[0]
        else {
            panic!("missing progress")
        };
        assert!(after.unlocked_recipes.contains("test:existing"));
        assert_eq!(
            after.progress["test:reward"].criteria["done"],
            Some(3276800)
        );
        let Effect::Put {
            value: Value::LivingProperty(LivingPropertyValue::Experience(after)),
            ..
        } = effects[1]
        else {
            panic!("missing experience")
        };
        assert_eq!(after, winning_xp.collect(7).unwrap());
    }

    #[test]
    fn duplicate_family_writes_merge_into_the_last_winning_value() {
        let (reference, actor, chunk, values) = fixture();
        let xp = targets(actor, chunk)[1];
        let make = |total| Effect::Put {
            target: xp,
            value: Value::LivingProperty(LivingPropertyValue::Experience(
                PlayerExperienceState::from_total(total).unwrap(),
            )),
        };
        let mut effects = vec![make(20), make(100)];
        append_effects(
            &State { slot: Some(65536) },
            reference,
            actor,
            chunk,
            &values,
            &mut effects,
            &AdvancementEvent::SleptInBed,
        )
        .unwrap();
        let Value::LivingProperty(LivingPropertyValue::Experience(after)) =
            winning_value(&effects, xp, &values[&xp]).unwrap()
        else {
            panic!("missing winning experience")
        };
        assert_eq!(
            after,
            PlayerExperienceState::from_total(100)
                .unwrap()
                .collect(7)
                .unwrap()
        );
    }

    #[test]
    fn unavailable_full_source_slot_defers_without_partial_effects() {
        let (reference, actor, chunk, values) = fixture();
        let mut effects = Vec::new();
        assert_eq!(
            append_effects(
                &State { slot: None },
                reference,
                actor,
                chunk,
                &values,
                &mut effects,
                &AdvancementEvent::SleptInBed
            ),
            Err(ApplyFailure::MissingStakeholder(
                crate::canonical_action::Stakeholder::Chunk(chunk)
            ))
        );
        assert!(effects.is_empty());
    }

    #[test]
    fn experience_overflow_rejects_before_publishing_progress() {
        let (reference, actor, chunk, mut values) = fixture();
        let xp = targets(actor, chunk)[1];
        values.get_mut(&xp).unwrap().value =
            Value::LivingProperty(LivingPropertyValue::Experience(
                PlayerExperienceState::from_total(i32::MAX as u32).unwrap(),
            ));
        let mut effects = Vec::new();
        assert_eq!(
            append_effects(
                &State { slot: Some(65536) },
                reference,
                actor,
                chunk,
                &values,
                &mut effects,
                &AdvancementEvent::SleptInBed
            ),
            Err(ApplyFailure::Overflow(xp))
        );
        assert!(effects.is_empty());
    }
}
