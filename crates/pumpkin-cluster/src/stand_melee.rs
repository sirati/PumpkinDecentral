use crate::{
    action_command::{ActionFootprint, CommandContext},
    canonical_action::{ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Target, Value},
    combat_command::{ActorObservation, hydrate_property, winning_living},
    armor_stand::{StandProperty, StandPropertyValue},
    living_property::{LivingProperty, LivingPropertyValue},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StandMeleeOutcome {
    Refused,
    Wobble(AppliedWrite),
    Break { drop_items: bool },
}

impl StandMeleeOutcome {
    pub(crate) fn successful_contact(&self) -> bool {
        !matches!(self, Self::Refused)
    }
}

pub(crate) struct StandMeleeResolution {
    pub footprint: ActionFootprint,
    pub outcome: StandMeleeOutcome,
}

fn property(actor: &ActorObservation, property: LivingProperty) -> Target {
    Target::Chunk { key: actor.actor.chunk(), part: ChunkPart::LivingProperty { actor: actor.id(), property }, inventory: None }
}

pub(crate) fn resolve<S: ActionState>(
    context: &CommandContext<'_, S>,
    source: &ActorObservation,
    victim: &ActorObservation,
) -> Result<Option<StandMeleeResolution>, ApplyFailure> {
    let crate::action_identity::ActionIdentity::Issued(id) = context.identity else { return Err(ApplyFailure::InvalidAction); };
    if id.actor != source.id() || source.id() == victim.id() { return Err(ApplyFailure::InvalidAction); }
    let incarnation = match &source.actor { crate::action_command::ActorReference::Player { lease, .. } => lease.incarnation, crate::action_command::ActorReference::Entity { id, .. } => u64::from(id.generation) };
    if id.incarnation != incarnation { return Err(ApplyFailure::InvalidAction); }
    let mut target = winning_living(context, &victim.actor)?;
    if target.stand.is_none() { return Ok(None); }
    let mut attacker = winning_living(context, &source.actor)?;
    if attacker.avatar.is_none() { return Err(ApplyFailure::InvalidAction); }
    let target_properties = [LivingProperty::Health, LivingProperty::Invulnerable,
        LivingProperty::ArmorStand(StandProperty::Flags), LivingProperty::ArmorStand(StandProperty::LastHit)];
    for facet in target_properties { hydrate_property(context, &victim.actor, &mut target, facet)?; }
    hydrate_property(context, &source.actor, &mut attacker, LivingProperty::GameMode)?;
    let last_hit = property(victim, LivingProperty::ArmorStand(StandProperty::LastHit));
    let mut reads = std::collections::BTreeSet::from([source.target(), victim.target(), property(source, LivingProperty::GameMode)]);
    reads.extend(target_properties.map(|facet| property(victim, facet)));
    let footprint = ActionFootprint { counterfactual_reads: reads.clone(), reads,
        writes: std::collections::BTreeSet::from([last_hit]), counterfactual_writes: std::collections::BTreeSet::from([last_hit]) };
    for target in &footprint.reads { context.lookup(*target)?.ok_or(ApplyFailure::Condition(*target))?; }
    let stand = target.stand.as_ref().ok_or(ApplyFailure::Type(victim.target()))?;
    let mode = attacker.avatar.as_ref().ok_or(ApplyFailure::Type(source.target()))?.game_mode;
    let outcome = if target.combat.health_milli == 0 || target.combat.status.invulnerable || stand.flags & 0x10 != 0 || !matches!(mode, 0 | 1) {
        StandMeleeOutcome::Refused
    } else if mode == 1 {
        StandMeleeOutcome::Break { drop_items: false }
    } else if stand.last_hit.is_some_and(|previous| context.tick.distance_since(previous) <= 5) {
        StandMeleeOutcome::Break { drop_items: true }
    } else {
        let before = context.lookup(last_hit)?.ok_or(ApplyFailure::Condition(last_hit))?;
        if !matches!(&before.value, Value::LivingProperty(LivingPropertyValue::ArmorStand(Some(StandPropertyValue::LastHit(_))))) {
            return Err(ApplyFailure::Type(last_hit));
        }
        StandMeleeOutcome::Wobble(AppliedWrite { target: last_hit, before: Some(before.clone()), after: Some(Cell {
            revision: before.revision.checked_add(1).ok_or(ApplyFailure::Overflow(last_hit))?,
            value: Value::LivingProperty(LivingPropertyValue::ArmorStand(Some(StandPropertyValue::LastHit(Some(context.tick))))),
        }) })
    };
    Ok(Some(StandMeleeResolution { footprint, outcome }))
}
