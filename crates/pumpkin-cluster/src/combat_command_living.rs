use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::action_command::{ActionFootprint, ActorReference, CommandContext};
use crate::action_identity::ActionIdentity;
use crate::actor::ActorId;
use crate::canonical_action::{
    ActionId, ActionState, ApplyFailure, Cell, ChunkPart, Effect, Target,
};
use crate::combat_action::{
    CombatIntent, EnvironmentalDamageIntent, EnvironmentalHazardKind, EnvironmentalRules,
    LivingTickIdentity, LivingTickIntent, LivingTickRules, MAX_ENVIRONMENTAL_BLOCKS,
};
use crate::identity::PlayerSeq;
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::living_property::LivingProperty;
use crate::protocol::{BlockPos, ChunkKey};
use crate::time::TickStamp;
use crate::world_rules::WorldRulesWitness;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingObservation {
    pub actor: ActorReference,
    pub position: [u64; 3],
    pub sequence: PlayerSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingStepCommand {
    pub source: LivingObservation,
    pub rules: WorldRulesWitness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentalCommand {
    pub source: LivingObservation,
    pub rules: WorldRulesWitness,
    pub kind: EnvironmentalHazardKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivingRegistryFacts {
    pub attributes: Vec<u16>,
    pub armor: u16,
    pub toughness: u16,
    pub max_health: u16,
    pub regeneration: u16,
    pub poison: u16,
    pub wither: u16,
    pub hunger: u16,
    pub fire_immune: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentalRegistryFacts {
    pub half_width: u64,
    pub height: u64,
    pub eye_height: u64,
    pub shape_properties: Vec<LivingProperty>,
    pub armor: u16,
    pub toughness: u16,
    pub water_breathing: u16,
    pub fire_immune: bool,
    pub can_breathe_water: bool,
    pub freeze_immune: bool,
}

pub trait LivingCommandState: ActionState {
    fn living_registry(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<LivingRegistryFacts, ApplyFailure>;

    fn environmental_registry(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<EnvironmentalRegistryFacts, ApplyFailure>;

    fn previous_environment_position(
        &self,
        source: &LivingObservation,
        tick: TickStamp,
    ) -> Result<[u64; 3], ApplyFailure>;
}

impl LivingObservation {
    pub fn identity(&self, tick: TickStamp) -> Result<LivingTickIdentity, ApplyFailure> {
        let (identity, epoch) = match &self.actor {
            ActorReference::Player { lease, epoch, .. } => {
                (ContactIdentity::Player(*lease), *epoch)
            }
            ActorReference::Entity {
                id, birth, epoch, ..
            } => (
                ContactIdentity::Entity {
                    id: *id,
                    birth: birth.clone(),
                },
                *epoch,
            ),
        };
        let value = LivingTickIdentity {
            identity,
            owner: self.actor.owner(),
            epoch,
            chunk: self.actor.chunk(),
            simulation_tick: tick,
        };
        if !value.valid()
            || self
                .position
                .into_iter()
                .map(f64::from_bits)
                .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 30_000_000.0)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(value)
    }
}

fn issued<S>(
    context: &CommandContext<'_, S>,
    identity: &LivingTickIdentity,
) -> Result<ActionId, ApplyFailure> {
    let ActionIdentity::Issued(id) = context.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    let incarnation = match &identity.identity {
        ContactIdentity::Player(lease) => lease.incarnation,
        ContactIdentity::Entity { id, .. } => u64::from(id.generation),
    };
    if id.actor != identity.actor() || id.incarnation != incarnation {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(id)
}

fn elapsed(previous: Option<TickStamp>, tick: TickStamp) -> Result<u16, ApplyFailure> {
    let distance = previous.map_or(1, |previous| tick.distance_since(previous));
    if distance == 0 || distance >= 0x8000 {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(distance)
}

fn input_footprint(intent: &CombatIntent, identity: &LivingTickIdentity) -> ActionFootprint {
    let mut reads = intent.resolution_targets();
    let permanent = reads
        .iter()
        .filter_map(|target| match *target {
            Target::Chunk {
                key,
                part:
                    ChunkPart::LivingProperty {
                        actor,
                        property: LivingProperty::Attribute(id),
                    },
                inventory: None,
            } => Some(Target::Chunk {
                key,
                part: ChunkPart::LivingProperty {
                    actor,
                    property: LivingProperty::PermanentAttributeModifiers(id),
                },
                inventory: None,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    reads.extend(permanent);
    reads.insert(identity.target());
    reads.insert(simulation_target(identity));
    ActionFootprint {
        reads,
        writes: possible_writes(intent),
        counterfactual_reads: BTreeSet::new(),
        counterfactual_writes: BTreeSet::new(),
    }
}

impl LivingStepCommand {
    fn legacy<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<CombatIntent, ApplyFailure> {
        let identity = self.source.identity(context.tick)?;
        issued(context, &identity)?;
        if self.rules.rules.world != identity.chunk.world {
            return Err(ApplyFailure::InvalidAction);
        }
        let policy = context.state.world_rules(self.rules, context.tick)?;
        let facts = context
            .state
            .living_registry(&self.source, context.tick, context.values)?;
        let previous_tick = previous_tick(context.values, &identity)?;
        elapsed(previous_tick, context.tick)?;
        let intent = CombatIntent {
            ticks: vec![LivingTickIntent {
                rules: LivingTickRules {
                    minimum_y: policy.min_y,
                    fire_damage: policy.fire_damage
                        || matches!(identity.actor(), ActorId::Entity(_)),
                    difficulty: policy.difficulty,
                    natural_regeneration: policy.natural_regeneration,
                    max_health_attribute: facts.max_health,
                    regeneration_effect: facts.regeneration,
                    poison_effect: facts.poison,
                    wither_effect: facts.wither,
                    hunger_effect: facts.hunger,
                },
                identity,
                effective_position: Some(self.source.position),
                previous_tick,
                attribute_ids: facts.attributes,
                fire_immune: facts.fire_immune,
                armor_attribute: facts.armor,
                toughness_attribute: facts.toughness,
            }],
            ..CombatIntent::default()
        };
        if !intent.is_valid(context.tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(intent)
    }

    pub fn footprint<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<ActionFootprint, ApplyFailure> {
        resolved_footprint(
            &self.legacy(context)?,
            &self.source.identity(context.tick)?,
            context,
        )
    }

    pub fn derive<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        let intent = self.legacy(context)?;
        let identity = self.source.identity(context.tick)?;
        numeric_effects(&intent, &identity, context)
    }
}

impl EnvironmentalCommand {
    fn legacy<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<CombatIntent, ApplyFailure> {
        let identity = self.source.identity(context.tick)?;
        issued(context, &identity)?;
        if self.rules.rules.world != identity.chunk.world {
            return Err(ApplyFailure::InvalidAction);
        }
        let policy = context.state.world_rules(self.rules, context.tick)?;
        let facts =
            context
                .state
                .environmental_registry(&self.source, context.tick, context.values)?;
        let elapsed = elapsed(previous_tick(context.values, &identity)?, context.tick)?;
        let actor = identity.actor();
        let contact = ContactObservation {
            identity: identity.identity,
            owner: identity.owner,
            epoch: identity.epoch,
            world: identity.chunk.world,
            chunk: identity.chunk,
            position: self.source.position,
            half_width: facts.half_width,
            height: facts.height,
            simulation_tick: context.tick,
            observed_sequence: self.source.sequence,
        };
        if !contact.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let position = self.source.position.map(f64::from_bits);
        let radius = f64::from_bits(facts.half_width);
        let height = f64::from_bits(facts.height);
        let eye_height = f64::from_bits(facts.eye_height);
        if !eye_height.is_finite() || eye_height < 0.0 || eye_height > height {
            return Err(ApplyFailure::InvalidAction);
        }
        let moved = if self.kind == EnvironmentalHazardKind::SweetBerry {
            let previous = context
                .state
                .previous_environment_position(&self.source, context.tick)?;
            if previous
                .into_iter()
                .map(f64::from_bits)
                .any(|value| !value.is_finite())
            {
                return Err(ApplyFailure::InvalidAction);
            }
            previous != self.source.position
        } else {
            true
        };
        let minimum = [
            (position[0] - radius).floor() as i32,
            position[1].floor() as i32,
            (position[2] - radius).floor() as i32,
        ];
        let maximum = [
            (position[0] + radius).ceil() as i32,
            (position[1] + height).ceil() as i32,
            (position[2] + radius).ceil() as i32,
        ];
        let count = (0..3)
            .try_fold(1_usize, |count, axis| {
                count.checked_mul((maximum[axis] - minimum[axis]).max(0) as usize)
            })
            .ok_or(ApplyFailure::ResourceLimit)?;
        if count > MAX_ENVIRONMENTAL_BLOCKS {
            return Err(ApplyFailure::ResourceLimit);
        }
        let upper = policy
            .min_y
            .checked_add(policy.height as i32)
            .ok_or(ApplyFailure::InvalidAction)?;
        let mut blocks = BTreeSet::new();
        if moved {
            for x in minimum[0]..maximum[0] {
                for y in minimum[1].max(policy.min_y)..maximum[1].min(upper) {
                    if self.kind == EnvironmentalHazardKind::Suffocation
                        && y != (position[1] + eye_height).floor() as i32
                    {
                        continue;
                    }
                    for z in minimum[2]..maximum[2] {
                        blocks.insert(block(contact.world, x, y, z));
                    }
                }
            }
        }
        let mut support = BTreeSet::new();
        if self.kind == EnvironmentalHazardKind::Water
            && (policy.min_y..upper).contains(&maximum[1])
        {
            for x in minimum[0]..maximum[0] {
                for z in minimum[2]..maximum[2] {
                    support.insert(block(contact.world, x, maximum[1], z));
                }
            }
        }
        if blocks.is_empty()
            && !(matches!(
                self.kind,
                EnvironmentalHazardKind::Water | EnvironmentalHazardKind::PowderSnow
            ) && (position[1] + height <= f64::from(policy.min_y)
                || position[1] >= f64::from(upper)))
        {
            return Ok(CombatIntent::default());
        }
        let intent = CombatIntent {
            environment: vec![EnvironmentalDamageIntent {
                contact,
                kind: self.kind,
                elapsed,
                world_rules: Some(self.rules),
                blocks: blocks.into_iter().collect(),
                support_blocks: support.into_iter().collect(),
                shape_properties: facts.shape_properties,
                armor_attribute: facts.armor,
                toughness_attribute: facts.toughness,
                fire_immune: facts.fire_immune,
                rules: EnvironmentalRules {
                    fire_damage: policy.fire_damage || matches!(actor, ActorId::Entity(_)),
                    drowning_damage: policy.drowning_damage || matches!(actor, ActorId::Entity(_)),
                    freeze_damage: policy.freeze_damage,
                    water_breathing_effect: facts.water_breathing,
                    can_breathe_water: facts.can_breathe_water,
                    freeze_immune: facts.freeze_immune,
                },
            }],
            ..CombatIntent::default()
        };
        if !intent.is_valid(context.tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(intent)
    }

    pub fn footprint<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<ActionFootprint, ApplyFailure> {
        resolved_footprint(
            &self.legacy(context)?,
            &self.source.identity(context.tick)?,
            context,
        )
    }

    pub fn derive<S: LivingCommandState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        let intent = self.legacy(context)?;
        if intent.environment.is_empty() {
            return Ok(Vec::new());
        }
        let identity = self.source.identity(context.tick)?;
        let values = bounded_values(&intent, &identity, context.values)?;
        intent.derive_with(
            context.state,
            issued(context, &identity)?,
            context.tick,
            &values,
        )
    }
}

fn block(world: crate::protocol::WorldId, x: i32, y: i32, z: i32) -> Target {
    Target::Chunk {
        key: ChunkKey::new(world, x.div_euclid(16), z.div_euclid(16)),
        part: ChunkPart::Block(BlockPos { x, y, z }),
        inventory: None,
    }
}

fn simulation_target(identity: &LivingTickIdentity) -> Target {
    Target::Chunk {
        key: identity.chunk,
        part: ChunkPart::LivingProperty {
            actor: identity.actor(),
            property: LivingProperty::SimulationTick,
        },
        inventory: None,
    }
}

fn previous_tick(
    values: &BTreeMap<Target, Cell>,
    identity: &LivingTickIdentity,
) -> Result<Option<TickStamp>, ApplyFailure> {
    let target = simulation_target(identity);
    match values.get(&target).map(|cell| &cell.value) {
        Some(crate::canonical_action::Value::LivingProperty(
            crate::living_property::LivingPropertyValue::OptionalTick(value),
        )) => Ok(*value),
        Some(_) => Err(ApplyFailure::Type(target)),
        None => Err(ApplyFailure::Condition(target)),
    }
}

fn bounded_values(
    intent: &CombatIntent,
    identity: &LivingTickIdentity,
    original: &BTreeMap<Target, Cell>,
) -> Result<BTreeMap<Target, Cell>, ApplyFailure> {
    let footprint = input_footprint(intent, identity);
    let mut values = footprint
        .reads
        .iter()
        .map(|target| {
            original
                .get(target)
                .cloned()
                .map(|cell| (*target, cell))
                .ok_or(ApplyFailure::Condition(*target))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let body_target = identity.target();
    let mut body = values
        .get(&body_target)
        .cloned()
        .ok_or(ApplyFailure::Condition(body_target))?;
    let living = crate::living_property::state_mut(&mut body.value)
        .ok_or(ApplyFailure::Type(body_target))?;
    for permanent in [true, false] {
        for (target, cell) in &values {
            if let Target::Chunk {
                key,
                part: ChunkPart::LivingProperty { actor, property },
                inventory: None,
            } = target
            {
                if *key != identity.chunk
                    || *actor != identity.actor()
                    || matches!(property, LivingProperty::PermanentAttributeModifiers(_))
                        != permanent
                {
                    continue;
                }
                let crate::canonical_action::Value::LivingProperty(value) = &cell.value else {
                    return Err(ApplyFailure::Type(*target));
                };
                if !property.hydrate(living, value) {
                    return Err(ApplyFailure::Type(*target));
                }
            }
        }
    }
    values.insert(body_target, body);
    Ok(values)
}

fn possible_writes(intent: &CombatIntent) -> BTreeSet<Target> {
    use LivingProperty as P;
    intent
        .property_targets()
        .into_iter()
        .filter(|target| {
            let Target::Chunk {
                part: ChunkPart::LivingProperty { property, .. },
                ..
            } = target
            else {
                return false;
            };
            matches!(
                property,
                P::Health
                    | P::Absorption
                    | P::LastDamage
                    | P::LastDamageKind
                    | P::HurtStarted
                    | P::HurtTicks
                    | P::FireTicks
                    | P::DeathTicks
                    | P::DeathCause
                    | P::LastAttacker
                    | P::ItemUse
            ) || (!intent.ticks.is_empty()
                && matches!(
                    property,
                    P::SimulationTick
                        | P::AttackTicks
                        | P::StatusEffects
                        | P::Food
                        | P::Strength
                        | P::Weakness
                        | P::Resistance
                        | P::FireResistance
                        | P::Blindness
                        | P::MobReproduction
                        | P::Attribute(_)
                ))
                || (!intent.environment.is_empty() && *property == P::Environment)
        })
        .collect()
}

fn numeric_effects<S: LivingCommandState>(
    intent: &CombatIntent,
    identity: &LivingTickIdentity,
    context: &CommandContext<'_, S>,
) -> Result<Vec<Effect>, ApplyFailure> {
    let tick = intent.ticks.first().ok_or(ApplyFailure::InvalidAction)?;
    if intent.ticks.len() != 1
        || tick.attribute_ids != context.state.inventory_equipment_attribute_ids()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let body = context
        .values
        .get(&identity.target())
        .ok_or(ApplyFailure::Condition(identity.target()))?;
    let living =
        crate::living_property::state(&body.value).ok_or(ApplyFailure::Type(identity.target()))?;
    let mut draft = crate::living_numeric::NumericLivingDraft::new(
        crate::living_numeric::NumericLivingView::new(living),
    );
    let reads = input_footprint(intent, identity).reads;
    for permanent in [true, false] {
        for target in &reads {
            if *target == identity.target() {
                continue;
            }
            let (body, property) =
                crate::living_property::body_target(*target).ok_or(ApplyFailure::Type(*target))?;
            if body != identity.target() {
                return Err(ApplyFailure::InvalidAction);
            }
            if matches!(property, LivingProperty::PermanentAttributeModifiers(_)) != permanent {
                continue;
            }
            let cell = context
                .values
                .get(target)
                .ok_or(ApplyFailure::Condition(*target))?;
            let crate::canonical_action::Value::LivingProperty(value) = &cell.value else {
                return Err(ApplyFailure::Type(*target));
            };
            if !draft.hydrate(property, value) {
                return Err(ApplyFailure::Type(*target));
            }
        }
    }
    draft.advance(tick, Some(context.state))?;
    let mut effects = Vec::new();
    for target in possible_writes(intent) {
        let (_, property) =
            crate::living_property::body_target(target).ok_or(ApplyFailure::Type(target))?;
        let Some(value) = draft.output(property) else {
            continue;
        };
        let before = context
            .values
            .get(&target)
            .ok_or(ApplyFailure::Condition(target))?;
        let value = crate::canonical_action::Value::LivingProperty(value);
        if before.value != value {
            effects.push(Effect::Put { target, value });
        }
    }
    crate::combat_action::append_death_causes(
        issued(context, identity)?,
        context.tick,
        context.values,
        &mut effects,
    )?;
    Ok(effects)
}

fn resolved_footprint<S: LivingCommandState>(
    intent: &CombatIntent,
    identity: &LivingTickIdentity,
    context: &CommandContext<'_, S>,
) -> Result<ActionFootprint, ApplyFailure> {
    let mut footprint = input_footprint(intent, identity);
    let effects = if intent.ticks.is_empty() && intent.environment.is_empty() {
        Vec::new()
    } else if !intent.ticks.is_empty() {
        numeric_effects(intent, identity, context)?
    } else {
        let values = bounded_values(intent, identity, context.values)?;
        intent.derive_with(
            context.state,
            issued(context, identity)?,
            context.tick,
            &values,
        )?
    };
    let writes = effects
        .iter()
        .map(|effect| match effect {
            Effect::Put { target, .. } => Ok(*target),
            _ => Err(ApplyFailure::InvalidAction),
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if !writes.is_subset(&footprint.writes) {
        return Err(ApplyFailure::InvalidAction);
    }
    footprint.counterfactual_writes = footprint.writes.difference(&writes).copied().collect();
    footprint.writes = writes;
    Ok(footprint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_action::Value;
    use crate::combat_action::{CombatAttributeModifier, CombatModifierOperation};
    use crate::living_property::LivingPropertyValue;

    #[test]
    fn temporary_hydration_uses_winning_combined_attributes_and_health() {
        let (_, mut living, contact) = crate::combat_action::tests::fixture(1);
        living
            .combat
            .effective_attributes
            .insert(7, 20.0_f64.to_bits());
        living
            .combat
            .effective_attributes
            .insert(8, 0.0_f64.to_bits());
        let old = CombatAttributeModifier {
            attribute: 7,
            slot: 7,
            id: "gear".into(),
            amount: 5.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        };
        living.permanent_attribute_modifiers.insert(7, vec![old]);
        let identity = LivingTickIdentity::from_contact(&contact);
        let root = Cell {
            revision: 0,
            value: Value::Living((living).into()),
        };
        let intent = CombatIntent {
            environment: vec![EnvironmentalDamageIntent {
                contact,
                kind: EnvironmentalHazardKind::Fire,
                elapsed: 1,
                world_rules: None,
                blocks: Vec::new(),
                support_blocks: Vec::new(),
                shape_properties: Vec::new(),
                armor_attribute: 7,
                toughness_attribute: 8,
                fire_immune: false,
                rules: EnvironmentalRules {
                    fire_damage: true,
                    drowning_damage: true,
                    freeze_damage: true,
                    water_breathing_effect: 1,
                    can_breathe_water: false,
                    freeze_immune: false,
                },
            }],
            ..CombatIntent::default()
        };
        let mut values = input_footprint(&intent, &identity)
            .reads
            .into_iter()
            .map(|target| {
                let value = if target == identity.target() {
                    root.clone()
                } else {
                    let (_, property) = crate::living_property::body_target(target).unwrap();
                    crate::derived_living::facet_cell(&root, property).unwrap()
                };
                (target, value)
            })
            .collect::<BTreeMap<_, _>>();
        let attribute = Target::Chunk {
            key: identity.chunk,
            part: ChunkPart::LivingProperty {
                actor: identity.actor(),
                property: LivingProperty::Attribute(7),
            },
            inventory: None,
        };
        let combined = CombatAttributeModifier {
            attribute: 7,
            slot: 7,
            id: "gear".into(),
            amount: 10.0_f64.to_bits(),
            operation: CombatModifierOperation::Add,
        };
        let permanent = Target::Chunk {
            key: identity.chunk,
            part: ChunkPart::LivingProperty {
                actor: identity.actor(),
                property: LivingProperty::PermanentAttributeModifiers(7),
            },
            inventory: None,
        };
        values.insert(
            permanent,
            Cell {
                revision: 1,
                value: Value::LivingProperty(LivingPropertyValue::PermanentAttributeModifiers(
                    vec![combined.clone()],
                )),
            },
        );
        values.insert(
            attribute,
            Cell {
                revision: 1,
                value: Value::LivingProperty(LivingPropertyValue::Attribute {
                    base: Some(20.0_f64.to_bits()),
                    modifiers: vec![combined],
                    transient_modifiers: Vec::new(),
                }),
            },
        );
        let health = Target::Chunk {
            key: identity.chunk,
            part: ChunkPart::LivingProperty {
                actor: identity.actor(),
                property: LivingProperty::Health,
            },
            inventory: None,
        };
        values.insert(
            health,
            Cell {
                revision: 2,
                value: Value::LivingProperty(LivingPropertyValue::Unsigned(8000)),
            },
        );
        let hydrated = bounded_values(&intent, &identity, &values).unwrap();
        let state = crate::living_property::state(&hydrated[&identity.target()].value).unwrap();
        assert_eq!(state.attribute(7, 20.0), 30.0);
        assert_eq!(state.combat.health_milli, 8000);
        let original = crate::living_property::state(&values[&identity.target()].value).unwrap();
        assert_eq!(original.attribute(7, 20.0), 25.0);
        let possible = possible_writes(&intent);
        assert!(!possible.contains(&attribute));
        assert!(possible.contains(&health));
        assert!(!possible.iter().any(|target| matches!(
            target,
            Target::Chunk {
                part: ChunkPart::LivingProperty {
                    property: LivingProperty::GameMode
                        | LivingProperty::Invulnerable
                        | LivingProperty::ArmorProtection,
                    ..
                },
                ..
            }
        )));
    }
}

#[cfg(test)]
mod footprint_tests {
    use super::*;
    use crate::canonical_action::{ActionContext, AppliedWrite, TruthView, Value};
    use crate::combat_action::{CombatAttributeModifier, CombatStatusFacts};

    struct Registry;
    impl ActionState for Registry {
        fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
            vec![0, 1, 2]
        }
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
        fn status_effect_facts(
            &self,
            _: &crate::active_effects::ActiveEffectSet,
        ) -> Result<CombatStatusFacts, ApplyFailure> {
            Ok(Default::default())
        }
        fn status_effect_modifiers(
            &self,
            _: &crate::active_effects::ActiveEffectSet,
        ) -> Result<Vec<CombatAttributeModifier>, ApplyFailure> {
            Ok(Vec::new())
        }
    }
    impl LivingCommandState for Registry {
        fn living_registry(
            &self,
            _: &LivingObservation,
            _: TickStamp,
            _: &BTreeMap<Target, Cell>,
        ) -> Result<LivingRegistryFacts, ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn environmental_registry(
            &self,
            _: &LivingObservation,
            _: TickStamp,
            _: &BTreeMap<Target, Cell>,
        ) -> Result<EnvironmentalRegistryFacts, ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn previous_environment_position(
            &self,
            _: &LivingObservation,
            _: TickStamp,
        ) -> Result<[u64; 3], ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
    }

    #[test]
    fn health_moves_between_factual_and_counterfactual_sets_when_fire_is_present() {
        for burning in [false, true] {
            let (lease, mut living, contact) = crate::combat_action::tests::fixture(1);
            for id in [0, 1, 2] {
                living.combat.effective_attributes.insert(
                    id,
                    if id == 2 {
                        20.0_f64.to_bits()
                    } else {
                        0.0_f64.to_bits()
                    },
                );
            }
            living.combat.timing.fire_ticks = if burning { 20 } else { 0 };
            let identity = LivingTickIdentity::from_contact(&contact);
            let intent = CombatIntent {
                ticks: vec![LivingTickIntent {
                    identity: identity.clone(),
                    effective_position: Some(contact.position),
                    previous_tick: None,
                    attribute_ids: vec![0, 1, 2],
                    fire_immune: false,
                    armor_attribute: 0,
                    toughness_attribute: 1,
                    rules: LivingTickRules {
                        minimum_y: -64,
                        fire_damage: true,
                        difficulty: 2,
                        natural_regeneration: false,
                        max_health_attribute: 2,
                        regeneration_effect: 0,
                        poison_effect: 1,
                        wither_effect: 2,
                        hunger_effect: 3,
                    },
                }],
                ..CombatIntent::default()
            };
            let body = Cell {
                revision: 0,
                value: Value::Living(living.into()),
            };
            let values = input_footprint(&intent, &identity)
                .reads
                .into_iter()
                .map(|target| {
                    let cell = if target == identity.target() {
                        body.clone()
                    } else {
                        let (_, property) = crate::living_property::body_target(target).unwrap();
                        crate::derived_living::facet_cell(&body, property).unwrap()
                    };
                    (target, cell)
                })
                .collect();
            let known_absent = BTreeSet::new();
            let context = CommandContext {
                known_absent: &known_absent,
                state: &Registry,
                tick: contact.simulation_tick,
                identity: ActionIdentity::Issued(ActionId {
                    actor: identity.actor(),
                    incarnation: lease.incarnation,
                    sequence: 1,
                    family: 13,
                }),
                values: &values,
            };
            let footprint = resolved_footprint(&intent, &identity, &context).unwrap();
            let health = Target::Chunk {
                key: identity.chunk,
                part: ChunkPart::LivingProperty {
                    actor: identity.actor(),
                    property: LivingProperty::Health,
                },
                inventory: None,
            };
            assert_eq!(footprint.writes.contains(&health), burning);
            assert_eq!(footprint.counterfactual_writes.contains(&health), !burning);
            assert!(
                footprint
                    .writes
                    .is_disjoint(&footprint.counterfactual_writes)
            );
            assert!(footprint.counterfactual_reads.is_empty());
            for property in [
                LivingProperty::GameMode,
                LivingProperty::Invulnerable,
                LivingProperty::ArmorProtection,
            ] {
                let target = Target::Chunk {
                    key: identity.chunk,
                    part: ChunkPart::LivingProperty {
                        actor: identity.actor(),
                        property,
                    },
                    inventory: None,
                };
                assert!(footprint.reads.contains(&target));
                assert!(!footprint.writes.contains(&target));
                assert!(!footprint.counterfactual_writes.contains(&target));
            }
        }
    }
}
