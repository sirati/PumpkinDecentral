use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{
    ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Condition, Effect, Target, Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldLightMode {
    Default,
    Full,
    Dark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldLightPlan {
    pub center: crate::protocol::ChunkKey,
    pub min_section_y: i32,
    pub section_count: u16,
    pub has_skylight: bool,
    pub mode: WorldLightMode,
    pub config_version: u64,
}

impl WorldLightPlan {
    pub fn config_fingerprint(self) -> u64 {
        let mode = match self.mode {
            WorldLightMode::Default => 0,
            WorldLightMode::Full => 1,
            WorldLightMode::Dark => 2,
        };
        let mut value = 0xcbf29ce484222325_u64;
        for field in [
            u64::from(self.center.world.key),
            u64::from(self.center.world.dimension),
            self.min_section_y as u32 as u64,
            u64::from(self.section_count),
            u64::from(self.has_skylight),
            mode,
        ] {
            for byte in field.to_le_bytes() {
                value = (value ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
        }
        value.max(1)
    }

    pub fn with_config_fingerprint(mut self) -> Self {
        self.config_version = self.config_fingerprint();
        self
    }

    pub fn section_targets(self) -> Result<Vec<Target>, ApplyFailure> {
        if self.section_count == 0 || self.section_count > 256 || self.config_version == 0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut targets = Vec::with_capacity(usize::from(self.section_count) * 9);
        for z in -1..=1 {
            for x in -1..=1 {
                let key = crate::protocol::ChunkKey::new(
                    self.center.world,
                    self.center
                        .x()
                        .checked_add(x)
                        .ok_or(ApplyFailure::InvalidAction)?,
                    self.center
                        .z()
                        .checked_add(z)
                        .ok_or(ApplyFailure::InvalidAction)?,
                );
                for section in 0..self.section_count {
                    targets.push(Target::Chunk {
                        key,
                        part: ChunkPart::BlockSection(
                            self.min_section_y
                                .checked_add(i32::from(section))
                                .ok_or(ApplyFailure::InvalidAction)?,
                        ),
                        inventory: None,
                    });
                }
            }
        }
        Ok(targets)
    }
    pub fn light_targets(self) -> Result<Vec<Target>, ApplyFailure> {
        self.section_targets()?;
        Ok((0..self.section_count)
            .map(|section| Target::Chunk {
                key: self.center,
                part: ChunkPart::LightSection(self.min_section_y + i32::from(section)),
                inventory: None,
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorldReplayIntent {
    pub steps: Vec<WorldReplayStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorldReplayStep {
    MenuSession(crate::canonical_menu::MenuSessionIntent),
    BlockInventoryTick(crate::canonical_block_inventory_tick::BlockInventoryTickIntent),
    WorldFluidRules {
        source: crate::canonical_action::ActionRef,
        witness: crate::world_rules::WorldRulesWitness,
        water_source_conversion: bool,
        lava_source_conversion: bool,
    },
    RequiredEffects {
        conditions: Vec<Condition>,
        effects: Vec<Effect>,
    },
    EvacuateInventory(crate::inventory_evacuation::InventoryEvacuationIntent),
    DerivedLight(WorldLightPlan),
    ExhaustFood(crate::combat_action::FoodExhaustionIntent),
    TransformBlock {
        target: Target,
        cases: Vec<BlockStateTransition>,
    },
    ConditionalEffects {
        conditions: Vec<Condition>,
        matches: Vec<Effect>,
        otherwise: Vec<Effect>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStateTransition {
    pub before: u16,
    pub after: u16,
}

impl WorldReplayIntent {
    pub fn read_targets(&self) -> BTreeSet<Target> {
        self.steps
            .iter()
            .flat_map(|step| match step {
                WorldReplayStep::MenuSession(intent) => intent.targets(),
                WorldReplayStep::BlockInventoryTick(intent) => intent.targets(),
                WorldReplayStep::WorldFluidRules { .. } => Vec::new(),
                WorldReplayStep::RequiredEffects { conditions, .. } => conditions
                    .iter()
                    .map(|condition| condition.target)
                    .collect(),
                WorldReplayStep::EvacuateInventory(intent) => {
                    intent.read_targets().into_iter().collect()
                }
                WorldReplayStep::DerivedLight(plan) => plan
                    .section_targets()
                    .unwrap_or_default()
                    .into_iter()
                    .chain(plan.light_targets().unwrap_or_default())
                    .collect(),
                WorldReplayStep::ExhaustFood(intent) => intent
                    .targets()
                    .into_iter()
                    .chain([intent.target()])
                    .collect(),
                WorldReplayStep::TransformBlock { target, .. } => vec![*target],
                WorldReplayStep::ConditionalEffects { conditions, .. } => conditions
                    .iter()
                    .map(|condition| condition.target)
                    .collect(),
            })
            .collect()
    }

    pub fn effect_targets(&self) -> BTreeSet<Target> {
        self.steps
            .iter()
            .flat_map(|step| match step {
                WorldReplayStep::MenuSession(intent) => intent.effect_targets(),
                WorldReplayStep::BlockInventoryTick(intent) => intent.effect_targets(),
                WorldReplayStep::WorldFluidRules { .. } => Vec::new(),
                WorldReplayStep::RequiredEffects { effects, .. } => {
                    effects.iter().flat_map(Effect::targets).collect()
                }
                WorldReplayStep::EvacuateInventory(intent) => {
                    intent.effect_targets().into_iter().collect()
                }
                WorldReplayStep::DerivedLight(plan) => plan.light_targets().unwrap_or_default(),
                WorldReplayStep::ExhaustFood(intent) => vec![intent.targets()[0]],
                WorldReplayStep::TransformBlock { target, .. } => vec![*target],
                WorldReplayStep::ConditionalEffects {
                    matches, otherwise, ..
                } => matches
                    .iter()
                    .chain(otherwise)
                    .flat_map(Effect::targets)
                    .collect(),
            })
            .collect()
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> {
        let mut targets = self.read_targets();
        targets.extend(self.effect_targets());
        targets.into_iter()
    }

    pub fn valid(&self) -> bool {
        if self.steps.is_empty() || self.steps.len() > 4096 {
            return false;
        }
        let mut count = 0usize;
        for step in &self.steps {
            match step {
                WorldReplayStep::MenuSession(intent) => {
                    if !intent.valid() {
                        return false;
                    }
                    count = count.saturating_add(intent.targets().len());
                }
                WorldReplayStep::BlockInventoryTick(intent) => {
                    if !intent.valid() {
                        return false;
                    }
                    count = count.saturating_add(intent.targets().len());
                }
                WorldReplayStep::WorldFluidRules {
                    source, witness, ..
                } => {
                    if source.id.incarnation == 0
                        || witness.slot as u16 != source.tick.0
                        || witness.rules.epoch == 0
                        || witness.rules.issuer.generation == 0
                    {
                        return false;
                    }
                    count = count.saturating_add(1);
                }
                WorldReplayStep::RequiredEffects {
                    conditions,
                    effects,
                } => {
                    if conditions.is_empty()
                        || effects.is_empty()
                        || conditions.len() > 4096
                        || effects.len() > 4096
                    {
                        return false;
                    }
                    count = count
                        .saturating_add(conditions.len())
                        .saturating_add(effects.len());
                }
                WorldReplayStep::EvacuateInventory(intent) => {
                    if !intent.valid() {
                        return false;
                    }
                    count = count.saturating_add(intent.read_targets().len());
                }
                WorldReplayStep::DerivedLight(plan) => {
                    let Ok(targets) = plan.section_targets() else {
                        return false;
                    };
                    count = count.saturating_add(targets.len());
                }
                WorldReplayStep::ExhaustFood(intent) => {
                    if !intent.is_valid() {
                        return false;
                    }
                    count = count.saturating_add(3);
                }
                WorldReplayStep::TransformBlock { target, cases } => {
                    if !matches!(
                        target,
                        Target::Chunk {
                            part: crate::canonical_action::ChunkPart::Block(_),
                            inventory: None,
                            ..
                        }
                    ) || cases.is_empty()
                        || cases.len() > 4096
                    {
                        return false;
                    }
                    let before: BTreeSet<_> = cases.iter().map(|case| case.before).collect();
                    if before.len() != cases.len() {
                        return false;
                    }
                    count = count.saturating_add(cases.len());
                }
                WorldReplayStep::ConditionalEffects {
                    conditions,
                    matches,
                    otherwise,
                } => {
                    if conditions.is_empty()
                        || conditions.len() > 4096
                        || matches.len() > 4096
                        || otherwise.len() > 4096
                    {
                        return false;
                    }
                    count = count
                        .saturating_add(conditions.len())
                        .saturating_add(matches.len())
                        .saturating_add(otherwise.len());
                }
            }
        }
        count <= 16_384 && self.targets().all(Target::is_valid)
    }

    pub fn guards_valid(&self, action: &crate::canonical_action::Action) -> bool {
        self.steps.iter().all(|step| match step {
            WorldReplayStep::MenuSession(intent) => intent.guards_valid(action),
            WorldReplayStep::BlockInventoryTick(intent) => intent.guards_valid(action),
            WorldReplayStep::WorldFluidRules { source, witness, .. } =>
                *source == crate::canonical_action::ActionRef { id: action.id, tick: action.tick }
                    && action.actor_chunk.is_some_and(|chunk| chunk.world == witness.rules.world),
            WorldReplayStep::RequiredEffects { conditions, .. } => conditions.iter().all(|required| action.conditions.iter().any(|guard| guard.target == required.target && (guard.expected == required.expected || matches!(&guard.expected, crate::canonical_action::Expected::Exact(cell) if required.expected.matches(Some(cell)))))),
            WorldReplayStep::EvacuateInventory(intent) => intent.guards_valid(action),
            WorldReplayStep::ExhaustFood(intent) => {
                action.id.actor == crate::actor::ActorId::Player(intent.lease.gid)
                    && action.id.incarnation == intent.lease.incarnation
                    && action.tick == intent.tick
                    && action.actor_chunk == Some(intent.chunk)
                    && action.player_leases.contains(&intent.lease)
                    && action.conditions.contains(&intent.guard())
            }
            _ => true,
        })
    }

    pub fn derive_effects(
        &self,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.derive_effects_with(
            values,
            &crate::canonical_action::OwnedStakeholderSet::default(),
        )
    }

    pub fn derive_effects_with<S: ActionState>(
        &self,
        values: &BTreeMap<Target, Cell>,
        state: &S,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut effects = Vec::new();
        for step in &self.steps {
            match step {
                WorldReplayStep::MenuSession(intent) => effects.extend(intent.derive(values)?),
                WorldReplayStep::BlockInventoryTick(intent) => {
                    effects.extend(intent.derive(state, values)?)
                }
                WorldReplayStep::WorldFluidRules {
                    source,
                    witness,
                    water_source_conversion,
                    lava_source_conversion,
                } => {
                    let rules = state.world_rules(*witness, source.tick)?;
                    if rules.water_source_conversion != *water_source_conversion
                        || rules.lava_source_conversion != *lava_source_conversion
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                WorldReplayStep::RequiredEffects {
                    conditions,
                    effects: required,
                } => {
                    if let Some(condition) = conditions.iter().find(|condition| {
                        !condition.expected.matches(values.get(&condition.target))
                    }) {
                        return Err(ApplyFailure::Condition(condition.target));
                    }
                    effects.extend(required.iter().cloned());
                }
                WorldReplayStep::EvacuateInventory(intent) => {
                    effects.extend(intent.derive(state, values)?);
                }
                WorldReplayStep::DerivedLight(plan) => {
                    let mut writes = BTreeMap::new();
                    for effect in &effects {
                        let block_writes = match effect {
                            Effect::Put {
                                target,
                                value: Value::Block(after),
                            } => vec![(*target, *after)],
                            Effect::TickBlockInventory { intent } => intent
                                .derive_writes(state, values)?
                                .writes
                                .into_iter()
                                .filter_map(|(target, value)| match value {
                                    Some(Value::Block(after)) => Some((target, after)),
                                    _ => None,
                                })
                                .collect(),
                            _ => Vec::new(),
                        };
                        for (target, after) in block_writes {
                            let before =
                                values.get(&target).ok_or(ApplyFailure::Condition(target))?;
                            writes.insert(
                                target,
                                AppliedWrite {
                                    target,
                                    before: Some(before.clone()),
                                    after: Some(Cell {
                                        revision: before
                                            .revision
                                            .checked_add(1)
                                            .ok_or(ApplyFailure::InvalidAction)?,
                                        value: Value::Block(after),
                                    }),
                                },
                            );
                        }
                    }
                    let writes: Vec<_> = writes.into_values().collect();
                    let mut projected = values.clone();
                    for target in plan.section_targets()? {
                        let before = values.get(&target).ok_or(ApplyFailure::Condition(target))?;
                        projected.insert(
                            target,
                            crate::block_section::project_writes(target, before, &writes)?,
                        );
                    }
                    effects.extend(state.derive_world_light(*plan, &projected)?);
                }
                WorldReplayStep::ExhaustFood(intent) => {
                    let mut projected = values.clone();
                    for effect in &effects {
                        if let Effect::Put { target, value } = effect {
                            if intent.targets().contains(target) {
                                let before = projected
                                    .get(target)
                                    .ok_or(ApplyFailure::Condition(*target))?;
                                projected.insert(
                                    *target,
                                    Cell {
                                        revision: before.revision,
                                        value: value.clone(),
                                    },
                                );
                            }
                        }
                    }
                    effects.extend(intent.derive_effects(&projected)?);
                }
                WorldReplayStep::TransformBlock { target, cases } => {
                    let Some(Cell {
                        value: Value::Block(before),
                        ..
                    }) = values.get(target)
                    else {
                        return Err(ApplyFailure::Type(*target));
                    };
                    let Some(case) = cases.iter().find(|case| case.before == *before) else {
                        return Err(ApplyFailure::Condition(*target));
                    };
                    effects.push(Effect::Put {
                        target: *target,
                        value: Value::Block(case.after),
                    });
                }
                WorldReplayStep::ConditionalEffects {
                    conditions,
                    matches,
                    otherwise,
                } => {
                    if conditions.iter().any(|condition| {
                        !values.contains_key(&condition.target)
                            && !matches!(
                                condition.expected,
                                crate::canonical_action::Expected::Missing
                            )
                    }) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    let selected = if conditions
                        .iter()
                        .all(|condition| condition.expected.matches(values.get(&condition.target)))
                    {
                        matches
                    } else {
                        otherwise
                    };
                    effects.extend(selected.iter().cloned());
                }
            }
        }
        if effects.len() > 4096 {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(effects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_action::{Expected, LivePlayerLease, PlayerKey, SpatialEpoch};
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::living_property::LivingPropertyValue;
    use crate::protocol::{BlockPos, ChunkKey, WorldId};

    #[test]
    fn failed_required_mining_predicate_cannot_emit_food_only_effects() {
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(3)),
            gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(3)),
            incarnation: 7,
        };
        let tick = crate::time::TickStamp(20);
        let exhaustion = crate::combat_action::FoodExhaustionIntent {
            lease,
            chunk,
            epoch: SpatialEpoch::Admission {
                lease,
                admitted_at: tick,
            },
            tick,
            amount_milli: 5,
        };
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 64, z: 1 }),
            inventory: None,
        };
        let [food, mode] = exhaustion.targets();
        let intent = WorldReplayIntent {
            steps: vec![
                WorldReplayStep::RequiredEffects {
                    conditions: vec![Condition {
                        target: block,
                        expected: Expected::BlockState(1),
                    }],
                    effects: vec![Effect::Put {
                        target: block,
                        value: Value::Block(0),
                    }],
                },
                WorldReplayStep::ExhaustFood(exhaustion),
            ],
        };
        let mut values = BTreeMap::from([
            (
                block,
                Cell {
                    revision: 4,
                    value: Value::Block(1),
                },
            ),
            (
                food,
                Cell {
                    revision: 6,
                    value: Value::LivingProperty(LivingPropertyValue::Food(
                        crate::item_action::ActorFoodState {
                            actor: crate::actor::ActorId::Player(lease.gid),
                            food: 20,
                            saturation_milli: 5_000,
                            exhaustion_milli: 30,
                            tick_timer: 0,
                            revision: 6,
                        },
                    )),
                },
            ),
            (
                mode,
                Cell {
                    revision: 0,
                    value: Value::LivingProperty(LivingPropertyValue::Unsigned(0)),
                },
            ),
        ]);
        let before = values.clone();
        let effects = intent.derive_effects(&values).unwrap();
        assert_eq!(effects.len(), 2);
        assert!(
            matches!(&effects[1], Effect::Put { target, value: Value::LivingProperty(LivingPropertyValue::Food(food_state)) } if *target == food && food_state.exhaustion_milli == 35)
        );
        assert_eq!(values, before);
        values.insert(
            block,
            Cell {
                revision: 5,
                value: Value::Block(2),
            },
        );
        assert_eq!(
            intent.derive_effects(&values),
            Err(ApplyFailure::Condition(block))
        );
        assert_eq!(values[&food], before[&food]);
    }
}
