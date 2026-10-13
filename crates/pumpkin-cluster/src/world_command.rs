use serde::{Deserialize, Serialize};

use crate::canonical_action::{ChunkPart, Target};
use crate::protocol::{BlockPos, ChunkKey, WorldId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockLocation {
    pub world: WorldId,
    pub position: BlockPos,
}

impl BlockLocation {
    pub fn valid(self) -> bool {
        [self.position.x, self.position.y, self.position.z]
            .into_iter()
            .all(|coordinate| coordinate.unsigned_abs() <= 30_000_000)
    }
    pub fn chunk(self) -> ChunkKey {
        ChunkKey::new(
            self.world,
            self.position.x.div_euclid(16),
            self.position.z.div_euclid(16),
        )
    }
    pub fn target(self) -> Target {
        Target::Chunk {
            key: self.chunk(),
            part: ChunkPart::Block(self.position),
            inventory: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StripLog {
    pub location: BlockLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrapeCopper {
    pub location: BlockLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveWax {
    pub location: BlockLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakBlock {
    pub location: BlockLocation,
    pub original_type: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceBlock {
    pub location: BlockLocation,
    pub item: u16,
    pub face: u8,
    pub hit: [u32; 3],
}

impl PlaceBlock {
    pub fn valid(self) -> bool {
        self.location.valid()
            && self.face <= 5
            && self.item != 0
            && self
                .hit
                .into_iter()
                .map(f32::from_bits)
                .all(|coordinate| coordinate.is_finite() && (0.0..=1.0).contains(&coordinate))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOperation {
    StripLog,
    ScrapeCopper,
    RemoveWax,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolAttributes {
    pub scale: u16,
    pub interaction_range: u16,
    pub used_statistic: u16,
}

impl StripLog {
    pub fn footprint<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ActionFootprint, crate::canonical_action::ApplyFailure> {
        tool_footprint(ToolOperation::StripLog, self.location, context, source)
    }
    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ResolutionRecord, crate::canonical_action::ApplyFailure>
    {
        derive_tool(ToolOperation::StripLog, self.location, context, source)
    }
}
impl ScrapeCopper {
    pub fn footprint<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ActionFootprint, crate::canonical_action::ApplyFailure> {
        tool_footprint(ToolOperation::ScrapeCopper, self.location, context, source)
    }
    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ResolutionRecord, crate::canonical_action::ApplyFailure>
    {
        derive_tool(ToolOperation::ScrapeCopper, self.location, context, source)
    }
}
impl RemoveWax {
    pub fn footprint<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ActionFootprint, crate::canonical_action::ApplyFailure> {
        tool_footprint(ToolOperation::RemoveWax, self.location, context, source)
    }
    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &crate::action_command::CommandContext<'_, S>,
        source: &crate::action_command::ItemCommandSource,
    ) -> Result<crate::action_command::ResolutionRecord, crate::canonical_action::ApplyFailure>
    {
        derive_tool(ToolOperation::RemoveWax, self.location, context, source)
    }
}

fn player_source(
    source: &crate::action_command::ItemCommandSource,
) -> Result<
    (
        crate::canonical_action::LivePlayerLease,
        ChunkKey,
        crate::canonical_action::SpatialEpoch,
    ),
    crate::canonical_action::ApplyFailure,
> {
    match source.actor {
        crate::action_command::ActorReference::Player {
            lease,
            chunk,
            epoch,
            ..
        } if source.cut.valid() => Ok((lease, chunk, epoch)),
        _ => Err(crate::canonical_action::ApplyFailure::InvalidAction),
    }
}

fn tool_footprint<S: crate::canonical_action::ActionState>(
    operation: ToolOperation,
    location: BlockLocation,
    context: &crate::action_command::CommandContext<'_, S>,
    source: &crate::action_command::ItemCommandSource,
) -> Result<crate::action_command::ActionFootprint, crate::canonical_action::ApplyFailure> {
    use crate::{
        actor::ActorId,
        canonical_action::{ApplyFailure, Value},
        living_property::LivingProperty as P,
    };
    let (lease, chunk, _) = player_source(source)?;
    if !location.valid() || chunk.world != location.world {
        return Err(ApplyFailure::InvalidAction);
    }
    let attrs = context.state.world_tool_attributes()?;
    let binding = crate::canonical_inventory::InventoryEquipmentBinding::player(
        lease,
        chunk,
        context.state.inventory_equipment_attribute_ids(),
    );
    let mut reads: std::collections::BTreeSet<_> = binding
        .targets()
        .into_iter()
        .chain(binding.property_targets())
        .collect();
    reads.insert(binding.living);
    reads.insert(location.target());
    for property in [
        P::Health,
        P::GameMode,
        P::PlayerStance,
        P::Attribute(attrs.scale),
        P::PermanentAttributeModifiers(attrs.scale),
        P::Attribute(attrs.interaction_range),
        P::PermanentAttributeModifiers(attrs.interaction_range),
    ] {
        reads.insert(Target::Chunk {
            key: chunk,
            part: ChunkPart::LivingProperty {
                actor: ActorId::Player(lease.gid),
                property,
            },
            inventory: None,
        });
    }
    let selector =
        context
            .values
            .get(&binding.selected)
            .ok_or(ApplyFailure::MissingStakeholder(
                binding.selected.stakeholder(),
            ))?;
    let Value::Inventory(selected) = &selector.value else {
        return Err(ApplyFailure::Type(binding.selected));
    };
    if selected.count != 1 || !selected.nbt.is_empty() {
        return Err(ApplyFailure::Inventory(binding.selected));
    }
    let slot = selected
        .item
        .checked_sub(1)
        .filter(|slot| *slot < 9)
        .ok_or(ApplyFailure::Inventory(binding.selected))?;
    let hand = match source.hand {
        crate::action_command::Hand::Main => binding.hotbar[usize::from(slot)],
        crate::action_command::Hand::Off => binding.offhand,
    };
    let held = context
        .values
        .get(&hand)
        .ok_or(ApplyFailure::MissingStakeholder(hand.stakeholder()))?;
    let Value::Inventory(item) = &held.value else {
        return Err(ApplyFailure::Inventory(hand));
    };
    let statistic = Target::Chunk {
        key: chunk,
        part: ChunkPart::LivingProperty {
            actor: ActorId::Player(lease.gid),
            property: P::Statistic {
                category: attrs.used_statistic,
                key: u32::from(item.item),
            },
        },
        inventory: None,
    };
    reads.insert(statistic);
    let mut writes: std::collections::BTreeSet<_> =
        binding.property_targets().into_iter().collect();
    let Value::Block(current) = context
        .values
        .get(&location.target())
        .ok_or(ApplyFailure::MissingStakeholder(
            location.target().stakeholder(),
        ))?
        .value
    else {
        return Err(ApplyFailure::Type(location.target()));
    };
    if let Some(companion) = context.state.world_tool_companion(location, current)? {
        reads.insert(companion.location.target());
        writes.insert(companion.location.target());
        for plan in context.state.world_tool_light_plans(
            companion.location,
            operation,
            companion.expected_state,
        )? {
            reads.extend(plan.section_targets()?);
            reads.extend(plan.light_targets()?);
            writes.extend(plan.light_targets()?);
        }
    }
    for plan in context
        .state
        .world_tool_light_plans(location, operation, current)?
    {
        reads.extend(plan.section_targets()?);
        reads.extend(plan.light_targets()?);
        writes.extend(plan.light_targets()?);
    }
    writes.extend([location.target(), hand, statistic]);
    let counterfactual_writes = writes
        .iter()
        .copied()
        .chain(binding.hotbar)
        .chain([binding.offhand])
        .collect();
    Ok(crate::action_command::ActionFootprint {
        counterfactual_reads: reads.clone(),
        reads,
        writes,
        counterfactual_writes,
    })
}

fn derive_tool<S: crate::canonical_action::ActionState>(
    operation: ToolOperation,
    location: BlockLocation,
    context: &crate::action_command::CommandContext<'_, S>,
    source: &crate::action_command::ItemCommandSource,
) -> Result<crate::action_command::ResolutionRecord, crate::canonical_action::ApplyFailure> {
    use crate::{
        action_command::{CommandResult, ResolutionRecord},
        actor::ActorId,
        canonical_action::{AppliedWrite, ApplyFailure, Cell, Effect, Value},
        living_property::{LivingProperty as P, LivingPropertyValue as V},
    };
    let footprint = tool_footprint(operation, location, context, source)?;
    let (lease, chunk, epoch) = player_source(source)?;
    let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    if id.actor != ActorId::Player(lease.gid)
        || id.incarnation != lease.incarnation
        || source.cut.sample_tick.is_after(context.tick)
        || !context.state.validate_live_player_lease(lease)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let values = footprint
        .reads
        .iter()
        .map(|target| {
            context
                .values
                .get(target)
                .cloned()
                .map(|cell| (*target, cell))
                .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
    let get = |target: Target| {
        values
            .get(&target)
            .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))
    };
    for target in &footprint.reads {
        get(*target)?;
    }
    let binding = crate::canonical_inventory::InventoryEquipmentBinding::player(
        lease,
        chunk,
        context.state.inventory_equipment_attribute_ids(),
    );
    let body = get(binding.living)?;
    let Value::Living(original) = &body.value else {
        return Err(ApplyFailure::Type(binding.living));
    };
    let mut living = original.clone();
    for target in &footprint.reads {
        if let Target::Chunk {
            part:
                ChunkPart::LivingProperty {
                    actor: ActorId::Player(gid),
                    property,
                },
            ..
        } = *target
        {
            if gid == lease.gid {
                let Value::LivingProperty(value) = &get(*target)?.value else {
                    return Err(ApplyFailure::Type(*target));
                };
                if !property.hydrate(&mut living, value) {
                    return Err(ApplyFailure::Type(*target));
                }
            }
        }
    }
    let avatar = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(binding.living))?;
    if living.combat.actor != ActorId::Player(lease.gid)
        || avatar.spatial_epoch != epoch
        || avatar.world != location.world
        || living.combat.health_milli == 0
        || avatar.game_mode > 1
        || avatar.presentation.sleeping.is_some()
    {
        return Err(ApplyFailure::Condition(binding.living));
    }
    let (owner, cut) = context
        .state
        .actor_occupancy_cut(binding.living, &body.value)?;
    if owner != source.actor.owner() || cut != source.cut {
        return Err(ApplyFailure::Condition(binding.living));
    }
    let attrs = context.state.world_tool_attributes()?;
    let base = living
        .combat
        .effective_attributes
        .get(&attrs.interaction_range)
        .copied()
        .map(f64::from_bits)
        .ok_or(ApplyFailure::InvalidAction)?;
    let reach = living.attribute(attrs.interaction_range, base);
    let eye = context.state.world_tool_eye_height(&living)?;
    let mut position = cut.position.map(f64::from_bits);
    position[1] += eye;
    let minimum = [
        location.position.x,
        location.position.y,
        location.position.z,
    ]
    .map(f64::from);
    let distance: f64 = position
        .into_iter()
        .zip(minimum)
        .map(|(value, low)| (value - value.clamp(low, low + 1.0)).powi(2))
        .sum();
    if !reach.is_finite()
        || !(0.0..=64.0).contains(&reach)
        || !eye.is_finite()
        || distance > reach * reach
    {
        return Err(ApplyFailure::Condition(location.target()));
    }
    let Value::Inventory(selector) = &get(binding.selected)?.value else {
        return Err(ApplyFailure::Inventory(binding.selected));
    };
    let slot = usize::from(selector.item - 1);
    let hand = match source.hand {
        crate::action_command::Hand::Main => binding.hotbar[slot],
        crate::action_command::Hand::Off => binding.offhand,
    };
    let Value::Inventory(item) = &get(hand)?.value else {
        return Err(ApplyFailure::Inventory(hand));
    };
    let Value::Block(before) = get(location.target())?.value else {
        return Err(ApplyFailure::Type(location.target()));
    };
    let after = context
        .state
        .world_tool_transform(operation, before, item.item)?;
    let Some(after) = after.filter(|_| !item.is_empty()) else {
        return Ok(ResolutionRecord {
            identity: context.identity,
            tick: context.tick,
            footprint,
            result: CommandResult::NotApplied,
        });
    };
    let mut effects = vec![Effect::Put {
        target: location.target(),
        value: Value::Block(after),
    }];
    if let Some(companion) = context.state.world_tool_companion(location, before)? {
        let target = companion.location.target();
        if get(target)?.value != Value::Block(companion.expected_state) {
            return Err(ApplyFailure::Condition(target));
        }
        let after = context
            .state
            .world_tool_transform(operation, companion.expected_state, item.item)?
            .ok_or(ApplyFailure::Condition(target))?;
        effects.push(Effect::Put {
            target,
            value: Value::Block(after),
        });
    }
    if avatar.game_mode != 1 {
        let output = context.state.item_transform(
            item,
            &crate::canonical_inventory::ItemTransformOperation::Damage { amount: 1 },
            id,
            context.tick,
        )?;
        if output.remainder.is_some() || output.source_after.loc != item.loc {
            return Err(ApplyFailure::Inventory(hand));
        }
        effects.push(Effect::Put {
            target: hand,
            value: Value::Inventory(output.source_after),
        });
        crate::canonical_inventory::append_equipment_effects(
            &values,
            &mut effects,
            &[binding.clone()],
            context.state,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    }
    let mut light_values = values.clone();
    for effect in &effects {
        if let Effect::Put { target, value } = effect {
            let old = get(*target)?;
            light_values.insert(
                *target,
                Cell {
                    revision: old.revision,
                    value: value.clone(),
                },
            );
            if matches!(
                *target,
                Target::Chunk {
                    part: ChunkPart::Block(_),
                    inventory: None,
                    ..
                }
            ) {
                if let Target::Chunk {
                    key,
                    part: ChunkPart::Block(position),
                    ..
                } = *target
                {
                    let section = Target::Chunk {
                        key,
                        part: ChunkPart::BlockSection(position.y.div_euclid(16)),
                        inventory: None,
                    };
                    if let Some(Cell {
                        value: Value::BlockSection(blocks),
                        ..
                    }) = light_values.get_mut(&section)
                    {
                        let Value::Block(block) = value else {
                            return Err(ApplyFailure::Type(*target));
                        };
                        let index =
                            ((position.y.rem_euclid(16) * 16 + position.z.rem_euclid(16)) * 16
                                + position.x.rem_euclid(16)) as usize;
                        *blocks.get_mut(index).ok_or(ApplyFailure::Type(section))? = *block;
                    }
                }
            }
        }
    }
    if let Some(companion) = context.state.world_tool_companion(location, before)? {
        for plan in context.state.world_tool_light_plans(
            companion.location,
            operation,
            companion.expected_state,
        )? {
            effects.extend(context.state.derive_world_light(plan, &light_values)?);
        }
    }
    for plan in context
        .state
        .world_tool_light_plans(location, operation, before)?
    {
        effects.extend(context.state.derive_world_light(plan, &light_values)?);
    }
    let statistic = Target::Chunk {
        key: chunk,
        part: ChunkPart::LivingProperty {
            actor: ActorId::Player(lease.gid),
            property: P::Statistic {
                category: attrs.used_statistic,
                key: u32::from(item.item),
            },
        },
        inventory: None,
    };
    let Value::LivingProperty(V::Unsigned(count)) = &get(statistic)?.value else {
        return Err(ApplyFailure::Type(statistic));
    };
    effects.push(Effect::Put {
        target: statistic,
        value: Value::LivingProperty(V::Unsigned(count.saturating_add(1).min(i32::MAX as u32))),
    });
    let mut projected = std::collections::BTreeMap::new();
    for effect in effects {
        let Effect::Put { target, value } = effect else {
            return Err(ApplyFailure::InvalidAction);
        };
        if !footprint.writes.contains(&target) {
            return Err(ApplyFailure::InvalidAction);
        }
        let old = get(target)?;
        projected.insert(
            target,
            Cell {
                revision: old
                    .revision
                    .checked_add(1)
                    .ok_or(ApplyFailure::InvalidAction)?,
                value,
            },
        );
    }
    let writes = projected
        .into_iter()
        .filter_map(|(target, after)| {
            let before = values.get(&target).cloned();
            (before
                .as_ref()
                .is_none_or(|before| before.value != after.value))
            .then_some(AppliedWrite {
                target,
                before,
                after: Some(after),
            })
        })
        .collect();
    Ok(ResolutionRecord {
        identity: context.identity,
        tick: context.tick,
        footprint,
        result: CommandResult::Applied(writes),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolCompanion {
    pub location: BlockLocation,
    pub expected_state: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillBucket {
    pub location: BlockLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyBucket {
    pub location: BlockLocation,
}
