use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ActionState, ApplyFailure, Cell, ChunkPart,
    ClientOrigin, Condition, Effect, Expected, InventoryId, LivePlayerLease, PlayerKey,
    SemanticIntent, Stakeholder, Target, TruthView, UndoProcedure, Value,
};
use crate::inventory::{
    InventoryGesture, InventoryScreenSession, InventorySlotRef, InventoryStateSnapshot,
    InventoryTarget, QuickCraftMode, reduce_inventory_gestures,
};
use crate::protocol::ChunkKey;
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketContents {
    Water,
    Lava,
    PowderSnow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemTransformOperation {
    Consume { count: u8 },
    Damage { amount: u16 },
    AttackDamage,
    MineDamage { hardness: u32 },
    ShieldDamage { incoming_milli: u32 },
    ThrowTrident,
    MendingRepair { experience: u32 },
    ExchangeBucket { contents: BucketContents, fill: bool },
    ArmorDamage { incoming_milli: u32, armor_slot: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemTransformIntent {
    pub source: InventorySlotBinding,
    pub item: u16,
    pub components: Vec<u8>,
    pub operation: ItemTransformOperation,
    pub remainder_destinations: Vec<InventorySlotBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemTransformOutput {
    pub source_after: crate::inventory::InventoryCell,
    pub remainder: Option<crate::inventory::InventoryCell>,
}

impl ItemTransformIntent {
    pub fn valid(&self) -> bool {
        self.source.is_valid()
            && self.item != 0
            && !self.components.is_empty()
            && self.components.len() <= 65_536
            && self.remainder_destinations.len() <= 128
            && match self.operation {
                ItemTransformOperation::Consume { count } => count > 0 && count <= 99,
                ItemTransformOperation::Damage { amount } => amount > 0,
                ItemTransformOperation::AttackDamage => true,
                ItemTransformOperation::MineDamage { hardness } => f32::from_bits(hardness).is_finite() && f32::from_bits(hardness) >= 0.0,
                ItemTransformOperation::ShieldDamage { incoming_milli } => incoming_milli > 0,
                ItemTransformOperation::ThrowTrident => true,
                ItemTransformOperation::MendingRepair { experience } => experience > 0 && experience <= i32::MAX as u32,
                ItemTransformOperation::ExchangeBucket { .. } => true,
                ItemTransformOperation::ArmorDamage {
                    incoming_milli,
                    armor_slot,
                } => incoming_milli > 0 && armor_slot < 4,
            }
            && self
                .remainder_destinations
                .iter()
                .all(InventorySlotBinding::is_valid)
            && self
                .remainder_destinations
                .iter()
                .map(|binding| binding.target)
                .collect::<BTreeSet<_>>()
                .len()
                == self.remainder_destinations.len()
            && !self
                .remainder_destinations
                .iter()
                .any(|binding| binding.target == self.source.target)
    }

    pub fn derive<S: ActionState>(
        &self,
        owners: &S,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<BTreeMap<Target, crate::inventory::InventoryCell>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let Some(Cell {
            value: Value::Inventory(source),
            ..
        }) = values.get(&self.source.target)
        else {
            return Err(ApplyFailure::Inventory(self.source.target));
        };
        if source.item != self.item || source.nbt != self.components || source.is_empty() {
            return Err(ApplyFailure::Inventory(self.source.target));
        }
        let output = owners.item_transform(source, &self.operation, id, tick)?;
        if output.source_after.loc != source.loc
            || output.source_after.count > self.source.max_stack
        {
            return Err(ApplyFailure::Inventory(self.source.target));
        }
        let mut writes = BTreeMap::from([(self.source.target, output.source_after)]);
        let Some(mut remainder) = output.remainder else {
            return Ok(writes);
        };
        if remainder.is_empty() {
            return Err(ApplyFailure::InvalidAction);
        }
        if writes[&self.source.target].is_empty() {
            let cap = self
                .source
                .max_stack
                .min(owners.inventory_limit(&remainder, self.source.target)?);
            let amount = remainder.count.min(cap);
            if amount != 0 {
                let mut after = remainder.clone();
                after.loc = source.loc;
                after.count = amount;
                writes.insert(self.source.target, after);
                remainder.count -= amount;
                if remainder.count == 0 {
                    return Ok(writes);
                }
            }
        }
        let candidates = std::iter::once(&self.source)
            .chain(self.remainder_destinations.iter())
            .collect::<Vec<_>>();
        for empty_phase in [false, true] {
            for binding in &candidates {
                let current = if let Some(cell) = writes.get(&binding.target) {
                    cell.clone()
                } else {
                    match values.get(&binding.target) {
                        Some(Cell {
                            value: Value::Inventory(cell),
                            ..
                        }) => cell.clone(),
                        _ => return Err(ApplyFailure::Inventory(binding.target)),
                    }
                };
                if current.is_empty() != empty_phase
                    || !current.is_empty()
                        && (current.item != remainder.item || current.nbt != remainder.nbt)
                {
                    continue;
                }
                let cap = binding
                    .max_stack
                    .min(owners.inventory_limit(&remainder, binding.target)?);
                let amount = remainder.count.min(cap.saturating_sub(current.count));
                if amount == 0 {
                    continue;
                }
                let mut after = remainder.clone();
                after.loc = current.loc;
                after.count = current.count + amount;
                writes.insert(binding.target, after);
                remainder.count -= amount;
                if remainder.count == 0 {
                    return Ok(writes);
                }
            }
        }
        Err(ApplyFailure::Inventory(self.source.target))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventorySlotBinding {
    pub slot: InventorySlotRef,
    pub target: Target,
    pub max_stack: u8,
}

pub fn capture_held_inventory_binding<S: ActionState>(
    view: &S,
    lease: LivePlayerLease,
) -> Result<(InventorySlotBinding, Cell, Condition), ApplyFailure> {
    let target = |inv, slot| Target::Player {
        key: lease.key,
        lease: Some(lease),
        inventory: Some(InventoryId::Player { inv, slot }),
    };
    let selector = target(crate::inventory::INV_SELECTED, 0);
    let selected = view
        .read(TruthView::Local, selector)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let Value::Inventory(selected_value) = &selected.value else {
        return Err(ApplyFailure::Type(selector));
    };
    if selected_value.count != 1 || !selected_value.nbt.is_empty() {
        return Err(ApplyFailure::Inventory(selector));
    }
    let slot = selected_value
        .item
        .checked_sub(1)
        .filter(|slot| *slot < 9)
        .ok_or(ApplyFailure::Inventory(selector))?;
    let source = target(crate::inventory::INV_MAIN, slot);
    let cell = view
        .read(TruthView::Local, source)
        .ok_or(ApplyFailure::Inventory(source))?;
    let Value::Inventory(item) = &cell.value else {
        return Err(ApplyFailure::Type(source));
    };
    let max_stack = view.inventory_limit(item, source)?;
    Ok((
        InventorySlotBinding {
            slot: InventorySlotRef::new(InventoryTarget::Player(lease.gid), item.loc),
            target: source,
            max_stack,
        },
        cell,
        Condition {
            target: selector,
            expected: Expected::Exact(selected),
        },
    ))
}

impl InventorySlotBinding {
    pub fn is_valid(&self) -> bool {
        let inventory = match self.target {
            Target::Player { inventory, .. } | Target::Chunk { inventory, .. } => inventory,
        };
        if !self.target.is_valid()
            || self.max_stack == 0
            || self.max_stack > 99
            || inventory.map(InventoryId::location) != Some((self.slot.loc.inv, self.slot.loc.slot))
        {
            return false;
        }
        match (self.slot.target, self.target) {
            (
                InventoryTarget::Player(player),
                Target::Player {
                    lease: Some(lease),
                    inventory: Some(InventoryId::Player { .. }),
                    ..
                },
            ) => player == lease.gid,
            (
                InventoryTarget::LeasedPlayer {
                    key,
                    player,
                    incarnation,
                },
                Target::Player {
                    lease: Some(lease),
                    inventory: Some(InventoryId::Player { .. }),
                    ..
                },
            ) => key == lease.key.0 && player == lease.gid && incarnation == lease.incarnation,
            (
                InventoryTarget::Block { chunk, position },
                Target::Chunk {
                    key,
                    part: ChunkPart::Block(pos),
                    inventory: Some(InventoryId::Block { .. }),
                },
            ) => chunk == key && position == pos,
            (
                InventoryTarget::Entity(entity),
                Target::Chunk {
                    part: ChunkPart::Entity(id),
                    inventory: Some(InventoryId::Entity { .. }),
                    ..
                },
            ) => entity == id.into(),
            (
                InventoryTarget::Virtual {
                    host,
                    owner: ActorId::Player(player),
                    session,
                    kind,
                },
                Target::Player {
                    lease: Some(lease),
                    inventory:
                        Some(InventoryId::Virtual {
                            host: target_host,
                            session: target_session,
                            kind: target_kind,
                            ..
                        }),
                    ..
                },
            ) => {
                player == lease.gid
                    && host == target_host
                    && session == target_session
                    && kind == target_kind
            }
            (
                InventoryTarget::LeasedVirtual {
                    host,
                    key,
                    player,
                    incarnation,
                    session,
                    kind,
                },
                Target::Player {
                    lease: Some(lease),
                    inventory:
                        Some(InventoryId::Virtual {
                            host: target_host,
                            session: target_session,
                            kind: target_kind,
                            ..
                        }),
                    ..
                },
            ) => {
                key == lease.key.0
                    && player == lease.gid
                    && incarnation == lease.incarnation
                    && host == target_host
                    && session == target_session
                    && kind == target_kind
            }
            (
                InventoryTarget::Virtual {
                    host,
                    owner: ActorId::Entity(entity),
                    session,
                    kind,
                },
                Target::Chunk {
                    part: ChunkPart::Entity(id),
                    inventory:
                        Some(InventoryId::Virtual {
                            host: target_host,
                            session: target_session,
                            kind: target_kind,
                            ..
                        }),
                    ..
                },
            ) => {
                entity == id
                    && host == target_host
                    && session == target_session
                    && kind == target_kind
            }
            (
                InventoryTarget::BlockVirtual {
                    host,
                    chunk,
                    position,
                    session,
                    kind,
                },
                Target::Chunk {
                    key,
                    part: ChunkPart::Block(pos),
                    inventory:
                        Some(InventoryId::Virtual {
                            host: target_host,
                            session: target_session,
                            kind: target_kind,
                            ..
                        }),
                },
            ) => {
                chunk == key
                    && position == pos
                    && host == target_host
                    && session == target_session
                    && kind == target_kind
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryGestureReplayIntent {
    pub gestures: Vec<InventoryGesture>,
    pub bindings: Vec<InventorySlotBinding>,
    pub equipment: Vec<InventoryEquipmentBinding>,
    #[serde(default)]
    pub entity_equipment: Vec<EntityInventoryEquipmentBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InventoryEquipmentSlot {
    MainHand,
    OffHand,
    Feet,
    Legs,
    Chest,
    Head,
    Body,
    Saddle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityInventoryEquipmentBinding {
    pub storage_slots: Vec<Target>,
    pub storage_properties: Vec<crate::mob_species_property::MobSpeciesProperty>,
    pub entity: crate::protocol::GlobalEntityId,
    pub kind: u16,
    pub birth: crate::entity_body::EntityBirth,
    pub living: Target,
    pub slots: Vec<(InventoryEquipmentSlot, Target)>,
    pub attribute_ids: Vec<u16>,
}

impl EntityInventoryEquipmentBinding {
    pub fn storage_property_targets(&self) -> BTreeSet<Target> {
        let Target::Chunk { key, .. } = self.living else { return BTreeSet::new(); };
        self.storage_properties.iter().map(|property| Target::Chunk {
            key, part: ChunkPart::LivingProperty { actor: ActorId::Entity(self.entity),
                property: crate::living_property::LivingProperty::MobSpeciesProperty(*property) },
            inventory: None,
        }).collect()
    }

    fn validate_storage<S: ActionState>(&self, values: &BTreeMap<Target, Cell>, context: &S) -> Result<(), InventoryGestureReplayFailure> {
        let facets = self.storage_property_targets().into_iter().map(|target| {
            let Target::Chunk { part: ChunkPart::LivingProperty { property: crate::living_property::LivingProperty::MobSpeciesProperty(property), .. }, .. } = target else { unreachable!(); };
            let Some(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MobSpeciesProperty(Some(value))), .. }) = values.get(&target) else {
                return Err(InventoryGestureReplayFailure::MissingCell(target));
            };
            Ok((property, value.clone()))
        }).collect::<Result<Vec<_>, _>>()?;
        let capacity = context.entity_inventory_storage_capacity(self.kind, &facets, self.storage_slots.len() as u16)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        for target in &self.storage_slots {
            let Some(Cell { value: Value::Inventory(item), .. }) = values.get(target) else {
                return Err(InventoryGestureReplayFailure::MissingCell(*target));
            };
            if item.loc.slot >= capacity && item.count != 0 {
                return Err(InventoryGestureReplayFailure::InvalidCell(*target));
            }
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        let Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None } = self.living else { return false; };
        entity == self.entity && self.birth.source.server == entity.origin
            && self.birth.source.generation == entity.generation && entity.generation != 0
            && self.storage_slots.len() <= 27
            && self.storage_slots.iter().enumerate().all(|(slot, target)| matches!(target,
                Target::Chunk { key: actual, part: ChunkPart::Entity(id), inventory: Some(InventoryId::Entity { inv, slot: actual_slot }) }
                    if *actual == key && *id == entity && *inv == crate::inventory::INV_MAIN && usize::from(*actual_slot) == slot))
            && self.storage_properties.len() <= 2
            && self.storage_properties.iter().collect::<BTreeSet<_>>().len() == self.storage_properties.len()
            && (6..=8).contains(&self.slots.len())
            && self.attribute_ids.len() <= 256
            && self.attribute_ids.iter().collect::<BTreeSet<_>>().len() == self.attribute_ids.len()
            && self.slots.iter().map(|(role, _)| role).collect::<BTreeSet<_>>().len() == self.slots.len()
            && [InventoryEquipmentSlot::MainHand, InventoryEquipmentSlot::OffHand,
                InventoryEquipmentSlot::Feet, InventoryEquipmentSlot::Legs,
                InventoryEquipmentSlot::Chest, InventoryEquipmentSlot::Head].iter()
                .all(|required| self.slots.iter().any(|(role, _)| role == required))
            && self.slots.iter().all(|(role, target)| {
                let slot = match role {
                    InventoryEquipmentSlot::MainHand => 0, InventoryEquipmentSlot::OffHand => 1,
                    InventoryEquipmentSlot::Feet => 2, InventoryEquipmentSlot::Legs => 3,
                    InventoryEquipmentSlot::Chest => 4, InventoryEquipmentSlot::Head => 5,
                    InventoryEquipmentSlot::Body => 6, InventoryEquipmentSlot::Saddle => 7,
                };
                matches!(target, Target::Chunk { key: actual, part: ChunkPart::Entity(id),
                    inventory: Some(InventoryId::Entity { inv, slot: actual_slot }) }
                    if *actual == key && *id == entity
                        && *inv == crate::entity_inventory_admission::INV_EQUIPMENT && *actual_slot == slot)
            })
    }

    pub fn property_targets(&self) -> BTreeSet<Target> {
        let Target::Chunk { key, .. } = self.living else { return BTreeSet::new(); };
        use crate::living_property::LivingProperty as P;
        [P::Weapon, P::ArmorProtection, P::Blocking, P::StatusEffects, P::ItemUse].into_iter()
            .chain(self.attribute_ids.iter().copied().flat_map(|id| [P::Attribute(id), P::PermanentAttributeModifiers(id)]))
            .map(|property| Target::Chunk { key, part: ChunkPart::LivingProperty {
                actor: ActorId::Entity(self.entity), property }, inventory: None }).collect()
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        self.slots.iter().map(|(_, target)| *target).chain(self.property_targets())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryEquipmentBinding {
    pub lease: LivePlayerLease,
    pub living: Target,
    pub selected: Target,
    pub hotbar: [Target; 9],
    pub armor: [Target; 4],
    pub offhand: Target,
    pub attribute_ids: Vec<u16>,
}

impl InventoryEquipmentBinding {
    pub fn player(lease: LivePlayerLease, chunk: ChunkKey, attribute_ids: Vec<u16>) -> Self {
        let target = |inv, slot| Target::Player {
            key: lease.key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv, slot }),
        };
        Self {
            lease,
            living: Target::Chunk {
                key: chunk,
                part: ChunkPart::Player(lease.gid),
                inventory: None,
            },
            selected: target(crate::inventory::INV_SELECTED, 0),
            hotbar: std::array::from_fn(|slot| target(crate::inventory::INV_MAIN, slot as u16)),
            armor: std::array::from_fn(|slot| target(crate::inventory::INV_ARMOR, slot as u16)),
            offhand: target(crate::inventory::INV_OFFHAND, 0),
            attribute_ids,
        }
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        self.property_targets()
            .into_iter()
            .chain(std::iter::once(self.selected))
            .chain(self.hotbar)
            .chain(self.armor)
            .chain(std::iter::once(self.offhand))
    }

    pub fn property_targets(&self) -> BTreeSet<Target> {
        let Target::Chunk { key, .. } = self.living else {
            return BTreeSet::new();
        };
        use crate::living_property::LivingProperty as P;
        [P::Weapon, P::ArmorProtection, P::Blocking, P::StatusEffects, P::ItemUse]
            .into_iter()
            .chain(self.attribute_ids.iter().copied().flat_map(|id| [P::Attribute(id), P::PermanentAttributeModifiers(id)]))
            .map(|property| Target::Chunk {
                key,
                part: ChunkPart::LivingProperty {
                    actor: ActorId::Player(self.lease.gid),
                    property,
                },
                inventory: None,
            })
            .collect()
    }

    fn inventory_target(&self, target: Target, inv: u8, slot: u16) -> bool {
        matches!(target, Target::Player { key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: actual_inv, slot: actual_slot }) }
            if key == self.lease.key && lease == self.lease && actual_inv == inv && actual_slot == slot)
    }

    pub fn is_valid(&self) -> bool {
        self.lease.incarnation != 0
            && self.attribute_ids.len() <= 256
            && self
                .attribute_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                == self.attribute_ids.len()
            && matches!(self.living, Target::Chunk { part: ChunkPart::Player(gid), inventory: None, .. } if gid == self.lease.gid)
            && self.inventory_target(self.selected, crate::inventory::INV_SELECTED, 0)
            && self.hotbar.iter().enumerate().all(|(slot, target)| {
                self.inventory_target(*target, crate::inventory::INV_MAIN, slot as u16)
            })
            && self.armor.iter().enumerate().all(|(slot, target)| {
                self.inventory_target(*target, crate::inventory::INV_ARMOR, slot as u16)
            })
            && self.inventory_target(self.offhand, crate::inventory::INV_OFFHAND, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryGestureProposal {
    pub gestures: Vec<InventoryGesture>,
    pub screen_session: Option<InventoryScreenSession>,
    pub actor_chunk: ChunkKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryGestureReplayFailure {
    InvalidIntent,
    MissingCell(Target),
    InvalidCell(Target),
    Reduction,
    ExternalEffect,
}

#[derive(Clone)]
pub struct InventoryActionCapture {
    pub edit_authority: Option<crate::inventory_edit_authority::InventoryEditAuthority>,
    pub id: ActionId,
    pub tick: TickStamp,
    pub actor_chunk: Option<ChunkKey>,
    pub player_leases: Vec<LivePlayerLease>,
    pub player_chunks: Vec<(PlayerKey, ChunkKey)>,
    pub guards: Vec<Condition>,
    pub client_origin: ClientOrigin,
}

impl InventoryGestureReplayIntent {
    pub fn capture_action<S: ActionState>(
        self,
        owners: &S,
        capture: InventoryActionCapture,
    ) -> Result<Action, ApplyFailure> {
        let mut targets = self.targets().collect::<Vec<_>>();
        targets.sort_unstable();
        let mut values = BTreeMap::new();
        let mut conditions = capture.guards;
        for target in &targets {
            let cell = owners
                .read(TruthView::Local, *target)
                .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))?;
            conditions.push(Condition {
                target: *target,
                expected: Expected::Exact(cell.clone()),
            });
            values.insert(*target, cell);
        }
        let effects = self
            .derive_effects_with_context(&values, owners)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(targets.iter().map(|target| target.stakeholder()))
            .chain(capture.actor_chunk.map(Stakeholder::Chunk))
            .chain(
                capture
                    .player_chunks
                    .iter()
                    .map(|(_, chunk)| Stakeholder::Chunk(*chunk)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        conditions.sort_by_key(|condition| condition.target);
        conditions.dedup();
        let action = Action {
            edit_authority: capture.edit_authority,
            id: capture.id,
            tick: capture.tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: capture.actor_chunk,
            player_leases: capture.player_leases,
            player_chunks: capture.player_chunks,
            stakeholders,
            dependencies: Vec::new(),
            conditions,
            effects,
            counterfactual_conditions: targets.clone(),
            counterfactual_effects: targets,
            semantic_intent: Some(SemanticIntent::InventoryGestures(self)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Inventory,
            presentation: Vec::new(),
            client_origin: capture.client_origin,
        };
        action.validate()?;
        Ok(action)
    }

    pub fn is_valid(&self) -> bool {
        if self.gestures.is_empty()
            || self.gestures.len() > 128
            || self.bindings.is_empty()
            || self.bindings.len() > 4096
            || self.bindings.iter().any(|binding| !binding.is_valid())
            || self.equipment.len() > 16
            || self.entity_equipment.len() > 16
            || self.entity_equipment.iter().any(|binding| !binding.is_valid()
                || binding.storage_slots.iter().any(|target| !self.bindings.iter().any(|slot| slot.target == *target))
                || binding.slots.iter().any(|(_, target)| !self.bindings.iter().any(|slot| slot.target == *target)))
            || self.equipment.iter().any(|equipment| {
                !equipment.is_valid()
                    || equipment
                        .targets()
                        .filter(|target| {
                            matches!(
                                target,
                                Target::Player {
                                    inventory: Some(_),
                                    ..
                                }
                            )
                        })
                        .any(|target| !self.bindings.iter().any(|binding| binding.target == target))
            })
        {
            return false;
        }
        let slots = self
            .bindings
            .iter()
            .map(|binding| binding.slot)
            .collect::<BTreeSet<_>>();
        let targets = self
            .bindings
            .iter()
            .map(|binding| binding.target)
            .collect::<BTreeSet<_>>();
        if slots.len() != self.bindings.len() || targets.len() != self.bindings.len() {
            return false;
        }
        self.gestures.iter().all(|gesture| {
            if gesture
                .slots()
                .iter()
                .any(|slot| slot.loc.inv == crate::inventory::INV_SELECTED)
                && !matches!(
                    gesture,
                    InventoryGesture::SelectHotbar { .. }
                        | InventoryGesture::SwapSelectedHand { .. }
                        | InventoryGesture::Expect { .. }
                )
            {
                return false;
            }
            match gesture {
                InventoryGesture::ExpectIdentity {
                    slot,
                    item,
                    components,
                } => {
                    slots.contains(slot)
                        && *item != 0
                        && components.len() <= crate::inventory::INVENTORY_MAX_NBT_BYTES
                }
                InventoryGesture::SwapSelectedHand { player } => {
                    matches!(
                        player,
                        InventoryTarget::Player(_) | InventoryTarget::LeasedPlayer { .. }
                    ) && gesture.slots().iter().all(|slot| slots.contains(slot))
                }
                InventoryGesture::SelectHotbar { slot, selected } => {
                    *selected < 9
                        && slots.contains(slot)
                        && slot.loc
                            == crate::inventory::InvLoc::new(crate::inventory::INV_SELECTED, 0)
                }
                InventoryGesture::Expect { slot, .. } => slots.contains(slot),
                InventoryGesture::Transfer {
                    source,
                    destination,
                    destination_limit,
                    ..
                }
                | InventoryGesture::TransferBy {
                    source,
                    destination,
                    destination_limit,
                    ..
                } => slots.contains(source) && self.permits(*destination, *destination_limit),
                InventoryGesture::ExchangeCursor {
                    slot,
                    cursor,
                    slot_limit,
                    cursor_limit,
                    ..
                } => self.permits(*slot, *slot_limit) && self.permits(*cursor, *cursor_limit),
                InventoryGesture::Swap {
                    first,
                    second,
                    first_limit,
                    second_limit,
                } => self.permits(*first, *first_limit) && self.permits(*second, *second_limit),
                InventoryGesture::QuickMove {
                    source,
                    destinations,
                }
                | InventoryGesture::DrainToPlayer {
                    source,
                    destinations,
                    ..
                } => {
                    slots.contains(source)
                        && !destinations.is_empty()
                        && destinations
                            .iter()
                            .all(|(slot, limit)| self.permits(*slot, *limit))
                }
                InventoryGesture::QuickCraft {
                    source,
                    destinations,
                    mode,
                } => {
                    *mode != QuickCraftMode::Fill
                        && slots.contains(source)
                        && !destinations.is_empty()
                        && destinations
                            .iter()
                            .all(|(slot, limit)| self.permits(*slot, *limit))
                }
                InventoryGesture::PickupAll {
                    destination,
                    sources,
                    destination_limit,
                } => {
                    self.permits(*destination, *destination_limit)
                        && !sources.is_empty()
                        && sources.iter().all(|source| slots.contains(source))
                }
                InventoryGesture::PlaceRecipe {
                    ingredients,
                    grid_slots,
                    player_sources,
                    ..
                } => {
                    ingredients
                        .iter()
                        .all(|ingredient| slots.contains(&ingredient.grid_slot))
                        && grid_slots
                            .iter()
                            .chain(player_sources)
                            .all(|slot| slots.contains(slot))
                }
                _ => false,
            }
        })
    }

    fn permits(&self, slot: InventorySlotRef, limit: u8) -> bool {
        limit != 0
            && self
                .bindings
                .iter()
                .any(|binding| binding.slot == slot && limit <= binding.max_stack)
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        self.bindings
            .iter()
            .map(|binding| binding.target)
            .chain(self.property_targets())
            .chain(self.entity_equipment.iter().flat_map(EntityInventoryEquipmentBinding::storage_property_targets))
    }

    pub fn property_targets(&self) -> BTreeSet<Target> {
        self.equipment
            .iter()
            .flat_map(InventoryEquipmentBinding::property_targets)
            .chain(self.entity_equipment.iter().flat_map(EntityInventoryEquipmentBinding::property_targets))
            .collect()
    }

    pub fn derive_effects(
        &self,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, InventoryGestureReplayFailure> {
        self.derive_effects_with_context(
            values,
            &crate::canonical_action::OwnedStakeholderSet::default(),
        )
    }

    pub fn derive_effects_with_context<S: ActionState>(
        &self,
        values: &BTreeMap<Target, Cell>,
        context: &S,
    ) -> Result<Vec<Effect>, InventoryGestureReplayFailure> {
        if !self.is_valid() {
            return Err(InventoryGestureReplayFailure::InvalidIntent);
        }
        let mut snapshots = BTreeMap::<InventoryTarget, InventoryStateSnapshot>::new();
        let mut bindings = BTreeMap::new();
        for binding in &self.bindings {
            let cell = values
                .get(&binding.target)
                .ok_or(InventoryGestureReplayFailure::MissingCell(binding.target))?;
            let Value::Inventory(value) = &cell.value else {
                return Err(InventoryGestureReplayFailure::InvalidCell(binding.target));
            };
            if value.loc != binding.slot.loc || value.count > binding.max_stack {
                return Err(InventoryGestureReplayFailure::InvalidCell(binding.target));
            }
            snapshots
                .entry(binding.slot.target)
                .or_insert_with(|| InventoryStateSnapshot {
                    target: binding.slot.target,
                    revision: 0,
                    cells: Vec::new(),
                })
                .cells
                .push(value.clone());
            bindings.insert(binding.slot, binding);
        }
        let snapshots = snapshots.into_values().collect::<Vec<_>>();
        let reduction = reduce_inventory_gestures(&snapshots, &self.gestures)
            .map_err(|_| InventoryGestureReplayFailure::Reduction)?;
        if !reduction.effects.is_empty() {
            return Err(InventoryGestureReplayFailure::ExternalEffect);
        }
        let mut effects = reduction
            .mutation
            .operations
            .into_iter()
            .map(|operation| {
                let slot = InventorySlotRef::new(operation.target, operation.after.loc);
                let binding = bindings
                    .get(&slot)
                    .ok_or(InventoryGestureReplayFailure::InvalidIntent)?;
                if operation.after.count > binding.max_stack {
                    return Err(InventoryGestureReplayFailure::InvalidCell(binding.target));
                }
                Ok(Effect::Put {
                    target: binding.target,
                    value: Value::Inventory(operation.after),
                })
            })
            .collect::<Result<Vec<_>, InventoryGestureReplayFailure>>()?;
        append_equipment_effects(values, &mut effects, &self.equipment, context)?;
        if !self.entity_equipment.is_empty() {
            let mut projected = values.clone();
            for effect in &effects {
                if let Effect::Put { target, value: Value::Inventory(item) } = effect {
                    let cell = projected.get_mut(target)
                        .ok_or(InventoryGestureReplayFailure::MissingCell(*target))?;
                    cell.value = Value::Inventory(item.clone());
                }
            }
            append_entity_equipment_effects_from_projection(values, &projected, &mut effects, &self.entity_equipment, context)?;
        }
        Ok(effects)
    }
}

pub fn append_equipment_effects<S: ActionState>(
    values: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    equipment: &[InventoryEquipmentBinding],
    context: &S,
) -> Result<(), InventoryGestureReplayFailure> {
    if equipment.is_empty() {
        return Ok(());
    }
    let mut projected = values.clone();
    for effect in effects.iter() {
        match effect {
            Effect::Put {
                target,
                value: Value::Inventory(value),
            } => {
                let cell = projected
                    .get_mut(target)
                    .ok_or(InventoryGestureReplayFailure::MissingCell(*target))?;
                cell.value = Value::Inventory(value.clone());
            }
            Effect::MoveItems {
                source,
                destination,
                count,
                ..
            } => {
                let before = projected
                    .get(source)
                    .ok_or(InventoryGestureReplayFailure::MissingCell(*source))?;
                let item = match &before.value {
                    Value::Inventory(item) => item.clone(),
                    Value::Entity(body) => match &body.payload {
                        crate::entity_body::EntityPayload::Item(item) => {
                            crate::inventory::InventoryCell {
                                loc: crate::inventory::InvLoc::new(0, 0),
                                item: item.item,
                                count: item.count,
                                nbt: item.components.clone(),
                            }
                        }
                        _ => return Err(InventoryGestureReplayFailure::InvalidCell(*source)),
                    },
                    _ => return Err(InventoryGestureReplayFailure::InvalidCell(*source)),
                };
                if item.count < *count {
                    return Err(InventoryGestureReplayFailure::InvalidCell(*source));
                }
                let cell = projected
                    .get_mut(destination)
                    .ok_or(InventoryGestureReplayFailure::MissingCell(*destination))?;
                let Value::Inventory(destination_value) = &mut cell.value else {
                    return Err(InventoryGestureReplayFailure::InvalidCell(*destination));
                };
                if !destination_value.is_empty()
                    && (destination_value.item != item.item || destination_value.nbt != item.nbt)
                {
                    return Err(InventoryGestureReplayFailure::InvalidCell(*destination));
                }
                destination_value.item = item.item;
                destination_value.nbt = item.nbt;
                destination_value.count = destination_value
                    .count
                    .checked_add(*count)
                    .ok_or(InventoryGestureReplayFailure::InvalidCell(*destination))?;
                let source_cell = projected.get_mut(source).unwrap();
                match &mut source_cell.value {
                    Value::Inventory(source_value) => {
                        source_value.count -= *count;
                        if source_value.count == 0 {
                            *source_value =
                                crate::inventory::InventoryCell::empty(source_value.loc);
                        }
                    }
                    Value::Entity(body) => {
                        if let crate::entity_body::EntityPayload::Item(item) = &mut body.payload {
                            item.count -= *count;
                        }
                    }
                    _ => return Err(InventoryGestureReplayFailure::InvalidCell(*source)),
                }
            }
            Effect::TransformItems { .. } => {
                return Err(InventoryGestureReplayFailure::InvalidIntent);
            }
            _ => {}
        }
    }
    append_equipment_effects_from_projection(values, &projected, effects, equipment, context)
}

pub fn append_equipment_effects_from_projection<S: ActionState>(
    values: &BTreeMap<Target, Cell>,
    projected: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    equipment: &[InventoryEquipmentBinding],
    context: &S,
) -> Result<(), InventoryGestureReplayFailure> {
    for equipment in equipment {
        if !equipment.is_valid() {
            return Err(InventoryGestureReplayFailure::InvalidIntent);
        }
        let inventory_cell = |target: Target| {
            projected
                .get(&target)
                .and_then(|cell| match &cell.value {
                    Value::Inventory(cell) => Some(cell.clone()),
                    _ => None,
                })
                .ok_or(InventoryGestureReplayFailure::MissingCell(target))
        };
        let selected = inventory_cell(equipment.selected)?
            .item
            .checked_sub(1)
            .filter(|selected| *selected < 9)
            .ok_or(InventoryGestureReplayFailure::InvalidCell(
                equipment.selected,
            ))? as usize;
        let mut cells = vec![
            (
                InventoryEquipmentSlot::MainHand,
                inventory_cell(equipment.hotbar[selected])?,
            ),
            (
                InventoryEquipmentSlot::OffHand,
                inventory_cell(equipment.offhand)?,
            ),
        ];
        for (slot, target) in [
            InventoryEquipmentSlot::Feet,
            InventoryEquipmentSlot::Legs,
            InventoryEquipmentSlot::Chest,
            InventoryEquipmentSlot::Head,
        ]
        .into_iter()
        .zip(equipment.armor)
        {
            cells.push((slot, inventory_cell(target)?));
        }
        let facts = context
            .inventory_equipment_facts(&cells)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        if facts
            .modifiers
            .iter()
            .any(|modifier| !equipment.attribute_ids.contains(&modifier.attribute))
        {
            return Err(InventoryGestureReplayFailure::InvalidIntent);
        }
        let old_cell = |target: Target| {
            values
                .get(&target)
                .and_then(|cell| match &cell.value {
                    Value::Inventory(cell) => Some(cell.clone()),
                    _ => None,
                })
                .ok_or(InventoryGestureReplayFailure::MissingCell(target))
        };
        let old_selected = old_cell(equipment.selected)?
            .item
            .checked_sub(1)
            .filter(|selected| *selected < 9)
            .ok_or(InventoryGestureReplayFailure::InvalidCell(
                equipment.selected,
            ))? as usize;
        let mut old_cells = vec![
            (
                InventoryEquipmentSlot::MainHand,
                old_cell(equipment.hotbar[old_selected])?,
            ),
            (
                InventoryEquipmentSlot::OffHand,
                old_cell(equipment.offhand)?,
            ),
        ];
        for (slot, target) in [
            InventoryEquipmentSlot::Feet,
            InventoryEquipmentSlot::Legs,
            InventoryEquipmentSlot::Chest,
            InventoryEquipmentSlot::Head,
        ]
        .into_iter()
        .zip(equipment.armor)
        {
            old_cells.push((slot, old_cell(target)?));
        }
        let old_facts = context
            .inventory_equipment_facts(&old_cells)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        append_equipment_facets(values, projected, effects, equipment.property_targets(), &facts, &old_facts, context)?;
        let item_use = equipment.property_targets().into_iter().find(|target| matches!(target,
            Target::Chunk { part: ChunkPart::LivingProperty { property: crate::living_property::LivingProperty::ItemUse, .. }, .. }))
            .ok_or(InventoryGestureReplayFailure::InvalidIntent)?;
        let current = values.get(&item_use).and_then(|cell| match &cell.value {
            Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(value)) => value.as_ref(), _ => None,
        });
        if let Some(current) = current {
            let source_target = match current.source.loc {
                crate::inventory::InvLoc { inv: crate::inventory::INV_MAIN, slot: 0..=8 } => Some(equipment.hotbar[current.source.loc.slot as usize]),
                crate::inventory::InvLoc { inv: crate::inventory::INV_OFFHAND, slot: 0 } => Some(equipment.offhand),
                _ => None,
            };
            let selected = current.actor == ActorId::Player(equipment.lease.gid)
                && match current.source.loc.inv {
                    crate::inventory::INV_MAIN => usize::from(current.source.loc.slot) == selected,
                    crate::inventory::INV_OFFHAND => true,
                    _ => false,
                };
            let source = source_target.and_then(|target| projected.get(&target)).and_then(|cell| match &cell.value {
                Value::Inventory(item) => Some(item), _ => None,
            });
            append_item_use_abort(values, effects, item_use, source, selected)?;
        }

    }
    Ok(())
}

pub fn append_entity_equipment_effects_from_projection<S: ActionState>(
    values: &BTreeMap<Target, Cell>,
    projected: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    equipment: &[EntityInventoryEquipmentBinding],
    context: &S,
) -> Result<(), InventoryGestureReplayFailure> {
    for binding in equipment {
        if !binding.is_valid() { return Err(InventoryGestureReplayFailure::InvalidIntent); }
        binding.validate_storage(values, context)?;
        binding.validate_storage(projected, context)?;
        let cells = |source: &BTreeMap<Target, Cell>| {
            binding.slots.iter().map(|(role, target)| {
                let Some(Cell { value: Value::Inventory(item), .. }) = source.get(target) else {
                    return Err(InventoryGestureReplayFailure::MissingCell(*target));
                };
                Ok((*role, item.clone()))
            }).collect::<Result<Vec<_>, _>>()
        };
        let projected_cells = cells(projected)?;
        context.entity_equipment_compatible(binding.kind, &projected_cells)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        let mut old = context.inventory_equipment_facts(&cells(values)?)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        let mut current = context.inventory_equipment_facts(&projected_cells)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)?;
        old.modifiers.retain(|modifier| binding.attribute_ids.contains(&modifier.attribute));
        current.modifiers.retain(|modifier| binding.attribute_ids.contains(&modifier.attribute));
        append_equipment_facets(values, projected, effects, binding.property_targets(), &current, &old, context)?;
        let item_use = binding.property_targets().into_iter().find(|target| matches!(target,
            Target::Chunk { part: ChunkPart::LivingProperty { property: crate::living_property::LivingProperty::ItemUse, .. }, .. }))
            .ok_or(InventoryGestureReplayFailure::InvalidIntent)?;
        let active = values.get(&item_use).and_then(|cell| match &cell.value {
            Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(value)) => value.as_ref(), _ => None,
        });
        if let Some(active) = active {
            let source = binding.slots.iter().find_map(|(_, target)| match target {
                Target::Chunk { inventory: Some(InventoryId::Entity { inv, slot }), .. }
                    if *inv == active.source.loc.inv && *slot == active.source.loc.slot => projected.get(target),
                _ => None,
            }).and_then(|cell| match &cell.value { Value::Inventory(item) => Some(item), _ => None });
            append_item_use_abort(values, effects, item_use, source, active.actor == ActorId::Entity(binding.entity))?;
        }

    }
    Ok(())
}

fn append_item_use_abort(
    values: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    target: Target,
    source: Option<&crate::inventory::InventoryCell>,
    selected: bool,
) -> Result<(), InventoryGestureReplayFailure> {
    if effects.iter().any(|effect| matches!(effect, Effect::Put { target: written, .. } if *written == target)) {
        return Ok(());
    }
    let Some(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(current)), .. }) = values.get(&target) else {
        return Err(InventoryGestureReplayFailure::MissingCell(target));
    };
    if let Some(current) = current {
        let trusted_wear = effects.iter().any(|effect| matches!(effect, Effect::TransformItems { intent }
            if intent.source.slot.loc == current.source.loc && intent.item == current.item.item && intent.components == current.item.nbt
                && match (intent.source.slot.target, current.source.target) {
                    (InventoryTarget::Player(a) | InventoryTarget::LeasedPlayer { player: a, .. },
                        InventoryTarget::Player(b) | InventoryTarget::LeasedPlayer { player: b, .. }) => a == b,
                    (a, b) => a == b,
                }
                && matches!(intent.operation, ItemTransformOperation::Damage { .. } | ItemTransformOperation::MineDamage { .. }
                    | ItemTransformOperation::ShieldDamage { .. } | ItemTransformOperation::MendingRepair { .. }
                    | ItemTransformOperation::ArmorDamage { .. })));
        let next = source.filter(|item| selected && !item.is_empty() && item.item == current.item.item && item.count == current.item.count
            && (item.nbt == current.item.nbt || trusted_wear)).map(|item| {
                let mut next = current.clone();
                next.item = item.clone();
                next
            });
        if next.as_ref() != Some(current) {
            effects.push(Effect::Put { target,
                value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(next)) });
        }
    }
    Ok(())
}

fn append_equipment_facets<S: ActionState>(
    values: &BTreeMap<Target, Cell>,
    projected: &BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
    property_targets: BTreeSet<Target>,
    facts: &crate::combat_action::CombatEquipmentFacts,
    old_facts: &crate::combat_action::CombatEquipmentFacts,
    context: &S,
) -> Result<(), InventoryGestureReplayFailure> {
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let status_target = property_targets.iter().copied()
        .find(|target| {
            matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::LivingProperty {
                        property: P::StatusEffects,
                        ..
                    },
                    ..
                }
            )
        })
        .ok_or(InventoryGestureReplayFailure::InvalidIntent)?;
    let status_modifiers = |cells: &BTreeMap<Target, Cell>| {
        let Some(Cell {
            value: Value::LivingProperty(V::StatusEffects(status)),
            ..
        }) = cells.get(&status_target)
        else {
            return Err(InventoryGestureReplayFailure::MissingCell(status_target));
        };
        context
            .status_effect_modifiers(status)
            .map_err(|_| InventoryGestureReplayFailure::InvalidIntent)
    };
    let old_status = status_modifiers(values)?;
    let new_status = status_modifiers(projected)?;
    for target in property_targets {
        let Some(Cell {
            value: Value::LivingProperty(before),
            ..
        }) = projected.get(&target)
        else {
            return Err(InventoryGestureReplayFailure::MissingCell(target));
        };
        let Target::Chunk {
            part: ChunkPart::LivingProperty { property, .. },
            ..
        } = target
        else {
            unreachable!()
        };
        let value = match property {
            P::Weapon => V::Weapon(crate::living_property::WeaponFacts {
                sharpness: facts.sharpness,
                smite: facts.smite,
                bane_of_arthropods: facts.bane_of_arthropods,
                knockback: facts.knockback,
                fire_aspect: facts.fire_aspect,
                breach: facts.breach,
                sweeping_edge: facts.sweeping_edge,
                sword: facts.sword,
                axe: facts.axe,
                mace: facts.mace,
                unarmed: facts.unarmed,
            }),
            P::ArmorProtection => {
                V::ArmorProtection(crate::living_property::ArmorProtectionFacts {
                    respiration: facts.respiration,
                    freeze_immune: facts.freeze_immune,
                    protection: facts.protection,
                    fire: facts.fire_protection,
                    explosion: facts.blast_protection,
                    projectile: facts.projectile_protection,
                    fall: facts.feather_falling,
                })
            }
            P::Blocking => V::Boolean(facts.shield),
            P::ItemUse => before.clone(),
            P::Attribute(id) => {
                let V::Attribute { base, transient_modifiers, .. } = before else {
                    return Err(InventoryGestureReplayFailure::InvalidCell(target));
                };
                let mut next = transient_modifiers
                    .iter()
                    .filter(|modifier| {
                        !old_facts
                            .modifiers
                            .iter()
                            .chain(&old_status)
                            .any(|old| old == *modifier)
                    })
                    .map(|modifier| (modifier.id.clone(), modifier.clone()))
                    .collect::<BTreeMap<_, _>>();
                for modifier in facts
                    .modifiers
                    .iter()
                    .chain(&new_status)
                    .filter(|modifier| modifier.attribute == id)
                {
                    next.insert(modifier.id.clone(), modifier.clone());
                }
                let transient_modifiers = next.into_values().collect::<Vec<_>>();
                let Target::Chunk { key, part: ChunkPart::LivingProperty { actor, .. }, .. } = target else { unreachable!() };
                let permanent_target = Target::Chunk { key, part: ChunkPart::LivingProperty { actor, property: P::PermanentAttributeModifiers(id) }, inventory: None };
                let Some(Cell { value: Value::LivingProperty(V::PermanentAttributeModifiers(permanent)), .. }) = projected.get(&permanent_target) else {
                    return Err(InventoryGestureReplayFailure::MissingCell(permanent_target));
                };
                let mut combined = transient_modifiers.iter().map(|modifier| (modifier.id.clone(), modifier.clone())).collect::<BTreeMap<_, _>>();
                for modifier in permanent { combined.insert(modifier.id.clone(), modifier.clone()); }
                V::Attribute { base: *base, modifiers: combined.into_values().collect(), transient_modifiers }

            }
            P::StatusEffects | P::PermanentAttributeModifiers(_) => continue,
            _ => return Err(InventoryGestureReplayFailure::InvalidIntent),
        };
        if &value != before {
            if let Some(effect) = effects.iter_mut().find(|effect| matches!(effect, Effect::Put { target: written, .. } if *written == target)) {
                    *effect = Effect::Put { target, value: Value::LivingProperty(value) };
                } else { effects.push(Effect::Put { target, value: Value::LivingProperty(value) }); }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_action::{LivePlayerLease, PlayerKey};
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::inventory::{INV_MAIN, InvLoc, InventoryCell, InventoryTransferCount};

    fn binding(slot: u16) -> InventorySlotBinding {
        let player = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(7)),
            gid: player,
            incarnation: 1,
        };
        InventorySlotBinding {
            slot: InventorySlotRef::new(
                InventoryTarget::Player(player),
                InvLoc::new(INV_MAIN, slot),
            ),
            target: Target::Player {
                key: lease.key,
                lease: Some(lease),
                inventory: Some(InventoryId::Player {
                    inv: INV_MAIN,
                    slot,
                }),
            },
            max_stack: 64,
        }
    }

    fn cell(binding: &InventorySlotBinding, item: u16, count: u8) -> Cell {
        Cell {
            revision: 0,
            value: Value::Inventory(if count == 0 {
                InventoryCell::empty(binding.slot.loc)
            } else {
                InventoryCell {
                    loc: binding.slot.loc,
                    item,
                    count,
                    nbt: vec![item as u8],
                }
            }),
        }
    }

    fn projected_count(effects: &[Effect], target: Target) -> u8 {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Put {
                    target: changed,
                    value: Value::Inventory(cell),
                } if *changed == target => Some(cell.count),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn selection_replays_against_changed_selection_without_creating_items() {
        let mut selected = binding(0);
        selected.slot.loc = InvLoc::new(crate::inventory::INV_SELECTED, 0);
        let Target::Player {
            inventory,
            lease: Some(lease),
            ..
        } = &mut selected.target
        else {
            unreachable!()
        };
        *inventory = Some(InventoryId::Player {
            inv: crate::inventory::INV_SELECTED,
            slot: 0,
        });
        let lease = *lease;
        let make_cell = |selection: u16| Cell {
            revision: 0,
            value: Value::Inventory(InventoryCell {
                loc: selected.slot.loc,
                item: selection + 1,
                count: 1,
                nbt: Vec::new(),
            }),
        };
        let mut owners = crate::canonical_action::OwnedStakeholderSet::default();
        owners.local.insert(selected.target, make_cell(0));
        let intent = InventoryGestureReplayIntent {
            entity_equipment: Vec::new(),
            gestures: vec![InventoryGesture::SelectHotbar {
                slot: selected.slot,
                selected: 6,
            }],
            bindings: vec![selected.clone()],
            equipment: Vec::new(),
        };
        let chunk = ChunkKey::new(crate::protocol::WorldId::new(0, 0), 0, 0);
        let capture = |sequence| InventoryActionCapture {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Player(lease.gid),
                incarnation: lease.incarnation,
                sequence,
                family: 2,
            },
            tick: TickStamp(20),
            actor_chunk: Some(chunk),
            player_leases: vec![lease],
            player_chunks: vec![(lease.key, chunk)],
            guards: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        let original = intent.clone().capture_action(&owners, capture(1)).unwrap();
        owners.local.insert(selected.target, make_cell(4));
        assert!(
            crate::canonical_action::prepare_action(&owners, &original, TruthView::Local).is_err()
        );
        let replay = intent.capture_action(&owners, capture(2)).unwrap();
        crate::canonical_action::apply_action(&mut owners, &replay, TruthView::Local).unwrap();
        assert!(
            matches!(&owners.local[&selected.target].value, Value::Inventory(value) if value.item == 7 && value.count == 1 && value.nbt.is_empty())
        );
        assert_eq!(owners.local.len(), 1);
    }

    #[test]
    fn selected_hand_swap_uses_prior_selection_in_the_same_action_scope() {
        let first = binding(0);
        let second = binding(1);
        let player = first.slot.target;
        let mut bindings = (0..9).map(binding).collect::<Vec<_>>();
        let mut selector = binding(0);
        selector.slot.loc = InvLoc::new(crate::inventory::INV_SELECTED, 0);
        let mut offhand = binding(0);
        offhand.slot.loc = InvLoc::new(crate::inventory::INV_OFFHAND, 0);
        for item in [&mut selector, &mut offhand] {
            let Target::Player { inventory, .. } = &mut item.target else {
                unreachable!()
            };
            *inventory = Some(InventoryId::Player {
                inv: item.slot.loc.inv,
                slot: 0,
            });
        }
        bindings.extend([selector.clone(), offhand.clone()]);
        let mut values = bindings
            .iter()
            .map(|binding| (binding.target, cell(binding, 0, 0)))
            .collect::<BTreeMap<_, _>>();
        values.insert(first.target, cell(&first, 4, 5));
        values.insert(second.target, cell(&second, 7, 3));
        values.insert(offhand.target, cell(&offhand, 9, 2));
        values.insert(
            selector.target,
            Cell {
                revision: 0,
                value: Value::Inventory(InventoryCell {
                    loc: selector.slot.loc,
                    item: 1,
                    count: 1,
                    nbt: Vec::new(),
                }),
            },
        );
        let intent = InventoryGestureReplayIntent {
            entity_equipment: Vec::new(),
            gestures: vec![
                InventoryGesture::SelectHotbar {
                    slot: selector.slot,
                    selected: 1,
                },
                InventoryGesture::SwapSelectedHand { player },
            ],
            bindings,
            equipment: Vec::new(),
        };
        let effects = intent.derive_effects(&values).unwrap();
        assert!(
            !effects
                .iter()
                .any(|effect| effect.targets().any(|target| target == first.target))
        );
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put { target, value: Value::Inventory(value) } if *target == second.target && value.item == 9 && value.count == 2)));
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put { target, value: Value::Inventory(value) } if *target == offhand.target && value.item == 7 && value.count == 3)));
    }

    #[test]
    fn half_stack_recomputes_after_collected_items_are_removed() {
        let source = binding(0);
        let cursor = binding(1);
        let intent = InventoryGestureReplayIntent {
            entity_equipment: Vec::new(),
            gestures: vec![InventoryGesture::TransferBy {
                source: source.slot,
                destination: cursor.slot,
                count: InventoryTransferCount::Half,
                destination_limit: 64,
            }],
            bindings: vec![source.clone(), cursor.clone()],
            equipment: Vec::new(),
        };
        let mut values = BTreeMap::from([
            (source.target, cell(&source, 9, 13)),
            (cursor.target, cell(&cursor, 0, 0)),
        ]);
        let initial = intent.derive_effects(&values).unwrap();
        assert_eq!(projected_count(&initial, source.target), 6);
        assert_eq!(projected_count(&initial, cursor.target), 7);
        values.insert(source.target, cell(&source, 9, 5));
        let replay = intent.derive_effects(&values).unwrap();
        assert_eq!(projected_count(&replay, source.target), 2);
        assert_eq!(projected_count(&replay, cursor.target), 3);
    }

    #[test]
    fn shift_move_relocates_into_later_candidate_when_first_changes() {
        let source = binding(0);
        let first = binding(1);
        let second = binding(2);
        let intent = InventoryGestureReplayIntent {
            entity_equipment: Vec::new(),
            gestures: vec![InventoryGesture::QuickMove {
                source: source.slot,
                destinations: vec![(first.slot, 64), (second.slot, 64)],
            }],
            bindings: vec![source.clone(), first.clone(), second.clone()],
            equipment: Vec::new(),
        };
        let mut values = BTreeMap::from([
            (source.target, cell(&source, 9, 10)),
            (first.target, cell(&first, 0, 0)),
            (second.target, cell(&second, 0, 0)),
        ]);
        let initial = intent.derive_effects(&values).unwrap();
        assert_eq!(projected_count(&initial, first.target), 10);
        values.insert(first.target, cell(&first, 8, 64));
        let replay = intent.derive_effects(&values).unwrap();
        assert_eq!(projected_count(&replay, source.target), 0);
        assert_eq!(projected_count(&replay, second.target), 10);
        assert!(
            !replay
                .iter()
                .any(|effect| effect.targets().any(|target| target == first.target))
        );
    }

    #[test]
    fn binding_cannot_redirect_a_players_slot_into_another_live_lease() {
        let mut forged = binding(0);
        if let Target::Player {
            lease: Some(ref mut lease),
            ..
        } = forged.target
        {
            lease.gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(2));
        }
        assert!(!forged.is_valid());
    }
}

pub fn append_status_effects<S: ActionState>(
    values: &BTreeMap<Target, Cell>, effects: &mut Vec<Effect>, body: Target,
    active: &crate::active_effects::ActiveEffectSet, attribute_ids: &[u16], context: &S,
) -> Result<(), ApplyFailure> {
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let Target::Chunk { key, part, inventory: None } = body else { return Err(ApplyFailure::Type(body)); };
    let actor = match part { ChunkPart::Player(player) => ActorId::Player(player), ChunkPart::Entity(entity) => ActorId::Entity(entity), _ => return Err(ApplyFailure::Type(body)) };
    if !active.valid() || attribute_ids.len() > 256 { return Err(ApplyFailure::InvalidAction); }
    let target = |property| Target::Chunk { key, part: ChunkPart::LivingProperty { actor, property }, inventory: None };
    let Some(Value::LivingProperty(V::StatusEffects(previous))) = values.get(&target(P::StatusEffects)).map(|cell| &cell.value) else {
        return Err(ApplyFailure::Type(target(P::StatusEffects)));
    };
    let old_modifiers = context.status_effect_modifiers(previous)?;
    let new_modifiers = context.status_effect_modifiers(active)?;
    let facts = context.status_effect_facts(active)?;
    let mut changes = vec![(P::StatusEffects, V::StatusEffects(active.clone())), (P::Strength, V::Amplifier(facts.strength)),
        (P::Weakness, V::Amplifier(facts.weakness)), (P::Resistance, V::Amplifier(facts.resistance)),
        (P::FireResistance, V::Boolean(facts.fire_resistance)), (P::Blindness, V::Boolean(facts.blindness))];
    for id in attribute_ids {
        let permanent_target = target(P::PermanentAttributeModifiers(*id));
        let target = target(P::Attribute(*id));
        let Some(Value::LivingProperty(V::Attribute { base, transient_modifiers, .. })) = values.get(&target).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Type(target));
        };
        let mut modifiers = transient_modifiers.clone();
        modifiers.retain(|modifier| !old_modifiers.iter().any(|old| old.attribute == modifier.attribute && old.id == modifier.id));
        for modifier in new_modifiers.iter().filter(|modifier| modifier.attribute == *id) {
            modifiers.retain(|existing| existing.id != modifier.id);
            modifiers.push(modifier.clone());
        }
        modifiers.sort_by(|a, b| (a.attribute, a.slot, a.id.as_str()).cmp(&(b.attribute, b.slot, b.id.as_str())));
        let Some(Value::LivingProperty(V::PermanentAttributeModifiers(permanent))) = values.get(&permanent_target).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Type(permanent_target));
        };
        let transient_modifiers = modifiers;
        let mut combined = transient_modifiers.iter().map(|modifier| (modifier.id.clone(), modifier.clone())).collect::<BTreeMap<_, _>>();
        for modifier in permanent { combined.insert(modifier.id.clone(), modifier.clone()); }
        changes.push((P::Attribute(*id), V::Attribute { base: *base, modifiers: combined.into_values().collect(), transient_modifiers }));

    }
    for (property, value) in changes {
        let target = target(property);
        let previous = values.get(&target).ok_or(ApplyFailure::Condition(target))?;
        if previous.value == Value::LivingProperty(value.clone()) { continue; }
        effects.retain(|effect| !matches!(effect, Effect::Put { target: written, .. } if *written == target));
        effects.push(Effect::Put { target, value: Value::LivingProperty(value) });
    }
    Ok(())
}
