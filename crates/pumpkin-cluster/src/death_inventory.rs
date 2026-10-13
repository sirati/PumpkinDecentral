use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
use crate::actor::ActorId;
use crate::canonical_action::*;
use crate::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding, ItemTransformIntent, ItemTransformOperation};
use crate::entity_body::{EntityBody, EntityPayload};
use crate::item_contact::{ContactIdentity};
use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeathInventoryDrop {
    pub source: InventorySlotBinding,
    pub destination: Target,
    pub template: EntityBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeathInventoryIntent {
    pub identity: crate::combat_action::LivingTickIdentity,
    pub spatial: crate::entity_body::EntitySpatial,
    pub death_cause: ActionRef,
    pub keep_inventory: bool,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub drops: Vec<DeathInventoryDrop>,
    pub equipment: Vec<InventoryEquipmentBinding>,
    pub entity_equipment: Option<crate::canonical_inventory::EntityInventoryEquipmentBinding>,
    pub close: Option<crate::canonical_inventory_close::InventoryCloseIntent>,
}

impl DeathInventoryIntent {
    pub fn preserves_slots(&self) -> bool { self.keep_inventory && matches!(self.identity.actor(), ActorId::Player(_)) }

    pub fn property(&self, property: P) -> Target {
        Target::Chunk { key: self.identity.chunk, part: ChunkPart::LivingProperty {
            actor: self.identity.actor(), property }, inventory: None }
    }

    pub fn targets(&self) -> BTreeSet<Target> {
        [self.identity.target(), self.property(P::Health), self.property(P::DeathCause), self.property(P::DeathInventoryProcessed)]
            .into_iter().chain(self.drops.iter().flat_map(|drop| [drop.source.target, drop.destination]))
            .chain(self.equipment.iter().flat_map(InventoryEquipmentBinding::targets))
            .chain(self.entity_equipment.iter().flat_map(|binding| binding.targets().chain(binding.storage_property_targets())))
            .chain(self.close.iter().flat_map(|close| close.targets())).collect()
    }

    pub fn effect_targets(&self) -> BTreeSet<Target> {
        std::iter::once(self.property(P::DeathInventoryProcessed))
            .chain(self.drops.iter().filter(|_| !self.preserves_slots()).map(|drop| drop.source.target))
            .chain(self.drops.iter().filter(|_| !self.keep_inventory).map(|drop| drop.destination))
            .chain(self.equipment.iter().filter(|_| !self.preserves_slots() || self.close.is_some()).flat_map(InventoryEquipmentBinding::property_targets))
            .chain(self.entity_equipment.iter().filter(|_| !self.preserves_slots()).flat_map(|binding| binding.property_targets()))
            .chain(self.close.iter().flat_map(|close| close.targets())).collect()
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        if self.spatial.world != self.identity.chunk.world || self.spatial.position.iter().any(|value| !f64::from_bits(*value).is_finite() || f64::from_bits(*value).abs() > 30_000_000.0) || !self.identity.valid() || self.identity.actor() != id.actor || self.identity.simulation_tick != tick
            || self.rules.rules.world != self.identity.chunk.world || self.rules.rules.issuer.generation == 0 || self.rules.rules.epoch == 0
            || self.rules.slot as u16 != tick.0
            || self.death_cause.tick.is_after(tick) || self.drops.len() > 128 || self.equipment.len() > 1 {
            return false;
        }
        if self.close.as_ref().is_some_and(|close| !close.valid(ActionRef { tick, id })
            || !matches!(&self.identity.identity, ContactIdentity::Player(lease) if *lease == close.lease)
            || close.menu != self.property(P::MenuSession)) { return false; }
        if self.entity_equipment.as_ref().is_some_and(|binding| !binding.is_valid()
            || !matches!(&self.identity.identity, ContactIdentity::Entity { id, birth }
                if binding.entity == *id && binding.birth == *birth && binding.living == self.identity.target())
            || binding.slots.iter().map(|(_, target)| target).chain(binding.storage_slots.iter())
                .any(|target| !self.drops.iter().any(|drop| drop.source.target == *target))) { return false; }
        if matches!(self.identity.identity, ContactIdentity::Entity { .. }) && self.entity_equipment.is_none()
            && self.drops.iter().any(|drop| matches!(drop.source.target, Target::Chunk {
                inventory: Some(InventoryId::Entity { inv: crate::entity_inventory_admission::INV_EQUIPMENT, .. }), .. })) { return false; }
        let mut sources = BTreeSet::new();
        let mut destinations = BTreeSet::new();
        self.drops.iter().all(|drop| {
            let Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None } = drop.destination else { return false; };
            drop.source.is_valid() && sources.insert(drop.source.target) && destinations.insert(drop.destination)
                && match (&self.identity.identity, drop.source.target) {
                    (ContactIdentity::Player(lease), Target::Player { key, lease: Some(source), inventory: Some(_) }) => source == *lease && key == lease.key,
                    (ContactIdentity::Entity { id, .. }, Target::Chunk { key, part: ChunkPart::Entity(source), inventory: Some(_) }) => source == *id && key == self.identity.chunk,
                    _ => false,
                }
                && drop.template.belongs_to(entity, key)
                && key.world == self.identity.chunk.world
                && drop.template.birth.action == (ActionRef { tick, id })
                && drop.template.birth.source == self.identity.owner
                && matches!(&drop.template.payload, EntityPayload::Item(item) if item.age == 0 && item.pickup_delay == 40)
        }) && self.equipment.iter().all(|equipment| equipment.is_valid()
            && matches!(&self.identity.identity, ContactIdentity::Player(lease) if equipment.lease == *lease && equipment.living == self.identity.target()))
    }

    pub fn derive<S: ActionState>(&self, context: &S, id: ActionId, tick: TickStamp,
        values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid(id, tick) { return Err(ApplyFailure::InvalidAction); }
        context.death_inventory_rule(ActionRef { tick, id }, self.rules, self.keep_inventory, self.identity.target())?;
        for (property, expected) in [(P::Health, V::Unsigned(0)), (P::DeathCause, V::ActionReference(Some(self.death_cause))),
            (P::DeathInventoryProcessed, V::Boolean(false))] {
            let target = self.property(property);
            if values.get(&target).map(|cell| &cell.value) != Some(&Value::LivingProperty(expected)) {
                return Err(ApplyFailure::Condition(target));
            }
        }
        let mut effects = Vec::new();
        let mut after_close = values.clone();
        if let Some(close) = &self.close {
            let derived = close.derive(context, ActionRef { tick, id }, values)?;
            for (target, cell) in derived.cells {
                let revision = values.get(&target).map_or(0, |cell| cell.revision);
                after_close.insert(target, Cell { revision, value: Value::Inventory(cell) });
            }
            for (target, body) in derived.drops {
                after_close.insert(target, Cell { revision: 0, value: Value::Entity((body).into()) });
            }
            if let Some(menu) = derived.menu {
                after_close.insert(close.menu, Cell { revision: values[&close.menu].revision,
                    value: Value::LivingProperty(V::MenuSession(menu)) });
            }
            effects.push(Effect::CloseInventory { intent: close.clone() });
        }
        let values = &after_close;
        if !self.preserves_slots() {
            for drop in &self.drops {
                if values.contains_key(&drop.destination) { return Err(ApplyFailure::Condition(drop.destination)); }
                let Some(Value::Inventory(source)) = values.get(&drop.source.target).map(|cell| &cell.value) else {
                    return Err(ApplyFailure::Inventory(drop.source.target));
                };
                if source.is_empty() { continue; }
                if self.keep_inventory {
                    effects.push(Effect::TransformItems { intent: ItemTransformIntent {
                        source: drop.source.clone(), item: source.item, components: source.nbt.clone(),
                        operation: ItemTransformOperation::Consume { count: source.count }, remainder_destinations: Vec::new(),
                    } });
                    continue;
                }
                let Target::Chunk { key, .. } = drop.destination else { return Err(ApplyFailure::InvalidAction); };
                if context.scheduled_executor(key)? != drop.template.owner { return Err(ApplyFailure::InvalidAction); }
                if let Some(body) = context.death_inventory_drop(&drop.template, source)? {
                    effects.push(Effect::Put { target: drop.destination, value: Value::Entity((body).into()) });
                    effects.push(Effect::Put { target: drop.source.target,
                        value: Value::Inventory(crate::inventory::InventoryCell::empty(source.loc)) });
                } else {
                    effects.push(Effect::TransformItems { intent: ItemTransformIntent {
                        source: drop.source.clone(), item: source.item, components: source.nbt.clone(),
                        operation: ItemTransformOperation::Consume { count: source.count }, remainder_destinations: Vec::new(),
                    } });
                }
            }
        }
        let mut projected = values.clone();
        for effect in &effects {
            match effect {
                Effect::Put { target, value: Value::Inventory(item) } => {
                    projected.insert(*target, Cell { revision: values.get(target).map_or(0, |cell| cell.revision), value: Value::Inventory(item.clone()) });
                }
                Effect::TransformItems { intent } => for (target, item) in intent.derive(context, id, tick, &projected)? {
                    projected.insert(target, Cell { revision: values.get(&target).map_or(0, |cell| cell.revision), value: Value::Inventory(item) });
                },
                _ => {}
            }
        }
        crate::canonical_inventory::append_equipment_effects_from_projection(values, &projected, &mut effects, &self.equipment, context)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        if let Some(binding) = &self.entity_equipment {
            crate::canonical_inventory::append_entity_equipment_effects_from_projection(values, &projected, &mut effects,
                std::slice::from_ref(binding), context).map_err(|_| ApplyFailure::InvalidAction)?;
        }
        effects.push(Effect::Put { target: self.property(P::DeathInventoryProcessed), value: Value::LivingProperty(V::Boolean(true)) });
        Ok(effects)
    }
}

pub fn capture<S: ActionState>(context: &S, id: ActionId, tick: TickStamp, intent: DeathInventoryIntent)
    -> Result<Action, ApplyFailure> {
    if !intent.valid(id, tick) { return Err(ApplyFailure::InvalidAction); }
    let mut values = BTreeMap::new();
    let mut conditions = Vec::new();
    let destinations = intent.drops.iter().map(|drop| drop.destination)
        .chain(intent.close.iter().flat_map(|close| close.overflow.iter().map(|drop| drop.target)))
        .collect::<BTreeSet<_>>();
    for target in intent.targets() {
        let expected = if destinations.contains(&target) {
            if context.read(TruthView::Local, target).is_some() { return Err(ApplyFailure::Condition(target)); }
            Expected::Missing
        } else if target == intent.identity.target() {
            Expected::SpatialEpochEquals(intent.identity.epoch)
        } else {
            let cell = context.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
            values.insert(target, cell.clone());
            Expected::Exact(cell)
        };
        conditions.push(Condition { target, expected });
    }
    if let ContactIdentity::Entity { birth, .. } = &intent.identity.identity {
        conditions.push(Condition { target: intent.identity.target(), expected: Expected::ActorControlled { birth: birth.clone(), issuer: intent.identity.owner } });
        if let Some(binding) = &intent.entity_equipment {
            conditions.push(Condition { target: intent.identity.target(), expected: Expected::EntityBirthKind {
                birth: birth.clone(), owner: intent.identity.owner, kind: binding.kind } });
        }
    }
    let leases = match intent.identity.identity { ContactIdentity::Player(lease) => vec![lease], _ => Vec::new() };
    let footprint = intent.targets().into_iter().collect::<Vec<_>>();
    let stakeholders = footprint.iter().map(|target| target.stakeholder()).collect::<BTreeSet<_>>().into_iter().collect();
    let action = Action { edit_authority: None, id, tick, class: ActionClass::Replayable, kind: ActionKind::Item,
        chunk_transition: false, actor_chunk: Some(intent.identity.chunk),
        player_chunks: leases.iter().map(|lease| (lease.key, intent.identity.chunk)).collect(), player_leases: leases,
        stakeholders, dependencies: vec![intent.death_cause], conditions,
        effects: intent.derive(context, id, tick, &values)?, counterfactual_conditions: footprint, counterfactual_effects: intent.effect_targets().into_iter().collect(),
        semantic_intent: Some(SemanticIntent::DeathInventory(intent)), factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets], client_origin: ClientOrigin::None, presentation: Vec::new() };
    action.validate()?;
    Ok(action)
}
