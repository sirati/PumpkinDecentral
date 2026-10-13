use std::collections::{BTreeMap, BTreeSet};
use serde::{Deserialize, Serialize};
use crate::actor::ActorId;
use crate::canonical_action::*;
use crate::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
use crate::entity_body::{EntityPayload, ProjectileKind};
use crate::item_contact::{ContactIdentity, PickupContact};
use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectileCollectionIntent {
    pub contact: PickupContact,
    pub destinations: Vec<InventorySlotBinding>,
    pub equipment: InventoryEquipmentBinding,
}

impl ProjectileCollectionIntent {
    pub fn property(&self, property: P) -> Target {
        Target::Chunk { key: self.contact.picker.chunk, part: ChunkPart::LivingProperty { actor: self.contact.picker.actor(), property }, inventory: None }
    }

    pub fn targets(&self) -> BTreeSet<Target> {
        [self.contact.picker.target(), self.contact.item.target(), self.property(P::Health), self.property(P::GameMode)]
            .into_iter().chain(self.destinations.iter().map(|binding| binding.target)).chain(self.equipment.targets()).collect()
    }

    pub fn effect_targets(&self) -> BTreeSet<Target> {
        std::iter::once(self.contact.item.target()).chain(self.destinations.iter().map(|binding| binding.target))
            .chain(self.equipment.property_targets()).collect()
    }

    pub fn valid(&self) -> bool {
        let ContactIdentity::Player(lease) = self.contact.picker.identity else { return false; };
        self.contact.valid() && self.equipment.is_valid() && self.equipment.lease == lease
            && self.equipment.living == self.contact.picker.target()
            && matches!(self.contact.item.identity, ContactIdentity::Entity { .. })
            && !self.destinations.is_empty() && self.destinations.len() <= 128
            && self.destinations.iter().all(|binding| binding.is_valid()
                && matches!(binding.target, Target::Player { key, lease: Some(bound), inventory: Some(_) } if key == lease.key && bound == lease))
            && self.destinations.iter().map(|binding| binding.target).collect::<BTreeSet<_>>().len() == self.destinations.len()
    }

    pub fn derive<S: ActionState>(&self, context: &S, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid() { return Err(ApplyFailure::InvalidAction); }
        if !Expected::LivingUnsignedAtLeast(1).matches(values.get(&self.property(P::Health))) {
            return Err(ApplyFailure::Condition(self.property(P::Health)));
        }
        let Some(Value::LivingProperty(V::Unsigned(mode))) = values.get(&self.property(P::GameMode)).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Type(self.property(P::GameMode)));
        };
        if *mode == 3 { return Err(ApplyFailure::InvalidAction); }
        let target = self.contact.item.target();
        let Some(Value::Entity(body)) = values.get(&target).map(|cell| &cell.value) else { return Err(ApplyFailure::Type(target)); };
        let ContactIdentity::Entity { id, birth } = &self.contact.item.identity else { return Err(ApplyFailure::InvalidAction); };
        let EntityPayload::Projectile(projectile) = &body.payload else { return Err(ApplyFailure::Type(target)); };
        if body.id != *id || body.birth != *birth || body.owner != self.contact.item.owner || body.spatial_epoch != self.contact.item.epoch
            || projectile.embedded.is_none() || projectile.embedded_age < 7
            || !matches!(projectile.kind, ProjectileKind::Arrow | ProjectileKind::SpectralArrow | ProjectileKind::Trident)
            || !projectile.pickup_allowed && !(projectile.pickup_creative_only && *mode == 1) {
            return Err(ApplyFailure::Condition(target));
        }
        let mut effects = vec![Effect::Delete { target }];
        if projectile.pickup_creative_only { return Ok(effects); }
        let carried = &projectile.carried_item;
        let Some(Value::Inventory(selected)) = values.get(&self.equipment.selected).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Inventory(self.equipment.selected));
        };
        if !(1..=9).contains(&selected.item) { return Err(ApplyFailure::InvalidAction); }
        let mut destinations = self.destinations.iter().collect::<Vec<_>>();
        destinations.sort_by_key(|binding| (
            values.get(&binding.target).is_none_or(|cell| !matches!(&cell.value, Value::Inventory(item) if !item.is_empty())),
            if binding.target == self.equipment.hotbar[usize::from(selected.item - 1)] { 0 }
            else if binding.target == self.equipment.offhand { 1 } else { 2 + binding.slot.loc.slot },
        ));
        for binding in destinations {
            let Some(Value::Inventory(before)) = values.get(&binding.target).map(|cell| &cell.value) else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            if before.is_empty() && binding.target == self.equipment.offhand { continue; }
            if !before.is_empty() && (before.item != carried.item || before.nbt != carried.nbt) { continue; }
            let cap = binding.max_stack.min(context.inventory_limit(carried, binding.target)?);
            if before.count >= cap { continue; }
            let mut after = carried.clone();
            after.loc = before.loc;
            after.count = before.count + 1;
            effects.push(Effect::Put { target: binding.target, value: Value::Inventory(after) });
            crate::canonical_inventory::append_equipment_effects(values, &mut effects, std::slice::from_ref(&self.equipment), context)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            return Ok(effects);
        }
        Err(ApplyFailure::Inventory(target))
    }
}

pub fn capture<S: ActionState>(context: &S, id: ActionId, tick: TickStamp, intent: ProjectileCollectionIntent)
    -> Result<Action, ApplyFailure> {
    let ContactIdentity::Player(lease) = intent.contact.picker.identity else { return Err(ApplyFailure::InvalidAction); };
    if !intent.valid() || id.actor != ActorId::Player(lease.gid) || id.incarnation != lease.incarnation || tick != intent.contact.tick {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut values = BTreeMap::new();
    let mut conditions = Vec::new();
    for target in intent.targets() {
        let cell = context.read(TruthView::Local, target).ok_or(ApplyFailure::Condition(target))?;
        conditions.push(Condition { target, expected: Expected::Exact(cell.clone()) });
        values.insert(target, cell);
    }
    conditions.push(Condition { target: intent.contact.picker.target(), expected: Expected::SpatialEpochEquals(intent.contact.picker.epoch) });
    conditions.push(Condition { target: intent.contact.item.target(), expected: Expected::SpatialEpochEquals(intent.contact.item.epoch) });
    let ContactIdentity::Entity { birth, .. } = &intent.contact.item.identity else { return Err(ApplyFailure::InvalidAction); };
    conditions.push(Condition { target: intent.contact.item.target(), expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: intent.contact.item.owner } });
    intent.derive(context, &values)?;
    let footprint = intent.targets().into_iter().collect::<Vec<_>>();
    let action = Action { edit_authority: None, id, tick, class: ActionClass::Replayable, kind: ActionKind::Item,
        chunk_transition: false, actor_chunk: Some(intent.contact.picker.chunk), player_leases: vec![lease], player_chunks: vec![(lease.key, intent.contact.picker.chunk)],
        stakeholders: footprint.iter().map(|target| target.stakeholder()).collect::<BTreeSet<_>>().into_iter().collect(), dependencies: Vec::new(), conditions,
        effects: vec![Effect::CollectProjectile { intent: intent.clone() }], counterfactual_conditions: footprint, counterfactual_effects: intent.effect_targets().into_iter().collect(),
        semantic_intent: Some(SemanticIntent::CollectProjectile(intent)), factual_result: None, undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None, presentation: Vec::new() };
    action.validate()?;
    Ok(action)
}
