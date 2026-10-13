use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ActionState, ApplyFailure, Cell, ClientOrigin,
    Condition, Effect, Expected, LivePlayerLease, SemanticIntent, Target, TruthView, UndoProcedure,
    Value,
};
use crate::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
use crate::entity_body::{EntityBody, EntityPayload};
use crate::inventory::InventoryTransferCount;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropItemIntent {
    pub selected: Option<Target>,
    pub candidates: Vec<InventorySlotBinding>,
    pub source: InventorySlotBinding,
    pub count: InventoryTransferCount,
    pub destination: Target,
    pub body: EntityBody,
    pub equipment: Vec<InventoryEquipmentBinding>,
}

pub fn capture<S: ActionState>(
    context: &S,
    id: ActionId,
    tick: TickStamp,
    actor_chunk: crate::protocol::ChunkKey,
    lease: Option<LivePlayerLease>,
    intent: DropItemIntent,
    client_origin: ClientOrigin,
) -> Result<Action, ApplyFailure> {
    if !intent.valid(id, tick) {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut values = BTreeMap::new();
    let mut conditions = Vec::new();
    for target in intent.targets() {
        if target == intent.destination {
            if context.read(TruthView::Local, target).is_some() {
                return Err(ApplyFailure::Condition(target));
            }
            conditions.push(Condition {
                target,
                expected: Expected::Missing,
            });
        } else {
            let cell = context
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            let expected = if target == intent.source.target && intent.selected.is_none() {
                let Value::Inventory(source) = &cell.value else {
                    return Err(ApplyFailure::Type(target));
                };
                Expected::InventoryItem {
                    item: source.item,
                    nbt: source.nbt.clone(),
                    minimum: match &intent.count {
                        InventoryTransferCount::Exact(count) => *count,
                        InventoryTransferCount::All => 1,
                        _ => return Err(ApplyFailure::InvalidAction),
                    },
                }
            } else {
                Expected::Exact(cell.clone())
            };
            conditions.push(Condition { target, expected });
            values.insert(target, cell);
        }
    }
    conditions.sort_by_key(|condition| condition.target);
    conditions.dedup_by_key(|condition| condition.target);
    let effects = intent.derive(context, id, tick, &values)?;
    let mut footprint = intent.targets().collect::<Vec<_>>();
    footprint.sort_unstable();
    footprint.dedup();
    let mut stakeholders = footprint
        .iter()
        .map(|target| target.stakeholder())
        .collect::<Vec<_>>();
    stakeholders.sort_unstable();
    stakeholders.dedup();
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Replayable,
        kind: ActionKind::Item,
        chunk_transition: false,
        actor_chunk: Some(actor_chunk),
        player_leases: lease.into_iter().collect(),
        player_chunks: lease
            .map(|lease| (lease.key, actor_chunk))
            .into_iter()
            .collect(),
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: footprint.clone(),
        counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::DropItem(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin,
        presentation: Vec::new(),
    };
    action.validate()?;
    Ok(action)
}

impl DropItemIntent {
    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        [self.source.target, self.destination].into_iter().chain(self.selected)
            .chain(self.candidates.iter().map(|binding| binding.target)).chain(
            self.equipment
                .iter()
                .flat_map(InventoryEquipmentBinding::targets),
        )
    }

    pub fn valid(&self, id: ActionId, tick: TickStamp) -> bool {
        let Target::Chunk {
            key,
            part: crate::canonical_action::ChunkPart::Entity(entity),
            inventory: None,
        } = self.destination
        else {
            return false;
        };
        self.source.is_valid()
            && match self.selected {
                None => self.candidates.is_empty(),
                Some(selector) => self.candidates.len() == 9
                    && matches!(selector, Target::Player { inventory: Some(crate::canonical_action::InventoryId::Player { inv: crate::inventory::INV_SELECTED, slot: 0 }), .. })
                    && self.candidates.iter().enumerate().all(|(index, binding)| binding.is_valid()
                        && binding.slot.target == self.source.slot.target
                        && binding.slot.loc.inv == crate::inventory::INV_MAIN && binding.slot.loc.slot == index as u16)
                    && match (selector, self.source.target) {
                        (Target::Player { key: selector_key, lease: selector_lease, .. }, Target::Player { key: source_key, lease: source_lease, .. }) => selector_key == source_key && selector_lease == source_lease,
                        _ => false,
                    }
                    && self.candidates.iter().map(|binding| binding.target).collect::<std::collections::BTreeSet<_>>().len() == 9,
            }
            && self.body.belongs_to(entity, key)
            && self.body.birth.action == crate::canonical_action::ActionRef { tick, id }
            && self.body.birth.source.server == entity.origin
            && self.body.birth.source.generation == entity.generation
            && self.body.owner.generation != 0
            && matches!(
                self.count,
                InventoryTransferCount::All | InventoryTransferCount::Exact(1..=99)
            )
            && matches!(&self.body.payload, EntityPayload::Item(item) if item.age == 0 && item.pickup_delay == 40)
            && self.equipment.len() <= 1
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &S,
        id: ActionId,
        tick: TickStamp,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid(id, tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        if values.contains_key(&self.destination) {
            return Err(ApplyFailure::Condition(self.destination));
        }
        let binding = if let Some(selected) = self.selected {
            let Some(Cell { value: Value::Inventory(selector), .. }) = values.get(&selected) else {
                return Err(ApplyFailure::Inventory(selected));
            };
            if !(1..=9).contains(&selector.item) { return Err(ApplyFailure::Inventory(selected)); }
            &self.candidates[usize::from(selector.item - 1)]
        } else { &self.source };
        let Some(Cell {
            value: Value::Inventory(source),
            ..
        }) = values.get(&binding.target)
        else {
            return Err(ApplyFailure::Inventory(self.source.target));
        };
        let EntityPayload::Item(template) = &self.body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        if source.is_empty() || self.selected.is_none() && (source.item != template.item || source.nbt != template.components) {
            return Err(ApplyFailure::Inventory(self.source.target));
        }
        let count = match &self.count {
            InventoryTransferCount::All => source.count,
            InventoryTransferCount::Exact(count) if *count <= source.count => *count,
            _ => return Err(ApplyFailure::Inventory(self.source.target)),
        };
        let mut after = source.clone();
        after.count -= count;
        if after.count == 0 {
            after.item = 0;
            after.nbt.clear();
        }
        let mut body = if self.selected.is_some() { context.dropped_item_body(&self.body, source)? } else { self.body.clone() };
        let EntityPayload::Item(item) = &mut body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        item.count = count;
        let mut effects = vec![
            Effect::Put {
                target: binding.target,
                value: Value::Inventory(after.clone()),
            },
            Effect::Put {
                target: self.destination,
                value: Value::Entity(Box::new(body)),
            },
        ];
        let mut projected = values.clone();
        projected.insert(
            binding.target,
            Cell {
                revision: values[&binding.target].revision,
                value: Value::Inventory(after),
            },
        );
        crate::canonical_inventory::append_equipment_effects_from_projection(
            values,
            &projected,
            &mut effects,
            &self.equipment,
            context,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(effects)
    }
}
