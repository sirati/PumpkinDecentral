use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{
    Action, ActionRef, ActionState, ApplyFailure, Cell, ChunkPart, Condition, Effect, Expected,
    InventoryId, Target, Value,
};
use crate::death_inventory::DeathInventoryDrop;
use crate::entity_body::EntityPayload;
use crate::inventory::InventoryTarget;
use crate::realm_completion::PeerGeneration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryEvacuationIntent {
    pub reference: ActionRef,
    pub producer: PeerGeneration,
    pub block: Target,
    pub state: u16,
    pub block_entity: Target,
    pub block_entity_before: Cell,
    pub drops: Vec<DeathInventoryDrop>,
}

impl InventoryEvacuationIntent {
    pub fn read_targets(&self) -> BTreeSet<Target> {
        [self.block, self.block_entity]
            .into_iter()
            .chain(
                self.drops
                    .iter()
                    .flat_map(|drop| [drop.source.target, drop.destination]),
            )
            .collect()
    }

    pub fn effect_targets(&self) -> BTreeSet<Target> {
        self.drops
            .iter()
            .flat_map(|drop| [drop.source.target, drop.destination])
            .collect()
    }

    pub fn valid(&self) -> bool {
        let Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(position),
            inventory: None,
        } = self.block
        else {
            return false;
        };
        if self.producer.generation == 0
            || self.reference.id.actor.origin() != self.producer.server
            || self.block_entity
                != (Target::Chunk {
                    key: chunk,
                    part: ChunkPart::BlockEntity(position),
                    inventory: None,
                })
            || !matches!(self.block_entity_before.value, Value::BlockEntity(_))
            || self.drops.len() > 128
        {
            return false;
        }
        let mut sources = BTreeSet::new();
        let mut destinations = BTreeSet::new();
        self.drops.iter().all(|drop| {
            let Target::Chunk { key, part: ChunkPart::Entity(entity), inventory: None } = drop.destination else { return false; };
            drop.source.is_valid() && sources.insert(drop.source.target) && destinations.insert(drop.destination)
                && drop.source.slot.target == (InventoryTarget::Block { chunk, position })
                && matches!(drop.source.target, Target::Chunk { key, part: ChunkPart::Block(pos), inventory: Some(InventoryId::Block { .. }) } if key == chunk && pos == position)
                && key.world == chunk.world && drop.template.belongs_to(entity, key)
                && drop.template.birth.action == self.reference && drop.template.birth.source == self.producer
                && matches!(&drop.template.payload, EntityPayload::Item(item) if item.age == 0 && item.pickup_delay == 40)
        })
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        self.valid()
            && self.reference
                == (ActionRef {
                    id: action.id,
                    tick: action.tick,
                })
            && action.conditions.contains(&Condition {
                target: self.block,
                expected: Expected::BlockState(self.state),
            })
            && action.conditions.contains(&Condition {
                target: self.block_entity,
                expected: Expected::Exact(self.block_entity_before.clone()),
            })
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &S,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        // The containing action validates the captured root guards before any
        // writes. Earlier steps may already replace the block and remove its BE.
        // Layout authority still derives only from those guarded original facts.
        let bindings = self
            .drops
            .iter()
            .map(|drop| drop.source.clone())
            .collect::<Vec<_>>();
        context.validate_block_inventory_layout(
            self.block,
            self.state,
            &self.block_entity_before,
            &bindings,
        )?;
        let mut effects = Vec::new();
        for drop in &self.drops {
            if values.contains_key(&drop.destination) {
                return Err(ApplyFailure::Condition(drop.destination));
            }
            let Some(Cell {
                value: Value::Inventory(source),
                ..
            }) = values.get(&drop.source.target)
            else {
                return Err(ApplyFailure::Inventory(drop.source.target));
            };
            if source.is_empty() {
                continue;
            }
            let Target::Chunk { key, .. } = drop.destination else {
                return Err(ApplyFailure::InvalidAction);
            };
            if context.scheduled_executor(key)? != drop.template.owner {
                return Err(ApplyFailure::InvalidAction);
            }
            let body = context.dropped_item_body(&drop.template, source)?;
            if !body.belongs_to(drop.template.id, key)
                || body.birth != drop.template.birth
                || body.owner != drop.template.owner
                || !matches!(&body.payload, EntityPayload::Item(item) if item.item == source.item && item.count == source.count && item.components == source.nbt)
            {
                return Err(ApplyFailure::InvalidAction);
            }
            effects.push(Effect::Put {
                target: drop.destination,
                value: Value::Entity((body).into()),
            });
            effects.push(Effect::Put {
                target: drop.source.target,
                value: Value::Inventory(crate::inventory::InventoryCell::empty(source.loc)),
            });
        }
        Ok(effects)
    }
}
