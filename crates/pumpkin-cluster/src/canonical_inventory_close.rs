use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::canonical_action::{ActionRef, ApplyFailure, Cell, LivePlayerLease, Target, Value};
use crate::canonical_inventory::InventorySlotBinding;
use crate::entity_body::{EntityBody, EntityPayload};
use crate::inventory::{InventoryCell, InventoryScreenSession};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryCloseProposal {
    pub session: Option<InventoryScreenSession>,
    pub token: u64,
    pub actor_chunk: crate::protocol::ChunkKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryCloseDrop {
    pub source: Target,
    pub target: Target,
    pub template: EntityBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryCloseIntent {
    pub lease: LivePlayerLease,
    pub session: InventoryScreenSession,
    pub token: u64,
    pub departure: bool,
    pub menu: Target,
    pub window_kind: u16,
    pub sources: Vec<InventorySlotBinding>,
    pub destinations: Vec<InventorySlotBinding>,
    pub results: Vec<InventorySlotBinding>,
    pub overflow: Vec<InventoryCloseDrop>,
}

pub struct InventoryCloseDerivation {
    pub cells: BTreeMap<Target, InventoryCell>,
    pub drops: BTreeMap<Target, EntityBody>,
    pub menu: Option<crate::canonical_menu::MenuSessionState>,
}

impl InventoryCloseIntent {
    pub fn birth_bodies(&self) -> impl Iterator<Item = &crate::entity_body::EntityBody> { self.overflow.iter().map(|drop| &drop.template) }
    pub fn capture_action<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        capture: crate::canonical_inventory::InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, ApplyFailure> {
        use crate::canonical_action::{
            Action, ActionClass, ActionKind, Condition, Effect, Expected, SemanticIntent,
            Stakeholder, TruthView, UndoProcedure,
        };
        let reference = ActionRef {
            tick: capture.tick,
            id: capture.id,
        };
        if !self.valid(reference) || !capture.player_leases.contains(&self.lease) {
            return Err(ApplyFailure::InvalidAction);
        }
        let targets = self.targets();
        let mut values = BTreeMap::new();
        let mut conditions = capture.guards;
        for target in &targets {
            let value = context.read(TruthView::Local, *target);
            let expected = if self.overflow.iter().any(|drop| drop.target == *target) {
                if value.is_some() {
                    return Err(ApplyFailure::Condition(*target));
                }
                Expected::Missing
            } else {
                let cell = value.ok_or(ApplyFailure::Inventory(*target))?;
                values.insert(*target, cell.clone());
                Expected::Exact(cell)
            };
            conditions.push(Condition {
                target: *target,
                expected,
            });
        }
        self.derive(context, reference, &values)?;
        let stakeholders = targets
            .iter()
            .map(|target| target.stakeholder())
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
        let action = Action {
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
            effects: vec![Effect::CloseInventory {
                intent: self.clone(),
            }],
            counterfactual_conditions: targets.clone(),
            counterfactual_effects: targets,
            semantic_intent: Some(SemanticIntent::InventoryClose(self.clone())),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Inventory,
            presentation: Vec::new(),
            client_origin: capture.client_origin,
            edit_authority: None,
        };
        action.validate()?;
        Ok(action)
    }
    pub fn bindings(&self) -> Vec<InventorySlotBinding> {
        self.sources
            .iter()
            .chain(&self.destinations)
            .chain(&self.results)
            .cloned()
            .collect()
    }

    pub fn targets(&self) -> Vec<Target> {
        self.bindings()
            .into_iter()
            .map(|binding| binding.target)
            .chain(self.overflow.iter().map(|drop| drop.target))
            .chain(std::iter::once(self.menu))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn valid(&self, reference: ActionRef) -> bool {
        if self.token == 0
            || !matches!(self.menu, Target::Chunk { part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: crate::actor::ActorId::Player(gid), property: crate::living_property::LivingProperty::MenuSession,
            }, inventory: None, .. } if gid == self.lease.gid)
            || self.session.actor != self.lease.gid
            || reference.id.actor != crate::actor::ActorId::Player(self.lease.gid)
            || reference.id.incarnation != self.lease.incarnation
            || self.sources.is_empty()
            || self.sources.len() > 10
            || self.destinations.len() != 36
            || self.results.len() > 1
            || self.overflow.len() != self.sources.len()
        {
            return false;
        }
        let bindings = self.bindings();
        let targets = bindings
            .iter()
            .map(|binding| binding.target)
            .collect::<BTreeSet<_>>();
        let overflow_sources = self
            .overflow
            .iter()
            .map(|drop| drop.source)
            .collect::<BTreeSet<_>>();
        let overflow_targets = self
            .overflow
            .iter()
            .map(|drop| drop.target)
            .collect::<BTreeSet<_>>();
        targets.len() == bindings.len()
            && bindings.iter().all(InventorySlotBinding::is_valid)
            && overflow_sources == self.sources.iter().map(|binding| binding.target).collect()
            && overflow_targets.len() == self.overflow.len()
            && overflow_targets.is_disjoint(&targets)
            && self.overflow.iter().all(|drop| {
                let Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::Entity(entity),
                    inventory: None,
                } = drop.target
                else {
                    return false;
                };
                drop.template.belongs_to(entity, key)
                    && drop.template.birth.action == reference
                    && drop.template.birth.source.server == self.lease.gid.server
                    && drop.template.owner.generation != 0
                    && matches!(&drop.template.payload, EntityPayload::Item(item)
                        if item.age == 0 && item.pickup_delay == 40)
            })
    }

    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        reference: ActionRef,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<InventoryCloseDerivation, ApplyFailure> {
        if !self.valid(reference) {
            return Err(ApplyFailure::InvalidAction);
        }
        let Some(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(menu)), .. }) = values.get(&self.menu)
            else { return Err(ApplyFailure::Condition(self.menu)); };
        if self.session.session != 0 && menu.session != self.session.session
            && menu.closed_sessions.iter().any(|closed| closed.session == self.session.session) {
            return Ok(InventoryCloseDerivation { cells: BTreeMap::new(), drops: BTreeMap::new(), menu: None });
        }
        if menu.session != self.session.session || menu.session != 0 && menu.window_kind != self.window_kind {
            return Err(ApplyFailure::Condition(self.menu));
        }
        let mut cells = BTreeMap::new();
        for binding in self.bindings() {
            let Some(Cell {
                value: Value::Inventory(cell),
                ..
            }) = values.get(&binding.target)
            else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            if cell.loc != binding.slot.loc || cell.count > binding.max_stack {
                return Err(ApplyFailure::Inventory(binding.target));
            }
            cells.insert(binding.target, cell.clone());
        }
        let mut drops = BTreeMap::new();
        for source in &self.sources {
            let mut item = cells[&source.target].clone();
            cells.insert(source.target, InventoryCell::empty(source.slot.loc));
            if item.is_empty() {
                continue;
            }
            for empty in [false, true] {
                for destination in &self.destinations {
                    let current = &cells[&destination.target];
                    if current.is_empty() != empty
                        || !current.is_empty()
                            && (current.item != item.item || current.nbt != item.nbt)
                    {
                        continue;
                    }
                    let limit = context
                        .inventory_limit(&item, destination.target)?
                        .min(destination.max_stack);
                    let count = item.count.min(limit.saturating_sub(current.count));
                    if count == 0 {
                        continue;
                    }
                    let mut after = item.clone();
                    after.loc = destination.slot.loc;
                    after.count = current.count + count;
                    cells.insert(destination.target, after);
                    item.count -= count;
                    if item.count == 0 {
                        break;
                    }
                }
                if item.count == 0 {
                    break;
                }
            }
            if item.count != 0 {
                let reserved = self
                    .overflow
                    .iter()
                    .find(|drop| drop.source == source.target)
                    .ok_or(ApplyFailure::InvalidAction)?;
                if values.contains_key(&reserved.target) {
                    return Err(ApplyFailure::Condition(reserved.target));
                }
                let body = context.dropped_item_body(&reserved.template, &item)?;
                drops.insert(reserved.target, body);
            }
        }
        for result in &self.results {
            cells.insert(result.target, InventoryCell::empty(result.slot.loc));
        }
        cells.retain(|target, after| {
            !matches!(values.get(target),
            Some(Cell { value: Value::Inventory(before), .. }) if before == after)
        });
        let mut next_menu = crate::canonical_menu::MenuSessionState::default();
        next_menu.last_close = Some(reference);
        next_menu.last_closed_session = menu.last_closed_session;
        next_menu.closed_sessions = menu.closed_sessions.clone();
        if self.session.session != 0 {
            next_menu.last_closed_session = self.session.session;
            next_menu.closed_sessions.retain(|closed| closed.session != self.session.session);
            if next_menu.closed_sessions.len() == 64 { next_menu.closed_sessions.remove(0); }
            next_menu.closed_sessions.push(crate::canonical_menu::MenuClosedSession {
                session: self.session.session, token: self.token, window_kind: self.window_kind, action: reference });
        }
        Ok(InventoryCloseDerivation { cells, drops, menu: Some(next_menu) })
    }
}
