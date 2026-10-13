use crate::canonical_action::{ActionState, ApplyFailure, Cell, Target, Value};
use crate::canonical_inventory::{
    InventoryActionCapture, InventoryEquipmentBinding, InventorySlotBinding,
};
use crate::inventory::InventoryCell;
use crate::living_property::{LivingProperty, LivingPropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreativeInventoryOperation {
    Conserve(crate::inventory::InventoryGesture),
    Set {
        destination: Target,
        stack: InventoryCell,
    },
    Clone {
        source: Target,
        destination: Target,
    },
    Fill {
        source: Target,
        destinations: Vec<Target>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_action::{ChunkPart, InventoryId, LivePlayerLease, PlayerKey, TruthView};
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::inventory::{INV_MAIN, InvLoc, InventorySlotRef, InventoryTarget};
    use crate::protocol::ChunkKey;

    pub(super) struct Registry;
    impl ActionState for Registry {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> {
            None
        }
        fn commit(
            &mut self,
            _: crate::canonical_action::ActionContext,
            _: TruthView,
            _: &[crate::canonical_action::AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn inventory_limit(&self, source: &InventoryCell, _: Target) -> Result<u8, ApplyFailure> {
            match (source.item, source.nbt.as_slice()) {
                (7, [7]) => Ok(16),
                (8, [8]) => Ok(99),
                _ => Err(ApplyFailure::InvalidAction),
            }
        }
    }

    pub(super) fn fixture() -> (CreativeInventoryIntent, BTreeMap<Target, Cell>) {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(1));
        let lease = LivePlayerLease {
            gid,
            key: PlayerKey(uuid::Uuid::from_u128(1)),
            incarnation: 1,
        };
        let mode = Target::Chunk {
            key: ChunkKey::new(crate::protocol::WorldId::new(0, 0), 0, 0),
            part: ChunkPart::LivingProperty {
                actor: crate::actor::ActorId::Player(gid),
                property: LivingProperty::GameMode,
            },
            inventory: None,
        };
        let bindings = (0..2)
            .map(|slot| InventorySlotBinding {
                slot: InventorySlotRef::new(
                    InventoryTarget::LeasedPlayer {
                        key: lease.key.0,
                        player: gid,
                        incarnation: 1,
                    },
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
                max_stack: 99,
            })
            .collect::<Vec<_>>();
        let mut values = BTreeMap::from([(
            mode,
            Cell {
                revision: 0,
                value: Value::LivingProperty(LivingPropertyValue::Unsigned(1)),
            },
        )]);
        for binding in &bindings {
            values.insert(
                binding.target,
                Cell {
                    revision: 0,
                    value: Value::Inventory(InventoryCell::empty(binding.slot.loc)),
                },
            );
        }
        let intent = CreativeInventoryIntent {
            mode,
            operations: vec![CreativeInventoryOperation::Clone {
                source: bindings[0].target,
                destination: bindings[1].target,
            }],
            bindings,
            equipment: Vec::new(),
        };
        (intent, values)
    }

    #[test]
    fn clone_replays_identity_and_component_capacity_from_symbolic_source() {
        let (intent, mut values) = fixture();
        let source = intent.bindings[0].target;
        let destination = intent.bindings[1].target;
        values.get_mut(&source).unwrap().value = Value::Inventory(InventoryCell {
            loc: intent.bindings[0].slot.loc,
            item: 7,
            count: 3,
            nbt: vec![7],
        });
        let first = intent.derive(&Registry, &values).unwrap();
        assert_eq!(
            (first[&destination].item, first[&destination].count),
            (7, 16)
        );
        values.get_mut(&source).unwrap().value = Value::Inventory(InventoryCell {
            loc: intent.bindings[0].slot.loc,
            item: 8,
            count: 2,
            nbt: vec![8],
        });
        let replay = intent.derive(&Registry, &values).unwrap();
        assert_eq!(
            (replay[&destination].item, replay[&destination].count),
            (8, 99)
        );
        assert!(!replay.contains_key(&source));
        values.get_mut(&destination).unwrap().value = Value::Inventory(InventoryCell {
            loc: intent.bindings[1].slot.loc,
            item: 7,
            count: 1,
            nbt: vec![7],
        });
        assert_eq!(
            intent.derive(&Registry, &values),
            Err(ApplyFailure::Inventory(destination))
        );
        values.get_mut(&intent.mode).unwrap().value =
            Value::LivingProperty(LivingPropertyValue::Unsigned(0));
        assert_eq!(
            intent.derive(&Registry, &values),
            Err(ApplyFailure::Condition(intent.mode))
        );
    }

    #[test]
    fn set_rejects_forged_components_and_actual_slot_overflow() {
        let (mut intent, values) = fixture();
        let destination = intent.bindings[1].target;
        let mut stack = InventoryCell {
            loc: intent.bindings[1].slot.loc,
            item: 7,
            count: 17,
            nbt: vec![7],
        };
        intent.operations = vec![CreativeInventoryOperation::Set {
            destination,
            stack: stack.clone(),
        }];
        assert!(intent.derive(&Registry, &values).is_err());
        stack.count = 1;
        stack.nbt = vec![8];
        intent.operations = vec![CreativeInventoryOperation::Set { destination, stack }];
        assert!(intent.derive(&Registry, &values).is_err());
    }

    #[test]
    fn canonical_prepare_derives_creation_and_rejects_survival_before_commit() {
        struct State(BTreeMap<Target, Cell>);
        impl ActionState for State {
            fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
                self.0.get(&target).cloned()
            }
            fn inventory_limit(
                &self,
                source: &InventoryCell,
                target: Target,
            ) -> Result<u8, ApplyFailure> {
                Registry.inventory_limit(source, target)
            }
            fn commit(
                &mut self,
                _: crate::canonical_action::ActionContext,
                _: TruthView,
                _: &[crate::canonical_action::AppliedWrite],
            ) -> Result<(), ApplyFailure> {
                Err(ApplyFailure::InvalidAction)
            }
        }
        let (intent, mut values) = fixture();
        let source = intent.bindings[0].target;
        let destination = intent.bindings[1].target;
        values.get_mut(&source).unwrap().value = Value::Inventory(InventoryCell {
            loc: intent.bindings[0].slot.loc,
            item: 7,
            count: 3,
            nbt: vec![7],
        });
        let Target::Player {
            lease: Some(lease), ..
        } = source
        else {
            unreachable!()
        };
        let Target::Chunk { key: chunk, .. } = intent.mode else {
            unreachable!()
        };
        let mut state = State(values);
        let action = intent
            .clone()
            .capture(
                &state,
                InventoryActionCapture {
                    edit_authority: None,
                    id: crate::canonical_action::ActionId {
                        actor: crate::actor::ActorId::Player(lease.gid),
                        incarnation: 1,
                        sequence: 0,
                        family: 2,
                    },
                    tick: crate::time::TickStamp(1),
                    actor_chunk: Some(chunk),
                    player_leases: vec![lease],
                    player_chunks: vec![(lease.key, chunk)],
                    guards: Vec::new(),
                    client_origin: crate::canonical_action::ClientOrigin::None,
                },
            )
            .unwrap();
        let prepared =
            crate::canonical_action::prepare_action(&state, &action, TruthView::Local).unwrap();
        assert!(prepared.writes.iter().any(|write| write.target == destination
            && matches!(&write.after, Some(Cell { value: Value::Inventory(cell), .. }) if cell.item == 7 && cell.count == 16)));
        assert_eq!(
            state.0[&destination].value,
            Value::Inventory(InventoryCell::empty(intent.bindings[1].slot.loc))
        );
        state.0.get_mut(&intent.mode).unwrap().value =
            Value::LivingProperty(LivingPropertyValue::Unsigned(0));
        assert!(
            crate::canonical_action::prepare_action(&state, &action, TruthView::Local).is_err()
        );
    }
}

#[cfg(test)]
mod mixed_tests {
    use super::*;
    #[test]
    fn creative_creation_and_ordinary_split_share_one_symbolic_transaction() {
        let (mut intent, values) = super::tests::fixture();
        let source = intent.bindings[0].target;
        let destination = intent.bindings[1].target;
        intent.operations = vec![
            CreativeInventoryOperation::Set {
                destination: source,
                stack: InventoryCell {
                    loc: intent.bindings[0].slot.loc,
                    item: 8,
                    count: 99,
                    nbt: vec![8],
                },
            },
            CreativeInventoryOperation::Conserve(crate::inventory::InventoryGesture::TransferBy {
                source: intent.bindings[0].slot,
                destination: intent.bindings[1].slot,
                count: crate::inventory::InventoryTransferCount::Half,
                destination_limit: 99,
            }),
        ];
        let after = intent.derive(&super::tests::Registry, &values).unwrap();
        assert_eq!(after[&source].count + after[&destination].count, 99);
        assert_eq!(after[&destination].count, 50);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreativeInventoryIntent {
    pub mode: Target,
    pub bindings: Vec<InventorySlotBinding>,
    pub operations: Vec<CreativeInventoryOperation>,
    pub equipment: Vec<InventoryEquipmentBinding>,
}

impl CreativeInventoryIntent {
    pub fn from_gestures(
        intent: &crate::canonical_inventory::InventoryGestureReplayIntent,
        mode: Target,
    ) -> Option<Self> {
        use crate::inventory::{InventoryGesture, QuickCraftMode};
        if !intent.gestures.iter().any(|gesture| {
            matches!(
                gesture,
                InventoryGesture::CreativeSet { .. }
                    | InventoryGesture::CreativeClone { .. }
                    | InventoryGesture::QuickCraft {
                        mode: QuickCraftMode::Fill,
                        ..
                    }
            )
        }) {
            return None;
        }
        let target = |slot| {
            intent
                .bindings
                .iter()
                .find(|binding| binding.slot == slot)
                .map(|binding| binding.target)
        };
        let operations = intent
            .gestures
            .iter()
            .map(|gesture| match gesture {
                InventoryGesture::CreativeClone {
                    source,
                    destination,
                    ..
                } => Some(CreativeInventoryOperation::Clone {
                    source: target(*source)?,
                    destination: target(*destination)?,
                }),
                InventoryGesture::CreativeSet {
                    destination, stack, ..
                } => Some(CreativeInventoryOperation::Set {
                    destination: target(*destination)?,
                    stack: stack.clone(),
                }),
                InventoryGesture::QuickCraft {
                    source,
                    destinations,
                    mode: QuickCraftMode::Fill,
                } => Some(CreativeInventoryOperation::Fill {
                    source: target(*source)?,
                    destinations: destinations
                        .iter()
                        .map(|(slot, _)| target(*slot))
                        .collect::<Option<Vec<_>>>()?,
                }),
                _ => Some(CreativeInventoryOperation::Conserve(gesture.clone())),
            })
            .collect::<Option<Vec<_>>>()?;
        let result = Self {
            mode,
            bindings: intent.bindings.clone(),
            operations,
            equipment: intent.equipment.clone(),
        };
        result.valid().then_some(result)
    }
    pub fn valid(&self) -> bool {
        matches!(
            self.mode,
            Target::Chunk {
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor: crate::actor::ActorId::Player(_),
                    property: LivingProperty::GameMode
                },
                inventory: None,
                ..
            }
        ) && !self.bindings.is_empty()
            && self.bindings.len() <= 128
            && !self.operations.is_empty()
            && self.operations.len() <= 128
            && self.equipment.len() <= 1
            && self.equipment.iter().all(|equipment| {
                equipment.is_valid()
                    && equipment
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
                        .all(|target| self.bindings.iter().any(|binding| binding.target == target))
            })
            && self.bindings.iter().all(InventorySlotBinding::is_valid)
            && self
                .bindings
                .iter()
                .map(|binding| binding.target)
                .collect::<BTreeSet<_>>()
                .len()
                == self.bindings.len()
            && self.operations.iter().all(|operation| match operation {
                CreativeInventoryOperation::Conserve(gesture) => {
                    crate::canonical_inventory::InventoryGestureReplayIntent {
                        entity_equipment: Vec::new(),
                        gestures: vec![gesture.clone()],
                        bindings: self.bindings.clone(),
                        equipment: Vec::new(),
                    }
                    .is_valid()
                }
                CreativeInventoryOperation::Set { destination, stack } => {
                    self.binding(*destination).is_some_and(|binding| {
                        stack.loc == binding.slot.loc
                            && stack.count <= binding.max_stack
                            && (stack.is_empty() && stack.item == 0 && stack.nbt.is_empty()
                                || !stack.is_empty()
                                    && stack.item != 0
                                    && !stack.nbt.is_empty()
                                    && stack.nbt.len() <= 65_536)
                    })
                }
                CreativeInventoryOperation::Clone {
                    source,
                    destination,
                } => {
                    self.binding(*source).is_some()
                        && self.binding(*destination).is_some()
                        && source != destination
                }
                CreativeInventoryOperation::Fill {
                    source,
                    destinations,
                } => {
                    self.binding(*source).is_some()
                        && !destinations.is_empty()
                        && destinations.len() <= 128
                        && destinations
                            .iter()
                            .all(|target| self.binding(*target).is_some())
                        && destinations.iter().copied().collect::<BTreeSet<_>>().len()
                            == destinations.len()
                }
            })
    }

    fn binding(&self, target: Target) -> Option<&InventorySlotBinding> {
        self.bindings.iter().find(|binding| {
            binding.target == target && binding.slot.loc.inv != crate::inventory::INV_SELECTED
        })
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        std::iter::once(self.mode)
            .chain(self.bindings.iter().map(|binding| binding.target))
            .chain(
                self.equipment
                    .iter()
                    .flat_map(InventoryEquipmentBinding::targets),
            )
    }

    pub fn effects<S: ActionState>(
        &self,
        context: &S,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, ApplyFailure> {
        let mut projected = values.clone();
        for (target, after) in self.derive(context, values)? {
            projected
                .get_mut(&target)
                .ok_or(ApplyFailure::Inventory(target))?
                .value = Value::Inventory(after);
        }
        let mut effects = vec![crate::canonical_action::Effect::CreativeItems {
            intent: self.clone(),
        }];
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

    pub fn capture<S: ActionState>(
        self,
        context: &S,
        capture: InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, ApplyFailure> {
        use crate::canonical_action::{
            Action, ActionClass, ActionKind, Condition, Expected, SemanticIntent, Stakeholder,
            TruthView, UndoProcedure,
        };
        let targets = self
            .targets()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let values = targets
            .iter()
            .map(|&target| {
                context
                    .read(TruthView::Local, target)
                    .map(|cell| (target, cell))
                    .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let effects = self.effects(context, &values)?;
        let mut conditions = capture.guards;
        conditions.extend(values.into_iter().map(|(target, cell)| Condition {
            target,
            expected: Expected::Exact(cell),
        }));
        conditions.sort_by_key(|condition| condition.target);
        conditions.dedup();
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
            edit_authority: None,
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
            semantic_intent: Some(SemanticIntent::CreativeInventory(self)),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Inventory,
            presentation: Vec::new(),
            client_origin: capture.client_origin,
        };
        action.validate()?;
        Ok(action)
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &S,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<BTreeMap<Target, InventoryCell>, ApplyFailure> {
        if !self.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        if !matches!(
            values.get(&self.mode),
            Some(Cell {
                value: Value::LivingProperty(LivingPropertyValue::Unsigned(1)),
                ..
            })
        ) {
            return Err(ApplyFailure::Condition(self.mode));
        }
        let mut cells = BTreeMap::new();
        for binding in &self.bindings {
            let Some(Cell {
                value: Value::Inventory(cell),
                ..
            }) = values.get(&binding.target)
            else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            if cell.loc != binding.slot.loc {
                return Err(ApplyFailure::Inventory(binding.target));
            }
            cells.insert(binding.target, cell.clone());
        }
        let mut changed = BTreeSet::new();
        for operation in &self.operations {
            if let CreativeInventoryOperation::Conserve(gesture) = operation {
                let mut symbolic = values.clone();
                for (target, cell) in &cells {
                    symbolic
                        .get_mut(target)
                        .ok_or(ApplyFailure::Inventory(*target))?
                        .value = Value::Inventory(cell.clone());
                }
                let intent = crate::canonical_inventory::InventoryGestureReplayIntent {
                    entity_equipment: Vec::new(),
                    gestures: vec![gesture.clone()],
                    bindings: self.bindings.clone(),
                    equipment: Vec::new(),
                };
                for effect in intent
                    .derive_effects_with_context(&symbolic, context)
                    .map_err(|_| ApplyFailure::InvalidAction)?
                {
                    let crate::canonical_action::Effect::Put {
                        target,
                        value: Value::Inventory(after),
                    } = effect
                    else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    cells.insert(target, after);
                    changed.insert(target);
                }
                continue;
            }
            let (template, destinations, fill) = match operation {
                CreativeInventoryOperation::Conserve(_) => unreachable!(),
                CreativeInventoryOperation::Set { destination, stack } => {
                    (stack.clone(), vec![*destination], false)
                }
                CreativeInventoryOperation::Clone {
                    source,
                    destination,
                } => {
                    if !cells[destination].is_empty() {
                        return Err(ApplyFailure::Inventory(*destination));
                    }
                    (cells[source].clone(), vec![*destination], true)
                }
                CreativeInventoryOperation::Fill {
                    source,
                    destinations,
                } => (cells[source].clone(), destinations.clone(), true),
            };
            if fill && template.is_empty() {
                return Err(ApplyFailure::InvalidAction);
            }
            for destination in destinations {
                let binding = self
                    .binding(destination)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let mut after = template.clone();
                after.loc = binding.slot.loc;
                if !after.is_empty() {
                    let cap = binding
                        .max_stack
                        .min(context.inventory_limit(&after, destination)?);
                    if cap == 0 || !fill && after.count > cap {
                        return Err(ApplyFailure::Inventory(destination));
                    }
                    if fill {
                        after.count = cap;
                    }
                }
                if cells[&destination] != after {
                    cells.insert(destination, after);
                    changed.insert(destination);
                }
            }
        }
        Ok(changed
            .into_iter()
            .map(|target| (target, cells.remove(&target).unwrap()))
            .collect())
    }
}
