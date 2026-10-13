use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, Cell, ChunkPart, ClientOrigin, Condition, Effect,
    Expected, Stakeholder, Target, UndoProcedure, Value,
};
use crate::entity_body::{DroppedItemChange, DroppedItemCondition, EntityBody, EntityPayload};
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

pub fn capture_pickup<S: crate::canonical_action::ActionState>(
    owners: &S,
    id: ActionId,
    contact: crate::item_contact::PickupContact,
    destinations: Vec<crate::canonical_action::InventoryReplayCandidate>,
    selector_revision: u64,
    destination_policy: crate::canonical_action::InventoryDestinationPolicy,
    selector_guard: Option<Condition>,
    equipment: Vec<crate::canonical_inventory::InventoryEquipmentBinding>,
) -> Result<Action, ItemLifecycleFailure> {
    use crate::canonical_action::{
        InventoryCountRule, InventoryReplayIntent, SemanticIntent, TruthView,
    };
    use crate::item_contact::ContactIdentity;
    let picker = owners.read(TruthView::Local, contact.picker.target());
    let hopper = picker.as_ref().is_some_and(|cell| matches!(&cell.value, Value::Entity(body)
        if matches!(&body.payload, EntityPayload::Vehicle(vehicle) if vehicle.kind == crate::entity_body::VehicleKind::HopperMinecart)));
    if !(if hopper { contact.valid_hopper() } else { contact.valid() })
        || id.actor != contact.picker.actor() || destinations.is_empty() {

        return Err(ItemLifecycleFailure::InvalidChange);
    }
    let source = contact.item.target();
    let Some(Cell {
        value: Value::Entity(body),
        ..
    }) = owners.read(TruthView::Local, source)
    else {
        return Err(ItemLifecycleFailure::InvalidEntity);
    };
    let ContactIdentity::Entity { id: item_id, birth } = &contact.item.identity else {
        return Err(ItemLifecycleFailure::InvalidEntity);
    };
    let EntityPayload::Item(item) = &body.payload else {
        return Err(ItemLifecycleFailure::InvalidEntity);
    };
    if body.id != *item_id
        || body.birth != *birth
        || body.spatial_epoch != contact.item.epoch
        || !item.matches(if hopper { DroppedItemCondition::HopperReady } else { DroppedItemCondition::PickupReady })
    {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let (player_leases, player_chunks) = match &contact.picker.identity {
        ContactIdentity::Player(lease) if id.incarnation == lease.incarnation => {
            (vec![*lease], vec![(lease.key, contact.picker.chunk)])
        }
        ContactIdentity::Entity { id: entity, .. }
            if id.incarnation == u64::from(entity.generation) =>
        {
            (Vec::new(), Vec::new())
        }
        _ => return Err(ItemLifecycleFailure::InvalidEntity),
    };
    let mut values = std::collections::BTreeMap::new();
    values.insert(source, Cell { revision: 0, value: Value::Entity((body.clone()).into()) });
    let mut conditions = vec![
        Condition { target: contact.picker.target(), expected: Expected::SpatialEpochEquals(contact.picker.epoch) },
        Condition { target: source, expected: Expected::SpatialEpochEquals(contact.item.epoch) },
        Condition { target: source, expected: Expected::DroppedItem { birth: birth.clone(),
            condition: if hopper { DroppedItemCondition::HopperReady } else { DroppedItemCondition::PickupReady } } },
    ];
    if let ContactIdentity::Entity { birth, .. } = &contact.picker.identity {
        conditions.push(Condition { target: contact.picker.target(), expected: Expected::ActorControlled { birth: birth.clone(), issuer: contact.picker.owner } });
    }
    if hopper {
        let ContactIdentity::Entity { birth, .. } = &contact.picker.identity else { return Err(ItemLifecycleFailure::InvalidEntity); };
        let expected = Expected::HopperCollector { birth: birth.clone(), owner: contact.picker.owner };
        if !expected.matches(picker.as_ref()) { return Err(ItemLifecycleFailure::InvalidEntity); }
        conditions.push(Condition { target: contact.picker.target(), expected });
        values.insert(contact.picker.target(), picker.clone().ok_or(ItemLifecycleFailure::InvalidEntity)?);
    } else {
        let health = Target::Chunk { key: contact.picker.chunk, part: ChunkPart::LivingProperty {
            actor: contact.picker.actor(), property: crate::living_property::LivingProperty::Health }, inventory: None };
        let health_cell = owners.read(TruthView::Local, health).ok_or(ItemLifecycleFailure::InvalidEntity)?;
        if !Expected::LivingUnsignedAtLeast(1).matches(Some(&health_cell)) { return Err(ItemLifecycleFailure::InvalidEntity); }
        values.insert(health, health_cell);
        conditions.push(Condition { target: health, expected: Expected::LivingUnsignedAtLeast(1) });
    }
    for destination in &destinations {
        let belongs_to_picker = match (&contact.picker.identity, destination.target) {
            (
                ContactIdentity::Player(lease),
                Target::Player {
                    key,
                    lease: Some(target_lease),
                    inventory: Some(_),
                },
            ) => key == lease.key && target_lease == *lease,
            (
                ContactIdentity::Entity { id, .. },
                Target::Chunk {
                    key,
                    part: ChunkPart::Entity(target),
                    inventory: Some(_),
                },
            ) => target == *id && key == contact.picker.chunk,
            _ => false,
        };
        if !belongs_to_picker || values.contains_key(&destination.target) {
            return Err(ItemLifecycleFailure::InvalidChange);
        }
        let cell = owners
            .read(TruthView::Local, destination.target)
            .ok_or(ItemLifecycleFailure::InvalidChange)?;
        conditions.push(Condition {
            target: destination.target,
            expected: Expected::Exact(cell.clone()),
        });
        values.insert(destination.target, cell);
    }
    if let Some(guard) = selector_guard {
        if guard.target.stakeholder() != contact.picker.target().stakeholder()
            || !guard
                .expected
                .matches(owners.read(TruthView::Local, guard.target).as_ref())
        {
            return Err(ItemLifecycleFailure::InvalidChange);
        }
        if let Some(cell) = owners.read(TruthView::Local, guard.target) {
            values.insert(guard.target, cell);
        }
        conditions.push(guard);
    }
    for binding in &equipment {
        for target in binding.targets() {
            if !values.contains_key(&target) {
                let cell = owners
                    .read(TruthView::Local, target)
                    .ok_or(ItemLifecycleFailure::InvalidChange)?;
                conditions.push(Condition {
                    target,
                    expected: Expected::Exact(cell.clone()),
                });
                values.insert(target, cell);
            }
        }
    }
    let intent = InventoryReplayIntent {
        source,
        item: item.item,
        components: item.components.clone(),
        count: if hopper { InventoryCountRule::Exact(1) } else { InventoryCountRule::All },
        ordered_destinations: destinations,
        selector_revision,
        destination_policy,
        equipment,
    };
    let effects = intent
        .derive_effects_with_context(&values, owners)
        .map_err(|_| ItemLifecycleFailure::InvalidChange)?;
    let footprint = values.keys().copied().collect::<Vec<_>>();
    let mut stakeholders = conditions
        .iter()
        .map(|condition| condition.target.stakeholder())
        .collect::<Vec<_>>();
    stakeholders.sort_unstable();
    stakeholders.dedup();
    Ok(Action {
        edit_authority: None,
        id,
        tick: contact.tick,
        class: ActionClass::Replayable,
        chunk_transition: false,
        presentation: Vec::new(),
        actor_chunk: Some(contact.picker.chunk),
        player_leases,
        player_chunks,
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: footprint.clone(),
        counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::Pickup {
            contact,
            inventory: intent,
        }),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::Item,
        client_origin: ClientOrigin::None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemLifecycleFailure {
    NotPrimary,
    InvalidEntity,
    InvalidChange,
    DuplicateEntity,
}

pub fn append_birth(
    action: &mut Action,
    body: EntityBody,
    chunk: ChunkKey,
) -> Result<(), ItemLifecycleFailure> {
    if !body.belongs_to(body.id, chunk)
        || !body.valid_at(action.tick)
        || body.birth.action
            != (crate::canonical_action::ActionRef {
                tick: action.tick,
                id: action.id,
            })
        || !matches!(
            body.payload,
            EntityPayload::Item(_) | EntityPayload::Experience(_) | EntityPayload::Projectile(_)
        )
    {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    if action.targets().contains(&target) {
        return Err(ItemLifecycleFailure::DuplicateEntity);
    }
    action.conditions.push(Condition {
        target,
        expected: Expected::Missing,
    });
    action.effects.push(Effect::Put {
        target,
        value: Value::Entity((body).into()),
    });
    action.stakeholders.push(Stakeholder::Chunk(chunk));
    action.stakeholders.sort_unstable();
    action.stakeholders.dedup();
    if action.undo.is_empty() {
        action.undo.push(UndoProcedure::RestoreWrittenTargets);
    }
    Ok(())
}

pub fn lifecycle_action(
    body: &EntityBody,
    chunk: ChunkKey,
    issuer: PeerGeneration,
    primary: PeerGeneration,
    tick: TickStamp,
    sequence: u16,
    change: DroppedItemChange,
) -> Result<Action, ItemLifecycleFailure> {
    if issuer != primary || issuer.generation == 0 {
        return Err(ItemLifecycleFailure::NotPrimary);
    }
    if !body.belongs_to(body.id, chunk) {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let EntityPayload::Item(item) = &body.payload else {
        return Err(ItemLifecycleFailure::InvalidEntity);
    };
    let condition = match change {
        DroppedItemChange::AdvanceLifetime => None,
        DroppedItemChange::Damage { amount_milli, fire } => {
            if amount_milli == 0 || fire && item.fire_immune {
                return Err(ItemLifecycleFailure::InvalidChange);
            }
            Some(DroppedItemCondition::Alive)
        }
        DroppedItemChange::Expire => {
            if !item.matches(DroppedItemCondition::Expired) {
                return Err(ItemLifecycleFailure::InvalidChange);
            }
            Some(DroppedItemCondition::Expired)
        }
    };
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    Ok(Action {
        edit_authority: None,
        id: ActionId {
            actor: ActorId::Entity(body.id),
            incarnation: u64::from(body.id.generation),
            sequence,
            family: 2,
        },
        tick,
        class: if condition.is_none() {
            ActionClass::Infallible
        } else {
            ActionClass::Undoable
        },
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: condition
            .into_iter()
            .map(|condition| Condition {
                target,
                expected: Expected::DroppedItem {
                    birth: body.birth.clone(),
                    condition,
                },
            })
            .collect(),
        effects: vec![Effect::ChangeDroppedItem {
            target,
            issuer,
            body_owner: body.owner,
            birth: body.birth.clone(),
            change,
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        presentation: Vec::new(),
        kind: ActionKind::Item,
        client_origin: ClientOrigin::None,
    })
}

pub fn chunk_transition(
    body: &EntityBody,
    source: ChunkKey,
    destination: ChunkKey,
    spatial: crate::entity_body::EntitySpatial,
    issuer: PeerGeneration,
    tick: TickStamp,
    sequence: u16,
) -> Result<Action, ItemLifecycleFailure> {
    if issuer.generation == 0
        || source == destination
        || !body.belongs_to(body.id, source)
        || !spatial.belongs_to(destination)
        || !matches!(
            body.payload,
            EntityPayload::Item(_) | EntityPayload::Experience(_)
        )
        || matches!(body.payload, EntityPayload::Experience(_)) && body.owner != issuer
    {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let source_target = Target::Chunk {
        key: source,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let destination_target = Target::Chunk {
        key: destination,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let mut stakeholders = vec![Stakeholder::Chunk(source), Stakeholder::Chunk(destination)];
    stakeholders.sort_unstable();
    let action = Action {
        edit_authority: None,
        id: ActionId {
            actor: ActorId::Entity(body.id),
            incarnation: u64::from(body.id.generation),
            sequence,
            family: 8,
        },
        presentation: Vec::new(),
        tick,
        class: ActionClass::Infallible,
        chunk_transition: true,
        actor_chunk: Some(source),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders,
        dependencies: Vec::new(),
        conditions: Vec::new(),
        effects: vec![Effect::MoveEntityBody {
            source: source_target,
            destination: destination_target,
            spatial,
            authority: Some(crate::canonical_action::EntityMoveAuthority {
                birth: body.birth.clone(),
                body_owner: body.owner,
                issuer,
            }),
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::Actor,
        client_origin: ClientOrigin::None,
    };
    action
        .validate()
        .map_err(|_| ItemLifecycleFailure::InvalidChange)?;
    Ok(action)
}

pub fn merge_action(
    source: (&EntityBody, ChunkKey, u64),
    destination: (&EntityBody, ChunkKey, u64),
    issuer: PeerGeneration,
    primary: PeerGeneration,
    tick: TickStamp,
    sequence: u16,
    max_stack: u8,
) -> Result<Action, ItemLifecycleFailure> {
    let mut action = lifecycle_action(
        source.0,
        source.1,
        issuer,
        primary,
        tick,
        sequence,
        DroppedItemChange::AdvanceLifetime,
    )?;
    if source.0.id == destination.0.id || !destination.0.belongs_to(destination.0.id, destination.1)
    {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let (EntityPayload::Item(source_item), EntityPayload::Item(destination_item)) =
        (&source.0.payload, &destination.0.payload)
    else {
        return Err(ItemLifecycleFailure::InvalidEntity);
    };
    if !destination_item.clone().merge_from(source_item, max_stack) {
        return Err(ItemLifecycleFailure::InvalidChange);
    }
    let targets = [source, destination].map(|(body, chunk, revision)| {
        (
            Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            },
            Cell {
                revision,
                value: Value::Entity((body.clone()).into()),
            },
        )
    });
    action.class = ActionClass::Undoable;
    action.stakeholders.push(Stakeholder::Chunk(destination.1));
    action.stakeholders.sort_unstable();
    action.stakeholders.dedup();
    action.conditions = targets
        .iter()
        .flat_map(|(target, cell)| {
            let Value::Entity(body) = &cell.value else {
                unreachable!();
            };
            let EntityPayload::Item(item) = &body.payload else {
                unreachable!();
            };
            [
                Condition {
                    target: *target,
                    expected: Expected::DroppedItemStack {
                        birth: body.birth.clone(),
                        owner: body.owner,
                        epoch: body.spatial_epoch,
                        item: item.item,
                        count: item.count,
                        components: item.components.clone(),
                    },
                },
                Condition {
                    target: *target,
                    expected: Expected::DroppedItem {
                        birth: body.birth.clone(),
                        condition: DroppedItemCondition::Alive,
                    },
                },
            ]
        })
        .collect();
    action.effects = vec![Effect::MergeDroppedItems {
        source: targets[0].0,
        destination: targets[1].0,
        max_stack,
        count: source_item.count,
    }];
    Ok(action)
}

pub fn derive_merge(
    contact: &crate::item_contact::MergeContact,
    max_stack: u8,
    values: &std::collections::BTreeMap<Target, Cell>,
) -> Result<Vec<Effect>, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::ApplyFailure;
    if !contact.valid() || !(1..=99).contains(&max_stack) {
        return Err(ApplyFailure::InvalidAction);
    }
    let source = contact.source.target();
    let destination = contact.destination.target();
    let body = |observation: &crate::item_contact::ContactObservation| {
        let crate::item_contact::ContactIdentity::Entity { id, birth } = &observation.identity
        else {
            return None;
        };
        let Value::Entity(body) = &values.get(&observation.target())?.value else {
            return None;
        };
        (body.id == *id
            && body.birth == *birth
            && body.owner == observation.owner
            && body.spatial_epoch == observation.epoch)
            .then_some(body)
    };
    let source_body = body(&contact.source).ok_or(ApplyFailure::Condition(source))?;
    let destination_body =
        body(&contact.destination).ok_or(ApplyFailure::Condition(destination))?;
    let (EntityPayload::Item(source_item), EntityPayload::Item(destination_item)) =
        (&source_body.payload, &destination_body.payload)
    else {
        return Err(ApplyFailure::Type(source));
    };
    if source_item.item != destination_item.item
        || source_item.components != destination_item.components
        || !source_item.matches(DroppedItemCondition::Alive)
        || !destination_item.matches(DroppedItemCondition::Alive)
    {
        return Err(ApplyFailure::InventoryConservation);
    }
    let count = source_item
        .count
        .min(max_stack.saturating_sub(destination_item.count));
    if count == 0 {
        return Err(ApplyFailure::InventoryConservation);
    }
    Ok(vec![Effect::MergeDroppedItems {
        source,
        destination,
        max_stack,
        count,
    }])
}

pub fn capture_merge<S: crate::canonical_action::ActionState>(
    owners: &S,
    id: ActionId,
    contact: crate::item_contact::MergeContact,
    max_stack: u8,
) -> Result<Action, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::{ApplyFailure, SemanticIntent, TruthView};
    if !contact.valid() || id.actor != contact.source.actor() {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut values = std::collections::BTreeMap::new();
    let mut conditions = Vec::new();
    for participant in [&contact.source, &contact.destination] {
        let target = participant.target();
        let cell = owners
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        let Value::Entity(body) = &cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        let EntityPayload::Item(item) = &body.payload else {
            return Err(ApplyFailure::Type(target));
        };
        conditions.extend([
            Condition {
                target,
                expected: Expected::DroppedItemStack {
                    birth: body.birth.clone(),
                    owner: body.owner,
                    epoch: body.spatial_epoch,
                    item: item.item,
                    count: item.count,
                    components: item.components.clone(),
                },
            },
            Condition {
                target,
                expected: Expected::DroppedItem {
                    birth: body.birth.clone(),
                    condition: DroppedItemCondition::Alive,
                },
            },
        ]);
        values.insert(target, cell);
    }
    let effects = derive_merge(&contact, max_stack, &values)?;
    let footprint = values.keys().copied().collect::<Vec<_>>();
    let mut stakeholders = footprint
        .iter()
        .map(|target| target.stakeholder())
        .collect::<Vec<_>>();
    stakeholders.sort_unstable();
    stakeholders.dedup();
    let action = Action {
        edit_authority: None,
        id,
        tick: contact.tick,
        class: ActionClass::Replayable,
        chunk_transition: false,
        presentation: Vec::new(),
        actor_chunk: Some(contact.source.chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: footprint.clone(),
        counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::MergeDroppedItems { contact, max_stack }),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::Item,
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}

pub fn hopper_pickup_guard(action: &Action, contact: &crate::item_contact::PickupContact,
    inventory: &crate::canonical_action::InventoryReplayIntent) -> bool {
    use crate::canonical_action::{InventoryCountRule, InventoryId};
    let crate::item_contact::ContactIdentity::Entity { id, birth } = &contact.picker.identity else { return false; };
    contact.valid_hopper()
        && action.conditions.iter().any(|condition| condition.target == contact.picker.target()
            && condition.expected == (Expected::HopperCollector { birth: birth.clone(), owner: contact.picker.owner }))
        && inventory.count == InventoryCountRule::Exact(1)
        && inventory.equipment.is_empty()
        && inventory.ordered_destinations.len() == 5
        && inventory.ordered_destinations.iter().enumerate().all(|(slot, candidate)| {
            candidate.target == (Target::Chunk { key: contact.picker.chunk, part: ChunkPart::Entity(*id),
                inventory: Some(InventoryId::Entity { inv: 0, slot: slot as u16 }) })
        })
}

impl crate::canonical_action::InventoryReplayIntent {
    pub(crate) fn hopper_manifest(&self, values: &std::collections::BTreeMap<Target, Cell>) -> bool {
        use crate::canonical_action::{InventoryCountRule, InventoryId};
        self.count == InventoryCountRule::Exact(1) && self.ordered_destinations.len() == 5
            && self.equipment.is_empty() && values.iter().any(|(target, cell)| {
                let Target::Chunk { key, part: ChunkPart::Entity(id), inventory: None } = target else { return false; };
                matches!(&cell.value, Value::Entity(body) if body.id == *id && matches!(&body.payload,
                    EntityPayload::Vehicle(vehicle) if vehicle.kind == crate::entity_body::VehicleKind::HopperMinecart && vehicle.hopper_enabled && vehicle.damage_milli <= 40_000))
                    && self.ordered_destinations.iter().enumerate().all(|(slot, candidate)| candidate.target == (Target::Chunk {
                        key: *key, part: ChunkPart::Entity(*id), inventory: Some(InventoryId::Entity { inv: 0, slot: slot as u16 }) }))
            })
    }
}
