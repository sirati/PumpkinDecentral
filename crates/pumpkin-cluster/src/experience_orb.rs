use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ChunkPart, ClientOrigin, Effect, Stakeholder,
    Target, UndoProcedure,
};
use crate::entity_body::{EntityBody, EntityPayload};
use crate::protocol::ChunkKey;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExperienceHazard {
    Fire,
    Lava,
    Cactus,
    Void,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceMerge {
    pub contact: crate::item_contact::MergeContact,
    pub amount: u32,
}

impl ExperienceMerge {
    pub fn targets(&self) -> [Target; 2] {
        [
            self.contact.source.target(),
            self.contact.destination.target(),
        ]
    }

    pub fn derive(
        &self,
        values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>,
    ) -> Result<Vec<Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Value};
        if !self.contact.valid()
            || self.amount == 0
            || self.contact.source.owner != self.contact.producer
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut bodies = Vec::new();
        for participant in [&self.contact.source, &self.contact.destination] {
            let crate::item_contact::ContactIdentity::Entity { birth, .. } = &participant.identity
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let Some(crate::canonical_action::Cell {
                value: Value::Entity(body),
                ..
            }) = values.get(&participant.target())
            else {
                return Err(ApplyFailure::Condition(participant.target()));
            };
            if body.birth != *birth
                || body.owner != participant.owner
                || body.spatial_epoch != participant.epoch
                || !matches!(&body.payload, EntityPayload::Experience(orb) if orb.amount == self.amount
                    && orb.count > 0 && orb.health_milli > 0 && orb.age < 6000)
            {
                return Err(ApplyFailure::Condition(participant.target()));
            }
            bodies.push(body);
        }
        let EntityPayload::Experience(source) = &bodies[0].payload else {
            unreachable!();
        };
        let mut after = bodies[1].clone();
        let EntityPayload::Experience(destination) = &mut after.payload else {
            unreachable!();
        };
        destination.count = destination
            .count
            .checked_add(source.count)
            .filter(|count| *count <= i32::MAX as u32)
            .ok_or(ApplyFailure::Overflow(self.contact.destination.target()))?;
        destination.age = destination.age.min(source.age);
        Ok(vec![
            Effect::Delete {
                target: self.contact.source.target(),
            },
            Effect::Put {
                target: self.contact.destination.target(),
                value: Value::Entity(after),
            },
        ])
    }
}

pub fn capture_merge<S: crate::canonical_action::ActionState>(
    context: &S,
    id: ActionId,
    intent: ExperienceMerge,
) -> Result<Action, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::{ApplyFailure, Condition, Expected, SemanticIntent, TruthView};
    if !intent.contact.valid() || id.actor != intent.contact.source.actor() {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut values = std::collections::BTreeMap::new();
    let mut conditions = Vec::new();
    for participant in [&intent.contact.source, &intent.contact.destination] {
        let crate::item_contact::ContactIdentity::Entity { birth, .. } = &participant.identity
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        values.insert(
            participant.target(),
            context
                .read(TruthView::Local, participant.target())
                .ok_or(ApplyFailure::Condition(participant.target()))?,
        );
        conditions.push(Condition {
            target: participant.target(),
            expected: Expected::ExperienceOrb {
                birth: birth.clone(),
                owner: participant.owner,
                epoch: participant.epoch,
                amount: intent.amount,
            },
        });
    }
    let crate::item_contact::ContactIdentity::Entity { birth, .. } =
        &intent.contact.source.identity
    else {
        unreachable!();
    };
    conditions.push(Condition {
        target: intent.contact.source.target(),
        expected: Expected::ActorControlled {
            birth: birth.clone(),
            issuer: intent.contact.source.owner,
        },
    });
    let mut stakeholders = intent
        .targets()
        .into_iter()
        .map(|target| target.stakeholder())
        .collect::<Vec<_>>();
    stakeholders.sort_unstable();
    stakeholders.dedup();
    let action = Action {
        id,
        edit_authority: None,
        tick: intent.contact.tick,
        class: ActionClass::Replayable,
        kind: ActionKind::Item,
        chunk_transition: false,
        actor_chunk: Some(intent.contact.source.chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects: intent.derive(&values)?,
        counterfactual_conditions: intent.targets().into(),
        counterfactual_effects: intent.targets().into(),
        semantic_intent: Some(SemanticIntent::MergeExperience(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None,
        presentation: Vec::new(),
    };
    action.validate()?;
    Ok(action)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceDamage {
    pub target: Target,
    pub birth: crate::entity_body::EntityBirth,
    pub owner: PeerGeneration,
    pub epoch: crate::canonical_action::SpatialEpoch,
    pub amount: u32,
    pub hazard: ExperienceHazard,
}

impl ExperienceDamage {
    pub fn derive(
        &self,
        values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>,
    ) -> Result<Vec<Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Value};
        let Some(crate::canonical_action::Cell {
            value: Value::Entity(body),
            ..
        }) = values.get(&self.target)
        else {
            return Err(ApplyFailure::Condition(self.target));
        };
        let EntityPayload::Experience(orb) = &body.payload else {
            return Err(ApplyFailure::Type(self.target));
        };
        if body.birth != self.birth
            || body.owner != self.owner
            || body.spatial_epoch != self.epoch
            || orb.amount != self.amount
            || orb.count == 0
            || orb.health_milli == 0
        {
            return Err(ApplyFailure::Condition(self.target));
        }
        let damage = match self.hazard {
            ExperienceHazard::Fire | ExperienceHazard::Cactus => 1_000,
            ExperienceHazard::Lava => 4_000,
            ExperienceHazard::Void => 5_000,
        };
        let mut after = body.clone();
        let EntityPayload::Experience(orb) = &mut after.payload else {
            unreachable!();
        };
        orb.health_milli = orb.health_milli.saturating_sub(damage);
        Ok(vec![if orb.health_milli == 0 {
            Effect::Delete {
                target: self.target,
            }
        } else {
            Effect::Put {
                target: self.target,
                value: Value::Entity(after),
            }
        }])
    }
}

pub fn capture_damage(
    id: ActionId,
    tick: TickStamp,
    cell: &crate::canonical_action::Cell,
    chunk: ChunkKey,
    hazard: ExperienceHazard,
) -> Result<Action, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::{ApplyFailure, Condition, Expected, SemanticIntent, Value};
    let Value::Entity(body) = &cell.value else {
        return Err(ApplyFailure::InvalidAction);
    };
    let EntityPayload::Experience(orb) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    if id.actor != ActorId::Entity(body.id)
        || id.incarnation != u64::from(body.id.generation)
        || !body.belongs_to(body.id, chunk)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let intent = ExperienceDamage {
        target,
        birth: body.birth.clone(),
        owner: body.owner,
        epoch: body.spatial_epoch,
        amount: orb.amount,
        hazard,
    };
    let values = [(target, cell.clone())].into_iter().collect();
    let action = Action {
        id,
        edit_authority: None,
        tick,
        class: ActionClass::Replayable,
        kind: ActionKind::Item,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: vec![
            Condition {
                target,
                expected: Expected::ActorControlled {
                    birth: body.birth.clone(),
                    issuer: body.owner,
                },
            },
            Condition {
                target,
                expected: Expected::ExperienceOrb {
                    birth: body.birth.clone(),
                    owner: body.owner,
                    epoch: body.spatial_epoch,
                    amount: orb.amount,
                },
            },
        ],
        effects: intent.derive(&values)?,
        counterfactual_conditions: vec![target],
        counterfactual_effects: vec![target],
        semantic_intent: Some(SemanticIntent::ExperienceDamage(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None,
        presentation: Vec::new(),
    };
    action.validate()?;
    Ok(action)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceCollection {
    pub contact: crate::item_contact::PickupContact,
    pub amount: u32,
    pub experience: Target,
    pub cooldowns: Target,
    pub equipment: crate::canonical_inventory::InventoryEquipmentBinding,
}

impl ExperienceCollection {
    pub fn health_target(&self) -> Target {
        Target::Chunk {
            key: self.contact.picker.chunk,
            part: ChunkPart::LivingProperty {
                actor: self.contact.picker.actor(),
                property: crate::living_property::LivingProperty::Health,
            },
            inventory: None,
        }
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        [
            self.contact.picker.target(),
            self.contact.item.target(),
            self.experience,
            self.cooldowns,
            self.health_target(),
        ]
        .into_iter()
        .chain(self.equipment.targets())
    }

    pub fn valid(&self) -> bool {
        use crate::item_contact::ContactIdentity;
        use crate::living_property::LivingProperty as P;
        let ContactIdentity::Player(lease) = &self.contact.picker.identity else {
            return false;
        };
        let property = |property| Target::Chunk {
            key: self.contact.picker.chunk,
            part: ChunkPart::LivingProperty {
                actor: ActorId::Player(lease.gid),
                property,
            },
            inventory: None,
        };
        self.contact.valid()
            && self.equipment.is_valid()
            && self.equipment.lease == *lease
            && self.equipment.living == self.contact.picker.target()
            && self.amount > 0
            && self.amount <= i32::MAX as u32
            && matches!(self.contact.item.identity, ContactIdentity::Entity { .. })
            && self.experience == property(P::Experience)
            && self.cooldowns == property(P::Cooldowns)
    }

    pub fn derive<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        id: ActionId,
        tick: TickStamp,
        values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>,
    ) -> Result<Vec<Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Value};
        use crate::living_property::LivingPropertyValue as V;
        if !self.valid() || self.contact.picker.simulation_tick != tick {
            return Err(ApplyFailure::InvalidAction);
        }
        if !crate::canonical_action::Expected::LivingUnsignedAtLeast(1)
            .matches(values.get(&self.health_target()))
        {
            return Err(ApplyFailure::Condition(self.health_target()));
        }
        let Some(crate::canonical_action::Cell {
            value: Value::Entity(body),
            ..
        }) = values.get(&self.contact.item.target())
        else {
            return Err(ApplyFailure::Condition(self.contact.item.target()));
        };
        let crate::item_contact::ContactIdentity::Entity { birth, .. } =
            &self.contact.item.identity
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        if body.birth != *birth
            || body.owner != self.contact.item.owner
            || body.spatial_epoch != self.contact.item.epoch
            || !matches!(&body.payload, EntityPayload::Experience(orb) if orb.amount == self.amount && orb.age < 6000 && orb.count > 0 && orb.health_milli > 0)
        {
            return Err(ApplyFailure::Condition(self.contact.item.target()));
        }
        let Some(crate::canonical_action::Cell {
            value: Value::LivingProperty(V::Experience(before)),
            ..
        }) = values.get(&self.experience)
        else {
            return Err(ApplyFailure::Type(self.experience));
        };
        let Some(Value::Inventory(selected)) = values.get(&self.equipment.selected).map(|cell| &cell.value) else {
            return Err(ApplyFailure::Inventory(self.equipment.selected));
        };
        if !(1..=9).contains(&selected.item) { return Err(ApplyFailure::InvalidAction); }
        let mut repair_targets = vec![self.equipment.hotbar[usize::from(selected.item - 1)], self.equipment.offhand];
        repair_targets.extend(self.equipment.armor);
        repair_targets.sort_by_key(|target| {
            let index = match target {
                Target::Player { inventory: Some(crate::canonical_action::InventoryId::Player { inv, slot }), .. } => (u64::from(*inv) << 16) | u64::from(*slot),
                _ => 0,
            };
            crate::order::rank_tick_actor(id.incarnation ^ (u64::from(id.sequence) << 32) ^ index, tick, id.actor)
        });
        let mut unspent = self.amount;
        let mut repairs = Vec::new();
        let mut projected = values.clone();
        for target in repair_targets {
            if unspent == 0 { break; }
            let Some(Value::Inventory(source)) = values.get(&target).map(|cell| &cell.value) else {
                return Err(ApplyFailure::Inventory(target));
            };
            let (output, used) = context.mending_repair(source, unspent)?;
            if used > unspent || output.remainder.is_some() { return Err(ApplyFailure::InvalidAction); }
            if output.source_after == *source { continue; }
            let crate::canonical_action::Target::Player { inventory: Some(crate::canonical_action::InventoryId::Player { inv, slot }), .. } = target else {
                return Err(ApplyFailure::Inventory(target));
            };
            repairs.push(Effect::TransformItems { intent: crate::canonical_inventory::ItemTransformIntent {
                source: crate::canonical_inventory::InventorySlotBinding {
                    slot: crate::inventory::InventorySlotRef::new(crate::inventory::InventoryTarget::Player(self.equipment.lease.gid), crate::inventory::InvLoc::new(inv, slot)),
                    target, max_stack: 99,
                },
                item: source.item, components: source.nbt.clone(),
                operation: crate::canonical_inventory::ItemTransformOperation::MendingRepair { experience: unspent },
                remainder_destinations: Vec::new(),
            } });
            projected.insert(target, crate::canonical_action::Cell { revision: values[&target].revision,
                value: Value::Inventory(output.source_after) });
            unspent -= used;
        }
        let after = before
            .collect(unspent)
            .ok_or(ApplyFailure::Overflow(self.experience))?;
        let Some(crate::canonical_action::Cell {
            value: Value::LivingProperty(V::Cooldowns(before_cooldowns)),
            ..
        }) = values.get(&self.cooldowns)
        else {
            return Err(ApplyFailure::Type(self.cooldowns));
        };
        let group = "minecraft:experience_pickup";
        if before_cooldowns
            .get(group)
            .is_some_and(|cooldown| cooldown.until.is_after(tick))
        {
            return Err(ApplyFailure::Condition(self.cooldowns));
        }
        let crate::item_contact::ContactIdentity::Player(lease) = &self.contact.picker.identity
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let mut cooldowns = before_cooldowns.clone();
        let revision = cooldowns
            .get(group)
            .map_or(Some(1), |state| state.revision.checked_add(1))
            .ok_or(ApplyFailure::Overflow(self.cooldowns))?;
        cooldowns.insert(
            group.to_string(),
            crate::item_action::ItemCooldownState {
                actor: ActorId::Player(lease.gid),
                group: group.to_string(),
                until: TickStamp(tick.0.wrapping_add(2)),
                revision,
            },
        );
        let mut remaining = body.clone();
        let EntityPayload::Experience(orb) = &mut remaining.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        orb.count -= 1;
        let body_effect = if orb.count == 0 {
            Effect::Delete {
                target: self.contact.item.target(),
            }
        } else {
            Effect::Put {
                target: self.contact.item.target(),
                value: Value::Entity((remaining).into()),
            }
        };
        let mut effects = vec![
            body_effect,
            Effect::Put {
                target: self.experience,
                value: Value::LivingProperty(V::Experience(after)),
            },
            Effect::Put {
                target: self.cooldowns,
                value: Value::LivingProperty(V::Cooldowns(cooldowns)),
            },
        ];
        effects.extend(repairs);
        crate::canonical_inventory::append_equipment_effects_from_projection(values, &projected, &mut effects, std::slice::from_ref(&self.equipment), context)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(effects)
    }
}

pub fn capture_collection<S: crate::canonical_action::ActionState>(
    context: &S,
    id: ActionId,
    tick: TickStamp,
    intent: ExperienceCollection,
) -> Result<Action, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::{ApplyFailure, Condition, Expected, SemanticIntent, TruthView};
    if !intent.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let crate::item_contact::ContactIdentity::Player(lease) = &intent.contact.picker.identity
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let crate::item_contact::ContactIdentity::Entity { birth, .. } = &intent.contact.item.identity
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let mut values = std::collections::BTreeMap::new();
    for target in intent.targets() {
        values.insert(
            target,
            context
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?,
        );
    }
    let effects = intent.derive(context, id, tick, &values)?;
    let mut conditions = vec![
        Condition {
            target: intent.health_target(),
            expected: Expected::LivingUnsignedAtLeast(1),
        },
        Condition {
            target: intent.contact.picker.target(),
            expected: Expected::SpatialEpochEquals(intent.contact.picker.epoch),
        },
        Condition {
            target: intent.contact.item.target(),
            expected: Expected::ExperienceOrb {
                birth: birth.clone(),
                owner: intent.contact.item.owner,
                epoch: intent.contact.item.epoch,
                amount: intent.amount,
            },
        },
        Condition {
            target: intent.experience,
            expected: Expected::Exact(values[&intent.experience].clone()),
        },
        Condition {
            target: intent.cooldowns,
            expected: Expected::Exact(values[&intent.cooldowns].clone()),
        },
    ];
    for target in intent.equipment.targets() {
        conditions.push(Condition { target, expected: Expected::Exact(values[&target].clone()) });
    }
    let footprint = intent.targets().collect::<Vec<_>>();
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
        actor_chunk: Some(intent.contact.picker.chunk),
        player_leases: vec![*lease],
        player_chunks: vec![(lease.key, intent.contact.picker.chunk)],
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: footprint.clone(),
        counterfactual_effects: footprint,
        semantic_intent: Some(SemanticIntent::CollectExperience(intent)),
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None,
        presentation: Vec::new(),
    };
    action.validate()?;
    Ok(action)
}

pub fn lifetime(
    body: &EntityBody,
    chunk: ChunkKey,
    source: PeerGeneration,
    tick: TickStamp,
    sequence: u16,
) -> Result<Action, crate::canonical_action::ApplyFailure> {
    if !body.belongs_to(body.id, chunk)
        || source != body.owner
        || source.generation == 0
        || !matches!(body.payload, EntityPayload::Experience(_))
    {
        return Err(crate::canonical_action::ApplyFailure::InvalidAction);
    }
    let action = Action {
        edit_authority: None,
        id: ActionId {
            actor: ActorId::Entity(body.id),
            incarnation: u64::from(body.id.generation),
            sequence,
            family: 2,
        },
        tick,
        class: ActionClass::Infallible,
        kind: ActionKind::Item,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: Vec::new(),
        effects: vec![Effect::AdvanceExperienceLifetime {
            target: Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            },
            owner: source,
            birth: body.birth.clone(),
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        client_origin: ClientOrigin::None,
        presentation: Vec::new(),
    };
    action.validate()?;
    Ok(action)
}
