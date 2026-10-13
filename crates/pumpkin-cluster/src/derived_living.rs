use crate::canonical_action::{AppliedWrite, ApplyFailure, Cell, Target, Value};
use crate::living_property::{LivingProperty, body_target, state, state_mut};

pub fn actor_body_target(target: Target) -> Option<Target> {
    body_target(target)
        .map(|(body, _)| body)
        .or_else(|| crate::actor_relation::body_target(target).map(|(body, _)| body))
        .or_else(|| crate::vehicle_property::body_target(target).map(|(body, _)| body))
        .or_else(|| crate::object_property::body_target(target).map(|(body, _)| body))
        .or_else(|| crate::actor_tags::body_target(target))
}

pub fn actor_facet_cell(body: &Cell, target: Target) -> Option<Cell> {
    if let Some((_, property)) = body_target(target) {
        facet_cell(body, property)
    } else if let Some((_, relation)) = crate::actor_relation::body_target(target) {
        crate::actor_relation::cell(body, relation)
    } else if let Some((_, property)) = crate::vehicle_property::body_target(target) {
        crate::vehicle_property::cell(body, property)
    } else if crate::actor_tags::body_target(target).is_some() {
        crate::actor_tags::cell(body)
    } else {
        crate::object_property::cell(body, crate::object_property::body_target(target)?.1)
    }
}

pub fn facet_cell(body: &Cell, property: LivingProperty) -> Option<Cell> {
    let living = state(&body.value)?;
    Some(Cell {
        revision: living
            .property_versions
            .get(&property)
            .copied()
            .unwrap_or(0),
        value: Value::LivingProperty(property.read(living)),
    })
}

pub fn normalize_species_writes(
    target: Target,
    before: &Cell,
    writes: &mut Vec<AppliedWrite>,
) -> Result<(), ApplyFailure> {
    use crate::living_property::LivingPropertyValue;
    use crate::mob_species_property::MobSpeciesProperty;
    if writes.iter().any(|write| write.target == target) {
        return Ok(());
    }
    let is_species = |property| {
        matches!(
            property,
            LivingProperty::MobSpecies | LivingProperty::MobSpeciesProperty(_)
        )
    };
    let species_writes = writes
        .iter()
        .filter(|write| {
            body_target(write.target)
                .is_some_and(|(body, property)| body == target && is_species(property))
        })
        .collect::<Vec<_>>();
    if species_writes.is_empty() {
        return Ok(());
    }
    let mut projected = before.clone();
    for broad in [true, false] {
        for write in &species_writes {
            let (_, property) =
                body_target(write.target).ok_or(ApplyFailure::Type(write.target))?;
            if (property == LivingProperty::MobSpecies) != broad {
                continue;
            }
            if facet_cell(before, property) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let Some(Cell {
                value: Value::LivingProperty(value),
                ..
            }) = &write.after
            else {
                return Err(ApplyFailure::Type(write.target));
            };
            let living = state_mut(&mut projected.value).ok_or(ApplyFailure::Type(target))?;
            if !property.write(living, value) {
                return Err(ApplyFailure::Type(write.target));
            }
        }
    }
    let properties = std::iter::once(LivingProperty::MobSpecies).chain(
        MobSpeciesProperty::ALL
            .iter()
            .copied()
            .map(LivingProperty::MobSpeciesProperty),
    );
    let mut aliases = Vec::new();
    for property in properties {
        let Some(old) = facet_cell(before, property) else {
            return Err(ApplyFailure::Type(target));
        };
        let Some(mut next) = facet_cell(&projected, property) else {
            return Err(ApplyFailure::Type(target));
        };
        if old.value == next.value {
            continue;
        }
        next.revision = old
            .revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(target))?;
        let Target::Chunk {
            key,
            part: crate::canonical_action::ChunkPart::Entity(entity),
            inventory: None,
        } = target
        else {
            return Err(ApplyFailure::Type(target));
        };
        let alias = Target::Chunk {
            key,
            part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: crate::actor::ActorId::Entity(entity),
                property,
            },
            inventory: None,
        };
        if let Some(explicit) = species_writes.iter().find(|write| write.target == alias) {
            if explicit.after.as_ref() != Some(&next) {
                return Err(ApplyFailure::Condition(alias));
            }
        } else {
            if matches!(
                next.value,
                Value::LivingProperty(LivingPropertyValue::MobSpeciesProperty(None))
            ) {
                continue;
            }
            aliases.push(AppliedWrite {
                target: alias,
                before: Some(old),
                after: Some(next),
            });
        }
    }
    if writes
        .len()
        .checked_add(aliases.len())
        .is_none_or(|count| count > 4096)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    writes.extend(aliases);
    Ok(())
}

pub fn normalize_body_versions(
    target: Target,
    before: Option<&Cell>,
    after: &mut Cell,
) -> Result<(), ApplyFailure> {
    if !crate::actor_tags::normalize_body_versions(before, after) {
        return Err(ApplyFailure::Type(target));
    }
    if let Some(before) = before {
        if !crate::actor_relation::normalize_body(before, after) {
            return Err(ApplyFailure::Type(target));
        }
        if matches!(&before.value, Value::Entity(body) if matches!(body.payload, crate::entity_body::EntityPayload::Vehicle(_)))
            && !crate::vehicle_property::normalize_body_versions(before, after)
        {
            return Err(ApplyFailure::Type(target));
        }
        if matches!(&before.value, Value::Entity(body) if matches!(body.payload, crate::entity_body::EntityPayload::Object(_)))
            && !crate::object_property::normalize_body_versions(before, after)
        {
            return Err(ApplyFailure::Type(target));
        }
    } else {
        match &mut after.value {
            Value::Living(living) => {
                if let Some(avatar) = &mut living.avatar {
                    avatar.relation_versions.clear();
                }
            }
            Value::Entity(body) => {
                body.relation_versions.clear();
                if let crate::entity_body::EntityPayload::Vehicle(vehicle) = &mut body.payload {
                    vehicle.property_versions.clear();
                }
                if let crate::entity_body::EntityPayload::Object(object) = &mut body.payload {
                    object.property_versions.clear();
                }
            }
            _ => {}
        }
    }
    let Some(next) = state_mut(&mut after.value) else {
        return Ok(());
    };
    let Some(previous) = before.and_then(|cell| state(&cell.value)) else {
        if before.is_some() {
            return Err(ApplyFailure::Type(target));
        }
        next.property_versions.clear();
        return Ok(());
    };
    let properties = LivingProperty::all(previous)
        .into_iter()
        .chain(LivingProperty::all(next))
        .chain(previous.property_versions.keys().copied())
        .collect::<std::collections::BTreeSet<_>>();
    let mut versions = previous.property_versions.clone();
    for property in properties {
        if property.read(previous) != property.read(next) {
            let revision = previous
                .property_versions
                .get(&property)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(ApplyFailure::Overflow(target))?;
            versions.insert(property, revision);
        }
    }
    next.property_versions = versions;
    Ok(())
}

pub fn project_body_writes(
    target: Target,
    before: Option<&Cell>,
    writes: &[AppliedWrite],
) -> Result<Option<Cell>, ApplyFailure> {
    if let Some(write) = writes.iter().find(|write| write.target == target) {
        if write.before.as_ref() != before {
            return Err(ApplyFailure::Condition(target));
        }
        for facet in writes
            .iter()
            .filter(|write| actor_body_target(write.target) == Some(target))
        {
            if before.and_then(|cell| actor_facet_cell(cell, facet.target)) != facet.before
                || write
                    .after
                    .as_ref()
                    .and_then(|cell| actor_facet_cell(cell, facet.target))
                    != facet.after
            {
                return Err(ApplyFailure::Condition(facet.target));
            }
        }
        return crate::frame_inventory_projection::project(target, before, write.after.clone(), writes);
    }
    let facets = writes
        .iter()
        .filter(|write| actor_body_target(write.target) == Some(target))
        .collect::<Vec<_>>();
    if facets.is_empty() {
        return crate::frame_inventory_projection::project(target, before, before.cloned(), writes);
    }
    let mut projected = before.cloned().ok_or(ApplyFailure::Condition(target))?;
    for write in facets {
        if crate::actor_tags::body_target(write.target).is_some() {
            if crate::actor_tags::cell(&projected) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let after = write.after.as_ref().ok_or(ApplyFailure::Type(write.target))?;
            if !crate::actor_tags::patch_cell(&mut projected, after) {
                return Err(ApplyFailure::Type(write.target));
            }
            continue;
        }
        if let Some((_, property)) = crate::object_property::body_target(write.target) {
            if crate::object_property::cell(&projected, property) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let after = write
                .after
                .as_ref()
                .ok_or(ApplyFailure::Type(write.target))?;
            if !crate::object_property::patch_cell(&mut projected, property, after) {
                return Err(ApplyFailure::Type(write.target));
            }
            continue;
        }
        if let Some((_, property)) = crate::vehicle_property::body_target(write.target) {
            if crate::vehicle_property::cell(&projected, property) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let after = write
                .after
                .as_ref()
                .ok_or(ApplyFailure::Type(write.target))?;
            if !crate::vehicle_property::patch_cell(&mut projected, property, after) {
                return Err(ApplyFailure::Type(write.target));
            }
            continue;
        }
        if let Some((_, relation)) = crate::actor_relation::body_target(write.target) {
            if crate::actor_relation::cell(&projected, relation) != write.before {
                return Err(ApplyFailure::Condition(write.target));
            }
            let after = write
                .after
                .as_ref()
                .ok_or(ApplyFailure::Type(write.target))?;
            if !crate::actor_relation::patch_cell(&mut projected, relation, after) {
                return Err(ApplyFailure::Type(write.target));
            }
            continue;
        }
        let (_, property) = body_target(write.target).ok_or(ApplyFailure::Type(write.target))?;
        let source = if matches!(
            property,
            LivingProperty::MobSpecies
                | LivingProperty::MobSpeciesProperty(_)
                | LivingProperty::Attribute(_)
                | LivingProperty::PermanentAttributeModifiers(_)
        ) {
            before.ok_or(ApplyFailure::Condition(target))?
        } else {
            &projected
        };
        if facet_cell(source, property) != write.before {
            return Err(ApplyFailure::Condition(write.target));
        }
        let Some(Cell {
            revision,
            value: Value::LivingProperty(value),
        }) = &write.after
        else {
            return Err(ApplyFailure::Type(write.target));
        };
        let living = state_mut(&mut projected.value).ok_or(ApplyFailure::Type(target))?;
        if !property.write(living, value) {
            return Err(ApplyFailure::Type(write.target));
        }
        if *revision == 0 {
            living.property_versions.remove(&property);
        } else {
            living.property_versions.insert(property, *revision);
        }
    }
    projected.revision = projected
        .revision
        .checked_add(1)
        .ok_or(ApplyFailure::Overflow(target))?;
    if let Value::Entity(body) = &mut projected.value
        && let crate::entity_body::EntityPayload::Living(living) = &body.payload
    {
        body.spatial.velocity_micro = living.combat.velocity_micro;
    }
    crate::frame_inventory_projection::project(target, before, Some(projected), writes)
}

pub fn project_facet_writes(
    target: Target,
    before: Option<&Cell>,
    writes: &[AppliedWrite],
) -> Result<Option<Cell>, ApplyFailure> {
    let body = actor_body_target(target).ok_or(ApplyFailure::Type(target))?;
    if let Some(write) = writes.iter().find(|write| write.target == target) {
        if write.before.as_ref() != before {
            return Err(ApplyFailure::Condition(target));
        }
        return Ok(write.after.clone());
    }
    if let Some(write) = writes.iter().find(|write| write.target == body) {
        if write
            .before
            .as_ref()
            .and_then(|cell| actor_facet_cell(cell, target))
            .as_ref()
            != before
        {
            return Err(ApplyFailure::Condition(target));
        }
        return Ok(write
            .after
            .as_ref()
            .and_then(|cell| actor_facet_cell(cell, target)));
    }
    Ok(before.cloned())
}

pub fn project_body_undo(
    target: Target,
    current: Option<&Cell>,
    original: &[AppliedWrite],
) -> Result<Option<Cell>, ApplyFailure> {
    let inverse = original
        .iter()
        .map(|write| AppliedWrite {
            target: write.target,
            before: write.after.clone(),
            after: write.before.clone(),
        })
        .collect::<Vec<_>>();
    let mut restored = project_body_writes(target, current, &inverse)?;
    if !original.iter().any(|write| write.target == target)
        && original
            .iter()
            .any(|write| actor_body_target(write.target) == Some(target))
        && let Some(cell) = &mut restored
    {
        cell.revision = current
            .ok_or(ApplyFailure::Condition(target))?
            .revision
            .checked_sub(1)
            .ok_or(ApplyFailure::Overflow(target))?;
    }
    Ok(restored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::ChunkPart;
    use crate::identity::ServerId;
    use crate::item_action::{ActorCombatState, ActorFoodState, ActorLivingState};
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};

    #[test]
    fn nonliving_passenger_facet_preserves_body_and_reverses_exactly() {
        use crate::actor_relation::ActorRelation;
        use crate::canonical_action::{ActionId, ActionRef, SpatialEpoch};
        use crate::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial};
        use crate::item_contact::ContactIdentity;
        use crate::realm_completion::PeerGeneration;
        use crate::time::TickStamp;
        let id = GlobalEntityId {
            origin: ServerId(1),
            local_id: 3,
            generation: 4,
        };
        let actor = ActorId::Entity(id);
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let reference = ActionRef {
            id: ActionId {
                actor,
                incarnation: 4,
                sequence: 0,
                family: 8,
            },
            tick: TickStamp(6),
        };
        let birth = EntityBirth {
            action: reference,
            source: PeerGeneration {
                server: ServerId(1),
                generation: 4,
            },
            uuid: uuid::Uuid::from_u128(7),
        };
        let original = Cell {
            revision: 8,
            value: Value::Entity((EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 0,
                id,
                birth: birth.clone(),
                owner: birth.source,
                kind: 1,
                relation_versions: Default::default(),
                passengers: Default::default(),
                spatial: EntitySpatial {
                    world: key.world,
                    position: [0; 3],
                    velocity_micro: [0; 3],
                    yaw: 0,
                    pitch: 0,
                    on_ground: false,
                },
                spatial_epoch: SpatialEpoch::Action(reference),
                payload: EntityPayload::Experience(crate::entity_body::ExperienceOrbState {
                    amount: 3,
                    count: 1,
                    health_milli: 5_000,
                    age: 17,
                    last_lifetime_tick: None,
                }),
            }).into()),
        };
        let facet = Target::Chunk {
            key,
            part: ChunkPart::ActorRelation {
                actor,
                relation: ActorRelation::Passenger(2),
            },
            inventory: None,
        };
        let body = actor_body_target(facet).unwrap();
        let before = actor_facet_cell(&original, facet).unwrap();
        let after = Cell {
            revision: 1,
            value: Value::ActorRelation(Some(ContactIdentity::Entity { id, birth })),
        };
        let write = AppliedWrite {
            target: facet,
            before: Some(before),
            after: Some(after.clone()),
        };
        let mounted = project_body_writes(body, Some(&original), &[write.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(actor_facet_cell(&mounted, facet), Some(after));
        let Value::Entity(entity) = &mounted.value else {
            panic!();
        };
        assert_eq!(
            entity.payload,
            EntityPayload::Experience(crate::entity_body::ExperienceOrbState {
                amount: 3,
                count: 1,
                health_milli: 5_000,
                age: 17,
                last_lifetime_tick: None
            })
        );
        assert_eq!(entity.spatial_epoch, SpatialEpoch::Action(reference));
        assert_eq!(mounted.revision, 9);
        assert_eq!(
            project_body_undo(body, Some(&mounted), &[write]).unwrap(),
            Some(original)
        );
    }

    #[test]
    fn independent_facets_compose_and_reverse_without_losing_causal_versions() {
        let actor = ActorId::Entity(GlobalEntityId {
            origin: ServerId(1),
            local_id: 3,
            generation: 4,
        });
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let facet = |property| Target::Chunk {
            key,
            part: ChunkPart::LivingProperty { actor, property },
            inventory: None,
        };
        let health = facet(LivingProperty::Health);
        let fire = facet(LivingProperty::FireTicks);
        let body = body_target(health).unwrap().0;
        let original = Cell {
            revision: 8,
            value: Value::Living((ActorLivingState {
                permanent_attribute_modifiers: Default::default(),
                menu: Default::default(),
                environment: Default::default(),
                tags: Default::default(),
                tags_revision: 0,
                advancements: Default::default(),
                stand: None,
                mob: None,
                merchant: None,
                simulation_tick: None,
                effects: Vec::new(),
                hidden_effects: std::collections::BTreeMap::new(),
                property_versions: [(LivingProperty::Health, 3)].into_iter().collect(),
                food: ActorFoodState {
                    exhaustion_milli: 0,
                    tick_timer: 0,
                    actor,
                    food: 20,
                    saturation_milli: 0,
                    revision: 0,
                },
                combat: ActorCombatState {
                    actor,
                    revision: 0,
                    health_milli: 20_000,
                    absorption_milli: 0,
                    velocity_micro: [0; 3],
                    timing: Default::default(),
                    effective_attributes: Default::default(),
                    equipment: Default::default(),
                    status: Default::default(),
                },
                use_state: None,
                cooldowns: Default::default(),
                avatar: None,
            }).into()),
        };
        let write = |target, property, current: &Cell, value| AppliedWrite {
            target,
            before: facet_cell(current, property),
            after: Some(Cell {
                revision: facet_cell(current, property).unwrap().revision + 1,
                value: Value::LivingProperty(value),
            }),
        };
        let damage = write(
            health,
            LivingProperty::Health,
            &original,
            crate::living_property::LivingPropertyValue::Unsigned(15_000),
        );
        let damaged = project_body_writes(body, Some(&original), &[damage.clone()])
            .unwrap()
            .unwrap();
        let ignition = write(
            fire,
            LivingProperty::FireTicks,
            &damaged,
            crate::living_property::LivingPropertyValue::Unsigned(40),
        );
        let ignited = project_body_writes(body, Some(&damaged), &[ignition.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(ignited.revision, 10);
        assert_eq!(facet_cell(&ignited, LivingProperty::Health), damage.after);
        let restored_damage = project_body_undo(body, Some(&ignited), &[ignition])
            .unwrap()
            .unwrap();
        assert_eq!(restored_damage, damaged);
        assert_eq!(
            project_body_undo(body, Some(&restored_damage), &[damage.clone()]).unwrap(),
            Some(original.clone())
        );
        let mut graphs = crate::action_graph::ActionGraphs::default();
        graphs
            .install_chunk_frontier(
                key,
                crate::action_graph::ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: crate::time::TickStamp(2),
                },
            )
            .unwrap();
        graphs
            .install_ground_anchor(
                body,
                crate::action_graph::GroundAnchor {
                    settled_through: crate::time::TickStamp(6),
                    value: Some(original.clone()),
                },
            )
            .unwrap();
        graphs
            .seed_chunk_target_anchor(health, key, facet_cell(&original, LivingProperty::Health))
            .unwrap();
        assert_eq!(
            graphs.ground_anchors[&health].settled_through,
            crate::time::TickStamp(6)
        );
        let action = crate::canonical_action::Action {
            edit_authority: None,
            id: crate::canonical_action::ActionId {
                actor: ActorId::Peer(ServerId(1)),
                incarnation: 1,
                sequence: 1,
                family: 0,
            },
            tick: crate::time::TickStamp(7),
            class: crate::canonical_action::ActionClass::Undoable,
            chunk_transition: false,
            actor_chunk: None,
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders: vec![health.stakeholder()],
            dependencies: Vec::new(),
            conditions: vec![crate::canonical_action::Condition {
                target: health,
                expected: crate::canonical_action::Expected::Exact(damage.before.clone().unwrap()),
            }],
            effects: vec![crate::canonical_action::Effect::Put {
                target: health,
                value: damage.after.clone().unwrap().value,
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![crate::canonical_action::UndoProcedure::RestoreWrittenTargets],
            kind: crate::canonical_action::ActionKind::World,
            client_origin: crate::canonical_action::ClientOrigin::None,
            presentation: Vec::new(),
        };
        graphs
            .insert_collected(action.tick, [action.clone()])
            .unwrap();
        let mut state = crate::canonical_action::OwnedStakeholderSet::default();
        state.local.insert(health, damage.before.clone().unwrap());
        let result = crate::canonical_action::prepare_action(
            &state,
            &action,
            crate::canonical_action::TruthView::Local,
        )
        .unwrap();
        let node = graphs
            .ticks
            .get_mut(&action.tick)
            .unwrap()
            .actions
            .get_mut(&action.id)
            .unwrap();
        node.factual.result = Some(result);
        node.stable = true;
        node.semantic_order = Some(0);
        graphs
            .seed_chunk_target_anchor(health, key, damage.after.clone())
            .unwrap();
        assert_eq!(graphs.ground_anchors[&health].value, damage.before);
        assert_eq!(
            graphs
                .symbolic_anchor_for(crate::time::TickStamp(8), body)
                .unwrap(),
            Some(damaged.clone())
        );
        assert_eq!(
            graphs
                .symbolic_anchor_for(crate::time::TickStamp(8), health)
                .unwrap(),
            damage.after
        );
        let node = graphs
            .ticks
            .get_mut(&action.tick)
            .unwrap()
            .actions
            .get_mut(&action.id)
            .unwrap();
        node.applied_realms = node.realms.clone();
        let snapshots = graphs.snapshot_for_chunk(key);
        let anchors = graphs.snapshot_anchors_for_graphs(key, &snapshots);
        let projected = crate::action_graph::project_chunk_graph_snapshot_ground(
            key,
            action.tick,
            &snapshots,
            &anchors,
        )
        .unwrap();
        assert_eq!(projected[&body], Some(damaged));
        assert_eq!(projected[&health], damage.after);
        assert!(
            graphs
                .ground_drain_targets(action.tick)
                .unwrap()
                .contains(&body)
        );
    }
}
