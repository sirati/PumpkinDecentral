use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_relation::ActorRelation;
use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::entity_body::{EntityPayload, VehicleKind};
use pumpkin_cluster::item_action::{ActorItemInteraction, ItemActionKind};
use pumpkin_cluster::item_contact::ContactIdentity;
use pumpkin_cluster::time::TickStamp;

pub(super) fn materialize(
    state: &impl ActionState,
    proposal: &ActionProposal,
    tick: TickStamp,
    captured: &super::cluster_item_use::CapturedItemUseSource,
) -> Result<Option<Action>, ApplyFailure> {
    let ClaimedAction::ItemUse(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let ItemActionKind::UseOnActor {
        target,
        target_chunk,
        interaction: ActorItemInteraction::Interact,
        ..
    } = claim.intent.kind
    else {
        return Ok(None);
    };
    let observation = claim
        .target_observation
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    if observation.actor() != target
        || observation.chunk != target_chunk
        || observation.simulation_tick.is_after(tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let Some(target_cell) = state.read(TruthView::Local, observation.target()) else {
        return Ok(None);
    };
    let Value::Entity(body) = &target_cell.value else {
        return Ok(None);
    };
    let horse = matches!(&body.payload, EntityPayload::Living(living)
        if living.mob.as_ref().is_some_and(|mob| matches!(mob.species, pumpkin_cluster::mob_continuation::MobSpecies::Horse { .. })));
    let mut saddle_guard = None;
    let capacity = match &body.payload {
        EntityPayload::Vehicle(vehicle) => match vehicle.kind {
            VehicleKind::Boat => 2,
            VehicleKind::ChestBoat | VehicleKind::Minecart => 1,
            _ => return Ok(None),
        },
        EntityPayload::Living(_) if horse => {
            if body.kind != pumpkin_data::entity::EntityType::HORSE.id {
                return Ok(None);
            }
            let Value::Inventory(item) = &captured.2.value else {
                return Err(ApplyFailure::Inventory(captured.1.target));
            };
            let saddle_target = Target::Chunk {
                key: target_chunk,
                part: ChunkPart::Entity(body.id),
                inventory: Some(InventoryId::Entity {
                    inv: pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT,
                    slot: 7,
                }),
            };
            let saddle = state
                .read(TruthView::Local, saddle_target)
                .ok_or(ApplyFailure::Inventory(saddle_target))?;
            let Value::Inventory(saddle_item) = &saddle.value else {
                return Err(ApplyFailure::Inventory(saddle_target));
            };
            if !super::cluster_owned_mob_initial::horse_mount_eligible(body, item, saddle_item)? {
                return Ok(None);
            }
            saddle_guard = Some(Condition {
                target: saddle_target,
                expected: Expected::Exact(saddle),
            });
            1
        }
        _ => return Ok(None),
    };
    let (lease, _, _, _, metadata) = captured;
    let source = metadata
        .source_observation
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    let ContactIdentity::Entity { id, birth } = &observation.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    let player_target = Target::Chunk {
        key: source.chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let Some(Cell {
        value: Value::Living(living),
        ..
    }) = state.read(TruthView::Local, player_target)
    else {
        return Err(ApplyFailure::Condition(player_target));
    };
    let avatar = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(player_target))?;
    if source.identity != ContactIdentity::Player(*lease)
        || avatar.game_mode == 3
        || living.combat.health_milli == 0
        || avatar.mount.is_some()
        || body.id != *id
        || body.birth != *birth
        || body.owner != observation.owner
        || body.spatial_epoch != observation.epoch
        || avatar.spatial_epoch != source.epoch
        || matches!(&body.payload, EntityPayload::Vehicle(vehicle) if vehicle.damage_milli > 40_000 || vehicle.underwater_ticks >= 60)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    if avatar.stance.sneaking {
        return Ok(None);
    }
    let attribute = u16::from(pumpkin_data::attributes::Attributes::ENTITY_INTERACTION_RANGE.id);
    let base = living
        .combat
        .effective_attributes
        .get(&attribute)
        .copied()
        .map(f64::from_bits)
        .ok_or(ApplyFailure::InvalidAction)?;
    let reach = living.attribute(attribute, base);
    let shape = state.actor_collision_shape(observation.target(), &target_cell.value)?;
    if !shape.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let position = observation.position.map(f64::from_bits);
    let mut eye = source.position.map(f64::from_bits);
    eye[1] += claim.eye_height;
    let minimum = std::array::from_fn::<_, 3, _>(|axis| {
        position[axis] + f64::from_bits(shape.offsets[0][axis])
    });
    let maximum = std::array::from_fn::<_, 3, _>(|axis| {
        position[axis] + f64::from_bits(shape.offsets[1][axis])
    });
    let distance = (0..3)
        .map(|axis| (eye[axis] - eye[axis].clamp(minimum[axis], maximum[axis])).powi(2))
        .sum::<f64>();
    if !reach.is_finite() || reach < 0.0 || distance > reach * reach {
        return Err(ApplyFailure::InvalidAction);
    }
    let seat = (0..capacity)
        .find(|seat| !body.passengers.contains_key(seat))
        .ok_or(ApplyFailure::InvalidAction)?;
    let mount = Target::Chunk {
        key: source.chunk,
        part: ChunkPart::ActorRelation {
            actor: ActorId::Player(lease.gid),
            relation: ActorRelation::Mount,
        },
        inventory: None,
    };
    let passenger = Target::Chunk {
        key: target_chunk,
        part: ChunkPart::ActorRelation {
            actor: ActorId::Entity(body.id),
            relation: ActorRelation::Passenger(seat),
        },
        inventory: None,
    };
    let before_mount = state
        .read(TruthView::Local, mount)
        .ok_or(ApplyFailure::Condition(mount))?;
    let before_passenger = state
        .read(TruthView::Local, passenger)
        .ok_or(ApplyFailure::Condition(passenger))?;
    let mut action = pumpkin_cluster::actor_relation::action(
        proposal.id,
        tick,
        source,
        observation,
        seat,
        true,
        before_mount,
        before_passenger,
    )
    .ok_or(ApplyFailure::InvalidAction)?;
    for property in [
        pumpkin_cluster::living_property::LivingProperty::Health,
        pumpkin_cluster::living_property::LivingProperty::GameMode,
        pumpkin_cluster::living_property::LivingProperty::PlayerStance,
    ] {
        let target = Target::Chunk {
            key: source.chunk,
            part: ChunkPart::LivingProperty {
                actor: ActorId::Player(lease.gid),
                property,
            },
            inventory: None,
        };
        let expected = if property == pumpkin_cluster::living_property::LivingProperty::Health {
            Expected::LivingUnsignedAtLeast(1)
        } else {
            Expected::Exact(
                state
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?,
            )
        };
        action.conditions.push(Condition { target, expected });
    }
    if horse {
        use pumpkin_cluster::living_property::LivingProperty as L;
        for property in [
            L::Health,
            L::MobReproduction,
            L::MobSpeciesProperty(
                pumpkin_cluster::mob_species_property::MobSpeciesProperty::HorseFlags,
            ),
        ] {
            let target = Target::Chunk {
                key: target_chunk,
                part: ChunkPart::LivingProperty {
                    actor: target,
                    property,
                },
                inventory: None,
            };
            action.conditions.push(Condition {
                target,
                expected: Expected::Exact(
                    state
                        .read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?,
                ),
            });
        }
        for seat in 0..32 {
            let target = Target::Chunk {
                key: target_chunk,
                part: ChunkPart::ActorRelation {
                    actor: ActorId::Entity(body.id),
                    relation: ActorRelation::Passenger(seat),
                },
                inventory: None,
            };
            let before = state
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            if before.value != Value::ActorRelation(None) {
                return Err(ApplyFailure::Condition(target));
            }
            if !action
                .conditions
                .iter()
                .any(|condition| condition.target == target)
            {
                action.conditions.push(Condition {
                    target,
                    expected: Expected::Exact(before),
                });
            }
        }
        action.conditions.extend([
            Condition {
                target: captured.1.target,
                expected: Expected::Exact(captured.2.clone()),
            },
            captured.3.clone(),
        ]);
        action.conditions.extend(saddle_guard);
        for property in [
            L::Attribute(attribute),
            L::PermanentAttributeModifiers(attribute),
        ] {
            let target = Target::Chunk {
                key: source.chunk,
                part: ChunkPart::LivingProperty {
                    actor: ActorId::Player(lease.gid),
                    property,
                },
                inventory: None,
            };
            action.conditions.push(Condition {
                target,
                expected: Expected::Exact(
                    state
                        .read(TruthView::Local, target)
                        .ok_or(ApplyFailure::Condition(target))?,
                ),
            });
        }
    }
    action.stakeholders.extend(
        action
            .conditions
            .iter()
            .map(|condition| condition.target.stakeholder()),
    );
    action.stakeholders.sort_unstable();
    action.stakeholders.dedup();
    action.client_origin = proposal.client_origin;
    action.validate().map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(Some(action))
}
