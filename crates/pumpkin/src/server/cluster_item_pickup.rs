use std::collections::{HashMap, HashSet};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{Action, ActionState, LivePlayerLease, SpatialEpoch};
use pumpkin_cluster::entity_body::{DroppedItemCondition, EntityPayload};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_contact::{ContactObservation, PickupContact};
use pumpkin_cluster::protocol::GlobalEntityId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

pub(super) fn capture<S: ActionState>(
    owners: &S,
    players: &[(
        LivePlayerLease,
        super::cluster_world_actor::ActorSpatialState,
        pumpkin_cluster::protocol::ChunkKey,
        SpatialEpoch,
    )],
    bodies: &[(
        pumpkin_cluster::protocol::ChunkKey,
        pumpkin_cluster::entity_body::EntityBody,
    )],
    spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
    producer: PeerGeneration,
    tick: TickStamp,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    drafts: &mut HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    issued: &mut HashSet<(GlobalPlayerId, TickStamp, GlobalEntityId)>,
    capacity: usize,
) -> (Vec<Action>, bool) {
    issued.retain(|(_, previous, _)| *previous == tick);
    let mut output = Vec::new();
    for (lease, state, chunk, epoch) in players {
        if lease.gid.server != producer.server || state.game_mode == 3 {
            continue;
        }
        let height = if state.crawling {
            0.6
        } else if state.sneaking {
            1.5
        } else {
            1.8
        };
        let Some(picker) = ContactObservation::player(
            *lease,
            producer,
            *epoch,
            *chunk,
            state.frame.effective,
            0.3,
            height,
            tick,
        ) else {
            continue;
        };
        let key = (ActorId::Player(lease.gid), lease.incarnation, tick);
        let draft = drafts
            .entry(key)
            .or_insert_with(|| ActorActionDraft::new(key.0, key.1, tick, 4096, 16384));
        for (item_chunk, body) in bodies {
            if output.len() == capacity {
                return (output, false);
            }
            if issued.contains(&(lease.gid, tick, body.id)) {
                continue;
            }
            if let EntityPayload::Projectile(projectile) = &body.payload {
                if projectile.embedded.is_none() || projectile.embedded_age < 7
                    || !projectile.pickup_allowed && !projectile.pickup_creative_only { continue; }
                let observed = spatial.project(body);
                let Some(item) = ContactObservation::entity(body, *item_chunk, &observed.spatial, 0.125, 0.25, tick,
                    spatial.get(body.id).map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| sample.sequence)) else { continue; };
                let contact = PickupContact { picker: picker.clone(), item, producer, tick };
                if !contact.valid() { continue; }
                let view = draft.view(owners);
                let Ok((candidates, _, _)) = super::cluster_world_inventory_actions::pickup_destinations(&view, *lease, &projectile.carried_item) else { continue; };
                let destinations = candidates.into_iter().map(|candidate| {
                    let pumpkin_cluster::canonical_action::Target::Player { inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player { inv, slot }), .. } = candidate.target else { unreachable!(); };
                    pumpkin_cluster::canonical_inventory::InventorySlotBinding { target: candidate.target, max_stack: candidate.max_stack,
                        slot: pumpkin_cluster::inventory::InventorySlotRef::new(pumpkin_cluster::inventory::InventoryTarget::Player(lease.gid),
                            pumpkin_cluster::inventory::InvLoc::new(inv, slot)) }
                }).collect();
                let intent = pumpkin_cluster::projectile_collection::ProjectileCollectionIntent { contact, destinations,
                    equipment: pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(*lease, *chunk,
                        view.inventory_equipment_attribute_ids()) };
                let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else { return (output, false); };
                let id = reservation.id(0, 12).expect("reserved projectile pickup identity");
                let Ok(action) = pumpkin_cluster::projectile_collection::capture(&view, id, tick, intent) else {
                    clock.release_unused(reservation); continue;
                };
                if draft.issue(owners, action).is_ok() {
                    issued.insert((lease.gid, tick, body.id)); output.push(draft.actions().last().unwrap().clone());
                } else { clock.release_unused(reservation); }
                continue;
            }
            if let EntityPayload::Experience(orb) = &body.payload {
                if orb.age >= 6000 {
                    continue;
                }
                let observed = spatial.project(body);
                let sequence = spatial
                    .get(body.id)
                    .filter(|sample| {
                        sample.birth == body.birth && sample.epoch == body.spatial_epoch
                    })
                    .map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| {
                        sample.sequence
                    });
                let Some(observation) = ContactObservation::entity(
                    body,
                    *item_chunk,
                    &observed.spatial,
                    0.25,
                    0.5,
                    tick,
                    sequence,
                ) else {
                    continue;
                };
                let contact = PickupContact {
                    picker: picker.clone(),
                    item: observation,
                    producer,
                    tick,
                };
                if !contact.valid() {
                    continue;
                }
                let view = draft.view(owners);
                let property = |property| pumpkin_cluster::canonical_action::Target::Chunk {
                    key: *chunk,
                    part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                        actor: key.0,
                        property,
                    },
                    inventory: None,
                };
                let intent = pumpkin_cluster::experience_orb::ExperienceCollection {
                    contact,
                    amount: orb.amount,
                    experience: property(
                        pumpkin_cluster::living_property::LivingProperty::Experience,
                    ),
                    cooldowns: property(
                        pumpkin_cluster::living_property::LivingProperty::Cooldowns,
                    ),
                    equipment: pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                        *lease, *chunk, view.inventory_equipment_attribute_ids(),
                    ),
                };
                let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else {
                    return (output, false);
                };
                let Some(id) = reservation.id(0, 12) else {
                    return (output, false);
                };
                let Ok(action) =
                    pumpkin_cluster::experience_orb::capture_collection(&view, id, tick, intent)
                else {
                    clock.release_unused(reservation);
                    continue;
                };
                if draft.issue(owners, action).is_ok() {
                    issued.insert((lease.gid, tick, body.id));
                    output.push(draft.actions().last().unwrap().clone());
                } else {
                    clock.release_unused(reservation);
                }
                continue;
            }
            let EntityPayload::Item(item) = &body.payload else {
                continue;
            };
            if !item.matches(DroppedItemCondition::PickupReady) {
                continue;
            }
            let observed = spatial.project(body);
            let sequence = spatial
                .get(body.id)
                .filter(|sample| sample.birth == body.birth && sample.epoch == body.spatial_epoch)
                .map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| {
                    sample.sequence
                });
            let Some(item_observation) = ContactObservation::entity(
                body,
                *item_chunk,
                &observed.spatial,
                0.125,
                0.25,
                tick,
                sequence,
            ) else {
                continue;
            };
            let contact = PickupContact {
                picker: picker.clone(),
                item: item_observation,
                producer,
                tick,
            };
            if !contact.valid() {
                continue;
            }
            let view = draft.view(owners);
            let source_cell = pumpkin_cluster::inventory::InventoryCell {
                loc: pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::inventory::INV_CURSOR,
                    0,
                ),
                item: item.item,
                count: item.count,
                nbt: item.components.clone(),
            };
            let Ok((destinations, policy, selector)) =
                super::cluster_world_inventory_actions::pickup_destinations(
                    &view,
                    *lease,
                    &source_cell,
                )
            else {
                continue;
            };
            let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else {
                return (output, false);
            };
            let Some(id) = reservation.id(0, 12) else {
                return (output, false);
            };
            let equipment = pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                *lease,
                *chunk,
                view.inventory_equipment_attribute_ids(),
            );
            let Ok(action) = pumpkin_cluster::item_lifecycle::capture_pickup(
                &view,
                id,
                contact,
                destinations,
                0,
                policy,
                Some(selector),
                vec![equipment],
            ) else {
                clock.release_unused(reservation);
                continue;
            };
            if draft.issue(owners, action).is_ok() {
                issued.insert((lease.gid, tick, body.id));
                output.push(draft.actions().last().unwrap().clone());
            } else {
                clock.release_unused(reservation);
            }
        }
    }
    (output, true)
}
