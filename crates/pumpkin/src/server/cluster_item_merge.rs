use std::collections::{BTreeMap, HashMap, HashSet};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::{ActorActionDraft, FrozenEntityIssueAuthority};
use pumpkin_cluster::canonical_action::{Action, ActionState};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::item_contact::{ContactObservation, MergeContact};
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

pub(super) fn capture<S: ActionState>(
    owners: &S,
    bodies: &[(ChunkKey, EntityBody)],
    spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
    producer: PeerGeneration,
    tick: TickStamp,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    drafts: &mut HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    issued: &mut HashSet<(TickStamp, GlobalEntityId, GlobalEntityId)>,
    capacity: usize,
    selected: impl Fn(ChunkKey, &EntityBody) -> bool,
) -> (Vec<Action>, bool) {
    let mut grid = BTreeMap::<_, Vec<usize>>::new();
    for (index, (chunk, body)) in bodies.iter().enumerate() {
        if !matches!(&body.payload, EntityPayload::Item(item) if item.matches(pumpkin_cluster::entity_body::DroppedItemCondition::Alive))
            && !matches!(&body.payload, EntityPayload::Experience(orb) if orb.age < 6000 && orb.count > 0 && orb.health_milli > 0)
        {
            continue;
        }
        let projected = spatial.project(body);
        let [x, y, z] = projected
            .spatial
            .position
            .map(f64::from_bits)
            .map(|coordinate| coordinate.floor() as i64);
        grid.entry((chunk.world, x, y, z)).or_default().push(index);
    }
    issued.retain(|(previous, _, _)| *previous == tick);
    let mut output = Vec::new();
    for (world_position, indices) in &grid {
        let (world, x, y, z) = *world_position;
        for index in indices {
            for dx in -1..=1 {
                for dy in -1..=1 {
                    for dz in -1..=1 {
                        let Some(neighbors) = grid.get(&(world, x + dx, y + dy, z + dz)) else {
                            continue;
                        };
                        for other in neighbors {
                            if index >= other {
                                continue;
                            }
                            let (left_chunk, left) = &bodies[*index];
                            let (right_chunk, right) = &bodies[*other];
                            if let (EntityPayload::Experience(a), EntityPayload::Experience(b)) =
                                (&left.payload, &right.payload)
                            {
                                if a.amount != b.amount
                                    || left.owner != producer
                                    || !selected(*left_chunk, left)
                                    || issued.contains(&(tick, left.id, right.id))
                                {
                                    continue;
                                }
                                if output.len() == capacity {
                                    return (output, false);
                                }
                                let observe = |body: &EntityBody, chunk| {
                                    let projected = spatial.project(body);
                                    let sequence = spatial
                                        .get(body.id)
                                        .filter(|sample| {
                                            sample.birth == body.birth
                                                && sample.epoch == body.spatial_epoch
                                        })
                                        .map_or(
                                            pumpkin_cluster::identity::PlayerSeq(0),
                                            |sample| sample.sequence,
                                        );
                                    ContactObservation::entity(
                                        body,
                                        chunk,
                                        &projected.spatial,
                                        0.25,
                                        0.5,
                                        tick,
                                        sequence,
                                    )
                                };
                                let (Some(source), Some(destination)) =
                                    (observe(left, *left_chunk), observe(right, *right_chunk))
                                else {
                                    continue;
                                };
                                let contact = MergeContact {
                                    source,
                                    destination,
                                    producer,
                                    tick,
                                };
                                if !contact.valid() {
                                    continue;
                                }
                                let key = (ActorId::Entity(left.id), left.id.generation, tick);
                                let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else { return (output, false); };
                                let Some(id) = reservation.id(0, 13) else { return (output, false); };
                                if !drafts.contains_key(&key) {
                                    let authority = FrozenEntityIssueAuthority {
                                        target: contact.source.target(),
                                        birth: left.birth.clone(),
                                        producer,
                                        controlled_owner: left.owner,
                                        effective_tick: tick,
                                        next_sequence: id.sequence,
                                    };
                                    let Ok(draft) = ActorActionDraft::new_entity(
                                        owners, authority, tick, 4096, 16384,
                                    ) else {
                                        clock.release_unused(reservation);
                                        continue;
                                    };
                                    drafts.insert(key, draft);
                                }
                                let draft = drafts.get_mut(&key).unwrap();
                                let result = pumpkin_cluster::experience_orb::capture_merge(
                                    &draft.view(owners),
                                    id,
                                    pumpkin_cluster::experience_orb::ExperienceMerge {
                                        contact,
                                        amount: a.amount,
                                    },
                                );
                                let Ok(action) = result else {
                                    clock.release_unused(reservation);
                                    continue;
                                };
                                if draft.issue(owners, action).is_ok() {
                                    issued.insert((tick, left.id, right.id));
                                    output.push(draft.actions().last().unwrap().clone());
                                } else {
                                    clock.release_unused(reservation);
                                }
                                continue;
                            }
                            let (EntityPayload::Item(a), EntityPayload::Item(b)) =
                                (&left.payload, &right.payload)
                            else {
                                continue;
                            };
                            if a.item != b.item || a.components != b.components || a.never_despawn || b.never_despawn {
                                continue;
                            }
                            let (source_chunk, source, destination_chunk, destination) =
                                if (a.count, left.id) <= (b.count, right.id) {
                                    (*left_chunk, left, *right_chunk, right)
                                } else {
                                    (*right_chunk, right, *left_chunk, left)
                                };
                            if !selected(source_chunk, source)
                                || issued.contains(&(tick, source.id, destination.id))
                            {
                                continue;
                            }
                            if output.len() == capacity {
                                return (output, false);
                            }
                            let observe = |chunk, body: &EntityBody| {
                                let projected = spatial.project(body);
                                let sequence = spatial
                                    .get(body.id)
                                    .filter(|sample| {
                                        sample.birth == body.birth
                                            && sample.epoch == body.spatial_epoch
                                    })
                                    .map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| {
                                        sample.sequence
                                    });
                                ContactObservation::entity(
                                    body,
                                    chunk,
                                    &projected.spatial,
                                    0.125,
                                    0.25,
                                    tick,
                                    sequence,
                                )
                            };
                            let (Some(source_contact), Some(destination_contact)) = (
                                observe(source_chunk, source),
                                observe(destination_chunk, destination),
                            ) else {
                                continue;
                            };
                            let contact = MergeContact {
                                source: source_contact,
                                destination: destination_contact,
                                producer,
                                tick,
                            };
                            if !contact.valid() {
                                continue;
                            }
                            let source_cell = pumpkin_cluster::inventory::InventoryCell {
                                loc: pumpkin_cluster::inventory::InvLoc::new(
                                    pumpkin_cluster::inventory::INV_CURSOR,
                                    0,
                                ),
                                item: a.item,
                                count: 1,
                                nbt: a.components.clone(),
                            };
                            let Ok(stack) =
                                super::cluster_invsee::item_stack_from_inventory_cell(&source_cell)
                            else {
                                continue;
                            };
                            let key = (
                                ActorId::Entity(source.id),
                                u64::from(source.id.generation),
                                tick,
                            );
                            let Some(reservation) = clock.reserve(key.0, key.1, tick, 1) else { return (output, false); };
                            let Some(id) = reservation.id(0, 13) else { return (output, false); };
                            if !drafts.contains_key(&key) {
                                let authority = FrozenEntityIssueAuthority {
                                    target: contact.source.target(),
                                    birth: source.birth.clone(),
                                    producer,
                                    controlled_owner: source.owner,
                                    effective_tick: tick,
                                    next_sequence: id.sequence,
                                };
                                let Ok(draft) = ActorActionDraft::new_entity(
                                    owners, authority, tick, 4096, 16384,
                                ) else {
                                    clock.release_unused(reservation);
                                    continue;
                                };
                                drafts.insert(key, draft);
                            }
                            let draft = drafts.get_mut(&key).unwrap();
                            let Ok(action) = pumpkin_cluster::item_lifecycle::capture_merge(
                                &draft.view(owners),
                                id,
                                contact,
                                stack.get_max_stack_size(),
                            ) else {
                                continue;
                            };
                            if draft.issue(owners, action).is_ok() {
                                issued.insert((tick, source.id, destination.id));
                                output.push(draft.actions().last().unwrap().clone());
                            }
                        }
                    }
                }
            }
        }
    }
    (output, true)
}
