use super::super::cluster_world_callback_snapshot::CallbackTickSnapshot;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload, EntitySpatial, VehicleKind};
use pumpkin_cluster::entity_origin::EntityOriginAllocator;
use pumpkin_cluster::inventory::{InvLoc, InventoryCell, InventorySlotRef, InventoryTarget};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::vehicle_terminal::{VehicleCargoDrop, VehicleTerminalIntent};
use std::sync::Arc;

pub(super) struct FrozenVehicleTerminal {
    body: EntityBody,
    spatial: EntitySpatial,
    chunk: ChunkKey,
    tick: TickStamp,
    rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    do_entity_drops: bool,
    cargo: Vec<(InventorySlotBinding, InventoryCell)>,
    detach: pumpkin_cluster::actor_relation::OwnerDetachPlan,
}

impl FrozenVehicleTerminal {
    pub(super) fn capture(
        snapshot: &CallbackTickSnapshot,
        draft: Option<&ActorActionDraft>,
        projected: &EntityBody,
        chunk: ChunkKey,
        tick: TickStamp,
    ) -> Result<Option<Self>, ApplyFailure> {
        let empty = ActorActionDraft::new(
            pumpkin_cluster::actor::ActorId::Entity(projected.id),
            projected.id.generation,
            tick,
            4096,
            16384,
        );
        let view = draft.unwrap_or(&empty).view(snapshot);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(projected.id),
            inventory: None,
        };
        let Value::Entity(body) = view
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?
            .value
        else {
            return Err(ApplyFailure::Type(target));
        };
        let body = *body;
        let EntityPayload::Vehicle(vehicle) = &body.payload else {
            return Ok(None);
        };
        if vehicle.damage_milli <= 40_000 {
            return Ok(None);
        }
        let (rules, policy) = snapshot
            .world_rules_at(chunk.world, tick)
            .ok_or(ApplyFailure::Capacity)?;
        let do_entity_drops = policy.do_entity_drops;
        let count = match vehicle.kind {
            VehicleKind::ChestBoat | VehicleKind::ChestMinecart => 27,
            VehicleKind::HopperMinecart => 5,
            _ => 0,
        };
        let mut cargo = Vec::with_capacity(count);
        for slot in 0..count {
            let loc = InvLoc::new(0, slot as u16);
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: Some(InventoryId::Entity {
                    inv: 0,
                    slot: slot as u16,
                }),
            };
            let Value::Inventory(before) = view
                .read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?
                .value
            else {
                return Err(ApplyFailure::Type(target));
            };
            cargo.push((
                InventorySlotBinding {
                    slot: InventorySlotRef::new(InventoryTarget::Entity(body.id.into()), loc),
                    target,
                    max_stack: 99,
                },
                before,
            ));
        }
        let detach = capture_detach(snapshot, &view, &body, chunk, tick)?;
        Ok(Some(Self {
            body,
            spatial: projected.spatial.clone(),
            chunk,
            tick,
            rules,
            do_entity_drops,
            cargo,
            detach,
        }))
    }

    pub(super) fn materialize(
        &self,
        snapshot: &mut Arc<CallbackTickSnapshot>,
        draft: &ActorActionDraft,
        id: ActionId,
        allocator: Option<&mut EntityOriginAllocator>,
    ) -> Result<Action, ApplyFailure> {
        let EntityPayload::Vehicle(vehicle) = &self.body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        let drops = self.do_entity_drops && vehicle.drop_loot;
        let count = if drops {
            1 + self
                .cargo
                .iter()
                .filter(|(_, cell)| !cell.is_empty())
                .count()
        } else {
            0
        };
        let reference = ActionRef {
            tick: self.tick,
            id,
        };
        let primary = snapshot.scheduled_executor(self.chunk)?;
        let mut templates = Vec::with_capacity(count);
        if count != 0 {
            let allocator = allocator.ok_or(ApplyFailure::Capacity)?;
            let identities = allocator
                .reserve(self.body.owner, reference, count)
                .map_err(|_| ApplyFailure::Capacity)?;
            let base = snapshot.vehicle_drop_item(&self.body)?;
            let cells = std::iter::once(&base).chain(
                self.cargo
                    .iter()
                    .map(|(_, cell)| cell)
                    .filter(|cell| !cell.is_empty()),
            );
            for (identity, cell) in identities.iter().zip(cells) {
                templates.push(
                    super::super::cluster_item_birth::reserved_item_body(
                        reference,
                        identity,
                        primary,
                        self.spatial.clone(),
                        cell,
                        40,
                    )
                    .map_err(|_| ApplyFailure::InvalidAction)?,
                );
            }
            let planned = templates
                .iter()
                .map(|body| (self.chunk, body.id))
                .collect::<Vec<_>>();
            *snapshot = snapshot.clone().with_reserved_births(
                allocator,
                reference,
                self.body.owner,
                &planned,
            )?;
        }
        let mut templates = templates.into_iter();
        let vehicle_drop = if drops { templates.next() } else { None };
        let cargo = self
            .cargo
            .iter()
            .map(|(binding, before)| VehicleCargoDrop {
                binding: binding.clone(),
                before: before.clone(),
                drop: if drops && !before.is_empty() {
                    templates.next()
                } else {
                    None
                },
            })
            .collect();
        let intent = VehicleTerminalIntent {
            chunk: self.chunk,
            rules: self.rules,
            do_entity_drops: self.do_entity_drops,
            body: self.body.clone(),
            cargo,
            vehicle_drop,
            detach: self.detach.clone(),
        };
        intent.capture(&draft.view(snapshot.as_ref()), id, self.tick)
    }
}

pub(super) fn capture_detach(
    snapshot: &CallbackTickSnapshot,
    state: &impl ActionState,
    body: &EntityBody,
    chunk: ChunkKey,
    tick: TickStamp,
) -> Result<pumpkin_cluster::actor_relation::OwnerDetachPlan, ApplyFailure> {
    let mut participants =
        Vec::with_capacity(body.passengers.len() + usize::from(body.mount.is_some()));
    for identity in body.mount.iter().chain(body.passengers.values()) {
        let participant = match identity {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => snapshot
                .player_relation_participant(*lease)
                .or_else(|| snapshot.historical_player_contact(*lease).filter(|contact| contact.simulation_tick == tick)
                    .and_then(pumpkin_cluster::actor_relation::ActorRelationParticipant::from_contact))
                .ok_or(ApplyFailure::Capacity)?,
            pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } => {
                let association = snapshot
                    .original_store()
                    .entity_body_chunk(false, *id)
                    .ok_or(ApplyFailure::Capacity)?;
                let target = Target::Chunk {
                    key: association,
                    part: ChunkPart::Entity(*id),
                    inventory: None,
                };
                let Some(Cell {
                    value: Value::Entity(entity),
                    ..
                }) = state.read(TruthView::Local, target)
                else {
                    return Err(ApplyFailure::Condition(target));
                };
                if &entity.birth != birth {
                    return Err(ApplyFailure::Condition(target));
                }
                pumpkin_cluster::actor_relation::ActorRelationParticipant::from_entity(
                    &entity,
                    association,
                )
                .ok_or(ApplyFailure::InvalidAction)?
            }
        };
        participants.push(participant);
    }
    pumpkin_cluster::actor_relation::capture_owner_detach_from_participants(
        state,
        body,
        chunk,
        &participants,
    )
    .ok_or(ApplyFailure::Capacity)
}
