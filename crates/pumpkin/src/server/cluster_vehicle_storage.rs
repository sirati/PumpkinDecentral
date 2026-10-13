use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::canonical_inventory::{InventoryActionCapture, InventorySlotBinding};
use pumpkin_cluster::entity_body::{EntityPayload, VehicleKind};
use pumpkin_cluster::inventory::{
    InvLoc, InventorySlotRef, InventoryStateSnapshot, InventoryTarget,
};
use pumpkin_cluster::item_action::{ActorItemInteraction, ItemActionKind};
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::time::TickStamp;

pub(super) struct FrozenVehicleStorage {
    lease: LivePlayerLease,
    viewer: ContactObservation,
    entity: ContactObservation,
    kind: u16,
    slots: u16,
    full_source_slot: u64,
    viewer_inventory: InventoryStateSnapshot,
    entity_inventory: InventoryStateSnapshot,
    bindings: Vec<InventorySlotBinding>,
}

impl FrozenVehicleStorage {
    pub(super) fn capture(
        state: &impl ActionState,
        proposal: &ActionProposal,
        metadata: &super::cluster_item_use::ItemUseSourceMetadata,
    ) -> Result<Option<Self>, ApplyFailure> {
        let ClaimedAction::ItemUse(claim) = &proposal.claim else {
            return Ok(None);
        };
        if !matches!(
            claim.intent.kind,
            ItemActionKind::UseOnActor {
                interaction: ActorItemInteraction::Interact,
                ..
            }
        ) {
            return Ok(None);
        }
        let entity = claim
            .target_observation
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?;
        let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = state.read(TruthView::Local, entity.target())
        else {
            return Ok(None);
        };
        let EntityPayload::Vehicle(vehicle) = &body.payload else {
            return Ok(None);
        };
        let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
        let viewer = metadata
            .source_observation
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?;
        let Some(Cell {
            value: Value::Living(living),
            ..
        }) = state.read(TruthView::Local, viewer.target())
        else {
            return Err(ApplyFailure::Condition(viewer.target()));
        };
        let avatar = living.avatar.as_ref().ok_or(ApplyFailure::InvalidAction)?;
        let slots = match vehicle.kind {
            VehicleKind::ChestMinecart => 27,
            VehicleKind::HopperMinecart => 5,
            VehicleKind::ChestBoat if avatar.stance.sneaking || !body.passengers.is_empty() => 27,
            _ => return Ok(None),
        };
        if vehicle.damage_milli > 40_000 || avatar.game_mode == 3 || living.combat.health_milli == 0
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut bindings = Vec::new();
        let mut viewer_cells = Vec::new();
        for loc in pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots() {
            let target = Target::Player {
                key: lease.key,
                lease: Some(lease),
                inventory: Some(InventoryId::Player {
                    inv: loc.inv,
                    slot: loc.slot,
                }),
            };
            let Some(Cell {
                value: Value::Inventory(cell),
                ..
            }) = state.read(TruthView::Local, target)
            else {
                return Err(ApplyFailure::Inventory(target));
            };
            bindings.push(InventorySlotBinding {
                slot: InventorySlotRef::new(
                    InventoryTarget::LeasedPlayer {
                        key: lease.key.0,
                        player: lease.gid,
                        incarnation: lease.incarnation,
                    },
                    loc,
                ),
                target,
                max_stack: if loc.inv == pumpkin_cluster::inventory::INV_ARMOR
                    || loc.inv == pumpkin_cluster::inventory::INV_SELECTED
                {
                    1
                } else {
                    99
                },
            });
            viewer_cells.push(cell);
        }
        let mut entity_cells = Vec::new();
        for slot in 0..slots {
            let loc = InvLoc::new(0, slot);
            let target = Target::Chunk {
                key: entity.chunk,
                part: ChunkPart::Entity(body.id),
                inventory: Some(InventoryId::Entity { inv: 0, slot }),
            };
            let Some(Cell {
                value: Value::Inventory(cell),
                ..
            }) = state.read(TruthView::Local, target)
            else {
                return Err(ApplyFailure::Inventory(target));
            };
            bindings.push(InventorySlotBinding {
                slot: InventorySlotRef::new(InventoryTarget::Entity(body.id.into()), loc),
                target,
                max_stack: 99,
            });
            entity_cells.push(cell);
        }
        Ok(Some(Self {
            lease,
            viewer: viewer.clone(),
            entity: entity.clone(),
            kind: body.kind,
            slots,
            full_source_slot: state
                .action_source_slot(ActionRef {
                    id: proposal.id,
                    tick: proposal.tick,
                })
                .ok_or(ApplyFailure::Capacity)?,
            viewer_inventory: InventoryStateSnapshot {
                target: InventoryTarget::LeasedPlayer {
                    key: lease.key.0,
                    player: lease.gid,
                    incarnation: lease.incarnation,
                },
                revision: 0,
                cells: viewer_cells,
            },
            entity_inventory: InventoryStateSnapshot {
                target: InventoryTarget::Entity(body.id.into()),
                revision: 0,
                cells: entity_cells,
            },
            bindings,
        }))
    }

    pub(super) fn bindings(&self) -> &[InventorySlotBinding] {
        &self.bindings
    }

    pub(super) fn targets(&self) -> Vec<Target> {
        let mut targets = vec![self.viewer.target(), self.entity.target()];
        targets.extend(self.bindings.iter().map(|binding| binding.target));
        for property in [
            pumpkin_cluster::living_property::LivingProperty::Health,
            pumpkin_cluster::living_property::LivingProperty::GameMode,
        ] {
            targets.push(Target::Chunk {
                key: self.viewer.chunk,
                part: ChunkPart::LivingProperty {
                    actor: pumpkin_cluster::actor::ActorId::Player(self.lease.gid),
                    property,
                },
                inventory: None,
            });
        }
        targets.push(Target::Chunk {
            key: self.viewer.chunk,
            part: ChunkPart::LivingProperty {
                actor: pumpkin_cluster::actor::ActorId::Player(self.lease.gid),
                property: pumpkin_cluster::living_property::LivingProperty::MenuSession,
            },
            inventory: None,
        });
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    fn screen(
        &self,
        reference: ActionRef,
    ) -> Result<
        (
            pumpkin_cluster::inventory::InventoryScreenSnapshot,
            super::cluster_invsee::ClusterScreenPresentation,
            pumpkin_cluster::canonical_menu::MenuSessionIntent,
        ),
        ApplyFailure,
    > {
        let identity =
            pumpkin_cluster::canonical_menu_identity::MenuPresentationIdentity::from_source(
                reference,
                self.full_source_slot,
            )
            .ok_or(ApplyFailure::InvalidAction)?;
        super::cluster_inventory_menu::entity_storage_menu_screen(
            self.lease,
            identity.session,
            identity.sync_id,
            self.viewer.clone(),
            self.entity.clone(),
            self.kind,
            &self.viewer_inventory,
            &self.entity_inventory,
            self.slots,
            pumpkin_util::text::TextComponent::text(if self.slots == 5 {
                "Hopper Minecart"
            } else {
                "Chest"
            }),
        )
        .map_err(|_| ApplyFailure::InvalidAction)
    }

    pub(super) fn materialize(
        &self,
        state: &impl ActionState,
        id: ActionId,
        tick: TickStamp,
        client_origin: ClientOrigin,
    ) -> Result<Action, ApplyFailure> {
        let (_, _, intent) = self.screen(ActionRef { id, tick })?;
        let mut action = intent.capture_action(
            state,
            InventoryActionCapture {
                edit_authority: None,
                id,
                tick,
                actor_chunk: Some(self.viewer.chunk),
                player_leases: vec![self.lease],
                player_chunks: vec![(self.lease.key, self.viewer.chunk)],
                guards: Vec::new(),
                client_origin,
            },
        )?;
        for property in [
            pumpkin_cluster::living_property::LivingProperty::Health,
            pumpkin_cluster::living_property::LivingProperty::GameMode,
        ] {
            let target = Target::Chunk {
                key: self.viewer.chunk,
                part: ChunkPart::LivingProperty {
                    actor: pumpkin_cluster::actor::ActorId::Player(self.lease.gid),
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
            action.counterfactual_conditions.push(target);
        }
        action.validate().map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(action)
    }

    pub(super) fn presentation(
        &self,
        reference: ActionRef,
    ) -> Result<super::cluster_invsee::CanonicalInventoryOpenPresentation, ApplyFailure> {
        let (screen, presentation, intent) = self.screen(reference)?;
        let pumpkin_cluster::canonical_menu::MenuSessionOperation::Open { state, .. } =
            intent.operation
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        Ok(super::cluster_invsee::CanonicalInventoryOpenPresentation {
            reference,
            lease: self.lease,
            screen,
            source: state.source,
            menu: None,
            merchant: None,
            presentation: Some(presentation),
        })
    }
}

impl FrozenVehicleStorage {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        super::cluster_inventory_menu::inventory_metadata_resource_usage(&(
            self.lease,
            &self.viewer,
            &self.entity,
            self.kind,
            self.slots,
            self.full_source_slot,
            &self.viewer_inventory,
            &self.entity_inventory,
            &self.bindings,
        ))
    }
}
