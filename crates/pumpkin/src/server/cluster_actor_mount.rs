use pumpkin_cluster::actor_mount::{ActorMount, MountObservation};
use pumpkin_cluster::canonical_action::*;
use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};

pub(super) struct FrozenActorMount {
    command: ActorMount,
    source: super::cluster_item_use::ItemUseSourceMetadata,
    targets: Box<[Target]>,
}

fn observation(contact: &ContactObservation) -> MountObservation {
    use pumpkin_cluster::action_command::ActorReference;
    let actor = match &contact.identity {
        ContactIdentity::Player(lease) => ActorReference::Player {
            lease: *lease,
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        },
        ContactIdentity::Entity { id, birth } => ActorReference::Entity {
            id: *id,
            birth: birth.clone(),
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        },
    };
    MountObservation {
        actor,
        cut: pumpkin_cluster::actor_occupancy::ActorOccupancyCut {
            position: contact.position,
            sample_tick: contact.simulation_tick,
            simulation_sequence: contact.observed_sequence,
        },
    }
}

impl FrozenActorMount {
    pub(super) fn capture(
        state: &impl ActionState,
        proposal: &ActionProposal,
        captured: &super::cluster_item_use::CapturedItemUseSource,
    ) -> Result<Option<Self>, ApplyFailure> {
        let ClaimedAction::ItemUse(claim) = &proposal.claim else {
            return Ok(None);
        };
        let Some(target) = &claim.target_observation else {
            return Ok(None);
        };
        let Some(Cell {
            value: Value::Entity(body),
            ..
        }) = state.read(TruthView::Local, target.target())
        else {
            return Ok(None);
        };
        if !matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Living(living)
            if living.mob.as_ref().is_some_and(|mob| matches!(mob.species, pumpkin_cluster::mob_continuation::MobSpecies::Horse { .. })))
        {
            return Ok(None);
        }
        let Some(action) = super::cluster_vehicle_interaction::materialize(
            state,
            proposal,
            proposal.tick,
            captured,
        )?
        else {
            return Ok(None);
        };
        let source = captured
            .4
            .source_observation
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?;
        let command = ActorMount {
            source: observation(source),
            target: observation(target),
            hand: if claim.intent.source.loc.inv == pumpkin_cluster::inventory::INV_OFFHAND {
                pumpkin_cluster::action_command::Hand::Off
            } else {
                pumpkin_cluster::action_command::Hand::Main
            },
        };
        let targets = action
            .conditions
            .iter()
            .map(|condition| condition.target)
            .chain(action.effects.iter().flat_map(Effect::targets))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Some(Self {
            command,
            source: captured.4.clone(),
            targets,
        }))
    }

    pub(super) fn targets(&self) -> &[Target] {
        &self.targets
    }

    pub(super) fn bindings(
        &self,
    ) -> Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding> {
        let pumpkin_cluster::action_command::ActorReference::Entity { id, chunk, .. } =
            self.command.target.actor
        else {
            return Vec::new();
        };
        vec![pumpkin_cluster::canonical_inventory::InventorySlotBinding {
            slot: pumpkin_cluster::inventory::InventorySlotRef::new(
                pumpkin_cluster::inventory::InventoryTarget::Entity(id.into()),
                pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT,
                    7,
                ),
            ),
            target: Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(id),
                inventory: Some(InventoryId::Entity {
                    inv: pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT,
                    slot: 7,
                }),
            },
            max_stack: 1,
        }]
    }

    pub(super) fn materialize(
        &self,
        state: &impl ActionState,
        proposal: &ActionProposal,
        tick: pumpkin_cluster::time::TickStamp,
    ) -> Result<Action, ApplyFailure> {
        if proposal.id.actor != self.command.source.actor.actor() {
            return Err(ApplyFailure::InvalidAction);
        }
        let source =
            super::cluster_item_use::normalize_captured_source(state, proposal, &self.source)?;
        super::cluster_vehicle_interaction::materialize(state, proposal, tick, &source)?.ok_or(
            ApplyFailure::Condition(self.command.target.actor.body_target()),
        )
    }

    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        let (records, bytes) = self.source.captured_resource_usage()?;
        let (more_records, more_bytes) =
            super::cluster_inventory_menu::inventory_metadata_resource_usage(&(
                &self.command,
                &self.targets,
            ))?;
        Ok((
            records
                .checked_add(more_records)
                .ok_or(ApplyFailure::InvalidAction)?,
            bytes
                .checked_add(more_bytes)
                .ok_or(ApplyFailure::InvalidAction)?,
        ))
    }
}
