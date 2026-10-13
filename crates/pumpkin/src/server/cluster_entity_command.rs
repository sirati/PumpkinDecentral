use pumpkin_cluster::{
    canonical_action::{ActionId, ActionProposal, ActionState, ApplyFailure, TruthView, Value},
    entity_body::{EntityBody, EntitySpatial},
    item_contact::ContactIdentity,
    player_command_control::{CommandTarget, PlayerCommandMutation, TeleportDestination},
    protocol::ChunkKey,
};

#[derive(Clone)]
pub(super) struct FrozenEntityCommand {
    pub id: ActionId,
    pub association: ChunkKey,
    pub body: EntityBody,
    pub destination: Option<EntitySpatial>,
    pub inventory: Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding>,
    pub detach: Option<pumpkin_cluster::actor_relation::OwnerDetachPlan>,
}

pub(super) fn freeze(
    server: &super::Server,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    state: &impl ActionState,
    proposal: &ActionProposal,
) -> Result<FrozenEntityCommand, ApplyFailure> {
    let (origin, transport_epoch, request) = proposal
        .command
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?;
    super::cluster_player_commands::authorize_authenticated(
        server,
        &super::cluster_player_commands::AuthenticatedPlayerCommand {
            origin: *origin,
            transport_epoch: *transport_epoch,
            request: request.clone(),
        },
    )
    .map_err(|_| ApplyFailure::InvalidAction)?;
    let CommandTarget::Entity { identity, owner } = &request.target else {
        return Err(ApplyFailure::InvalidAction);
    };
    let ContactIdentity::Entity { id, birth } = identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    if proposal.id.actor != pumpkin_cluster::actor::ActorId::Entity(*id)
        || proposal.id.incarnation != u64::from(id.generation)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let association = original
        .original_store()
        .entity_body_chunk(false, *id)
        .ok_or(ApplyFailure::InvalidAction)?;
    let observed = original
        .original_store()
        .entity_body(false, association, *id)
        .ok_or(ApplyFailure::InvalidAction)?;
    let Value::Entity(observed) = &observed.value else {
        return Err(ApplyFailure::InvalidAction);
    };
    if observed.birth != *birth || observed.owner != *owner {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = pumpkin_cluster::canonical_action::Target::Chunk {
        key: association,
        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(*id),
        inventory: None,
    };
    let Value::Entity(body) = state
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::Condition(target))?
        .value
    else {
        return Err(ApplyFailure::Type(target));
    };
    let body = *body;
    if body.birth != *birth || body.owner != *owner || body.spatial_epoch != observed.spatial_epoch
    {
        return Err(ApplyFailure::Condition(target));
    }
    let destination = match &request.mutation {
        PlayerCommandMutation::Tags { .. }
        | PlayerCommandMutation::Kill
        | PlayerCommandMutation::Attribute(_) => None,
        PlayerCommandMutation::Teleport(destination) => {
            let (world, position, rotation) = match destination {
                TeleportDestination::Absolute {
                    world,
                    position,
                    rotation,
                } => (*world, *position, *rotation),
                TeleportDestination::Player { gid, uuid } => {
                    let lease = original
                        .inventory_source_lease(*gid)
                        .filter(|lease| lease.key.0.as_bytes() == uuid)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let presentation = original.combat_target(&ContactIdentity::Player(lease))?;
                    let target = &presentation.contact;
                    if !target.valid() || target.simulation_tick.is_after(proposal.tick) {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    (target.world, target.position.map(f64::from_bits), None)
                }
            };
            let rotation = rotation.unwrap_or([
                f32::from_bits(body.spatial.yaw),
                f32::from_bits(body.spatial.pitch),
            ]);
            Some(EntitySpatial {
                world,
                position: position.map(f64::to_bits),
                velocity_micro: [0; 3],
                yaw: rotation[0].to_bits(),
                pitch: rotation[1].to_bits(),
                on_ground: false,
            })
        }
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let inventory = if matches!(request.mutation, PlayerCommandMutation::Kill)
        && !matches!(
            body.payload,
            pumpkin_cluster::entity_body::EntityPayload::Living(_)
        ) {
        state
            .entity_inventory_layout(&body)?
            .into_iter()
            .map(
                |loc| pumpkin_cluster::canonical_inventory::InventorySlotBinding {
                    slot: pumpkin_cluster::inventory::InventorySlotRef::new(
                        pumpkin_cluster::inventory::InventoryTarget::Entity(body.id.into()),
                        loc,
                    ),
                    target: pumpkin_cluster::canonical_action::Target::Chunk {
                        key: association,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(body.id),
                        inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Entity {
                            inv: loc.inv,
                            slot: loc.slot,
                        }),
                    },
                    max_stack: pumpkin_cluster::inventory::INV_MAX_STACK,
                },
            )
            .collect()
    } else {
        Vec::new()
    };
    let detach = if matches!(request.mutation, PlayerCommandMutation::Kill)
        && !matches!(
            body.payload,
            pumpkin_cluster::entity_body::EntityPayload::Living(_)
        ) {
        let participants = body
            .mount
            .iter()
            .chain(body.passengers.values())
            .map(|identity| match identity {
                ContactIdentity::Player(lease) => original
                    .player_relation_participant(*lease)
                    .ok_or(ApplyFailure::InvalidAction),
                ContactIdentity::Entity { id, birth } => {
                    let chunk = original
                        .original_store()
                        .entity_body_chunk(false, *id)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let cell = original
                        .original_store()
                        .entity_body(false, chunk, *id)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let Value::Entity(other) = &cell.value else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    if &other.birth != birth {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    pumpkin_cluster::actor_relation::ActorRelationParticipant::from_entity(
                        other, chunk,
                    )
                    .ok_or(ApplyFailure::InvalidAction)
                }
            })
            .collect::<Result<Vec<_>, ApplyFailure>>()?;
        Some(
            pumpkin_cluster::actor_relation::capture_owner_detach_from_participants(
                state,
                &body,
                association,
                &participants,
            )
            .ok_or(ApplyFailure::InvalidAction)?,
        )
    } else {
        None
    };
    Ok(FrozenEntityCommand {
        detach,
        inventory,
        id: proposal.id,
        association,
        body,
        destination,
    })
}

impl FrozenEntityCommand {
    pub(super) fn targets(&self) -> Vec<pumpkin_cluster::canonical_action::Target> {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target};
        let source = Target::Chunk {
            key: self.association,
            part: ChunkPart::Entity(self.body.id),
            inventory: None,
        };
        let mut targets = vec![source];
        targets.extend(self.inventory.iter().map(|binding| binding.target));
        if let Some(detach) = &self.detach {
            targets.extend(detach.conditions.iter().map(|condition| condition.target));
            targets.extend(
                detach
                    .effects
                    .iter()
                    .flat_map(pumpkin_cluster::canonical_action::Effect::targets),
            );
        }
        if let Some(spatial) = &self.destination {
            let at = spatial.position.map(f64::from_bits);
            targets.push(Target::Chunk {
                key: ChunkKey::new(
                    spatial.world,
                    (at[0].floor() as i32).div_euclid(16),
                    (at[2].floor() as i32).div_euclid(16),
                ),
                part: ChunkPart::Entity(self.body.id),
                inventory: None,
            });
        } else {
            targets.push(Target::Chunk {
                key: self.association,
                part: ChunkPart::ActorTags {
                    actor: self.id.actor,
                },
                inventory: None,
            });
        }
        targets
    }

    pub(super) fn materialize(
        &self,
        state: &impl ActionState,
        proposal: &ActionProposal,
    ) -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
        use pumpkin_cluster::canonical_action::*;
        if let Some((_, _, request)) = &proposal.command
            && let PlayerCommandMutation::Attribute(mutation) = &request.mutation
        {
            let target = Target::Chunk {
                key: self.association,
                part: ChunkPart::Entity(self.body.id),
                inventory: None,
            };
            return super::cluster_attribute_commands::materialize(
                state,
                self.id,
                proposal.tick,
                target,
                None,
                self.body.spatial_epoch,
                Some((self.body.birth.clone(), self.body.owner)),
                mutation,
            );
        }
        if let Some((_, _, request)) = &proposal.command
            && matches!(request.mutation, PlayerCommandMutation::Kill)
        {
            let target = self.targets()[0];
            let guards = vec![
                Condition {
                    target,
                    expected: Expected::ActorControlled {
                        birth: self.body.birth.clone(),
                        issuer: self.body.owner,
                    },
                },
                Condition {
                    target,
                    expected: Expected::SpatialEpochEquals(self.body.spatial_epoch),
                },
            ];
            if matches!(
                self.body.payload,
                pumpkin_cluster::entity_body::EntityPayload::Living(_)
            ) {
                let combat = pumpkin_cluster::combat_action::CombatIntent {
                    damages: vec![pumpkin_cluster::combat_action::DamageIntent {
                        kind: pumpkin_cluster::combat_action::DamageKind::OutOfWorld,
                        source: self.id.actor,
                        target,
                        tick: proposal.tick,
                        damage_milli: u32::MAX,
                        knockback_micro: [0; 3],
                        bypass_hurt_cooldown: true,
                        fire_ticks: 0,
                        reduction: None,
                        bypass_invulnerability: true,
                    }],
                    ..Default::default()
                };
                let values = combat
                    .targets()
                    .filter_map(|target| {
                        state
                            .read(TruthView::Local, target)
                            .map(|cell| (target, cell))
                    })
                    .collect();
                let mut action =
                    combat.capture_with(state, self.id, self.association, &values, guards)?;
                action.dependencies.push(self.body.birth.action);
                action.client_origin = proposal.client_origin;
                action.validate()?;
                return Ok(action);
            }
            return pumpkin_cluster::canonical_entity_retirement::EntityRetirementIntent {
                target,
                birth: self.body.birth.clone(),
                owner: self.body.owner,
                kind: self.body.kind,
                inventory: self.inventory.clone(),
                living_death: None,
                disposition: pumpkin_cluster::canonical_entity_retirement::EntityInventoryDisposition::Destroy,
            }.capture_action_with_detach(state, self.id, proposal.tick, proposal.client_origin, self.detach.clone());
        }
        if let Some((_, _, request)) = &proposal.command
            && let PlayerCommandMutation::Tags { tag, present } = &request.mutation
        {
            return pumpkin_cluster::actor_tags::ActorTagsIntent {
                actor: self.id.actor,
                chunk: self.association,
                tag: tag.clone(),
                present: *present,
            }
            .capture(
                state,
                pumpkin_cluster::canonical_inventory::InventoryActionCapture {
                    id: self.id,
                    tick: proposal.tick,
                    edit_authority: None,
                    actor_chunk: Some(self.association),
                    player_leases: Vec::new(),
                    player_chunks: Vec::new(),
                    guards: vec![Condition {
                        target: self.targets()[0],
                        expected: Expected::ActorControlled {
                            birth: self.body.birth.clone(),
                            issuer: self.body.owner,
                        },
                    }],
                    client_origin: proposal.client_origin,
                },
            );
        }
        let spatial = self
            .destination
            .clone()
            .ok_or(ApplyFailure::InvalidAction)?;
        let targets = self.targets();
        let Target::Chunk {
            key: destination, ..
        } = targets[1]
        else {
            return Err(ApplyFailure::InvalidAction);
        };
        let mut stakeholders = vec![
            Stakeholder::Chunk(self.association),
            Stakeholder::Chunk(destination),
        ];
        stakeholders.sort_unstable();
        stakeholders.dedup();
        let mut dependencies = vec![self.body.birth.action];
        if let SpatialEpoch::Action(reference) = self.body.spatial_epoch {
            dependencies.push(reference);
        }
        dependencies.sort_unstable();
        dependencies.dedup();
        let action = Action {
            id: self.id,
            tick: proposal.tick,
            class: ActionClass::Infallible,
            chunk_transition: self.association != destination,
            edit_authority: None,
            actor_chunk: Some(self.association),
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            stakeholders,
            dependencies,
            conditions: Vec::new(),
            effects: vec![Effect::MoveEntityBody {
                source: targets[0],
                destination: targets[1],
                spatial,
                authority: Some(EntityMoveAuthority {
                    birth: self.body.birth.clone(),
                    body_owner: self.body.owner,
                    issuer: self.body.owner,
                }),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            presentation: Vec::new(),
            client_origin: ClientOrigin::None,
        };
        action.validate()?;
        Ok(action)
    }
}
