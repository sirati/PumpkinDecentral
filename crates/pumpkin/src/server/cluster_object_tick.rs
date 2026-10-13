use pumpkin_cluster::{
    actor::ActorId,
    actor_action_scope::{ActorActionDraft, FrozenEntityIssueAuthority},
    actor_tick_frame::FrozenActorTickFrame,
    canonical_action::{Action, ActionState, ApplyFailure, ChunkPart, Target, TruthView, Value},
    entity_body::{EntityBody, EntityPayload, EntitySpatial},
    object_tick::ObjectTickPlan,
    protocol::{ChunkKey, WorldId},
    realm_completion::PeerGeneration,
    time::TickStamp,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[derive(Clone)]
pub(super) struct ObjectPhaseObservation {
    pub chunk: ChunkKey,
    pub body: EntityBody,
    pub effective_spatial: EntitySpatial,
}

#[derive(Clone)]
pub(super) struct FrozenObjectTickJob {
    pub observation: ObjectPhaseObservation,
    pub motion: super::cluster_object_motion::ObjectMotion,
    pub plan: ObjectTickPlan,
    pub snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    pub draft: Option<ActorActionDraft>,
    pub terminal_contacts: Arc<Vec<super::cluster_object_terminal::TerminalContact>>,
    pub relation_participants: Arc<Vec<pumpkin_cluster::actor_relation::ActorRelationParticipant>>,
    pub rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    pub do_entity_drops: bool,
}

impl FrozenObjectTickJob {
    pub(super) fn actor(&self) -> ActorId {
        ActorId::Entity(self.observation.body.id)
    }
    pub(super) fn incarnation(&self) -> u64 {
        u64::from(self.observation.body.id.generation)
    }
}

pub(super) struct ObjectEmissionPage {
    pub completed_jobs: usize,
    pub actions: Vec<(Action, ActorActionDraft)>,
    pub samples: Vec<(ChunkKey, pumpkin_cluster::item_spatial::ItemSpatialSample)>,
}

pub(super) fn capture_frame(
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    observations: Vec<ObjectPhaseObservation>,
    source: PeerGeneration,
    tick: TickStamp,
    bounds: &BTreeMap<WorldId, (i32, i32)>,
    drafts: &HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    item_spatial: &pumpkin_cluster::item_spatial::OwnedItemSpatial,
) -> Result<FrozenActorTickFrame<FrozenObjectTickJob>, ApplyFailure> {
    let relation_participants = Arc::new(Vec::new());
    let terminal_contacts = Arc::new(Vec::new());
    let mut jobs = Vec::new();
    for observation in observations {
        let actor = ActorId::Entity(observation.body.id);
        let incarnation = u64::from(observation.body.id.generation);
        if observation.body.owner != source {
            continue;
        }
        let (rules, policy) = snapshot
            .world_rules_at(observation.chunk.world, tick)
            .ok_or(ApplyFailure::MissingStakeholder(
                pumpkin_cluster::canonical_action::Stakeholder::Chunk(observation.chunk),
            ))?;
        let do_entity_drops = policy.do_entity_drops;
        let draft = drafts.get(&(actor, incarnation, tick)).cloned();
        let target = Target::Chunk {
            key: observation.chunk,
            part: ChunkPart::Entity(observation.body.id),
            inventory: None,
        };
        let cell = match &draft {
            Some(draft) => draft.view(snapshot.as_ref()).read(TruthView::Local, target),
            None => snapshot.read(TruthView::Local, target),
        }
        .ok_or(ApplyFailure::Condition(target))?;
        let Value::Entity(body) = cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        let body = *body;
        if body.birth != observation.body.birth
            || body.owner != source
            || body.spatial_epoch != observation.body.spatial_epoch
            || observation.effective_spatial.world != observation.chunk.world
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let object = match &body.payload {
            EntityPayload::Object(state)
                if matches!(
                    state.kind,
                    pumpkin_cluster::object_state::ObjectKind::PrimedTnt(_)
                        | pumpkin_cluster::object_state::ObjectKind::FallingBlock(_)
                ) =>
            {
                Some(state)
            }
            EntityPayload::Vehicle(_) if super::cluster_vehicle_control::due_tnt(&body, tick) => {
                None
            }
            _ => continue,
        };
        let Some((minimum_y, maximum_y)) = bounds.get(&observation.chunk.world) else {
            return Err(ApplyFailure::InvalidAction);
        };
        let motion = if object.is_some() {
            match &draft {
                Some(draft) => super::cluster_object_motion::simulate(
                    &draft.view(snapshot.as_ref()),
                    &body,
                    &observation.effective_spatial,
                    *minimum_y,
                    *maximum_y,
                ),
                None => super::cluster_object_motion::simulate(
                    snapshot.as_ref(),
                    &body,
                    &observation.effective_spatial,
                    *minimum_y,
                    *maximum_y,
                ),
            }?
        } else {
            let mut projected = body.clone();
            projected.spatial = observation.effective_spatial.clone();
            let empty = ActorActionDraft::new(actor, incarnation, tick, 4096, 16384);
            let loaded = snapshot.original_store().owned_chunks().collect();
            let frame = super::cluster_vehicle_control::simulate_uncontrolled(
                snapshot.as_ref(),
                draft.as_ref().unwrap_or(&empty),
                &projected,
                tick,
                &loaded,
            )?;
            let position = frame.effective_spatial.position.map(f64::from_bits);
            super::cluster_object_motion::ObjectMotion {
                destination: ChunkKey::new(
                    frame.effective_spatial.world,
                    (position[0].floor() as i32).div_euclid(16),
                    (position[2].floor() as i32).div_euclid(16),
                ),
                spatial: frame.effective_spatial,
                landing: None,
                fall_distance: 0.0,
            }
        };
        let Some(mut plan) = pumpkin_cluster::object_tick::capture_body(
            &body,
            tick,
            &motion.spatial,
            *minimum_y,
            *maximum_y,
        ) else {
            continue;
        };
        for (_, value) in &mut plan.updates {
            if let pumpkin_cluster::object_property::ObjectPropertyValue::Falling(falling) = value {
                falling.fall_distance = motion.fall_distance.to_bits();
            }
        }
        if plan.terminal.is_none()
            && motion.landing.is_some()
            && let Some(pumpkin_cluster::object_state::ObjectState {
                kind: pumpkin_cluster::object_state::ObjectKind::FallingBlock(falling),
                ..
            }) = object
        {
            plan.terminal = Some(
                pumpkin_cluster::object_tick::ObjectTerminal::FallingLanded {
                    block_state: falling.block_state,
                    drop_item: falling.drop_item,
                },
            );
        }
        jobs.push(FrozenObjectTickJob {
            observation: ObjectPhaseObservation {
                body,
                ..observation
            },
            motion,
            plan,
            snapshot: snapshot.clone(),
            terminal_contacts: terminal_contacts.clone(),
            relation_participants: relation_participants.clone(),
            rules,
            do_entity_drops,
            draft,
        });
    }
    if jobs.iter().any(|job| job.plan.terminal.is_some()) {
        let mut contacts = snapshot
            .player_contacts(source, tick)?
            .into_iter()
            .map(|contact| {
                let height = f64::from_bits(contact.height);
                let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
                    &contact.identity
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let yaw = snapshot
                    .player_projection(lease.gid)
                    .ok_or(ApplyFailure::InvalidAction)?
                    .frame
                    .effective
                    .yaw
                    .to_bits();
                Ok(super::cluster_object_terminal::TerminalContact {
                    yaw,
                    contact,
                    eye_height: if height < 1.0 {
                        0.4
                    } else if height < 1.8 {
                        1.27
                    } else {
                        1.62
                    },
                })
            })
            .collect::<Result<Vec<_>, ApplyFailure>>()?;
        for (chunk, cell) in snapshot.original_store().local_entity_bodies() {
            let Value::Entity(body) = &cell.value else {
                continue;
            };
            let EntityPayload::Living(living) = &body.payload else {
                continue;
            };
            let Some(dimensions) = super::cluster_owned_mob_metadata::dimensions(living, body.kind)
            else {
                continue;
            };
            if dimensions.width <= 0.0 || dimensions.height <= 0.0 {
                continue;
            }
            let projected = item_spatial.project(body);
            if let Some(contact) = pumpkin_cluster::item_contact::ContactObservation::entity(
                body,
                chunk,
                &projected.spatial,
                f64::from(dimensions.width) / 2.0,
                f64::from(dimensions.height),
                tick,
                item_spatial
                    .get(body.id)
                    .map_or(pumpkin_cluster::identity::PlayerSeq(0), |sample| {
                        sample.sequence
                    }),
            ) {
                contacts.push(super::cluster_object_terminal::TerminalContact {
                    contact,
                    eye_height: f64::from(dimensions.eye_height),
                    yaw: projected.spatial.yaw,
                });
            }
        }
        let mut participants = contacts
            .iter()
            .filter_map(|observed| {
                pumpkin_cluster::actor_relation::ActorRelationParticipant::from_contact(
                    &observed.contact,
                )
            })
            .collect::<Vec<_>>();
        participants.extend(snapshot.original_store().local_entity_bodies().filter_map(
            |(chunk, cell)| {
                let Value::Entity(body) = &cell.value else {
                    return None;
                };
                pumpkin_cluster::actor_relation::ActorRelationParticipant::from_entity(body, chunk)
            },
        ));
        let relation_participants = Arc::new(participants);
        let terminal_contacts = Arc::new(contacts);
        for job in jobs.iter_mut().filter(|job| job.plan.terminal.is_some()) {
            job.terminal_contacts = terminal_contacts.clone();
            job.relation_participants = relation_participants.clone();
        }
    }
    jobs.sort_by_key(FrozenObjectTickJob::actor);
    FrozenActorTickFrame::new(source, tick, jobs).ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn emit_continuations(
    frame: &FrozenActorTickFrame<FrozenObjectTickJob>,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    capacity: usize,
) -> Result<ObjectEmissionPage, ApplyFailure> {
    let mut page = ObjectEmissionPage {
        completed_jobs: 0,
        actions: Vec::new(),
        samples: Vec::new(),
    };
    for job in frame.remaining() {
        if job.plan.terminal.is_some() {
            break;
        }
        let crossing = job.motion.destination != job.observation.chunk;
        let count = 1 + usize::from(crossing);
        if count > capacity.saturating_sub(page.actions.len()) {
            break;
        }
        let Some(reservation) =
            clock.reserve(job.actor(), job.incarnation(), frame.tick, count as u32)
        else {
            break;
        };
        let result = (|| {
            let id = reservation.id(0, 15).ok_or(ApplyFailure::InvalidAction)?;
            let target = Target::Chunk {
                key: job.observation.chunk,
                part: ChunkPart::Entity(job.observation.body.id),
                inventory: None,
            };
            let mut draft = match &job.draft {
                Some(draft) => draft.clone(),
                None => ActorActionDraft::new_entity(
                    job.snapshot.as_ref(),
                    FrozenEntityIssueAuthority {
                        target,
                        birth: job.observation.body.birth.clone(),
                        producer: frame.source,
                        controlled_owner: frame.source,
                        effective_tick: frame.tick,
                        next_sequence: id.sequence,
                    },
                    frame.tick,
                    4096,
                    16384,
                )
                .map_err(|_| ApplyFailure::InvalidAction)?,
            };
            let mut actions = Vec::new();
            let EntityPayload::Object(object) = &job.observation.body.payload else {
                return Err(ApplyFailure::InvalidAction);
            };
            let (kind, travel_down) = match &object.kind {
                pumpkin_cluster::object_state::ObjectKind::PrimedTnt(_) => {
                    (pumpkin_cluster::object_tick::ObjectTickKind::Tnt, 0.0f32)
                }
                pumpkin_cluster::object_state::ObjectKind::FallingBlock(falling) => (
                    pumpkin_cluster::object_tick::ObjectTickKind::Falling,
                    (job.motion.fall_distance - f32::from_bits(falling.fall_distance)).max(0.0),
                ),
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let intent = pumpkin_cluster::object_tick::ObjectTickIntent {
                entity: job.observation.body.id,
                birth: job.observation.body.birth.clone(),
                owner: frame.source,
                chunk: job.observation.chunk,
                kind,
                travel_down: travel_down.to_bits(),
            };
            let action = intent.capture(
                &draft.view(job.snapshot.as_ref()),
                id,
                frame.tick,
                job.observation.body.spatial_epoch,
            )?;
            draft
                .issue(job.snapshot.as_ref(), action)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            actions.push((draft.actions().last().unwrap().clone(), draft.clone()));
            if crossing {
                let cell = draft
                    .view(job.snapshot.as_ref())
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::Condition(target))?;
                let Value::Entity(body) = cell.value else {
                    return Err(ApplyFailure::Type(target));
                };
                let body = *body;
                let id = reservation.id(1, 8).ok_or(ApplyFailure::InvalidAction)?;
                let action = pumpkin_cluster::entity_transition::capture(
                    &body,
                    job.observation.chunk,
                    job.motion.spatial.clone(),
                    frame.tick,
                    id,
                )?;
                draft
                    .issue(job.snapshot.as_ref(), action)
                    .map_err(|_| ApplyFailure::InvalidAction)?;
                actions.push((draft.actions().last().unwrap().clone(), draft.clone()));
            }
            Ok::<_, ApplyFailure>(actions)
        })();
        match result {
            Ok(actions) => {
                if !crossing {
                    page.samples.push((
                        job.observation.chunk,
                        pumpkin_cluster::item_spatial::ItemSpatialSample {
                            id: job.observation.body.id,
                            birth: job.observation.body.birth.clone(),
                            epoch: job.observation.body.spatial_epoch,
                            issuer: frame.source,
                            tick: frame.tick,
                            sequence: pumpkin_cluster::identity::PlayerSeq(
                                reservation.id(0, 15).unwrap().sequence,
                            ),
                            spatial: job.motion.spatial.clone(),
                            head_yaw: job.motion.spatial.yaw,
                        },
                    ));
                }
                page.actions.extend(actions);
                page.completed_jobs += 1;
            }
            Err(reason) => {
                clock.release_unused(reservation);
                if page.completed_jobs == 0 {
                    return Err(reason);
                }
                break;
            }
        }
    }
    Ok(page)
}

pub(super) struct FallingLandingContext {
    pub source: pumpkin_cluster::canonical_action::ActionRef,
    pub source_chunk: ChunkKey,
    pub body: EntityBody,
    pub target: pumpkin_cluster::protocol::BlockPos,
    pub supporting_block: pumpkin_cluster::protocol::BlockPos,
    pub effective_spatial: EntitySpatial,
    pub snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    pub draft: ActorActionDraft,
    pub terminal_leg: pumpkin_cluster::object_tick::ObjectTerminalLeg,
}

pub(super) fn landing_context(
    job: &FrozenObjectTickJob,
    source: pumpkin_cluster::canonical_action::ActionRef,
    draft: ActorActionDraft,
) -> Result<FallingLandingContext, ApplyFailure> {
    if !job.plan.updates.iter().any(|(property, value)| {
        *property == pumpkin_cluster::object_state::ObjectProperty::SimulationTick
            && *value
                == pumpkin_cluster::object_property::ObjectPropertyValue::Tick(Some(source.tick))
    }) || source.id.actor != job.actor()
        || source.id.incarnation != job.incarnation()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let supporting = job.motion.landing.ok_or(ApplyFailure::InvalidAction)?;
    let terminal = job
        .plan
        .terminal
        .as_ref()
        .filter(|terminal| {
            matches!(
                terminal,
                pumpkin_cluster::object_tick::ObjectTerminal::FallingLanded { .. }
            )
        })
        .ok_or(ApplyFailure::InvalidAction)?;
    let target_body = Target::Chunk {
        key: job.observation.chunk,
        part: ChunkPart::Entity(job.observation.body.id),
        inventory: None,
    };
    let view = draft.view(job.snapshot.as_ref());
    let Value::Entity(body) = view
        .read(TruthView::Local, target_body)
        .ok_or(ApplyFailure::Condition(target_body))?
        .value
    else {
        return Err(ApplyFailure::Type(target_body));
    };
    let body = *body;
    if body.birth != job.observation.body.birth
        || body.owner != job.observation.body.owner
        || body.spatial_epoch != job.observation.body.spatial_epoch
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let terminal_leg =
        pumpkin_cluster::object_tick::terminal_leg(&body, job.observation.chunk, &view, terminal)?;
    let position = job.motion.spatial.position.map(f64::from_bits);
    Ok(FallingLandingContext {
        source,
        source_chunk: job.observation.chunk,
        body,
        target: pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32,
            y: position[1].floor() as i32,
            z: position[2].floor() as i32,
        },
        supporting_block: pumpkin_cluster::protocol::BlockPos {
            x: supporting.0.x,
            y: supporting.0.y,
            z: supporting.0.z,
        },
        effective_spatial: job.motion.spatial.clone(),
        snapshot: job.snapshot.clone(),
        draft,
        terminal_leg,
    })
}

pub(super) fn prepare_terminal(
    job: &FrozenObjectTickJob,
    reference: pumpkin_cluster::canonical_action::ActionRef,
    server: &Arc<super::Server>,
) -> Result<
    (
        super::cluster_object_terminal::ObjectTerminalContext,
        ActorActionDraft,
        crate::world::loot::LootContextParameters,
        Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    ),
    ApplyFailure,
> {
    let terminal = job
        .plan
        .terminal
        .clone()
        .ok_or(ApplyFailure::InvalidAction)?;
    let target = Target::Chunk {
        key: job.observation.chunk,
        part: ChunkPart::Entity(job.observation.body.id),
        inventory: None,
    };
    if reference.id.actor != job.actor() || reference.id.incarnation != job.incarnation() {
        return Err(ApplyFailure::InvalidAction);
    }
    let draft = match &job.draft {
        Some(draft) => draft.clone(),
        None => ActorActionDraft::new_entity(
            job.snapshot.as_ref(),
            FrozenEntityIssueAuthority {
                target,
                birth: job.observation.body.birth.clone(),
                producer: job.observation.body.owner,
                controlled_owner: job.observation.body.owner,
                effective_tick: reference.tick,
                next_sequence: reference.id.sequence,
            },
            reference.tick,
            4096,
            16384,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?,
    };
    let world = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == job.observation.chunk.world)
        .cloned()
        .ok_or(ApplyFailure::InvalidAction)?;
    let mut loot = crate::world::loot::LootContextParameters::default();
    if let Some(environment) = job.snapshot.mob_environment(job.observation.chunk.world) {
        loot.world_time = environment.time.world_age as u64;
        loot.is_raining = Some(environment.weather.raining);
        loot.is_thundering = Some(environment.weather.thundering);
    }
    let fixed = job.snapshot.clone();
    let witness = pumpkin_cluster::object_tick::ObjectTerminalWitness {
        fall_distance: job.motion.fall_distance.to_bits(),
        combat: Default::default(),
        chunk: job.observation.chunk,
        rules: job.rules,
        do_entity_drops: job.do_entity_drops,
        body: job.observation.body.clone(),
        origin_spatial: job.observation.effective_spatial.clone(),
        effective_spatial: job.motion.spatial.clone(),
        source_epoch: job.observation.body.spatial_epoch,
        source_tick: reference.tick,
        terminal: terminal.clone(),
    };
    let context = super::cluster_object_terminal::ObjectTerminalContext {
        witness,
        reference,
        producer: job.observation.body.owner,
        world,
        chunk: job.observation.chunk,
        body: job.observation.body.clone(),
        spatial: job.motion.spatial.clone(),
        terminal,
        support: job.motion.landing,
        contacts: job.terminal_contacts.clone(),
        relation_participants: job.relation_participants.clone(),
    };
    if !context.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok((context, draft, loot, fixed))
}
